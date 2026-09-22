"""Typed Alpha receipt and narrow contract-unit accreditation.

The job pair proposes only the operation.  The host preserves its exact grounded
interval and independently accredits the contractual unit from structural source
occurrences, falling back to one literal ``contrato(s)`` token inside that interval.
The full evidence quote is never consumed as executable evidence.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

from app.core.analytics.contract_activity.models import Operation
from app.core.source_occurrence import source_request_occurrences
from app.utils.money import normalize_text


CONTRACT_ACTIVITY = "analytics.contract_activity"

_PAIR_OPERATIONS = {
    ("ANALYZE", "CONTRACT_COUNT"): Operation.TOTAL,
    ("ANALYZE", "CONTRACTS_BY_MODALITY"): Operation.BY_MODALITY,
    ("ANALYZE", "CONTRACTS_BY_STATE"): Operation.BY_REPORTED_STATE,
}
_CONTRACT_TOKEN = re.compile(r"(?<!\w)contratos?(?!\w)", re.IGNORECASE)
_PROCESS_TOKEN = re.compile(r"(?<!\w)procesos?(?!\w)", re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ContractActivityBinding:
    capability: str
    action: str
    aspect: str
    evidence_interval: tuple[int, int]
    operation: Operation

    @classmethod
    def from_job(cls, job) -> ContractActivityBinding | None:
        operation = _PAIR_OPERATIONS.get((job.action, job.aspect))
        if operation is None:
            return None
        return cls(
            CONTRACT_ACTIVITY,
            job.action,
            job.aspect,
            (job.start, job.end),
            operation,
        )

    def receipt(self, raw: str) -> dict:
        return {
            "capability": self.capability,
            "action": self.action,
            "aspect": self.aspect,
            "operation": self.operation.value,
            "evidence_interval": list(self.evidence_interval),
            "raw_fingerprint": hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        }


@dataclass(frozen=True, slots=True)
class UnitAccreditation:
    accredited: bool
    reason: str
    authority: str = ""
    span: tuple[int, int] | None = None


def accredit_contract_unit(raw: str, binding: ContractActivityBinding) -> UnitAccreditation:
    """Accredit one compatible contract unit without trusting the whole quote."""
    start, end = binding.evidence_interval
    if start < 0 or end <= start or end > len(raw):
        return UnitAccreditation(False, "EVIDENCE_INTERVAL_INVALID")

    normalized = normalize_text(raw)
    if _PROCESS_TOKEN.search(normalized):
        return UnitAccreditation(False, "PROCESS_CONTRACT_UNIT_CONFLICT")
    occurrences = source_request_occurrences(normalized)
    process_occurrences = tuple(item for item in occurrences if item.is_process)
    contract_occurrences = tuple(item for item in occurrences if not item.is_process)
    if process_occurrences:
        return UnitAccreditation(False, "PROCESS_CONTRACT_UNIT_CONFLICT")
    if len(contract_occurrences) == 1:
        item = contract_occurrences[0]
        return UnitAccreditation(True, "", "source_request_occurrences", item.span)
    if len(contract_occurrences) > 1:
        return UnitAccreditation(False, "MULTIPLE_CONTRACT_UNITS")

    interval = raw[start:end]
    tokens = tuple(_CONTRACT_TOKEN.finditer(interval))
    if len(tokens) != 1:
        return UnitAccreditation(False, "CONTRACT_UNIT_NOT_ACCREDITED")
    token = tokens[0]
    return UnitAccreditation(
        True,
        "",
        "grounded_interval_literal",
        (start + token.start(), start + token.end()),
    )
