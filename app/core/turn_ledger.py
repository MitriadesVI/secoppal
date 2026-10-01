"""Turn ledger v1: one append-only row per decision stage, per turn (FOUNDATION_ENTRY_ADMISSION M0).

Purely observational. Nothing here decides a route, reads user text to choose
anything, or changes a result: the ledger only remembers WHY a stage went the way
it did, which no other artifact persists today (e.g. why Foundation did or did
not take a turn).

Two rules keep it from becoming a second, unaudited copy of the query:

- ``detail`` holds only identifiers, numbers, booleans and short ``[A-Za-z0-9_.:-]``
  tokens. Anything else (free text, spaces, nested objects) is replaced by a
  fixed marker instead of being stored.
- Digests are short sha256 prefixes of canonical inputs/outputs, never the inputs.

The record lives in a per-turn recorder (a contextvar opened by ``run_query``)
because ``log_trace`` runs before the dispatcher decorates the result on the
legacy route. With no open recorder every ``record`` is a no-op.
"""

from __future__ import annotations

import contextvars
import hashlib
import json
import math
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum

LEDGER_VERSION = 1
DIGEST_CHARS = 16
REDACTED = "<redacted>"
_TOKEN = re.compile(r"[A-Za-z0-9_.:\-]{1,64}")


class StageName(str, Enum):
    PROTOCOL = "PROTOCOL"
    ALPHA = "ALPHA"
    ADMISSION = "ADMISSION"
    INTERPRETER = "INTERPRETER"
    HOST = "HOST"
    CAPABILITY = "CAPABILITY"
    EXECUTION = "EXECUTION"
    PRESENTATION = "PRESENTATION"
    SHADOW = "SHADOW"


class AdmissionCode(str, Enum):
    """Closed set of reasons Foundation took or did not take a turn."""

    FLAG_OFF = "FLAG_OFF"  # never recorded: with the flag off the ledger stays empty (parity)
    CONTINUATION_BLOCKED = "CONTINUATION_BLOCKED"
    INELIGIBLE_SEED_OR_SELECTION = "INELIGIBLE_SEED_OR_SELECTION"
    INELIGIBLE_HISTORY = "INELIGIBLE_HISTORY"
    INELIGIBLE_LEXICAL = "INELIGIBLE_LEXICAL"
    DECLINED_OWNER_ACTOR = "DECLINED_OWNER:ACTOR"
    DECLINED_OWNER_ENTITY_PROFILE = "DECLINED_OWNER:ENTITY_PROFILE"
    DECLINED_OWNER_CONTRACT_ACTIVITY = "DECLINED_OWNER:CONTRACT_ACTIVITY"
    DECLINED_OWNER_ANALYTICS = "DECLINED_OWNER:ANALYTICS"
    DECLINED_OWNER_CAPABILITY_BINDING = "DECLINED_OWNER:CAPABILITY_BINDING"
    DECLINED_OWNER_NATURAL_ALPHA_JOB = "DECLINED_OWNER:NATURAL_ALPHA_JOB"
    ADMITTED = "ADMITTED"


def declined_by(owner: str) -> AdmissionCode:
    """The closed code for an owner name; an unknown owner is a bug, not a new code."""
    return AdmissionCode(f"DECLINED_OWNER:{owner}")


def digest(value) -> str | None:
    """Short sha256 of a canonical value; ``None`` when there is nothing to digest."""
    if value is None:
        return None
    raw = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str,
                                                          separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8", errors="surrogatepass")).hexdigest()[:DIGEST_CHARS]


def _clean(value, depth: int = 0):
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, Enum):
        value = value.value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        return value if _TOKEN.fullmatch(value) else REDACTED
    if isinstance(value, (list, tuple)) and depth < 2:
        return [_clean(item, depth + 1) for item in value[:32]]
    return REDACTED


def _token(value) -> str | None:
    cleaned = _clean(value)
    return cleaned if isinstance(cleaned, str) else None


@dataclass(frozen=True)
class Stage:
    name: StageName
    authority: str
    decision: str
    reason_code: str | None = None
    input_digest: str | None = None
    output_digest: str | None = None
    detail: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "name": StageName(self.name).value,
            "authority": _token(self.authority),
            "decision": _token(self.decision),
            "reason_code": _token(self.reason_code),
            "input_digest": _token(self.input_digest),
            "output_digest": _token(self.output_digest),
            "detail": {str(_token(key)): _clean(item) for key, item in dict(self.detail or {}).items()},
        }


@dataclass
class TurnLedger:
    stages: list[Stage] = field(default_factory=list)

    def append(self, stage: Stage) -> None:
        self.stages.append(stage)

    def to_dict(self) -> dict:
        return {"version": LEDGER_VERSION, "stages": [stage.to_dict() for stage in self.stages]}


# ── per-turn recorder ─────────────────────────────────────────────────────────
_ledger: contextvars.ContextVar[TurnLedger | None] = contextvars.ContextVar("secoppal_turn_ledger", default=None)


@contextmanager
def turn_ledger_scope():
    """Open this turn's ledger; the token restores an outer one on nesting."""
    token = _ledger.set(TurnLedger())
    try:
        yield _ledger.get()
    finally:
        _ledger.reset(token)


def record(stage: Stage) -> None:
    """Append a row. No-op when no turn is recording."""
    current = _ledger.get()
    if current is not None:
        current.append(stage)


def current_ledger() -> TurnLedger | None:
    return _ledger.get()


def turn_ledger_trace() -> dict:
    """Trace keys for the open turn: empty unless at least one row exists."""
    current = _ledger.get()
    if current is None or not current.stages:
        return {}
    return {"turn_ledger": current.to_dict()}


# ── stage builders ────────────────────────────────────────────────────────────
_ADMISSION_DECISION = {AdmissionCode.ADMITTED: "ADMIT", AdmissionCode.CONTINUATION_BLOCKED: "BLOCK"}


def admission_stage(code: AdmissionCode, query: str) -> Stage:
    return Stage(StageName.ADMISSION, "FOUNDATION_GATE", _ADMISSION_DECISION.get(code, "DECLINE"),
                 reason_code=AdmissionCode(code).value, input_digest=digest(query))


def record_admission(code: AdmissionCode, query: str) -> None:
    record(admission_stage(code, query))


def production_stages(telemetry: dict, result: dict, query: str) -> list[Stage]:
    """INTERPRETER / HOST / EXECUTION rows mapped from ``foundation_telemetry``.

    Only already-enumerated fields are read; the provider payload and the user
    text never enter. Digests cover the canonical acceptance, universe policy and
    the ordered published row ids when the result carries them.
    """
    h7_authority = list(telemetry.get("h7_authority") or ())
    reason = telemetry.get("internal_reason_code")
    valid = telemetry.get("proposal_valid")
    interpreter = Stage(
        StageName.INTERPRETER, "PRODUCER", "PROPOSED" if valid else "NO_PROPOSAL",
        reason_code=telemetry.get("acceptance_status"), input_digest=digest(query),
        output_digest=digest(result.get("foundation_acceptance")),
        detail={"producer_version": telemetry.get("producer_version"),
                "prompt_sha256": telemetry.get("prompt_sha256"),
                "provider_latency_ms": telemetry.get("provider_latency_ms"),
                "proposal_valid": valid, "acceptance_status": telemetry.get("acceptance_status")})
    host = Stage(
        StageName.HOST, "H7", ("CONFLICT" if telemetry.get("h7_conflict") else "APPLIED") if h7_authority
        else "NOT_APPLIED",
        reason_code=reason, output_digest=digest(result.get("foundation_universe")),
        detail={"h7_authority": h7_authority, "h7_rule_ids": [list(rules) for rules in
                                                                (telemetry.get("h7_rule_ids") or ())],
                "internal_reason_code": reason})
    row_ids = [group.get("row_ids") for group in result.get("groups") or () if isinstance(group, dict)]
    execution = Stage(
        StageName.EXECUTION, "EXECUTION_GUARD", result.get("foundation_terminal") or "UNKNOWN",
        reason_code=result.get("outcome") or None, output_digest=digest(row_ids) if row_ids else None,
        detail={"source_calls": telemetry.get("source_calls"), "outcome": result.get("outcome") or None})
    return [interpreter, host, execution]


def record_production(telemetry: dict, result: dict, query: str) -> None:
    for stage in production_stages(telemetry, result, query):
        record(stage)
