"""T1B-3 contractual signature axis.

An observed signature date is distinct from an inference that signing must
have happened before a later contractual state. This module never parses a
date or assigns an inferred year.
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Literal

from app.core.truth_layer.evidence import (
    AssessmentConflict,
    Evidence,
    ResolutionQuality,
    SourceError,
    build_resolution_quality,
    validate_evidence_set,
)
from app.core.truth_layer.models import ContractLifecycleSignals, FieldObservation
from app.core.truth_layer.sentinels import (
    SentinelAssessment,
    assess_sentinel,
    get_field_sentinel_policy,
)

__all__ = [
    "SignatureStatus",
    "SignatureDateStatus",
    "ContractSignatureAssessment",
    "resolve_contract_signature",
]


_DATASET = "jbjy-vk9h"
_FIELD_STATE = "estado_contrato"
_FIELD_SIGNATURE_DATE = "fecha_de_firma"

SignatureStatus = Literal[
    "directly_observed",
    "inferred_from_post_signature_evidence",
    "not_signed",
    "unknown",
    "conflicting",
]
SignatureDateStatus = Literal["observed", "missing", "not_observed"]

_VALID_SIGNATURE = frozenset(
    {
        "directly_observed",
        "inferred_from_post_signature_evidence",
        "not_signed",
        "unknown",
        "conflicting",
    }
)
_VALID_DATE = frozenset({"observed", "missing", "not_observed"})
_PRE_SIGNATURE_STATES = frozenset({"borrador", "en aprobacion", "aprobado"})
_POST_SIGNATURE_STATES = frozenset(
    {
        "en ejecucion",
        "suspendido",
        "terminado",
        "cerrado",
        "modificado",
        "prorrogado",
        "cedido",
    }
)


def _fold(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = unicodedata.normalize("NFKD", value.casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def _role(observation: FieldObservation, *, contradicts: bool = False) -> str:
    if observation.presence == "missing":
        return "missing"
    if observation.presence == "not_observed":
        return "not_observed"
    return "contradicts" if contradicts else "supports"


def _evidence(
    field: str,
    observation: FieldObservation,
    *,
    code: str,
    contradicts: bool = False,
) -> Evidence:
    assessment: SentinelAssessment | None = None
    normalized = None
    policy = get_field_sentinel_policy(_DATASET, field)
    if policy is not None and observation.presence == "observed":
        assessment = assess_sentinel(observation, policy)
        normalized = assessment.normalized_comparison_value
    return Evidence(
        code=code,
        dataset=_DATASET,
        field=field,
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=normalized,
        role=_role(observation, contradicts=contradicts),  # type: ignore[arg-type]
    )


def _validate_source_errors(
    source_errors: tuple[SourceError, ...],
    signals: ContractLifecycleSignals,
) -> None:
    if not isinstance(source_errors, tuple):
        raise TypeError("source_errors must be a tuple")
    for error in source_errors:
        if not isinstance(error, SourceError):
            raise TypeError("source_errors entries must be SourceError")
        if error.dataset != _DATASET:
            raise ValueError("contract source errors must belong to jbjy-vk9h")
    if source_errors and not any(
        observation.presence == "not_observed"
        for observation in (
            signals.raw_contract_state,
            signals.raw_signature_date,
        )
    ):
        raise ValueError(
            "signature source errors require at least one not_observed input"
        )


def _derive(
    signals: ContractLifecycleSignals,
    source_errors: tuple[SourceError, ...],
) -> tuple[
    SignatureStatus,
    SignatureDateStatus,
    tuple[Evidence, ...],
    ResolutionQuality,
]:
    _validate_source_errors(source_errors, signals)
    state_observation = signals.raw_contract_state
    date_observation = signals.raw_signature_date
    state = (
        _fold(state_observation.raw_value)
        if state_observation.presence == "observed"
        else ""
    )
    date_status: SignatureDateStatus = date_observation.presence
    conflict = (
        date_status == "observed" and state in _PRE_SIGNATURE_STATES
    )

    if conflict:
        status: SignatureStatus = "conflicting"
        reason = "CONTRACT_SIGNATURE_DATE_VS_PRE_SIGNATURE_STATE"
    elif date_status == "observed":
        status = "directly_observed"
        reason = "CONTRACT_SIGNATURE_DATE_OBSERVED"
    elif state in _POST_SIGNATURE_STATES:
        status = "inferred_from_post_signature_evidence"
        reason = "CONTRACT_SIGNATURE_INFERRED_FROM_POST_SIGNATURE_STATE"
    elif state in _PRE_SIGNATURE_STATES:
        status = "not_signed"
        reason = "CONTRACT_SIGNATURE_PRE_SIGNATURE_STATE"
    else:
        status = "unknown"
        reason = (
            "CONTRACT_SIGNATURE_SOURCE_ERROR"
            if source_errors
            else "CONTRACT_SIGNATURE_INDETERMINATE"
        )

    evidence = (
        _evidence(
            _FIELD_STATE,
            state_observation,
            code="CONTRACT_SIGNATURE_STATE_EVIDENCE",
            contradicts=conflict,
        ),
        _evidence(
            _FIELD_SIGNATURE_DATE,
            date_observation,
            code="CONTRACT_SIGNATURE_DATE_EVIDENCE",
            contradicts=conflict,
        ),
    )
    conflicts: tuple[AssessmentConflict, ...] = ()
    if conflict:
        conflicts = (
            AssessmentConflict(
                code="CONTRACT_SIGNATURE_PRE_STATE_WITH_DATE",
                evidences=evidence,
                consequence="CONTRACT_SIGNATURE_STATUS_UNDECIDED",
                severity="material",
            ),
        )

    missing = tuple(
        field
        for field, observation in (
            (_FIELD_STATE, state_observation),
            (_FIELD_SIGNATURE_DATE, date_observation),
        )
        if observation.presence == "missing"
    )
    if status == "conflicting":
        completeness = "complete"
        consistency = "conflicting"
        confidence = "low"
    elif status == "directly_observed" and not missing and not source_errors:
        completeness = "complete"
        consistency = "consistent"
        confidence = "high"
    elif status in {
        "directly_observed",
        "inferred_from_post_signature_evidence",
        "not_signed",
    }:
        completeness = "partial"
        consistency = "unknown" if source_errors else "consistent"
        confidence = "medium" if not source_errors else "low"
    else:
        completeness = "insufficient"
        consistency = "unknown" if source_errors else "consistent"
        confidence = "low"

    quality = build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=(reason,),
        missing_fields=missing,
        source_errors=source_errors,
        conflicts=conflicts,
    )
    return status, date_status, evidence, quality


@dataclass(frozen=True, slots=True)
class ContractSignatureAssessment:
    signals: ContractLifecycleSignals
    signature_status: SignatureStatus
    signature_date_status: SignatureDateStatus
    raw_signature_date: FieldObservation
    raw_contract_state: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality

    def __post_init__(self) -> None:
        if not isinstance(self.signals, ContractLifecycleSignals):
            raise TypeError("signals must be ContractLifecycleSignals")
        if self.signature_status not in _VALID_SIGNATURE:
            raise ValueError(f"invalid SignatureStatus: {self.signature_status!r}")
        if self.signature_date_status not in _VALID_DATE:
            raise ValueError(
                f"invalid SignatureDateStatus: {self.signature_date_status!r}"
            )
        if self.raw_signature_date is not self.signals.raw_signature_date:
            raise ValueError("raw_signature_date must come from signals")
        if self.raw_contract_state is not self.signals.raw_contract_state:
            raise ValueError("raw_contract_state must come from signals")
        validate_evidence_set(self.evidence)
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be ResolutionQuality")
        expected_status, expected_date, evidence, quality = _derive(
            self.signals, self.quality.source_errors
        )
        if self.signature_status != expected_status:
            raise ValueError(
                f"signature_status={self.signature_status!r} contradicts its "
                f"inputs, which yield {expected_status!r}"
            )
        if self.signature_date_status != expected_date:
            raise ValueError(
                "signature_date_status must preserve the T1A presence exactly"
            )
        if self.evidence != evidence:
            raise ValueError("signature evidence must be canonical and complete")
        if self.quality != quality:
            raise ValueError("signature quality must be derived from its inputs")


def resolve_contract_signature(
    signals: ContractLifecycleSignals,
    *,
    source_errors: tuple[SourceError, ...] = (),
) -> ContractSignatureAssessment:
    if not isinstance(signals, ContractLifecycleSignals):
        raise TypeError(
            "resolve_contract_signature requires ContractLifecycleSignals"
        )
    status, date_status, evidence, quality = _derive(signals, source_errors)
    return ContractSignatureAssessment(
        signals=signals,
        signature_status=status,
        signature_date_status=date_status,
        raw_signature_date=signals.raw_signature_date,
        raw_contract_state=signals.raw_contract_state,
        evidence=evidence,
        quality=quality,
    )
