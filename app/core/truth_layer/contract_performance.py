"""T1B-3 contractual performance axis.

Pure interpretation of ``estado_contrato`` from the T1A
``ContractLifecycleSignals`` transport model. Current change literals remain
change evidence; they never become performance by implication.
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import Literal

from app.core.truth_layer.evidence import (
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
    "CONTRACT_DATASET_ID",
    "ContractPerformanceState",
    "ContractPerformanceAssessment",
    "resolve_contract_performance",
]


CONTRACT_DATASET_ID = "jbjy-vk9h"
_FIELD_CONTRACT_STATE = "estado_contrato"

ContractPerformanceState = Literal[
    "draft",
    "approval",
    "executing",
    "suspended",
    "terminated",
    "closed",
    "cancelled",
    "unknown",
]

_VALID_STATES = frozenset(
    {
        "draft",
        "approval",
        "executing",
        "suspended",
        "terminated",
        "closed",
        "cancelled",
        "unknown",
    }
)
_STATE_CATALOG: dict[str, ContractPerformanceState] = {
    "borrador": "draft",
    "en aprobacion": "approval",
    "aprobado": "approval",
    "en ejecucion": "executing",
    "suspendido": "suspended",
    "terminado": "terminated",
    "cerrado": "closed",
    "cancelado": "cancelled",
}
_CHANGE_ONLY_STATES = frozenset({"modificado", "prorrogado", "cedido"})


def _fold(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = unicodedata.normalize("NFKD", value.casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.split())


def _role(observation: FieldObservation) -> str:
    if observation.presence == "missing":
        return "missing"
    if observation.presence == "not_observed":
        return "not_observed"
    return "supports"


def _evidence(observation: FieldObservation) -> Evidence:
    assessment: SentinelAssessment | None = None
    normalized = None
    policy = get_field_sentinel_policy(CONTRACT_DATASET_ID, _FIELD_CONTRACT_STATE)
    if policy is not None and observation.presence == "observed":
        assessment = assess_sentinel(observation, policy)
        normalized = assessment.normalized_comparison_value
    return Evidence(
        code="CONTRACT_PERFORMANCE_STATE_EVIDENCE",
        dataset=CONTRACT_DATASET_ID,
        field=_FIELD_CONTRACT_STATE,
        raw_observation=observation,
        sentinel_assessment=assessment,
        normalized_value=normalized,
        role=_role(observation),  # type: ignore[arg-type]
    )


def _validate_source_errors(
    source_errors: tuple[SourceError, ...],
    observation: FieldObservation,
) -> None:
    if not isinstance(source_errors, tuple):
        raise TypeError("source_errors must be a tuple")
    for error in source_errors:
        if not isinstance(error, SourceError):
            raise TypeError("source_errors entries must be SourceError")
        if error.dataset != CONTRACT_DATASET_ID:
            raise ValueError("contract source errors must belong to jbjy-vk9h")
    if source_errors and observation.presence != "not_observed":
        raise ValueError(
            "a performance source error requires estado_contrato not_observed"
        )


def _derive(
    signals: ContractLifecycleSignals,
    source_errors: tuple[SourceError, ...],
) -> tuple[
    ContractPerformanceState,
    tuple[Evidence, ...],
    ResolutionQuality,
]:
    _validate_source_errors(source_errors, signals.raw_contract_state)
    observation = signals.raw_contract_state
    normalized = (
        _fold(observation.raw_value)
        if observation.presence == "observed"
        else ""
    )

    if normalized in _STATE_CATALOG:
        state = _STATE_CATALOG[normalized]
        reason = f"CONTRACT_PERFORMANCE_{state.upper()}"
    elif normalized in _CHANGE_ONLY_STATES:
        state = "unknown"
        reason = "CONTRACT_STATE_CHANGE_ONLY"
    elif observation.presence == "missing":
        state = "unknown"
        reason = "CONTRACT_STATE_MISSING"
    elif observation.presence == "not_observed":
        state = "unknown"
        reason = (
            "CONTRACT_STATE_SOURCE_ERROR"
            if source_errors
            else "CONTRACT_STATE_NOT_OBSERVED"
        )
    else:
        state = "unknown"
        reason = "CONTRACT_STATE_UNRECOGNIZED"

    if state == "unknown":
        completeness = "insufficient"
        confidence = "low"
    else:
        completeness = "complete"
        confidence = "high"
    consistency = "unknown" if source_errors else "consistent"
    missing = (
        (_FIELD_CONTRACT_STATE,)
        if observation.presence == "missing"
        else ()
    )
    quality = build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=(reason,),
        missing_fields=missing,
        source_errors=source_errors,
    )
    return state, (_evidence(observation),), quality


@dataclass(frozen=True, slots=True)
class ContractPerformanceAssessment:
    """Derived performance state with its T1A provenance."""

    signals: ContractLifecycleSignals
    performance: ContractPerformanceState
    raw_contract_state: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality

    def __post_init__(self) -> None:
        if not isinstance(self.signals, ContractLifecycleSignals):
            raise TypeError("signals must be ContractLifecycleSignals")
        if self.performance not in _VALID_STATES:
            raise ValueError(f"invalid ContractPerformanceState: {self.performance!r}")
        if self.raw_contract_state is not self.signals.raw_contract_state:
            raise ValueError("raw_contract_state must come from signals")
        validate_evidence_set(self.evidence)
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be ResolutionQuality")
        expected, evidence, quality = _derive(
            self.signals, self.quality.source_errors
        )
        if self.performance != expected:
            raise ValueError(
                f"performance={self.performance!r} contradicts estado_contrato; "
                f"the derived value is {expected!r}"
            )
        if self.evidence != evidence:
            raise ValueError("performance evidence must be canonical and complete")
        if self.quality != quality:
            raise ValueError("performance quality must be derived from its inputs")


def resolve_contract_performance(
    signals: ContractLifecycleSignals,
    *,
    source_errors: tuple[SourceError, ...] = (),
) -> ContractPerformanceAssessment:
    """Resolve contractual performance without dates, I/O or inference."""
    if not isinstance(signals, ContractLifecycleSignals):
        raise TypeError(
            "resolve_contract_performance requires ContractLifecycleSignals"
        )
    performance, evidence, quality = _derive(signals, source_errors)
    return ContractPerformanceAssessment(
        signals=signals,
        performance=performance,
        raw_contract_state=signals.raw_contract_state,
        evidence=evidence,
        quality=quality,
    )
