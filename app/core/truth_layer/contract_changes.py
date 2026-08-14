"""T1B-3 contractual change axes.

The current ``estado_contrato`` literal can positively indicate a change, but
it cannot prove that another change never happened historically. Signal,
source coverage and historical conclusion therefore remain separate.
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
    "ChangeKind",
    "ChangeSignal",
    "SourceCoverage",
    "HistoricalChangeConclusion",
    "ChangeAxisAssessment",
    "ContractChangeAssessment",
    "resolve_contract_changes",
]


_DATASET = "jbjy-vk9h"
_FIELD_STATE = "estado_contrato"

ChangeKind = Literal["modified", "extended", "assigned"]
ChangeSignal = Literal["observed", "not_indicated", "conflicting"]
SourceCoverage = Literal["sufficient", "partial", "insufficient", "unknown"]
HistoricalChangeConclusion = Literal[
    "observed",
    "indeterminate",
    "conflicting",
]

_VALID_KINDS = frozenset({"modified", "extended", "assigned"})
_VALID_SIGNALS = frozenset({"observed", "not_indicated", "conflicting"})
_VALID_COVERAGE = frozenset(
    {"sufficient", "partial", "insufficient", "unknown"}
)
_VALID_HISTORICAL = frozenset(
    {"observed", "indeterminate", "conflicting"}
)
_LITERAL_FOR_KIND: dict[str, str] = {
    "modified": "modificado",
    "extended": "prorrogado",
    "assigned": "cedido",
}


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


def _state_evidence(observation: FieldObservation) -> Evidence:
    assessment: SentinelAssessment | None = None
    normalized = None
    policy = get_field_sentinel_policy(_DATASET, _FIELD_STATE)
    if policy is not None and observation.presence == "observed":
        assessment = assess_sentinel(observation, policy)
        normalized = assessment.normalized_comparison_value
    return Evidence(
        code="CONTRACT_CHANGE_STATE_EVIDENCE",
        dataset=_DATASET,
        field=_FIELD_STATE,
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
        if error.dataset != _DATASET:
            raise ValueError("contract source errors must belong to jbjy-vk9h")
    if source_errors and observation.presence != "not_observed":
        raise ValueError(
            "change source errors require estado_contrato not_observed"
        )


def _derive_axis(
    kind: ChangeKind,
    signals: ContractLifecycleSignals,
    source_errors: tuple[SourceError, ...],
) -> tuple[
    ChangeSignal,
    SourceCoverage,
    HistoricalChangeConclusion,
    tuple[Evidence, ...],
    ResolutionQuality,
]:
    _validate_source_errors(source_errors, signals.raw_contract_state)
    observation = signals.raw_contract_state
    current = (
        _fold(observation.raw_value)
        if observation.presence == "observed"
        else ""
    )
    if observation.presence == "observed":
        coverage: SourceCoverage = "partial"
        if current == _LITERAL_FOR_KIND[kind]:
            signal: ChangeSignal = "observed"
            historical: HistoricalChangeConclusion = "observed"
            axis_reason = f"CONTRACT_CHANGE_{kind.upper()}_OBSERVED"
        else:
            signal = "not_indicated"
            historical = "indeterminate"
            axis_reason = f"CONTRACT_CHANGE_{kind.upper()}_NOT_INDICATED"
        completeness = "partial"
        consistency = "consistent"
        confidence = "medium"
        source_reason = "CONTRACT_CHANGE_CURRENT_STATE_COVERAGE_PARTIAL"
    elif observation.presence == "missing":
        signal = "not_indicated"
        coverage = "insufficient"
        historical = "indeterminate"
        completeness = "insufficient"
        consistency = "unknown"
        confidence = "low"
        axis_reason = f"CONTRACT_CHANGE_{kind.upper()}_NOT_INDICATED"
        source_reason = "CONTRACT_CHANGE_STATE_MISSING"
    else:
        signal = "not_indicated"
        coverage = "unknown" if source_errors else "insufficient"
        historical = "indeterminate"
        completeness = "insufficient"
        consistency = "unknown"
        confidence = "low"
        axis_reason = f"CONTRACT_CHANGE_{kind.upper()}_NOT_INDICATED"
        source_reason = (
            "CONTRACT_CHANGE_STATE_SOURCE_ERROR"
            if source_errors
            else "CONTRACT_CHANGE_STATE_NOT_OBSERVED"
        )

    missing = (
        (_FIELD_STATE,)
        if observation.presence == "missing"
        else ()
    )
    evidence = (_state_evidence(observation),)
    quality = build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=(axis_reason, source_reason),
        missing_fields=missing,
        source_errors=source_errors,
    )
    return signal, coverage, historical, evidence, quality


@dataclass(frozen=True, slots=True)
class ChangeAxisAssessment:
    signals: ContractLifecycleSignals
    kind: ChangeKind
    signal: ChangeSignal
    source_coverage: SourceCoverage
    historical_conclusion: HistoricalChangeConclusion
    raw_contract_state: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality

    def __post_init__(self) -> None:
        if not isinstance(self.signals, ContractLifecycleSignals):
            raise TypeError("signals must be ContractLifecycleSignals")
        if self.kind not in _VALID_KINDS:
            raise ValueError(f"invalid ChangeKind: {self.kind!r}")
        if self.signal not in _VALID_SIGNALS:
            raise ValueError(f"invalid ChangeSignal: {self.signal!r}")
        if self.source_coverage not in _VALID_COVERAGE:
            raise ValueError(f"invalid SourceCoverage: {self.source_coverage!r}")
        if self.historical_conclusion not in _VALID_HISTORICAL:
            raise ValueError(
                "invalid HistoricalChangeConclusion: "
                f"{self.historical_conclusion!r}"
            )
        if self.raw_contract_state is not self.signals.raw_contract_state:
            raise ValueError("raw_contract_state must come from signals")
        validate_evidence_set(self.evidence)
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be ResolutionQuality")
        signal, coverage, historical, evidence, quality = _derive_axis(
            self.kind, self.signals, self.quality.source_errors
        )
        if (
            self.signal,
            self.source_coverage,
            self.historical_conclusion,
        ) != (signal, coverage, historical):
            raise ValueError(
                f"{self.kind} change conclusion contradicts estado_contrato"
            )
        if self.evidence != evidence:
            raise ValueError("change-axis evidence must be canonical and complete")
        if self.quality != quality:
            raise ValueError("change-axis quality must be derived from its inputs")


def _axis(
    kind: ChangeKind,
    signals: ContractLifecycleSignals,
    source_errors: tuple[SourceError, ...],
) -> ChangeAxisAssessment:
    signal, coverage, historical, evidence, quality = _derive_axis(
        kind, signals, source_errors
    )
    return ChangeAxisAssessment(
        signals=signals,
        kind=kind,
        signal=signal,
        source_coverage=coverage,
        historical_conclusion=historical,
        raw_contract_state=signals.raw_contract_state,
        evidence=evidence,
        quality=quality,
    )


def _parent_quality(
    axes: tuple[ChangeAxisAssessment, ...],
) -> ResolutionQuality:
    completeness = (
        "insufficient"
        if any(axis.quality.completeness == "insufficient" for axis in axes)
        else "partial"
    )
    consistency = (
        "conflicting"
        if any(axis.quality.consistency == "conflicting" for axis in axes)
        else (
            "unknown"
            if any(axis.quality.consistency == "unknown" for axis in axes)
            else "consistent"
        )
    )
    confidence = (
        "low"
        if any(axis.quality.confidence == "low" for axis in axes)
        else "medium"
    )
    return build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=(
            "CONTRACT_CHANGES_AXES_PRESERVED",
            *(
                code
                for axis in axes
                for code in axis.quality.reason_codes
            ),
        ),
        missing_fields=tuple(
            field for axis in axes for field in axis.quality.missing_fields
        ),
        source_errors=tuple(
            error for axis in axes for error in axis.quality.source_errors
        ),
        conflicts=tuple(
            conflict for axis in axes for conflict in axis.quality.conflicts
        ),
    )


@dataclass(frozen=True, slots=True)
class ContractChangeAssessment:
    signals: ContractLifecycleSignals
    modified: ChangeAxisAssessment
    extended: ChangeAxisAssessment
    assigned: ChangeAxisAssessment
    raw_contract_state: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality

    def __post_init__(self) -> None:
        if not isinstance(self.signals, ContractLifecycleSignals):
            raise TypeError("signals must be ContractLifecycleSignals")
        axes = (self.modified, self.extended, self.assigned)
        for axis in axes:
            if not isinstance(axis, ChangeAxisAssessment):
                raise TypeError("change axes must be ChangeAxisAssessment")
            if axis.signals is not self.signals:
                raise ValueError("all change axes must share the same signals object")
        if tuple(axis.kind for axis in axes) != (
            "modified",
            "extended",
            "assigned",
        ):
            raise ValueError("change axes are assigned to the wrong named fields")
        if len({axis.quality.source_errors for axis in axes}) != 1:
            raise ValueError("all change axes must preserve the same source errors")
        if self.raw_contract_state is not self.signals.raw_contract_state:
            raise ValueError("raw_contract_state must come from signals")
        validate_evidence_set(self.evidence)
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be ResolutionQuality")
        expected_evidence = self.modified.evidence
        if self.evidence != expected_evidence:
            raise ValueError("change evidence must be shared and canonical")
        expected_quality = _parent_quality(axes)
        if self.quality != expected_quality:
            raise ValueError("change quality must aggregate all three axes")


def resolve_contract_changes(
    signals: ContractLifecycleSignals,
    *,
    source_errors: tuple[SourceError, ...] = (),
) -> ContractChangeAssessment:
    if not isinstance(signals, ContractLifecycleSignals):
        raise TypeError("resolve_contract_changes requires ContractLifecycleSignals")
    modified = _axis("modified", signals, source_errors)
    extended = _axis("extended", signals, source_errors)
    assigned = _axis("assigned", signals, source_errors)
    axes = (modified, extended, assigned)
    return ContractChangeAssessment(
        signals=signals,
        modified=modified,
        extended=extended,
        assigned=assigned,
        raw_contract_state=signals.raw_contract_state,
        evidence=modified.evidence,
        quality=_parent_quality(axes),
    )
