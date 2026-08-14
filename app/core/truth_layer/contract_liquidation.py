"""T1B-3 contractual liquidation axis.

The liquidation flag and liquidation dates govern this assessment.
``estado_contrato="Cerrado"`` is retained as corroborating evidence only and
never rewritten into liquidation.
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
    "LiquidationStatus",
    "LiquidationCompletion",
    "ContractLiquidationAssessment",
    "resolve_contract_liquidation",
]


_DATASET = "jbjy-vk9h"
_FIELD_STATE = "estado_contrato"
_FIELD_END = "fecha_fin_liquidacion"
_FIELD_START = "fecha_inicio_liquidacion"
_FIELD_FLAG = "liquidaci_n"

LiquidationStatus = Literal[
    "yes",
    "no",
    "in_progress",
    "unknown",
    "conflicting",
]
LiquidationCompletion = Literal[
    "finished",
    "started",
    "not_started",
    "unknown",
    "conflicting",
]

_VALID_STATUS = frozenset(
    {"yes", "no", "in_progress", "unknown", "conflicting"}
)
_VALID_COMPLETION = frozenset(
    {"finished", "started", "not_started", "unknown", "conflicting"}
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
            signals.raw_liquidation_flag,
            signals.raw_liquidation_start_date,
            signals.raw_liquidation_end_date,
        )
    ):
        raise ValueError(
            "liquidation source errors require at least one not_observed input"
        )


def _derive(
    signals: ContractLifecycleSignals,
    source_errors: tuple[SourceError, ...],
) -> tuple[
    LiquidationStatus,
    LiquidationCompletion,
    FieldObservation | None,
    tuple[Evidence, ...],
    ResolutionQuality,
]:
    _validate_source_errors(source_errors, signals)
    flag_observation = signals.raw_liquidation_flag
    start_observation = signals.raw_liquidation_start_date
    end_observation = signals.raw_liquidation_end_date
    flag = (
        _fold(flag_observation.raw_value)
        if flag_observation.presence == "observed"
        else ""
    )
    flag_yes = flag == "si"
    flag_no = flag == "no"
    start = start_observation.presence == "observed"
    end = end_observation.presence == "observed"
    flag_date_conflict = flag_no and (start or end)
    sequence_conflict = end and not start and not flag_yes and not flag_no

    if flag_date_conflict:
        status: LiquidationStatus = "conflicting"
        completion: LiquidationCompletion = "conflicting"
        reason = "LIQUIDATION_NEGATIVE_FLAG_WITH_DATE"
    elif flag_yes and end:
        status = "yes"
        completion = "finished"
        reason = "LIQUIDATION_FLAG_YES_WITH_END_DATE"
    elif flag_yes and start:
        status = "in_progress"
        completion = "started"
        reason = "LIQUIDATION_FLAG_YES_WITH_START_DATE"
    elif flag_yes:
        status = "yes"
        completion = "unknown"
        reason = "LIQUIDATION_FLAG_YES_WITHOUT_DATES"
    elif flag_no:
        status = "no"
        completion = "not_started"
        reason = "LIQUIDATION_FLAG_NO_WITHOUT_DATES"
    elif end:
        status = "yes"
        completion = "finished"
        reason = (
            "LIQUIDATION_END_DATE_WITHOUT_START"
            if sequence_conflict
            else "LIQUIDATION_INFERRED_FROM_END_DATE"
        )
    elif start:
        status = "in_progress"
        completion = "started"
        reason = "LIQUIDATION_INFERRED_FROM_START_DATE"
    else:
        status = "unknown"
        completion = "unknown"
        reason = (
            "LIQUIDATION_SOURCE_ERROR"
            if source_errors
            else "LIQUIDATION_INDETERMINATE"
        )

    conflicting_fields: set[str] = set()
    if flag_date_conflict:
        conflicting_fields.add(_FIELD_FLAG)
        if start:
            conflicting_fields.add(_FIELD_START)
        if end:
            conflicting_fields.add(_FIELD_END)
    if sequence_conflict:
        conflicting_fields.add(_FIELD_END)

    # This literal order is the canonical Evidence order: field names are
    # ascending and every field occurs exactly once in this resolver.
    evidence = (
        _evidence(
            _FIELD_STATE,
            signals.raw_contract_state,
            code="CONTRACT_LIQUIDATION_STATE_EVIDENCE",
        ),
        _evidence(
            _FIELD_END,
            end_observation,
            code="LIQUIDATION_END_DATE_EVIDENCE",
            contradicts=_FIELD_END in conflicting_fields,
        ),
        _evidence(
            _FIELD_START,
            start_observation,
            code="LIQUIDATION_START_DATE_EVIDENCE",
            contradicts=_FIELD_START in conflicting_fields,
        ),
        _evidence(
            _FIELD_FLAG,
            flag_observation,
            code="LIQUIDATION_FLAG_EVIDENCE",
            contradicts=_FIELD_FLAG in conflicting_fields,
        ),
    )
    by_field = {item.field: item for item in evidence}
    conflicts: list[AssessmentConflict] = []
    if flag_date_conflict:
        conflicts.append(
            AssessmentConflict(
                code="LIQUIDATION_FLAG_VS_DATES",
                evidences=tuple(
                    by_field[field]
                    for field in (_FIELD_END, _FIELD_START, _FIELD_FLAG)
                    if field in conflicting_fields
                ),
                consequence="LIQUIDATION_STATUS_UNDECIDED",
                severity="critical",
            )
        )
    if sequence_conflict:
        conflicts.append(
            AssessmentConflict(
                code="LIQUIDATION_END_WITHOUT_START",
                evidences=(by_field[_FIELD_END], by_field[_FIELD_START]),
                consequence="LIQUIDATION_SEQUENCE_INCOMPLETE",
                severity="material",
            )
        )

    missing = tuple(
        field
        for field, observation in (
            (_FIELD_STATE, signals.raw_contract_state),
            (_FIELD_END, end_observation),
            (_FIELD_START, start_observation),
            (_FIELD_FLAG, flag_observation),
        )
        if observation.presence == "missing"
    )
    if conflicts:
        completeness = "partial"
        consistency = "conflicting"
        confidence = "low"
    elif status == "unknown":
        completeness = "insufficient"
        consistency = "unknown" if source_errors else "consistent"
        confidence = "low"
    elif flag_no and not start and not end:
        completeness = "complete"
        consistency = "consistent"
        confidence = "high"
    elif flag_yes and end and start and not source_errors:
        completeness = "complete"
        consistency = "consistent"
        confidence = "high"
    else:
        completeness = "partial"
        consistency = "unknown" if source_errors else "consistent"
        confidence = "low" if source_errors else "medium"

    quality = build_resolution_quality(
        completeness=completeness,
        consistency=consistency,
        confidence=confidence,
        reason_codes=(reason,),
        missing_fields=missing,
        source_errors=source_errors,
        conflicts=tuple(conflicts),
    )
    completion_event = end_observation if end else None
    return status, completion, completion_event, evidence, quality


@dataclass(frozen=True, slots=True)
class ContractLiquidationAssessment:
    signals: ContractLifecycleSignals
    liquidation_status: LiquidationStatus
    completion: LiquidationCompletion
    completion_event_observation: FieldObservation | None
    raw_contract_state: FieldObservation
    raw_liquidation_flag: FieldObservation
    raw_liquidation_start_date: FieldObservation
    raw_liquidation_end_date: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality

    def __post_init__(self) -> None:
        if not isinstance(self.signals, ContractLifecycleSignals):
            raise TypeError("signals must be ContractLifecycleSignals")
        if self.liquidation_status not in _VALID_STATUS:
            raise ValueError(
                f"invalid LiquidationStatus: {self.liquidation_status!r}"
            )
        if self.completion not in _VALID_COMPLETION:
            raise ValueError(
                f"invalid LiquidationCompletion: {self.completion!r}"
            )
        for name in (
            "raw_contract_state",
            "raw_liquidation_flag",
            "raw_liquidation_start_date",
            "raw_liquidation_end_date",
        ):
            signal_name = name
            if getattr(self, name) is not getattr(self.signals, signal_name):
                raise ValueError(f"{name} must come from signals")
        validate_evidence_set(self.evidence)
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be ResolutionQuality")
        status, completion, event, evidence, quality = _derive(
            self.signals, self.quality.source_errors
        )
        if (self.liquidation_status, self.completion) != (status, completion):
            raise ValueError(
                "liquidation status/completion contradict flag and dates"
            )
        if self.completion_event_observation is not event:
            raise ValueError(
                "completion event must be the observed raw liquidation end date"
            )
        if self.evidence != evidence:
            raise ValueError("liquidation evidence must be canonical and complete")
        if self.quality != quality:
            raise ValueError("liquidation quality must be derived from its inputs")


def resolve_contract_liquidation(
    signals: ContractLifecycleSignals,
    *,
    source_errors: tuple[SourceError, ...] = (),
) -> ContractLiquidationAssessment:
    if not isinstance(signals, ContractLifecycleSignals):
        raise TypeError(
            "resolve_contract_liquidation requires ContractLifecycleSignals"
        )
    status, completion, event, evidence, quality = _derive(
        signals, source_errors
    )
    return ContractLiquidationAssessment(
        signals=signals,
        liquidation_status=status,
        completion=completion,
        completion_event_observation=event,
        raw_contract_state=signals.raw_contract_state,
        raw_liquidation_flag=signals.raw_liquidation_flag,
        raw_liquidation_start_date=signals.raw_liquidation_start_date,
        raw_liquidation_end_date=signals.raw_liquidation_end_date,
        evidence=evidence,
        quality=quality,
    )
