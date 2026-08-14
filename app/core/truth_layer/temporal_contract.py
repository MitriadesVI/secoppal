"""SECOP Truth Layer T1B-4 — contract axes as temporal questions.

One responsibility: turning an already-resolved CONTRACT axis (T1B-3) into a
temporal question, without importing or modifying that axis. Inputs arrive
through STRUCTURAL protocols, so the module DAG keeps its shape.

Because a structural input is not a real type, every status is checked against
the presence of its own raw observation: a duck-typed stub may not claim an
inference and an observed valid date at once — a combination the real T1B-3
resolvers can never produce.

The axis's own evidence, conflicts and source errors travel with the question.
A valid date never upgrades a conflicted axis into a confirmed fact.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_3_CONTRACT_CLOSEOUT.md
"""
from __future__ import annotations

from typing import Literal, Protocol

from app.core.truth_layer.evidence import (
    AssessmentConflict,
    Evidence,
    ResolutionQuality,
    SourceError,
)
from app.core.truth_layer.models import FieldObservation, TemporalQueryContext
from app.core.truth_layer.temporal_range import TemporalEvent
from app.core.truth_layer.temporal_resolver import (
    TemporalAssessment,
    build_temporal_assessment,
    validate_source_errors,
)
from app.core.truth_layer.temporal_value import (
    SECOP_SOURCE_TIMEZONE,
    TemporalValueResolution,
    resolve_temporal_value,
)

__all__ = [
    "CONTRACT_DATASET_ID",
    "SignatureTemporalInput",
    "LiquidationTemporalInput",
    "resolve_signature_event",
    "resolve_liquidation_event",
]


# Inlined rather than imported: the DAG forbids importing a sibling contract
# resolver, and a dataset id is not an abstraction worth that edge.
CONTRACT_DATASET_ID = "jbjy-vk9h"

_FIELD_SIGNATURE_DATE = "fecha_de_firma"
_FIELD_LIQUIDATION_START = "fecha_inicio_liquidacion"
_FIELD_LIQUIDATION_END = "fecha_fin_liquidacion"


def _typed_tuple(items: object, expected: type, label: str) -> tuple:
    if not isinstance(items, tuple):
        raise TypeError(f"{label} must be a tuple")
    for item in items:
        if not isinstance(item, expected):
            raise TypeError(f"{label} entries must be {expected.__name__}")
    return items


class SignatureTemporalInput(Protocol):
    """The part of a signature assessment this module reads.

    Structural, so ``contract_signature.py`` is neither imported nor modified
    and the module DAG keeps its shape. A real
    ``ContractSignatureAssessment`` satisfies it as-is. ``evidence`` and
    ``quality`` are part of the contract precisely so that the axis's
    conflicts and source errors cannot be left behind.
    """

    signature_status: str
    raw_signature_date: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality


class LiquidationTemporalInput(Protocol):
    """The part of a liquidation assessment this module reads."""

    liquidation_status: str
    completion: str
    completion_event_observation: FieldObservation | None
    raw_liquidation_start_date: FieldObservation
    raw_liquidation_end_date: FieldObservation
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality


def _require_observation(value: object, label: str) -> FieldObservation:
    if not isinstance(value, FieldObservation):
        raise TypeError(f"{label} must be a FieldObservation")
    return value


def _axis_state(
    axis: object, dataset: str
) -> tuple[tuple[Evidence, ...], tuple[AssessmentConflict, ...], tuple[SourceError, ...]]:
    """Read and validate the evidence and quality of a structural axis input."""
    evidence = _typed_tuple(getattr(axis, "evidence", None), Evidence, "axis_evidence")
    quality = getattr(axis, "quality", None)
    if not isinstance(quality, ResolutionQuality):
        raise TypeError("axis quality must be a ResolutionQuality")
    errors = validate_source_errors(
        quality.source_errors, dataset, "axis quality source_errors"
    )
    return evidence, quality.conflicts, errors


# Whether each signature status REQUIRES (True) or FORBIDS (False) an observed
# signature date. ``unknown`` is handled separately: it forbids only a date
# that is also valid, since that would support ``directly_observed``.
_SIGNATURE_DATE_PRESENCE: dict[str, bool] = {
    "directly_observed": True,
    "conflicting": True,
    "inferred_from_post_signature_evidence": False,
    "not_signed": False,
}


def _validate_signature_coherence(
    status: object, observation: FieldObservation, value: TemporalValueResolution
) -> None:
    """Reject a status that its own raw observation cannot support.

    A structural input is not a real type, so this is where a duck-typed stub
    is stopped from claiming an inference AND an observed valid date at once —
    a combination ``resolve_contract_signature`` can never produce.
    """
    observed = observation.presence == "observed"
    required = _SIGNATURE_DATE_PRESENCE.get(status)  # type: ignore[arg-type]
    if required is True and not observed:
        raise ValueError(
            f"signature_status={status!r} requires an observed signature date"
        )
    if required is False and observed:
        raise ValueError(
            f"signature_status={status!r} is incompatible with an observed "
            "signature date"
        )
    if status == "unknown" and observed and value.is_valid:
        raise ValueError(
            "signature_status='unknown' is incompatible with an observed valid "
            "signature date, which would support 'directly_observed'"
        )


def resolve_signature_event(
    signature: SignatureTemporalInput,
    *,
    context: TemporalQueryContext,
    source_timezone: str = SECOP_SOURCE_TIMEZONE,
    dataset: str = CONTRACT_DATASET_ID,
    source_errors: tuple[SourceError, ...] = (),
) -> TemporalAssessment:
    """Evaluate the signature date without laundering the axis it came from.

    ``directly_observed`` is a statement about the PRESENCE of a signature
    date, not about its validity: an unparseable raw scalar yields no temporal
    claim. ``inferred_from_post_signature_evidence`` never authorizes a year.
    ``conflicting`` keeps its date but never becomes a consistent assessment:
    the contract conflict travels with it. The contract start date is never
    used as a fallback — a different field is a different fact.
    """
    validate_source_errors(source_errors, dataset, "source_errors")
    observation = _require_observation(
        getattr(signature, "raw_signature_date", None),
        "signature.raw_signature_date",
    )
    status = getattr(signature, "signature_status", None)
    value = resolve_temporal_value(observation, source_timezone=source_timezone)
    _validate_signature_coherence(status, observation, value)
    axis_evidence, axis_conflicts, axis_errors = _axis_state(signature, dataset)

    reasons: list[str] = []
    if status == "inferred_from_post_signature_evidence":
        reasons.append("SIGNATURE_INFERENCE_CARRIES_NO_DATE")
    elif status == "directly_observed" and not value.is_valid:
        reasons.append("SIGNATURE_DATE_RAW_INVALID")
    if axis_conflicts:
        reasons.append("SIGNATURE_AXIS_CONFLICT_PRESERVED")

    return build_temporal_assessment(
        context=context,
        requested_event="contract_signature",
        applied_field=_FIELD_SIGNATURE_DATE,
        dataset=dataset,
        value=value,
        axis_evidence=axis_evidence,
        axis_conflicts=axis_conflicts,
        axis_source_errors=axis_errors,
        axis_reason_codes=tuple(reasons),
        call_source_errors=source_errors,
    )


def _validate_liquidation_coherence(
    completion: object,
    start: FieldObservation,
    end: FieldObservation,
) -> None:
    """Reject a completion its own raw dates cannot support."""
    started = start.presence == "observed"
    ended = end.presence == "observed"
    rule = {
        "finished": (ended, "requires an observed end date"),
        "started": (
            started and not ended,
            "requires an observed start date and no observed end date",
        ),
        "not_started": (
            not started and not ended,
            "is incompatible with an observed liquidation date",
        ),
        "conflicting": (started or ended, "requires at least one observed date"),
    }.get(completion)  # type: ignore[arg-type]
    if rule is not None and not rule[0]:
        raise ValueError(f"completion={completion!r} {rule[1]}")


def resolve_liquidation_event(
    liquidation: LiquidationTemporalInput,
    *,
    phase: Literal["start", "end"],
    context: TemporalQueryContext,
    source_timezone: str = SECOP_SOURCE_TIMEZONE,
    dataset: str = CONTRACT_DATASET_ID,
    source_errors: tuple[SourceError, ...] = (),
) -> TemporalAssessment:
    """Evaluate a liquidation date, keeping start and end distinct.

    A started liquidation is not a finished one, so ``phase`` is explicit and
    each phase reads its own field. "Liquidated in 2025" is a claim about
    ``fecha_fin_liquidacion``, and only when that raw scalar is a valid date.
    A valid date never upgrades a conflicted axis into a confirmed
    liquidation: the conflict is inherited.
    """
    if phase not in ("start", "end"):
        raise ValueError("phase must be 'start' or 'end'")
    validate_source_errors(source_errors, dataset, "source_errors")

    start = _require_observation(
        getattr(liquidation, "raw_liquidation_start_date", None),
        "liquidation.raw_liquidation_start_date",
    )
    end = _require_observation(
        getattr(liquidation, "raw_liquidation_end_date", None),
        "liquidation.raw_liquidation_end_date",
    )
    _validate_liquidation_coherence(
        getattr(liquidation, "completion", None), start, end
    )
    axis_evidence, axis_conflicts, axis_errors = _axis_state(liquidation, dataset)

    if phase == "end":
        observation, field = end, _FIELD_LIQUIDATION_END
        event: TemporalEvent = "liquidation_end"
        reasons = []
    else:
        observation, field = start, _FIELD_LIQUIDATION_START
        event = "liquidation_start"
        reasons = ["LIQUIDATION_START_IS_NOT_COMPLETION"]
    if axis_conflicts:
        reasons.append("LIQUIDATION_AXIS_CONFLICT_PRESERVED")

    value = resolve_temporal_value(observation, source_timezone=source_timezone)
    return build_temporal_assessment(
        context=context,
        requested_event=event,
        applied_field=field,
        dataset=dataset,
        value=value,
        axis_evidence=axis_evidence,
        axis_conflicts=axis_conflicts,
        axis_source_errors=axis_errors,
        axis_reason_codes=tuple(reasons),
        call_source_errors=source_errors,
    )
