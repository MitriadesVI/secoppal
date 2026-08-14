"""SECOP Truth Layer T1B-4 — temporal assessment composition.

One responsibility: assembling a ``TemporalAssessment`` out of already-solved
pieces — a validated value (``temporal_value``), a relation and a range
membership (``temporal_range``) — plus the Evidence and Quality of T1B-1. No
ISO parser and no DST gap/fold detection live here.

Two rules this module exists to enforce:

  * a temporal claim NEVER launders away the quality of the axis that asked
    for it. If the contract axis found a conflict, the assessment inherits
    it: it cannot be ``consistent``/``high`` on top of a contradiction T1B-3
    preserved. Conflicts are facts, not noise;
  * a ``SourceError`` belongs to the dataset it was raised for; one from
    another dataset cannot qualify this assessment.

Domain axes ask their questions through ``temporal_contract``, which adapts an
already-resolved contract axis into this API; nothing here knows about
signatures or liquidations.

This module decides nothing about offerability, visibility or ranking.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R2.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R3.md
"""
from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from datetime import datetime

from app.core.truth_layer.evidence import (
    AssessmentConflict,
    Evidence,
    EvidenceRole,
    ResolutionQuality,
    SourceError,
    build_resolution_quality,
    validate_evidence_set,
)
from app.core.truth_layer.models import FieldObservation, TemporalQueryContext
from app.core.truth_layer.sentinels import (
    SentinelAssessment,
    assess_sentinel,
    get_field_sentinel_policy,
)
from app.core.truth_layer.temporal_range import (
    VALID_EVENTS,
    VALID_MEMBERSHIP,
    VALID_RELATIONS,
    RangeMembership,
    TemporalEvent,
    TemporalRelation,
    effective_instant,
    resolve_membership,
    resolve_relation,
)
from app.core.truth_layer.temporal_value import (
    SECOP_SOURCE_TIMEZONE,
    TemporalValueResolution,
    resolve_temporal_value,
)

__all__ = [
    "AXIS_REASON_CODES",
    "TemporalAssessment",
    "build_temporal_assessment",
    "validate_source_errors",
    "resolve_temporal_assessment",
]


# Closed vocabulary. A domain axis may only contribute a code that this block
# actually defines: an open list would let any caller staple arbitrary prose
# onto a finished assessment's quality.
AXIS_REASON_CODES: frozenset[str] = frozenset(
    {
        "SIGNATURE_INFERENCE_CARRIES_NO_DATE",
        "SIGNATURE_DATE_RAW_INVALID",
        "SIGNATURE_AXIS_CONFLICT_PRESERVED",
        "LIQUIDATION_AXIS_CONFLICT_PRESERVED",
        "LIQUIDATION_START_IS_NOT_COMPLETION",
    }
)


# ---------------------------------------------------------------------------
# Shared validation.
# ---------------------------------------------------------------------------


def _typed_tuple(items: object, expected: type, label: str) -> tuple:
    if not isinstance(items, tuple):
        raise TypeError(f"{label} must be a tuple")
    for item in items:
        if not isinstance(item, expected):
            raise TypeError(f"{label} entries must be {expected.__name__}")
    return items


def validate_source_errors(
    source_errors: object, dataset: str, label: str
) -> tuple[SourceError, ...]:
    """Reject anything that is not a ``SourceError`` of THIS dataset.

    A failure of another dataset says nothing about this one; letting it
    qualify this assessment would import a doubt never raised here.
    """
    errors = _typed_tuple(source_errors, SourceError, label)
    for error in errors:
        if error.dataset != dataset:
            raise ValueError(
                f"{label} must belong to dataset {dataset!r}; got {error.dataset!r}"
            )
    return errors


def _validate_axis_reason_codes(codes: object) -> tuple[str, ...]:
    if not isinstance(codes, tuple):
        raise TypeError("axis_reason_codes must be a tuple")
    for code in codes:
        if code not in AXIS_REASON_CODES:
            raise ValueError(f"unauthorized axis reason code: {code!r}")
    return codes


# ---------------------------------------------------------------------------
# Evidence and quality.
# ---------------------------------------------------------------------------


def _role_for(value: TemporalValueResolution) -> EvidenceRole:
    presence = value.raw_observation.presence
    if presence == "missing":
        return "missing"
    if presence == "not_observed":
        return "not_observed"
    # An observed scalar that is not a date contradicts the temporal claim
    # instead of supporting it; it is still evidence, never an absence.
    return "supports" if value.is_valid else "contradicts"


def _build_evidence(
    dataset: str, applied_field: str | None, value: TemporalValueResolution
) -> tuple[Evidence, ...]:
    if applied_field is None:
        return ()
    observation = value.raw_observation
    assessment: SentinelAssessment | None = None
    normalized = None
    policy = get_field_sentinel_policy(dataset, applied_field)
    if policy is not None and observation.presence == "observed":
        assessment = assess_sentinel(observation, policy)
        normalized = assessment.normalized_comparison_value
    return (
        Evidence(
            code="TEMPORAL_EVENT_DATE_EVIDENCE",
            dataset=dataset,
            field=applied_field,
            raw_observation=observation,
            sentinel_assessment=assessment,
            normalized_value=normalized,
            role=_role_for(value),
        ),
    )


def _build_quality(
    value: TemporalValueResolution,
    relation: TemporalRelation,
    membership: RangeMembership,
    applied_field: str | None,
    derived_reasons: tuple[str, ...],
    axis_reason_codes: tuple[str, ...],
    axis_conflicts: tuple[AssessmentConflict, ...],
    axis_source_errors: tuple[SourceError, ...],
    call_source_errors: tuple[SourceError, ...],
) -> ResolutionQuality:
    reasons = list(derived_reasons) + list(axis_reason_codes)
    range_unusable = membership == "unknown" and any(
        code.startswith("TEMPORAL_RANGE_") for code in derived_reasons
    )

    if applied_field is None or relation == "unknown" or range_unusable:
        completeness = "insufficient"
        confidence = "low"
    elif membership == "unknown":
        completeness = "partial"
        confidence = "low"
    elif value.has_time_of_day:
        completeness = "complete"
        confidence = "high"
    else:
        # A bare civil date supports a day-level claim, never an hour-level
        # one: the contract requires the reduced confidence to be explicit.
        completeness = "partial"
        confidence = "medium"
        reasons.append("TEMPORAL_DATE_WITHOUT_TIME_OF_DAY")

    source_errors = tuple(axis_source_errors) + tuple(call_source_errors)
    if axis_conflicts:
        # An inherited contradiction outranks everything: a temporal claim
        # cannot be more consistent than the axis that produced it.
        consistency = "conflicting"
        confidence = "low"
    elif source_errors:
        consistency = "unknown"
        confidence = "low"
    else:
        consistency = "consistent"

    return build_resolution_quality(
        completeness=completeness,  # type: ignore[arg-type]
        consistency=consistency,  # type: ignore[arg-type]
        confidence=confidence,  # type: ignore[arg-type]
        reason_codes=tuple(reasons),
        missing_fields=(
            (applied_field,)
            if applied_field is not None
            and value.raw_observation.presence == "missing"
            else ()
        ),
        source_errors=source_errors,
        conflicts=axis_conflicts,
    )


@dataclass(frozen=True, slots=True)
class _TemporalDerivation:
    """Everything a ``TemporalAssessment`` must be able to recompute."""

    context: TemporalQueryContext
    requested_event: str
    applied_field: str | None
    dataset: str
    value: TemporalValueResolution
    axis_reason_codes: tuple[str, ...]
    axis_conflicts: tuple[AssessmentConflict, ...]
    axis_source_errors: tuple[SourceError, ...]
    call_source_errors: tuple[SourceError, ...]
    relation: TemporalRelation
    membership: RangeMembership
    range_start: str | None
    range_end_exclusive: str | None
    effective: datetime | None
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality


def _derive(
    context: TemporalQueryContext,
    requested_event: str,
    applied_field: str | None,
    dataset: str,
    value: TemporalValueResolution,
    axis_reason_codes: tuple[str, ...],
    axis_conflicts: tuple[AssessmentConflict, ...],
    axis_source_errors: tuple[SourceError, ...],
    call_source_errors: tuple[SourceError, ...],
) -> _TemporalDerivation:
    relation, relation_reasons = resolve_relation(
        value, context, requested_event, applied_field
    )
    membership, bounds, range_reasons = resolve_membership(
        value, context, requested_event, applied_field
    )
    return _TemporalDerivation(
        context=context,
        requested_event=requested_event,
        applied_field=applied_field,
        dataset=dataset,
        value=value,
        axis_reason_codes=axis_reason_codes,
        axis_conflicts=axis_conflicts,
        axis_source_errors=axis_source_errors,
        call_source_errors=call_source_errors,
        relation=relation,
        membership=membership,
        range_start=bounds.range_start,
        range_end_exclusive=bounds.range_end_exclusive,
        effective=effective_instant(value, requested_event),
        evidence=_build_evidence(dataset, applied_field, value),
        quality=_build_quality(
            value,
            relation,
            membership,
            applied_field,
            relation_reasons + range_reasons,
            axis_reason_codes,
            axis_conflicts,
            axis_source_errors,
            call_source_errors,
        ),
    )


# ---------------------------------------------------------------------------
# TemporalAssessment.
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TemporalAssessment:
    """One temporal question asked of one observation, with its answer.

    Carries the frozen contract fields (SEMANTIC_CONTRACT section 8) plus the
    validity resolution, the range membership and the inherited axis quality,
    which the contract's rules require but its field list predates.

    ``observed_value`` is the raw scalar only when textual; the complete raw
    observation stays reachable through ``value.raw_observation``. The
    ``axis_*`` fields carry the state of the domain axis that asked: its
    evidence stays auditable here, and its conflicts and source errors are
    re-derived into ``quality`` so neither can be dropped nor invented.

    Every conclusion is recomputed in ``__post_init__``, so no public
    constructor — ``dataclasses.replace`` included — can mint an assessment
    whose inputs contradict it.
    """

    context: TemporalQueryContext
    requested_event: TemporalEvent
    applied_field: str | None
    dataset: str
    value: TemporalValueResolution
    axis_evidence: tuple[Evidence, ...]
    axis_conflicts: tuple[AssessmentConflict, ...]
    axis_source_errors: tuple[SourceError, ...]
    axis_reason_codes: tuple[str, ...]
    call_source_errors: tuple[SourceError, ...]
    range_start: str | None
    range_end_exclusive: str | None
    effective_instant: datetime | None
    relation_to_reference: TemporalRelation
    range_membership: RangeMembership
    evidence: tuple[Evidence, ...]
    quality: ResolutionQuality
    _resolved_derivation: InitVar[_TemporalDerivation | None] = field(
        default=None, kw_only=True
    )

    # The contract's carried context/value data (SEMANTIC_CONTRACT section 8)
    # are properties, not stored copies: a datum that is derived on access can
    # not be falsified at all, which is a stronger guarantee than validating a
    # stored copy after the fact.
    @property
    def reference_instant(self) -> datetime:
        return self.context.reference_instant

    @property
    def reference_timezone(self) -> str:
        return self.context.reference_timezone

    @property
    def local_reference(self) -> datetime:
        return self.context.local_reference

    @property
    def range_timezone(self) -> str:
        return self.context.range_timezone

    @property
    def source_timezone(self) -> str:
        return self.value.source_timezone

    @property
    def observed_value(self) -> str | None:
        """The raw scalar, only when it is textual."""
        raw = self.value.raw_observation.raw_value
        return raw if isinstance(raw, str) else None

    def __post_init__(
        self, _resolved_derivation: _TemporalDerivation | None
    ) -> None:
        if not isinstance(self.context, TemporalQueryContext):
            raise TypeError("context must be a TemporalQueryContext")
        if not isinstance(self.value, TemporalValueResolution):
            raise TypeError("value must be a TemporalValueResolution")
        for label, value, vocabulary in (
            ("TemporalEvent", self.requested_event, VALID_EVENTS),
            ("TemporalRelation", self.relation_to_reference, VALID_RELATIONS),
            ("RangeMembership", self.range_membership, VALID_MEMBERSHIP),
        ):
            if value not in vocabulary:
                raise ValueError(f"invalid {label}: {value!r}")
        if self.applied_field is not None and (
            not isinstance(self.applied_field, str) or not self.applied_field
        ):
            raise ValueError("applied_field must be a non-empty string or None")
        if not isinstance(self.dataset, str) or not self.dataset:
            raise ValueError("dataset must be a non-empty string")

        _typed_tuple(self.axis_evidence, Evidence, "axis_evidence")
        _typed_tuple(self.axis_conflicts, AssessmentConflict, "axis_conflicts")
        _validate_axis_reason_codes(self.axis_reason_codes)
        validate_source_errors(
            self.axis_source_errors, self.dataset, "axis_source_errors"
        )
        validate_source_errors(
            self.call_source_errors, self.dataset, "call_source_errors"
        )
        if not isinstance(self.quality, ResolutionQuality):
            raise TypeError("quality must be a ResolutionQuality")

        if _resolved_derivation is None:
            validate_evidence_set(self.evidence)
            derived = _derive(
                self.context,
                self.requested_event,
                self.applied_field,
                self.dataset,
                self.value,
                self.axis_reason_codes,
                self.axis_conflicts,
                self.axis_source_errors,
                self.call_source_errors,
            )
        else:
            if type(_resolved_derivation) is not _TemporalDerivation:
                raise TypeError(
                    "_resolved_derivation must be an internal temporal derivation"
                )
            derived = _resolved_derivation
            for label, mine, theirs in (
                ("context", self.context, derived.context),
                ("value", self.value, derived.value),
                ("axis_reason_codes", self.axis_reason_codes, derived.axis_reason_codes),
                ("axis_conflicts", self.axis_conflicts, derived.axis_conflicts),
                ("axis_source_errors", self.axis_source_errors, derived.axis_source_errors),
                ("call_source_errors", self.call_source_errors, derived.call_source_errors),
            ):
                if mine is not theirs:
                    raise ValueError(
                        f"resolved temporal derivation belongs to different {label}"
                    )
            for label, mine, theirs in (
                ("requested_event", self.requested_event, derived.requested_event),
                ("applied_field", self.applied_field, derived.applied_field),
                ("dataset", self.dataset, derived.dataset),
            ):
                if type(mine) is not type(theirs) or mine != theirs:
                    raise ValueError(
                        f"resolved temporal derivation has different {label}"
                    )
            if (
                self.evidence is not derived.evidence
                or self.quality is not derived.quality
            ):
                raise ValueError(
                    "resolved temporal derivation belongs to different evidence or quality"
                )
        # Inherited conflicts and source errors are part of the derivation, so
        # a quality that dropped or invented one cannot survive this check.
        for label, mine, theirs in (
            ("relation_to_reference", self.relation_to_reference, derived.relation),
            ("range_membership", self.range_membership, derived.membership),
            ("range_start", self.range_start, derived.range_start),
            (
                "range_end_exclusive",
                self.range_end_exclusive,
                derived.range_end_exclusive,
            ),
            ("effective_instant", self.effective_instant, derived.effective),
            ("evidence", self.evidence, derived.evidence),
            ("quality", self.quality, derived.quality),
        ):
            if type(mine) is not type(theirs) or mine != theirs:
                raise ValueError(
                    f"{label} contradicts its inputs, which yield {theirs!r}"
                )


def build_temporal_assessment(
    *,
    context: TemporalQueryContext,
    requested_event: TemporalEvent,
    applied_field: str | None,
    dataset: str,
    value: TemporalValueResolution,
    axis_evidence: tuple[Evidence, ...] = (),
    axis_conflicts: tuple[AssessmentConflict, ...] = (),
    axis_source_errors: tuple[SourceError, ...] = (),
    axis_reason_codes: tuple[str, ...] = (),
    call_source_errors: tuple[SourceError, ...] = (),
) -> TemporalAssessment:
    if not isinstance(context, TemporalQueryContext):
        raise TypeError("context must be a TemporalQueryContext")
    derived = _derive(
        context,
        requested_event,
        applied_field,
        dataset,
        value,
        axis_reason_codes,
        axis_conflicts,
        axis_source_errors,
        call_source_errors,
    )
    return TemporalAssessment(
        context=context,
        requested_event=requested_event,
        applied_field=applied_field,
        dataset=dataset,
        value=value,
        axis_evidence=axis_evidence,
        axis_conflicts=axis_conflicts,
        axis_source_errors=axis_source_errors,
        axis_reason_codes=axis_reason_codes,
        call_source_errors=call_source_errors,
        range_start=derived.range_start,
        range_end_exclusive=derived.range_end_exclusive,
        effective_instant=derived.effective,
        relation_to_reference=derived.relation,
        range_membership=derived.membership,
        evidence=derived.evidence,
        quality=derived.quality,
        _resolved_derivation=derived,
    )


def resolve_temporal_assessment(
    observation: FieldObservation,
    *,
    context: TemporalQueryContext,
    requested_event: TemporalEvent,
    applied_field: str | None,
    dataset: str,
    source_timezone: str = SECOP_SOURCE_TIMEZONE,
    source_errors: tuple[SourceError, ...] = (),
) -> TemporalAssessment:
    """Evaluate one temporal observation against an already-resolved instant."""
    validate_source_errors(source_errors, dataset, "source_errors")
    value = resolve_temporal_value(observation, source_timezone=source_timezone)
    return build_temporal_assessment(
        context=context,
        requested_event=requested_event,
        applied_field=applied_field,
        dataset=dataset,
        value=value,
        call_source_errors=source_errors,
    )
