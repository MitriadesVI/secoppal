"""T1B-0C/T1B-4 — temporal validity, relations and the wall-time boundary."""
from __future__ import annotations

import ast
import dataclasses
import inspect
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import app.core.truth_layer.local_wall_time as local_wall_time_mod
import app.core.truth_layer.temporal_contract as contract_mod
import app.core.truth_layer.temporal_range as range_mod
import app.core.truth_layer.temporal_resolver as temporal_mod
import app.core.truth_layer.temporal_value as value_mod
from app.core.truth_layer import (
    ContractLifecycleSignals,
    FieldObservation,
    LocalWallTimeResolver,
    SourceError,
    TemporalAssessment,
    TemporalQueryContext,
    TemporalValueResolution,
    build_resolution_quality,
    resolve_contract_liquidation,
    resolve_contract_signature,
    resolve_liquidation_event,
    resolve_signature_event,
    resolve_temporal_assessment,
    resolve_temporal_value,
)


ROOT = Path(__file__).resolve().parents[1]

BOGOTA = ZoneInfo("America/Bogota")
CONTRACTS = "jbjy-vk9h"
PROCESSES = "p6dx-8zbt"


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def _context(
    *,
    instant: datetime | None = None,
    zone: str = "America/Bogota",
    start: str | None = None,
    end: str | None = None,
    event: str | None = None,
) -> TemporalQueryContext:
    return TemporalQueryContext(
        reference_instant=instant or datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc),
        reference_timezone=zone,
        requested_range_start=start,
        requested_range_end=end,
        requested_event_raw=event,
    )


def _contract_signals(
    *,
    state: object = "En ejecución",
    signature: object = "2025-03-14T00:00:00.000",
    start: object = "2025-03-20T00:00:00.000",
    end: object = "2025-12-31T00:00:00.000",
    flag: object = "Si",
    liquidation_start: object = "2025-06-01T00:00:00.000",
    liquidation_end: object = "2025-06-30T00:00:00.000",
) -> ContractLifecycleSignals:
    def field(value: object) -> FieldObservation:
        if value is None:
            return _missing()
        if value is Ellipsis:
            return _not_observed()
        return _obs(value)

    return ContractLifecycleSignals(
        raw_contract_state=field(state),
        raw_signature_date=field(signature),
        raw_start_date=field(start),
        raw_end_date=field(end),
        raw_liquidation_flag=field(flag),
        raw_liquidation_start_date=field(liquidation_start),
        raw_liquidation_end_date=field(liquidation_end),
    )


# ---------------------------------------------------------------------------
# Date validation: presence is not validity.
# ---------------------------------------------------------------------------


def test_secop_iso_without_offset_is_read_in_the_declared_source_zone() -> None:
    value = resolve_temporal_value(_obs("2025-03-14T10:30:00.000"))

    assert value.status == "valid"
    assert value.has_time_of_day is True
    assert value.parsed_instant == datetime(2025, 3, 14, 10, 30, tzinfo=BOGOTA)
    assert value.parsed_instant.utcoffset() == timedelta(hours=-5)
    # The naive scalar was NOT assumed to be UTC.
    assert value.parsed_instant != datetime(2025, 3, 14, 10, 30, tzinfo=timezone.utc)
    assert value.reason_codes == ("TEMPORAL_VALUE_NAIVE_READ_IN_SOURCE_ZONE",)


def test_source_timezone_is_an_explicit_parameter_not_a_hidden_assumption() -> None:
    in_utc = resolve_temporal_value(
        _obs("2025-03-14T10:30:00"), source_timezone="UTC"
    )

    assert in_utc.parsed_instant == datetime(2025, 3, 14, 10, 30, tzinfo=ZoneInfo("UTC"))


def test_iso_with_z_keeps_its_absolute_instant() -> None:
    value = resolve_temporal_value(_obs("2025-03-14T10:30:00Z"))

    assert value.status == "valid"
    assert value.parsed_instant == datetime(2025, 3, 14, 10, 30, tzinfo=timezone.utc)
    assert value.reason_codes == ("TEMPORAL_VALUE_EXPLICIT_OFFSET",)


def test_iso_with_explicit_offset_keeps_its_absolute_instant() -> None:
    value = resolve_temporal_value(_obs("2025-03-14T10:30:00-08:00"))

    assert value.parsed_instant == datetime(
        2025, 3, 14, 10, 30, tzinfo=timezone(timedelta(hours=-8))
    )
    assert value.parsed_instant.utcoffset() == timedelta(hours=-8)


def test_civil_date_has_no_time_of_day() -> None:
    value = resolve_temporal_value(_obs("2025-03-14"))

    assert value.status == "valid"
    assert value.has_time_of_day is False
    assert value.civil_date.isoformat() == "2025-03-14"


@pytest.mark.parametrize("raw", ["", "   ", "\t\n", "pendiente", "2025-02-30", "14/03/2025"])
def test_unusable_observed_text_is_invalid_format_never_absence(raw: str) -> None:
    value = resolve_temporal_value(_obs(raw))

    assert value.status == "invalid_format"
    assert value.parsed_instant is None
    # The source DID answer; the answer is unusable. That is not `missing`.
    assert value.raw_observation.presence == "observed"
    assert value.raw_observation.raw_value == raw


@pytest.mark.parametrize("raw", [True, False, 0, 1, 1741000000, 4.25])
def test_non_string_scalars_are_not_dates(raw: object) -> None:
    value = resolve_temporal_value(_obs(raw))

    assert value.status == "invalid_type"
    assert value.parsed_instant is None
    assert value.raw_observation.raw_value is raw or value.raw_observation.raw_value == raw


def test_missing_stays_missing_and_not_observed_stays_not_observed() -> None:
    assert resolve_temporal_value(_missing()).status == "missing"
    assert resolve_temporal_value(_not_observed()).status == "not_observed"


def test_valid_date_preserves_the_raw_observation_by_identity() -> None:
    observation = _obs("2025-03-14T10:30:00.000")
    value = resolve_temporal_value(observation)

    assert value.raw_observation is observation
    assert value.raw_observation.raw_value == "2025-03-14T10:30:00.000"
    assert value.parsed_instant is not observation.raw_value


def test_nonexistent_source_wall_time_in_a_dst_zone_is_not_silently_shifted() -> None:
    value = resolve_temporal_value(
        _obs("2026-03-08T02:30:00"), source_timezone="America/New_York"
    )

    assert value.status == "nonexistent_local_time"
    assert value.parsed_instant is None


def test_ambiguous_source_wall_time_in_a_dst_zone_picks_no_fold() -> None:
    value = resolve_temporal_value(
        _obs("2026-11-01T01:30:00"), source_timezone="America/New_York"
    )

    assert value.status == "ambiguous_local_time"
    assert value.parsed_instant is None


def test_the_same_wall_time_is_unambiguous_in_bogota() -> None:
    value = resolve_temporal_value(
        _obs("2026-11-01T01:30:00"), source_timezone="America/Bogota"
    )

    assert value.status == "valid"


# ---------------------------------------------------------------------------
# Local wall time boundary.
# ---------------------------------------------------------------------------


def test_local_wall_time_resolver_detects_a_nonexistent_hour() -> None:
    resolution = LocalWallTimeResolver().resolve(
        "2026-03-08T02:30:00", "America/New_York"
    )

    assert resolution.status == "nonexistent"
    assert resolution.instant is None
    assert resolution.reason_codes == ("LOCAL_WALL_TIME_NONEXISTENT",)


def test_local_wall_time_resolver_requires_a_fold_when_ambiguous() -> None:
    resolution = LocalWallTimeResolver().resolve(
        "2026-11-01T01:30:00", "America/New_York"
    )

    assert resolution.status == "ambiguous"
    assert resolution.instant is None
    assert "LOCAL_WALL_TIME_FOLD_REQUIRED" in resolution.reason_codes


def test_explicit_folds_produce_two_different_instants() -> None:
    resolver = LocalWallTimeResolver()
    first = resolver.resolve("2026-11-01T01:30:00", "America/New_York", fold=0)
    second = resolver.resolve("2026-11-01T01:30:00", "America/New_York", fold=1)

    assert first.status == second.status == "resolved"
    assert first.fold == 0 and second.fold == 1
    # Same wall time, two different instants. Intra-zone `==` ignores fold by
    # design (PEP 495), so the comparison must be made in absolute terms.
    first_utc = first.instant.astimezone(timezone.utc)
    second_utc = second.instant.astimezone(timezone.utc)
    assert first_utc != second_utc
    assert second_utc - first_utc == timedelta(hours=1)


def test_local_wall_time_with_an_explicit_offset_needs_clarification() -> None:
    resolution = LocalWallTimeResolver().resolve(
        "2026-11-01T01:30:00-04:00", "America/New_York"
    )

    assert resolution.status == "clarification_required"
    assert resolution.instant is None


def test_unparsable_local_wall_time_needs_clarification() -> None:
    resolution = LocalWallTimeResolver().resolve("mañana a las 3", "America/Bogota")

    assert resolution.status == "clarification_required"
    assert resolution.instant is None


def test_local_wall_time_resolver_rejects_an_invalid_zone() -> None:
    with pytest.raises(ValueError, match="IANA"):
        LocalWallTimeResolver().resolve("2026-07-29T10:00:00", "Mars/Olympus_Mons")


# ---------------------------------------------------------------------------
# Relations and ranges.
# ---------------------------------------------------------------------------


def test_past_present_and_future_are_resolved_against_the_reference() -> None:
    context = _context(instant=datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc))

    def relation(raw: str) -> str:
        return resolve_temporal_assessment(
            _obs(raw),
            context=context,
            requested_event="process_publication",
            applied_field="fecha_de_publicacion_del",
            dataset=PROCESSES,
        ).relation_to_reference

    # 14:00Z is 09:00 in Bogotá on 2026-07-29.
    assert relation("2026-07-28") == "past"
    assert relation("2026-07-29") == "today"
    assert relation("2026-07-30") == "future"
    assert relation("2026-07-29T08:00:00") == "past"
    assert relation("2026-07-29T10:00:00") == "future"


def test_an_unusable_scalar_yields_an_unknown_relation() -> None:
    assessment = resolve_temporal_assessment(
        _obs("pendiente"),
        context=_context(),
        requested_event="process_publication",
        applied_field="fecha_de_publicacion_del",
        dataset=PROCESSES,
    )

    assert assessment.value.status == "invalid_format"
    assert assessment.relation_to_reference == "unknown"
    assert assessment.effective_instant is None
    assert assessment.quality.completeness == "insufficient"


def test_an_open_state_alone_supports_no_temporal_claim() -> None:
    """``estado_de_apertura_del_proceso="Abierto"`` is not a date."""
    assessment = resolve_temporal_assessment(
        _obs("Abierto"),
        context=_context(),
        requested_event="offer_deadline",
        applied_field="estado_de_apertura_del_proceso",
        dataset=PROCESSES,
    )

    assert assessment.relation_to_reference == "unknown"


def test_a_deadline_given_as_a_bare_date_runs_to_the_end_of_that_day() -> None:
    context = _context(instant=datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc))

    assessment = resolve_temporal_assessment(
        _obs("2026-07-29"),
        context=context,
        requested_event="offer_deadline",
        applied_field="fecha_de_recepcion_de",
        dataset=PROCESSES,
    )

    assert assessment.effective_instant == datetime(2026, 7, 30, tzinfo=BOGOTA)
    assert assessment.relation_to_reference == "future"
    assert "TEMPORAL_DATE_WITHOUT_TIME_OF_DAY" in assessment.quality.reason_codes
    assert assessment.quality.confidence == "medium"


def test_a_civil_day_range_includes_its_final_day() -> None:
    context = _context(start="2025-01-01", end="2025-12-31")

    inside = resolve_temporal_assessment(
        _obs("2025-12-31"),
        context=context,
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )
    outside = resolve_temporal_assessment(
        _obs("2026-01-01"),
        context=context,
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert inside.range_membership == "inside"
    assert inside.range_end_exclusive == "2026-01-01"
    assert outside.range_membership == "outside"


def test_a_range_with_a_time_of_day_is_half_open() -> None:
    context = _context(
        start="2025-01-01T00:00:00", end="2025-01-02T00:00:00"
    )

    boundary = resolve_temporal_assessment(
        _obs("2025-01-02T00:00:00"),
        context=context,
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert boundary.range_membership == "outside"
    assert boundary.range_end_exclusive == "2025-01-02T00:00:00"


def test_an_invalid_range_bound_yields_unknown_membership_not_a_false_negative() -> None:
    assessment = resolve_temporal_assessment(
        _obs("2025-06-01"),
        context=_context(start="ayer", end="2025-12-31"),
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert assessment.range_membership == "unknown"
    assert "TEMPORAL_RANGE_START_INVALID" in assessment.quality.reason_codes


def test_no_requested_range_is_not_an_empty_range() -> None:
    assessment = resolve_temporal_assessment(
        _obs("2025-06-01"),
        context=_context(),
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert assessment.range_membership == "not_requested"
    assert assessment.range_start is None
    assert assessment.range_end_exclusive is None


def test_no_applied_field_yields_a_not_applicable_relation() -> None:
    assessment = resolve_temporal_assessment(
        _not_observed(),
        context=_context(),
        requested_event="unspecified",
        applied_field=None,
        dataset=PROCESSES,
    )

    assert assessment.relation_to_reference == "not_applicable"
    assert assessment.evidence == ()


def test_the_requested_event_is_separate_from_the_current_state() -> None:
    context = _context()
    publication = resolve_temporal_assessment(
        _obs("2025-01-05"),
        context=context,
        requested_event="process_publication",
        applied_field="fecha_de_publicacion_del",
        dataset=PROCESSES,
    )
    adjudication = resolve_temporal_assessment(
        _obs("2025-01-05"),
        context=context,
        requested_event="adjudication",
        applied_field="fecha_adjudicacion",
        dataset=PROCESSES,
    )

    assert publication.requested_event != adjudication.requested_event
    assert publication.applied_field != adjudication.applied_field
    assert publication.relation_to_reference == adjudication.relation_to_reference


def test_local_reference_is_carried_and_the_instant_is_untouched() -> None:
    instant = datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc)
    context = _context(instant=instant)

    assessment = resolve_temporal_assessment(
        _obs("2025-01-05"),
        context=context,
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert assessment.reference_instant is instant
    assert assessment.local_reference == datetime(2026, 7, 29, 9, 0, tzinfo=BOGOTA)


# ---------------------------------------------------------------------------
# Contract integration (T1B-3 consumed, never modified).
# ---------------------------------------------------------------------------


def test_a_signature_signed_in_2025_answers_the_2025_cohort() -> None:
    signature = resolve_contract_signature(
        _contract_signals(signature="2025-03-14T00:00:00.000")
    )

    assessment = resolve_signature_event(
        signature, context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert signature.signature_status == "directly_observed"
    assert assessment.value.status == "valid"
    assert assessment.range_membership == "inside"
    assert assessment.applied_field == "fecha_de_firma"


def test_an_inferred_signature_carries_no_year() -> None:
    signals = _contract_signals(state="terminado", signature=None)
    signature = resolve_contract_signature(signals)

    assessment = resolve_signature_event(
        signature, context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert signature.signature_status == "inferred_from_post_signature_evidence"
    assert assessment.value.status == "missing"
    assert assessment.relation_to_reference == "unknown"
    assert assessment.range_membership == "unknown"
    assert assessment.effective_instant is None
    assert "SIGNATURE_INFERENCE_CARRIES_NO_DATE" in assessment.quality.reason_codes


def test_directly_observed_is_not_enough_when_the_raw_date_is_invalid() -> None:
    signature = resolve_contract_signature(_contract_signals(signature="N/A"))

    assessment = resolve_signature_event(
        signature, context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert signature.signature_status == "directly_observed"
    assert assessment.value.status == "invalid_format"
    assert assessment.relation_to_reference == "unknown"
    assert assessment.range_membership == "unknown"
    assert "SIGNATURE_DATE_RAW_INVALID" in assessment.quality.reason_codes


def test_the_contract_start_date_is_never_a_signature_fallback() -> None:
    signals = _contract_signals(signature=None, start="2025-01-05T00:00:00.000")
    signature = resolve_contract_signature(signals)

    assessment = resolve_signature_event(signature, context=_context())

    assert assessment.applied_field == "fecha_de_firma"
    assert assessment.value.raw_observation is signals.raw_signature_date
    assert assessment.observed_value is None
    assert assessment.effective_instant is None


def test_a_finished_liquidation_answers_the_liquidation_year() -> None:
    liquidation = resolve_contract_liquidation(
        _contract_signals(flag="Si", liquidation_end="2025-06-30T00:00:00.000")
    )

    assessment = resolve_liquidation_event(
        liquidation, phase="end", context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert liquidation.liquidation_status == "yes"
    assert liquidation.completion == "finished"
    assert assessment.requested_event == "liquidation_end"
    assert assessment.range_membership == "inside"


def test_a_started_liquidation_is_not_a_finished_one() -> None:
    signals = _contract_signals(
        flag="Si",
        liquidation_start="2025-06-01T00:00:00.000",
        liquidation_end=None,
    )
    liquidation = resolve_contract_liquidation(signals)

    started = resolve_liquidation_event(
        liquidation, phase="start", context=_context(start="2025-01-01", end="2025-12-31")
    )
    finished = resolve_liquidation_event(
        liquidation, phase="end", context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert liquidation.completion == "started"
    assert started.requested_event == "liquidation_start"
    assert started.value.status == "valid"
    assert started.range_membership == "inside"
    assert finished.requested_event == "liquidation_end"
    assert finished.value.status == "missing"
    assert finished.range_membership == "unknown"


def test_an_invalid_liquidation_end_date_is_not_a_temporal_event() -> None:
    signals = _contract_signals(flag="Si", liquidation_end="No aplica")
    liquidation = resolve_contract_liquidation(signals)

    assessment = resolve_liquidation_event(
        liquidation, phase="end", context=_context(start="2025-01-01", end="2025-12-31")
    )

    # T1B-3 sees an observed scalar and reports completion; T1B-4 refuses to
    # turn that scalar into a date.
    assert liquidation.completion_event_observation is signals.raw_liquidation_end_date
    assert assessment.value.status == "invalid_format"
    assert assessment.relation_to_reference == "unknown"
    assert assessment.range_membership == "unknown"


def test_a_liquidation_end_without_start_still_dates_the_end() -> None:
    signals = _contract_signals(
        flag=None,
        liquidation_start=None,
        liquidation_end="2025-06-30T00:00:00.000",
    )
    liquidation = resolve_contract_liquidation(signals)

    assessment = resolve_liquidation_event(
        liquidation, phase="end", context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert liquidation.quality.conflicts  # sequence conflict preserved by T1B-3
    assert assessment.value.status == "valid"
    assert assessment.range_membership == "inside"


def test_contract_integration_does_not_modify_the_t1b3_assessments() -> None:
    signature = resolve_contract_signature(_contract_signals())
    liquidation = resolve_contract_liquidation(_contract_signals())
    before = (signature, liquidation)

    resolve_signature_event(signature, context=_context())
    resolve_liquidation_event(liquidation, phase="end", context=_context())

    assert (signature, liquidation) == before
    assert signature.signature_status == "directly_observed"


# ---------------------------------------------------------------------------
# Public invariants and mutation probes.
# ---------------------------------------------------------------------------


def _sample_assessment() -> TemporalAssessment:
    return resolve_temporal_assessment(
        _obs("2025-03-14T10:30:00.000"),
        context=_context(start="2025-01-01", end="2025-12-31"),
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )


def test_public_types_are_frozen_slotted_and_hashable() -> None:
    assessment = _sample_assessment()
    for model in (TemporalAssessment, TemporalValueResolution):
        assert model.__dataclass_params__.frozen is True
        assert "__slots__" in model.__dict__
        assert "__dict__" not in model.__dict__
    assert isinstance(hash(assessment), int)
    assert isinstance(hash(assessment.value), int)
    assert assessment == _sample_assessment()


def test_replace_cannot_rewrite_the_relation() -> None:
    assessment = _sample_assessment()

    with pytest.raises(ValueError, match="relation_to_reference"):
        dataclasses.replace(assessment, relation_to_reference="future")


def test_replace_cannot_rewrite_the_range_membership() -> None:
    assessment = _sample_assessment()

    with pytest.raises(ValueError, match="range_membership"):
        dataclasses.replace(assessment, range_membership="outside")


def test_replace_cannot_invent_a_parsed_value() -> None:
    value = resolve_temporal_value(_obs("pendiente"))

    with pytest.raises(ValueError, match="parsed_instant"):
        dataclasses.replace(
            value, parsed_instant=datetime(2025, 1, 1, tzinfo=timezone.utc)
        )


def test_replace_cannot_rewrite_a_value_status() -> None:
    value = resolve_temporal_value(_obs(True))

    with pytest.raises(ValueError, match="contradicts"):
        dataclasses.replace(value, status="valid")


def test_evidence_from_a_different_observation_is_rejected() -> None:
    assessment = _sample_assessment()
    other = resolve_temporal_assessment(
        _obs("2024-01-01T10:30:00.000"),
        context=assessment.context,
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    with pytest.raises(ValueError, match="evidence"):
        dataclasses.replace(assessment, evidence=other.evidence)


def test_quality_cannot_drop_a_source_error() -> None:
    error = SourceError(
        code="CONTRACT_LOOKUP_TIMEOUT", dataset=CONTRACTS, kind="timeout"
    )
    assessment = resolve_temporal_assessment(
        _not_observed(),
        context=_context(),
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
        source_errors=(error,),
    )

    assert assessment.quality.source_errors == (error,)
    stripped = build_resolution_quality(
        completeness=assessment.quality.completeness,
        consistency=assessment.quality.consistency,
        confidence=assessment.quality.confidence,
        reason_codes=assessment.quality.reason_codes,
        missing_fields=assessment.quality.missing_fields,
        source_errors=(),
    )
    with pytest.raises(ValueError, match="quality"):
        dataclasses.replace(assessment, quality=stripped)


def test_quality_cannot_gain_a_conflict_that_was_never_derived() -> None:
    assessment = _sample_assessment()
    inflated = build_resolution_quality(
        completeness=assessment.quality.completeness,
        consistency="conflicting",
        confidence=assessment.quality.confidence,
        reason_codes=assessment.quality.reason_codes,
    )

    with pytest.raises(ValueError, match="quality"):
        dataclasses.replace(assessment, quality=inflated)


def test_a_context_swap_that_changes_the_answer_is_rejected() -> None:
    assessment = _sample_assessment()

    with pytest.raises(ValueError, match="contradicts its inputs"):
        dataclasses.replace(assessment, context=_context(start=None, end=None))


def test_no_confidence_scores_are_produced() -> None:
    assessment = _sample_assessment()

    assert assessment.quality.confidence in {"high", "medium", "low"}
    for field in dataclasses.fields(assessment):
        value = getattr(assessment, field.name)
        assert not isinstance(value, float)


# ---------------------------------------------------------------------------
# Final rework probes — the four blockers found in independent review.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _StubSignature:
    """A duck-typed signature input, used to probe the protocol boundary."""

    signature_status: str
    raw_signature_date: FieldObservation
    evidence: tuple = ()
    quality: object = None


def _stub_quality() -> object:
    return build_resolution_quality(
        completeness="partial", consistency="consistent", confidence="medium"
    )


def test_an_inferred_status_cannot_carry_an_observed_valid_date() -> None:
    """Unreachable from T1B-3, therefore checked at the structural boundary.

    ``resolve_contract_signature`` never emits ``inferred`` with an observed
    date. A duck-typed stub can, and it must be rejected rather than allowed
    to place an inference inside an annual cohort.
    """
    stub = _StubSignature(
        "inferred_from_post_signature_evidence",
        _obs("2025-03-14T00:00:00.000"),
        (),
        _stub_quality(),
    )

    with pytest.raises(ValueError, match="incompatible with an observed"):
        resolve_signature_event(stub, context=_context(start="2025-01-01", end="2025-12-31"))


def test_not_signed_cannot_carry_an_observed_valid_date() -> None:
    stub = _StubSignature(
        "not_signed", _obs("2025-03-14T00:00:00.000"), (), _stub_quality()
    )

    with pytest.raises(ValueError, match="incompatible with an observed"):
        resolve_signature_event(stub, context=_context())


def test_directly_observed_requires_an_observed_date() -> None:
    stub = _StubSignature("directly_observed", _missing(), (), _stub_quality())

    with pytest.raises(ValueError, match="requires an observed"):
        resolve_signature_event(stub, context=_context())


def test_a_conflicting_signature_never_becomes_a_consistent_assessment() -> None:
    """The blocker: a preserved contradiction was being laundered away.

    ``Borrador`` plus an observed signature date is a real T1B-3 conflict. The
    temporal claim may keep its date, but it may not report itself as
    consistent and confident on top of that contradiction.
    """
    signals = _contract_signals(state="Borrador", signature="2025-03-14T00:00:00.000")
    signature = resolve_contract_signature(signals)
    assert signature.signature_status == "conflicting"
    assert len(signature.quality.conflicts) == 1

    assessment = resolve_signature_event(
        signature, context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert assessment.range_membership == "inside"
    assert assessment.quality.consistency == "conflicting"
    assert assessment.quality.confidence == "low"
    assert assessment.quality.conflicts == signature.quality.conflicts
    assert "SIGNATURE_AXIS_CONFLICT_PRESERVED" in assessment.quality.reason_codes
    # The contract evidence stays auditable from the temporal assessment.
    assert assessment.axis_evidence == signature.evidence


def test_a_conflicted_liquidation_never_becomes_a_confirmed_one() -> None:
    signals = _contract_signals(
        flag="No",
        liquidation_start=None,
        liquidation_end="2025-06-30T00:00:00.000",
    )
    liquidation = resolve_contract_liquidation(signals)
    assert liquidation.liquidation_status == "conflicting"
    assert liquidation.quality.conflicts

    assessment = resolve_liquidation_event(
        liquidation, phase="end", context=_context(start="2025-01-01", end="2025-12-31")
    )

    assert assessment.value.status == "valid"
    assert assessment.range_membership == "inside"
    assert assessment.quality.consistency == "conflicting"
    assert assessment.quality.confidence == "low"
    assert assessment.quality.conflicts == liquidation.quality.conflicts
    assert "LIQUIDATION_AXIS_CONFLICT_PRESERVED" in assessment.quality.reason_codes


def test_a_liquidation_completion_incompatible_with_its_dates_is_rejected() -> None:
    @dataclass(frozen=True)
    class StubLiquidation:
        liquidation_status: str
        completion: str
        completion_event_observation: FieldObservation | None
        raw_liquidation_start_date: FieldObservation
        raw_liquidation_end_date: FieldObservation
        evidence: tuple
        quality: object

    stub = StubLiquidation(
        "yes", "finished", None, _missing(), _missing(), (), _stub_quality()
    )

    with pytest.raises(ValueError, match="requires an observed end date"):
        resolve_liquidation_event(stub, phase="end", context=_context())


def test_a_source_error_from_another_dataset_is_rejected() -> None:
    foreign = SourceError(
        code="PROCESS_LOOKUP_TIMEOUT", dataset=PROCESSES, kind="timeout"
    )

    with pytest.raises(ValueError, match="must belong to dataset"):
        resolve_temporal_assessment(
            _not_observed(),
            context=_context(),
            requested_event="contract_signature",
            applied_field="fecha_de_firma",
            dataset=CONTRACTS,
            source_errors=(foreign,),
        )


def test_a_reversed_range_is_unknown_never_a_confident_zero() -> None:
    assessment = resolve_temporal_assessment(
        _obs("2025-06-15"),
        context=_context(start="2026-01-01", end="2025-01-01"),
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert assessment.range_membership == "unknown"
    assert assessment.range_membership != "outside"
    assert "TEMPORAL_RANGE_REVERSED" in assessment.quality.reason_codes
    assert assessment.quality.completeness == "insufficient"
    assert assessment.quality.confidence == "low"


def test_a_degenerate_timestamp_range_is_unknown() -> None:
    assessment = resolve_temporal_assessment(
        _obs("2025-06-15T10:00:00"),
        context=_context(start="2025-06-15T00:00:00", end="2025-06-15T00:00:00"),
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert assessment.range_membership == "unknown"
    assert "TEMPORAL_RANGE_REVERSED" in assessment.quality.reason_codes


def test_a_single_civil_day_range_is_valid_and_inclusive() -> None:
    assessment = resolve_temporal_assessment(
        _obs("2025-06-15"),
        context=_context(start="2025-06-15", end="2025-06-15"),
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert assessment.range_membership == "inside"
    assert assessment.range_end_exclusive == "2025-06-16"


def test_the_range_zone_is_explicit_and_defaults_to_the_reference_zone() -> None:
    default_zone = _context(start="2025-01-01", end="2025-12-31")
    explicit_zone = TemporalQueryContext(
        reference_instant=datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc),
        reference_timezone="America/Bogota",
        requested_range_start="2025-01-01",
        requested_range_end="2025-12-31",
        requested_event_raw="firmados",
        requested_range_timezone="UTC",
    )

    assert default_zone.range_timezone == "America/Bogota"
    assert explicit_zone.range_timezone == "UTC"

    from_default = resolve_temporal_assessment(
        _obs("2025-06-15"),
        context=default_zone,
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )
    from_explicit = resolve_temporal_assessment(
        _obs("2025-06-15"),
        context=explicit_zone,
        requested_event="contract_signature",
        applied_field="fecha_de_firma",
        dataset=CONTRACTS,
    )

    assert from_default.range_timezone == "America/Bogota"
    assert from_explicit.range_timezone == "UTC"
    assert from_default.source_timezone == from_explicit.source_timezone == "America/Bogota"


def test_the_range_zone_actually_anchors_the_bounds() -> None:
    """The declared zone must MOVE the interval, not just label it.

    The civil day 2025-01-01 spans different absolute instants in Bogotá
    (UTC-5) and in Kiritimati (UTC+14). An instant at 20:00Z on that day is
    inside the Bogotá day and past the end of the Kiritimati one, so the two
    zones must disagree — otherwise the bounds are not being anchored at all.
    """
    instant = _obs("2025-01-01T20:00:00Z")

    def membership(range_zone: str | None) -> str:
        context = TemporalQueryContext(
            reference_instant=datetime(2026, 7, 29, 14, 0, tzinfo=timezone.utc),
            reference_timezone="Pacific/Kiritimati",
            requested_range_start="2025-01-01",
            requested_range_end="2025-01-01",
            requested_event_raw="firmados",
            requested_range_timezone=range_zone,
        )
        return resolve_temporal_assessment(
            instant,
            context=context,
            requested_event="contract_signature",
            applied_field="fecha_de_firma",
            dataset=CONTRACTS,
        ).range_membership

    assert membership("America/Bogota") == "inside"
    # Falling back to the reference zone (UTC+14) must give a different answer.
    assert membership(None) == "outside"


def test_the_range_and_source_zones_cannot_be_falsified_at_all() -> None:
    """Stronger than validation: they are derived, so there is nothing to set.

    ``range_timezone`` and ``source_timezone`` are properties of the context
    and of the value, so ``dataclasses.replace`` cannot even name them.
    """
    assessment = _sample_assessment()

    with pytest.raises(TypeError):
        dataclasses.replace(assessment, range_timezone="UTC")
    with pytest.raises(TypeError):
        dataclasses.replace(assessment, source_timezone="UTC")
    assert assessment.range_timezone == assessment.context.range_timezone
    assert assessment.source_timezone == assessment.value.source_timezone


def test_a_context_with_a_different_range_zone_is_a_different_assessment() -> None:
    assessment = _sample_assessment()
    other = TemporalQueryContext(
        reference_instant=assessment.context.reference_instant,
        reference_timezone=assessment.context.reference_timezone,
        requested_range_start=assessment.context.requested_range_start,
        requested_range_end=assessment.context.requested_range_end,
        requested_event_raw=assessment.context.requested_event_raw,
        requested_range_timezone="Pacific/Kiritimati",
    )

    swapped = dataclasses.replace(assessment, context=other)

    assert swapped.range_timezone == "Pacific/Kiritimati"
    assert swapped.range_start == assessment.range_start
    # The bounds are re-anchored, so the two assessments are not interchangeable.
    assert swapped != assessment


def test_an_unauthorized_axis_reason_code_is_rejected() -> None:
    assessment = _sample_assessment()

    with pytest.raises(ValueError, match="unauthorized axis reason code"):
        dataclasses.replace(
            assessment, axis_reason_codes=("SIGNATURE_LOOKS_FINE_TO_ME",)
        )


def test_quality_cannot_drop_an_inherited_conflict() -> None:
    signals = _contract_signals(state="Borrador", signature="2025-03-14T00:00:00.000")
    signature = resolve_contract_signature(signals)
    assessment = resolve_signature_event(signature, context=_context())

    stripped = build_resolution_quality(
        completeness=assessment.quality.completeness,
        consistency="consistent",
        confidence=assessment.quality.confidence,
        reason_codes=assessment.quality.reason_codes,
        missing_fields=assessment.quality.missing_fields,
        source_errors=assessment.quality.source_errors,
    )

    with pytest.raises(ValueError, match="quality"):
        dataclasses.replace(assessment, quality=stripped)
    # Dropping the axis conflicts themselves is equally rejected.
    with pytest.raises(ValueError, match="quality"):
        dataclasses.replace(assessment, axis_conflicts=())


def test_quality_cannot_drop_an_inherited_source_error() -> None:
    error = SourceError(
        code="CONTRACT_LOOKUP_TIMEOUT", dataset=CONTRACTS, kind="timeout"
    )
    signals = _contract_signals(state=Ellipsis, signature=Ellipsis)
    signature = resolve_contract_signature(signals, source_errors=(error,))
    assessment = resolve_signature_event(signature, context=_context())

    assert error in assessment.quality.source_errors
    assert assessment.axis_source_errors == (error,)

    with pytest.raises(ValueError, match="quality"):
        dataclasses.replace(assessment, axis_source_errors=())


def test_call_and_axis_source_errors_are_both_preserved() -> None:
    axis_error = SourceError(
        code="CONTRACT_LOOKUP_TIMEOUT", dataset=CONTRACTS, kind="timeout"
    )
    call_error = SourceError(
        code="CONTRACT_TRANSPORT_ERROR", dataset=CONTRACTS, kind="transport_error"
    )
    signals = _contract_signals(state=Ellipsis, signature=Ellipsis)
    signature = resolve_contract_signature(signals, source_errors=(axis_error,))

    assessment = resolve_signature_event(
        signature, context=_context(), source_errors=(call_error,)
    )

    assert set(assessment.quality.source_errors) == {axis_error, call_error}
    assert assessment.quality.consistency == "unknown"


# ---------------------------------------------------------------------------
# Modularity.
# ---------------------------------------------------------------------------


def _defined_names(module: object) -> set[str]:
    return {
        node.name
        for node in ast.walk(ast.parse(inspect.getsource(module)))  # type: ignore[arg-type]
        if isinstance(node, (ast.ClassDef, ast.FunctionDef))
    }


def test_each_temporal_responsibility_lives_in_exactly_one_module() -> None:
    assert local_wall_time_mod.LocalWallTimeResolver.__module__ == (
        "app.core.truth_layer.local_wall_time"
    )
    assert value_mod.TemporalValueResolution.__module__ == (
        "app.core.truth_layer.temporal_value"
    )

    composer = _defined_names(temporal_mod)
    assert "TemporalValueResolution" not in composer
    assert "LocalWallTimeResolution" not in composer
    assert "LocalWallTimeResolver" not in composer
    assert "RangeBounds" not in composer
    assert "TemporalAssessment" in composer

    assert "LocalWallTimeResolver" not in _defined_names(value_mod)
    assert "LocalWallTimeResolver" not in _defined_names(range_mod)
    assert "TemporalValueResolution" not in _defined_names(range_mod)


def test_the_composer_contains_no_parser_and_no_dst_detection() -> None:
    executable = _executable_source(temporal_mod)

    for forbidden in ("fromisoformat", "fold=", "utcoffset", "astimezone", "zoneinfo"):
        assert forbidden not in executable


def test_range_rules_live_only_in_the_range_module() -> None:
    composer = _executable_source(temporal_mod)
    value_source = _executable_source(value_mod)

    for forbidden in ("timedelta(days=1)", "range_end_exclusive_instant"):
        assert forbidden not in composer
    assert "requested_range_start" not in value_source
    assert "membership" not in value_source


_TEMPORAL_MODULES = (
    temporal_mod,
    value_mod,
    range_mod,
    local_wall_time_mod,
    contract_mod,
)


def test_no_temporal_module_exceeds_the_size_gate() -> None:
    sizes = {
        module.__name__: len(inspect.getsource(module).splitlines())
        for module in _TEMPORAL_MODULES
    }

    for name, size in sizes.items():
        assert size <= 600, f"{name} has {size} lines"


def test_contract_integration_lives_outside_the_composer() -> None:
    composer = _defined_names(temporal_mod)
    adapter = _defined_names(contract_mod)

    assert "resolve_signature_event" in adapter
    assert "resolve_liquidation_event" in adapter
    assert "SignatureTemporalInput" in adapter
    assert "resolve_signature_event" not in composer
    assert "SignatureTemporalInput" not in composer

    # The composer holds no contract LOGIC and no SECOP field names. It does
    # hold the closed vocabulary of axis reason codes, because it is the
    # component that validates them — that is the point of closing the list.
    composer_source = _executable_source(temporal_mod)
    for forbidden in ("fecha_de_firma", "fecha_fin_liquidacion", "estado_contrato"):
        assert forbidden not in composer_source
    assert not [
        name
        for name in composer
        if "signature" in name.lower() or "liquidation" in name.lower()
    ]


def test_the_temporal_import_graph_is_acyclic() -> None:
    edges: dict[str, set[str]] = {}
    for module in _TEMPORAL_MODULES:
        tree = ast.parse(inspect.getsource(module))
        edges[module.__name__] = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
            and node.module
            and node.module.startswith("app.core.truth_layer.")
        }

    # temporal_value is the root: it depends on models only.
    assert edges[value_mod.__name__] == {"app.core.truth_layer.models"}
    # Nothing upstream may depend on a module further down the chain.
    for upstream in (value_mod, range_mod, local_wall_time_mod):
        assert temporal_mod.__name__ not in edges[upstream.__name__]
        assert contract_mod.__name__ not in edges[upstream.__name__]
    assert contract_mod.__name__ not in edges[temporal_mod.__name__]
    assert range_mod.__name__ not in edges[value_mod.__name__]
    assert local_wall_time_mod.__name__ not in edges[value_mod.__name__]


def test_the_contract_adapter_imports_no_sibling_contract_resolver() -> None:
    tree = ast.parse(inspect.getsource(contract_mod))
    modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }

    assert all(
        module.startswith(("app.core.truth_layer.", "__future__", "typing"))
        for module in modules
    )
    assert not any(
        module.startswith("app.core.truth_layer.contract_") for module in modules
    )
    assert "app.core.truth_layer.process_lifecycle" not in modules


def test_the_contract_adapter_reads_no_clock_and_performs_no_io() -> None:
    tree = ast.parse(inspect.getsource(contract_mod))
    called: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Attribute):
                called.add(target.attr)
            elif isinstance(target, ast.Name):
                called.add(target.id)

    for forbidden in ("now", "utcnow", "today", "open", "getenv", "read_text"):
        assert forbidden not in called


def test_root_exports_are_preserved_after_the_split() -> None:
    import app.core.truth_layer as root

    for name in (
        "SECOP_SOURCE_TIMEZONE",
        "TemporalValueStatus",
        "TemporalRelation",
        "RangeMembership",
        "TemporalEvent",
        "TemporalValueResolution",
        "TemporalAssessment",
        "LocalWallTimeStatus",
        "LocalWallTimeResolution",
        "LocalWallTimeResolver",
        "SignatureTemporalInput",
        "LiquidationTemporalInput",
        "resolve_temporal_value",
        "resolve_temporal_assessment",
        "resolve_signature_event",
        "resolve_liquidation_event",
    ):
        assert name in root.__all__
        assert hasattr(root, name)


# ---------------------------------------------------------------------------
# Purity and scope.
# ---------------------------------------------------------------------------


def test_temporal_resolver_imports_nothing_outside_its_layer() -> None:
    tree = ast.parse(inspect.getsource(temporal_mod))
    roots: set[str] = set()
    app_modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            roots.add(node.module.split(".", 1)[0])
            if node.module.startswith("app."):
                app_modules.add(node.module)

    assert roots <= {"__future__", "dataclasses", "datetime", "typing", "zoneinfo", "app"}
    assert all(
        module.startswith("app.core.truth_layer.") for module in app_modules
    )
    # Sibling resolvers stay unimported: contract inputs arrive structurally.
    assert not any(
        module.startswith("app.core.truth_layer.contract_") for module in app_modules
    )
    assert "app.core.truth_layer.process_lifecycle" not in app_modules


def test_temporal_resolver_reads_no_clock_and_performs_no_io() -> None:
    tree = ast.parse(inspect.getsource(temporal_mod))
    called: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Attribute):
                called.add(target.attr)
            elif isinstance(target, ast.Name):
                called.add(target.id)

    for forbidden in ("now", "utcnow", "today", "open", "getenv", "read_text", "get"):
        assert forbidden not in called


def test_temporal_modules_have_no_runtime_consumers() -> None:
    layer_files = {
        "temporal_resolver.py",
        "temporal_value.py",
        "temporal_range.py",
        "temporal_contract.py",
        "local_wall_time.py",
        "__init__.py",
    }
    markers = (
        "truth_layer.temporal_resolver",
        "truth_layer.temporal_value",
        "truth_layer.temporal_range",
        "truth_layer.temporal_contract",
        "truth_layer.local_wall_time",
    )
    consumers: list[str] = []
    for path in (ROOT / "app").rglob("*.py"):
        if path.parent.name == "truth_layer" and path.name in layer_files:
            continue
        source = path.read_text(encoding="utf-8")
        if any(marker in source for marker in markers):
            consumers.append(str(path.relative_to(ROOT)))

    assert consumers == []


def _executable_source(module: object) -> str:
    """Module source with docstrings stripped.

    Prose that *names* a forbidden concept in order to disclaim it must not
    read as an implementation of it.
    """
    tree = ast.parse(inspect.getsource(module))  # type: ignore[arg-type]
    for node in ast.walk(tree):
        if not isinstance(
            node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
    return ast.unparse(ast.fix_missing_locations(tree)).casefold()


def test_temporal_resolver_decides_no_offerability_ranking_or_retrieval() -> None:
    executable = _executable_source(temporal_mod)

    for forbidden in (
        "offerability",
        "bid_ready",
        "opportunityrole",
        "queryframe",
        "soql",
        "order by",
        "numero_de_lotes",
    ):
        assert forbidden not in executable


def test_temporal_resolver_is_single_pass_and_replace_rederives(monkeypatch) -> None:
    calls = 0
    captured = []
    original = temporal_mod._derive

    def counted(*args, **kwargs):
        nonlocal calls
        calls += 1
        derived = original(*args, **kwargs)
        captured.append(derived)
        return derived

    monkeypatch.setattr(temporal_mod, "_derive", counted)
    assessment = _sample_assessment()
    assert calls == 1
    assert dataclasses.replace(assessment) == assessment
    assert calls == 2

    manual_kwargs = {
        field.name: getattr(assessment, field.name)
        for field in dataclasses.fields(assessment)
    }
    assert type(assessment)(**manual_kwargs) == assessment
    assert calls == 3

    proof = captured[0]
    assert (
        dataclasses.replace(assessment, _resolved_derivation=proof)
        == assessment
    )
    assert calls == 3


def test_temporal_initvar_is_ephemeral_and_checks_exact_provenance(
    monkeypatch,
) -> None:
    captured = []
    original = temporal_mod._derive

    def capture(*args, **kwargs):
        derived = original(*args, **kwargs)
        captured.append(derived)
        return derived

    monkeypatch.setattr(temporal_mod, "_derive", capture)
    assessment = _sample_assessment()
    proof = captured[0]

    names = {field.name for field in dataclasses.fields(assessment)}
    assert "_resolved_derivation" not in names
    assert "_resolved_derivation" not in assessment.__slots__
    assert "_resolved_derivation" not in repr(assessment)

    with pytest.raises(TypeError, match="internal temporal derivation"):
        dataclasses.replace(assessment, _resolved_derivation=object())
    foreign_context = dataclasses.replace(assessment.context)
    with pytest.raises(ValueError, match="different context"):
        dataclasses.replace(
            assessment,
            _resolved_derivation=dataclasses.replace(
                proof, context=foreign_context
            ),
        )
    with pytest.raises(ValueError, match="different evidence or quality"):
        dataclasses.replace(
            assessment,
            _resolved_derivation=dataclasses.replace(
                proof, evidence=tuple(list(proof.evidence))
            ),
        )
    with pytest.raises(ValueError, match="different evidence or quality"):
        dataclasses.replace(
            assessment,
            _resolved_derivation=dataclasses.replace(
                proof, quality=dataclasses.replace(proof.quality)
            ),
        )
