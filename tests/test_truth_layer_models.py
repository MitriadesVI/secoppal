from __future__ import annotations

import ast
import dataclasses
import inspect
import json
import math
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import app.core.truth_layer.models as models
from app.core.truth_layer import (
    ContractLifecycleSignals,
    FieldObservation,
    ProcessContractReference,
    ProcessLifecycleSignals,
    PublicationVisibilitySignals,
    TemporalQueryContext,
)


OBSERVED = FieldObservation("observed", "value")
MISSING = FieldObservation("missing", None)
NOT_OBSERVED = FieldObservation("not_observed", None)


def _process_signals() -> ProcessLifecycleSignals:
    return ProcessLifecycleSignals(
        raw_procedure_state=OBSERVED,
        raw_procedure_state_id=OBSERVED,
        raw_opening_state=OBSERVED,
        raw_phase=MISSING,
        raw_summary_state=NOT_OBSERVED,
        raw_adjudicated=OBSERVED,
        raw_adjudication_date=MISSING,
        raw_adjudication_id=OBSERVED,
        raw_portfolio_id=OBSERVED,
        raw_modality=OBSERVED,
    )


def _contract_signals() -> ContractLifecycleSignals:
    return ContractLifecycleSignals(
        raw_contract_state=OBSERVED,
        raw_signature_date=MISSING,
        raw_start_date=OBSERVED,
        raw_end_date=NOT_OBSERVED,
        raw_liquidation_flag=OBSERVED,
        raw_liquidation_start_date=MISSING,
        raw_liquidation_end_date=NOT_OBSERVED,
    )


def test_all_model_dataclasses_are_frozen_and_slotted() -> None:
    classes = (
        FieldObservation,
        ProcessLifecycleSignals,
        ContractLifecycleSignals,
        ProcessContractReference,
        PublicationVisibilitySignals,
        TemporalQueryContext,
    )
    for model in classes:
        params = model.__dataclass_params__
        assert params.frozen is True
        assert "__slots__" in model.__dict__
        assert "__dict__" not in model.__dict__


def test_models_have_stable_equality_hashing_and_serialization() -> None:
    first = _process_signals()
    second = _process_signals()

    assert first == second
    assert hash(first) == hash(second)
    first_json = json.dumps(
        dataclasses.asdict(first),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    second_json = json.dumps(
        dataclasses.asdict(second),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    assert first_json == second_json


@pytest.mark.parametrize(
    ("value", "expected_type"),
    [
        ("", str),
        ("   ", str),
        (True, bool),
        (False, bool),
        (0, int),
        (0.0, float),
        ("text", str),
        (42, int),
        (4.25, float),
    ],
)
def test_observed_preserves_exact_scalar(value: object, expected_type: type) -> None:
    observation = FieldObservation("observed", value)  # type: ignore[arg-type]

    assert observation.raw_value == value
    assert type(observation.raw_value) is expected_type


def test_missing_and_not_observed_are_valid_and_distinct() -> None:
    assert MISSING != NOT_OBSERVED
    assert MISSING.raw_value is None
    assert NOT_OBSERVED.raw_value is None


@pytest.mark.parametrize(
    ("presence", "value"),
    [
        ("observed", None),
        ("missing", "value"),
        ("not_observed", 0),
        ("unknown", None),
    ],
)
def test_invalid_presence_value_combinations_are_rejected(
    presence: str,
    value: object,
) -> None:
    with pytest.raises(ValueError):
        FieldObservation(presence, value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "value",
    [
        [],
        {},
        set(),
        (),
        object(),
        complex(1, 2),
    ],
)
def test_non_scalar_raw_values_are_rejected(value: object) -> None:
    with pytest.raises(TypeError):
        FieldObservation("observed", value)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_non_finite_floats_are_rejected(value: float) -> None:
    with pytest.raises(ValueError):
        FieldObservation("observed", value)


def test_signal_models_require_field_observations() -> None:
    values = {
        field.name: OBSERVED
        for field in dataclasses.fields(ProcessLifecycleSignals)
    }
    values["raw_phase"] = "Presentación de oferta"

    with pytest.raises(TypeError, match="raw_phase"):
        ProcessLifecycleSignals(**values)  # type: ignore[arg-type]


def test_process_and_contract_signal_models_are_hashable() -> None:
    assert isinstance(hash(_process_signals()), int)
    assert isinstance(hash(_contract_signals()), int)


def test_portfolio_identities_remain_separate() -> None:
    reference = ProcessContractReference(
        process_portfolio_id=FieldObservation("observed", "CO1.BDOS.1"),
        procurement_portfolio_id=FieldObservation("observed", "CO1.BDOS.2"),
    )

    assert reference.process_portfolio_id != reference.procurement_portfolio_id
    assert not hasattr(reference, "source_process_id")


def test_visibility_signals_contain_observations_without_inference() -> None:
    signals = PublicationVisibilitySignals(
        raw_url=MISSING,
        normalized_url=MISSING,
        raw_process_id=OBSERVED,
        normalization_reason=FieldObservation("observed", "login_url_rejected"),
    )

    assert all(
        isinstance(getattr(signals, field.name), FieldObservation)
        for field in dataclasses.fields(signals)
    )
    for forbidden in ("visibility", "prepublication", "bid_ready", "confidence"):
        assert not hasattr(signals, forbidden)


def test_temporal_context_accepts_coherent_iana_zone() -> None:
    instant = datetime(2026, 7, 27, 9, 30, tzinfo=ZoneInfo("America/Bogota"))

    context = TemporalQueryContext(
        reference_instant=instant,
        reference_timezone="America/Bogota",
        requested_range_start="2026-07-01",
        requested_range_end="2026-07-31",
        requested_event_raw="publicación",
    )

    assert context.reference_instant is instant


def test_temporal_context_accepts_coherent_fixed_offset_instant() -> None:
    instant = datetime(
        2026,
        7,
        27,
        9,
        30,
        tzinfo=timezone(timedelta(hours=-5)),
    )

    context = TemporalQueryContext(
        reference_instant=instant,
        reference_timezone="America/Bogota",
        requested_range_start=None,
        requested_range_end=None,
        requested_event_raw=None,
    )

    assert context.reference_timezone == "America/Bogota"


def test_temporal_context_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        TemporalQueryContext(
            reference_instant=datetime(2026, 7, 27, 9, 30),
            reference_timezone="America/Bogota",
            requested_range_start=None,
            requested_range_end=None,
            requested_event_raw=None,
        )


def test_temporal_context_rejects_invalid_iana_zone() -> None:
    with pytest.raises(ValueError, match="valid IANA"):
        TemporalQueryContext(
            reference_instant=datetime.now(timezone.utc),
            reference_timezone="Mars/Olympus_Mons",
            requested_range_start=None,
            requested_range_end=None,
            requested_event_raw=None,
        )


def test_temporal_context_accepts_utc_representation_of_the_same_instant() -> None:
    """Option B: the instant and the interpretive zone are independent.

    ``2026-07-27T14:30Z`` and ``2026-07-27T09:30-05:00`` are the same instant.
    Requiring the original offset to equal the zone's offset would reject a
    perfectly valid equivalent representation.
    """
    utc_instant = datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc)

    context = TemporalQueryContext(
        reference_instant=utc_instant,
        reference_timezone="America/Bogota",
        requested_range_start=None,
        requested_range_end=None,
        requested_event_raw=None,
    )

    assert context.reference_instant is utc_instant
    assert context.local_reference == datetime(
        2026, 7, 27, 9, 30, tzinfo=ZoneInfo("America/Bogota")
    )


def test_temporal_context_accepts_any_offset_denoting_an_absolute_instant() -> None:
    instant = datetime(2026, 7, 27, 9, 30, tzinfo=timezone(timedelta(hours=-4)))

    context = TemporalQueryContext(
        reference_instant=instant,
        reference_timezone="America/Bogota",
        requested_range_start=None,
        requested_range_end=None,
        requested_event_raw=None,
    )

    assert context.local_reference.utcoffset() == timedelta(hours=-5)
    assert context.local_reference == instant


def test_local_reference_is_derived_without_mutating_the_instant() -> None:
    utc_instant = datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc)
    context = TemporalQueryContext(
        reference_instant=utc_instant,
        reference_timezone="America/Bogota",
        requested_range_start=None,
        requested_range_end=None,
        requested_event_raw=None,
    )

    first = context.local_reference
    second = context.local_reference

    assert first == second
    assert context.reference_instant is utc_instant
    assert context.reference_instant.tzinfo is timezone.utc
    assert context.reference_instant.utcoffset() == timedelta(0)
    # Same point in time, read from two vantage points.
    assert first == context.reference_instant


def test_temporal_context_rejects_non_string_timezone() -> None:
    with pytest.raises(TypeError):
        TemporalQueryContext(
            reference_instant=datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc),
            reference_timezone=5,  # type: ignore[arg-type]
            requested_range_start=None,
            requested_range_end=None,
            requested_event_raw=None,
        )


def test_temporal_context_rejects_non_datetime_instant() -> None:
    with pytest.raises(TypeError):
        TemporalQueryContext(
            reference_instant="2026-07-27T14:30:00Z",  # type: ignore[arg-type]
            reference_timezone="America/Bogota",
            requested_range_start=None,
            requested_range_end=None,
            requested_event_raw=None,
        )


def test_range_timezone_defaults_to_the_reference_zone() -> None:
    context = TemporalQueryContext(
        reference_instant=datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc),
        reference_timezone="America/Bogota",
        requested_range_start="2025-01-01",
        requested_range_end="2025-12-31",
        requested_event_raw="firmados",
    )

    assert context.requested_range_timezone is None
    assert context.range_timezone == "America/Bogota"


def test_range_timezone_can_be_declared_explicitly() -> None:
    context = TemporalQueryContext(
        reference_instant=datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc),
        reference_timezone="UTC",
        requested_range_start="2025-01-01",
        requested_range_end="2025-12-31",
        requested_event_raw="firmados",
        requested_range_timezone="America/Bogota",
    )

    # The query's calendar and the reference's calendar are separate facts.
    assert context.range_timezone == "America/Bogota"
    assert context.reference_timezone == "UTC"


@pytest.mark.parametrize(
    ("value", "expected"),
    [("Mars/Olympus_Mons", ValueError), (5, TypeError), (object(), TypeError)],
)
def test_invalid_range_timezones_are_rejected(value: object, expected: type) -> None:
    with pytest.raises(expected):
        TemporalQueryContext(
            reference_instant=datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc),
            reference_timezone="America/Bogota",
            requested_range_start=None,
            requested_range_end=None,
            requested_event_raw=None,
            requested_range_timezone=value,  # type: ignore[arg-type]
        )


def test_range_timezone_is_backward_compatible_with_existing_constructors() -> None:
    positional = TemporalQueryContext(
        datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc),
        "America/Bogota",
        None,
        None,
        None,
    )

    assert positional.requested_range_timezone is None
    assert positional.range_timezone == "America/Bogota"


def test_temporal_context_does_not_interpret_a_raw_local_wall_time() -> None:
    """The wall-time boundary is explicit and lives elsewhere.

    A nonexistent or ambiguous local wall time must never be smuggled in as a
    reference instant, so the context accepts no such field and no such string.
    """
    context = TemporalQueryContext(
        reference_instant=datetime(2026, 7, 27, 14, 30, tzinfo=timezone.utc),
        reference_timezone="America/Bogota",
        requested_range_start=None,
        requested_range_end=None,
        requested_event_raw=None,
    )

    for forbidden in ("local_wall_time", "local_wall_time_raw", "fold", "resolve"):
        assert not hasattr(context, forbidden)


def test_models_have_no_clock_io_or_external_dependencies() -> None:
    source = inspect.getsource(models)
    tree = ast.parse(source)
    imported_roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(
                alias.name.split(".", 1)[0] for alias in node.names
            )
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots <= {
        "__future__",
        "math",
        "dataclasses",
        "datetime",
        "typing",
        "zoneinfo",
    }
    for forbidden in (
        "datetime.now(",
        "datetime.utcnow(",
        "date.today(",
        "time.time(",
        "open(",
        "getenv(",
        "requests.",
        "urllib.",
    ):
        assert forbidden not in source


def test_t1a_exports_no_assessments_evidence_or_resolvers() -> None:
    forbidden = {
        "Evidence",
        "AssessmentConflict",
        "ResolutionQuality",
        "ProcessLifecycleAssessment",
        "ContractLifecycleAssessment",
        "OpportunityRoleAssessment",
        "PublicationVisibilityAssessment",
        "TemporalAssessment",
        "resolvers",
    }

    assert forbidden.isdisjoint(vars(models))
