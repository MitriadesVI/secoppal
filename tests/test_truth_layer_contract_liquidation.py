"""T1B-3 contract liquidation tests."""
from __future__ import annotations

import dataclasses

import pytest

from app.core.truth_layer import (
    ContractLifecycleSignals,
    FieldObservation,
    SourceError,
    resolve_contract_liquidation,
)


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def _signals(
    *,
    state: object = None,
    flag: object = None,
    start: object = None,
    end: object = None,
    absent: FieldObservation | None = None,
) -> ContractLifecycleSignals:
    blank = absent if absent is not None else _missing()

    def field(value: object) -> FieldObservation:
        if isinstance(value, FieldObservation):
            return value
        return blank if value is None else _obs(value)

    return ContractLifecycleSignals(
        raw_contract_state=field(state),
        raw_signature_date=blank,
        raw_start_date=blank,
        raw_end_date=blank,
        raw_liquidation_flag=field(flag),
        raw_liquidation_start_date=field(start),
        raw_liquidation_end_date=field(end),
    )


def test_yes_flag_and_end_date_is_finished() -> None:
    raw_end = " 2025-12-31T00:00:00.000 "
    assessment = resolve_contract_liquidation(
        _signals(flag="Sí", start="2025-01-01", end=raw_end)
    )
    assert assessment.liquidation_status == "yes"
    assert assessment.completion == "finished"
    assert assessment.completion_event_observation is assessment.raw_liquidation_end_date
    assert assessment.completion_event_observation.raw_value == raw_end


def test_yes_flag_start_without_end_is_in_progress() -> None:
    assessment = resolve_contract_liquidation(
        _signals(flag="Si", start="2025-01-01")
    )
    assert assessment.liquidation_status == "in_progress"
    assert assessment.completion == "started"
    assert assessment.completion_event_observation is None


def test_yes_flag_without_dates_is_yes_with_unknown_completion() -> None:
    assessment = resolve_contract_liquidation(_signals(flag="Si"))
    assert assessment.liquidation_status == "yes"
    assert assessment.completion == "unknown"
    assert assessment.quality.completeness == "partial"


def test_no_flag_without_dates_is_no() -> None:
    assessment = resolve_contract_liquidation(_signals(flag="No"))
    assert assessment.liquidation_status == "no"
    assert assessment.completion == "not_started"


@pytest.mark.parametrize(
    "start,end",
    [
        ("2025-01-01", None),
        (None, "2025-02-01"),
        ("2025-01-01", "2025-02-01"),
    ],
)
def test_no_flag_with_any_date_is_conflicting(
    start: str | None,
    end: str | None,
) -> None:
    assessment = resolve_contract_liquidation(
        _signals(flag="No", start=start, end=end)
    )
    assert assessment.liquidation_status == "conflicting"
    assert assessment.completion == "conflicting"
    assert assessment.quality.consistency == "conflicting"
    assert assessment.quality.conflicts


def test_start_without_flag_is_explicitly_inferred_in_progress() -> None:
    assessment = resolve_contract_liquidation(
        _signals(start="2025-01-01")
    )
    assert assessment.liquidation_status == "in_progress"
    assert assessment.completion == "started"
    assert "LIQUIDATION_INFERRED_FROM_START_DATE" in assessment.quality.reason_codes


def test_end_without_start_or_flag_is_yes_with_sequence_conflict() -> None:
    assessment = resolve_contract_liquidation(
        _signals(end="2025-02-01")
    )
    assert assessment.liquidation_status == "yes"
    assert assessment.completion == "finished"
    assert assessment.quality.consistency == "conflicting"
    assert {
        conflict.code for conflict in assessment.quality.conflicts
    } == {"LIQUIDATION_END_WITHOUT_START"}


def test_closed_with_negative_flag_remains_not_liquidated() -> None:
    assessment = resolve_contract_liquidation(
        _signals(state="Cerrado", flag="No")
    )
    assert assessment.liquidation_status == "no"
    assert not assessment.quality.conflicts


def test_executing_with_positive_flag_preserves_both_without_fake_conflict() -> None:
    assessment = resolve_contract_liquidation(
        _signals(state="En ejecución", flag="Si")
    )
    assert assessment.liquidation_status == "yes"
    assert assessment.raw_contract_state.raw_value == "En ejecución"
    assert not assessment.quality.conflicts


def test_missing_is_distinct_from_not_observed_and_source_error() -> None:
    missing = resolve_contract_liquidation(_signals())
    error = SourceError(
        code="LIQUIDATION_SOURCE_TIMEOUT",
        dataset="jbjy-vk9h",
        kind="timeout",
    )
    not_observed = resolve_contract_liquidation(
        _signals(absent=_not_observed()), source_errors=(error,)
    )
    assert missing.liquidation_status == not_observed.liquidation_status == "unknown"
    assert {item.role for item in missing.evidence} == {"missing"}
    assert {item.role for item in not_observed.evidence} == {"not_observed"}
    assert not_observed.quality.source_errors == (error,)


def test_public_invariant_rejects_yes_with_negative_flag_without_conflict() -> None:
    assessment = resolve_contract_liquidation(_signals(flag="No"))
    with pytest.raises(ValueError, match="contradict"):
        dataclasses.replace(
            assessment,
            liquidation_status="yes",
            completion="finished",
        )


def test_assessment_is_frozen_slotted_hashable_and_has_no_temporal_parse() -> None:
    assessment = resolve_contract_liquidation(
        _signals(flag="Si", end="not-parsed-by-this-block")
    )
    assert assessment.liquidation_status == "yes"
    assert assessment.__dataclass_params__.frozen is True
    assert "__slots__" in type(assessment).__dict__
    assert "__dict__" not in type(assessment).__dict__
    assert isinstance(hash(assessment), int)
    assert not {
        "liquidation_year",
        "date_range",
        "relation_to_reference",
    } & {field.name for field in dataclasses.fields(assessment)}
