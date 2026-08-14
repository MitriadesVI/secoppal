"""T1B-3 contract change tests."""
from __future__ import annotations

import dataclasses

import pytest

from app.core.truth_layer import (
    ContractLifecycleSignals,
    FieldObservation,
    SourceError,
    resolve_contract_changes,
)


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def _signals(
    state: object = None,
    *,
    absent: FieldObservation | None = None,
) -> ContractLifecycleSignals:
    blank = absent if absent is not None else _missing()
    raw_state = (
        state
        if isinstance(state, FieldObservation)
        else blank if state is None else _obs(state)
    )
    return ContractLifecycleSignals(
        raw_contract_state=raw_state,
        raw_signature_date=blank,
        raw_start_date=blank,
        raw_end_date=blank,
        raw_liquidation_flag=blank,
        raw_liquidation_start_date=blank,
        raw_liquidation_end_date=blank,
    )


@pytest.mark.parametrize(
    "literal,observed_axis",
    [
        ("Modificado", "modified"),
        ("Prorrogado", "extended"),
        ("cedido", "assigned"),
    ],
)
def test_change_literal_observes_only_its_current_axis(
    literal: str,
    observed_axis: str,
) -> None:
    assessment = resolve_contract_changes(_signals(literal))
    axes = {
        "modified": assessment.modified,
        "extended": assessment.extended,
        "assigned": assessment.assigned,
    }
    for name, axis in axes.items():
        assert axis.source_coverage == "partial"
        if name == observed_axis:
            assert axis.signal == "observed"
            assert axis.historical_conclusion == "observed"
        else:
            assert axis.signal == "not_indicated"
            assert axis.historical_conclusion == "indeterminate"


def test_executing_state_never_claims_changes_never_happened() -> None:
    assessment = resolve_contract_changes(_signals("En ejecución"))
    for axis in (
        assessment.modified,
        assessment.extended,
        assessment.assigned,
    ):
        assert axis.signal == "not_indicated"
        assert axis.source_coverage == "partial"
        assert axis.historical_conclusion == "indeterminate"
        assert not hasattr(axis, "never_happened")


def test_new_current_literal_still_has_only_partial_historical_coverage() -> None:
    assessment = resolve_contract_changes(_signals("Nuevo Estado 2032"))
    assert assessment.modified.source_coverage == "partial"
    assert assessment.extended.historical_conclusion == "indeterminate"
    assert assessment.assigned.signal == "not_indicated"


def test_missing_and_not_observed_have_different_coverage_and_evidence() -> None:
    missing = resolve_contract_changes(_signals())
    not_observed = resolve_contract_changes(
        _signals(absent=_not_observed())
    )
    assert missing.modified.source_coverage == "insufficient"
    assert not_observed.modified.source_coverage == "insufficient"
    assert missing.evidence[0].role == "missing"
    assert not_observed.evidence[0].role == "not_observed"
    assert missing.quality.missing_fields == ("estado_contrato",)
    assert not_observed.quality.missing_fields == ()


def test_source_error_is_preserved_as_unknown_coverage() -> None:
    error = SourceError(
        code="CONTRACT_CHANGE_TIMEOUT",
        dataset="jbjy-vk9h",
        kind="timeout",
    )
    assessment = resolve_contract_changes(
        _signals(absent=_not_observed()), source_errors=(error,)
    )
    for axis in (
        assessment.modified,
        assessment.extended,
        assessment.assigned,
    ):
        assert axis.source_coverage == "unknown"
        assert axis.quality.source_errors == (error,)
    assert assessment.quality.source_errors == (error,)


def test_public_invariant_rejects_observed_axis_for_incompatible_literal() -> None:
    assessment = resolve_contract_changes(_signals("En ejecución"))
    with pytest.raises(ValueError, match="contradicts"):
        dataclasses.replace(assessment.modified, signal="observed")


def test_parent_rejects_axes_from_different_signal_objects() -> None:
    first = resolve_contract_changes(_signals("Modificado"))
    second = resolve_contract_changes(_signals("Modificado"))
    with pytest.raises(ValueError, match="same signals object"):
        dataclasses.replace(first, extended=second.extended)


def test_change_assessments_are_frozen_slotted_and_hashable() -> None:
    assessment = resolve_contract_changes(_signals("Prorrogado"))
    for item in (
        assessment,
        assessment.modified,
        assessment.extended,
        assessment.assigned,
    ):
        assert item.__dataclass_params__.frozen is True
        assert "__slots__" in type(item).__dict__
        assert "__dict__" not in type(item).__dict__
        assert isinstance(hash(item), int)


def test_raw_state_object_is_shared_not_normalized_or_rebuilt() -> None:
    signals = _signals("  Prórrogado  ")
    assessment = resolve_contract_changes(signals)
    assert assessment.raw_contract_state is signals.raw_contract_state
    assert assessment.raw_contract_state.raw_value == "  Prórrogado  "
    assert assessment.extended.signal == "observed"
