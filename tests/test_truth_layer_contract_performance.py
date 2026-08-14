"""T1B-3 contract performance tests."""
from __future__ import annotations

import dataclasses

import pytest

from app.core.truth_layer import (
    ContractLifecycleSignals,
    FieldObservation,
    SourceError,
    resolve_contract_performance,
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
    signature: object = None,
    start: object = None,
    end: object = None,
    flag: object = None,
    liquidation_start: object = None,
    liquidation_end: object = None,
) -> ContractLifecycleSignals:
    blank = absent if absent is not None else _missing()

    def field(value: object) -> FieldObservation:
        if isinstance(value, FieldObservation):
            return value
        return blank if value is None else _obs(value)

    return ContractLifecycleSignals(
        raw_contract_state=field(state),
        raw_signature_date=field(signature),
        raw_start_date=field(start),
        raw_end_date=field(end),
        raw_liquidation_flag=field(flag),
        raw_liquidation_start_date=field(liquidation_start),
        raw_liquidation_end_date=field(liquidation_end),
    )


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("En ejecución", "executing"),
        ("Suspendido", "suspended"),
        ("terminado", "terminated"),
        ("Cerrado", "closed"),
        ("Cancelado", "cancelled"),
        ("Borrador", "draft"),
        ("En aprobación", "approval"),
        ("Aprobado", "approval"),
    ],
)
def test_explicit_state_catalog(raw: str, expected: str) -> None:
    assessment = resolve_contract_performance(_signals(raw))
    assert assessment.performance == expected
    assert assessment.raw_contract_state.raw_value == raw
    assert assessment.quality.confidence == "high"


@pytest.mark.parametrize("raw", ["Modificado", "Prorrogado", "cedido"])
def test_change_only_literal_never_infers_performance(raw: str) -> None:
    assessment = resolve_contract_performance(_signals(raw))
    assert assessment.performance == "unknown"
    assert "CONTRACT_STATE_CHANGE_ONLY" in assessment.quality.reason_codes


def test_catalog_is_exact_not_substring_matching() -> None:
    assessment = resolve_contract_performance(_signals("Cerrado con novedad"))
    assert assessment.performance == "unknown"
    assert "CONTRACT_STATE_UNRECOGNIZED" in assessment.quality.reason_codes


def test_dates_signature_and_liquidation_do_not_rewrite_performance() -> None:
    assessment = resolve_contract_performance(
        _signals(
            "terminado",
            signature="2025-01-01",
            start="2025-01-02",
            end="2025-02-01",
            flag="Si",
            liquidation_start="2025-02-02",
            liquidation_end="2025-02-03",
        )
    )
    assert assessment.performance == "terminated"
    assert len(assessment.evidence) == 1


def test_missing_and_not_observed_remain_distinct() -> None:
    missing = resolve_contract_performance(_signals())
    not_observed = resolve_contract_performance(
        _signals(absent=_not_observed())
    )
    assert missing.performance == not_observed.performance == "unknown"
    assert missing.evidence[0].role == "missing"
    assert not_observed.evidence[0].role == "not_observed"
    assert missing.quality.missing_fields == ("estado_contrato",)
    assert not_observed.quality.missing_fields == ()


def test_source_error_is_not_an_absence() -> None:
    error = SourceError(
        code="CONTRACT_SOURCE_TIMEOUT",
        dataset="jbjy-vk9h",
        kind="timeout",
    )
    assessment = resolve_contract_performance(
        _signals(absent=_not_observed()), source_errors=(error,)
    )
    assert assessment.performance == "unknown"
    assert assessment.quality.source_errors == (error,)
    assert assessment.evidence[0].role == "not_observed"


def test_public_invariant_rejects_forged_performance() -> None:
    assessment = resolve_contract_performance(_signals("En ejecución"))
    with pytest.raises(ValueError, match="contradicts"):
        dataclasses.replace(assessment, performance="closed")


def test_assessment_is_frozen_slotted_and_hashable() -> None:
    assessment = resolve_contract_performance(_signals("Cerrado"))
    assert assessment.__dataclass_params__.frozen is True
    assert "__slots__" in type(assessment).__dict__
    assert "__dict__" not in type(assessment).__dict__
    assert isinstance(hash(assessment), int)
    with pytest.raises(dataclasses.FrozenInstanceError):
        assessment.performance = "executing"  # type: ignore[misc]
