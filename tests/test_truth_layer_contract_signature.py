"""T1B-3 contract signature tests."""
from __future__ import annotations

import dataclasses

import pytest

from app.core.truth_layer import (
    ContractLifecycleSignals,
    FieldObservation,
    SourceError,
    resolve_contract_signature,
)


def _obs(value: object) -> FieldObservation:
    return FieldObservation("observed", value)  # type: ignore[arg-type]


def _missing() -> FieldObservation:
    return FieldObservation("missing", None)


def _not_observed() -> FieldObservation:
    return FieldObservation("not_observed", None)


def _signals(
    state: object = None,
    signature: object = None,
    *,
    absent: FieldObservation | None = None,
) -> ContractLifecycleSignals:
    blank = absent if absent is not None else _missing()

    def field(value: object) -> FieldObservation:
        if isinstance(value, FieldObservation):
            return value
        return blank if value is None else _obs(value)

    return ContractLifecycleSignals(
        raw_contract_state=field(state),
        raw_signature_date=field(signature),
        raw_start_date=blank,
        raw_end_date=blank,
        raw_liquidation_flag=blank,
        raw_liquidation_start_date=blank,
        raw_liquidation_end_date=blank,
    )


def test_observed_signature_date_is_direct_and_raw_is_preserved() -> None:
    raw = " 2025-06-10T00:00:00.000 "
    assessment = resolve_contract_signature(_signals("En ejecución", raw))
    assert assessment.signature_status == "directly_observed"
    assert assessment.signature_date_status == "observed"
    assert assessment.raw_signature_date.raw_value == raw


@pytest.mark.parametrize(
    "state",
    [
        "En ejecución",
        "Suspendido",
        "terminado",
        "Cerrado",
        "Modificado",
        "Prorrogado",
        "cedido",
        "Aprobado",
    ],
)
def test_post_signature_state_can_infer_signing_without_inventing_a_date(
    state: str,
) -> None:
    assessment = resolve_contract_signature(_signals(state))
    assert (
        assessment.signature_status
        == "inferred_from_post_signature_evidence"
    )
    assert assessment.signature_date_status == "missing"
    assert assessment.raw_signature_date.raw_value is None
    assert not {
        "signature_year",
        "inferred_signature_date",
        "date_range",
    } & {field.name for field in dataclasses.fields(assessment)}


@pytest.mark.parametrize("state", ["Borrador", "En aprobación", "enviado Proveedor"])
def test_pre_signature_state_without_date_is_not_signed(state: str) -> None:
    assessment = resolve_contract_signature(_signals(state))
    assert assessment.signature_status == "not_signed"


@pytest.mark.parametrize("state", ["Borrador", "En aprobación", "enviado Proveedor"])
def test_pre_signature_state_with_date_is_conflicting(state: str) -> None:
    assessment = resolve_contract_signature(
        _signals(state, "2025-01-01")
    )
    assert assessment.signature_status == "conflicting"
    assert assessment.quality.consistency == "conflicting"
    assert len(assessment.quality.conflicts) == 1
    assert {item.role for item in assessment.evidence} == {"contradicts"}


def test_missing_date_alone_never_means_not_signed() -> None:
    assessment = resolve_contract_signature(_signals())
    assert assessment.signature_date_status == "missing"
    assert assessment.signature_status == "unknown"


def test_not_observed_date_is_distinct_from_missing() -> None:
    missing = resolve_contract_signature(_signals())
    not_observed = resolve_contract_signature(
        _signals(signature=_not_observed())
    )
    assert missing.signature_date_status == "missing"
    assert not_observed.signature_date_status == "not_observed"
    by_field = {item.field: item for item in not_observed.evidence}
    assert by_field["fecha_de_firma"].role == "not_observed"


def test_source_error_remains_unknown_not_unsigned() -> None:
    error = SourceError(
        code="CONTRACT_SIGNATURE_TIMEOUT",
        dataset="jbjy-vk9h",
        kind="timeout",
    )
    assessment = resolve_contract_signature(
        _signals(absent=_not_observed()), source_errors=(error,)
    )
    assert assessment.signature_status == "unknown"
    assert assessment.quality.source_errors == (error,)


def test_public_invariant_rejects_direct_signature_without_date() -> None:
    assessment = resolve_contract_signature(_signals())
    with pytest.raises(ValueError, match="contradicts"):
        dataclasses.replace(
            assessment,
            signature_status="directly_observed",
        )


def test_public_invariant_rejects_inferred_signature_with_invented_date() -> None:
    inferred = resolve_contract_signature(_signals("En ejecución"))
    with pytest.raises(ValueError, match="must come from signals"):
        dataclasses.replace(
            inferred,
            raw_signature_date=_obs("2025-01-01"),
        )


def test_assessment_is_frozen_slotted_and_hashable() -> None:
    assessment = resolve_contract_signature(
        _signals("En ejecución", "2025-01-01")
    )
    assert assessment.__dataclass_params__.frozen is True
    assert "__slots__" in type(assessment).__dict__
    assert "__dict__" not in type(assessment).__dict__
    assert isinstance(hash(assessment), int)
