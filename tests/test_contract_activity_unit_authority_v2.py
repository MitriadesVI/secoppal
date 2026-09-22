from __future__ import annotations

from hypothesis import given, settings, strategies as st

from app.core.capability_binding.contract_activity_binding import (
    ContractActivityBinding,
    accredit_contract_unit,
)
from app.core.natural_alpha.models import GroundedJob


def _binding(raw: str, quote: str, aspect: str = "CONTRACT_COUNT") -> ContractActivityBinding:
    start = raw.index(quote)
    job = GroundedJob("ANALYZE", aspect, start, start + len(quote))
    binding = ContractActivityBinding.from_job(job)
    assert binding is not None
    return binding


def test_exact_contract_token_inside_interval_is_the_only_fallback_unit() -> None:
    raw = "Informe de aseo: ¿Cuántos contratos firmó la Alcaldía de Galapa?"
    accredited = accredit_contract_unit(raw, _binding(raw, "Cuántos contratos"))
    omitted = accredit_contract_unit(raw, _binding(raw, "Cuántos"))

    assert accredited.accredited is True
    assert accredited.authority == "grounded_interval_literal"
    assert raw[slice(*accredited.span)] == "contratos"
    assert omitted.accredited is False
    assert omitted.reason == "CONTRACT_UNIT_NOT_ACCREDITED"


def test_structural_contract_occurrence_precedes_under_specified_interval() -> None:
    raw = (
        "Para los contratos de Cartagena superiores a 100 millones, "
        "muéstrame la distribución por modalidad."
    )
    result = accredit_contract_unit(
        raw,
        _binding(raw, "muéstrame la distribución por modalidad", "CONTRACTS_BY_MODALITY"),
    )
    assert result.accredited is True
    assert result.authority == "source_request_occurrences"


def test_buyer_name_collision_outside_interval_does_not_accredit_unit() -> None:
    raw = "¿Cuántos registros firmó la Agencia Contratos del Caribe?"
    result = accredit_contract_unit(raw, _binding(raw, "Cuántos registros"))
    assert result.accredited is False
    assert result.reason == "CONTRACT_UNIT_NOT_ACCREDITED"


def test_process_contract_conflict_and_multiple_units_are_absorbing() -> None:
    conflict = "Cuenta procesos y contratos de Galapa."
    conflict_result = accredit_contract_unit(conflict, _binding(conflict, conflict))
    assert conflict_result.accredited is False
    assert conflict_result.reason == "PROCESS_CONTRACT_UNIT_CONFLICT"


def test_contrataciones_never_becomes_contract_unit() -> None:
    raw = "¿Cuántas contrataciones hizo la Alcaldía de Galapa?"
    result = accredit_contract_unit(raw, _binding(raw, "Cuántas contrataciones hizo"))
    assert result.accredited is False
    assert result.reason == "CONTRACT_UNIT_NOT_ACCREDITED"


def test_same_raw_different_grounded_intervals_have_different_authority() -> None:
    raw = "Informe de aseo: ¿Cuántos contratos firmó Galapa?"
    with_unit = accredit_contract_unit(raw, _binding(raw, "Cuántos contratos"))
    without_unit = accredit_contract_unit(raw, _binding(raw, "Cuántos"))
    assert with_unit.accredited is True
    assert without_unit.accredited is False


@settings(max_examples=40, deadline=None)
@given(
    left=st.text(alphabet=" abcdefghijklmnñopqrstuvwxyz", max_size=12),
    right=st.text(alphabet=" abcdefghijklmnñopqrstuvwxyz", max_size=12),
    plural=st.booleans(),
)
def test_interval_unit_property_is_exact_and_omission_fails(
    left: str, right: str, plural: bool,
) -> None:
    token = "contratos" if plural else "contrato"
    raw = f"Material {left} {token} {right} para Galapa"
    start = raw.index(token)
    job = GroundedJob("ANALYZE", "CONTRACT_COUNT", start, start + len(token))
    binding = ContractActivityBinding.from_job(job)
    assert binding is not None
    assert accredit_contract_unit(raw, binding).accredited is True

    omitted = GroundedJob("ANALYZE", "CONTRACT_COUNT", 0, len("Material"))
    omitted_binding = ContractActivityBinding.from_job(omitted)
    assert omitted_binding is not None
    result = accredit_contract_unit(raw, omitted_binding)
    # A generated sentence may accidentally form a structural source head; if
    # it does, that higher-precedence authority is the only allowed success.
    if result.accredited:
        assert result.authority == "source_request_occurrences"
    else:
        assert result.reason == "CONTRACT_UNIT_NOT_ACCREDITED"
