from __future__ import annotations

from app.core.estado_families import ESTADO_FAMILIES, collect_estado_values


def _collect_all_states_from_families() -> set[str]:
    states: set[str] = set()
    for family in ESTADO_FAMILIES.values():
        for dataset_config in family.values():
            for values in dataset_config.values():
                states.update(values)
    return states


def test_no_invalid_states_declared():
    """
    INV-005 (apoyo): valores declarados deben existir en jbjy-vk9h.
    'Firmado' y 'Celebrado' no existen y no deben aparecer en familias.
    """
    invalid_states = {"Firmado", "Celebrado"}
    all_declared = _collect_all_states_from_families()
    forbidden = invalid_states & all_declared

    assert not forbidden, f"Estados inexistentes en SECOP declarados: {forbidden}"


def test_contract_state_literals_preserve_secop_capitalization():
    """
    Los estados de contrato deben ser literales exactos del dataset SECOP.
    """
    contract_states = set(collect_estado_values("contratos"))

    assert "En ejecución" in contract_states
    assert "terminado" in contract_states
    assert "cedido" in contract_states
    assert "Terminado" not in contract_states
    assert "Cedido" not in contract_states
