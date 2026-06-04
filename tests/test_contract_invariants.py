"""Unit tests for pure SECOP contract invariants."""

from __future__ import annotations

from copy import deepcopy

import app.core.contract_invariants as ci
from app.core._followup_constants import SCOPE_TOPIC_KEYS


def test_apply_signed_dataset_invariant_forces_contracts():
    params = {"dataset": "procesos", "objeto": ["mantenimiento"]}

    cleaned, flags = ci.apply_signed_dataset_invariant(
        "contratos firmados de mantenimiento",
        params,
    )

    assert cleaned["dataset"] == "contratos"
    assert cleaned["objeto"] == ["mantenimiento"]
    assert flags == ["firmado_dataset_forced"]
    assert params["dataset"] == "procesos"


def test_purge_invalid_contract_states_string_and_list_case_insensitive():
    params = {
        "estado": "Firmado",
        "estado_contrato": ["Celebrado", "En ejecución", "firmada"],
        "estado_field": "estado_contrato",
    }

    cleaned, flags = ci.purge_invalid_contract_states(params)

    assert "estado" not in cleaned
    assert cleaned["estado_contrato"] == ["En ejecución"]
    assert cleaned["estado_field"] == "estado_contrato"
    assert flags == ["invalid_estado_purged"]


def test_purge_invalid_contract_states_preserves_real_states():
    params = {
        "estado": "Cerrado",
        "estado_contrato": ["En ejecución", "Modificado", "Prorrogado", "Cerrado"],
        "estado_field": "estado_contrato",
    }
    original = deepcopy(params)

    cleaned, flags = ci.purge_invalid_contract_states(params)

    assert cleaned == original
    assert flags == []


def test_firmados_en_ejecucion_preserves_active_states():
    params = {
        "dataset": "contratos",
        "estado_contrato": ["Firmado", "En ejecución", "Modificado", "Prorrogado"],
        "estado_field": "estado_contrato",
    }

    cleaned, flags = ci.apply_signed_dataset_invariant(
        "contratos firmados que estén en ejecución",
        params,
    )

    assert "Firmado" not in cleaned["estado_contrato"]
    assert cleaned["estado_contrato"] == ["En ejecución", "Modificado", "Prorrogado"]
    assert "En ejecución" in cleaned["estado_contrato"]
    assert flags == ["invalid_estado_purged"]


def test_apply_opportunity_invariant_forces_open_processes_and_purges_closed_states():
    params = {
        "dataset": "contratos",
        "estado": "Cerrado",
        "estado_contrato": ["Cerrado", "En ejecución"],
        "estado_field": "estado_contrato",
        "objeto": ["obras"],
    }

    cleaned, flags = ci.apply_opportunity_invariant(
        "quiero presentarme a obras",
        params,
    )

    assert cleaned["dataset"] == "procesos"
    assert cleaned["intent_type"] == "opportunity_search"
    assert cleaned["estado_family"] == "oferta_abierta"
    assert "estado" not in cleaned
    assert cleaned["estado_contrato"] == ["En ejecución"]
    assert flags == ["opportunity_enforced"]


def test_apply_opportunity_invariant_is_idempotent():
    params = {
        "dataset": "procesos",
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "objeto": ["mantenimiento"],
    }

    once, _ = ci.apply_opportunity_invariant("quiero presentarme", params)
    twice, second_flags = ci.apply_opportunity_invariant("quiero presentarme", once)

    assert twice == once
    assert second_flags == []


def test_enforce_oferta_abierta_not_contracts_selects_processes():
    cleaned, flags = ci.enforce_oferta_abierta_not_contracts({
        "dataset": "contratos",
        "estado_family": "oferta_abierta",
    })

    assert cleaned["dataset"] == "procesos"
    assert cleaned["intent_type"] == "opportunity_search"
    assert flags == ["oferta_abierta_not_contracts"]


def test_has_substantial_filter_uses_canonical_scope_topic_keys():
    assert ci.SCOPE_TOPIC_KEYS is SCOPE_TOPIC_KEYS
    assert ci.has_substantial_filter({"fecha_desde": "2026-05-01"}) is False
    assert ci.has_substantial_filter({"valor_min": 10, "ordering_signal": "valor_desc"}) is False
    assert ci.has_substantial_filter({"objeto": ["aseo"]}) is True
    assert ci.has_substantial_filter({"departamento_resolved": "Boyacá"}) is True
