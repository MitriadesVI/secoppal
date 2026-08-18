"""Money V2 / M9 — canario determinista por el workflow normal, sin red."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def _workflow(query_calls: list[tuple[str, str]] | None = None) -> SecopalWorkflow:
    workflow = SecopalWorkflow(Settings())

    def _query(dataset_id: str, soql_query: str) -> list[dict]:
        if query_calls is not None:
            query_calls.append((dataset_id, soql_query))
        if dataset_id == "jbjy-vk9h":
            return [{
                "referencia_del_contrato": "C-MONEY-V2",
                "nombre_entidad": "ENTIDAD CANARIO",
                "objeto_del_contrato": "ALIMENTACION",
                "valor_del_contrato": "3000000000",
                "estado_contrato": "En ejecución",
                "fecha_de_firma": "2026-01-15T00:00:00.000",
                "proveedor_adjudicado": "PROVEEDOR CANARIO",
                "documento_proveedor": "900123456",
                "urlproceso": "https://example.test/contract",
            }]
        return [{
            "referencia_del_proceso": "P-MONEY-V2",
            "nombre_del_procedimiento": "ALIMENTACION ESCOLAR",
            "entidad": "ENTIDAD CANARIO",
            "precio_base": "3000000000",
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-01-15T00:00:00.000",
            "urlproceso": "https://example.test/process",
        }]

    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = lambda dataset_id, soql_query: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]
    return workflow


@pytest.mark.parametrize(
    ("query", "predicate", "contractor"),
    [
        ("contratos de alimentacion de mas de 500 millones", "valor_del_contrato >= 500000000", None),
        ("contratos de alimentacion de mas de 1.500 millones", "valor_del_contrato >= 1500000000", None),
        ("contratos de alimentacion de mas de dos mil millones", "valor_del_contrato >= 2000000000", None),
        ("contratos de alimentacion del NIT 900123456", "documento_proveedor = '900123456'", ["900123456"]),
        ("contratos de alimentacion mayores a 1000 SMMLV en 2026", "valor_del_contrato >= 1750905000", None),
    ],
)
def test_money_paths_reach_expected_soql(
    query: str, predicate: str, contractor: list[str] | None
) -> None:
    result = _workflow().run_query(query, channel="streamlit")
    assert predicate in result["soql_query"]
    assert result["resolved_params"].get("contratista") == contractor
    assert result["total_count"] == 1


def test_smmlv_followup_reuses_previous_year_in_real_workflow() -> None:
    workflow = _workflow()
    chat_id = "money-v2-followup-canary"
    workflow.run_query(
        "contratos de alimentacion en 2025", channel="streamlit", chat_id=chat_id
    )
    result = workflow.run_query(
        "ahora mayores a 1500 SMMLV", channel="streamlit", chat_id=chat_id
    )
    assert result["followup"] is True
    assert "valor_del_contrato >= 2135250000" in result["soql_query"]
    metadata = result["resolved_params"]["money_metadata"]
    assert metadata["reference_year"] == 2025
    assert metadata["year_authority"] == "inherited_query_date_scope"


def test_p16_blocks_inherited_smmlv_after_multi_year_mutation_in_real_workflow() -> None:
    query_calls: list[tuple[str, str]] = []
    workflow = _workflow(query_calls)
    chat_id = "money-v2-p16-mutation-canary"
    workflow.run_query(
        "contratos de mantenimiento mayores a 1000 SMMLV en 2026",
        channel="streamlit",
        chat_id=chat_id,
    )
    calls_after_first_turn = len(query_calls)

    result = workflow.run_query(
        "ahora de 2024 a 2026", channel="streamlit", chat_id=chat_id
    )

    assert calls_after_first_turn == 1
    assert len(query_calls) == calls_after_first_turn
    assert result["needs_clarification"] is True
    assert result["query_plan"]["should_execute"] is False
    assert result["query_plan"]["reason"] == "smmlv_year_scope_unsupported"
    assert result["resolved_params"].get("valor_min") is None


def test_inherited_smmlv_rebases_to_new_year_in_real_workflow(monkeypatch) -> None:
    monkeypatch.setattr("app.core.query_router.current_colombian_year", lambda: 2026)
    workflow = _workflow()
    chat_id = "money-v2-smmlv-year-change-canary"
    workflow.run_query(
        "contratos de mantenimiento mayores a 1000 SMMLV",
        channel="streamlit",
        chat_id=chat_id,
    )

    result = workflow.run_query("solo 2025", channel="streamlit", chat_id=chat_id)

    assert result["needs_clarification"] is False
    assert "valor_del_contrato >= 1423500000" in result["soql_query"]
    assert result["resolved_params"]["money_metadata"]["reference_year"] == 2025


def test_2000_millones_followup_is_money_not_year_in_real_workflow() -> None:
    workflow = _workflow()
    chat_id = "money-v2-year-vs-money-canary"
    workflow.run_query(
        "contratos de mantenimiento mayores a 500 millones",
        channel="streamlit",
        chat_id=chat_id,
    )

    result = workflow.run_query(
        "ahora mayores a 2000 millones", channel="streamlit", chat_id=chat_id
    )

    assert result["followup"] is True
    assert result["followup_intent_type"] == "refine_filter"
    assert result["resolved_params"]["valor_min"] == 2_000_000_000
    assert "valor_del_contrato >= 2000000000" in result["soql_query"]


def test_cardinal_people_count_does_not_become_money_in_real_workflow() -> None:
    result = _workflow().run_query(
        "procesos de alimentacion para mil quinientos estudiantes",
        channel="streamlit",
    )

    assert result["resolved_params"].get("valor_min") is None
    assert result["resolved_params"].get("valor_max") is None
    assert "precio_base >=" not in result["soql_query"]
    assert "precio_base <=" not in result["soql_query"]
