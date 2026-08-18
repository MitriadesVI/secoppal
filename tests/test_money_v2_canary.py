"""Money V2 / M9 — canario determinista por el workflow normal, sin red."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def _workflow() -> SecopalWorkflow:
    workflow = SecopalWorkflow(Settings())

    def _query(dataset_id: str, soql_query: str) -> list[dict]:
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
