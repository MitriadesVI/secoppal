"""SEARCH-SH1: frozen dogfood regressions for semantic dimensions and money.

These cases intentionally exercise the public parser/follow-up/workflow paths.
They are the executable record of R1-R4 from the 2026-08 SEARCH-SH1 audit.
"""
from __future__ import annotations

import re

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.followup_engine import detect_and_merge
from app.core.orchestrator import SecopalWorkflow
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


@pytest.fixture
def router() -> QueryRouter:
    return QueryRouter()


def test_r1_convenios_is_not_inferred_as_topic(router: QueryRouter) -> None:
    parsed = router.parse("convenios de alcaldia de barranquilla en 2026")

    assert parsed.params["dataset"] == "procesos"
    assert parsed.params["fecha_desde"] == "2026-01-01"
    assert "BARRANQUILLA" in parsed.params["entidad_resolved"]
    assert "convenios" not in parsed.params.get("objeto", [])


def test_r2_convenio_and_regimen_especial_are_independent_dimensions(
    router: QueryRouter,
) -> None:
    parsed = router.parse(
        "MUESTRAME CONVENIOS DE REGIMEN ESPECIAL DE ALCALDIA DE "
        "BARRANQUILLA POR MAS DE 2000 MILLONES EN 2026"
    )

    assert parsed.params["modalidad_include_procedure_ids"] == [
        "regimen_especial",
        "regimen_especial_con_ofertas",
    ]
    assert parsed.params["valor_min"] == 2_000_000_000
    assert "convenios" not in parsed.params.get("objeto", [])
    assert "regimen_especial" not in parsed.params.get("contract_type_include", [])


def test_r3_same_year_money_delta_survives_every_deterministic_boundary(
    router: QueryRouter,
) -> None:
    base = router.parse(
        "contratos adulto mayor gobernacion de atlantico en 2026"
    ).params
    followup_text = "por mas de 100 millones en 2026"
    current = router.parse(followup_text).params
    trace: dict = {}

    intent, merged = detect_and_merge(
        followup_text,
        current,
        frame_from_params(base),
        mutation_trace=trace,
    )
    soql = SoQLBuilder().build(SoQLBuilder.CONTRATOS_DATASET, merged)

    assert current["valor_min"] == 100_000_000
    assert intent == "refine_filter"
    assert "valor_min" in trace["accepted_fields"]
    assert "valor_min" not in trace["rejected_fields"]
    assert merged["valor_min"] == 100_000_000
    assert merged["objeto"] == ["adulto_mayor"]
    assert "ATLANTICO" in merged["entidad_resolved"]
    assert merged["fecha_desde"] == "2026-01-01"
    assert "valor_del_contrato >= 100000000" in soql
    assert "precio_base >=" not in soql


def _contract_row(index: int, value: int) -> dict:
    return {
        "id_contrato": f"CID-{index:03d}",
        "referencia_del_contrato": f"REF-{index:03d}",
        "nombre_entidad": "DEPARTAMENTO DEL ATLANTICO**",
        "departamento": "Atlántico",
        "ciudad": "Barranquilla",
        "objeto_del_contrato": f"Atención integral adulto mayor {index}",
        "descripcion_del_proceso": "Programa para personas mayores",
        "tipo_de_contrato": "Prestación de servicios",
        "modalidad_de_contratacion": "Contratación directa",
        "valor_del_contrato": str(value),
        "estado_contrato": "En ejecución",
        "fecha_de_firma": "2026-06-01T00:00:00.000",
        "proveedor_adjudicado": f"PROVEEDOR {index}",
        "documento_proveedor": str(900_000_000 + index),
        "urlproceso": f"https://secop.gov.co/c/{index}",
    }


def test_r4_pagination_after_money_uses_the_filtered_universe(
    tmp_path, monkeypatch,
) -> None:
    high = [_contract_row(i, 130_000_000 - i * 1_000_000) for i in range(30)]
    low = [_contract_row(i + 30, 99_000_000 - i * 1_000_000) for i in range(16)]
    universe = high + low

    def selected(soql: str) -> list[dict]:
        return high if "valor_del_contrato >= 100000000" in soql else universe

    def query(_dataset_id: str, soql: str) -> list[dict]:
        rows = selected(soql)
        offset_match = re.search(r"OFFSET (\d+)", soql)
        limit_match = re.search(r"LIMIT (\d+)", soql)
        offset = int(offset_match.group(1)) if offset_match else 0
        limit = int(limit_match.group(1)) if limit_match else 50
        return rows[offset:offset + limit]

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *args, **kwargs: None,
    )
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = query
    workflow.secop_client.count = lambda _dataset_id, soql: len(selected(soql))
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None
    workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    chat_id = "search_sh1_r4"

    base = workflow.run_query(
        "contratos adulto mayor gobernacion de atlantico en 2026",
        channel="streamlit",
        chat_id=chat_id,
    )
    refined = workflow.run_query(
        "por mas de 100 millones en 2026",
        channel="streamlit",
        chat_id=chat_id,
    )
    next_page = workflow.run_query(
        "más resultados",
        channel="streamlit",
        chat_id=chat_id,
    )

    assert base["total_count"] == 46
    assert refined["total_count"] == 30
    assert refined["resolved_params"]["valor_min"] == 100_000_000
    assert all(int(row["valor_del_contrato"]) >= 100_000_000 for row in refined["results"])
    assert next_page["route_reason"] == "pagination_more"
    assert next_page["pagination_buffer_hit"] is True
    assert next_page["results"]
    assert all(int(row["valor_del_contrato"]) >= 100_000_000 for row in next_page["results"])
    assert not (
        {row["id_contrato"] for row in refined["rows"]}
        & {row["id_contrato"] for row in next_page["rows"]}
    )

