"""Canarios del cierre temporal para grupos de compradores grandes.

ADR-074 prohíbe ejecutar un solo miembro cuando el conjunto declarado excede
la capacidad segura actual.  Estos tests congelan la política temporal: los
grupos de hasta 15 miembros conservan el OR actual; los mayores se bloquean con
estado estructurado y sin tocar SECOP.  La estrategia real de ejecución queda
fuera de este cambio (Etapa 3A).
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.soql_builder import (
    BuyerCandidateSetExceedsSafeLimitError,
    SoQLBuilder,
)


EXPECTED_REASON = "candidate_set_exceeds_safe_limit"
SAFE_LIMIT = 15


def _process_row(entity: str) -> dict:
    return {
        "id_del_proceso": "CO1.TEST.ICBF",
        "referencia_del_proceso": "ICBF-TEST",
        "nombre_del_procedimiento": "Proceso de prueba",
        "entidad": entity,
        "precio_base": "1000000",
        "estado_de_apertura_del_proceso": "Abierto",
        "fecha_de_publicacion_del": "2026-09-01T00:00:00.000",
        "urlproceso": "https://community.secop.gov.co/Public/Tendering/NoticeDetail/Index?id=1",
    }


def _workflow(tmp_path) -> tuple[SecopalWorkflow, MagicMock]:
    workflow = SecopalWorkflow(Settings(
        deepseek_api_key=None,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "secoppal.db",
    ))
    query = MagicMock(return_value=[_process_row("ICBF REGIONAL ANTIOQUIA")])
    workflow.secop_client.query = query  # type: ignore[method-assign]
    workflow.secop_client.count = MagicMock(return_value=1)  # type: ignore[method-assign]
    workflow.secop_client.aggregate = MagicMock(return_value=[])  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]
    return workflow, query


@pytest.mark.parametrize("text", ["procesos del icbf", "icbf nacional"])
def test_icbf_national_executes_the_complete_accredited_registry(
    tmp_path, text: str,
) -> None:
    workflow, query = _workflow(tmp_path)

    result = workflow.run_query(text, channel="streamlit")

    query.assert_called_once()
    assert result["needs_clarification"] is False
    assert result["query_plan"]["should_execute"] is True
    from app.core.buyer_groups import get_groups
    group = get_groups()["icbf_national"]
    assert len(group.members) == 33
    assert all(name in result["soql_query"] for name in group.names_for('p6dx-8zbt'))
    assert not result["resolved_params"].get("entidad_in_candidates")
    assert "sede" in result["response"].lower()
    assert "32 regionales" in result["response"].lower()
    assert "regional" in result["response"].lower()


def test_icbf_regional_still_executes(tmp_path) -> None:
    workflow, query = _workflow(tmp_path)

    result = workflow.run_query("icbf en antioquia", channel="streamlit")

    query.assert_called()
    assert result["needs_clarification"] is False
    assert result["resolved_params"].get("buyer_group_execution_blocked") is not True
    assert "Antioquia" in result["soql_query"]
    assert "ICBF" in result["soql_query"]


def test_icbf_macro_region_within_limit_executes_every_member(tmp_path) -> None:
    workflow, query = _workflow(tmp_path)

    result = workflow.run_query(
        "procesos del icbf en costa atlántica", channel="streamlit"
    )

    query.assert_called()
    assert result["needs_clarification"] is False
    candidates = result["resolved_params"]["entidad_in_candidates"]
    assert len(candidates) == 8
    assert all(candidate in result["soql_query"] for candidate in candidates)


def test_candidate_set_at_safe_limit_keeps_complete_or() -> None:
    members = [f"BUYER UNIT {index:02d}" for index in range(SAFE_LIMIT)]

    query = SoQLBuilder().build(
        SoQLBuilder.PROCESOS_DATASET,
        {"entidad_in_candidates": members},
    )

    assert all(member in query for member in members)


def test_builder_rejects_candidate_set_above_safe_limit() -> None:
    members = [f"BUYER UNIT {index:02d}" for index in range(SAFE_LIMIT + 1)]

    with pytest.raises(
        BuyerCandidateSetExceedsSafeLimitError,
        match=EXPECTED_REASON,
    ) as exc_info:
        SoQLBuilder().build(
            SoQLBuilder.PROCESOS_DATASET,
            {"entidad_in_candidates": members},
        )
    assert exc_info.value.candidate_count == SAFE_LIMIT + 1
    assert exc_info.value.safe_limit == SAFE_LIMIT
