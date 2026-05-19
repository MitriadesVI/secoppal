"""Tests AQ-001A — aggregate_sum analytical queries (versión reforzada)."""

from __future__ import annotations

from unittest.mock import MagicMock

from app.config import Settings
from app.core.analytics import (
    build_aggregate_sum_query,
    detect_analytical_intent,
    format_aggregate_sum_response,
    has_analytics_scope,
    maybe_handle_analytical_query,
)
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.soql_builder import SoQLBuilder


# ============================================================
# A. build_aggregate_sum_query — Contratos
# ============================================================
class TestBuildAggregateSumQueryContratos:
    def test_contratos_genera_sum_valor_del_contrato(self):
        builder = SoQLBuilder()
        params = {
            "objeto": ["mantenimiento"],
            "ciudad": "Barranquilla",
            "fecha_desde": "2025-01-01",
        }
        soql = build_aggregate_sum_query(builder, "jbjy-vk9h", params)

        assert "SUM(valor_del_contrato)" in soql
        assert "COUNT(*)" in soql
        assert "WHERE" in soql
        assert "1=1" not in soql
        assert "objeto_del_contrato" in soql or "mantenimiento" in soql.lower()


# ============================================================
# B. build_aggregate_sum_query — Procesos
# ============================================================
class TestBuildAggregateSumQueryProcesos:
    def test_procesos_genera_sum_precio_base(self):
        builder = SoQLBuilder()
        params = {
            "objeto": ["consultoría"],
            "fecha_desde": "2025-01-01",
        }
        soql = build_aggregate_sum_query(builder, "p6dx-8zbt", params)

        assert "SUM(precio_base)" in soql
        assert "WHERE" in soql
        assert "1=1" not in soql


# ============================================================
# C. Guard sin scope
# ============================================================
class TestGuardSinScope:
    def test_sin_scope_retorna_needs_clarification(self):
        mock_client = MagicMock()

        result = maybe_handle_analytical_query(
            user_query="cuánto se contrató",
            params={"dataset": "contratos"},
            dataset_id="jbjy-vk9h",
            secop_client=mock_client,
            soql_builder=SoQLBuilder(),
        )

        assert result is not None
        assert result.get("needs_clarification") is True
        mock_client.aggregate.assert_not_called()


# ============================================================
# D. Format response
# ============================================================
class TestFormatAggregateSumResponse:
    def test_formato_con_valor_y_cantidad(self):
        rows = [{"total_value": 123456789, "total_count": 7}]
        params = {"ciudad": "Barranquilla"}
        response = format_aggregate_sum_response(rows, params, "jbjy-vk9h")

        assert "123.5 millones" in response or "123 millones" in response
        assert "7 registros" in response
        assert "lista de contratos" not in response.lower()
        assert "Universo analizado" in response


# ============================================================
# E. No regresión
# ============================================================
class TestNoRegresion:
    def test_queries_normales_no_activan_analytical_intent(self):
        assert detect_analytical_intent("contratos de mantenimiento en Barranquilla") is None
        assert detect_analytical_intent("mostrar contratos en Atlántico") is None
        assert detect_analytical_intent("procesos de obra en Bogotá") is None


# ============================================================
# F. Helper con aggregate mock
# ============================================================
class TestMaybeHandleAnalyticalQueryConMock:
    def test_llama_aggregate_y_retorna_resultado(self):
        mock_client = MagicMock()
        mock_client.aggregate.return_value = [
            {"total_value": 1000000, "total_count": 2}
        ]

        result = maybe_handle_analytical_query(
            user_query="cuánto se contrató en Barranquilla",
            params={"ciudad": "Barranquilla", "objeto": ["mantenimiento"]},
            dataset_id="jbjy-vk9h",
            secop_client=mock_client,
            soql_builder=SoQLBuilder(),
        )

        assert result is not None
        assert result["analytical_intent"] == "aggregate_sum"
        assert result["results"] == []
        assert "1 millón" in result["response"] or "$1 millón" in result["response"]
        mock_client.aggregate.assert_called_once()


# ============================================================
# Tests de integración end-to-end con SecopalWorkflow
# ============================================================
def _make_workflow(tmp_path):
    """Construye un SecopalWorkflow aislado en tmp_path."""
    wf = SecopalWorkflow(Settings(secop_feedback_path=tmp_path / "feedback.jsonl"))
    wf.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    # Neutralizar narrator y silenciar logs
    wf.narrator.narrate = lambda **kw: None  # type: ignore[method-assign]
    return wf


class TestAnalyticalQueriesIntegration:
    def test_query_directa_aggregate_sum(self, tmp_path):
        """Query directa analítica produce respuesta agregada visible al usuario."""
        wf = _make_workflow(tmp_path)
        wf.secop_client.aggregate = MagicMock(
            return_value=[{"total_value": 500_000_000, "total_count": 12}]
        )
        wf.secop_client.query = MagicMock()
        wf.secop_client.count = MagicMock()

        result = wf.run_query(
            "cuánto se contrató en mantenimiento en Barranquilla en 2026",
            channel="streamlit",
        )

        assert result.get("analytical_intent") == "aggregate_sum"
        assert result.get("route_reason") == "analytics_aggregate_sum"
        assert result.get("results") == []
        assert result.get("needs_clarification") is False
        # La respuesta visible al usuario debe contener el agregado formateado.
        assert "500 millones" in result["response"]
        assert "12 registros" in result["response"]
        # No debe haberse caído al camino tabular.
        wf.secop_client.query.assert_not_called()
        wf.secop_client.count.assert_not_called()
        wf.secop_client.aggregate.assert_called_once()

    def test_guard_sin_scope(self, tmp_path, monkeypatch):
        """Query analítica sin scope pide clarificación sin llamar a SECOP.

        Forzamos el parser a devolver params vacíos para aislar la integración:
        si el parser asignara "cuanto" como objeto, el guard de scope pasaría
        (eso es un caso del parser, no del módulo analytics).
        """
        from app.core.query_router import ParsedQuery

        wf = _make_workflow(tmp_path)
        wf.secop_client.aggregate = MagicMock()
        wf.secop_client.query = MagicMock()
        wf.secop_client.count = MagicMock()
        monkeypatch.setattr(
            wf.query_router,
            "parse",
            lambda q: ParsedQuery(
                params={"dataset": "contratos"},
                needs_llm=False,
                route_reason="heuristic_only",
            ),
        )

        result = wf.run_query("cuánto se contrató", channel="streamlit")

        assert result.get("needs_clarification") is True
        assert result.get("analytical_intent") == "aggregate_sum"
        assert result.get("route_reason") == "analytics_aggregate_sum_no_scope"
        wf.secop_client.aggregate.assert_not_called()
        wf.secop_client.query.assert_not_called()
        wf.secop_client.count.assert_not_called()

    def test_no_regresion_query_normal(self, tmp_path):
        """Query normal NO activa ruta analítica y sigue el camino tabular."""
        wf = _make_workflow(tmp_path)
        wf.secop_client.aggregate = MagicMock()
        wf.secop_client.query = MagicMock(return_value=[
            {
                "id_contrato": "C-1",
                "objeto_del_contrato": "Mantenimiento vial",
                "nombre_entidad": "Distrito de Barranquilla",
                "valor_del_contrato": 100_000_000,
                "fecha_de_firma": "2026-02-01",
                "estado_contrato": "En ejecución",
            }
        ])
        wf.secop_client.count = MagicMock(return_value=1)

        result = wf.run_query(
            "contratos de mantenimiento en Barranquilla",
            channel="streamlit",
        )

        assert result.get("analytical_intent") in ("", None)
        assert result.get("route_reason") != "analytics_aggregate_sum"
        wf.secop_client.aggregate.assert_not_called()
        # Camino tabular sí debió correr.
        assert wf.secop_client.query.called

    def test_followup_analitico_hereda_contexto(self, tmp_path):
        """Follow-up analítico hereda topic/scope del turno previo."""
        wf = _make_workflow(tmp_path)
        wf.secop_client.query = MagicMock(return_value=[
            {
                "id_contrato": "C-1",
                "objeto_del_contrato": "Mantenimiento vial",
                "nombre_entidad": "Distrito de Barranquilla",
                "valor_del_contrato": 100_000_000,
                "fecha_de_firma": "2026-02-01",
                "estado_contrato": "En ejecución",
            }
        ])
        wf.secop_client.count = MagicMock(return_value=1)
        wf.secop_client.aggregate = MagicMock(
            return_value=[{"total_value": 750_000_000, "total_count": 8}]
        )

        chat = "st_aq001_followup"
        wf.run_query(
            "contratos de mantenimiento en Barranquilla",
            channel="streamlit",
            chat_id=chat,
        )
        result = wf.run_query("cuánto suma", channel="streamlit", chat_id=chat)

        assert result.get("analytical_intent") == "aggregate_sum"
        assert result.get("route_reason") == "analytics_aggregate_sum"
        assert "750 millones" in result["response"]
        assert "8 registros" in result["response"]
        # El follow-up debió heredar el scope/topic del turno anterior.
        resolved = result.get("resolved_params", {})
        assert resolved.get("ciudad") in ("Barranquilla", "barranquilla") or \
               "mantenimiento" in str(resolved.get("objeto", []))
        wf.secop_client.aggregate.assert_called_once()

    def test_followup_analitico_plural_hereda_contexto(self, tmp_path):
        """CORPUS-AN003: "cuánto suman" (plural) hereda contexto igual que el singular."""
        wf = _make_workflow(tmp_path)
        wf.secop_client.query = MagicMock(return_value=[
            {
                "id_contrato": "C-1",
                "objeto_del_contrato": "Primera infancia",
                "nombre_entidad": "Distrito de Barranquilla",
                "valor_del_contrato": 100_000_000,
                "fecha_de_firma": "2026-02-01",
                "estado_contrato": "En ejecución",
            }
        ])
        wf.secop_client.count = MagicMock(return_value=1)
        wf.secop_client.aggregate = MagicMock(
            return_value=[{"total_value": 1_200_000_000, "total_count": 15}]
        )

        chat = "st_an003_plural"
        wf.run_query(
            "contratos de primera infancia en Barranquilla 2026",
            channel="streamlit",
            chat_id=chat,
        )
        result = wf.run_query("cuanto suman", channel="streamlit", chat_id=chat)

        assert result.get("analytical_intent") == "aggregate_sum"
        assert result.get("route_reason") == "analytics_aggregate_sum"
        assert result.get("needs_clarification") is False
        resolved = result.get("resolved_params", {})
        # Topic heredado de "primera infancia" — "cuanto/suman" no deben quedar como objeto.
        objeto_blob = str(resolved.get("objeto", []))
        assert "primera" in objeto_blob.lower(), f"objeto perdido: {objeto_blob}"
        assert "cuanto" not in objeto_blob.lower(), f"cuanto se coló en objeto: {objeto_blob}"
        assert "suman" not in objeto_blob.lower(), f"suman se coló en objeto: {objeto_blob}"
        # Scope heredado.
        assert (resolved.get("ciudad") or "").lower() == "barranquilla" \
               or "BARRANQUILLA" in (resolved.get("entidad_resolved") or "")
        wf.secop_client.aggregate.assert_called_once()

    def test_followup_analitico_sin_contexto_pide_clarificacion(self, tmp_path):
        """CORPUS-AN003 / SAFE-SOQL: "cuánto suma" como primer turno NO consulta SECOP.

        Sin scope/topic, ni `maybe_handle_analytical_query` ni la ruta tabular
        deben emitir consultas. Se devuelve needs_clarification.
        """
        wf = _make_workflow(tmp_path)
        wf.secop_client.aggregate = MagicMock()
        wf.secop_client.query = MagicMock()
        wf.secop_client.count = MagicMock()

        result = wf.run_query("cuánto suman", channel="streamlit")

        assert result.get("needs_clarification") is True
        # La ruta analítica debió detectar la intención y rechazar por falta de scope.
        assert result.get("analytical_intent") == "aggregate_sum"
        assert result.get("route_reason") == "analytics_aggregate_sum_no_scope"
        wf.secop_client.aggregate.assert_not_called()
        wf.secop_client.query.assert_not_called()
        wf.secop_client.count.assert_not_called()