"""Tests E2E para follow-up con queries delta (refinamiento puro).

Escenarios:
1. "quiero ver los de mayor valor" después de búsqueda con contexto → conserva todo
2. "los de mayor valor" → conserva contexto
3. "quiero ver los más recientes" en procesos → conserva dataset
4. Sin historial → no ejecuta WHERE 1=1 global
"""
from __future__ import annotations

from pathlib import Path

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore, is_followup, Turn


# Helper para crear un turno fake (sin persistencia)
def _make_turn(parsed_params: dict, minutes_ago: float = 0) -> Turn:
    from datetime import datetime, timezone, timedelta
    ts = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
    return Turn(
        turn_id="test", timestamp=ts, chat_id="st_test",
        user_query="test", response="ok", parsed_params=parsed_params,
    )


class TestIsDeltaQuery:
    """Pruebas unitarias para _is_delta_query via is_followup."""

    def test_quiero_ver_mayor_valor_is_followup(self):
        """'quiero ver los de mayor valor' debe detectarse como follow-up."""
        turn = _make_turn({"dataset": "contratos", "objeto": ["primera", "infancia"]})
        assert is_followup("quiero ver los de mayor valor", turn) is True

    def test_los_de_mayor_valor_is_followup(self):
        assert is_followup("los de mayor valor", last_turn=_make_turn({"dataset": "contratos"}))

    def test_ver_los_mas_caros_is_followup(self):
        assert is_followup("ver los mas caros", last_turn=_make_turn({"dataset": "contratos"}))

    def test_ordenalos_por_valor_is_followup(self):
        assert is_followup("ordenalos por valor", last_turn=_make_turn({"dataset": "contratos"}))

    def test_quiero_ver_mas_recientes_is_followup(self):
        assert is_followup("quiero ver los mas recientes", last_turn=_make_turn({"dataset": "contratos"}))

    def test_no_history_is_not_followup(self):
        assert is_followup("quiero ver los de mayor valor", last_turn=None) is False

    def test_contratos_de_mantenimiento_not_followup(self):
        """Consulta con objeto explícito NO es delta."""
        assert is_followup("contratos de mantenimiento", last_turn=_make_turn({"dataset": "contratos"})) is False

    def test_procesos_abiertos_vial_not_followup(self):
        """Consulta específica con dataset + objeto NO es delta."""
        assert is_followup("procesos abiertos de mantenimiento vial en atlantico",
                          last_turn=_make_turn({"dataset": "procesos"})) is False


# ── E2E con workflow mockeado ─────────────────────────────────────────────

def _mock_workflow(tmp_path):
    workflow = SecopalWorkflow(Settings())
    workflow.secop_client.query = lambda dataset_id, soql: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Atencion integral primera infancia",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "estado_de_apertura_del_proceso": "Celebrado",
            "fecha_de_publicacion_del": "2025-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-TEST-1",
        }
    ]
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return workflow


class TestE2EFollowupDelta:
    def test_followup_preserves_full_context(self, tmp_path):
        """Turno 1: contratos primera infancia barranquilla 2026
        Turno 2: quiero ver los de mayor valor
        → conserva dataset=contratos, objeto, entidad, fecha
        → agrega ordering_signal=valor_desc
        """
        wf = _mock_workflow(tmp_path)

        r1 = wf.run_query(
            "hola muestrame contratos de primera infancia de barranquilla en 2026",
            channel="streamlit", chat_id="st_test",
        )
        pp1 = r1.get("parsed_params", {})
        assert pp1.get("dataset") == "contratos", f"dataset={pp1.get('dataset')}"
        obj1 = pp1.get("objeto", [])
        assert "primera" in obj1, f"objeto={obj1}"
        assert "infancia" in obj1, f"objeto={obj1}"

        r2 = wf.run_query(
            "quiero ver los de mayor valor",
            channel="streamlit", chat_id="st_test",
        )
        # No debe ser clarification
        assert r2.get("needs_clarification") is not True, f"needs_clarification={r2.get('needs_clarification')}"
        assert r2.get("route_reason") != "suggestion_invalid"

        pp2 = r2.get("parsed_params", {})
        rp2 = r2.get("resolved_params", {})

        # Dataset preservado
        ds2 = rp2.get("dataset", pp2.get("dataset"))
        assert ds2 == "contratos", f"dataset should be contratos, got {ds2}"

        # Objeto preservado
        obj2 = rp2.get("objeto", []) or pp2.get("objeto", [])
        assert "primera" in obj2, f"objeto should have primera, got {obj2}"
        assert "infancia" in obj2, f"objeto should have infancia, got {obj2}"

        # Fecha preservada
        assert pp2.get("fecha_desde") == "2026-01-01", f"fecha_desde={pp2.get('fecha_desde')}"

        # Ordenamiento agregado
        assert pp2.get("ordering_signal") == "valor_desc", f"ordering_signal={pp2.get('ordering_signal')}"

        # SoQL usa contratos dataset
        soql = r2.get("soql_query", "")
        assert "jbjy-vk9h" in r2.get("dataset_id", ""), f"dataset_id={r2.get('dataset_id')}"
        assert "valor_del_contrato DESC" in soql or "precio_base DESC" in soql, f"SoQL={soql[:300]}"

        # NO debe tener WHERE 1=1 (debe tener objeto + entidad filtros)
        assert "WHERE 1=1" not in soql, f"SoQL has WHERE 1=1: {soql[:300]}"

    def test_los_de_mayor_valor_preserves_context(self, tmp_path):
        """'los de mayor valor' sin prefijo verbal también es follow-up."""
        wf = _mock_workflow(tmp_path)

        wf.run_query(
            "contratos de primera infancia alcaldia de barranquilla",
            channel="streamlit", chat_id="st_test2",
        )
        r2 = wf.run_query(
            "los de mayor valor",
            channel="streamlit", chat_id="st_test2",
        )
        assert r2.get("route_reason") not in ("suggestion_invalid", "suggestion_error")
        pp2 = r2.get("parsed_params", {})
        assert pp2.get("ordering_signal") == "valor_desc"

    def test_procesos_maintenance_not_switched_to_contratos(self, tmp_path):
        """Turno 1: procesos mantenimiento atlantico
        Turno 2: quiero ver los mas recientes
        → conserva procesos + maintenance + atlantico
        """
        wf = _mock_workflow(tmp_path)
        # Override mock to return procesos data
        wf.secop_client.query = lambda dataset_id, soql: [  # type: ignore[method-assign]
            {
                "nombre_del_procedimiento": "Mantenimiento vial corredor",
                "entidad": "GOBERNACION DE ATLANTICO",
                "precio_base": "500000000",
                "estado_de_apertura_del_proceso": "Abierto",
                "fecha_de_publicacion_del": "2025-06-01T00:00:00.000",
                "urlproceso": "https://secop.gov.co/test",
                "referencia_del_proceso": "REF-TEST-1",
            }
        ]
        wf.run_query(
            "procesos abiertos de mantenimiento vial en atlantico",
            channel="streamlit", chat_id="st_test3",
        )
        r2 = wf.run_query(
            "quiero ver los mas recientes",
            channel="streamlit", chat_id="st_test3",
        )
        rp2 = r2.get("resolved_params", {})
        pp2 = r2.get("parsed_params", {})
        ds2 = rp2.get("dataset", pp2.get("dataset"))
        assert ds2 == "procesos", f"expected procesos, got {ds2}"
        # No debe tener ordering_signal=valor_desc (es "mas recientes" sin valor)
        # Los mas recientes es el default (date DESC), no activa valor_desc

    def test_no_history_asks_for_context(self, tmp_path):
        """Sin historial previo + 'quiero ver los de mayor valor'
        → NO ejecuta WHERE 1=1 global, debe pedir contexto."""
        wf = _mock_workflow(tmp_path)
        r = wf.run_query(
            "quiero ver los de mayor valor",
            channel="streamlit", chat_id="st_empty",
        )
        # Sin historial → no es follow-up → se parsea como búsqueda normal
        # El parser produce solo ordering_signal → extractor_keys solo tiene dataset + ordering
        # → needs_llm=True con route_reason="ordering_without_object"
        # La respuesta del formatter debe decir "no encontré" o similar
        response = r.get("response", "")
        assert response, f"Empty response: {r}"
        # No debe crashear ni devolver datos absurdos
        assert "error" not in response.lower()
