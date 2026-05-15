"""Tests E2E: follow-up robusto post-merge — delta queries conservan contexto."""
from __future__ import annotations

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore


def _make_workflow(tmp_path):
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = lambda ds, soql: [  # type: ignore[method-assign]
        {
            "nombre_del_procedimiento": "Atencion integral primera infancia",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "estado_de_apertura_del_proceso": "Celebrado",
            "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-1",
        }
    ]
    wf.secop_client.count = lambda ds, soql: 1  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    return wf


class TestFollowupPostMerge:
    """Test 1: flujo completo — delta query conserva todo el contexto."""

    def test_full_context_preserved(self, tmp_path):
        wf = _make_workflow(tmp_path)

        # ── Turno 1 ──────────────────────────────────────────────────
        r1 = wf.run_query(
            "contratos de primera infancia de barranquilla en 2026",
            channel="streamlit", chat_id="st_test",
        )
        pp1 = r1.get("parsed_params", {})
        assert pp1.get("dataset") == "contratos", f"dataset={pp1.get('dataset')}"
        assert "primera" in pp1.get("objeto", [])
        assert "infancia" in pp1.get("objeto", [])
        assert pp1.get("ciudad") == "Barranquilla" or "DISTRITO" in str(pp1.get("entidad_resolved", "")).upper()
        assert pp1.get("fecha_desde") == "2026-01-01", f"fecha_desde={pp1.get('fecha_desde')}"
        assert pp1.get("fecha_hasta") == "2026-12-31", f"fecha_hasta={pp1.get('fecha_hasta')}"

        # ── Turno 2 (follow-up delta) ────────────────────────────────
        r2 = wf.run_query(
            "quiero ver los de mayor valor",
            channel="streamlit", chat_id="st_test",
        )

        # No debe ser clarification
        assert r2.get("needs_clarification") is not True, f"needs_clarification={r2.get('needs_clarification')}"
        assert r2.get("route_reason") not in ("suggestion_invalid", "suggestion_error")

        pp2 = r2.get("parsed_params", {})
        rp2 = r2.get("resolved_params", {})

        # Dataset conservado (contratos)
        ds2 = rp2.get("dataset", pp2.get("dataset"))
        assert ds2 == "contratos", f"dataset should be contratos, got {ds2}"

        # Objeto conservado
        obj2 = rp2.get("objeto", []) or pp2.get("objeto", [])
        assert "primera" in obj2, f"objeto should have primera, got {obj2}"
        assert "infancia" in obj2, f"objeto should have infancia, got {obj2}"

        # Ciudad/entidad conservada
        er2 = rp2.get("entidad_resolved", pp2.get("entidad_resolved", ""))
        c2 = rp2.get("ciudad", pp2.get("ciudad", ""))
        assert "Barranquilla" in str(er2) or "Barranquilla" in str(c2), (
            f"neither ciudad nor entidad_resolved has Barranquilla: ciudad={c2}, er={er2}"
        )

        # Fechas conservadas
        assert pp2.get("fecha_desde") == "2026-01-01", f"fecha_desde={pp2.get('fecha_desde')}"
        assert pp2.get("fecha_hasta") == "2026-12-31", f"fecha_hasta={pp2.get('fecha_hasta')}"

        # Ordenamiento agregado
        assert pp2.get("ordering_signal") == "valor_desc", f"ordering_signal={pp2.get('ordering_signal')}"

        # needs_llm debe ser False (el merge ya tiene contexto)
        # No podemos probar needs_llm directamente porque se actualiza en apply_context
        # pero verificamos que NO paso por llm_parse (route_reason no debe ser heuristic_plus_llm)
        rr = r2.get("route_reason", "")
        assert rr != "heuristic_plus_llm", f"route_reason should not be heuristic_plus_llm: {rr}"

        # SoQL
        soql = r2.get("soql_query", "")
        assert "jbjy-vk9h" in r2.get("dataset_id", ""), f"dataset_id={r2.get('dataset_id')}"
        assert "valor_del_contrato DESC" in soql, f"SoQL missing valor_del_contrato DESC: {soql[:300]}"
        assert "WHERE 1=1" not in soql, f"SoQL has WHERE 1=1: {soql[:300]}"

    """Test 2: sin historial + delta query → pide contexto."""

    def test_no_history_asks_for_context(self, tmp_path):
        wf = _make_workflow(tmp_path)
        r = wf.run_query(
            "quiero ver los de mayor valor",
            channel="streamlit", chat_id="st_no_history",
        )
        # Sin historial previo, pero con chat_id, conv_store devuelve []
        # → is_followup retorna False (no hay last_turn)
        # → no apply_context, no merge
        # → el parser produce solo ordering_signal
        # → llega a parse_query → extract_keys = {dataset, ordering_signal}
        # → route_reason = "ordering_without_object" o "heuristic_only"
        resp = r.get("response", "")
        assert resp, f"empty response: {r}"
        # No debe crashear ni dar resultados absurdos
        assert "error" not in resp.lower(), f"response has error: {resp[:200]}"

    """Test 3: procesos + mantenimiento → conserva dataset."""

    def test_procesos_not_switched(self, tmp_path):
        wf = _make_workflow(tmp_path)
        wf.secop_client.query = lambda ds, soql: [  # type: ignore[method-assign]
            {
                "nombre_del_procedimiento": "Mantenimiento vial corredor",
                "entidad": "GOBERNACION DE ATLANTICO",
                "precio_base": "500000000",
                "estado_de_apertura_del_proceso": "Abierto",
                "fecha_de_publicacion_del": "2025-06-01T00:00:00.000",
                "urlproceso": "https://secop.gov.co/test",
                "referencia_del_proceso": "REF-1",
            }
        ]
        r1 = wf.run_query(
            "procesos abiertos de mantenimiento vial en atlantico",
            channel="streamlit", chat_id="st_test3",
        )
        pp1 = r1.get("parsed_params", {})
        assert pp1.get("dataset") == "procesos", f"turno1 dataset={pp1.get('dataset')}"

        r2 = wf.run_query(
            "quiero ver los mas recientes",
            channel="streamlit", chat_id="st_test3",
        )
        pp2 = r2.get("parsed_params", {})
        rp2 = r2.get("resolved_params", {})
        ds2 = rp2.get("dataset", pp2.get("dataset"))
        assert ds2 == "procesos", f"dataset should stay procesos, got {ds2}"
        soql = r2.get("soql_query", "")
        assert "WHERE 1=1" not in soql, f"SoQL has WHERE 1=1: {soql[:300]}"
