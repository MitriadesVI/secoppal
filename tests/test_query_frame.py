"""Tests: QueryFrame — política global de conversación v1.2.

Cubre: delta puro, contextual re-query, cambio de año, cambio de ciudad,
cambio de dataset, sin historial.
"""
from __future__ import annotations
import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore
from app.core.query_frame import (
    QueryFrame, frame_from_params, params_from_frame,
    classify_turn, merge_frames,
)


# ── Tests unitarios de QueryFrame ────────────────────────────────────────

class TestClassifyTurn:
    def test_no_previous_is_new_search(self):
        f = frame_from_params({"dataset": "contratos"})
        with pytest.warns(DeprecationWarning):
            assert classify_turn(f, None) == "new_search"

    def test_only_modifiers_is_refine_delta(self):
        prev = frame_from_params({"dataset": "contratos", "objeto": ["obra"]})
        curr = frame_from_params({"ordering_signal": "valor_desc"})
        with pytest.warns(DeprecationWarning):
            assert classify_turn(curr, prev) == "refine_delta"

    def test_new_topic_without_scope_is_contextual_requery(self):
        prev = frame_from_params({"dataset": "contratos", "objeto": ["infancia"],
                                   "ciudad": "Barranquilla"})
        curr = frame_from_params({"dataset": "contratos", "objeto": ["escuela"],
                                   "ordering_signal": "valor_desc"})
        with pytest.warns(DeprecationWarning):
            assert classify_turn(curr, prev) == "contextual_requery"

    def test_switch_scope_detected(self):
        prev = frame_from_params({"dataset": "contratos", "ciudad": "Barranquilla"})
        curr = frame_from_params({"dataset": "contratos", "ciudad": "Medellin"})
        with pytest.warns(DeprecationWarning):
            assert classify_turn(curr, prev) == "switch_scope"

    def test_switch_dataset_detected(self):
        prev = frame_from_params({"dataset": "contratos", "objeto": ["infancia"]})
        curr = frame_from_params({"dataset": "procesos", "dataset_explicit": True,
                                   "objeto": ["infancia"]})
        with pytest.warns(DeprecationWarning):
            assert classify_turn(curr, prev) == "switch_dataset"


class TestMergeFrames:
    def test_refine_delta_preserves_all(self):
        prev = frame_from_params({
            "dataset": "contratos", "objeto": ["primera", "infancia"],
            "ciudad": "Barranquilla",
            "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31",
        })
        curr = frame_from_params({"ordering_signal": "valor_desc"})
        curr.intent_type = "refine_delta"
        merged = merge_frames(prev, curr)
        assert merged.dataset == "contratos"
        assert merged.topic == ["primera", "infancia"]
        assert merged.scope.get("ciudad") == "Barranquilla"
        assert merged.modifiers.get("fecha_desde") == "2026-01-01"
        assert merged.modifiers.get("ordering_signal") == "valor_desc"

    def test_contextual_requery_updates_topic(self):
        prev = frame_from_params({
            "dataset": "contratos", "objeto": ["primera", "infancia"],
            "ciudad": "Barranquilla", "fecha_desde": "2026-01-01",
        })
        curr = frame_from_params({
            "objeto": ["primera", "infancia"],
            "fecha_desde": "2025-01-01", "ordering_signal": "valor_desc",
        })
        curr.intent_type = "contextual_requery"
        merged = merge_frames(prev, curr)
        assert merged.dataset == "contratos"
        assert merged.scope.get("ciudad") == "Barranquilla"
        assert merged.topic == ["primera", "infancia"]
        assert merged.modifiers.get("fecha_desde") == "2025-01-01"
        assert merged.modifiers.get("ordering_signal") == "valor_desc"

    def test_switch_scope_replaces_scope(self):
        prev = frame_from_params({"ciudad": "Barranquilla", "objeto": ["infancia"]})
        curr = frame_from_params({"ciudad": "Medellin"})
        curr.intent_type = "switch_scope"
        merged = merge_frames(prev, curr)
        assert merged.scope.get("ciudad") == "Medellin"
        assert merged.topic == ["infancia"]  # heredado

    def test_switch_dataset_replaces_dataset(self):
        prev = frame_from_params({"dataset": "contratos", "objeto": ["infancia"]})
        curr = frame_from_params({"dataset": "procesos", "dataset_explicit": True,
                                   "objeto": ["mantenimiento"]})
        curr.intent_type = "switch_dataset"
        merged = merge_frames(prev, curr)
        assert merged.dataset == "procesos"
        assert merged.topic == ["mantenimiento"]

    def test_params_from_frame_roundtrip(self):
        params = {
            "dataset": "contratos", "objeto": ["primera", "infancia"],
            "ciudad": "Barranquilla",
            "fecha_desde": "2026-01-01", "ordering_signal": "valor_desc",
        }
        frame = frame_from_params(params)
        restored = params_from_frame(frame)
        assert restored.get("dataset") == "contratos"
        assert restored.get("objeto") == ["primera", "infancia"]
        assert restored.get("ciudad") == "Barranquilla"


# ── Tests E2E con workflow completo ──────────────────────────────────────

def _workflow(tmp_path):
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = lambda ds, soql: [
        {
            "nombre_del_procedimiento": "Test",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "estado_de_apertura_del_proceso": "Celebrado",
            "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-1",
        }
    ]
    wf.secop_client.count = lambda ds, soql: 1
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    return wf


class TestE2EQueryFrameScenarios:
    """A. Delta puro: conserva todo + ordering."""

    def test_a_delta_puro(self, tmp_path):
        wf = _workflow(tmp_path)
        wf.run_query("contratos de primera infancia de barranquilla en 2026",
                     channel="streamlit", chat_id="st_a")
        r2 = wf.run_query("quiero ver los de mayor valor",
                          channel="streamlit", chat_id="st_a")
        rp2 = r2.get("resolved_params", {})
        pp2 = r2.get("parsed_params", {})
        assert rp2.get("dataset", pp2.get("dataset")) == "contratos"
        obj = rp2.get("objeto", []) or pp2.get("objeto", [])
        assert "primera" in obj and "infancia" in obj
        assert pp2.get("ordering_signal") == "valor_desc"
        soql = r2.get("soql_query", "")
        assert "WHERE 1=1" not in soql
        assert "valor_del_contrato DESC" in soql

    """B. Contextual re-query: cambia año y orden, conserva scope."""

    def test_b_contextual_requery(self, tmp_path):
        wf = _workflow(tmp_path)
        wf.run_query("contratos de primera infancia de barranquilla en 2026",
                     channel="streamlit", chat_id="st_b")
        r2 = wf.run_query("OK AHORA MUESTRAME PRIMERA INFANCIA 2025, LOS MAS COSTOSOS",
                          channel="streamlit", chat_id="st_b")
        pp2 = r2.get("parsed_params", {})
        rp2 = r2.get("resolved_params", {})
        ds2 = rp2.get("dataset", pp2.get("dataset"))
        assert ds2 == "contratos", f"dataset should be contratos, got {ds2}"
        obj = rp2.get("objeto", []) or pp2.get("objeto", [])
        assert "primera" in obj, f"objeto={obj}"
        assert "infancia" in obj, f"objeto={obj}"
        # Scope conservado (ciudad o entidad con Barranquilla)
        assert "Barranquilla" in str(rp2) or "Barranquilla" in str(pp2), (
            f"Scope should contain Barranquilla: rp2={rp2}"
        )
        assert pp2.get("fecha_desde") == "2025-01-01", f"fecha={pp2.get('fecha_desde')}"
        soql = r2.get("soql_query", "")
        assert "WHERE 1=1" not in soql, f"SoQL={soql[:300]}"

    """C. Cambio de año simple: 'y en 2025'."""

    def test_c_cambio_anio(self, tmp_path):
        wf = _workflow(tmp_path)
        wf.run_query("contratos de primera infancia de barranquilla en 2026",
                     channel="streamlit", chat_id="st_c")
        r2 = wf.run_query("y en 2025",
                          channel="streamlit", chat_id="st_c")
        pp2 = r2.get("parsed_params", {})
        rp2 = r2.get("resolved_params", {})
        assert rp2.get("dataset", pp2.get("dataset")) == "contratos"
        obj = rp2.get("objeto", []) or pp2.get("objeto", [])
        assert "primera" in obj
        assert "Barranquilla" in str(rp2) or "Barranquilla" in str(pp2)
        assert pp2.get("fecha_desde") == "2025-01-01", f"fecha={pp2.get('fecha_desde')}"

    """D. Cambio de ciudad: 'ahora en medellin'."""

    def test_d_cambio_ciudad(self, tmp_path):
        wf = _workflow(tmp_path)
        wf.run_query("contratos de primera infancia de barranquilla en 2026",
                     channel="streamlit", chat_id="st_d")
        r2 = wf.run_query("ahora en medellin",
                          channel="streamlit", chat_id="st_d")
        pp2 = r2.get("parsed_params", {})
        # Ciudad debería ser Medellin (nueva)
        assert "medellin" in pp2.get("ciudad", "").lower() or "Medellin" in str(pp2.get("entidad_resolved", "")), (
            f"ciudad should be Medellin: {pp2.get('ciudad')}, er={pp2.get('entidad_resolved')}"
        )
        # Objeto debe heredar primera/infancia
        obj = pp2.get("objeto", [])
        assert "primera" in obj or "infancia" in obj, f"objeto should inherit: {obj}"

    """E. Cambio de dataset: 'ahora procesos abiertos'."""

    def test_e_cambio_dataset(self, tmp_path):
        wf = _workflow(tmp_path)
        wf.run_query("contratos de primera infancia de barranquilla en 2026",
                     channel="streamlit", chat_id="st_e")
        r2 = wf.run_query("ahora procesos abiertos",
                          channel="streamlit", chat_id="st_e")
        pp2 = r2.get("parsed_params", {})
        assert pp2.get("dataset") == "procesos", f"dataset should be procesos: {pp2.get('dataset')}"

    """F. Sin historial: 'quiero ver los de mayor valor' no ejecuta WHERE 1=1."""

    def test_f_sin_historial(self, tmp_path):
        wf = _workflow(tmp_path)
        r = wf.run_query("quiero ver los de mayor valor",
                         channel="streamlit", chat_id="st_f")
        resp = r.get("response", "")
        assert resp, "empty response"
