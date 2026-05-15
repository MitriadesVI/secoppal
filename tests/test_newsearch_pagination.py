"""Tests: new_search completo + paginación robusta + anti-WHERE 1=1."""
from __future__ import annotations

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore, is_followup, is_pagination_phrase, normalize_pagination_text, Turn
from app.core.query_frame import is_complete_new_search, frame_from_params, QueryFrame


# ── Task 1: is_complete_new_search ────────────────────────────────────────

class TestIsCompleteNewSearch:
    def test_complete_search_detected(self):
        f = frame_from_params({
            "dataset": "contratos", "dataset_explicit": True,
            "objeto": ["adulto_mayor"],
            "ciudad": "Barranquilla",
        })
        assert is_complete_new_search(f) is True

    def test_no_topic_not_complete(self):
        f = frame_from_params({"dataset": "contratos", "dataset_explicit": True})
        assert is_complete_new_search(f) is False

    def test_no_scope_no_date_false(self):
        f = frame_from_params({
            "dataset": "contratos", "dataset_explicit": True,
            "objeto": ["adulto"],
        })
        assert is_complete_new_search(f) is False

    def test_with_fecha_count_as_complete(self):
        f = frame_from_params({
            "dataset": "contratos", "dataset_explicit": True,
            "objeto": ["adulto"], "fecha_desde": "2026-01-01",
        })
        assert is_complete_new_search(f) is True


# ── Task 1+2: "quiero ver" + búsqueda completa NO es follow-up ───────────

class TestCompleteSearchNotFollowup:
    def _turn(self):
        from datetime import datetime, timezone, timedelta
        ts = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
        return Turn(
            turn_id="x", timestamp=ts,
            chat_id="x", user_query="x", response="x",
            parsed_params={"dataset": "contratos", "objeto": ["obras"], "ordering_signal": "valor_desc"},
        )

    def test_quiero_ver_contratos_completo_not_followup(self):
        t = self._turn()
        assert is_followup("hola quiero ver contratos de adulto mayor de alcaldia de barranquilla de 2026", t) is False

    def test_quiero_ver_solo_orden_is_followup(self):
        t = self._turn()
        assert is_followup("quiero ver los de mayor valor", t) is True

    def test_quiero_ver_firmados_is_followup(self):
        t = self._turn()
        assert is_followup("quiero ver los firmados", t) is True

    def test_quiero_ver_2025_is_followup(self):
        t = self._turn()
        assert is_followup("quiero ver los de 2025", t) is True


# ── Task 3: Paginación robusta ────────────────────────────────────────────

class TestPaginationPhrase:
    def test_muestrame_mas_normal(self):
        assert is_pagination_phrase("muestrame mas") is True

    def test_muestrame_mas_por_favor(self):
        assert is_pagination_phrase("muestrame mas por favor") is True

    def test_muestrame_mas_con_tilde(self):
        assert is_pagination_phrase("muéstrame más, por favor") is True

    def test_otros_diez(self):
        assert is_pagination_phrase("otros diez") is True

    def test_quiero_ver_mas(self):
        assert is_pagination_phrase("quiero ver mas") is True

    def test_dale_siguientes(self):
        assert is_pagination_phrase("dale siguientes") is True

    def test_contratos_de_mantenimiento_no_pagination(self):
        assert is_pagination_phrase("contratos de mantenimiento") is False

    def test_muestrame_mas_with_new_contract_filters_is_not_pagination(self):
        assert is_pagination_phrase("muestrame mas contratos de adulto mayor de 2026") is False

    def test_muestrame_mas_with_year_filter_is_not_pagination(self):
        assert is_pagination_phrase("muestrame mas de 2026") is False

    def test_muestrame_mas_with_state_filter_is_not_pagination(self):
        assert is_pagination_phrase("muestrame mas contratos en ejecucion") is False

    def test_muestrame_mas_with_scope_filter_is_not_pagination(self):
        assert is_pagination_phrase("muestrame mas procesos del sena atlantico") is False

    def test_muestrame_mas_with_order_filter_is_not_pagination(self):
        assert is_pagination_phrase("muestrame mas de mayor valor") is False


# ── E2E: paginación persistente ──────────────────────────────────────────

class TestPaginationE2E:
    def _wf(self, tmp_path):
        wf = SecopalWorkflow(Settings())
        wf.secop_client.query = lambda ds, soql: [{
            "nombre_del_procedimiento": "Atencion adulto mayor",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-1",
        }]
        wf.secop_client.count = lambda ds, soql: 50
        wf.narrator.narrate_with_grounding = lambda **kw: None
        wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
        return wf

    def test_pagination_por_favor_with_context(self, tmp_path):
        wf = self._wf(tmp_path)
        wf.run_query("contratos de adulto mayor de alcaldia de barranquilla de 2026",
                     channel="streamlit", chat_id="st_p")
        r2 = wf.run_query("muestrame mas por favor", channel="streamlit", chat_id="st_p")
        assert r2.get("route_reason") == "pagination_more", f"route={r2.get('route_reason')}"
        soql = r2.get("soql_query", "")
        assert "OFFSET 10" in soql, f"SoQL={soql[:300]}"
        assert "WHERE 1=1" not in soql

    def test_pagination_increments_offset(self, tmp_path):
        wf = self._wf(tmp_path)
        wf.run_query("contratos de adulto mayor de alcaldia de barranquilla de 2026",
                     channel="streamlit", chat_id="st_p2")
        r2 = wf.run_query("muestrame mas", channel="streamlit", chat_id="st_p2")
        assert r2.get("route_reason") == "pagination_more"
        assert "OFFSET 10" in r2.get("soql_query", "")
        r3 = wf.run_query("muestrame mas", channel="streamlit", chat_id="st_p2")
        assert "OFFSET 20" in r3.get("soql_query", ""), f"SoQL={r3.get('soql_query')[:300]}"

    def test_pagination_no_context_asks_primero(self, tmp_path):
        wf = self._wf(tmp_path)
        r = wf.run_query("muestrame mas por favor", channel="streamlit", chat_id="st_empty")
        assert "busqueda" in r.get("response", "").lower() or "consulta" in r.get("response", "").lower(), f"response={r['response'][:200]}"
        assert r.get("route_reason") == "pagination_no_context"

    def test_mas_with_new_2026_filter_is_refinement_not_pagination(self, tmp_path):
        wf = self._wf(tmp_path)
        chat_id = "st_more_filter"

        wf.run_query(
            "holaa muestrame los contratos mas altos de adulto mayor del distrito de barranquilla 2025-2026",
            channel="streamlit",
            chat_id=chat_id,
        )

        r2 = wf.run_query(
            "muestrame mas contratos de adulto mayor de 2026",
            channel="streamlit",
            chat_id=chat_id,
        )

        params = r2.get("resolved_params", {})
        soql = r2.get("soql_query", "")
        assert r2.get("route_reason") != "pagination_more"
        assert r2.get("followup") is True
        assert params.get("dataset") == "contratos"
        assert params.get("entidad_resolved") == "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"
        assert params.get("objeto") == ["adulto_mayor"]
        assert params.get("fecha_desde") == "2026-01-01"
        assert params.get("fecha_hasta") == "2026-12-31"
        assert params.get("ordering_signal") == "valor_desc"
        assert params.get("offset", 0) == 0
        assert "fecha_de_firma >= '2026-01-01'" in soql
        assert "fecha_de_firma <= '2026-12-31'" in soql
        assert "OFFSET 10" not in soql

    def test_refinement_after_pagination_resets_offset(self, tmp_path):
        wf = self._wf(tmp_path)
        chat_id = "st_more_filter_after_page"

        wf.run_query(
            "holaa muestrame los contratos mas altos de adulto mayor del distrito de barranquilla 2025-2026",
            channel="streamlit",
            chat_id=chat_id,
        )
        page_2 = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat_id)
        assert page_2.get("route_reason") == "pagination_more"
        assert page_2.get("resolved_params", {}).get("offset") == 10

        refined = wf.run_query(
            "muestrame mas contratos de adulto mayor de 2026",
            channel="streamlit",
            chat_id=chat_id,
        )

        assert refined.get("route_reason") != "pagination_more"
        assert refined.get("resolved_params", {}).get("offset", 0) == 0
        assert "OFFSET 10" not in refined.get("soql_query", "")
        assert "fecha_de_firma >= '2026-01-01'" in refined.get("soql_query", "")


# ── E2E: new_search completo no hereda ordering ──────────────────────────

class TestNewSearchNoInherit:
    def test_no_ordering_inherited(self, tmp_path):
        wf = SecopalWorkflow(Settings())
        wf.secop_client.query = lambda ds, soql: [{
            "nombre_del_procedimiento": "Atencion adulto mayor",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-1",
        }]
        wf.secop_client.count = lambda ds, soql: 1
        wf.narrator.narrate_with_grounding = lambda **kw: None
        wf.conv_store = ConversationStore(base_path=tmp_path / "conv")

        # Turn 1 con ordering_signal
        wf.run_query("contratos de primera infancia de barranquilla", channel="streamlit", chat_id="st_noinh")

        # Turn 2: búsqueda completa nueva - NO debe heredar ordering
        r2 = wf.run_query(
            "HOLA QUIERO VER CONTRATOS DE ADULTO MAYOR DE ALCALDIA DE BARRANQUILLA DE 2026",
            channel="streamlit", chat_id="st_noinh",
        )
        pp2 = r2.get("parsed_params", {})
        # NO debe tener ordering_signal heredado
        assert pp2.get("ordering_signal") is None or pp2.get("ordering_signal") == "", (
            f"ordering should NOT be inherited: {pp2.get('ordering_signal')}"
        )
        soql = r2.get("soql_query", "")
        assert "WHERE 1=1" not in soql, f"SoQL={soql[:300]}"
