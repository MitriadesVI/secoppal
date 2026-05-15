"""Tests: protected phrases + pagination + temporal anaphora + ESE guard + degrade conservative."""
from __future__ import annotations

from pathlib import Path

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.conversation_store import ConversationStore
from app.core.query_router import QueryRouter
from app.core.entity_types import rewrite_entity


# ── Task 1: Protected phrase "adulto mayor" ──────────────────────────────

class TestAdultoMayor:
    def test_adulto_mayor_as_bigram(self):
        qr = QueryRouter()
        r = qr.parse("contratos de adulto mayor de alcaldia de barranquilla de 2026")
        obj = r.params.get("objeto", [])
        # Debe contener adulto_mayor (o adulto) — NO solo "adulto"
        assert "adulto_mayor" in obj or "adulto mayor" in " ".join(obj).lower() or "adulto" in obj, f"objeto={obj}"
        # "mayor" NO debe estar solo (sería solo si el bigram no funciona)
        assert "mayor" not in obj, f"mayor should not be in objeto: {obj}"

    def test_adulto_mayor_e2e(self, tmp_path):
        from tempfile import TemporaryDirectory
        wf = SecopalWorkflow(Settings())
        wf.secop_client.query = lambda ds, soql: [{
            "nombre_del_procedimiento": "Test",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-1",
        }]
        wf.secop_client.count = lambda ds, soql: 1
        wf.narrator.narrate_with_grounding = lambda **kw: None
        result = wf.run_query(
            "contratos de adulto mayor de alcaldia de barranquilla de 2026",
            channel="streamlit",
        )
        soql = result.get("soql_query", "")
        assert "WHERE 1=1" not in soql, f"SoQL={soql[:300]}"
        # SoQL debe contener variante de adulto_mayor
        assert "adulto" in soql.lower(), f"SoQL missing adulto variant: {soql[:300]}"


# ── Task 2: Pagination "muéstrame más" ───────────────────────────────────

class TestPagination:
    def _workflow(self, tmp_path):
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

    def test_muestrame_mas_with_context(self, tmp_path):
        wf = self._workflow(tmp_path)
        wf.run_query(
            "contratos de adulto mayor de alcaldia de barranquilla de 2026",
            channel="streamlit", chat_id="st_pag",
        )
        r2 = wf.run_query("muestrame mas", channel="streamlit", chat_id="st_pag")
        assert r2.get("route_reason") == "pagination_more", f"route={r2.get('route_reason')}"
        assert r2.get("intent_type") == "pagination_more"
        soql = r2.get("soql_query", "")
        assert "OFFSET" in soql, f"SoQL missing OFFSET: {soql[:300]}"

    def test_muestrame_mas_without_context(self, tmp_path):
        wf = self._workflow(tmp_path)
        r = wf.run_query("muestrame mas", channel="streamlit", chat_id="st_empty")
        assert "busqueda" in r.get("response", "").lower() or "consulta" in r.get("response", "").lower(), f"response={r['response'][:200]}"

    def test_offset_in_soql(self):
        from app.core.soql_builder import SoQLBuilder
        sb = SoQLBuilder()
        soql = sb.build("jbjy-vk9h", {"objeto": ["adulto"], "offset": 10, "limit": 10})
        assert "OFFSET 10" in soql, f"SoQL missing OFFSET: {soql}"
        assert "LIMIT 10" in soql


# ── Task 3+4: Temporal anaphora + ESE guard ──────────────────────────────

class TestTemporalAnaphora:
    def test_ese_ano_sets_flag(self):
        qr = QueryRouter()
        r = qr.parse("los mas caros de ese año")
        assert r.params.get("_temporal_anaphora") is True, f"params={r.params}"

    def test_ese_ano_not_entity(self):
        qr = QueryRouter()
        r = qr.parse("los mas caros de ese año")
        # No debe tener entidad ESE
        er = r.params.get("entidad_resolved", "")
        assert "ESE" not in er.upper() and "EMPRESA" not in er.upper(), f"entidad_resolved={er}"

    def test_entity_types_skips_ese_ano(self):
        candidates = rewrite_entity("los mas caros de ese año")
        assert len(candidates) == 0, f"Should skip ESE on 'ese año': {candidates}"

    def test_entity_types_allows_ese_hospital(self):
        candidates = rewrite_entity("ese hospital san rafael")
        # Debe encontrar al menos un candidate
        found = any("Hospital" in c[0] or "ESE" in c[0] for c in candidates)
        assert found, f"Candidates: {candidates}"

    def test_muestrame_caros_ese_ano_e2e(self, tmp_path):
        wf = SecopalWorkflow(Settings())
        wf.secop_client.query = lambda ds, soql: [{
            "nombre_del_procedimiento": "Atencion adulto mayor",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "500000000",
            "fecha_de_publicacion_del": "2025-06-01T00:00:00.000",
            "urlproceso": "https://secop.gov.co/test",
            "referencia_del_proceso": "REF-1",
        }]
        wf.secop_client.count = lambda ds, soql: 1
        wf.narrator.narrate_with_grounding = lambda **kw: None
        wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
        # Turno 1
        wf.run_query(
            "muestrame contratos de adulto mayor de alcaldia de barranquilla en 2025",
            channel="streamlit", chat_id="st_ese",
        )
        # Turno 2
        r2 = wf.run_query(
            "muestrame los mas caros de ese año",
            channel="streamlit", chat_id="st_ese",
        )
        pp2 = r2.get("parsed_params", {})
        # NO debe tener entidad ESE
        assert "ESE" not in str(pp2), f"ESE detected: {pp2}"
        # Debe conservar dataset y scope
        assert r2.get("route_reason") not in ("suggestion_invalid", "suggestion_error")
        soql = r2.get("soql_query", "")
        assert "WHERE 1=1" not in soql
