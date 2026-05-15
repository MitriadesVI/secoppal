"""Tests: Bloque 1 — Guard anti-WHERE 1=1 + valor_desc vs fecha_desc + suggestions limpio."""
from __future__ import annotations

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter


# ── Task A: Guard anti-WHERE 1=1 ─────────────────────────────────────────

class TestGuardAntiWhere1:
    def _wf(self):
        wf = SecopalWorkflow(Settings())
        wf.secop_client.query = lambda ds, soql: [{"precio_base": "1000000", "fecha_de_publicacion_del": "2026-01-01"}]
        wf.secop_client.count = lambda ds, soql: 1
        wf.narrator.narrate_with_grounding = lambda **kw: None
        return wf

    def test_empty_params_triggers_clarification(self):
        """Params vacios → clarificacion, sin query a SECOP."""
        wf = self._wf()
        r = wf.run_query("quiero ver los de mayor valor", channel="streamlit")
        assert r.get("needs_clarification") is True, f"needs_clarification={r.get('needs_clarification')}"
        assert "filtro" in r.get("response", "").lower() or "necesito" in r.get("response", "").lower(), f"response={r['response'][:200]}"

    def test_only_estado_triggers_clarification(self):
        """Solo estado → clarificacion."""
        wf = self._wf()
        r = wf.run_query("contratos firmados", channel="streamlit")
        # "firmados" es stopword, "contratos" es stopword → sin objeto real
        assert r.get("needs_clarification") is True or "filtro" in r.get("response", "").lower(), f"response={r['response'][:200]}"

    def test_only_dataset_triggers_clarification(self):
        """Solo dataset label → clarificacion."""
        wf = self._wf()
        r = wf.run_query("contratos", channel="streamlit")
        assert r.get("needs_clarification") is True or "filtro" in r.get("response", "").lower()

    def test_objeto_alone_executes(self):
        """objeto=["pavimentacion"] → ejecuta normalmente."""
        wf = self._wf()
        r = wf.run_query("pavimentacion", channel="streamlit")
        assert "pavimentacion" in r.get("soql_query", "").lower()

    def test_only_fecha_triggers_clarification(self):
        """Solo fecha → clarificacion (modifier sin scope)."""
        wf = self._wf()
        r = wf.run_query("en 2026", channel="streamlit")
        assert r.get("needs_clarification") is True or "filtro" in r.get("response", "").lower()

    def test_only_valor_triggers_clarification(self):
        """Solo valor → clarificacion (modifier sin scope)."""
        wf = self._wf()
        r = wf.run_query("mayores a 500 millones", channel="streamlit")
        assert r.get("needs_clarification") is True or "filtro" in r.get("response", "").lower()

    def test_objeto_plus_fecha_executes(self):
        """objeto + fecha → combinacion valida."""
        wf = self._wf()
        r = wf.run_query("pavimentacion 2026", channel="streamlit")
        assert "pavimentacion" in r.get("soql_query", "").lower()
        assert r.get("needs_clarification") is not True

    def test_departamento_plus_valor_executes(self):
        """departamento + valor → combinacion valida."""
        wf = self._wf()
        r = wf.run_query("obras en atlantico mayores a 500 millones", channel="streamlit")
        # Debe ejecutar (tiene scope: atlantico + objeto: obras)
        assert "departamento_entidad" in r.get("soql_query", ""), f"SoQL: {r.get('soql_query', '')[:200]}"
        assert r.get("needs_clarification") is not True


# ── Task B: valor_desc vs fecha_desc ──────────────────────────────────────

class TestValorVsFecha:
    def test_mas_caros_is_valor_desc(self):
        qr = QueryRouter()
        r = qr.parse("contratos mas caros")
        assert r.params.get("ordering_signal") == "valor_desc", f"params={r.params}"

    def test_mayor_valor_is_valor_desc(self):
        qr = QueryRouter()
        r = qr.parse("mayor valor")
        assert r.params.get("ordering_signal") == "valor_desc"

    def test_mas_recientes_is_fecha_desc(self):
        qr = QueryRouter()
        r = qr.parse("los mas recientes")
        assert r.params.get("ordering_signal") == "fecha_desc", f"params={r.params}"

    def test_ultimos_is_fecha_desc(self):
        qr = QueryRouter()
        r = qr.parse("los ultimos")
        assert r.params.get("ordering_signal") == "fecha_desc", f"params={r.params}"

    def test_no_ordering_defaults_none(self):
        qr = QueryRouter()
        r = qr.parse("contratos de mantenimiento")
        assert r.params.get("ordering_signal") not in ("valor_desc", "fecha_desc"), f"params={r.params}"


# ── Task C: Sugerencias numeradas, sin contratista ────────────────────────

class TestSugerenciasLimpias:
    def test_formatter_numbers_suggestions(self):
        from app.core.formatter import Formatter
        f = Formatter()
        result, rows = f.format_for_channel(
            [{"precio_base": "1000000", "fecha_de_publicacion_del": "2026-01-01"}],
            "p6dx-8zbt", "streamlit", total_count=10,
            suggestions=[
                type("S", (), {"label": "Ver solo contratos firmados"})(),
                type("S", (), {"label": "Ordenar por mayor valor"})(),
            ],
        )
        assert "1." in result, f"Missing numbering: {result}"
        assert "2." in result, f"Missing numbering: {result}"

    def test_suggester_no_contratista(self):
        """Agrupar por contratista debe estar eliminado."""
        from app.core.suggester import generate_suggestions
        suggestions = generate_suggestions(
            params={}, universe_insights=None, total_count=100,
            rows=[{"a": 1}], dataset_id="jbjy-vk9h",
        )
        labels = [s.label for s in suggestions]
        assert not any("contratista" in l.lower() for l in labels), f"labels={labels}"
