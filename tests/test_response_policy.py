"""Tests: Bloque 3 — Política de respuesta asesora."""
from __future__ import annotations

import pytest
from unittest.mock import MagicMock

from app.core.response_policy import (
    build_advisor_response,
    _build_interpretation,
    _build_ordering_label,
    _build_lectura_rapida,
    _insights_summary_deterministico,
    _suggest_paths_from_partial_params,
    _summarize_active_filters,
)
from app.core.observer import UniverseInsights


# ── Unit helpers ─────────────────────────────────────────────────────────

class TestInterpretation:
    def test_contratos_objeto_depto(self):
        r = _build_interpretation({
            "dataset": "contratos", "objeto": ["adulto_mayor"],
            "departamento_resolved": "ATLÁNTICO",
        })
        assert "Contratos" in r
        assert "adulto_mayor" in r
        assert "ATLÁNTICO" in r

    def test_procesos_entidad(self):
        r = _build_interpretation({
            "dataset": "procesos", "objeto": ["pavimentacion"],
            "entidad_resolved": "INVÍAS",
        })
        assert "Procesos" in r
        assert "pavimentacion" in r
        assert "INVÍAS" in r

    def test_with_fecha_desde(self):
        r = _build_interpretation({
            "dataset": "contratos", "objeto": ["primera", "infancia"],
            "fecha_desde": "2025-01-01",
        })
        assert "desde 2025" in r


class TestOrderingLabel:
    def test_valor_desc(self):
        assert _build_ordering_label({"ordering_signal": "valor_desc"}) == "de mayor valor"

    def test_default(self):
        assert _build_ordering_label({}) == "más recientes"

    def test_fecha_desc(self):
        assert _build_ordering_label({"ordering_signal": "fecha_desc"}) == "más recientes"


class TestLecturaRapida:
    def test_no_signals_returns_none(self):
        i = UniverseInsights(
            total_count=10, sample_size=5,
            date_range=None, top_entities=[], value_stats=None,
            top_modalities=[], temporal_distribution={},
            computed_from="sample",
            has_dominant_entity=False, dominant_entity_name=None,
            dominant_entity_pct=None, has_temporal_concentration=False,
            dominant_year=None, dominant_year_pct=None,
            has_value_outlier=False, outlier_value=None,
            has_diverse_modalities=False,
        )
        assert _build_lectura_rapida(i, None, [], "", "") is None

    def test_with_signal_uses_deterministic(self):
        i = UniverseInsights(
            total_count=100, sample_size=10,
            date_range=("2024-01-01", "2025-12-31"),
            top_entities=[("INVÍAS", 60)],
            value_stats={"mean": 100_000_000, "median": 80_000_000, "min": 1_000, "max": 500_000_000},
            top_modalities=[("LP", 30)],
            temporal_distribution={2025: 70, 2024: 30},
            computed_from="sample",
            has_dominant_entity=True, dominant_entity_name="INVÍAS",
            dominant_entity_pct=60.0,
            has_temporal_concentration=True, dominant_year=2025,
            dominant_year_pct=70.0,
            has_value_outlier=False, outlier_value=None,
            has_diverse_modalities=False,
        )
        result = _build_lectura_rapida(i, None, [], "", "")
        assert result is not None
        assert "INVÍAS" in result or "60" in result

    def test_with_narrator_calls_it(self):
        i = UniverseInsights(
            total_count=100, sample_size=10,
            date_range=None, top_entities=[("INVÍAS", 60)],
            value_stats=None, top_modalities=[], temporal_distribution={2025: 70},
            computed_from="sample",
            has_dominant_entity=True, dominant_entity_name="INVÍAS",
            dominant_entity_pct=60.0,
            has_temporal_concentration=False, dominant_year=None,
            dominant_year_pct=None, has_value_outlier=False,
            outlier_value=None, has_diverse_modalities=False,
        )
        mock_narrator = MagicMock()
        mock_narrator.narrate.return_value = "Lectura rapida generada por LLM"
        result = _build_lectura_rapida(i, mock_narrator, [], "test", "streamlit", {}, 100)
        assert mock_narrator.narrate.called
        assert "LLM" in result

    def test_valor_desc_uses_deterministic_top_values_not_narrator(self):
        i = UniverseInsights(
            total_count=206, sample_size=10,
            date_range=("2025-01-01", "2026-12-31"), top_entities=[],
            value_stats=None, top_modalities=[], temporal_distribution={2025: 10},
            computed_from="sample",
            has_dominant_entity=False, dominant_entity_name=None,
            dominant_entity_pct=None,
            has_temporal_concentration=True, dominant_year=2025,
            dominant_year_pct=100.0, has_value_outlier=False,
            outlier_value=None, has_diverse_modalities=False,
        )
        rows = [
            {"valor": "$9,436,117,766", "contratista": "CORPORACION UNIVERSIDAD DE LA COSTA CUC", "fecha": "2025-05-30"},
            {"valor": "$5,661,670,660", "contratista": "FUNDACARIBE", "fecha": "2025-05-21"},
        ]
        mock_narrator = MagicMock()
        mock_narrator.narrate.return_value = "Encontré 10 contratos. El más costoso es $3.500 millones."

        result = _build_lectura_rapida(
            i, mock_narrator, rows, "test", "streamlit", {"ordering_signal": "valor_desc"}, 206
        )

        assert not mock_narrator.narrate.called
        assert "$9,436,117,766" in result
        assert "$5,661,670,660" in result
        assert "3.500" not in result
        assert "Encontré 10 contratos" not in result

    def test_narrator_numbers_are_discarded_even_without_valor_desc(self):
        i = UniverseInsights(
            total_count=206, sample_size=10,
            date_range=None, top_entities=[("INVÍAS", 60)],
            value_stats=None, top_modalities=[], temporal_distribution={},
            computed_from="sample",
            has_dominant_entity=True, dominant_entity_name="INVÍAS",
            dominant_entity_pct=60.0,
            has_temporal_concentration=False, dominant_year=None,
            dominant_year_pct=None, has_value_outlier=False,
            outlier_value=None, has_diverse_modalities=False,
        )
        mock_narrator = MagicMock()
        mock_narrator.narrate.return_value = "Encontré 10 contratos y el segundo vale 3500 millones."

        result = _build_lectura_rapida(i, mock_narrator, [], "test", "streamlit", {}, 206)

        assert mock_narrator.narrate.called
        assert "Encontré 10 contratos" not in result
        assert "3500" not in result
        assert "INVÍAS" in result


class TestFiltersAndPaths:
    def test_summarize_active_filters(self):
        f = _summarize_active_filters({
            "objeto": ["adulto_mayor"],
            "departamento_resolved": "ATLÁNTICO",
            "fecha_desde": "2025-01-01",
            "fecha_hasta": "2025-12-31",
        })
        assert len(f) >= 2
        assert any("adulto_mayor" in x for x in f)

    def test_suggest_paths_from_partial(self):
        paths = _suggest_paths_from_partial_params({
            "dataset": "contratos", "departamento_resolved": "ATLÁNTICO",
        })
        assert len(paths) >= 1
        assert all("ATLÁNTICO" in p for p in paths)


# ── E2E: build_advisor_response ──────────────────────────────────────────

class TestBuildAdvisorResponse:
    def _base_context(self, **overrides):
        ctx = {
            "user_query": "test",
            "resolved_params": {},
            "total_count": 0,
            "rows": [],
            "universe_insights": None,
            "suggestions": [],
            "dataset_id": "",
            "channel": "streamlit",
            "needs_clarification": False,
            "clarification_reason": "",
            "degraded": False,
            "degraded_hint": "",
            "query_error": "",
            "timeout_suggestions": [],
        }
        ctx.update(overrides)
        return ctx

    def test_clear_query_streamlit_does_not_duplicate_result_list(self):
        rows = [{"titulo": "Test", "entidad": "INVÍAS", "valor": "$100M", "estado": "",
                 "fecha": "2026-01-01", "contratista": "", "url": ""}]
        r = build_advisor_response(self._base_context(
            resolved_params={"dataset": "contratos", "objeto": ["adulto_mayor"],
                             "departamento_resolved": "ATLÁNTICO"},
            rows=rows, total_count=5,
            channel="streamlit",
        ))
        assert "📋" in r or "Contratos" in r
        assert "resultado" in r.lower()
        assert "1. Test" not in r

    def test_clear_query_telegram_includes_result_list(self):
        rows = [{"titulo": "Test", "entidad": "INVÍAS", "valor": "$100M", "estado": "",
                 "fecha": "2026-01-01", "contratista": "", "url": ""}]
        r = build_advisor_response(self._base_context(
            resolved_params={"dataset": "contratos", "objeto": ["adulto_mayor"],
                             "departamento_resolved": "ATLÁNTICO"},
            rows=rows, total_count=5,
            channel="telegram",
        ))
        assert "1. Test" in r

    def test_ambiguous_query_produces_clarification_paths(self):
        r = build_advisor_response(self._base_context(
            needs_clarification=True,
            clarification_reason="No pude interpretar la consulta.",
        ))
        assert "No pude interpretar" in r
        assert "1." in r

    def test_no_results_produces_options(self):
        r = build_advisor_response(self._base_context(
            resolved_params={"objeto": ["pavimentacion"], "dataset": "procesos"},
            rows=[],
        ))
        assert "No encontre resultados" in r
        assert "Puedo intentar" in r

    def test_query_error_response(self):
        r = build_advisor_response(self._base_context(
            query_error="Timeout connecting to SECOP",
        ))
        assert "no respondio" in r.lower()

    def test_valor_desc_response_uses_total_count_not_sample_count_or_fake_narrator_values(self):
        rows = [
            {
                "titulo": "Contrato adulto mayor 1",
                "entidad": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
                "valor": "$9,436,117,766",
                "estado": "En ejecución",
                "fecha": "2025-05-30",
                "contratista": "CORPORACION UNIVERSIDAD DE LA COSTA CUC",
                "url": "",
            },
            {
                "titulo": "Contrato adulto mayor 2",
                "entidad": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
                "valor": "$5,661,670,660",
                "estado": "En ejecución",
                "fecha": "2025-05-21",
                "contratista": "FUNDACARIBE",
                "url": "",
            },
        ]
        insights = UniverseInsights(
            total_count=206, sample_size=10,
            date_range=("2025-01-01", "2026-12-31"), top_entities=[],
            value_stats=None, top_modalities=[], temporal_distribution={2025: 10},
            computed_from="sample",
            has_dominant_entity=False, dominant_entity_name=None,
            dominant_entity_pct=None, has_temporal_concentration=True,
            dominant_year=2025, dominant_year_pct=100.0,
            has_value_outlier=False, outlier_value=None,
            has_diverse_modalities=False,
        )
        narrator = MagicMock()
        narrator.narrate.return_value = "Encontré 10 contratos. El más costoso es $3.500 millones."

        r = build_advisor_response(self._base_context(
            resolved_params={"dataset": "contratos", "objeto": ["adulto_mayor"], "ordering_signal": "valor_desc"},
            rows=rows,
            total_count=206,
            universe_insights=insights,
            channel="telegram",
        ), narrator=narrator)

        assert "Encontre 206 resultados. Te muestro los 2 de mayor valor" in r
        assert "Encontré 10 contratos" not in r
        assert "3.500" not in r
        assert "$9,436,117,766" in r
        assert "$5,661,670,660" in r
