"""Tests tarea 1.4 — mensaje 'sin resultados' amigable con sugerencias."""
from __future__ import annotations

import pytest

from app.core.formatter import Formatter, build_no_results_message

F = Formatter()


class TestBuildNoResultsMessage:
    def test_no_params_returns_generic(self):
        msg = build_no_results_message(None)
        assert "No encontre" in msg

    def test_estado_suggests_removing_estado(self):
        params = {"estado": "Abierto", "objeto": ["mantenimiento"]}
        msg = build_no_results_message(params)
        assert "estado" in msg

    def test_fecha_suggests_amplia_fechas(self):
        params = {"fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31"}
        msg = build_no_results_message(params)
        assert "fecha" in msg

    def test_valor_suggests_cambiar_rango(self):
        params = {"valor_min": 50_000_000, "valor_max": 200_000_000}
        msg = build_no_results_message(params)
        assert "valor" in msg

    def test_departamento_suggests_todo_pais(self):
        params = {"departamento_resolved": "Choco", "objeto": ["obra"]}
        msg = build_no_results_message(params)
        assert "pais" in msg

    def test_multi_objeto_suggests_single(self):
        params = {"objeto": ["vias", "pavimentacion", "obra"]}
        msg = build_no_results_message(params, "jbjy-vk9h")
        assert "vias" in msg

    def test_contratos_suggests_procesos(self):
        params = {"dataset": "contratos"}
        msg = build_no_results_message(params, "jbjy-vk9h")
        assert "procesos" in msg

    def test_procesos_suggests_contratos(self):
        params = {"dataset": "procesos"}
        msg = build_no_results_message(params, "p6dx-8zbt")
        assert "contratos" in msg

    def test_max_3_suggestions(self):
        # All filters set — should still cap at 3 suggestions
        params = {
            "estado": "Abierto",
            "fecha_desde": "2026-01-01",
            "valor_min": 100_000_000,
            "departamento_resolved": "Antioquia",
            "entidad_like": "alcaldia",
            "objeto": ["obra", "vial"],
        }
        msg = build_no_results_message(params)
        # Count semicolons — max 2 semicolons = max 3 suggestions
        assert msg.count(";") <= 2


class TestFormatterNoResultsIntegration:
    def test_telegram_no_results_with_params(self):
        params = {"estado": "Abierto", "departamento_resolved": "Choco"}
        msg = F.format_telegram([], params=params, dataset_id="jbjy-vk9h")
        assert "No encontre" in msg
        assert "estado" in msg

    def test_streamlit_no_results_with_params(self):
        params = {"valor_min": 50_000_000}
        msg = F.format_streamlit([], params=params, dataset_id="jbjy-vk9h")
        assert "valor" in msg

    def test_whatsapp_no_results_with_params(self):
        params = {"entidad_like": "alcaldia de ocana"}
        msg = F.format_whatsapp([], params=params, dataset_id="p6dx-8zbt")
        assert "entidad" in msg

    def test_format_for_channel_passes_params(self):
        params = {"fecha_desde": "2025-01-01"}
        msg, rows = F.format_for_channel([], "jbjy-vk9h", "telegram", params=params)
        assert rows == []
        assert "fecha" in msg

    def test_results_present_ignores_no_results_logic(self):
        # When results exist, normal format is returned (no sugerencias)
        results = [{"objeto_del_contrato": "OBRA X", "nombre_entidad": "ENT", "valor_del_contrato": 1_000_000, "estado_contrato": "Activo", "fecha_de_firma": "2025-01-01", "urlproceso": ""}]
        msg, rows = F.format_for_channel(results, "jbjy-vk9h", "telegram")
        assert "Encontre" in msg
        assert len(rows) == 1


class TestTotalCountHeader:
    """Tests tarea 2.0 — header con universo total cuando count > shown."""

    FAKE_ROW = {
        "nombre_del_procedimiento": "Pavimentacion via rural",
        "entidad": "GOBERNACION DE CORDOBA",
        "precio_base": 300_000_000,
        "estado_de_apertura_del_proceso": "Abierto",
        "fecha_de_publicacion_del": "2026-01-01",
        "urlproceso": "https://example.com",
    }

    def test_whatsapp_shows_universe_when_total_exceeds_shown(self):
        msg, _ = F.format_for_channel(
            [self.FAKE_ROW], "p6dx-8zbt", "whatsapp", total_count=500
        )
        assert "500" in msg
        assert "más recientes" in msg

    def test_whatsapp_no_universe_line_when_total_equals_shown(self):
        msg, _ = F.format_for_channel(
            [self.FAKE_ROW], "p6dx-8zbt", "whatsapp", total_count=1
        )
        assert "más recientes" not in msg
        assert "Encontre 1 resultados" in msg

    def test_telegram_shows_universe(self):
        msg, _ = F.format_for_channel(
            [self.FAKE_ROW], "p6dx-8zbt", "telegram", total_count=1234
        )
        assert "1,234" in msg
        assert "más recientes" in msg

    def test_streamlit_shows_universe(self):
        msg, _ = F.format_for_channel(
            [self.FAKE_ROW], "p6dx-8zbt", "streamlit", total_count=88
        )
        assert "88" in msg
        assert "más recientes" in msg

    def test_default_total_count_zero_acts_as_no_universe(self):
        """total_count=0 (default) should fall through to plain 'Encontre N' header."""
        msg, _ = F.format_for_channel(
            [self.FAKE_ROW], "p6dx-8zbt", "whatsapp"
        )
        assert "más recientes" not in msg
