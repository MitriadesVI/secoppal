"""Tests del ExecutionAnalyzer — ejecución contratado vs pagado.

Cubre: detección, no-colisión con búsqueda/estado, SoQL generado, formato (con
pendiente DERIVADO), guard de dataset (solo contratos) y el escalón de billones.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.core.analytics import detect_analytical_intent, maybe_handle_analytical_query
from app.core.analytics.base import format_currency
from app.core.analytics.execution import ExecutionAnalyzer
from app.core.soql_builder import SoQLBuilder

CONTRATOS = "jbjy-vk9h"
PROCESOS = "p6dx-8zbt"


class TestDeteccion:
    def test_detecta_pago_y_ejecucion(self):
        for q in [
            "cuánto se ha pagado en mantenimiento en Antioquia",
            "nivel de ejecución de la alcaldía de Barranquilla",
            "cuánto falta por pagar en Soacha",
            "ejecución presupuestal de educación en Bogotá",
        ]:
            assert detect_analytical_intent(q) == "execution_status", q

    def test_no_colisiona_con_estado_ni_busqueda(self):
        # "en ejecución" es ESTADO de contrato, no análisis de ejecución.
        assert detect_analytical_intent("contratos en ejecución en Antioquia") is None
        assert detect_analytical_intent("contratos de mantenimiento en Barranquilla") is None
        assert detect_analytical_intent("procesos de obra en Bogotá") is None

    def test_no_colisiona_con_aggregate_sum(self):
        assert detect_analytical_intent("cuánto se contrató en Barranquilla") == "aggregate_sum"


class TestBuild:
    def test_genera_contratado_y_pagado(self):
        soql = ExecutionAnalyzer().build(
            SoQLBuilder(), CONTRATOS,
            {"objeto": ["mantenimiento"], "departamento_resolved": "Antioquia"},
        )
        assert "sum(valor_del_contrato)" in soql
        assert "sum(valor_pagado)" in soql
        assert "WHERE" in soql
        assert "1=1" not in soql
        # NUNCA debe leer el campo crudo contaminado.
        assert "valor_pendiente_de_ejecucion" not in soql

    def test_solo_contratos_no_procesos(self):
        import pytest
        with pytest.raises(ValueError):
            ExecutionAnalyzer().build(SoQLBuilder(), PROCESOS, {"objeto": ["consultoría"]})


class TestFormat:
    def test_pendiente_es_derivado(self):
        rows = [{"contratado": 1_000_000_000, "pagado": 300_000_000, "total_count": 10}]
        out = ExecutionAnalyzer().format(rows, {"departamento_resolved": "Antioquia"}, CONTRATOS)
        assert "1 mil millones" in out or "1.0 mil millones" in out
        assert "300 millones" in out          # pagado
        assert "30% ejecutado" in out          # 300M / 1000M
        assert "700 millones" in out           # pendiente = 1000M - 300M (derivado)
        assert "10 contratos" in out

    def test_sin_valor(self):
        out = ExecutionAnalyzer().format([{"contratado": None}], {}, CONTRATOS)
        assert "No encontré" in out


class TestFormatCurrencyBillones:
    def test_escala_a_billones(self):
        assert format_currency(1_000_000_000_000) == "1 billón"
        assert format_currency(25_200_000_000_000) == "25.2 billones"
        # No rompe los tramos previos:
        assert format_currency(1_000_000) == "1 millón"
        assert format_currency(2_500_000_000) == "2.5 mil millones"


class TestEndToEndConMock:
    def test_maybe_handle_ejecucion(self):
        client = MagicMock()
        client.with_timeout.return_value = client
        client.aggregate.return_value = [
            {"contratado": 5_000_000_000, "pagado": 1_000_000_000, "total_count": 20}
        ]
        result = maybe_handle_analytical_query(
            user_query="cuánto se ha pagado de mantenimiento en Antioquia",
            params={"objeto": ["mantenimiento"], "departamento_resolved": "Antioquia"},
            dataset_id=CONTRATOS,
            secop_client=client,
            soql_builder=SoQLBuilder(),
        )
        assert result is not None
        assert result["analytical_intent"] == "execution_status"
        assert result["route_reason"] == "analytics_execution_status"
        assert result["results"] == []
        assert "20% ejecutado" in result["response"]   # 1000M / 5000M
        client.aggregate.assert_called_once()

    def test_guard_sin_scope(self):
        client = MagicMock()
        result = maybe_handle_analytical_query(
            user_query="cuánto se ha pagado",
            params={"dataset": "contratos"},
            dataset_id=CONTRATOS,
            secop_client=client,
            soql_builder=SoQLBuilder(),
        )
        assert result["needs_clarification"] is True
        assert result["route_reason"] == "analytics_execution_status_no_scope"
        client.aggregate.assert_not_called()
