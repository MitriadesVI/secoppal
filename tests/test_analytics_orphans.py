"""Tests de los analyzers cableados desde builders huérfanos: ranking, temporal,
value_stats. Foco: detección, NO-COLISIÓN cruzada (entre analyzers y con búsqueda),
SoQL generado y formato.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.core.analytics import detect_analytical_intent, maybe_handle_analytical_query
from app.core.analytics.ranking import RankingAnalyzer
from app.core.analytics.temporal import TemporalAnalyzer
from app.core.analytics.value_stats import ValueStatsAnalyzer
from app.core.soql_builder import SoQLBuilder

CONTRATOS = "jbjy-vk9h"


class TestDeteccion:
    @pytest.mark.parametrize("query,intent", [
        ("qué entidades contratan más en Antioquia", "ranking"),
        ("ranking de entidades de aseo", "ranking"),
        ("principales entidades en Bogotá", "ranking"),
        ("evolución de la contratación de educación", "temporal"),
        ("cómo ha evolucionado la contratación en Soacha", "temporal"),
        ("contratación anual de la alcaldía", "temporal"),
        ("valor promedio de los contratos de obra", "value_stats"),
        ("cuál es el contrato más grande en Antioquia", "value_stats"),
        ("ticket promedio en Barranquilla", "value_stats"),
    ])
    def test_detecta(self, query, intent):
        assert detect_analytical_intent(query) == intent, query


class TestNoColision:
    @pytest.mark.parametrize("query", [
        "contratos de mantenimiento en Barranquilla",
        "contratos en ejecución en Antioquia",
        "procesos de obra en Bogotá",
        "mostrar contratos de aseo",
    ])
    def test_busqueda_normal_no_dispara_analytics(self, query):
        assert detect_analytical_intent(query) is None, query

    def test_analyzers_no_se_pisan(self):
        # Cada intención cae en SU analyzer, no en otro.
        assert detect_analytical_intent("cuánto se ha pagado en Antioquia") == "execution_status"
        assert detect_analytical_intent("cuánto se contrató en Antioquia") == "aggregate_sum"
        assert detect_analytical_intent("valor promedio en Antioquia") == "value_stats"
        assert detect_analytical_intent("qué entidades contratan más") == "ranking"
        assert detect_analytical_intent("evolución por año") == "temporal"


class TestBuildYFormat:
    def test_ranking(self):
        soql = RankingAnalyzer().build(SoQLBuilder(), CONTRATOS,
                                       {"objeto": ["mantenimiento"], "departamento_resolved": "Antioquia"})
        assert "count(*)" in soql.lower() and "group by" in soql.lower()
        rows = [{"nombre_entidad": "ALCALDIA X", "cnt": "150"},
                {"nombre_entidad": "GOBERNACION Y", "cnt": "90"}]
        out = RankingAnalyzer().format(rows, {"departamento_resolved": "Antioquia"}, CONTRATOS)
        assert "ALCALDIA X" in out and "150" in out
        assert RankingAnalyzer().total_count(rows) == 240

    def test_temporal(self):
        rows = [{"yr": "2026-01-01T00:00:00.000", "cnt": "12"},
                {"yr": "2025-01-01T00:00:00.000", "cnt": "30"}]
        out = TemporalAnalyzer().format(rows, {"ciudad": "Soacha"}, CONTRATOS)
        assert "2026: 12" in out and "2025: 30" in out

    def test_value_stats(self):
        rows = [{"mean": 1_500_000_000, "min_val": 1_000_000, "max_val": 8_000_000_000}]
        out = ValueStatsAnalyzer().format(rows, {"ciudad": "Barranquilla"}, CONTRATOS)
        assert "promedio" in out and "1.5 mil millones" in out
        assert "máximo" in out and "8.0 mil millones" in out


class TestEndToEndConMock:
    def test_ranking_end_to_end(self):
        client = MagicMock()
        client.with_timeout.return_value = client
        client.aggregate.return_value = [{"nombre_entidad": "ALCALDIA X", "cnt": "150"}]
        result = maybe_handle_analytical_query(
            user_query="qué entidades contratan más en Antioquia",
            params={"departamento_resolved": "Antioquia"},
            dataset_id=CONTRATOS, secop_client=client, soql_builder=SoQLBuilder(),
        )
        assert result["analytical_intent"] == "ranking"
        assert result["route_reason"] == "analytics_ranking"
        assert "ALCALDIA X" in result["response"]
