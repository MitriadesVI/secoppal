"""Tests para resiliencia parcial del observer (B5)."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.core.observer import observe
from app.core.soql_builder import SoQLBuilder


@pytest.fixture
def soql_builder():
    return SoQLBuilder()


class TestObserverPartial:
    def test_partial_when_all_aggregations_fail(self, soql_builder):
        """All aggregation queries fail → empty insights with partial=True."""
        client = MagicMock()
        client.with_timeout.return_value = client
        client.aggregate.side_effect = RuntimeError("timeout")
        insights = observe(
            [], total_count=200, dataset_id="jbjy-vk9h",
            params={}, secop_client=client, soql_builder=soql_builder,
        )
        assert insights is not None
        assert insights.partial is True
        assert insights.computed_from == "aggregation"
        assert insights.top_entities == []
        assert insights.value_stats is None
        assert insights.top_modalities == []
        assert insights.temporal_distribution == {}
        assert insights.date_range is None

    def test_partial_when_some_aggregations_fail(self, soql_builder):
        """Some queries succeed, some fail → partial=True with available data."""
        client = MagicMock()
        client.with_timeout.return_value = client

        def side_effect(*args, **kwargs):
            soql = args[1] if len(args) > 1 else kwargs.get("soql", "")
            if "avg(" in soql:
                return [{"mean": "500000000", "min_val": "10000", "max_val": "5000000000"}]
            if "date_trunc_y" in soql:
                return [{"yr": "2025", "cnt": "70"}]
            if "min(fecha_de_firma)" in soql:
                return [{"date_min": "2024-01-01", "date_max": "2025-12-31"}]
            if "nombre_entidad, count" in soql:
                return [{"nombre_entidad": "INVIAS", "cnt": "100"}]
            raise RuntimeError("timeout")

        client.aggregate.side_effect = side_effect
        insights = observe(
            [], total_count=200, dataset_id="jbjy-vk9h",
            params={}, secop_client=client, soql_builder=soql_builder,
        )
        assert insights is not None
        assert insights.partial is True
        assert insights.top_entities == [("INVIAS", 100)]
        assert insights.value_stats is not None
        assert insights.date_range == ("2024-01-01", "2025-12-31")
        assert insights.top_modalities == []  # failed

    def test_no_partial_when_all_succeed(self, soql_builder):
        """All aggregation queries succeed → partial=False."""
        client = MagicMock()
        client.with_timeout.return_value = client
        client.aggregate.side_effect = [
            [{"nombre_entidad": "INVIAS", "cnt": "100"}],
            [{"mean": "500000000", "min_val": "10000", "max_val": "5000000000"}],
            [{"modalidad_de_contratacion": "LP", "cnt": "80"}],
            [{"date_min": "2024-01-01", "date_max": "2025-12-31"}],
            [{"yr": "2025", "cnt": "70"}],
        ]
        insights = observe(
            [], total_count=200, dataset_id="jbjy-vk9h",
            params={}, secop_client=client, soql_builder=soql_builder,
        )
        assert insights is not None
        assert insights.partial is False
        assert insights.top_entities == [("INVIAS", 100)]
