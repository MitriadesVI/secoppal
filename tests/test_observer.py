"""Tests para observer.py — Tarea 2.6."""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from app.core.observer import (
    UniverseInsights,
    _compute_from_sample,
    _derive_signals,
    observe,
)
from app.core.soql_builder import SoQLBuilder
from tests.helpers.mock_data import make_contrato, make_proceso


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def soql_builder():
    return SoQLBuilder()


@pytest.fixture
def sample_contratos():
    return [
        make_contrato(entidad="INVIAS", valor=100_000_000, modalidad="Licitacion Publica", fecha="2025-01-15T00:00:00"),
        make_contrato(entidad="INVIAS", valor=200_000_000, modalidad="Licitacion Publica", fecha="2025-03-10T00:00:00"),
        make_contrato(entidad="ANLA", valor=50_000_000, modalidad="Contratacion Directa", fecha="2024-06-01T00:00:00"),
        make_contrato(entidad="INVIAS", valor=2_300_000_000, modalidad="Licitacion Publica", fecha="2025-11-20T00:00:00"),
        make_contrato(entidad="FNA", valor=80_000_000, modalidad="Seleccion Abreviada", fecha="2024-09-05T00:00:00"),
    ]


@pytest.fixture
def sample_procesos():
    return [
        make_proceso(entidad="E.S.E. HOSPITAL SAN RAFAEL", valor=500_000_000, modalidad="Licitacion Publica", fecha="2025-02-01T00:00:00"),
        make_proceso(entidad="MUNICIPIO DE GIRARDOT", valor=300_000_000, modalidad="Seleccion Abreviada", fecha="2025-05-15T00:00:00"),
        make_proceso(entidad="E.S.E. HOSPITAL SAN RAFAEL", valor=150_000_000, modalidad="Licitacion Publica", fecha="2024-11-01T00:00:00"),
    ]


# ── _compute_from_sample ──────────────────────────────────────────────────

class TestComputeFromSample:
    def test_basic_stats_contratos(self, sample_contratos):
        insights = _compute_from_sample(sample_contratos, total_count=10)
        assert insights.total_count == 10
        assert insights.sample_size == 5
        assert insights.computed_from == "sample"
        assert len(insights.top_entities) >= 3
        assert insights.top_entities[0][0] == "INVIAS"  # 3 of 5
        assert insights.value_stats is not None
        assert insights.value_stats["mean"] == pytest.approx(546_000_000)
        assert insights.value_stats["median"] == pytest.approx(100_000_000)
        assert insights.value_stats["max"] == pytest.approx(2_300_000_000)
        assert insights.date_range is not None
        assert "2024" in insights.date_range[0]
        assert "2025" in insights.date_range[1]

    def test_basic_stats_procesos(self, sample_procesos):
        insights = _compute_from_sample(sample_procesos, total_count=6)
        assert insights.total_count == 6
        assert insights.sample_size == 3
        assert insights.computed_from == "sample"
        assert len(insights.top_entities) == 2
        assert "HOSPITAL" in insights.top_entities[0][0]
        assert insights.value_stats is not None
        assert insights.value_stats["min"] == pytest.approx(150_000_000)
        assert insights.value_stats["max"] == pytest.approx(500_000_000)

    def test_empty_results(self):
        insights = _compute_from_sample([], total_count=0)
        assert insights.total_count == 0
        assert insights.sample_size == 0
        assert insights.top_entities == []
        assert insights.value_stats is None
        assert insights.date_range is None
        assert insights.temporal_distribution == {}

    def test_top_modalities_and_temporal(self, sample_contratos):
        insights = _compute_from_sample(sample_contratos, total_count=5)
        assert len(insights.top_modalities) >= 2
        assert any("Licitacion" in m for m, _ in insights.top_modalities)
        assert 2024 in insights.temporal_distribution
        assert 2025 in insights.temporal_distribution
        assert insights.temporal_distribution[2025] == 3

    def test_single_result(self):
        row = make_contrato(entidad="SENA", valor=1_000_000, modalidad="Directa", fecha="2025-01-01T00:00:00")
        insights = _compute_from_sample([row], total_count=1)
        assert insights.sample_size == 1
        assert len(insights.top_entities) == 1
        assert insights.top_entities[0][0] == "SENA"
        assert insights.value_stats["mean"] == 1_000_000
        assert insights.value_stats["median"] == 1_000_000


# ── _derive_signals ───────────────────────────────────────────────────────

class TestDeriveSignals:
    def test_dominant_entity_above_40pct(self):
        raw = {
            "top_entities": [("INVIAS", 30), ("ANLA", 10)],
            "value_stats": None,
            "top_modalities": [("LP", 20)],
            "temporal_dist": {2025: 25},
        }
        signals = _derive_signals(raw, total_count=50)
        assert signals["has_dominant_entity"] is True
        assert signals["dominant_entity_name"] == "INVIAS"
        assert signals["dominant_entity_pct"] == 60.0

    def test_no_dominant_entity(self):
        raw = {
            "top_entities": [("INVIAS", 15), ("ANLA", 12), ("SENA", 10)],
            "value_stats": None,
            "top_modalities": [],
            "temporal_dist": {},
        }
        signals = _derive_signals(raw, total_count=50)
        assert signals["has_dominant_entity"] is False
        assert signals["dominant_entity_name"] is None

    def test_temporal_concentration_above_60pct(self):
        raw = {
            "top_entities": [],
            "value_stats": None,
            "top_modalities": [],
            "temporal_dist": {2025: 100, 2024: 20, 2023: 10},
        }
        signals = _derive_signals(raw, total_count=130)
        assert signals["has_temporal_concentration"] is True
        assert signals["dominant_year"] == 2025
        assert signals["dominant_year_pct"] == pytest.approx(76.9, rel=0.1)

    def test_no_temporal_concentration(self):
        raw = {
            "top_entities": [],
            "value_stats": None,
            "top_modalities": [],
            "temporal_dist": {2025: 40, 2024: 35, 2023: 25},
        }
        signals = _derive_signals(raw, total_count=100)
        assert signals["has_temporal_concentration"] is False

    def test_value_outlier_above_5x_median(self):
        raw = {
            "top_entities": [],
            "value_stats": {"mean": 200_000_000, "median": 100_000_000, "min": 10_000_000, "max": 2_300_000_000},
            "top_modalities": [],
            "temporal_dist": {},
        }
        signals = _derive_signals(raw, total_count=100)
        assert signals["has_value_outlier"] is True
        assert signals["outlier_value"] == 2_300_000_000

    def test_no_value_outlier(self):
        raw = {
            "top_entities": [],
            "value_stats": {"mean": 200_000_000, "median": 100_000_000, "min": 10_000_000, "max": 400_000_000},
            "top_modalities": [],
            "temporal_dist": {},
        }
        signals = _derive_signals(raw, total_count=100)
        assert signals["has_value_outlier"] is False

    def test_value_outlier_without_median(self):
        raw = {
            "top_entities": [],
            "value_stats": {"mean": 200_000_000, "median": None, "min": 10_000_000, "max": 2_300_000_000},
            "top_modalities": [],
            "temporal_dist": {},
        }
        signals = _derive_signals(raw, total_count=100)
        assert signals["has_value_outlier"] is False

    def test_diverse_modalities_top_lt_50pct(self):
        raw = {
            "top_entities": [],
            "value_stats": None,
            "top_modalities": [("Licitacion", 20), ("Directa", 15), ("Abreviada", 10)],
            "temporal_dist": {},
        }
        signals = _derive_signals(raw, total_count=60)
        assert signals["has_diverse_modalities"] is True

    def test_not_diverse_modalities(self):
        raw = {
            "top_entities": [],
            "value_stats": None,
            "top_modalities": [("Licitacion", 50), ("Directa", 5)],
            "temporal_dist": {},
        }
        signals = _derive_signals(raw, total_count=60)
        assert signals["has_diverse_modalities"] is False

    def test_empty_raw_data(self):
        raw = {
            "top_entities": [],
            "value_stats": None,
            "top_modalities": [],
            "temporal_dist": {},
        }
        signals = _derive_signals(raw, total_count=0)
        assert signals["has_dominant_entity"] is False
        assert signals["has_temporal_concentration"] is False
        assert signals["has_value_outlier"] is False
        assert signals["has_diverse_modalities"] is False


# ── observe() entry point ─────────────────────────────────────────────────

class TestObserveEntryPoint:
    def test_uses_sample_when_total_count_50_or_less(self, sample_contratos, soql_builder):
        client = MagicMock()
        insights = observe(sample_contratos, total_count=10, dataset_id="jbjy-vk9h",
                           params={}, secop_client=client, soql_builder=soql_builder)
        assert insights is not None
        assert insights.computed_from == "sample"
        client.aggregate.assert_not_called()

    def test_uses_aggregation_when_total_count_above_50(self, sample_contratos, soql_builder):
        client = MagicMock()
        # Mock all 5 aggregation results
        client.aggregate.side_effect = [
            [{"nombre_entidad": "INVIAS", "cnt": "100"}, {"nombre_entidad": "ANLA", "cnt": "50"}],  # top_entities
            [{"mean": "500000000", "min_val": "10000", "max_val": "5000000000"}],  # value_stats
            [{"modalidad_de_contratacion": "LP", "cnt": "80"}, {"modalidad_de_contratacion": "CD", "cnt": "40"}],  # top_modalities
            [{"date_min": "2024-01-01", "date_max": "2025-12-31"}],  # date_range
            [{"yr": "2025", "cnt": "70"}, {"yr": "2024", "cnt": "50"}],  # temporal_dist
        ]
        insights = observe(sample_contratos, total_count=200, dataset_id="jbjy-vk9h",
                           params={}, secop_client=client, soql_builder=soql_builder)
        assert insights is not None
        assert insights.computed_from == "aggregation"
        assert client.aggregate.call_count == 5

    def test_returns_none_on_critical_failure(self, soql_builder):
        client = MagicMock()
        # Force exception in _compute_from_aggregation
        client.aggregate.side_effect = RuntimeError("SECOP down")
        insights = observe([], total_count=200, dataset_id="jbjy-vk9h",
                           params={}, secop_client=client, soql_builder=soql_builder)
        assert insights is None

    def test_sample_executes_no_network(self, soql_builder):
        client = MagicMock()
        insights = observe([], total_count=0, dataset_id="jbjy-vk9h",
                           params={}, secop_client=client, soql_builder=soql_builder)
        assert insights is not None
        assert insights.computed_from == "sample"
        client.aggregate.assert_not_called()

    def test_aggregation_partial_timeout(self, soql_builder):
        """Some queries timeout, others return — best-effort gives partial insights."""
        client = MagicMock()
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
            raise RuntimeError("timeout")  # top_modalities, date_range fail
        client.aggregate.side_effect = side_effect
        insights = observe([], total_count=200, dataset_id="jbjy-vk9h",
                           params={}, secop_client=client, soql_builder=soql_builder)
        assert insights is not None
        assert insights.computed_from == "aggregation"
        assert len(insights.top_entities) == 1
        assert insights.top_modalities == []  # failed query
        assert insights.value_stats is not None
        assert insights.date_range is not None


# ── UniverseInsights dataclass ─────────────────────────────────────────────

class TestUniverseInsightsDataclass:
    def test_all_fields_present(self, sample_contratos):
        insights = _compute_from_sample(sample_contratos, total_count=5)
        # Required fields
        assert hasattr(insights, "total_count")
        assert hasattr(insights, "sample_size")
        assert hasattr(insights, "date_range")
        assert hasattr(insights, "top_entities")
        assert hasattr(insights, "value_stats")
        assert hasattr(insights, "top_modalities")
        assert hasattr(insights, "temporal_distribution")
        assert hasattr(insights, "computed_from")
        # Signal fields
        assert hasattr(insights, "has_dominant_entity")
        assert hasattr(insights, "dominant_entity_name")
        assert hasattr(insights, "dominant_entity_pct")
        assert hasattr(insights, "has_temporal_concentration")
        assert hasattr(insights, "dominant_year")
        assert hasattr(insights, "dominant_year_pct")
        assert hasattr(insights, "has_value_outlier")
        assert hasattr(insights, "outlier_value")
        assert hasattr(insights, "has_diverse_modalities")

    def test_signal_values_within_sample(self, sample_contratos):
        insights = _compute_from_sample(sample_contratos, total_count=5)
        # INVIAS is 3/5 = 60% → dominant
        assert insights.has_dominant_entity is True
        assert insights.dominant_entity_name == "INVIAS"
        assert insights.dominant_entity_pct == 60.0
        # 2025 is 3/5 = 60% → NOT dominant (needs >60%)
        assert insights.has_temporal_concentration is False
        # max=2.3B, median=100M → 23x → outlier
        assert insights.has_value_outlier is True
        # top modality = Licitacion 3/5 = 60% → NOT diverse (<50%)
        assert insights.has_diverse_modalities is False
