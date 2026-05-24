"""Tests Observer 2.0 — nuevos signals enriquecidos."""
from __future__ import annotations

from app.core.observer import UniverseInsights, _derive_signals


def test_entity_concentration_computed_always():
    """entity_concentration_pct siempre se computa cuando hay entidades."""
    raw = {
        "top_entities": [("INVIAS", 65)],
        "value_stats": None,
        "top_modalities": [],
        "temporal_dist": {},
        "temporal_dist_quarter": {},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["entity_concentration_name"] == "INVIAS"
    assert signals["entity_concentration_pct"] == 65.0


def test_modality_dominance_when_over_50pct():
    """modality_dominance solo aparece si top modality >= 50%."""
    raw = {
        "top_entities": [],
        "value_stats": None,
        "top_modalities": [("Licitacion Publica", 78)],
        "temporal_dist": {},
        "temporal_dist_quarter": {},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["modality_dominance_name"] == "Licitacion Publica"
    assert signals["modality_dominance_pct"] == 78.0


def test_modality_dominance_skipped_when_under_50pct():
    """modality_dominance es None cuando top modality < 50%."""
    raw = {
        "top_entities": [],
        "value_stats": None,
        "top_modalities": [("Contratacion Directa", 30)],
        "temporal_dist": {},
        "temporal_dist_quarter": {},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["modality_dominance_name"] is None
    assert signals["modality_dominance_pct"] is None


def test_temporal_skew_quarter_when_over_70pct():
    """temporal_skew detecta trimestre con >70% de concentración."""
    raw = {
        "top_entities": [],
        "value_stats": None,
        "top_modalities": [],
        "temporal_dist": {},
        "temporal_dist_quarter": {"Q1 2026": 82},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["temporal_skew_quarter"] == "Q1 2026"
    assert signals["temporal_skew_pct"] == 82.0


def test_temporal_skew_fallback_year():
    """Cuando no hay quarter_dist, usa años como fallback."""
    raw = {
        "top_entities": [],
        "value_stats": None,
        "top_modalities": [],
        "temporal_dist": {2026: 85},
        "temporal_dist_quarter": {},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["temporal_skew_quarter"] == "2026"
    assert signals["temporal_skew_pct"] == 85.0


def test_temporal_skew_skipped_when_under_70pct():
    """temporal_skew es None cuando concentración <= 70%."""
    raw = {
        "top_entities": [],
        "value_stats": None,
        "top_modalities": [],
        "temporal_dist": {2026: 50},
        "temporal_dist_quarter": {},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["temporal_skew_quarter"] is None
    assert signals["temporal_skew_pct"] is None


def test_value_outlier_ratio_when_max_over_10x_mean():
    """value_outlier_ratio señala cuando max > 10x mean."""
    raw = {
        "top_entities": [],
        "value_stats": {"mean": 100_000_000, "max": 2_000_000_000},
        "top_modalities": [],
        "temporal_dist": {},
        "temporal_dist_quarter": {},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["has_value_outlier_ratio"] is True
    assert signals["value_outlier_ratio"] == 20.0


def test_value_outlier_ratio_skipped_when_under_10x():
    """value_outlier_ratio es None cuando max <= 10x mean."""
    raw = {
        "top_entities": [],
        "value_stats": {"mean": 100_000_000, "max": 500_000_000},
        "top_modalities": [],
        "temporal_dist": {},
        "temporal_dist_quarter": {},
    }
    signals = _derive_signals(raw, total_count=100)
    assert signals["has_value_outlier_ratio"] is False
    assert signals["value_outlier_ratio"] is None


def test_insights_dataclass_defaults():
    """UniverseInsights acepta los nuevos campos opcionales."""
    ui = UniverseInsights(
        total_count=100,
        sample_size=50,
        date_range=None,
        top_entities=[("INVIAS", 65)],
        value_stats=None,
        top_modalities=[("LP", 78)],
        temporal_distribution={2026: 100},
        computed_from="sample",
        has_dominant_entity=True,
        dominant_entity_name="INVIAS",
        dominant_entity_pct=65.0,
        has_temporal_concentration=False,
        dominant_year=None,
        dominant_year_pct=None,
        has_value_outlier=False,
        outlier_value=None,
        has_diverse_modalities=False,
        entity_concentration_pct=65.0,
        entity_concentration_name="INVIAS",
        modality_dominance_name="LP",
        modality_dominance_pct=78.0,
        temporal_skew_quarter="Q1 2026",
        temporal_skew_pct=82.0,
        value_outlier_ratio=20.0,
        has_value_outlier_ratio=True,
    )
    assert ui.entity_concentration_pct == 65.0
    assert ui.modality_dominance_pct == 78.0
