"""Tests Suggester contextual por intent_type (Sprint 3 fase 2)."""
from __future__ import annotations

import pytest

from app.core.modality_dataset_projection import (
    ModalityDatasetProjectionError,
)
from app.core.observer import UniverseInsights
from app.core.suggester import generate_suggestions


def _empty_insights() -> UniverseInsights | None:
    return None


def _modality_insights(raw_value: str) -> UniverseInsights:
    return UniverseInsights(
        total_count=100, sample_size=50, date_range=None,
        top_entities=[], value_stats=None,
        top_modalities=[(raw_value, 78)],
        temporal_distribution={}, computed_from="sample",
        has_dominant_entity=False, dominant_entity_name=None, dominant_entity_pct=None,
        has_temporal_concentration=False, dominant_year=None, dominant_year_pct=None,
        has_value_outlier=False, outlier_value=None,
        has_diverse_modalities=False,
        modality_dominance_name=raw_value,
        modality_dominance_pct=78.0,
    )


def test_opportunity_search_suggestions():
    """opportunity_search genera sugerencias de cuantía, modalidad y dataset."""
    params = {"objeto": ["obra"], "dataset": "procesos"}
    sugs = generate_suggestions(
        params=params, universe_insights=_empty_insights(),
        total_count=50, rows=[], dataset_id="p6dx-8zbt",
        intent_type="opportunity_search",
    )
    labels = [s.label for s in sugs]
    assert "Restringir a >100 millones" in labels
    assert "Ver contratos firmados" in labels


def test_opportunity_search_modalidad_from_insights():
    """Si insights reporta modality_dominance, se sugiere filtrar por ella."""
    ui = _modality_insights("Licitación pública")
    params = {
        "objeto": ["obra"],
        "dataset": "procesos",
        "modalidad_exclude_procedure_ids": ["contratacion_directa"],
    }
    sugs = generate_suggestions(
        params=params, universe_insights=ui,
        total_count=50, rows=[], dataset_id="p6dx-8zbt",
        intent_type="opportunity_search",
    )
    modality = next(
        suggestion
        for suggestion in sugs
        if suggestion.label == "Filtrar por Licitación pública"
    )
    assert modality.modified_params["modalidad_include_procedure_ids"] == [
        "licitacion_publica"
    ]
    assert "modalidad" not in modality.modified_params
    assert "modalidad_exclude_procedure_ids" not in modality.modified_params
    assert modality.modified_params["objeto"] == ["obra"]
    assert modality.modified_params["dataset"] == "procesos"


def test_unknown_dominant_modality_is_not_suggested():
    sugs = generate_suggestions(
        params={"objeto": ["obra"], "dataset": "procesos"},
        universe_insights=_modality_insights("Raw desconocido"),
        total_count=50, rows=[], dataset_id="p6dx-8zbt",
        intent_type="opportunity_search",
    )
    assert not any(
        "modalidad_include_procedure_ids" in suggestion.modified_params
        for suggestion in sugs
    )
    assert [suggestion.label for suggestion in sugs] == [
        "Restringir a >100 millones",
        "Ver contratos firmados",
        "Ordenar por mayor valor",
    ]


def test_ambiguous_raw_projection_fails_closed(monkeypatch):
    def _collision(*args, **kwargs):
        raise ModalityDatasetProjectionError("raw modality collision")

    monkeypatch.setattr(
        "app.core.suggester.procedure_id_for_raw_value",
        _collision,
    )
    with pytest.raises(
        ModalityDatasetProjectionError,
        match="raw modality collision",
    ):
        generate_suggestions(
            params={"objeto": ["obra"], "dataset": "procesos"},
            universe_insights=_modality_insights("Raw duplicado"),
            total_count=50, rows=[], dataset_id="p6dx-8zbt",
            intent_type="opportunity_search",
        )


def test_contract_search_suggestions():
    """contract_search genera sugerencias de estado, valor y dataset."""
    params = {"objeto": ["obra"], "dataset": "contratos"}
    sugs = generate_suggestions(
        params=params, universe_insights=_empty_insights(),
        total_count=50, rows=[], dataset_id="jbjy-vk9h",
        intent_type="contract_search",
    )
    labels = [s.label for s in sugs]
    assert "Ver solo adjudicados" in labels
    assert "Ordenar por mayor valor" in labels
    assert "Buscar en procesos" in labels


def test_entity_query_suggestions():
    """entity_query genera sugerencias de procesos, valor y contratos."""
    params = {"entidad_resolved": "INVIAS"}
    sugs = generate_suggestions(
        params=params, universe_insights=_empty_insights(),
        total_count=50, rows=[], dataset_id="jbjy-vk9h",
        intent_type="entity_query",
    )
    labels = [s.label for s in sugs]
    assert "Ver procesos abiertos" in labels
    assert "Ver contratos" in labels


def test_generic_fallback_when_no_intent():
    """Sin intent_type conocido, cae a sugerencias genéricas."""
    params = {"objeto": ["obra"], "dataset": "contratos"}
    sugs = generate_suggestions(
        params=params, universe_insights=_empty_insights(),
        total_count=50, rows=[], dataset_id="jbjy-vk9h",
        intent_type="",
    )
    labels = [s.label for s in sugs]
    # Debe tener al menos una sugerencia genérica
    assert len(sugs) >= 1


def test_max_three_suggestions():
    """Nunca más de 3 sugerencias."""
    params = {"objeto": ["obra"], "dataset": "contratos"}
    sugs = generate_suggestions(
        params=params, universe_insights=_empty_insights(),
        total_count=50, rows=[], dataset_id="jbjy-vk9h",
        intent_type="contract_search",
    )
    assert len(sugs) <= 3
