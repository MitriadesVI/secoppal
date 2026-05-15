"""Tests para suggester.py — generación de sugerencias accionables."""
from __future__ import annotations

import pytest

from app.core.suggester import Suggestion, generate_suggestions


class TestSuggestionDataclass:
    def test_minimal_suggestion(self):
        s = Suggestion(label="Ver solo contratos", modified_params={"estado": "Celebrado"})
        assert s.label == "Ver solo contratos"
        assert s.modified_params == {"estado": "Celebrado"}
        assert s.expected_count is None
        assert s.reason == ""

    def test_full_suggestion(self):
        s = Suggestion(label="Top", modified_params={}, expected_count=42, reason="test")
        assert s.expected_count == 42
        assert s.reason == "test"


class TestGenerateSuggestions:
    def test_contratos_suggests_estado_when_no_estado(self):
        suggestions = generate_suggestions(
            params={}, universe_insights=None, total_count=100,
            rows=[{"a": 1}], dataset_id="jbjy-vk9h",
        )
        labels = [s.label for s in suggestions]
        assert any("activos" in l.lower() for l in labels)

    def test_estado_families_have_list_values(self):
        suggestions = generate_suggestions(
            params={}, universe_insights=None, total_count=100,
            rows=[{"a": 1}], dataset_id="jbjy-vk9h",
        )
        for s in suggestions[:3]:
            if "estado" in s.modified_params:
                val = s.modified_params["estado"]
                assert isinstance(val, list), f"{s.label}: estado should be list, got {type(val)}"

    def test_procesos_no_estado_suggestion(self):
        suggestions = generate_suggestions(
            params={}, universe_insights=None, total_count=100,
            rows=[{"a": 1}], dataset_id="p6dx-8zbt",
        )
        labels = [s.label for s in suggestions]
        assert not any("activos" in l.lower() for l in labels)

    def test_suggests_ordering_when_not_already_ordered(self):
        suggestions = generate_suggestions(
            params={}, universe_insights=None, total_count=100,
            rows=[{"a": 1}], dataset_id="jbjy-vk9h",
        )
        labels = [s.label for s in suggestions]
        assert any("valor" in l.lower() for l in labels)

    def test_skips_ordering_when_already_set(self):
        suggestions = generate_suggestions(
            params={"ordering_signal": "valor_desc"}, universe_insights=None,
            total_count=100, rows=[{"a": 1}], dataset_id="jbjy-vk9h",
        )
        labels = [s.label for s in suggestions]
        assert not any("valor" in l.lower() for l in labels)

    def test_suggester_no_contratista(self):
        """Agrupar por contratista removed — deferred to v1.3."""
        suggestions = generate_suggestions(
            params={}, universe_insights=None, total_count=100,
            rows=[{"a": 1}], dataset_id="jbjy-vk9h",
        )
        labels = [s.label for s in suggestions]
        assert not any("contratista" in l.lower() for l in labels)

    def test_suggests_dataset_switch(self):
        suggestions = generate_suggestions(
            params={}, universe_insights=None, total_count=50,
            rows=[], dataset_id="p6dx-8zbt",  # procesos, no tiene contratista/estado sugerencias
        )
        labels = [s.label for s in suggestions]
        assert any("contratos" in l.lower() for l in labels)

    def test_estado_families_fill_when_no_competition(self):
        schema = {"orden": "mas recientes", "contratista": "SAS"}
        suggestions = generate_suggestions(
            params=schema, universe_insights=None, total_count=0,
            rows=[], dataset_id="jbjy-vk9h",
        )
        labels = [s.label for s in suggestions]
        assert any("activos" in l.lower() for l in labels)
        assert any("ejecución" in l.lower() for l in labels)
        assert any("suspendidos" in l.lower() for l in labels)
        # terminados no debería entrar (4 items, max 3)

    def test_max_3_suggestions(self):
        suggestions = generate_suggestions(
            params={"contratista": "SAS", "ordering_signal": "valor_desc"},
            universe_insights=None, total_count=50,
            rows=[], dataset_id="jbjy-vk9h",
        )
        assert len(suggestions) <= 3

    def test_empty_params_no_crash(self):
        suggestions = generate_suggestions(
            params=None, universe_insights=None, total_count=0,
            rows=[], dataset_id="jbjy-vk9h",
        )
        assert isinstance(suggestions, list)
