"""
Tests for Entity Ecosystem Router.

Covers ICBF + SENA unification and downstream filter metadata generation.
"""

from __future__ import annotations

import pytest

from app.core.entity_ecosystem_router import (
    EntityEcosystemMatch,
    resolve_entity_ecosystem,
    build_entity_ecosystem_filter,
)


class TestICBFEcosystem:
    @pytest.mark.parametrize("query,expected_type,expected_dept", [
        ("procesos del icbf activos", "icbf_national", None),
        ("contratos del instituto colombiano de bienestar familiar", "icbf_national", None),
        ("muéstrame procesos del icbf", "icbf_national", None),
    ])
    def test_icbf_national(self, query, expected_type, expected_dept):
        result = resolve_entity_ecosystem(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.family == "ICBF"
        assert result.resolution_type == expected_type
        assert result.department == expected_dept
        assert result.confidence == "high"
        assert result.warning is not None

    @pytest.mark.parametrize("query,expected_dept", [
        ("procesos del icbf atlántico", "Atlántico"),
        ("procesos del icbf atlantico", "Atlántico"),
        ("bienestar familiar córdoba", "Córdoba"),
        ("icbf en magdalena", "Magdalena"),
        ("instituto colombiano de bienestar familiar regional bolivar", "Bolívar"),
        ("icbf valle del cauca", "Valle del Cauca"),
    ])
    def test_icbf_regional(self, query, expected_dept):
        result = resolve_entity_ecosystem(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.family == "ICBF"
        assert result.resolution_type == "icbf_regional"
        assert result.department == expected_dept

    def test_icbf_macro(self):
        result = resolve_entity_ecosystem("alimentación del icbf en costa atlántica")
        assert result is not None
        assert result.family == "ICBF"
        assert result.resolution_type == "icbf_macro_region"
        assert result.region_macro == "costa_atlantica"

    def test_icbf_not_sena(self):
        """ICBF query must not be misdetected as SENA."""
        result = resolve_entity_ecosystem("icbf atlántico")
        assert result is not None
        assert result.family == "ICBF"
        assert result.resolution_type == "icbf_regional"


class TestSENAEcosystem:
    @pytest.mark.parametrize("query", [
        "procesos del sena activos",
        "contratos del servicio nacional de aprendizaje",
        "sena",
        "muéstrame procesos del sena",
    ])
    def test_sena_national(self, query):
        result = resolve_entity_ecosystem(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.family == "SENA"
        assert result.resolution_type == "sena_national"
        assert result.warning is not None
        assert result.confidence == "high"

    @pytest.mark.parametrize("query,expected_dept", [
        ("procesos del sena magdalena", "Magdalena"),
        ("sena regional magdalena", "Magdalena"),
        ("qué ha contratado el sena en santander", "Santander"),
        ("sena atlantico", "Atlántico"),
        ("sena atlántico", "Atlántico"),
        ("sena valle del cauca", "Valle del Cauca"),
    ])
    def test_sena_regional(self, query, expected_dept):
        result = resolve_entity_ecosystem(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.family == "SENA"
        assert result.resolution_type == "sena_regional_ecosystem"
        assert result.department == expected_dept

    def test_sena_macro(self):
        result = resolve_entity_ecosystem("procesos del sena en costa atlántica")
        assert result is not None
        assert result.family == "SENA"
        assert result.resolution_type == "sena_macro_region"
        assert result.region_macro == "costa_atlantica"

    def test_sena_contains_all(self):
        result = resolve_entity_ecosystem("sena santander")
        assert result is not None
        assert result.family == "SENA"
        assert result.contains_all == ["SENA", "SANTANDER"]

    def test_sena_not_icbf(self):
        """SENA query must not be misdetected as ICBF."""
        result = resolve_entity_ecosystem("sena magdalena")
        assert result is not None
        assert result.family == "SENA"


class TestNegativeCases:
    @pytest.mark.parametrize("query", [
        "contratos en atlántico",
        "alimentación en costa atlántica",
        "procesos en córdoba",
        "contratos de alimentación en valle del cauca",
        "formación para el trabajo en santander",
        "servicios de aprendizaje en atlántico",
        "procesos de infraestructura",
        "alcaldía de medellín",
    ])
    def test_no_ecosystem(self, query):
        result = resolve_entity_ecosystem(query)
        assert result is None, f"'{query}' should NOT resolve to an ecosystem"


class TestBuildFilterMetadata:
    def test_icbf_national_filter(self):
        match = resolve_entity_ecosystem("procesos del icbf activos")
        assert match is not None
        meta = build_entity_ecosystem_filter(match)
        assert meta["entity_ecosystem"] == "ICBF"
        assert meta["entity_ecosystem_resolution_type"] == "icbf_national"
        assert "entity_ecosystem_warning" in meta
        assert meta["entidad_in_candidates"]  # official names

    def test_sena_regional_filter(self):
        match = resolve_entity_ecosystem("sena santander")
        assert match is not None
        meta = build_entity_ecosystem_filter(match)
        assert meta["entity_ecosystem"] == "SENA"
        assert meta["entity_ecosystem_resolution_type"] == "sena_regional_ecosystem"
        assert meta["entity_ecosystem_department"] == "Santander"
        assert meta["entidad_contains_all"] == ["SENA", "SANTANDER"]

    def test_sena_macro_filter(self):
        match = resolve_entity_ecosystem("procesos del sena en costa atlántica")
        assert match is not None
        meta = build_entity_ecosystem_filter(match)
        assert meta["entity_ecosystem"] == "SENA"
        assert meta["entity_ecosystem_region_macro"] == "costa_atlantica"
        assert meta["entidad_contains_all"]  # multi-dept contains_all
