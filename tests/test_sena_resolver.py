"""
Tests for SENA Entity Ecosystem Resolver.

Covers: national, regional_ecosystem, macro_region, negative cases.
"""

from __future__ import annotations

import pytest

from app.core.sena_resolver import (
    SENAResolution,
    resolve_sena_reference,
    build_sena_filter_metadata,
)


# =========================================================================
# National resolution
# =========================================================================


class TestSENANational:
    @pytest.mark.parametrize("query", [
        "procesos del sena activos",
        "contratos del servicio nacional de aprendizaje",
        "sena",
        "muéstrame procesos del sena",
        "qué ha contratado el sena",
    ])
    def test_national(self, query):
        result = resolve_sena_reference(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.resolution_type == "sena_national"
        assert result.confidence == "high"

    def test_sena_alone_is_national(self):
        """'sena' alone should NOT resolve to a single arbitrary entity."""
        result = resolve_sena_reference("sena")
        assert result is not None
        assert result.resolution_type == "sena_national"
        assert len(result.official_names) >= 2  # Secretaría + Dirección

    def test_national_has_warning(self):
        result = resolve_sena_reference("procesos del sena")
        assert result is not None
        assert result.warning is not None
        assert "ecosistema nacional" in result.warning

    def test_national_metadata(self):
        result = resolve_sena_reference("procesos del sena")
        assert result is not None
        meta = build_sena_filter_metadata(result)
        assert meta["entity_family"] == "SENA"
        assert meta["resolution_type"] == "sena_national"
        assert "warning" in meta


# =========================================================================
# Regional ecosystem
# =========================================================================


class TestSENARegional:
    @pytest.mark.parametrize("query,expected_dept,expected_contains", [
        ("procesos del sena magdalena", "Magdalena", ["SENA", "MAGDALENA"]),
        ("sena regional magdalena", "Magdalena", ["SENA", "MAGDALENA"]),
        ("qué ha contratado el sena en santander", "Santander", ["SENA", "SANTANDER"]),
        ("oportunidades del sena en bolivar", "Bolívar", ["SENA", "BOLIVAR"]),
        ("sena atlantico", "Atlántico", ["SENA", "ATLANTICO"]),
        ("sena atlántico", "Atlántico", ["SENA", "ATLANTICO"]),
        ("sena valle del cauca", "Valle del Cauca", ["SENA", "VALLE"]),
        ("sena distrito capital", "Distrito Capital de Bogotá", ["SENA", "DISTRITO CAPITAL"]),
        ("sena antioquia", "Antioquia", ["SENA", "ANTIOQUIA"]),
        ("sena nariño", "Nariño", ["SENA", "NARINO"]),
        ("sena cauca", "Cauca", ["SENA", "CAUCA"]),
        ("sena cundinamarca", "Cundinamarca", ["SENA", "CUNDINAMARCA"]),
        ("sena sucre", "Sucre", ["SENA", "SUCRE"]),
        ("sena cesar", "Cesar", ["SENA", "CESAR"]),
        ("sena norte de santander", "Norte de Santander", ["SENA", "NORTE DE SANTANDER"]),
        ("sena cordoba", "Córdoba", ["SENA", "CORDOBA"]),
        ("sena guajira", "La Guajira", ["SENA", "GUAJIRA"]),
    ])
    def test_regional(self, query, expected_dept, expected_contains):
        result = resolve_sena_reference(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.resolution_type == "sena_regional_ecosystem"
        assert result.department == expected_dept, (
            f"Expected dept={expected_dept}, got {result.department}"
        )
        assert result.confidence == "high"
        for token in expected_contains:
            assert token in result.contains_all, (
                f"Missing '{token}' in contains_all: {result.contains_all}"
            )

    def test_regional_metadata(self):
        result = resolve_sena_reference("sena atlántico")
        assert result is not None
        meta = build_sena_filter_metadata(result)
        assert meta["entity_family"] == "SENA"
        assert meta["resolution_type"] == "sena_regional_ecosystem"
        assert meta["department"] == "Atlántico"
        assert "contains_all" in meta


# =========================================================================
# Macro region
# =========================================================================


class TestSENAMacroRegion:
    @pytest.mark.parametrize("query,expected_macro", [
        ("procesos del sena en costa atlántica", "costa_atlantica"),
        ("oportunidades del sena en el caribe", "caribe"),
    ])
    def test_macro_region(self, query, expected_macro):
        result = resolve_sena_reference(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.resolution_type == "sena_macro_region"
        assert result.region_macro == expected_macro
        assert result.confidence == "high"
        # Should contain multiple department tokens
        assert len(result.contains_all) > 4

    def test_macro_includes_expected_depts(self):
        result = resolve_sena_reference("procesos del sena en costa atlántica")
        assert result is not None
        all_contains = " ".join(result.contains_all).upper()
        for dept_token in ["ATLANTICO", "BOLIVAR", "MAGDALENA", "SUCRE", "CESAR", "CORDOBA"]:
            assert dept_token in all_contains, f"Missing {dept_token}"

    def test_macro_metadata(self):
        result = resolve_sena_reference("sena en costa atlántica")
        assert result is not None
        meta = build_sena_filter_metadata(result)
        assert meta["entity_family"] == "SENA"
        assert meta["resolution_type"] == "sena_macro_region"
        assert meta["region_macro"] == "costa_atlantica"


# =========================================================================
# Negative cases
# =========================================================================


class TestSENANegative:
    @pytest.mark.parametrize("query", [
        "procesos en santander",
        "contratos en magdalena",
        "alimentación en costa atlántica",
        "contratos de mantenimiento en valle del cauca",
        "formación para el trabajo en santander",
        "servicios de aprendizaje en atlántico",
        "contratos de infraestructura en bolivar",
        "procesos de salud en antioquia",
    ])
    def test_no_sena_mention_returns_none(self, query):
        """Queries without SENA mention should return None."""
        result = resolve_sena_reference(query)
        assert result is None, f"'{query}' should NOT resolve (got {result.resolution_type if result else None})"


# =========================================================================
# Edge cases
# =========================================================================


class TestSENAEdgeCases:
    def test_sena_servicio_nacional(self):
        """'servicio nacional de aprendizaje' should resolve as national."""
        result = resolve_sena_reference("contratos del servicio nacional de aprendizaje")
        assert result is not None
        assert result.resolution_type == "sena_national"

    def test_sena_bogota(self):
        """'sena bogotá' should match Distrito Capital."""
        result = resolve_sena_reference("sena bogotá")
        assert result is not None
        assert result.resolution_type == "sena_regional_ecosystem"
        assert result.department == "Distrito Capital de Bogotá"

    def test_sena_en_bogota(self):
        """'sena en bogotá' should also match."""
        result = resolve_sena_reference("sena en bogotá")
        assert result is not None
        assert result.resolution_type == "sena_regional_ecosystem"
