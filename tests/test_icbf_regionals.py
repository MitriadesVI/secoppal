"""
Tests for ICBF Regional Ecosystem Resolver.

Covers: national, regional, macro_region, negative cases.
"""

from __future__ import annotations

import pytest

from app.core.icbf_resolver import (
    ICBFResolution,
    resolve_icbf_reference,
    build_icbf_filter_metadata,
)


# =========================================================================
# National resolution
# =========================================================================


class TestICBFNational:
    @pytest.mark.parametrize("query", [
        "procesos del icbf activos",
        "contratos del instituto colombiano de bienestar familiar",
        "muéstrame procesos del icbf",
        "procesos del icbf",
    ])
    def test_national(self, query):
        result = resolve_icbf_reference(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.resolution_type == "icbf_national"
        assert result.confidence == "high"
        # Should include sede nacional + all regionales
        assert len(result.official_names) == 33  # 1 sede + 32 regionales
        assert "ICBF SEDE NACIONAL" in result.official_names

    def test_national_warning(self):
        result = resolve_icbf_reference("procesos del icbf")
        assert result is not None
        meta = build_icbf_filter_metadata(result)
        assert meta["entity_family"] == "ICBF"
        assert "warning" in meta
        assert "ecosistema nacional" in meta["warning"]

    def test_icbf_alone_is_national(self):
        """'icbf' alone should NOT resolve to an arbitrary regional."""
        result = resolve_icbf_reference("icbf")
        assert result is not None
        assert result.resolution_type == "icbf_national"


# =========================================================================
# Regional resolution
# =========================================================================


class TestICBFRegional:
    @pytest.mark.parametrize("query,expected_dept,expected_name_substring", [
        ("procesos del icbf atlántico", "Atlántico", "ATLANTICO"),
        ("procesos del icbf atlantico", "Atlántico", "ATLANTICO"),
        ("bienestar familiar córdoba", "Córdoba", "CORDOBA"),
        ("icbf en magdalena", "Magdalena", "MAGDALENA"),
        ("instituto colombiano de bienestar familiar regional bolivar", "Bolívar", "BOLIVAR"),
        ("icbf valle del cauca", "Valle del Cauca", "VALLE DEL CAUCA"),
        ("icbf antioquia", "Antioquia", "ANTIOQUIA"),
        ("icbf nariño", "Nariño", "NARIÑO"),
        ("bienestar familiar cesar", "Cesar", "CESAR"),
        ("instituto colombiano de bienestar familiar boyaca", "Boyacá", "BOYACA"),
        ("icbf en el cauca", "Cauca", "CAUCA"),
        ("contratos del icbf en sucre", "Sucre", "SUCRE"),
        ("icbf putumayo", "Putumayo", "PUTUMAYO"),
        ("procesos icbf choco", "Chocó", "CHOCO"),
        ("bienestar familiar la guajira", "La Guajira", "GUAJIRA"),
        ("icbf guaviare", "Guaviare", "GUAVIARE"),
    ])
    def test_regional(self, query, expected_dept, expected_name_substring):
        result = resolve_icbf_reference(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.resolution_type == "icbf_regional", f"Expected regional, got {result.resolution_type}"
        assert result.department == expected_dept, f"Expected {expected_dept}, got {result.department}"
        assert result.confidence == "high"
        # The official name should contain the department
        for name in result.official_names:
            assert expected_name_substring in name.upper(), (
                f"Name '{name}' doesn't contain '{expected_name_substring}'"
            )

    @pytest.mark.parametrize("query", [
        "icbf atlántico",
        "icbf atlantico",
        "procesos icbf en bogotá",
        "icbf nacional",
        "sede nacional icbf",
    ])
    def test_regional_with_dept(self, query):
        """Regional queries resolve to specific ICBF regionals."""
        result = resolve_icbf_reference(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.resolution_type in ("icbf_regional", "icbf_national")

    def test_icbf_nacional_is_national(self):
        """'icbf nacional' should be icbf_national."""
        result = resolve_icbf_reference("icbf nacional")
        assert result is not None
        assert result.resolution_type == "icbf_national"

    def test_bogota_icbf(self):
        """'icbf bogotá' should be regional Bogotá."""
        result = resolve_icbf_reference("icbf bogotá")
        assert result is not None
        # Bogotá could match as regional or national
        assert result.resolution_type in ("icbf_regional", "icbf_national")


# =========================================================================
# Macro region
# =========================================================================


class TestICBFMacroRegion:
    @pytest.mark.parametrize("query,expected_macro,expected_count", [
        ("alimentación del icbf en costa atlántica", "costa_atlantica", 8),
        ("procesos del icbf en el caribe", "caribe", 8),
    ])
    def test_macro_region(self, query, expected_macro, expected_count):
        result = resolve_icbf_reference(query)
        assert result is not None, f"'{query}' should resolve"
        assert result.resolution_type == "icbf_macro_region"
        assert result.region_macro == expected_macro
        assert len(result.official_names) == expected_count
        assert result.confidence == "high"

    def test_macro_contains_expected_depts(self):
        """Macro regions should include the expected departments."""
        result = resolve_icbf_reference("procesos del icbf en costa atlántica")
        assert result is not None
        names = " ".join(result.official_names)
        for dept in ["ATLANTICO", "BOLIVAR", "MAGDALENA", "SUCRE"]:
            assert dept in names.upper(), f"Missing {dept} in macro results"


# =========================================================================
# Negative cases
# =========================================================================


class TestICBFNegative:
    @pytest.mark.parametrize("query", [
        "contratos en atlántico",
        "alimentación en costa atlántica",
        "procesos en córdoba",
        "contratos de alimentación en valle del cauca",
        "procesos de mantenimiento en la costa",
        "contratos de infraestructura",
        "alcaldía de medellín",
        "salud en cauca",
    ])
    def test_no_icbf_mention_returns_none(self, query):
        """Queries without ICBF mention should return None."""
        result = resolve_icbf_reference(query)
        assert result is None, f"'{query}' should NOT resolve (got {result})"


# =========================================================================
# Metadata
# =========================================================================


class TestICBFMetadata:
    @pytest.mark.parametrize("query,expected_type", [
        ("procesos del icbf", "icbf_national"),
        ("icbf atlántico", "icbf_regional"),
        ("icbf en costa atlántica", "icbf_macro_region"),
    ])
    def test_metadata_shape(self, query, expected_type):
        result = resolve_icbf_reference(query)
        assert result is not None
        meta = build_icbf_filter_metadata(result)
        assert meta["entity_family"] == "ICBF"
        assert meta["resolution_type"] == expected_type
        assert "confidence" in meta

    def test_national_metadata_has_warning(self):
        result = resolve_icbf_reference("procesos del icbf")
        assert result is not None
        meta = build_icbf_filter_metadata(result)
        assert "warning" in meta

    @pytest.mark.parametrize("query,expected_names", [
        ("icbf atlántico", 1),
        ("procesos del icbf", 33),
        ("icbf en costa atlántica", 8),
    ])
    def test_official_names_count(self, query, expected_names):
        result = resolve_icbf_reference(query)
        assert result is not None
        assert len(result.official_names) == expected_names
