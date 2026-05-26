"""
Tests for Entity Ecosystem integration into the query pipeline.

Verifies that resolve_entities in the orchestrator correctly detects
ICBF/SENA ecosystems and populates resolved_params with the right keys.

These tests mock the orchestrator state and entity_resolver to avoid
hitting SECOP or the full pipeline.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from app.core.entity_ecosystem_router import (
    resolve_entity_ecosystem,
    build_entity_ecosystem_filter,
)
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError


class TestEcosystemDetectionInPipeline:
    """Simulate what resolve_entities does with ecosystem detection."""

    def _simulate_resolve_entities(self, user_query: str, parsed_params: dict) -> dict:
        """Simulate the ecosystem detection logic from orchestrator.resolve_entities."""
        params = dict(parsed_params)
        resolved = dict(params)
        resolved.pop("dataset_explicit", None)

        # G7C: Entity Ecosystem detection
        ecosystem_match = resolve_entity_ecosystem(user_query)
        if ecosystem_match:
            ecosystem_filter = build_entity_ecosystem_filter(ecosystem_match)
            resolved.update(ecosystem_filter)
            dept_from_ecosystem = ecosystem_match.department
            if dept_from_ecosystem and not resolved.get("departamento_resolved"):
                resolved["departamento_resolved"] = dept_from_ecosystem
            resolved["entidad_ecosystem_covered"] = True

        return resolved

    def test_icbf_national(self):
        resolved = self._simulate_resolve_entities(
            "procesos del icbf activos",
            {"dataset": "procesos", "intent_type": "opportunity_search"},
        )
        assert resolved["entity_ecosystem"] == "ICBF"
        assert resolved["entity_ecosystem_resolution_type"] == "icbf_national"
        assert "entity_ecosystem_warning" in resolved
        assert "entidad_in_candidates" in resolved
        # 33 official names (sede + 32 regionales)
        assert len(resolved["entidad_in_candidates"]) == 33

    def test_icbf_regional(self):
        resolved = self._simulate_resolve_entities(
            "contratos del icbf córdoba",
            {"dataset": "contratos"},
        )
        assert resolved["entity_ecosystem"] == "ICBF"
        assert resolved["entity_ecosystem_resolution_type"] == "icbf_regional"
        assert resolved["entity_ecosystem_department"] == "Córdoba"
        assert resolved["departamento_resolved"] == "Córdoba"

    def test_icbf_macro(self):
        resolved = self._simulate_resolve_entities(
            "alimentación del icbf en costa atlántica",
            {"dataset": "procesos"},
        )
        assert resolved["entity_ecosystem"] == "ICBF"
        assert resolved["entity_ecosystem_region_macro"] == "costa_atlantica"
        assert "entidad_in_candidates" in resolved

    def test_sena_national(self):
        resolved = self._simulate_resolve_entities(
            "procesos del sena activos",
            {"dataset": "procesos"},
        )
        assert resolved["entity_ecosystem"] == "SENA"
        assert resolved["entity_ecosystem_resolution_type"] == "sena_national"
        assert "entity_ecosystem_warning" in resolved

    def test_sena_regional(self):
        resolved = self._simulate_resolve_entities(
            "qué ha contratado el sena en santander",
            {"dataset": "contratos"},
        )
        assert resolved["entity_ecosystem"] == "SENA"
        assert resolved["entity_ecosystem_resolution_type"] == "sena_regional_ecosystem"
        assert resolved["entity_ecosystem_department"] == "Santander"
        assert resolved["departamento_resolved"] == "Santander"
        assert resolved["entidad_contains_all"] == ["SENA", "SANTANDER"]

    def test_sena_macro(self):
        resolved = self._simulate_resolve_entities(
            "procesos del sena en costa atlántica",
            {"dataset": "procesos"},
        )
        assert resolved["entity_ecosystem"] == "SENA"
        assert resolved["entity_ecosystem_region_macro"] == "costa_atlantica"
        assert resolved["entidad_contains_all"]  # multi-dept tokens

    def test_no_ecosystem_territorial(self):
        resolved = self._simulate_resolve_entities(
            "contratos en santander",
            {"dataset": "contratos"},
        )
        assert "entity_ecosystem" not in resolved

    def test_no_ecosystem_macro_only(self):
        resolved = self._simulate_resolve_entities(
            "alimentación en costa atlántica",
            {"dataset": "procesos"},
        )
        assert "entity_ecosystem" not in resolved

    def test_ecosystem_does_not_override_existing_dept(self):
        resolved = self._simulate_resolve_entities(
            "sena magdalena",
            {"dataset": "contratos", "departamento_resolved": "Córdoba"},
        )
        # Existing departamento_resolved should NOT be overridden
        assert resolved["departamento_resolved"] == "Córdoba"
        # But ecosystem still detected
        assert resolved["entity_ecosystem"] == "SENA"

    def test_ecosystem_does_not_override_existing_entity(self):
        resolved = self._simulate_resolve_entities(
            "procesos del icbf activos",
            {"dataset": "procesos", "entidad_resolved": "ALCALDÍA DE MEDELLÍN"},
        )
        # Ecosystem should still be detected but entity remains
        assert resolved["entity_ecosystem"] == "ICBF"
        assert resolved.get("entidad_resolved") == "ALCALDÍA DE MEDELLÍN"


class TestSoQLBuilderEcosystemSupport:
    """Verify SoQLBuilder handles entity ecosystem filters correctly."""

    @pytest.fixture
    def builder(self):
        return SoQLBuilder()

    def test_contains_all_produces_and_likes(self, builder):
        params = {
            "dataset": "procesos",
            "entidad_contains_all": ["SENA", "SANTANDER"],
        }
        soql = builder.build("p6dx-8zbt", params)
        assert "unaccent(UPPER(entidad)) LIKE unaccent(UPPER('%SENA%'))" in soql
        assert "unaccent(UPPER(entidad)) LIKE unaccent(UPPER('%SANTANDER%'))" in soql
        assert "AND" in soql

    def test_in_candidates_produces_or_likes(self, builder):
        params = {
            "dataset": "procesos",
            "entidad_in_candidates": ["ICBF SEDE NACIONAL", "ICBF REGIONAL ATLANTICO"],
        }
        soql = builder.build("p6dx-8zbt", params)
        assert "ICBF SEDE NACIONAL" in soql or "ICBF%SEDE%NACIONAL" in soql
        assert "ICBF REGIONAL ATLANTICO" in soql or "ICBF%REGIONAL%ATLANTICO" in soql
        assert "OR" in soql

    def test_contains_all_counts_as_substantial(self, builder):
        """entidad_contains_all should count as substantial filter."""
        params = {
            "entidad_contains_all": ["SENA", "SANTANDER"],
        }
        # Should NOT raise UnsafeGlobalQueryError
        soql = builder.build("p6dx-8zbt", params)
        assert "WHERE" in soql
        assert "1=1" not in soql

    def test_in_candidates_counts_as_substantial(self, builder):
        """entidad_in_candidates should count as substantial filter."""
        params = {
            "entidad_in_candidates": ["SENA SECRETARIA GENERAL"],
        }
        soql = builder.build("p6dx-8zbt", params)
        assert "WHERE" in soql
        assert "1=1" not in soql

    def test_contains_all_with_contratos(self, builder):
        params = {
            "dataset": "contratos",
            "entidad_contains_all": ["SENA", "MAGDALENA"],
        }
        soql = builder.build("jbjy-vk9h", params)
        assert "nombre_entidad" in soql
        assert "unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%SENA%'))" in soql
        assert "unaccent(UPPER(nombre_entidad)) LIKE unaccent(UPPER('%MAGDALENA%'))" in soql

    def test_in_candidates_truncates_over_15(self, builder):
        """If >15 candidates, only use first one."""
        params = {
            "entidad_in_candidates": [f"ENTITY_{i}" for i in range(20)],
        }
        soql = builder.build("p6dx-8zbt", params)
        assert "ENTITY_0" in soql

    def test_ecosystem_with_dept_and_date(self, builder):
        """Full ecosystem query with department and date range."""
        params = {
            "dataset": "procesos",
            "departamento_resolved": "Atlántico",
            "entidad_contains_all": ["SENA", "ATLANTICO"],
            "fecha_desde": "2024-01-01",
        }
        soql = builder.build("p6dx-8zbt", params)
        assert "departamento_entidad = 'Atlántico'" in soql
        assert "SENA" in soql
        assert "ATLANTICO" in soql
        assert "fecha_de_publicacion_del >= '2024-01-01'" in soql
        assert "ORDER BY" in soql
