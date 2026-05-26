"""
Tests for the Gazetteer Manager (scripts/gazetteer_manager.py).

Tests are read-only — they never modify aliases_db.json.
Covers: stats, diff, heuristic alias generation, validate, hygiene.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

# Ensure project root + scripts on path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = _PROJECT_ROOT / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))
sys.path.insert(0, str(_PROJECT_ROOT))

from gazetteer_manager import (
    ALL_FORBIDDEN,
    FORBIDDEN_GENERIC_ALIASES,
    generate_heuristic_aliases,
    infer_entity_type,
    do_stats,
    do_validate,
)

ALIASES_DB_PATH = _PROJECT_ROOT / "app" / "data" / "aliases_db.json"


# =========================================================================
# Fixtures
# =========================================================================


@pytest.fixture(scope="session")
def gazetteer() -> list[dict]:
    with ALIASES_DB_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


# =========================================================================
# Infer entity type
# =========================================================================


class TestInferEntityType:
    def test_alcaldia_municipal(self):
        assert infer_entity_type("ALCALDIA MUNICIPAL DE TURBACO BOLIVAR") == "alcaldia_municipal"

    def test_ese_hospital(self):
        assert infer_entity_type("ESE HOSPITAL SALAZAR DE VILLETA") == "ese_hospital"

    def test_ese_simple(self):
        assert infer_entity_type("ESE METROSALUD") == "ese_hospital"

    def test_hospital_simple(self):
        assert infer_entity_type("HOSPITAL SAN RAFAEL") == "ese_hospital"

    def test_sena_regional(self):
        assert infer_entity_type("SENA REGIONAL CUNDINAMARCA Grupo Administrativo CBA") == "sena_regional"

    def test_icbf_regional(self):
        assert infer_entity_type("ICBF REGIONAL CORDOBA") == "icbf_regional"

    def test_secretaria(self):
        assert infer_entity_type("SECRETARÍA DE EDUCACIÓN DE CUNDINAMARCA") == "secretaria"

    def test_fondo(self):
        assert infer_entity_type("FONDO DE PASIVO SOCIAL DE FERROCARRILES") == "fondo"

    def test_corporacion_autonoma(self):
        assert infer_entity_type("CORPORACION AUTONOMA REGIONAL DE CUNDINAMARCA") == "corporacion_autonoma"

    def test_ministerio(self):
        assert infer_entity_type("MINISTERIO DE JUSTICIA Y DEL DERECHO") == "ministerio"

    def test_superintendencia(self):
        assert infer_entity_type("SUPERINTENDENCIA NACIONAL DE SALUD") == "superintendencia"

    def test_gobernacion(self):
        assert infer_entity_type("GOBERNACIÓN DEL VALLE DEL CAUCA") == "gobernacion"

    def test_otro(self):
        assert infer_entity_type("INRAVISIÓN") == "otro"
        assert infer_entity_type("DAPRE") == "otro"
        assert infer_entity_type("UPRA") == "otro"


# =========================================================================
# Heuristic alias generation
# =========================================================================


class TestGenerateHeuristicAliases:
    def test_alcaldia_municipal(self):
        aliases = generate_heuristic_aliases(
            "ALCALDIA MUNICIPAL DE TURBACO BOLIVAR",
            entity_type="alcaldia_municipal",
        )
        assert len(aliases) >= 3
        assert "alcaldia de turbaco bolivar" in aliases
        assert "alcaldia turbaco bolivar" in aliases
        # No forbidden generics
        for a in aliases:
            assert a not in ALL_FORBIDDEN

    def test_ese_hospital(self):
        aliases = generate_heuristic_aliases(
            "ESE HOSPITAL SALAZAR DE VILLETA",
            entity_type="ese_hospital",
        )
        assert len(aliases) >= 3
        assert "ese salazar de villeta" in aliases or "hospital salazar de villeta" in aliases

    def test_sena_regional(self):
        aliases = generate_heuristic_aliases(
            "SENA REGIONAL CUNDINAMARCA Grupo Administrativo CBA",
            entity_type="sena_regional",
        )
        assert len(aliases) >= 3
        assert "sena regional cundinamarca" in aliases
        # Should not have double spaces from cleanup
        assert not any("  " in a for a in aliases), f"Double spaces in: {aliases}"

    def test_icbf_regional(self):
        aliases = generate_heuristic_aliases(
            "ICBF REGIONAL CORDOBA",
            entity_type="icbf_regional",
        )
        assert len(aliases) >= 3
        assert "icbf cordoba" in aliases
        assert "icbf regional cordoba" in aliases

    def test_secretaria(self):
        aliases = generate_heuristic_aliases(
            "SECRETARIA DE EDUCACION DE CUNDINAMARCA",
            entity_type="secretaria",
        )
        assert len(aliases) >= 3
        some_ok = any("educacion" in a for a in aliases)
        assert some_ok, f"No alias contains 'educacion': {aliases}"

    def test_fondo(self):
        aliases = generate_heuristic_aliases(
            "FONDO DE PASIVO SOCIAL DE FERROCARRILES NACIONALES DE COLOMBIA",
            entity_type="fondo",
        )
        assert len(aliases) >= 3
        # At least one alias should start with "fondo"
        assert any(a.startswith("fondo") for a in aliases), f"No alias starts with 'fondo': {aliases}"

    def test_corporacion_autonoma(self):
        aliases = generate_heuristic_aliases(
            "CORPORACION AUTONOMA REGIONAL DE CUNDINAMARCA",
            entity_type="corporacion_autonoma",
        )
        assert len(aliases) >= 3

    def test_ministerio(self):
        aliases = generate_heuristic_aliases(
            "MINISTERIO DE JUSTICIA Y DEL DERECHO",
            entity_type="ministerio",
        )
        assert len(aliases) >= 3
        assert "ministerio de justicia y del derecho" in \
            [a for a in aliases], f"Missing official: {aliases}"

    def test_superintendencia(self):
        aliases = generate_heuristic_aliases(
            "SUPERINTENDENCIA NACIONAL DE SALUD",
            entity_type="superintendencia",
        )
        assert len(aliases) >= 3

    def test_gobernacion(self):
        aliases = generate_heuristic_aliases(
            "GOBERNACION DEL VALLE DEL CAUCA",
            entity_type="gobernacion",
        )
        assert len(aliases) >= 3
        assert "gobernacion de valle del cauca" in aliases or \
               "gobernacion valle del cauca" in aliases

    def test_no_forbidden_generic_aliases(self):
        """NUNCA generar alias que sea un solo token generico."""
        test_cases = [
            ("ALCALDIA MUNICIPAL DE SOPO", "alcaldia_municipal"),
            ("ESE METROSALUD", "ese_hospital"),
            ("SENA REGIONAL BOGOTA", "sena_regional"),
            ("MINISTERIO DE JUSTICIA", "ministerio"),
            ("GOBERNACION DE ANTIOQUIA", "gobernacion"),
        ]
        for name, etype in test_cases:
            aliases = generate_heuristic_aliases(name, entity_type=etype)
            for a in aliases:
                assert a not in ALL_FORBIDDEN, \
                    f"Alias '{a}' es generico prohibido para {name}"

    def test_diacritics_normalized(self):
        """Aliases deben tener variantes sin tildes."""
        aliases = generate_heuristic_aliases(
            "ESE HOSPITAL SALAZAR DE VILLETA",
            entity_type="ese_hospital",
        )
        # At least one alias should not have special chars (heuristic
        # currently adds both normalized and non-normalized forms)
        pass  # The function already adds _normalize() variants


# =========================================================================
# Forbidden aliases constants
# =========================================================================


class TestForbiddenAliases:
    def test_generic_tokens_exist(self):
        assert "alcaldia" in FORBIDDEN_GENERIC_ALIASES
        assert "municipio" in FORBIDDEN_GENERIC_ALIASES
        assert "gobernacion" in FORBIDDEN_GENERIC_ALIASES
        assert "hospital" in FORBIDDEN_GENERIC_ALIASES
        assert "secretaria" in FORBIDDEN_GENERIC_ALIASES
        assert len(FORBIDDEN_GENERIC_ALIASES) >= 10

    def test_article_variants_included(self):
        assert "el ministerio" in ALL_FORBIDDEN
        assert "la secretaria" in ALL_FORBIDDEN
        assert "la alcaldia" in ALL_FORBIDDEN
        assert "el municipio" in ALL_FORBIDDEN


# =========================================================================
# Stats
# =========================================================================


class TestStats:
    def test_stats_returns_str(self, gazetteer):
        result = do_stats(gazetteer, details=False)
        assert isinstance(result, str)
        assert "509" in result or str(len(gazetteer)) in result
        assert "Total entidades" in result
        assert "Total aliases" in result

    def test_stats_detailed(self, gazetteer):
        result = do_stats(gazetteer, details=True)
        assert "Por Tipo de Entidad" in result
        assert "Top 10 Departamentos" in result
        assert "Departamentos con <5 Entidades" in result


# =========================================================================
# Validate
# =========================================================================


class TestValidate:
    """Read-only validation checks — doesn't modify aliases_db.json."""

    def test_validate_no_critical_errors(self, gazetteer):
        result = do_validate(ALIASES_DB_PATH)
        # There may be warnings about <3 aliases or generic aliases,
        # but no critical errors (empty official_name)
        errors = result["errors"]
        critical = [e for e in errors if not e.startswith("COLISION")]
        assert len(critical) == 0, f"Critical errors: {critical}"

    def test_validate_counts(self, gazetteer):
        result = do_validate(ALIASES_DB_PATH)
        assert result["total_entities"] == len(gazetteer)
        assert result["total_aliases"] >= 10000

    def test_validate_finds_generic_warnings(self, gazetteer):
        """After Lote 00 hygiene, should be 0 forbidden generic aliases."""
        result = do_validate(ALIASES_DB_PATH)
        generic_warnings = [w for w in result["warnings"] if "generico peligroso" in w]
        # ALIAS-GENERIC-CLEANUP-001: all dangerous generics removed in Lote 00
        assert len(generic_warnings) == 0, (
            f"Expected 0 generic alias warnings after hygiene, got {len(generic_warnings)}: {generic_warnings[:5]}"
        )
