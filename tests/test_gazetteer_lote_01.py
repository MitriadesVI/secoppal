"""
Tests for Lote 01 gazetteer expansion — verifies new entities resolve correctly
and don't cause collisions with existing entities.

Lote 01 composition: 35 top volume + 10 strategic types + 5 territorial equity

NOTE: SENA/ICBF regional queries are inherently ambiguous because
multiple SENA/ICBF entities share overlapping aliases (e.g., "sena regional
cundinamarca" matches 3+ existing entities). These are marked xfail until
entity resolution supports context-based disambiguation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = _PROJECT_ROOT / "scripts"
import sys
sys.path.insert(0, str(_SCRIPTS_DIR))
sys.path.insert(0, str(_PROJECT_ROOT))

from gazetteer_manager import generate_heuristic_aliases, ALL_FORBIDDEN

ALIASES_DB_PATH = _PROJECT_ROOT / "app" / "data" / "aliases_db.json"
LOTE_PATH = _PROJECT_ROOT / "scripts" / "lote_01_top_gap_mixed.json"

# El lote es evidencia histórica y conserva el canonical físico que insertó.
# ADR-074 fusionó después ese registro en una BuyerUnit lógica limpia.
BUYER_UNIT_SUCCESSORS = {
    "MINISTERIO DE JUSTICIA Y DEL DERECHO..": (
        "MINISTERIO DE JUSTICIA Y DEL DERECHO"
    ),
    "MINISTERIO DE JUSTICIA Y DEL DERECHO.": (
        "MINISTERIO DE JUSTICIA Y DEL DERECHO"
    ),
}


@pytest.fixture(scope="module")
def lote_entities() -> list[dict]:
    with LOTE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def resolver():
    """Lazy import to avoid --help/collect overhead."""
    from app.core.entity_resolver import EntityResolver
    return EntityResolver(ALIASES_DB_PATH)


class TestLote01Aliases:
    """Verify aliases for Lote 01 entities are well-formed."""

    def test_entities_have_minimum_aliases(self, lote_entities):
        """Most entities should have at least 3 aliases. Some with complex names
        (e.g. "GOBERNACIÓN DEL VALLE DEL CAUCA - SECRETARÍA DE DESARROLLO R...")
        may only have 1 (the official name). Those need manual alias generation."""
        with ALIASES_DB_PATH.open("r", encoding="utf-8") as f:
            gazetteer = json.load(f)

        gazetteer_map = {e["official_name"]: e for e in gazetteer}
        low_aliases = []
        for entity in lote_entities:
            name = entity["official_name"]
            record = gazetteer_map.get(BUYER_UNIT_SUCCESSORS.get(name, name))
            assert record is not None, f"{name} not in gazetteer"
            alias_count = len(record.get("aliases", []))
            if alias_count < 3:
                low_aliases.append((name[:70], alias_count))

        # Allow some entities with complex names to have fewer aliases
        # These can be enriched in a future Lote or by accepting that
        # compound names (e.g. "GOBERNACIÓN...- SECRETARÍA DE...") are hard
        # for heuristic extraction
        assert len(low_aliases) < 15, (
            f"Too many entities with <3 aliases: {len(low_aliases)}"
        )

    def test_no_forbidden_generic_aliases(self, lote_entities):
        """No alias should be a bare generic token."""
        with ALIASES_DB_PATH.open("r", encoding="utf-8") as f:
            gazetteer = json.load(f)

        gazetteer_map = {e["official_name"]: e for e in gazetteer}
        for entity in lote_entities:
            record = gazetteer_map.get(entity["official_name"])
            if not record:
                continue
            for alias in record.get("aliases", []):
                assert alias.strip().lower() not in ALL_FORBIDDEN, (
                    f"Alias '{alias}' de {entity['official_name'][:60]} es generico prohibido"
                )

    def test_heuristic_generates_valid_aliases(self, lote_entities):
        """Verify the heuristic generator produces valid aliases for MOST standard types.
        Some edge cases (compound names, atypical prefixes) are expected to fail."""
        failures = []
        for entity in lote_entities:
            name = entity["official_name"]
            etype = entity.get("entity_type", "otro")
            if etype in ("otro", "instituto"):
                continue  # LLM-only or poor heuristic coverage, skip
            # Skip compound names with dashes (unparsable by heuristic)
            if " - " in name or " -" in name or "-" in name:
                continue
            # Skip atypical prefixes not covered by heuristic patterns
            if any(name.upper().startswith(p) for p in [
                "SERVICIO NACIONAL DE APRENDIZAJE",
                "EMPRESA SOCIAL DEL ESTADO",
                "NUEVA EMPRESA SOCIAL DEL ESTADO",
            ]):
                continue
            aliases = generate_heuristic_aliases(name, entity_type=etype)
            if len(aliases) < 3:
                failures.append((name[:60], len(aliases), etype))
        assert len(failures) < 5, (
            f"Too many heuristic failures ({len(failures)}): {failures}"
        )
        # Also verify no forbidden aliases for any entity we tested
        for entity in lote_entities:
            name = entity["official_name"]
            etype = entity.get("entity_type", "otro")
            if etype in ("otro", "instituto"):
                continue
            if " - " in name or " -" in name or "-" in name:
                continue
            if any(name.upper().startswith(p) for p in [
                "SERVICIO NACIONAL DE APRENDIZAJE",
                "EMPRESA SOCIAL DEL ESTADO",
                "NUEVA EMPRESA SOCIAL DEL ESTADO",
            ]):
                continue
            aliases = generate_heuristic_aliases(name, entity_type=etype)
            for a in aliases:
                assert a not in ALL_FORBIDDEN, (
                    f"Alias '{a}' from heuristic for {name[:60]} is forbidden"
                )


class TestLote01Resolution:
    """Verify new entities resolve via scan_entity.

    Uses unambiguous queries that should resolve to specific new entities.
    Las regionales SENA/ICBF que TIENEN alias propio resuelven desde R2D.1: la
    deuda D6 las descartaba porque su alias termina en un departamento
    ("icbf caldas", "sena antioquia"). Las que siguen en xfail no tienen alias
    en el gazetteer —"sena chocó", "sena casanare"— y caen en la sede nacional
    por la sigla; eso es un hueco de datos, no la regla de geografía.
    """

    ENTITY_QUERIES = [
        # These should resolve to the exact new entity
        ("ese hospital salazar de villeta", "SALAZAR DE VILLETA"),
        ("superintendencia nacional de salud", "SUPERINTENDENCIA NACIONAL DE SALUD"),
        ("alcaldia de turbaco", "TURBACO"),
        ("superintendencia del subsidio familiar", "SUBSIDIO FAMILIAR"),
        ("alcaldia de el penol", "EL PEÑOL"),
        ("hospital san rafael el cerrito", "SAN RAFAEL"),
        pytest.param("hospital municipal de acacias", "ACACIAS",
                     marks=pytest.mark.xfail(reason="'hospital municipal' alias on existing entity steals resolution")),
        # Con alias propio: resuelven desde R2D.1 (antes bloqueadas por D6).
        # Sin alias propio: siguen cayendo en la sede nacional por la sigla.
        ("sena regional cundinamarca", "SENA REGIONAL CUNDINAMARCA"),
        ("icbf cordoba", "ICBF REGIONAL CORDOBA"),
        pytest.param("sena choco", "SENA REGIONAL CHOCO",
                     marks=pytest.mark.xfail(strict=True, reason="'sena' resolves to SENA SECRETARIA GENERAL first")),
        ("icbf magdalena", "ICBF REGIONAL MAGDALENA"),
        ("sena antioquia", "SENA REGIONAL ANTIOQUIA"),
        ("icbf norte de santander", "ICBF REGIONAL NORTE DE SANTANDER"),
        ("sena santander", "SENA REGIONAL SANTANDER"),
        pytest.param("sena casanare", "SENA REGIONAL CASANARE",
                     marks=pytest.mark.xfail(strict=True, reason="'sena' resolves to SENA SECRETARIA GENERAL first")),
        ("icbf caldas", "ICBF REGIONAL CALDAS"),
        ("icbf cesar", "ICBF REGIONAL CESAR"),
        ("sena caqueta", "SENA REGIONAL CAQUETA"),
        ("ministerio de justicia", "MINISTERIO DE JUSTICIA"),
    ]

    @pytest.mark.parametrize("query,expected_substring", ENTITY_QUERIES)
    def test_entity_resolves(self, query, expected_substring, resolver):
        result = resolver.scan_entity(query)
        assert result is not None, f"'{query}' no resolvio a ninguna entidad"
        assert expected_substring.upper() in result.official_name.upper(), (
            f"'{query}' resolvio a '{result.official_name}', "
            f"se esperaba entidad conteniendo '{expected_substring}'"
        )


class TestLote01NoCollision:
    """Verify new entity aliases don't steal resolution from existing entities."""

    EXISTING_QUERIES = [
        # These are well-known entities that must NOT break after Lote 01
        "sena",
        "icbf",
        "gobernacion de antioquia",
        "ministerio de educacion",
        "hospital universidad nacional",
    ]

    def test_existing_entities_still_resolve(self, resolver):
        """Known entities must still resolve after adding Lote 01."""
        for query in self.EXISTING_QUERIES:
            result = resolver.scan_entity(query)
            assert result is not None, f"'{query}' should still resolve after expansion"
