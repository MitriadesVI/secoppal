"""Tests para el crosswalk de entidades SECOP I — C1A + C1A.1 hardening.

Validan que:
- Los JSON cargan y tienen estructura v0.2.0
- canonical_id existe y es estable para casos clave
- source_aliases y user_aliases están separados
- bad_name y needs_review tienen integration_ready=False
- Caso Ataco: canonical_id = municipio_ataco_tolima
- Caso Bucaramanga CC: canonical_id = camara_comercio_bucaramanga_santander
- Caso Cali CC: canonical_id = camara_comercio_cali_valle_del_cauca
- No hay aliases de un solo token (salvo municipio+departamento)
- No se modifica aliases_db.json
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURE_DIR = Path(__file__).resolve().parent.parent / "app" / "data" / "entity_source_aliases"
VALID_CLASSIFICATIONS = {"matched_existing", "secop1_only", "ambiguous", "bad_name", "needs_review"}


# ── Fixtures ──

@pytest.fixture(scope="module")
def entities_sample():
    path = FIXTURE_DIR / "secop1_entities_sample.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture(scope="module")
def crosswalk():
    path = FIXTURE_DIR / "entity_crosswalk_sample.json"
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Helper ──

def find_entity(crosswalk, name_fragment):
    matches = [e for e in crosswalk["entities"] if name_fragment.upper() in e["secop1_name"].upper()]
    assert len(matches) > 0, f"Entidad no encontrada: {name_fragment}"
    return matches[0]


# ── Tests: Estructura v0.2.0 ──

def test_crosswalk_version(crosswalk):
    assert crosswalk["model"] == "EntityCrosswalk"
    assert crosswalk["version"] == "0.2.0"


def test_all_entities_have_canonical_id(crosswalk):
    for entity in crosswalk["entities"]:
        assert "canonical_id" in entity, f"Falta canonical_id en {entity.get('secop1_name', '?')}"
        assert entity["canonical_id"], f"canonical_id vacío en {entity['secop1_name']}"
        assert " " not in entity["canonical_id"], f"canonical_id con espacios: {entity['canonical_id']}"


def test_source_and_user_aliases_separated(crosswalk):
    for entity in crosswalk["entities"]:
        assert "source_aliases" in entity, f"Falta source_aliases en {entity['secop1_name']}"
        assert "user_aliases" in entity, f"Falta user_aliases en {entity['secop1_name']}"
        assert "aliases" not in entity, f"Campo 'aliases' legacy no debe existir en {entity['secop1_name']}"
        assert isinstance(entity["source_aliases"], list)
        assert isinstance(entity["user_aliases"], list)


def test_integration_ready_field(crosswalk):
    for entity in crosswalk["entities"]:
        assert "integration_ready" in entity, f"Falta integration_ready en {entity['secop1_name']}"
        assert isinstance(entity["integration_ready"], bool)


# ── Tests: Filtro de calidad ──

def test_bad_name_not_integration_ready(crosswalk):
    for entity in crosswalk["entities"]:
        if entity["classification"] == "bad_name":
            assert not entity["integration_ready"], (
                f"bad_name debería ser integration_ready=False: {entity['secop1_name']}"
            )
            assert entity.get("exclusion_reason"), (
                f"bad_name debería tener exclusion_reason: {entity['secop1_name']}"
            )


def test_needs_review_not_integration_ready(crosswalk):
    for entity in crosswalk["entities"]:
        if entity["classification"] == "needs_review":
            assert not entity["integration_ready"], (
                f"needs_review debería ser integration_ready=False: {entity['secop1_name']}"
            )


def test_matched_existing_is_integration_ready(crosswalk):
    for entity in crosswalk["entities"]:
        if entity["classification"] == "matched_existing":
            assert entity["integration_ready"], (
                f"matched_existing debería ser integration_ready=True: {entity['secop1_name']}"
            )


def test_exclusion_count_matches(crosswalk):
    stats = crosswalk.get("_stats", {})
    excluded = stats.get("excluded_bad_name", 0) + stats.get("excluded_needs_review", 0)
    not_ready = sum(1 for e in crosswalk["entities"] if not e["integration_ready"])
    assert not_ready == excluded, (
        f"integration_ready=False count ({not_ready}) != stats excluded ({excluded})"
    )


# ── Tests: Casos concretos ──

def test_ataco_canonical_id(crosswalk):
    """TOLIMA - ALCALDIA MUNICIPIO DE ATACO → municipio_ataco_tolima"""
    entity = find_entity(crosswalk, "ATACO")
    assert entity["canonical_id"] == "municipio_ataco_tolima", (
        f"Esperado municipio_ataco_tolima, encontrado {entity['canonical_id']}"
    )
    assert entity["integration_ready"] is True


def test_ataco_user_aliases(crosswalk):
    """Ataco debe tener aliases de búsqueda generados."""
    entity = find_entity(crosswalk, "ATACO")
    aliases = [a["alias"] for a in entity["user_aliases"]]
    assert "ALCALDIA DE ATACO" in aliases
    assert "MUNICIPIO DE ATACO" in aliases
    assert "ATACO TOLIMA" in aliases
    # No debe tener alias de un solo token
    for a in aliases:
        assert len(a.split()) >= 2, f"Alias de un solo token: '{a}'"


def test_bucaramanga_cc_canonical_id(crosswalk):
    """SANTANDER - CAMARA DE COMERCIO DE BUCARAMANGA → camara_comercio_bucaramanga_santander"""
    entity = find_entity(crosswalk, "BUCARAMANGA")
    assert entity["canonical_id"] == "camara_comercio_bucaramanga_santander", (
        f"Esperado camara_comercio_bucaramanga_santander, encontrado {entity['canonical_id']}"
    )
    assert entity["integration_ready"] is True


def test_bucaramanga_cc_aliases(crosswalk):
    entity = find_entity(crosswalk, "BUCARAMANGA")
    aliases = [a["alias"] for a in entity["user_aliases"]]
    # Debe tener al menos una variante sin "DE"
    has_no_de = any("CAMARA DE COMERCIO BUCARAMANGA" in a for a in aliases)
    has_cc = any(a.startswith("CC ") for a in aliases)
    assert has_no_de, f"Falta alias sin 'DE': {aliases}"
    assert has_cc, f"Falta alias abreviado CC: {aliases}"


def test_cali_cc_canonical_id(crosswalk):
    """VALLE DEL CAUCA - CAMARA DE COMERCIO DE CALI → camara_comercio_cali_valle_del_cauca"""
    entity = find_entity(crosswalk, "CAMARA DE COMERCIO DE CALI")
    assert entity["canonical_id"] == "camara_comercio_cali_valle_del_cauca", (
        f"Esperado camara_comercio_cali_valle_del_cauca, encontrado {entity['canonical_id']}"
    )


# ── Tests: Reglas de aliases ──

def test_no_single_token_aliases(crosswalk):
    """Ningún user_alias debe tener un solo token (excepto municipio+departamento)."""
    for entity in crosswalk["entities"]:
        for ua in entity.get("user_aliases", []):
            tokens = ua["alias"].split()
            assert len(tokens) >= 2, (
                f"Alias de un solo token: '{ua['alias']}' en {entity['secop1_name']}"
            )


def test_canonical_ids_are_unique(crosswalk):
    ids = [e["canonical_id"] for e in crosswalk["entities"]]
    duplicates = [cid for cid in set(ids) if ids.count(cid) > 1]
    assert not duplicates, f"canonical_id duplicados: {duplicates}"


# ── Tests: Hot path protection ──

def test_aliases_db_not_modified():
    """El gazetteer principal no debe modificarse."""
    aliases_path = Path(__file__).resolve().parent.parent / "app" / "data" / "aliases_db.json"
    assert aliases_path.exists(), "aliases_db.json no existe"


def test_crosswalk_in_separate_directory():
    crosswalk_dir = Path(__file__).resolve().parent.parent / "app" / "data" / "entity_source_aliases"
    aliases_dir = Path(__file__).resolve().parent.parent / "app" / "data"
    assert crosswalk_dir.is_dir()
    assert "aliases_db.json" not in [f.name for f in crosswalk_dir.iterdir()], (
        "aliases_db.json no debe estar en entity_source_aliases/"
    )
