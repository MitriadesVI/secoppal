"""Tests for region-based search (búsqueda por zonas/regiones).

Cubre la cadena completa offline (sin red):
  datos (DEPARTAMENTOS_BY_REGION) → detección (router) → builder (IN clause),
  más las dos trampas de diseño: colisión Amazonas/Amazonía y precedencia
  de un departamento específico sobre la región.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.data.colombia_geography import (
    DEPARTAMENTOS_BY_REGION,
    REGION_BY_DEPARTMENT,
    ZONES_BY_NAME,
)
from app.data.departamentos import DEPARTAMENTOS_OFICIALES

_ALIAS_DB = Path("app/data/aliases_db.json")
_CONTRATOS = "jbjy-vk9h"


@pytest.fixture
def router() -> QueryRouter:
    return QueryRouter(entity_resolver=EntityResolver(_ALIAS_DB))


# ── 1. Datos: el cruce región → oficiales SECOP está completo y es válido ──

def test_all_regions_present():
    expected = set(REGION_BY_DEPARTMENT.values())
    assert set(DEPARTAMENTOS_BY_REGION.keys()) == expected


def test_all_zone_departments_are_official_secop_values():
    # Cubre regiones canónicas Y subregiones (EXTRA_ZONES) en una pasada.
    oficiales = set(DEPARTAMENTOS_OFICIALES)
    for zone, depts in ZONES_BY_NAME.items():
        assert depts, f"{zone} quedó vacía"
        for d in depts:
            assert d in oficiales, f"{d} ({zone}) no es un valor oficial SECOP"


def test_caribe_has_expected_departments():
    caribe = set(DEPARTAMENTOS_BY_REGION["Caribe"])
    assert {"Atlántico", "Bolívar", "Magdalena", "La Guajira", "Sucre"} <= caribe


# ── 2. Detección en el router ──────────────────────────────────────────────

def test_router_detects_region(router):
    p = router.parse("contratos en la costa caribe").params
    assert p.get("region") == "Caribe"
    assert "departamentos_in" in p
    assert "Atlántico" in p["departamentos_in"]
    # una región NO debe fijar un departamento escalar
    assert "departamento_resolved" not in p


def test_router_detects_pacifico(router):
    p = router.parse("licitaciones en el pacifico").params
    assert p.get("region") == "Pacifica"
    assert "Valle del Cauca" in p["departamentos_in"]


# ── 2b. Subregiones (departamentos completos) ──────────────────────────────

def test_router_detects_eje_cafetero(router):
    p = router.parse("contratos en el eje cafetero").params
    assert p.get("region") == "Eje Cafetero"
    assert set(p["departamentos_in"]) == {"Caldas", "Quindío", "Risaralda"}


def test_router_detects_santanderes(router):
    p = router.parse("licitaciones en los santanderes").params
    assert p.get("region") == "Santanderes"
    assert set(p["departamentos_in"]) == {"Norte de Santander", "Santander"}


def test_router_detects_tolima_grande(router):
    p = router.parse("obras en el tolima grande").params
    assert p.get("region") == "Tolima Grande"
    assert set(p["departamentos_in"]) == {"Huila", "Tolima"}


def test_santander_alone_is_department_not_subregion(router):
    # "santander" (singular) NO debe activar la subregión Santanderes
    p = router.parse("contratos en santander").params
    assert p.get("departamento_resolved") == "Santander"
    assert "region" not in p


# ── 3. Builder: departamentos_in → departamento IN (...) ───────────────────

def test_builder_emits_in_clause():
    soql = SoQLBuilder().build(
        _CONTRATOS, {"departamentos_in": ["Atlántico", "Bolívar"]}
    )
    assert "departamento IN (" in soql
    assert "'Atlántico'" in soql and "'Bolívar'" in soql


def test_builder_single_department_in_list():
    # lista de 1 → igualdad, no IN (comportamiento de _add_estado_clause)
    soql = SoQLBuilder().build(_CONTRATOS, {"departamentos_in": ["Atlántico"]})
    assert "departamento = 'Atlántico'" in soql


# ── 4. Colisión Amazonas (departamento) vs Amazonía (región) ───────────────

def test_amazonas_is_department_not_region(router):
    p = router.parse("contratos en amazonas").params
    assert p.get("departamento_resolved") == "Amazonas"
    assert "region" not in p


def test_amazonia_is_region(router):
    p = router.parse("contratos en la amazonia").params
    assert p.get("region") == "Amazonia"
    assert "departamento_resolved" not in p


# ── 5. Precedencia: un departamento específico gana sobre la región ────────

def test_specific_department_wins_over_region(router):
    # "bolívar" es del Caribe, pero al ser un depto explícito debe ganar
    p = router.parse("contratos en bolivar").params
    assert p.get("departamento_resolved") == "Bolívar"
    assert "region" not in p
