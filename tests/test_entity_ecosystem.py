"""Tests ENTITY-ECOSYSTEM-001 — entity ecosystem expansion."""
from __future__ import annotations

from app.core.entity_ecosystem import get_ecosystem_entities, should_expand_ecosystem


def test_get_bogota_ecosystem():
    entities = get_ecosystem_entities(ciudad="Bogotá")
    assert entities is not None
    assert len(entities) >= 10
    assert "ALCALDÍA MAYOR DE BOGOTÁ D.C." in entities


def test_get_medellin_ecosystem():
    entities = get_ecosystem_entities(ciudad="Medellín")
    assert entities is not None
    assert len(entities) >= 8


def test_get_cali_ecosystem():
    entities = get_ecosystem_entities(ciudad="Cali")
    assert entities is not None
    assert len(entities) >= 8


def test_get_antioquia_ecosystem():
    entities = get_ecosystem_entities(departamento="Antioquia")
    assert entities is not None
    assert len(entities) >= 5


def test_unknown_city_returns_none():
    assert get_ecosystem_entities(ciudad="Paipa") is None


def test_should_expand_when_city_set_no_entity():
    params = {"ciudad": "Bogota"}
    assert should_expand_ecosystem(params) is True


def test_should_not_expand_when_entity_exists():
    params = {"ciudad": "Bogota", "entidad_resolved": "ALCALDÍA MAYOR DE BOGOTÁ"}
    assert should_expand_ecosystem(params) is False


def test_should_not_expand_without_place():
    params = {"objeto": ["obra"]}
    assert should_expand_ecosystem(params) is False
