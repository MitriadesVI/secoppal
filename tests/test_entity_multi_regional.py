"""Tests ENTITY-MULTI-REGIONAL-001 — multi-regional entity resolution."""
from __future__ import annotations

import pytest

from app.data.regional_entities import resolve_regional


def test_sena_bolivar_resolves_to_regional():
    regional, canonical = resolve_regional("SENA", "Bolívar")
    assert regional == "SENA REGIONAL BOLÍVAR"
    assert canonical == "SENA"


def test_sena_atlantico_resolves_to_regional():
    regional, canonical = resolve_regional("SENA", "Atlántico")
    assert regional == "SENA REGIONAL ATLÁNTICO"
    assert canonical == "SENA"


def test_icbf_magdalena_resolves_to_regional():
    regional, canonical = resolve_regional("ICBF", "Magdalena")
    assert regional == "ICBF REGIONAL MAGDALENA"
    assert canonical == "ICBF"


def test_icbf_bogota_resolves_to_direccion():
    regional, canonical = resolve_regional("ICBF", "Bogotá")
    assert regional == "ICBF SEDE DIRECCIÓN GENERAL"
    assert canonical == "ICBF"


def test_unknown_entity_returns_none():
    """Non-multi-regional entities return (None, None)."""
    regional, canonical = resolve_regional("INVIAS", "Cundinamarca")
    assert regional is None
    assert canonical is None


def test_unknown_department_for_multi_entity_returns_none():
    """If the department has no specific regional, return (None, None)."""
    # DNP only has Bogotá in the catalog
    regional, canonical = resolve_regional("DNP", "Bolívar")
    assert regional is None
    assert canonical is None
