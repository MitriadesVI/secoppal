"""Tests ENTITY-MULTI-REGIONAL-001 — multi-regional entity resolution."""
from __future__ import annotations

import pytest

from app.data.regional_entities import resolve_regional


def test_sena_bolivar_resolves_to_regional():
    assert resolve_regional("SENA", "Bolívar") == "SENA REGIONAL BOLÍVAR"


def test_sena_atlantico_resolves_to_regional():
    assert resolve_regional("SENA", "Atlántico") == "SENA REGIONAL ATLÁNTICO"


def test_icbf_magdalena_resolves_to_regional():
    assert resolve_regional("ICBF", "Magdalena") == "ICBF REGIONAL MAGDALENA"


def test_icbf_bogota_resolves_to_direccion():
    assert resolve_regional("ICBF", "Bogotá") == "ICBF SEDE DIRECCIÓN GENERAL"


def test_unknown_entity_returns_none():
    """Non-multi-regional entities return None."""
    assert resolve_regional("INVIAS", "Cundinamarca") is None


def test_unknown_department_for_multi_entity_returns_none():
    """If the department has no specific regional, return None — keep central entity."""
    # DNP only has Bogotá in the catalog
    assert resolve_regional("DNP", "Bolívar") is None
