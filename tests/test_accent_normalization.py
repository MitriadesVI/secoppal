"""Tests ACCENT-NORMALIZATION-001 — unaccent() en SoQL + strip_accents Python.

Casos canónicos: consultoría, jardinería, diseño, sólidos.
Socrata SoQL soporta unaccent() (PostgreSQL backend) — NO regexp_replace.
"""
from __future__ import annotations

import pytest

from app.core.soql_builder import SoQLBuilder
from app.utils.text import strip_accents


# ── SoQL generation: unaccent (Socrata compatible) ─────────────────────────

def test_soql_uses_unaccent_for_consultoria():
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["consultoria"], "departamento_resolved": "Bolívar"},
    )
    assert "unaccent" in soql
    assert "regexp_replace" not in soql
    assert "consultoria" in soql


def test_soql_uses_unaccent_for_diseno():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, {"objeto": ["diseno"]})
    assert "unaccent" in soql
    assert "regexp_replace" not in soql
    assert "diseno" in soql


def test_soql_uses_unaccent_for_jardineria_contratos():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.CONTRATOS_DATASET, {"objeto": ["jardineria"]})
    assert "unaccent" in soql
    assert "regexp_replace" not in soql
    assert "objeto_del_contrato" in soql


def test_soql_uses_unaccent_for_solidos():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, {"objeto": ["solidos"]})
    assert "unaccent" in soql
    assert "regexp_replace" not in soql
    assert "solidos" in soql


# ── LIKE pattern correctness ──────────────────────────────────────────────

def test_soql_like_clause_wraps_both_sides_in_unaccent():
    """The LIKE clause wraps field AND pattern in unaccent()."""
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["turísticos"], "departamento_resolved": "Huila"},
    )
    assert "LIKE" in soql
    # unaccent should appear at least twice (field side + pattern side)
    assert soql.count("unaccent") >= 2


# ── Python strip_accents helper ───────────────────────────────────────────

@pytest.mark.parametrize("inp,expected", [
    ("consultoría", "consultoria"),
    ("CONSULTORÍA", "CONSULTORIA"),
    ("diseño", "diseno"),
    ("DISEÑO", "DISENO"),
    ("jardinería", "jardineria"),
    ("sólidos", "solidos"),
    ("SÓLIDOS", "SOLIDOS"),
    ("cafetería", "cafeteria"),
    ("artísticos", "artisticos"),
    ("gestión", "gestion"),
    ("contratación", "contratacion"),
    ("sin tilde", "sin tilde"),
])
def test_strip_accents_python(inp, expected):
    assert strip_accents(inp) == expected


# ── Real-world matches (object with/without tilde) ────────────────────────

def test_object_without_accent_matches_data_with_accent():
    """User types 'consultoria', SECOP has 'CONSULTORÍA'. unaccent LIKE matches both."""
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["consultoria"], "departamento_resolved": "Bolívar"},
    )
    assert "LIKE" in soql
    assert "unaccent" in soql


def test_object_with_accent_matches_data_without_accent():
    """User types 'consultoría', SECOP has 'CONSULTORIA'. unaccent LIKE matches both."""
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["consultoría"], "departamento_resolved": "Bolívar"},
    )
    assert "LIKE" in soql
    assert "unaccent" in soql
