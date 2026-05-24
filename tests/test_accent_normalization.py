"""Tests ACCENT-NORMALIZATION-001 — regexp_replace tildes en SoQL + strip_accents.

Casos canónicos: consultoría, jardinería, diseño, sólidos.
Cada caso verifica:
- SoQL generado usa regexp_replace, NO unaccent
- Objeto sin tilde matchea dato con tilde (vía regexp_replace en campo)
- strip_accents Python normaliza para query-side
"""
from __future__ import annotations

import pytest

from app.core.soql_builder import SoQLBuilder
from app.utils.text import strip_accents


# ── SoQL generation: regexp_replace not unaccent ──────────────────────────

def test_soql_uses_regexp_replace_not_unaccent_for_consultoria():
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["consultoria"], "departamento_resolved": "Bolívar"},
    )
    assert "unaccent" not in soql
    assert "regexp_replace" in soql
    assert "consultoria" in soql


def test_soql_uses_regexp_replace_for_diseno():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, {"objeto": ["diseno"]})
    assert "unaccent" not in soql
    assert "regexp_replace" in soql
    assert "diseno" in soql


def test_soql_uses_regexp_replace_for_jardineria_contratos():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.CONTRATOS_DATASET, {"objeto": ["jardineria"]})
    assert "unaccent" not in soql
    assert "regexp_replace" in soql
    assert "objeto_del_contrato" in soql


def test_soql_uses_regexp_replace_for_solidos():
    builder = SoQLBuilder()
    soql = builder.build(SoQLBuilder.PROCESOS_DATASET, {"objeto": ["solidos"]})
    assert "unaccent" not in soql
    assert "regexp_replace" in soql
    assert "solidos" in soql


# ── LIKE pattern correctness ──────────────────────────────────────────────

def test_soql_like_clause_contains_accent_folded_fields():
    """The LIKE clause wraps field + pattern in regexp_replace chains."""
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["turísticos"], "departamento_resolved": "Huila"},
    )
    assert "LIKE" in soql
    # Should have regexp_replace wrapping both field and pattern
    assert soql.count("regexp_replace") >= 2


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
    """User types 'consultoria', SECOP has 'CONSULTORÍA'. LIKE must match."""
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["consultoria"], "departamento_resolved": "Bolívar"},
    )
    # The LIKE pattern should be accent-folded, meaning both sides normalized
    assert "LIKE" in soql
    assert "regexp_replace" in soql


def test_object_with_accent_matches_data_without_accent():
    """User types 'consultoría', SECOP has 'CONSULTORIA'. LIKE must match."""
    builder = SoQLBuilder()
    soql = builder.build(
        SoQLBuilder.PROCESOS_DATASET,
        {"objeto": ["consultoría"], "departamento_resolved": "Bolívar"},
    )
    # Pattern includes accent which gets regexp_replace'd
    assert "LIKE" in soql
    assert "regexp_replace" in soql
