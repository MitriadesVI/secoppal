"""Commit 5: el builder consume objeto_phrases con un WHERE recall-first.

Solo expansión de candidatos: anclas co-ocurrentes en el MISMO campo, OR entre
campos, candidate_limit ampliado. Sin validador, sin ranking, sin display.
"""
from __future__ import annotations

import re

import pytest

from app.core.soql_builder import SoQLBuilder

DATASET = "jbjy-vk9h"


@pytest.fixture
def builder():
    return SoQLBuilder()


def test_phrase_emits_same_field_anchor_clause(builder):
    sql = builder.build(DATASET, {
        "objeto": [["elaboracion", "actualizacion"]],
        "objeto_phrases": ["manual de contratacion"],
    })
    low = sql.lower()
    # anclas co-ocurrentes EN EL MISMO campo (AND dentro del campo)
    assert "objeto_del_contrato" in low and "descripcion_del_proceso" in low
    assert "manual" in low and "contratac" in low
    # la acción sobrevive como su propia cláusula, separada de la frase
    assert "elaboracion" in low and "actualizacion" in low


def test_anchors_not_crossed_between_fields(builder):
    # Cada campo debe exigir AMBAS anclas; no se permite manual(obj) + contratac(desc).
    sql = builder.build(DATASET, {"objeto_phrases": ["manual de contratacion"]}).lower()
    # debe existir un grupo donde objeto exige manual AND contratac dentro del mismo LIKE-bloque
    # heurística: el nombre de campo objeto aparece al menos dos veces (una por ancla)
    assert sql.count("objeto_del_contrato") >= 2
    assert sql.count("descripcion_del_proceso") >= 2


def test_candidate_limit_widens_with_phrases(builder):
    with_phrase = builder.build(DATASET, {"objeto_phrases": ["manual de contratacion"]})
    without = builder.build(DATASET, {"objeto": ["pavimentacion"]})
    assert "LIMIT 200" in with_phrase
    assert "LIMIT 50" in without


def test_phrase_only_query_has_no_where_1_1(builder):
    # objeto_phrases es topic suficiente: nunca debe degenerar en WHERE 1=1
    sql = builder.build(DATASET, {"objeto_phrases": ["manual de contratacion"]})
    assert "1=1" not in sql
    assert re.search(r"where\s+\(", sql.lower())


def test_unknown_canonical_is_ignored(builder):
    # un canonical fuera del catálogo no añade cláusula (no rompe)
    sql = builder.build(DATASET, {
        "objeto": ["pavimentacion"],
        "objeto_phrases": ["frase inexistente xyz"],
    })
    assert "1=1" not in sql
    assert "pavimentacion" in sql.lower()
