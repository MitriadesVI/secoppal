"""Commit 6A: candidate_limit (fetch) separado de display_limit (mostrado=25).

El pool de candidatos se amplía a 200 SOLO cuando hay frase recall-first, para
que los positivos más allá del puesto 50 lleguen al validador (Commit 6B). El
display sigue recortando a 25 aguas abajo (formatter), sin filtrarse al builder.
"""
from __future__ import annotations

from app.core.workflow.actions.planning import (
    _DEFAULT_LIMIT,
    _PHRASE_CANDIDATE_LIMIT,
    _candidate_limit,
)


def test_widens_only_with_phrases():
    assert _candidate_limit({"objeto_phrases": ["manual de contratacion"]}) == _PHRASE_CANDIDATE_LIMIT
    assert _PHRASE_CANDIDATE_LIMIT == 200


def test_standard_limit_without_phrases():
    assert _candidate_limit({"objeto": ["pavimentacion"]}) == _DEFAULT_LIMIT
    assert _candidate_limit({}) == _DEFAULT_LIMIT
    assert _DEFAULT_LIMIT == 50


def test_explicit_limit_is_respected_by_planning():
    # setdefault: si ya viene un limit (p.ej. paginación), NO se pisa.
    params = {"objeto_phrases": ["manual de contratacion"], "limit": 25}
    params.setdefault("limit", _candidate_limit(params))
    assert params["limit"] == 25
