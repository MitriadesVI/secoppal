"""Tests MORPHO-EXPANSION-INCONSISTENT-001 — ambas rutas expanden igual."""
from __future__ import annotations

from app.core.soql_builder import SoQLBuilder


def test_heuristic_path_expands_morphology():
    """Path heuristic_only: objeto=['transporte'] → SoQL contiene transportes."""
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {"objeto": ["transporte"], "departamento_resolved": "Bolívar"})
    assert "transportes" in soql
    assert "transporte" in soql


def test_llm_path_single_term_expands_morphology():
    """Path LLM: objeto=['transporte'] post-LLM → misma expansión morfológica."""
    from app.core.llm_handler import _promote_synonym_expansion
    params = {"objeto": ["transporte"]}
    # _promote_synonym_expansion no cambia single terms — solo pairs
    result = _promote_synonym_expansion(params)
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {**result, "departamento_resolved": "Bolívar"})
    assert "transportes" in soql
    assert "transporte" in soql


def test_both_paths_produce_same_morpho_expansion():
    """Ambas rutas producen el mismo número de LIKE clauses para 'transporte'."""
    builder = SoQLBuilder()
    base = {"departamento_resolved": "Bolívar"}

    # Path heuristic
    soql_h = builder.build("p6dx-8zbt", {**base, "objeto": ["transporte"]})

    # Path LLM (simulado)
    from app.core.llm_handler import _promote_synonym_expansion
    llm_params = _promote_synonym_expansion({"objeto": ["transporte"]})
    soql_l = builder.build("p6dx-8zbt", {**base, **llm_params})

    assert soql_h.count(" LIKE ") == soql_l.count(" LIKE ")
    assert soql_h.count("transportes") == soql_l.count("transportes")
