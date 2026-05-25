"""Tests MORPHO-EXPANSION-INCONSISTENT-001 — ambas rutas expanden igual."""
from __future__ import annotations

from app.core.soql_builder import SoQLBuilder


def test_heuristic_path_expands_morphology():
    """Path heuristic_only: objeto=['transporte'] → SoQL contiene transporte.
    'transportes' es substring-contenido → deduped. Fixes VARIANT-EXPLOSION-001."""
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {"objeto": ["transporte"], "departamento_resolved": "Bolívar"})
    assert "transporte" in soql
    # 'transportes' deduped — 'transporte' ya lo cubre via LIKE '%transporte%'


def test_llm_path_single_term_expands_morphology():
    """Path LLM: objeto=['transporte'] → misma expansión que path heuristico."""
    from app.core.llm_handler import _promote_synonym_expansion
    params = {"objeto": ["transporte"]}
    result = _promote_synonym_expansion(params)
    builder = SoQLBuilder()
    soql = builder.build("p6dx-8zbt", {**result, "departamento_resolved": "Bolívar"})
    assert "transporte" in soql
    # 'transportes' deduped — comportamiento unificado con heuristic path


def test_both_paths_produce_same_morpho_expansion():
    """Ambas rutas producen el mismo conjunto de LIKE clauses (equivalencia
    semantica, no string identity — el orden de clausulas puede variar)."""
    import re
    builder = SoQLBuilder()
    base = {"departamento_resolved": "Bolivar"}

    # Path heuristic
    soql_h = builder.build("p6dx-8zbt", {**base, "objeto": ["transporte"]})

    # Path LLM (simulado)
    from app.core.llm_handler import _promote_synonym_expansion
    llm_params = _promote_synonym_expansion({"objeto": ["transporte"]})
    soql_l = builder.build("p6dx-8zbt", {**base, **llm_params})

    # Extraer patrones LIKE de cada ruta (ignorar orden)
    like_pattern = re.compile(r"LIKE\s+\w+\(UPPER\('[^']+'\)\)", re.IGNORECASE)
    likes_h = set(like_pattern.findall(soql_h))
    likes_l = set(like_pattern.findall(soql_l))

    assert likes_h, "Path heuristico no genero LIKEs"
    assert likes_h == likes_l, (
        f"LIKEs divergen entre rutas:\n  heuristic: {sorted(likes_h)}\n  llm: {sorted(likes_l)}"
    )
