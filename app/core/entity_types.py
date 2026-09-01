"""
Entity Types — semantic entity rewriting for SECOPPAL.

Maps common morphologic/syntactic variants of public entity names
to canonical forms that match the gazetteer (aliases_db.json).

Deterministic, no LLM. Runs BEFORE entity_resolver so the rewritten
forms feed into the existing alias→exact→fuzzy→LIKE pipeline.
"""

import re
from typing import Callable

# Each entry: (compiled_regex, replacer_fn)
# replacer_fn receives the captured entity name (str) and returns list[str] of
# canonical candidates to try against the gazetteer, in priority order.
ENTITY_REWRITES: list[tuple[re.Pattern, Callable]] = [
    # ── Alcaldía variants ──
    # Stop capture at: para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$
    # Use \b to prevent "concordia" matching "con"
    (
        re.compile(
            r"\balcald[ií]a\s+(?:municipal\s+)?de\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$))",
            re.IGNORECASE,
        ),
        lambda x: [
            f"Municipio de {x}",
            f"Alcaldía de {x}",
            f"Alcaldía Municipal de {x}",
        ],
    ),
    # ── Gobernación variants ──
    # ``de(?:l)?`` — INV-NO-SILENT-SEMANTIC-FALLBACK (ADR-069). Con solo "de",
    # "gobernación DEL Vichada" no generaba ningún candidato de entidad: el span
    # sobrevivía hasta el scan de departamento y una pregunta por el COMPRADOR
    # se contestaba con TODO el territorio. "del" es la forma natural para la
    # mitad de los departamentos (del Atlántico, del Huila, del Valle), así que
    # la asimetría no era un borde raro. ``generic_buyer._NAME_QUALIFIER`` ya
    # trataba ambas formas como comprador calificado; esto lo alinea.
    (
        re.compile(
            r"\bgobernaci[oó]n\s+de(?:l)?\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$))",
            re.IGNORECASE,
        ),
        lambda x: [
            f"Departamento de {x}",
            f"Gobernación de {x}",
        ],
    ),
    # ── ESE / Empresa Social del Estado ──
    (
        re.compile(
            r"\bese\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$))",
            re.IGNORECASE,
        ),
        lambda x: [
            f"Empresa Social del Estado {x}",
            f"ESE {x}",
        ],
    ),
    # ── Hospital variants ──
    (
        re.compile(
            r"\b(?:el\s+)?hospital\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$))",
            re.IGNORECASE,
        ),
        lambda x: [
            f"Hospital {x}",
            f"ESE Hospital {x}",
        ],
    ),
    # ── Universidad ──
    (
        re.compile(
            r"\b(?:la\s+)?universidad\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$))",
            re.IGNORECASE,
        ),
        lambda x: [
            f"Universidad {x}",
        ],
    ),
    # ── Municipio de X → keep as-is but also try Alcaldía ──
    (
        re.compile(
            r"\bmunicipio\s+de\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|sobre\b|$))",
            re.IGNORECASE,
        ),
        lambda x: [
            f"Municipio de {x}",
            f"Alcaldía de {x}",
        ],
    ),
]

# Cuántos tokens trailing como máximo se intentan descartar al hacer shrink.
# Cubre "este mes" (1) / "pavimentacion" (1) / "este mes pasado" (2) y sobra;
# acota el costo cuando la captura agarra una cola larga de basura.
_MAX_SHRINK_LEVELS = 5


def rewrite_entity(query: str) -> list[tuple[str, str, str]]:
    """Scan query for entity-type patterns and generate canonical candidates.

    Skips temporal/measure phrases that look like entities (e.g., "ese año").

    Shrink-fallback (ENTITY-TRAILING-001): la captura ``(.+?)`` es greedy hasta
    el siguiente conector o el fin de línea, así que se traga tokens trailing
    que NO son parte del nombre de la entidad ("este mes", "pavimentacion",
    basura). Para tolerarlo, por cada match generamos candidatos con la captura
    progresivamente más corta — del span MÁS LARGO al más corto — cada uno con
    su propio ``consumed_span``. El caller (query_router 4.6) prueba en orden y
    se queda con el primero que resuelve = el prefijo válido más largo. Esto:
      - NO regresa casos que hoy resuelven (el span completo se prueba primero),
      - evita over-shrink (longest-prefix-that-matches),
      - deja el leftover (p.ej. "pavimentacion") libre para fluir al objeto.
    """
    # Words that should NOT trigger entity rewrite after ESE/patterns
    _EXCLUSION_CAPTURES = frozenset({
        "año", "ano", "periodo", "período", "rango",
        "año fiscal", "mismo año",
    })
    results: list[tuple[str, str, str]] = []

    for pattern, replacer in ENTITY_REWRITES:
        match = pattern.search(query)
        if not match:
            continue
        words = match.group(1).split()
        if not words:
            continue
        # Prefijo literal entre el inicio del match y el inicio de la captura
        # (ej. "alcaldía de "), preservado tal cual aparece en `query` para que
        # `consumed_span` sea siempre un substring exacto de la entrada.
        prefix = query[match.start(0):match.start(1)]
        rewrite_type = pattern.pattern.split(r"\b")[1].split(r"\s")[0].lower()
        lowest_k = max(1, len(words) - _MAX_SHRINK_LEVELS + 1)
        for k in range(len(words), lowest_k - 1, -1):
            captured = " ".join(words[:k]).strip()
            if not captured or captured.lower() in _EXCLUSION_CAPTURES:
                continue
            consumed_span = (prefix + captured).strip()
            for candidate in replacer(captured):
                results.append((candidate, consumed_span, rewrite_type))

    return results
