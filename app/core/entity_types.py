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
# replacer_fn receives the match and returns list[str] of canonical candidates
# to try against the gazetteer, in priority order.
ENTITY_REWRITES: list[tuple[re.Pattern, Callable]] = [
    # ── Alcaldía variants ──
    # Stop capture at: para|donde|por\b|con\b|en\b|del\b|de\b|$
    # Use \b to prevent "concordia" matching "con"
    (
        re.compile(
            r"\balcald[ií]a\s+(?:municipal\s+)?de\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|$))",
            re.IGNORECASE,
        ),
        lambda m: [
            f"Municipio de {m.group(1).strip()}",
            f"Alcaldía de {m.group(1).strip()}",
            f"Alcaldía Municipal de {m.group(1).strip()}",
        ],
    ),
    # ── Gobernación variants ──
    (
        re.compile(
            r"\bgobernaci[oó]n\s+de\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|$))",
            re.IGNORECASE,
        ),
        lambda m: [
            f"Departamento de {m.group(1).strip()}",
            f"Gobernación de {m.group(1).strip()}",
        ],
    ),
    # ── ESE / Empresa Social del Estado ──
    (
        re.compile(
            r"\bese\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|$))",
            re.IGNORECASE,
        ),
        lambda m: [
            f"Empresa Social del Estado {m.group(1).strip()}",
            f"ESE {m.group(1).strip()}",
        ],
    ),
    # ── Hospital variants ──
    (
        re.compile(
            r"\b(?:el\s+)?hospital\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|$))",
            re.IGNORECASE,
        ),
        lambda m: [
            f"Hospital {m.group(1).strip()}",
            f"ESE Hospital {m.group(1).strip()}",
        ],
    ),
    # ── Universidad ──
    (
        re.compile(
            r"\b(?:la\s+)?universidad\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|$))",
            re.IGNORECASE,
        ),
        lambda m: [
            f"Universidad {m.group(1).strip()}",
        ],
    ),
    # ── Municipio de X → keep as-is but also try Alcaldía ──
    (
        re.compile(
            r"\bmunicipio\s+de\s+(.+?)(?=\s*(?:para|donde|por\b|con\b|en\b|del\b|de\b|$))",
            re.IGNORECASE,
        ),
        lambda m: [
            f"Municipio de {m.group(1).strip()}",
            f"Alcaldía de {m.group(1).strip()}",
        ],
    ),
]


def rewrite_entity(query: str) -> list[tuple[str, str, str]]:
    """Scan query for entity-type patterns and generate canonical candidates.
    Skips temporal/measure phrases that look like entities (e.g., "ese año").
    """
    # Words that should NOT trigger entity rewrite after ESE/patterns
    _EXCLUSION_CAPTURES = frozenset({
        "año", "ano", "periodo", "período", "rango",
        "año fiscal", "mismo año",
    })
    results: list[tuple[str, str, str]] = []

    for pattern, replacer in ENTITY_REWRITES:
        match = pattern.search(query)
        if match:
            consumed_span = match.group(0).strip()
            captured = match.group(1).strip().lower()
            # Skip if captured text is a non-entity phrase (temporal, measure)
            if captured in _EXCLUSION_CAPTURES:
                continue
            rewrite_type = pattern.pattern.split(r"\b")[1].split(r"\s")[0].lower()
            for candidate in replacer(match):
                results.append((candidate, consumed_span, rewrite_type))

    return results
