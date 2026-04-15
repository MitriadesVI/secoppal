"""Spell correction for Colombian public procurement queries.

Corrects common typos in SECOP search queries before they hit the parser.
Uses rapidfuzz for fuzzy matching against a curated vocabulary of
contratación pública terms.
"""
from __future__ import annotations

import re

from rapidfuzz import fuzz, process

# ── Curated vocabulary of contratación pública terms ─────────────────────────
VOCABULARY: set[str] = {
    "prestacion",
    "servicios",
    "profesionales",
    "contrato",
    "contratos",
    "licitacion",
    "alcaldia",
    "gobernacion",
    "construccion",
    "mantenimiento",
    "pavimentacion",
    "alimentacion",
    "suministro",
    "interventoria",
    "consultoria",
    "obra",
    "transporte",
    "infraestructura",
    "educacion",
    "salud",
    "vivienda",
    "acueducto",
    "saneamiento",
    "municipio",
    "departamento",
    "adjudicacion",
    "adquisicion",
    "arrendamiento",
    "capacitacion",
    "dotacion",
    "mejoramiento",
    "rehabilitacion",
    "remodelacion",
    "ampliacion",
    "supervision",
    "asesoria",
}

# Pre-sorted list for rapidfuzz (tuple for immutability)
_VOCAB_LIST: tuple[str, ...] = tuple(sorted(VOCABULARY))

# Characters that sometimes appear as typos inside words (keyboard accidents)
_GARBAGE_CHARS_RE = re.compile(r"[=+\[\]{}<>|\\@#~^]")

# Threshold for fuzzy matching (0-100 scale, rapidfuzz uses 0-100)
_SCORE_THRESHOLD = 85

# Maximum edit distance allowed (we use score threshold, but also cap length diff)
_MAX_LEN_DIFF = 2


def _clean_token(token: str) -> str:
    """Remove garbage characters embedded in a token (e.g. 'servid=cios' → 'servicios')."""
    return _GARBAGE_CHARS_RE.sub("", token)


def _is_numeric_or_special(token: str) -> bool:
    """Return True if the token is a number or contains non-alpha chars (after cleaning)."""
    return bool(re.search(r"[^a-z]", token))


def _correct_word(word: str) -> str:
    """Attempt to correct a single word against the vocabulary.

    Rules:
    - Words of 3 or fewer chars → untouched
    - Words already in vocabulary → untouched
    - Words with numbers or remaining special chars → untouched
    - Otherwise, fuzzy-match and accept if score >= threshold
    """
    # Don't correct very short words
    if len(word) <= 3:
        return word

    # Already a known term
    if word in VOCABULARY:
        return word

    # Don't correct numeric or special-char tokens
    if _is_numeric_or_special(word):
        return word

    # Fuzzy match against vocabulary
    result = process.extractOne(
        word,
        _VOCAB_LIST,
        scorer=fuzz.ratio,
        score_cutoff=_SCORE_THRESHOLD,
    )

    if result is None:
        return word

    match, score, _idx = result

    # Additional guard: don't correct if length difference is too large
    if abs(len(word) - len(match)) > _MAX_LEN_DIFF:
        return word

    return match


def correct_query(text: str) -> str:
    """Correct typos in a query string against the procurement vocabulary.

    Steps:
    1. Split into tokens
    2. Clean garbage characters from each token
    3. Fuzzy-match each token against vocabulary
    4. Reconstruct the query

    Args:
        text: Normalized (lowercased, no diacritics) query string.

    Returns:
        Query string with typos corrected.
    """
    tokens = text.split()
    corrected: list[str] = []

    for token in tokens:
        # Step 1: Clean garbage chars (e.g. 'servid=cios' → 'servicios')
        cleaned = _clean_token(token)

        # Step 2: Correct the cleaned token
        corrected.append(_correct_word(cleaned))

    return " ".join(corrected)
