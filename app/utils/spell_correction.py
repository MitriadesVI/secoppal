"""Spell correction for Colombian public procurement queries.

Corrects common typos in SECOP search queries before they hit the parser.
Uses rapidfuzz for fuzzy matching against a curated vocabulary of
contratación pública terms.
"""
from __future__ import annotations

from pathlib import Path
import re

import yaml
from rapidfuzz.distance import DamerauLevenshtein

from app.core.estado_families import NATURAL_TO_FAMILY
from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS,
    ESTADO_CONTRATO_SYNONYMS,
    ESTADO_PROCEDIMIENTO_SYNONYMS,
    ESTADO_SYNONYMS,
)
from app.utils.money import normalize_text

_MODALITY_LEXICON_PATH = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "modality_lexicon_v1.yaml"
)

# Valid semantic forms not currently present in M2 phrases.  They remain
# identity-protected without automatically becoming fuzzy destinations.
_SEMANTIC_IDENTITY_FORMS = frozenset({
    "proveedor",
    "publico",
    "publicos",
    "publicado",
    "publicada",
    "publicados",
    "publicadas",
})


def _load_modality_protected_tokens() -> frozenset[str]:
    """Load normalized M2 phrase tokens once, without importing the resolver."""
    with _MODALITY_LEXICON_PATH.open(encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    rules = payload.get("rules", []) if isinstance(payload, dict) else []
    phrases = (
        phrase
        for rule in rules
        if isinstance(rule, dict)
        for phrase in rule.get("phrases", [])
        if isinstance(phrase, str)
    )
    return frozenset(
        token
        for phrase in phrases
        for token in normalize_text(phrase).split()
    )


def _load_state_protected_tokens() -> frozenset[str]:
    """Load normalized state aliases once as identity-only vocabulary."""
    aliases = set(NATURAL_TO_FAMILY)
    for mapping in (
        ESTADO_APERTURA_SYNONYMS,
        ESTADO_CONTRATO_SYNONYMS,
        ESTADO_PROCEDIMIENTO_SYNONYMS,
        ESTADO_SYNONYMS,
    ):
        aliases.update(mapping)
    return frozenset(
        token
        for alias in aliases
        for token in normalize_text(alias).split()
    )


# This set answers only "must this valid token remain identical?".  It is
# deliberately separate from the fuzzy correction candidates below.
PROTECTED_MODALITY_VOCABULARY = _load_modality_protected_tokens()
PROTECTED_STATE_VOCABULARY = _load_state_protected_tokens()
PROTECTED_VOCABULARY: frozenset[str] = (
    PROTECTED_MODALITY_VOCABULARY
    | PROTECTED_STATE_VOCABULARY
    | _SEMANTIC_IDENTITY_FORMS
)

# ── Curated vocabulary of contratación pública terms ─────────────────────────
VOCABULARY: set[str] = {
    # ── Tipos de contrato / proceso ──────────────────────────────────────────
    "contrato",
    "contratos",
    "proceso",
    "procesos",
    "licitacion",
    "licitaciones",
    "convocatoria",
    "convocatorias",
    "adjudicacion",
    "adjudicaciones",
    "adquisicion",
    "arrendamiento",
    "concurso",
    "minima",
    "cuantia",
    "directa",
    "publica",
    # ── Entidades ────────────────────────────────────────────────────────────
    "alcaldia",
    "gobernacion",
    "municipio",
    "departamento",
    "secretaria",
    "ministerio",
    "instituto",
    "corporacion",
    "empresa",
    "entidad",
    "contratista",
    "proveedor",
    "proveedores",
    # ── Objetos de contrato frecuentes (feedback + SECOP) ────────────────────
    "construccion",
    "mantenimiento",
    "mantenimientos",
    "pavimentacion",
    "pavimento",
    "alimentacion",
    "suministro",
    "suministros",
    "interventoria",
    "consultoria",
    "obra",
    "obras",
    "transporte",
    "transportes",
    "infraestructura",
    "educacion",
    "salud",
    "vivienda",
    "acueducto",
    "alcantarillado",
    "saneamiento",
    "reparacion",
    "reparaciones",
    "rehabilitacion",
    "remodelacion",
    "ampliacion",
    "mejoramiento",
    "dotacion",
    "dotaciones",
    "capacitacion",
    "supervision",
    "asesoria",
    "prestacion",
    "servicios",
    "profesionales",
    "adecuacion",
    "adecuaciones",
    # ── Términos del feedback.jsonl ──────────────────────────────────────────
    "vial",
    "viales",
    "vias",
    "ferreas",
    "ferrea",
    "hidrografia",
    "hidrograficos",
    "mobiliario",
    "destronque",
    "destronques",
    "agroforestales",
    "agroforestal",
    "diplomado",
    "diplomados",
    # ── Estados ──────────────────────────────────────────────────────────────
    "adjudicado",
    "adjudicados",
    "publicado",
    "publicados",
    "abierto",
    "abiertos",
    "liquidado",
    "liquidados",
    "terminado",
    "terminados",
    "celebrado",
    "celebrados",
    # ── Otros frecuentes en Colombia ─────────────────────────────────────────
    "aseo",
    "vigilancia",
    "energia",
    "electrico",
    "electrica",
    "ambiental",
    "residuos",
    "recreacion",
    "deportivo",
    "deportiva",
    "escolar",
    "colegio",
    "colegios",
    "hospital",
    "hospitales",
    "parque",
    "parques",
    "aceras",
    "andenes",
    "señalizacion",
    "senalizacion",
}

# Pre-sorted list for rapidfuzz (tuple for immutability)
_VOCAB_LIST: tuple[str, ...] = tuple(sorted(VOCABULARY))

# Characters that sometimes appear as typos inside words (keyboard accidents)
_GARBAGE_CHARS_RE = re.compile(r"[=+\[\]{}<>|\\@#~^]")

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
    - Protected or known words → untouched
    - Words with numbers or remaining special chars → untouched
    - Otherwise, correct only a unique Damerau-Levenshtein distance-1 match
    """
    # Don't correct very short words
    if len(word) <= 3:
        return word

    # Exact domain language must never be rewritten.
    if word in PROTECTED_VOCABULARY or word in VOCABULARY:
        return word

    # Don't correct numeric or special-char tokens
    if _is_numeric_or_special(word):
        return word

    matches = [
        candidate
        for candidate in _VOCAB_LIST
        if DamerauLevenshtein.distance(word, candidate) == 1
    ]
    return matches[0] if len(matches) == 1 else word


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
