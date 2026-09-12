"""Spell correction for Colombian public procurement queries.

Corrects common typos in SECOP search queries before they hit the parser.
Uses rapidfuzz for fuzzy matching against a curated vocabulary of
contratación pública terms.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Callable

import yaml
from rapidfuzz.distance import DamerauLevenshtein

from app.core.estado_families import NATURAL_TO_FAMILY
from app.data.estados import (
    ESTADO_APERTURA_SYNONYMS,
    ESTADO_CONTRATO_SYNONYMS,
    ESTADO_PROCEDIMIENTO_SYNONYMS,
    ESTADO_SYNONYMS,
)
from app.data.stopwords import STOPWORDS
from app.core.text_lineage import PartitionReceipt, Proof
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
    # Autoridad temporal: estos tokens deben llegar intactos al parser. Sin
    # protección, ``dias`` se corregía por distancia 1 a ``vias`` y contaminaba
    # el objeto contractual de "últimos 30 días".
    "dia",
    "dias",
    "semana",
    "semanas",
    "mes",
    "meses",
    "ano",
    "desde",
    "hasta",
    "pasado",
    "pasada",
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


# ── INV-SPELL-CORRECTION-CANNOT-CREATE-SEMANTIC-DIMENSION ────────────────
#
# Una corrección ortográfica puede arreglar cómo se escribió una palabra. No
# puede decidir DE QUÉ habla la consulta.
#
# ``otra`` es una palabra corriente del español; ``obra`` es un tipo
# contractual, y están a distancia 1. El corrector convertía la una en la otra,
# así que "empecemos otra vez" pedía obras. Lo revelador es por qué pasaba la
# defensa: ``otra`` SÍ estaba en el lenguaje funcional, pero ese scrub corre
# DESPUÉS. Para cuando actuaba, la palabra ya era ``obra`` — que no es relleno,
# sino un tema legítimo. Una capa reescribe el texto y las defensas de la
# siguiente quedan apuntando a un texto que ya no existe.
#
# El arreglo es de orden, no de catálogo: el lenguaje funcional se protege por
# identidad ANTES de que el corrector pueda tocarlo. No se sacrifica ninguna
# corrección real —el vocabulario de destino no cambia—, solo se prohíbe que el
# origen sea una palabra que el dominio ya declaró vacía.
PROTECTED_MODALITY_VOCABULARY = _load_modality_protected_tokens()
PROTECTED_STATE_VOCABULARY = _load_state_protected_tokens()
PROTECTED_FUNCTIONAL_VOCABULARY = frozenset(
    normalize_text(token) for token in STOPWORDS
)
PROTECTED_VOCABULARY: frozenset[str] = (
    PROTECTED_MODALITY_VOCABULARY
    | PROTECTED_STATE_VOCABULARY
    | PROTECTED_FUNCTIONAL_VOCABULARY
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


@dataclass(frozen=True, slots=True)
class SpellCorrectionResult:
    text: str
    receipts: tuple[PartitionReceipt, ...]


ReceiptSink = Callable[[str, tuple[PartitionReceipt, ...]], None]


def _correct_query_result(
    text: str,
    *,
    protected_spans: tuple[tuple[int, int], ...],
    emit_receipts: bool,
) -> SpellCorrectionResult:
    from app.core.query_text_consumption import shield_spans_with_origins

    shielded = shield_spans_with_origins(text, protected_spans)
    tokens: list[tuple[tuple[int, int], str]] = []
    for match in re.finditer(r"\S+", shielded.text):
        origins = {
            origin
            for index in range(match.start(), match.end())
            for origin in shielded.original_intervals[index]
        }
        if not origins:
            continue
        input_span = (min(start for start, _ in origins), max(end for _, end in origins))
        corrected = _correct_word(_clean_token(match.group(0)))
        for marker, original in shielded.replacements.items():
            corrected = corrected.replace(marker, original)
        tokens.append((input_span, corrected))

    corrected_text = " ".join(token for _, token in tokens)
    if not emit_receipts:
        return SpellCorrectionResult(corrected_text, ())

    receipts: list[PartitionReceipt] = []
    input_cursor = output_cursor = 0
    for index, (input_span, corrected) in enumerate(tokens):
        gap_output = "" if index == 0 else " "
        gap_input = text[input_cursor:input_span[0]]
        receipts.append(PartitionReceipt(
            input_span=(input_cursor, input_span[0]),
            output_span=(output_cursor, output_cursor + len(gap_output)),
            input_text=gap_input,
            output_text=gap_output,
            operation=(
                "KEEP" if gap_input == gap_output else "MERGE" if gap_output else "DELETE"
            ),
            proof=Proof.EXACT if gap_input == gap_output else Proof.WHOLE_SEGMENT_ONLY,
        ))
        output_cursor += len(gap_output)
        original = text[slice(*input_span)]
        receipts.append(PartitionReceipt(
            input_span=input_span,
            output_span=(output_cursor, output_cursor + len(corrected)),
            input_text=original,
            output_text=corrected,
            operation="KEEP" if original == corrected else "CORRECT",
            proof=Proof.EXACT if original == corrected else Proof.WHOLE_SEGMENT_ONLY,
        ))
        input_cursor = input_span[1]
        output_cursor += len(corrected)
    tail = text[input_cursor:]
    receipts.append(PartitionReceipt(
        input_span=(input_cursor, len(text)),
        output_span=(output_cursor, output_cursor),
        input_text=tail,
        output_text="",
        operation="KEEP" if not tail else "DELETE",
        proof=Proof.EXACT,
    ))
    return SpellCorrectionResult(corrected_text, tuple(receipts))


def correct_query_with_receipts(
    text: str,
    *,
    protected_spans: tuple[tuple[int, int], ...] = (),
    receipt_sink: ReceiptSink | None = None,
) -> SpellCorrectionResult:
    """Correct once and expose the token receipts emitted by that same loop."""
    result = _correct_query_result(
        text,
        protected_spans=protected_spans,
        emit_receipts=True,
    )
    if receipt_sink is not None:
        receipt_sink(result.text, result.receipts)
    return result


def correct_query(text: str, *, protected_spans: tuple[tuple[int, int], ...] = ()) -> str:
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
    return _correct_query_result(
        text,
        protected_spans=protected_spans,
        emit_receipts=False,
    ).text
