"""Shared lexical primitives for deterministic Spanish negation handling.

This module deliberately knows nothing about query dimensions or execution
policy.  It owns the linguistic vocabulary that used to be duplicated across
the modality subsystem and exposes offset-preserving helpers for callers that
already know which text is eligible for negation analysis.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.core.modality_text import contains_phrase, find_occurrences


# Keep this tuple behavior-equivalent to the former modality-local table.
EXTERNAL_NEGATIVE_OPERATORS = (
    "que no sean", "que no sea", "diferentes de", "diferente de",
    "distintos de", "distinto de", "excluyendo", "excepto", "salvo",
    "no", "sin",
)

# Additional surface forms used by the cross-dimensional binding layer.  The
# modality resolver intentionally continues to use the narrower tuple above.
DIMENSIONAL_NEGATIVE_OPERATORS = (
    "que no sean", "que no sea", "no sean", "no sea",
    "diferentes de", "diferente de",
    "distintos de", "distinto de", "que excluyan", "excluyendo",
    "excluyan", "excluye", "excluir", "excepto", "salvo", "menos",
    "no", "sin",
)

ANTI_NEGATION_PATTERNS = (
    "no solo", "sin descartar", "sin excluir",
    "no quiero excluir", "no busco excluir",
)

# Cross-dimensional protection extends the exact legacy modality table without
# changing the vocabulary imported by modality itself.
DIMENSIONAL_ANTI_NEGATION_PATTERNS = (
    *ANTI_NEGATION_PATTERNS,
    # Existing modality follow-up controls; their marker mutates a prior
    # filter and does not negate the following lexical value generically.
    "sin filtro de modalidad", "sin modalidad",
    "no importa la modalidad", "deja de excluir",
    # Evidence-backed Colombian procurement/domain terms (ADR-064).
    "entidades sin animo de lucro", "entidad sin animo de lucro",
    "sin animo de lucro", "sin formula de reajuste", "sin perjuicio de",
)

# The composition layer historically used this exact subset as attachment
# blockers.  Keeping it here removes the third marker table without expanding
# modality behavior.
REFINEMENT_NEGATIVE_BLOCKERS = (
    "excepto", "salvo", "no", "sin", "diferente de", "diferentes de",
    "distinto de", "distintos de", "excluyendo",
)


@dataclass(frozen=True, slots=True)
class NegativeMarker:
    text: str
    start: int
    end: int


def _overlaps(span: tuple[int, int], others: Iterable[tuple[int, int]]) -> bool:
    start, end = span
    return any(start < other_end and other_start < end for other_start, other_end in others)


def mask_phrases(text: str, phrases: Iterable[str]) -> str:
    """Mask every phrase occurrence with spaces while preserving offsets."""
    chars = list(text)
    for phrase in phrases:
        for start, end in find_occurrences(text, phrase):
            chars[start:end] = " " * (end - start)
    return "".join(chars)


def has_external_negative_operator(norm_gap: str) -> bool:
    """Return whether an eligible gap contains an external negative marker.

    Callers remain responsible for supplying a gap outside a recognized alias.
    This is the exact discipline used by modality before the extraction of this
    shared module.
    """
    operator_gap = mask_phrases(norm_gap, ANTI_NEGATION_PATTERNS)
    return any(
        contains_phrase(operator_gap, phrase)
        for phrase in EXTERNAL_NEGATIVE_OPERATORS
    )


def find_dimensional_negative_markers(
    text: str,
    *,
    protected_spans: Iterable[tuple[int, int]] = (),
) -> tuple[NegativeMarker, ...]:
    """Find non-overlapping dimensional markers outside protected spans.

    Longest forms win (``que no sean`` over the nested ``no``).  Protected
    spans are typically resolved aliases, monetary comparators, or other text
    whose internal ``no``/``sin`` is not a cross-dimensional operator.
    """
    protected = tuple(protected_spans)
    eligible = mask_phrases(text, DIMENSIONAL_ANTI_NEGATION_PATTERNS)
    candidates: list[NegativeMarker] = []
    for phrase in DIMENSIONAL_NEGATIVE_OPERATORS:
        for start, end in find_occurrences(eligible, phrase):
            if _overlaps((start, end), protected):
                continue
            candidates.append(NegativeMarker(text[start:end], start, end))

    accepted: list[NegativeMarker] = []
    for candidate in sorted(
        candidates,
        key=lambda item: (item.start, -(item.end - item.start), item.text),
    ):
        if _overlaps((candidate.start, candidate.end), (
            (item.start, item.end) for item in accepted
        )):
            continue
        accepted.append(candidate)
    return tuple(sorted(accepted, key=lambda item: (item.start, item.end)))
