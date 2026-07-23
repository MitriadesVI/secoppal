"""Primitivas de texto trazables para el resolver de modalidades M2.

Este módulo concentra únicamente normalización, offsets, tokenización, cláusulas,
contextos locales y operaciones de spans. No conoce el catálogo, el lexicón ni
los estados públicos de resolución.
"""
from __future__ import annotations

import bisect
import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Protocol

_STRONG_BOUNDARY_CHARS = frozenset(".;:?!\n\r")
_CONTRASTIVE_BOUNDARIES = ("sin embargo", "en cambio", "pero", "sino")
_CONTEXT_WINDOW_TOKENS = 3


@dataclass(frozen=True)
class Clause:
    """Rango de cláusula en coordenadas originales y normalizadas."""

    index: int
    orig_start: int
    orig_end: int
    norm_start: int
    norm_end: int


class HasOriginalSpan(Protocol):
    original_start: int
    original_end: int


def _fold_char(ch: str) -> list[str]:
    folded = unicodedata.normalize("NFD", ch.casefold())
    return [candidate for candidate in folded if not unicodedata.combining(candidate)]


def normalize_with_map(text: str) -> tuple[str, tuple[int, ...]]:
    """Devuelve texto normalizado y mapa índice normalizado → índice original."""
    raw_chars: list[str] = []
    raw_indexes: list[int] = []
    for original_index, ch in enumerate(text):
        for folded in _fold_char(ch):
            raw_chars.append(folded if folded.isalnum() else " ")
            raw_indexes.append(original_index)

    normalized: list[str] = []
    positions: list[int] = []
    previous_space = True
    for ch, original_index in zip(raw_chars, raw_indexes):
        if ch == " ":
            if previous_space:
                continue
            normalized.append(" ")
            positions.append(original_index)
            previous_space = True
        else:
            normalized.append(ch)
            positions.append(original_index)
            previous_space = False
    while normalized and normalized[-1] == " ":
        normalized.pop()
        positions.pop()
    return "".join(normalized), tuple(positions)


def normalize_phrase(text: str) -> str:
    return normalize_with_map(text)[0]


def find_occurrences(norm: str, phrase: str) -> list[tuple[int, int]]:
    """Busca una frase con límites alfanuméricos, sin substring parcial."""
    occurrences: list[tuple[int, int]] = []
    if not phrase:
        return occurrences
    cursor = 0
    while True:
        start = norm.find(phrase, cursor)
        if start < 0:
            return occurrences
        end = start + len(phrase)
        before = norm[start - 1] if start else " "
        after = norm[end] if end < len(norm) else " "
        if not before.isalnum() and not after.isalnum():
            occurrences.append((start, end))
        cursor = start + 1


def contains_phrase(text: str, phrase: str) -> bool:
    return bool(find_occurrences(text, phrase))


def token_distance(norm: str, left_end: int, right_start: int) -> int:
    """Cuenta tokens normalizados estrictamente entre dos spans."""
    if right_start < left_end:
        left_end, right_start = right_start, left_end
    return len(norm[left_end:right_start].split())


def comma_count(text: str) -> int:
    return text.count(",")


def is_separator_only(norm_gap: str) -> bool:
    """Verdadero cuando la normalización eliminó toda puntuación del gap."""
    return not norm_gap.strip()


def _merge_ranges(ranges: Iterable[tuple[int, int]]) -> list[tuple[int, int]]:
    merged: list[tuple[int, int]] = []
    for start, end in sorted(ranges):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    return merged


def segment_clauses(text: str, norm: str, positions: tuple[int, ...]) -> tuple[Clause, ...]:
    """Segmenta por fronteras fuertes y contrastivas; la coma permanece débil."""
    boundaries: list[tuple[int, int]] = []
    for index, char in enumerate(text):
        if char not in _STRONG_BOUNDARY_CHARS:
            continue
        if char == ".":
            token_match = re.search(r"([A-Za-zÁÉÍÓÚÜÑáéíóúüñ]+)$", text[:index])
            if token_match and token_match.group(1).casefold() in {"min"}:
                continue
        boundaries.append((index, index + 1))

    for marker in _CONTRASTIVE_BOUNDARIES:
        for norm_start, norm_end in find_occurrences(norm, marker):
            boundaries.append((positions[norm_start], positions[norm_end - 1] + 1))

    original_ranges: list[tuple[int, int]] = []
    cursor = 0
    for start, end in _merge_ranges(boundaries):
        if cursor < start:
            original_ranges.append((cursor, start))
        cursor = max(cursor, end)
    if cursor < len(text):
        original_ranges.append((cursor, len(text)))
    if not original_ranges:
        original_ranges = [(0, len(text))]

    clauses: list[Clause] = []
    for orig_start, orig_end in original_ranges:
        norm_start = bisect.bisect_left(positions, orig_start)
        norm_end = bisect.bisect_left(positions, orig_end)
        if norm_start >= norm_end:
            continue
        clauses.append(Clause(
            index=len(clauses),
            orig_start=orig_start,
            orig_end=orig_end,
            norm_start=norm_start,
            norm_end=norm_end,
        ))
    if not clauses and norm:
        clauses.append(Clause(0, 0, len(text), 0, len(norm)))
    return tuple(clauses)


def clause_for_span(
    clauses: tuple[Clause, ...], original_start: int, original_end: int,
) -> Clause | None:
    for clause in clauses:
        if clause.orig_start <= original_start and original_end <= clause.orig_end:
            return clause
    return None


def context_present_locally(
    norm: str,
    clause: Clause,
    candidate_start: int,
    candidate_end: int,
    phrases: tuple[str, ...],
    *,
    window_tokens: int = _CONTEXT_WINDOW_TOKENS,
) -> bool:
    """Evalúa contexto en una ventana tokenizada de la misma cláusula."""
    if not phrases:
        return False
    token_matches = list(re.finditer(r"[^\s]+", norm[clause.norm_start:clause.norm_end]))
    if not token_matches:
        return False
    tokens = [
        (clause.norm_start + match.start(), clause.norm_start + match.end())
        for match in token_matches
    ]
    first = next((i for i, (_, end) in enumerate(tokens) if end > candidate_start), 0)
    last = max(
        first,
        next((
            i for i in range(len(tokens) - 1, -1, -1)
            if tokens[i][0] < candidate_end
        ), first),
    )
    low = max(0, first - window_tokens)
    high = min(len(tokens) - 1, last + window_tokens)
    window = norm[tokens[low][0]:tokens[high][1]]
    return any(contains_phrase(window, phrase) for phrase in phrases)


def consumed_spans(*collections: Iterable[HasOriginalSpan]) -> tuple[tuple[int, int], ...]:
    spans = {
        (item.original_start, item.original_end)
        for collection in collections
        for item in collection
    }
    return tuple(sorted(spans))


def validate_non_overlapping_spans(*collections: Iterable[HasOriginalSpan]) -> None:
    """Falla cerrado si dos salidas finales ocupan rangos originales solapados."""
    ordered = sorted(
        (
            item.original_start,
            item.original_end,
            type(item).__name__,
        )
        for collection in collections
        for item in collection
    )
    for previous, current in zip(ordered, ordered[1:]):
        if current[0] < previous[1]:
            raise ValueError(
                "invariante de spans violada: salidas solapadas "
                f"{previous[:2]} y {current[:2]}"
            )


def residual_text(text: str, spans: tuple[tuple[int, int], ...]) -> str:
    if not spans:
        return " ".join(text.split())
    covered = [False] * len(text)
    for start, end in spans:
        for index in range(start, min(end, len(text))):
            covered[index] = True
    return " ".join("".join(
        char for index, char in enumerate(text) if not covered[index]
    ).split())
