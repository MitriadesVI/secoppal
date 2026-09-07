"""Occurrence-level ownership for procurement source words."""
from __future__ import annotations

from dataclasses import dataclass
import re

from app.data.stopwords import STOPWORDS
from app.core.entity_profile.clauses import Clause, split_clauses
from app.core.entity_profile.temporal_event import (
    _CURRENT_MARKER,
    _PUBLICATION,
    _SIGNATURE,
    _UNSUPPORTED_EVENTS,
)
from app.core.estado_families import detect_estado_family
from app.core.query_text_consumption import mask_consumed_spans
from app.core.temporal_semantics import resolve_temporal_scope


PROCESS_SOURCE_WORDS = re.compile(
    r"\b(?:procesos?|licitaciones?|convocatorias?|procedimientos?)\b",
)
CONTRACT_SOURCE_WORDS = re.compile(r"\b(?:contratos?)\b")
ANY_SOURCE_WORD = re.compile(
    r"\b(?:procesos?|licitaciones?|convocatorias?|procedimientos?|contratos?)\b",
)

_HEAD_FILLERS = frozenset({
    "a", "actualmente", "ahora", "analiza", "analizame", "analisis", "aun",
    "como", "contrata", "contratado", "contratista", "contratistas", "dame",
    "de", "del", "e", "el", "en", "entidad", "evolucion", "ha", "hoy",
    "la", "las", "los", "modalidad", "modalidades", "monto", "montos",
    "muestra", "muestrame", "o", "perfil", "principales", "proveedor",
    "proveedores", "que", "quiero", "radiografia", "reciente", "recientes",
    "sabes", "solo", "solamente", "sobre", "sus", "tambien", "todavia", "un",
    "una", "unicamente", "ultimo", "ultimos", "valor", "valores", "ver", "y",
})


@dataclass(frozen=True, slots=True)
class SourceOccurrence:
    span: tuple[int, int]
    surface: str
    is_process: bool
    clause_index: int


def source_head_occurrences(text: str) -> tuple[SourceOccurrence, ...]:
    """Return only source-word occurrences accredited as clause heads."""
    clauses = split_clauses(text)
    candidates: list[SourceOccurrence] = []
    for index, clause in enumerate(clauses):
        owned: list[tuple[int, int]] = []
        for match in ANY_SOURCE_WORD.finditer(clause.text):
            prefix = mask_consumed_spans(
                clause.text[:match.start()],
                tuple(span for span in owned if span[1] <= match.start()),
            )
            if not _is_request_framing(prefix):
                continue
            candidates.append(_occurrence(clause.start, match, index))
            owned.append(match.span())

    found: list[SourceOccurrence] = []
    for candidate in candidates:
        previous = found[-1] if found else None
        if (
            previous is not None
            and previous.clause_index != candidate.clause_index
            and _opens_thematic_complement(clauses[previous.clause_index], previous)
            and not _has_source_qualifier(clauses[candidate.clause_index], candidate)
        ):
            continue
        found.append(candidate)
    return tuple(found)


def mask_source_heads(text: str) -> str:
    return mask_consumed_spans(
        text, (occurrence.span for occurrence in source_head_occurrences(text)),
    )


def is_source_word(token: str) -> bool:
    return ANY_SOURCE_WORD.fullmatch(token) is not None


def _is_request_framing(prefix: str) -> bool:
    """The existing functional-word authority may precede a source head."""
    tokens = re.findall(r"\b\w+\b", prefix)
    return all(token in STOPWORDS or token in _HEAD_FILLERS for token in tokens)


def _opens_thematic_complement(
    clause: Clause,
    occurrence: SourceOccurrence,
) -> bool:
    """Whether the first head already introduced material topic language."""
    if _has_source_qualifier(clause, occurrence):
        return False
    relative_end = occurrence.span[1] - clause.start
    tail = clause.text[relative_end:]
    return any(
        token not in STOPWORDS and token not in _HEAD_FILLERS
        for token in re.findall(r"\b\w+\b", tail)
    )


def _has_source_qualifier(
    clause: Clause,
    occurrence: SourceOccurrence,
) -> bool:
    """Reuse temporal/state authorities to distinguish a new source clause."""
    relative_end = occurrence.span[1] - clause.start
    tail = clause.text[relative_end:]
    if any(
        pattern.search(tail)
        for pattern in (_PUBLICATION, _SIGNATURE, _UNSUPPORTED_EVENTS, _CURRENT_MARKER)
    ):
        return True
    if resolve_temporal_scope(tail).matched:
        return True
    dataset = "procesos" if occurrence.is_process else "contratos"
    return detect_estado_family(tail, dataset) is not None


def _occurrence(offset: int, match: re.Match[str], clause_index: int):
    surface = match.group(0)
    return SourceOccurrence(
        span=(offset + match.start(), offset + match.end()),
        surface=surface,
        is_process=PROCESS_SOURCE_WORDS.fullmatch(surface) is not None,
        clause_index=clause_index,
    )
