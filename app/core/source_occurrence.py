"""Occurrence-level ownership for procurement source requests."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
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
from app.core.negation_markers import has_external_negative_operator
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
_POSTPOSED_SOURCE = re.compile(
    r"\ben\s+(?:(?:el|la|los|las)\s+)?"
    r"(?P<source>procesos?|licitaciones?|convocatorias?|procedimientos?|contratos?)\b",
)


class SourceOccurrenceRole(str, Enum):
    HEAD = "HEAD"
    POSTPOSED = "POSTPOSED"


@dataclass(frozen=True, slots=True)
class SourceOccurrence:
    span: tuple[int, int]
    surface: str
    is_process: bool
    clause_index: int
    role: SourceOccurrenceRole = SourceOccurrenceRole.HEAD


def source_request_occurrences(text: str) -> tuple[SourceOccurrence, ...]:
    """Return source words structurally accredited as user requests.

    The admitted grammar is intentionally narrow: existing clause heads, or a
    single terminal ``en [article] source`` construction when no head already
    owns the request.  The latter admits orthogonal temporal text but rejects a
    material tail, so ``en procesos industriales`` remains topic language.
    """
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
            and _continues_thematic_complement(clauses, previous, candidate)
        ):
            continue
        found.append(candidate)
    if found:
        return tuple(found)
    return _postposed_occurrences(clauses)


def source_head_occurrences(text: str) -> tuple[SourceOccurrence, ...]:
    """Compatibility name for the single source-request authority."""
    return source_request_occurrences(text)


def mask_source_requests(text: str) -> str:
    return mask_consumed_spans(
        text, (occurrence.span for occurrence in source_request_occurrences(text)),
    )


def mask_source_heads(text: str) -> str:
    """Compatibility name for masking every accredited source request."""
    return mask_source_requests(text)


def is_source_word(token: str) -> bool:
    return ANY_SOURCE_WORD.fullmatch(token) is not None


def _is_request_framing(prefix: str) -> bool:
    """The existing functional-word authority may precede a source head."""
    if has_external_negative_operator(prefix):
        return False
    tokens = re.findall(r"\b\w+\b", prefix)
    return all(token in STOPWORDS or token in _HEAD_FILLERS for token in tokens)


def _postposed_occurrences(
    clauses: tuple[Clause, ...],
) -> tuple[SourceOccurrence, ...]:
    candidates: list[SourceOccurrence] = []
    for index, clause in enumerate(clauses):
        for match in _POSTPOSED_SOURCE.finditer(clause.text):
            prefix = clause.text[:match.start()]
            tail = clause.text[match.end():]
            if not _has_material_text(prefix) or not _is_orthogonal_tail(tail):
                continue
            candidates.append(_occurrence(
                clause.start,
                match,
                index,
                SourceOccurrenceRole.POSTPOSED,
                group="source",
            ))
    return tuple(candidates) if len(candidates) == 1 else ()


def _has_material_text(text: str) -> bool:
    return any(
        token not in STOPWORDS and token not in _HEAD_FILLERS
        for token in re.findall(r"\b\w+\b", text)
    )


def _is_orthogonal_tail(tail: str) -> bool:
    resolution = resolve_temporal_scope(tail)
    residual = tail
    if resolution.matched and resolution.span is not None:
        residual = mask_consumed_spans(residual, (resolution.span,))
    return not _has_material_text(residual)


def _continues_thematic_complement(
    clauses: tuple[Clause, ...],
    previous: SourceOccurrence,
    candidate: SourceOccurrence,
) -> bool:
    if previous.clause_index == candidate.clause_index:
        return False
    if not _opens_thematic_complement(clauses[previous.clause_index], previous):
        return False
    candidate_clause = clauses[candidate.clause_index]
    if previous.is_process == candidate.is_process:
        # A year alone is orthogonal to the theme and cannot turn its repeated
        # noun into a fresh source clause.  An event/state can own a new clause.
        return not _has_event_or_state_qualifier(candidate_clause, candidate)
    return not _has_source_qualifier(candidate_clause, candidate)


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
    if _has_event_or_state_qualifier(clause, occurrence):
        return True
    relative_end = occurrence.span[1] - clause.start
    return resolve_temporal_scope(clause.text[relative_end:]).matched


def _has_event_or_state_qualifier(
    clause: Clause,
    occurrence: SourceOccurrence,
) -> bool:
    """An event/state can reopen a source clause; a bare period cannot."""
    relative_end = occurrence.span[1] - clause.start
    tail = clause.text[relative_end:]
    if any(
        pattern.search(tail)
        for pattern in (_PUBLICATION, _SIGNATURE, _UNSUPPORTED_EVENTS, _CURRENT_MARKER)
    ):
        return True
    dataset = "procesos" if occurrence.is_process else "contratos"
    return detect_estado_family(tail, dataset) is not None


def _occurrence(
    offset: int,
    match: re.Match[str],
    clause_index: int,
    role: SourceOccurrenceRole = SourceOccurrenceRole.HEAD,
    *,
    group: int | str = 0,
):
    surface = match.group(group)
    return SourceOccurrence(
        span=(offset + match.start(group), offset + match.end(group)),
        surface=surface,
        is_process=PROCESS_SOURCE_WORDS.fullmatch(surface) is not None,
        clause_index=clause_index,
        role=role,
    )
