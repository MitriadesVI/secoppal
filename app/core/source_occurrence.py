"""Occurrence-level ownership for procurement source words."""
from __future__ import annotations

from dataclasses import dataclass
import re

from app.core.entity_profile.clauses import split_clauses
from app.core.query_text_consumption import mask_consumed_spans


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
    "sabes", "solo", "solamente", "sobre", "tambien", "todavia", "un",
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
    found: list[SourceOccurrence] = []
    for index, clause in enumerate(split_clauses(text)):
        head = ANY_SOURCE_WORD.search(clause.text)
        if head is None:
            continue
        prefix = clause.text[:head.start()].split()
        if not all(token in _HEAD_FILLERS for token in prefix):
            continue
        found.append(_occurrence(clause.start, head, index))

        # ``o`` is intentionally not a clause separator because it can express
        # a thematic disjunction.  When two source heads form the explicit pair
        # "procesos o contratos", however, both occurrences are structural.
        tail = clause.text[head.end():]
        second = re.match(rf"\s+o\s+({ANY_SOURCE_WORD.pattern})", tail)
        if second is not None:
            nested = ANY_SOURCE_WORD.search(second.group(0))
            if nested is not None:
                offset = clause.start + head.end()
                found.append(_occurrence(offset, nested, index))
    return tuple(found)


def mask_source_heads(text: str) -> str:
    return mask_consumed_spans(
        text, (occurrence.span for occurrence in source_head_occurrences(text)),
    )


def is_source_word(token: str) -> bool:
    return ANY_SOURCE_WORD.fullmatch(token) is not None


def _occurrence(offset: int, match: re.Match[str], clause_index: int):
    surface = match.group(0)
    return SourceOccurrence(
        span=(offset + match.start(), offset + match.end()),
        surface=surface,
        is_process=PROCESS_SOURCE_WORDS.fullmatch(surface) is not None,
        clause_index=clause_index,
    )
