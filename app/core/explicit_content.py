"""Occurrence ownership for text explicitly mentioned as contract subject matter.

Quotes alone confer no authority.  The surrounding object construction must
accredit the literal before analytic controls may inspect the turn.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.material_residual import OccurrenceOwnership


_OBJECT_LITERAL = re.compile(
    r"\b(?:cuyo\s+objeto|objeto\s+que)\s+"
    r"(?:contenga|mencione|diga|incluya)\s*"
    r"(?P<open>['\"“«])(?P<value>[^'\"“”«»]+)(?P<close>['\"”»])",
)
_SEARCH_LITERAL = re.compile(
    r"\bbusca\s+(?P<open>['\"“«])(?P<value>[^'\"“”«»]+)"
    r"(?P<close>['\"”»])\s+en\s+(?:el|su)\s+objeto\b",
)
_OBJECT_WORD = re.compile(
    r"\b(?:cuyo\s+objeto|objeto\s+que)\s+"
    r"(?:contenga|mencione|diga|incluya)\s+(?P<value>[\w]+)\b",
)
_SUBJECT_WORD = re.compile(
    r"\bsobre\s+(?:(?:el|la|los|las|un|una)\s+)?(?P<value>\w+)\b"
)
_CONTRACT_HEAD = re.compile(r"\b(?:contratos?|procesos?)\b")
_CLOSE = {"'": "'", '"': '"', "“": "”", "«": "»"}


@dataclass(frozen=True, slots=True)
class ContentOccurrence:
    start: int
    end: int
    surface: str
    strong: bool = False

    @property
    def span(self) -> tuple[int, int]:
        return self.start, self.end


def explicit_content_occurrences(text: str) -> tuple[ContentOccurrence, ...]:
    """Return only the value occurrence governed by an accredited construction."""
    found: list[ContentOccurrence] = []
    for pattern in (_OBJECT_LITERAL, _SEARCH_LITERAL):
        for match in pattern.finditer(text):
            if _CLOSE[match.group("open")] != match.group("close"):
                continue
            start, end = match.span("value")
            found.append(ContentOccurrence(start, end, text[start:end], strong=True))
    for match in _OBJECT_WORD.finditer(text):
        start, end = match.span("value")
        if not any(start < item.end and item.start < end for item in found):
            found.append(ContentOccurrence(start, end, text[start:end], strong=True))
    for match in _SUBJECT_WORD.finditer(text):
        if not _CONTRACT_HEAD.search(text[:match.start()]):
            continue
        start, end = match.span("value")
        if not any(start < item.end and item.start < end for item in found):
            found.append(ContentOccurrence(start, end, text[start:end]))
    return tuple(sorted(found, key=lambda item: item.start))


def strong_content_spans(text: str) -> tuple[tuple[int, int], ...]:
    return tuple(item.span for item in explicit_content_occurrences(text) if item.strong)


def adjudicate_buyer_ownership(
    snapshot: str, buyer_span: tuple[int, int],
) -> OccurrenceOwnership | None:
    """Strong content wins a shared span; weak content yields to the buyer."""
    start, end = buyer_span
    content = explicit_content_occurrences(snapshot)
    def overlaps(item: ContentOccurrence) -> bool:
        return start < item.end and item.start < end

    if any(item.strong and overlaps(item) for item in content):
        return None
    return OccurrenceOwnership(
        snapshot=snapshot,
        buyer_span=buyer_span,
        content_spans=tuple(item.span for item in content if not overlaps(item)),
    )


__all__ = [
    "ContentOccurrence", "explicit_content_occurrences", "strong_content_spans",
    "adjudicate_buyer_ownership",
]
