"""Material residual — the subject matter a turn still carries, losslessly.

``objeto`` is an executable projection, not evidence: the thematic extractor
drops tokens shorter than four letters, folds variants to roots, deduplicates
and keeps at most six terms.  ``contratos de gas`` and ``contratos de TIC``
therefore reach every consumer with an empty ``objeto`` — indistinguishable
from a turn that named no subject at all.  A capability that reads "empty
objeto" as "no subject" answers a question the user did not ask.

This module separates, over the same residual text the extractor receives,
**structural** tokens (the closed grammatical class the credentialed residual
already adjudicated — ``accreditation.assembly.material_tokens`` —, source-unit
framing such as ``contratos``, procurement predicates, numbers, money units,
closed state literals) from **material** tokens, which are everything else.  It applies no
length threshold, no root folding and no cap, and it keeps catalog phrases as
units.  It is the upstream owner of that distinction: consumers must not keep
their own list of "functional" words.
"""
from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Iterable

from app.core.source_occurrence import is_source_word
from app.data.departamentos import DEPARTAMENTOS
from app.data.estados import ESTADO_SYNONYMS
from app.utils.money import SMMLV_UNIT_PATTERN

_NUMBER = re.compile(r"\b\d+(?:[.,]\d+)?\b")
_MONEY_UNIT = re.compile(
    rf"\b(?:millon(?:es)?|mil|billon(?:es)?|palos?|barras?|pesos|{SMMLV_UNIT_PATTERN})\b",
)
_PUNCTUATION = re.compile(r"[?¿!¡.,;:\"'(){}\[\]]")
# Coordinators. The thematic extractor already reads ``o`` as a connector and
# drops ``y`` only because it is short; neither names a subject.
_COORDINATORS = frozenset({"o", "u", "y", "e"})


@dataclass(frozen=True, slots=True)
class MaterialOccurrence:
    """An unconsumed subject token in offsets over ``MaterialEvidence.snapshot``."""

    start: int
    end: int
    surface: str

    def to_state(self) -> dict[str, object]:
        return {"start": self.start, "end": self.end, "surface": self.surface}


@dataclass(frozen=True, slots=True)
class OccurrenceOwnership:
    """Buyer and explicit content claims over one pre-consumption snapshot."""

    snapshot: str
    buyer_span: tuple[int, int]
    content_spans: tuple[tuple[int, int], ...]

    def valid(self) -> bool:
        start, end = self.buyer_span
        return (
            0 <= start < end <= len(self.snapshot)
            and all(
                0 <= a < b <= len(self.snapshot)
                and (b <= start or end <= a)
                for a, b in self.content_spans
            )
        )

    def to_state(self) -> dict[str, object]:
        return {
            "snapshot": self.snapshot,
            "buyer_span": list(self.buyer_span),
            "content_spans": [list(span) for span in self.content_spans],
        }

    @classmethod
    def from_state(cls, value: object) -> "OccurrenceOwnership | None":
        if isinstance(value, cls):
            return value if value.valid() else None
        if not isinstance(value, dict):
            return None
        try:
            snapshot = value["snapshot"]
            buyer = tuple(value["buyer_span"])
            content = tuple(tuple(span) for span in value["content_spans"])
            if not isinstance(snapshot, str) or len(buyer) != 2:
                return None
            result = cls(snapshot, (int(buyer[0]), int(buyer[1])), tuple(
                (int(span[0]), int(span[1])) for span in content if len(span) == 2
            ))
        except (KeyError, TypeError, ValueError):
            return None
        return result if result.valid() and len(result.content_spans) == len(content) else None


@dataclass(frozen=True, slots=True)
class MaterialEvidence:
    """Post-authority residual snapshot; buyer/control spans were consumed first."""

    snapshot: str
    occurrences: tuple[MaterialOccurrence, ...]
    ownership: OccurrenceOwnership | None = None

    def to_state(self) -> dict[str, object]:
        return {
            "snapshot": self.snapshot,
            "occurrences": [item.to_state() for item in self.occurrences],
            "ownership": self.ownership.to_state() if self.ownership else None,
        }

    @classmethod
    def from_state(cls, value: object) -> "MaterialEvidence | None":
        if isinstance(value, cls):
            return value if cls._valid(value.snapshot, value.occurrences, value.ownership) else None
        if not isinstance(value, dict):
            return None
        snapshot = value.get("snapshot")
        items = value.get("occurrences")
        if not isinstance(snapshot, str) or not isinstance(items, list):
            return None
        try:
            occurrences = tuple(
                MaterialOccurrence(int(item["start"]), int(item["end"]), str(item["surface"]))
                for item in items
            )
        except (KeyError, TypeError, ValueError):
            return None
        owner_value = value.get("ownership")
        ownership = OccurrenceOwnership.from_state(owner_value) if owner_value is not None else None
        if owner_value is not None and ownership is None:
            return None
        if not cls._valid(snapshot, occurrences, ownership):
            return None
        return cls(snapshot, occurrences, ownership)

    @staticmethod
    def _valid(
        snapshot: str,
        occurrences: tuple[MaterialOccurrence, ...],
        ownership: OccurrenceOwnership | None,
    ) -> bool:
        return (ownership is None or ownership.valid()) and not any(
            item.start < 0 or item.end <= item.start or item.end > len(snapshot)
            or snapshot[item.start:item.end] != item.surface
            for item in occurrences
        )


def _is_structural(token: str, procurement_words: frozenset[str]) -> bool:
    return (
        token in _COORDINATORS
        or is_source_word(token)
        or token in DEPARTAMENTOS
        or token in procurement_words
    )


def material_residual(residual_text: str, phrases: Iterable[str] = ()) -> tuple[str, ...]:
    """Material surfaces left in ``residual_text`` plus recognized ``phrases``.

    ``residual_text`` is the text the thematic extractor is about to tokenize,
    after every typed authority consumed its spans; ``phrases`` are catalog
    phrases already lifted out of it.  Order is stable; duplicates collapse.
    """
    # Lazy: both sit above the parser in the import graph.
    from app.core.accreditation.assembly import material_tokens
    from app.core.intent_vocabulary import (
        PROCUREMENT_ACTION_NOUNS,
        PROCUREMENT_ACTION_VERB_FORMS,
    )

    procurement_words = frozenset(PROCUREMENT_ACTION_NOUNS) | frozenset(
        PROCUREMENT_ACTION_VERB_FORMS
    )
    cleaned = residual_text.lower()
    for alias in ESTADO_SYNONYMS:
        cleaned = re.sub(rf"\b{re.escape(alias)}\b", " ", cleaned)
    cleaned = _NUMBER.sub(" ", cleaned)
    cleaned = _MONEY_UNIT.sub(" ", cleaned)
    cleaned = _PUNCTUATION.sub(" ", cleaned)
    surfaces = [str(phrase) for phrase in phrases if str(phrase).strip()]
    surfaces.extend(
        token for token in material_tokens(cleaned)
        if not _is_structural(token, procurement_words)
    )
    return tuple(dict.fromkeys(surfaces))


def material_evidence(residual_text: str) -> MaterialEvidence:
    """Keep every material token occurrence after prior authorities consumed spans.

    The compatibility ``material_residual`` projection deduplicates values.
    This receipt retains two identical surfaces at two distinct offsets.
    """
    from app.core.accreditation.assembly import material_tokens
    from app.core.intent_vocabulary import (
        PROCUREMENT_ACTION_NOUNS,
        PROCUREMENT_ACTION_VERB_FORMS,
    )

    procurement_words = frozenset(PROCUREMENT_ACTION_NOUNS) | frozenset(
        PROCUREMENT_ACTION_VERB_FORMS
    )

    cleaned = residual_text.lower()
    for pattern in (re.compile(rf"\b{re.escape(alias)}\b") for alias in ESTADO_SYNONYMS):
        cleaned = pattern.sub(lambda match: " " * len(match.group()), cleaned)
    for pattern in (_NUMBER, _MONEY_UNIT, _PUNCTUATION):
        cleaned = pattern.sub(lambda match: " " * len(match.group()), cleaned)
    remaining = Counter(material_tokens(cleaned))
    topical_spans = tuple(
        match.span("value")
        for match in re.finditer(
            r"\bsobre\s+(?:(?:el|la|los|las|un|una)\s+)?(?P<value>\w+)\b",
            cleaned,
        )
    )
    occurrences: list[MaterialOccurrence] = []
    for match in re.finditer(r"\b\w+\b", cleaned):
        token = match.group()
        topical = any(
            match.start() < end and start < match.end()
            for start, end in topical_spans
        )
        if topical or (remaining[token] > 0 and not _is_structural(token, procurement_words)):
            occurrences.append(MaterialOccurrence(match.start(), match.end(), residual_text[slice(*match.span())]))
            if remaining[token] > 0:
                remaining[token] -= 1
    return MaterialEvidence(residual_text, tuple(occurrences))


__all__ = [
    "MaterialEvidence", "MaterialOccurrence", "OccurrenceOwnership",
    "material_evidence", "material_residual",
]
