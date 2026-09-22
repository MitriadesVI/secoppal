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


__all__ = ["material_residual"]
