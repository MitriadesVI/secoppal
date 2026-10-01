"""The ONLY constructor of ``EvidenceWindow`` (spine v5, C0 §5.5.2; invariant 6).

Takes the raw turn and the cited ``RawSpan``s of one condition and returns the quote plus at most
``WINDOW_TOKENS`` folded tokens on each side.  The raw text never leaves this function: the window
holds no reference to it and an authority never receives it.  Call it after ``validate_raw``.

Folding: the same as M1-H0 (``foundation_time_coverage``): ``app.utils.money.normalize_text``
(NFKD, combining marks removed, lowercase) and then the tokens ``[a-z0-9]+``.  A token that
compatibility folding creates («²» -> «2») is a token of the folded neighbour text, never a
character the turn does not contain.

Declared behaviour:
- The window is built around the FIRST cited span (left) and the LAST one (right); text between
  spans is never exposed.  Therefore spans that are not one contiguous phrase are refused with
  ``DISJOINT_EVIDENCE`` (a repeated citation of the same phrase in two places would otherwise hide
  the qualifier of the middle occurrences).  The authority never RESOLVES what it cannot see.
- Neighbouring text is cut at the span boundary even inside a word: ``«obras»`` cited in
  ``«sobras»`` has the left token ``«s»``.  It is the literal adjacent text and takes one slot.
- A window does not stop at punctuation or sentence ends: more context only withdraws authority.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

from app.core.foundation_authority_contract import WINDOW_TOKENS, EvidenceWindow
from app.core.foundation_evidence import material_phrase
from app.core.interpretation_contract import ContractError, RawSpan
from app.core.semantic_dimensions import SemanticDimension
from app.utils.money import normalize_text

_TOKEN = re.compile(r"[a-z0-9]+")


class EvidenceWindowError(ContractError):
    """The spans cannot be turned into one window; ``reason`` is a closed code, never raw text."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def window_tokens(text: str) -> tuple[str, ...]:
    """The folded ``[a-z0-9]+`` tokens of ``text`` (the single tokenizer of the window)."""
    return tuple(_TOKEN.findall(normalize_text(text)))


def build_window(raw: str, spans: Iterable[RawSpan],
                 dimension: SemanticDimension | str) -> EvidenceWindow:
    if type(raw) is not str:
        raise EvidenceWindowError("INVALID_RAW_TEXT")
    ordered = sorted({(span.start, span.end): span for span in spans}.values(),
                     key=lambda span: (span.start, span.end))
    if not ordered or any(type(span) is not RawSpan for span in ordered):
        raise EvidenceWindowError("NO_MATERIAL_EVIDENCE")
    if any(not span.binds(raw) for span in ordered):
        raise EvidenceWindowError("UNBOUND_RAW_SPAN")
    name = dimension.value if isinstance(dimension, SemanticDimension) else str(dimension)
    phrase, reason = material_phrase(ordered, raw, name)
    if phrase is None:
        raise EvidenceWindowError(reason)
    reach = ordered[0].end
    for span in ordered[1:]:
        if raw[reach:span.start].strip():
            raise EvidenceWindowError("DISJOINT_EVIDENCE")
        reach = max(reach, span.end)
    first, last = ordered[0].start, reach
    return EvidenceWindow(
        spans=tuple(ordered),
        quote=unicodedata.normalize("NFC", phrase),
        left=window_tokens(raw[:first])[-WINDOW_TOKENS:],
        right=window_tokens(raw[last:])[:WINDOW_TOKENS],
    )
