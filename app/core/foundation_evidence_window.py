"""The ONLY constructor of ``EvidenceWindow`` (spine v5, C0 §5.5.2; invariant 6; M1-C1.1 R1).

Takes the raw turn and the cited ``RawSpan``s of one condition and returns the cited SEGMENTS, the
GAPS between them and at most ``WINDOW_TOKENS`` folded tokens on each outer side.  The raw text never
leaves this function: the window holds no reference to it and an authority never receives it.  Call
it after ``validate_raw``.

Folding: the same as M1-H0 (``foundation_time_coverage``): ``app.utils.money.normalize_text``
(NFKD, combining marks removed, lowercase) and then the tokens ``[a-z0-9]+``.  A token that
compatibility folding creates («²» -> «2») is a token of the folded neighbour text, never a
character the turn does not contain.

Declared behaviour:
- Cites that overlap or are separated only by whitespace are one SEGMENT (the raw slice from the first
  start to the last end).  Any other text between two segments is a GAP, exposed WHOLE as folded
  tokens: if it is longer than ``2 * WINDOW_TOKENS`` tokens the window is refused
  (``EVIDENCE_GAP_TOO_WIDE``), never elided.  So «publicados o abiertos» (two cites) and a phrase
  cited twice (G1) build a valid window; the text between the cites is in front of the authority.
- A gap is structural evidence, not context: it may withdraw or condition authority, never change
  the value (INV-C0-7b).  ``truncated`` / ``stripped`` keep it.
- A cite that starts or ends INSIDE a word («obras» in «sobras») is refused (``SPAN_INSIDE_WORD``):
  no authority could tell «s obras» from «sobras».
- The outer context does not stop at punctuation or sentence ends: more context only withdraws
  authority.
- Errors are a closed set of codes (``WINDOW_ERROR_CODES``), never raw text.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable

from app.core.foundation_authority_contract import (
    MAX_EVIDENCE_SEGMENTS, MAX_GAP_TOKENS, WINDOW_TOKENS, EvidenceWindow,
)
from app.core.interpretation_contract import ContractError, RawSpan
from app.utils.money import normalize_text

_TOKEN = re.compile(r"[a-z0-9]+")

WINDOW_ERROR_CODES = frozenset({
    "INVALID_RAW_TEXT", "NO_MATERIAL_EVIDENCE", "UNBOUND_RAW_SPAN", "SPAN_INSIDE_WORD",
    "EVIDENCE_GAP_TOO_WIDE", "TOO_MANY_EVIDENCE_SEGMENTS",
})


class EvidenceWindowError(ContractError):
    """The spans cannot be turned into one window; ``reason`` is a closed code, never raw text."""

    def __init__(self, reason: str) -> None:
        if reason not in WINDOW_ERROR_CODES:
            raise ValueError("evidence window error codes are a closed set")
        super().__init__(reason)
        self.reason = reason


def window_tokens(text: str) -> tuple[str, ...]:
    """The folded ``[a-z0-9]+`` tokens of ``text`` (the single tokenizer of the window)."""
    return tuple(_TOKEN.findall(normalize_text(text)))


def _inside_word(raw: str, index: int) -> bool:
    """True if the boundary before ``raw[index]`` has an alphanumeric character on each side."""
    return 0 < index < len(raw) and raw[index - 1].isalnum() and raw[index].isalnum()


def build_window(raw: str, spans: Iterable[RawSpan]) -> EvidenceWindow:
    if type(raw) is not str:
        raise EvidenceWindowError("INVALID_RAW_TEXT")
    cited = list(spans)
    if not cited or any(type(span) is not RawSpan for span in cited):
        raise EvidenceWindowError("NO_MATERIAL_EVIDENCE")
    ordered = sorted({(span.start, span.end): span for span in cited}.values(),
                     key=lambda span: (span.start, span.end))
    if any(not span.binds(raw) for span in ordered):
        raise EvidenceWindowError("UNBOUND_RAW_SPAN")
    if any(_inside_word(raw, span.start) or _inside_word(raw, span.end) for span in ordered):
        raise EvidenceWindowError("SPAN_INSIDE_WORD")
    runs: list[list[int]] = []  # [start, end] of each maximal run of cites that overlap or touch
    for span in ordered:
        if runs and not raw[runs[-1][1]:span.start].strip():  # only spaces between (empty: overlap)
            runs[-1][1] = max(runs[-1][1], span.end)
        else:
            runs.append([span.start, span.end])
    if len(runs) > MAX_EVIDENCE_SEGMENTS:
        raise EvidenceWindowError("TOO_MANY_EVIDENCE_SEGMENTS")
    gaps = tuple(window_tokens(raw[before[1]:after[0]]) for before, after in zip(runs, runs[1:]))
    if any(len(gap) > MAX_GAP_TOKENS for gap in gaps):
        raise EvidenceWindowError("EVIDENCE_GAP_TOO_WIDE")
    return EvidenceWindow(
        spans=tuple(ordered),
        segments=tuple(unicodedata.normalize("NFC", raw[start:end]) for start, end in runs),
        gaps=gaps,
        left=window_tokens(raw[:runs[0][0]])[-WINDOW_TOKENS:],
        right=window_tokens(raw[runs[-1][1]:])[:WINDOW_TOKENS],
    )
