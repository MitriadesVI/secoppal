"""An exact buyer wins over the period; a period is trimmed off a capture one mention at a time.

One responsibility: the buyer the actor grammar captured next to a period. «con la alcaldía de X los últimos
2 años» captures «la alcaldía de X los últimos 2 años»; the period is not part of the buyer, but a buyer's
own name can carry temporal words («Hospital 7 de Agosto», «Colegio 20 de Julio»). Precedence:

1. The capture is resolved as written, exactly as before wave 1. If it resolves, it stays, temporal words
   included: the exact buyer wins over the period.
2. Only when it does not resolve are the temporal mentions the authority recognised trimmed off it, ONE BY
   ONE (each mention on its own, borders first, never the stretch that joins two of them), and the
   capture is resolved again after each cut.
3. A period that OPENS the sentence («en 2025 con quien ha contratado …») makes the grammar read a territory
   switch and capture no buyer at all. Only then the text is read again with that leading period masked, and
   the new capture is adopted if it resolves exactly and no declared group claims it (a group keeps the
   path it had: «en 2025 con quien ha contratado el ICBF» is the group, not its headquarters).

The mentions are the temporal authority's (``TemporalResolution.mentions``); this module only searches
them in the capture, it never reads a period itself. Words of an accredited buyer never become the topic:
the span masked from the residual text is the buyer's own (the capture minus the mentions already masked
there), found again even if the separators differ (``aligned_pattern``).
"""
from __future__ import annotations

import re
from dataclasses import replace
from typing import Callable, Iterator, TypeVar

_Instruction = TypeVar("_Instruction")

# Glue words a cut can leave dangling at the new edge of a capture («… atlántico durante» once the window
# is cut). Finite and declared; an adopted form must still resolve exactly, so a wrong guess cannot ship.
_GLUE = frozenset({"en", "durante", "desde", "hasta", "de", "del", "entre", "a", "al", "para", "por", "y"})
_PUNCTUATION = " ,.;:¿?¡!"


def aligned_pattern(span: str) -> str:
    """A regex that finds ``span`` again in a residual text whose separators differ from the grammar's.

    The actor grammar reads a normalised copy of the sentence (punctuation folded); the residual text
    keeps it («… sur occidente ese. en» vs «… sur occidente ese en»). An exact search then misses the
    buyer's own span and its words survive as topic. Tokens must appear in order, separated by anything
    that is not a word character.
    """
    tokens = re.findall(r"\w+", span)
    return r"\b" + r"[\W_]+".join(re.escape(token) for token in tokens) + r"\b"


_PUNCTUATION_BEFORE = re.compile(r"[,;:]\s*$")
_INTRODUCER = re.compile(r"(?<!\w)(?:durante|desde|hasta|entre|en)\s+$", re.IGNORECASE)


def consumed(text: str, spans: tuple[tuple[int, int], ...]) -> tuple[tuple[int, int], ...]:
    """The spans to mask for a period: the mentions, each with the introducer written right before it.

    Only for a window next to another period. The envelope span used to swallow «durante» with them; masking
    each mention alone would leave it behind as topic. A single period keeps its own span (a leftover
    «durante» there is the base's, not touched here).
    """
    if len(spans) < 2:
        return spans
    out: list[tuple[int, int]] = []
    for start, end in sorted(spans):
        hit = _INTRODUCER.search(text[:start]) or _PUNCTUATION_BEFORE.search(text[:start])
        start = hit.start() if hit else start
        # Punctuation alone between two mentions («de 2025, los últimos 3 meses») is not a buyer or a topic.
        if out and not re.search(r"\w", text[out[-1][1]:start]):
            start = out.pop()[0]
        out.append((start, end))
    return tuple(out)


def mention_texts(text: str, spans: tuple[tuple[int, int], ...]) -> tuple[str, ...]:
    """The text of each temporal mention (``spans`` are the authority's), longest first."""
    texts = {text[start:end].strip() for start, end in spans}
    return tuple(sorted((item for item in texts if re.search(r"\w", item)), key=len, reverse=True))


def without_mentions(span: str, mentions: tuple[str, ...]) -> str:
    """``span`` minus every temporal mention in it (already masked in the residual text)."""
    for mention in mentions:
        span = re.sub(aligned_pattern(mention), " ", span, flags=re.IGNORECASE)
    return " ".join(span.split())


def _has_word(text: str) -> bool:
    return re.search(r"\w", text) is not None


def _next_cut(current: str, mentions: list[str]) -> tuple[str, int, int] | None:
    """The mention to cut next: right border, then left border, then any other place."""
    hits = [
        (mention, hit.start(), hit.end())
        for mention in mentions
        for hit in re.finditer(aligned_pattern(mention), current, re.IGNORECASE)
    ]
    for edge in (
        lambda item: not _has_word(current[item[2]:]),
        lambda item: not _has_word(current[:item[1]]),
        lambda item: True,
    ):
        found = [item for item in hits if edge(item)]
        if found:
            return min(found, key=lambda item: (-(item[2] - item[1]), item[1]))
    return None


def _tidy(text: str) -> str:
    return " ".join(text.split()).strip(_PUNCTUATION)


def _strip_glue(text: str) -> str:
    words = text.split()
    while words and words[-1] in _GLUE:
        words.pop()
    while words and words[0] in _GLUE:
        words.pop(0)
    return " ".join(words)


def trimmed_forms(capture: str, mentions: tuple[str, ...]) -> Iterator[str]:
    """Successive forms of ``capture`` with one more mention cut each time (plain cut, then without glue)."""
    current = capture
    remaining = list(mentions)
    while remaining:
        cut = _next_cut(current, remaining)
        if cut is None:
            return
        mention, start, end = cut
        remaining.remove(mention)
        plain = _tidy(current[:start] + " " + current[end:])
        bare = _strip_glue(plain)
        for form in dict.fromkeys((plain, bare)):
            if form:
                yield form
        current = bare


# What may stand before a period that still opens the sentence: its introducer and article («en el último
# año», «los últimos 30 días», «durante 2025»).
_OPENERS = _GLUE | {"el", "la", "los", "las", "este", "esta", "estos", "estas"}


def without_leading_period(text: str, spans: tuple[tuple[int, int], ...]) -> str:
    """``text`` with the periods that open the sentence (and their introducers) masked, same length; else unchanged."""
    opened = 0
    for start, end in sorted(spans):
        if start < opened or not all(word in _OPENERS for word in text[opened:start].lower().split()):
            break
        opened = end
    return " " * opened + text[opened:] if opened else text


def exact_buyer_first(
    instruction: _Instruction,
    mentions: tuple[str, ...],
    resolves: Callable[[str], bool],
) -> _Instruction:
    """``instruction`` with the buyer capture that resolves: as written first, then trimmed mention by mention.

    ``resolves`` is the router's own relation-buyer resolution (exact evidence only). When the capture
    holds no temporal mention, or resolves as written, the instruction is returned untouched.
    """
    capture = getattr(instruction, "buyer_mention", None)
    if not capture or not any(re.search(aligned_pattern(item), capture, re.IGNORECASE) for item in mentions):
        return instruction
    if resolves(capture):
        return instruction
    for form in trimmed_forms(capture, mentions):
        if resolves(form):
            return replace(instruction, buyer_mention=form)  # type: ignore[type-var]
    return instruction


def read_buyer(
    text: str,
    spans: tuple[tuple[int, int], ...],
    mentions: tuple[str, ...],
    interpret: Callable[[str], _Instruction],
    resolves: Callable[[str], bool],
    claimed_by_group: Callable[[str], bool],
) -> _Instruction:
    """The actor instruction of ``text`` with the buyer capture that resolves (see the module docstring)."""
    instruction = exact_buyer_first(interpret(text), mentions, resolves)
    if getattr(instruction, "buyer_mention", None):
        return instruction
    unopened = without_leading_period(text, spans)
    if unopened == text:
        return instruction
    reread = exact_buyer_first(interpret(unopened), mentions, resolves)
    capture = getattr(reread, "buyer_mention", None)
    if capture and resolves(capture) and not claimed_by_group(capture):
        return reread
    return instruction
