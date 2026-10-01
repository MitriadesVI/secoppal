"""A relative window next to another period is never resolved: it is asked (one responsibility).

«los últimos 3 meses de 2025» names two periods: a window counted back from
today, and a year. ``temporal_semantics`` resolves ONE period per text; picking
either silently executes something the person did not ask for (the window alone
is disjoint from 2025; the year alone drops the window). Resolving the window
INSIDE the anchor («octubre a diciembre de 2025») would be a new capability, so
the authority fails closed: it reports ``relative_window_explicit_anchor`` and
a clarification that names both readings.

The rule is deliberately about the whole turn, not about adjacency: any
adjacency test («de», «del», a comma…) is a finite vocabulary that «contratos de
2025 de aseo en los últimos 3 meses» walks around. «Another period» is whatever
the authority itself recognises once the window is blanked out, so the lexicon
is never copied here (it cannot drift from ``temporal_semantics``).

Pure: no I/O, no clock, no import of ``temporal_semantics`` (the authority hands
in its own patterns and its own resolver).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Iterable

ERROR_CODE = "relative_window_explicit_anchor"


@dataclass(frozen=True)
class AnchoredWindow:
    """The window, the other period, and the span from the first to the last of them."""

    window: str
    anchor: str
    span: tuple[int, int]
    # Each period the authority recognised, on its own: the windows and the other period. ``span``
    # ENVELOPES them and everything written between them (a buyer, a topic), so a consumer that
    # masks the envelope erases what is not a period.
    mentions: tuple[tuple[int, int], ...] = ()


def _blank(text: str, spans: Iterable[tuple[int, int]]) -> str:
    chars = list(text)
    for start, end in spans:
        chars[start:end] = " " * (end - start)
    return "".join(chars)


def find_anchored_window(
    text: str,
    window_patterns: Iterable[re.Pattern[str]],
    ignored_patterns: Iterable[re.Pattern[str]],
    resolve_rest: Callable[[str], tuple[bool, tuple[int, int] | None]],
) -> AnchoredWindow | None:
    """The first relative window of ``text`` together with another period, if any.

    ``window_patterns`` are the relative windows (counted back from today).
    ``ignored_patterns`` are words that look temporal but are not a period here
    («hoy» is currentness, never an anchor). ``resolve_rest`` is the authority's
    own resolver applied to a text: ``(matched, span)``. Windows and ignored
    words are blanked first, so the rest can only match a different period.
    """
    windows = [hit for pattern in window_patterns for hit in pattern.finditer(text)]
    if not windows:
        return None
    ignored = [hit.span() for pattern in ignored_patterns for hit in pattern.finditer(text)]
    rest = _blank(text, [hit.span() for hit in windows] + ignored)
    matched, span = resolve_rest(rest)
    if not matched or span is None:
        return None
    first = min(windows, key=lambda hit: hit.start())
    return AnchoredWindow(
        window=first.group(0),
        anchor=text[span[0]:span[1]].strip(),
        span=(min(first.start(), span[0]), max(first.end(), span[1])),
        mentions=tuple(sorted({hit.span() for hit in windows} | {span})),
    )


# Periods the person can write back. The question offers ONLY phrases the temporal authority
# already resolves, never numbered options or a «completo»: any other word of the reply
# («2», «la opción 2», «2025 completo») would be read as the topic of a search.
EXAMPLES = ("octubre a diciembre de 2025", "desde julio de 2026")


def rewrite_hint() -> str:
    """Ask for the period written out; the examples are phrases the authority resolves."""
    first, second = EXAMPLES
    return (
        f"Escríbeme el periodo con fechas explícitas, por ejemplo «{first}» o «{second}»."
    )


def clarification(found: AnchoredWindow) -> str:
    """Spanish question naming both periods; it never picks one and offers no numbered options."""
    window = " ".join(found.window.split())
    anchor = " ".join(found.anchor.split())
    return (
        f"Mencionas «{window}» y también «{anchor}». No sé si la ventana se cuenta hacia atrás "
        f"desde hoy o dentro de ese periodo, y no voy a elegir por ti. {rewrite_hint()}"
    )
