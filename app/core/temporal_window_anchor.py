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
    )


def clarification(found: AnchoredWindow) -> str:
    """Spanish question naming both readings; it never picks one."""
    window = " ".join(found.window.split())
    anchor = " ".join(found.anchor.split())
    return (
        f"Mencionas «{window}» y también «{anchor}». ¿Cuál quieres? "
        f"(1) «{window}», contados hacia atrás desde hoy; "
        f"(2) el periodo «{anchor}» completo. "
        "Si querías la ventana dentro de ese periodo, dime las fechas exactas "
        "(por ejemplo, del 2025-10-01 al 2025-12-31)."
    )
