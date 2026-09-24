"""Which of the owner's radars a free-text turn names, by literal occurrence.

The owner's store is the only authority for a radar's identity: a radar is named
when its whole stored name occurs in the turn, compared case- and accent-blind
and on word boundaries. No alias, no fuzzy match, no partial name.
"""
from __future__ import annotations

import re
from typing import Iterable

from app.core.radar.store import Radar
from app.utils.money import normalize_text

_TOKEN = re.compile(r"\w+")


def _pattern(name: str) -> re.Pattern[str] | None:
    tokens = _TOKEN.findall(normalize_text(name))
    if not tokens:
        return None
    return re.compile(rf"(?<!\w){r'\W+'.join(map(re.escape, tokens))}(?!\w)")


def radars_named_in(text: str, radars: Iterable[Radar]) -> tuple[Radar, ...]:
    """Every radar whose full name occurs in ``text``; the caller decides on ties."""
    folded = normalize_text(text)
    named = []
    for radar in radars:
        pattern = _pattern(radar.name)
        if pattern is not None and pattern.search(folded):
            named.append(radar)
    return tuple(named)
