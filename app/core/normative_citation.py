"""A citation of a norm is not a period (one responsibility).

«decreto 1082 de 2015», «Ley 80 de 1993», «Resolución 1234 de 2025» name a document; the
year is part of its name. The relative-window rule of ``temporal_window_anchor`` asks when a
window shares the turn with ANOTHER period, so «contratos por decreto 1082 de 2015 en los
últimos 6 meses» must not read the 2015 as that other period.

Closed shape: ``<kind> [adjective] [n.º] <number> de|del <year>``, with ``kind`` a norm type
of the Colombian procurement domain. Pure, no I/O.
"""
from __future__ import annotations

import re

KINDS = (
    "ley", "decreto", "decreto ley", "decreto-ley", "resolucion", "resolución", "acuerdo",
    "ordenanza", "circular", "circular externa", "circular interna", "circular conjunta",
    "circular unica", "circular única",
)
_KIND = "|".join(sorted((re.escape(k).replace(r"\ ", r"\s+") for k in KINDS), key=len, reverse=True))
CITATION = re.compile(
    rf"(?<![a-zA-Z0-9])(?:{_KIND})\s+(?:n(?:o|º|°|\.º|\.°|um(?:ero)?)?\.?\s*)?"
    r"\d[\d.]*(?:\s*[-/]\s*\d+)?\s+(?:de|del)\s+(?:19|20)\d{2}(?![\d-])",
    re.IGNORECASE,
)


def spans(text: str) -> tuple[tuple[int, int], ...]:
    """The spans of the normative citations of ``text`` (their years are not periods)."""
    return tuple(hit.span() for hit in CITATION.finditer(text))
