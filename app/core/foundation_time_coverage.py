"""Does the cited year stand alone, or does the raw text qualify it? (M1-H0).

The producer's ``during_year`` cites only the year (prompt rule Y1), so «agosto de
2026», «desde 2023» or «antes de 2024» reach the host as «2026», «2023», «2024» and
would run as the whole calendar year: widening, narrowing or inverting the ask.
The contract has no month, bound or range, so the only honest answer is to ask.

This is a coverage check on the frontier of the cited span, not a temporal parser:
nothing is resolved here.  Starting at the year and walking outwards through
*glue* words only, any word of a CLOSED lexicon (months, bounds, partial periods)
or another year means the year is qualified.  Any other word stops the walk, so
«hasta 500 millones en 2025» (an amount bound) and «contratos del 2025» stay a plain
year.  Both directions are bounded to ``WINDOW`` tokens, marker included.

Known limits (fail-closed, over-asking): a month word used as a proper name
(«Avenida Primero de Mayo en 2025») asks; a qualifier farther than ``WINDOW`` glue
tokens, or one that is not in the lexicon, is not seen.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from app.utils.money import normalize_text

WINDOW = 4  # «a partir de el año» is the longest closed qualifier chain: 4 tokens

_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")
_TOKEN = re.compile(r"[a-z0-9]+")

MONTHS = frozenset({
    "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "setiembre",
    "octubre", "noviembre", "diciembre",
    "ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "sept", "oct", "nov", "dic",
})
PERIODS = frozenset({
    "semestre", "trimestre", "bimestre", "cuatrimestre", "quincena", "semana", "mes", "mitad",
    "inicio", "inicios", "comienzo", "comienzos", "principio", "principios", "mediados", "final",
    "finales", "fines",
})
# Bounds and ranges that precede the year («a partir de 2023» walks over «de» to «partir»).
LEADING_BOUNDS = frozenset({
    "desde", "hasta", "entre", "antes", "despues", "tras", "partir", "previo", "previa", "previos",
    "posterior", "posteriores", "anterior", "anteriores",
})
# What follows the year: «2023 en adelante», «2023 y posteriores», «2023 hasta hoy».
TRAILING_BOUNDS = frozenset({
    "adelante", "atras", "posterior", "posteriores", "anterior", "anteriores", "siguientes", "hoy", "fecha",
})
_GLUE = frozenset({"de", "del", "el", "la", "los", "las", "ano", "anos", "y", "e", "o", "u", "a", "al"})
_TRAILING_GLUE = _GLUE | {"en", "hacia", "para", "hasta"}

LEADING_LEXICON = MONTHS | PERIODS | LEADING_BOUNDS
TRAILING_LEXICON = MONTHS | PERIODS | TRAILING_BOUNDS


def _walk(tokens: list[str], lexicon: frozenset[str], glue: frozenset[str]) -> bool:
    """True when, within WINDOW tokens of glue, a lexicon word or another year appears."""
    for token in tokens[:WINDOW]:
        if token in lexicon or _YEAR.fullmatch(token):
            return True
        if token not in glue:
            return False
    return False


def qualifies_year(raw: str, start: int, end: int) -> bool:
    """Whether the year at raw[start:end] is qualified by the text around it."""
    before = _TOKEN.findall(normalize_text(raw[:start]))[::-1]
    after = _TOKEN.findall(normalize_text(raw[end:]))
    return (_walk(before, LEADING_LEXICON, _GLUE)
            or _walk(after, TRAILING_LEXICON, _TRAILING_GLUE))


def uncited_time_qualifier(raw: str, spans: Iterable) -> bool:
    """True when any year inside the cited spans is qualified by text the claim does not carry."""
    for span in spans:
        for match in _YEAR.finditer(raw[span.start:span.end]):
            if qualifies_year(raw, span.start + match.start(), span.start + match.end()):
                return True
    return False
