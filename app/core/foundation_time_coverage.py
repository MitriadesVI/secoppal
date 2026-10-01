"""Does the cited year stand alone, or does the raw text qualify it? (M1-H0, R1).

The producer's ``during_year`` cites only the year (prompt rule Y1), so «agosto de
2026», «desde 2023» or «antes de 2024» reach the host as «2026», «2023», «2024» and
would run as the whole calendar year: widening, narrowing or inverting the ask.
The contract has no month, bound or range, so the only honest answer is to ask.

GUARANTEE (limited, on purpose): this is a coverage guard on the frontier of the cited
span, not a temporal parser, and nothing is resolved here.  Starting at the year and
walking outwards through *glue* words only (articles, ordinals, quantities and small
numbers), any word of a CLOSED lexicon (months, partial periods, bounds) or another year
means the year is qualified.  Any other word stops the walk, so «hasta 500 millones en
2025» (an amount bound) and «contratos del 2025» stay a plain year.  Numeric dates that
contain the year («15/03/2025», «2025-03», «2025-1», «2025-I») are read from the raw
characters next to it.  Both directions are bounded to ``WINDOW`` tokens, marker included.
The full temporal authority is the job of the D-TIME mission, not of this guard.

Known limits.  Over-asking (fail-closed): a lexicon word used as a proper name next to the
year («Avenida Primero de Mayo 2025», «Hospital 20 de Julio 2025», «acta de inicio de
2025», «dragado del mar 2025») asks.  Under-asking: a qualifier farther than ``WINDOW`` glue
tokens, separated from the year by a non-glue word («agosto del ICBF en 2025») or absent
from the lexicon is not seen.  The conjunction «en» is glue only after the year, so
«Primero de Mayo en 2025» stays a plain year.
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
    "semestre", "trimestre", "bimestre", "cuatrimestre", "quincena", "semana", "mes", "mitad", "dia",
    "semestres", "trimestres", "bimestres", "cuatrimestres", "quincenas", "semanas", "meses", "mitades", "dias",
    "inicio", "inicios", "comienzo", "comienzos", "principio", "principios", "mediados", "final",
    "finales", "fines", "fin", "cierre",
})
# Bounds and ranges that precede the year («a partir de 2023» walks over «de» to «partir»).
LEADING_BOUNDS = frozenset({
    "desde", "hasta", "entre", "antes", "despues", "tras", "partir", "previo", "previa", "previos",
    "posterior", "posteriores", "anterior", "anteriores", "pre", "post",
})
# What follows the year: «2023 en adelante», «2023 y posteriores», «2023 hasta hoy», «2023 o antes».
TRAILING_BOUNDS = frozenset({
    "adelante", "atras", "posterior", "posteriores", "anterior", "anteriores", "siguientes", "hoy", "fecha",
    "antes", "despues", "ahora", "actualidad", "presente", "aca",
})
# Ordinals and quantities glue a period to the year in both orders: «segundo semestre de 2025» and
# «2025 segundo semestre», «primeros 100 días de 2025», «2025 los dos primeros trimestres».
ORDINALS = frozenset({
    "primer", "primero", "primera", "primeros", "primeras", "segundo", "segunda", "segundos", "segundas",
    "tercer", "tercero", "tercera", "terceros", "terceras", "cuarto", "cuarta", "cuartos", "cuartas",
    "ultimo", "ultima", "ultimos", "ultimas", "un", "una", "dos", "tres", "cuatro", "cinco", "seis", "siete",
    "ocho", "nueve", "diez", "once", "doce",
})
_GLUE = frozenset({"de", "del", "el", "la", "los", "las", "ano", "anos", "y", "e", "o", "u", "a", "al"}) | ORDINALS
_TRAILING_GLUE = _GLUE | {"en", "hacia", "para", "hasta"}

LEADING_LEXICON = MONTHS | PERIODS | LEADING_BOUNDS
TRAILING_LEXICON = MONTHS | PERIODS | TRAILING_BOUNDS

_SMALL_NUMBER = re.compile(r"\d{1,3}")  # «15 de marzo», «primeros 100 días»: glue, never a year
_QUARTER = re.compile(r"[qthsb][1-4]|[1-4][qthsb]")  # Q1, T1, H1, 1T
# Numeric dates that contain the year, read from the raw characters adjacent to it.
_DATE_BEFORE = re.compile(r"\d{1,4}\s?[/\-.]\s?$")
_DATE_AFTER = re.compile(r"^\s?[/\-.]\s?(?:\d{1,4}|(?:i{1,3}|iv)\b)", re.IGNORECASE)


def _walk(tokens: list[str], lexicon: frozenset[str], glue: frozenset[str]) -> bool:
    """True when, within WINDOW tokens of glue, a lexicon word or another year appears."""
    for token in tokens[:WINDOW]:
        if token in lexicon or _YEAR.fullmatch(token) or _QUARTER.fullmatch(token):
            return True
        if token not in glue and not _SMALL_NUMBER.fullmatch(token):
            return False
    return False


def qualifies_year(raw: str, start: int, end: int) -> bool:
    """Whether the year at raw[start:end] is qualified by the text around it."""
    before = _TOKEN.findall(normalize_text(raw[:start]))[::-1]
    after = _TOKEN.findall(normalize_text(raw[end:]))
    return (_DATE_BEFORE.search(raw[:start]) is not None
            or _DATE_AFTER.search(raw[end:]) is not None
            or _walk(before, LEADING_LEXICON, _GLUE)
            or _walk(after, TRAILING_LEXICON, _TRAILING_GLUE))


def uncited_time_qualifier(raw: str, spans: Iterable) -> bool:
    """True when any year inside the cited spans is qualified by text the claim does not carry."""
    for span in spans:
        for match in _YEAR.finditer(raw[span.start:span.end]):
            if qualifies_year(raw, span.start + match.start(), span.start + match.end()):
                return True
    return False
