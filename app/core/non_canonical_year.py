"""Years the temporal authority does not read, written the way people write them (one responsibility).

«los últimos 3 meses de dos mil veinticinco», «… de 2.025», «… de 25» name a YEAR; the
authority only reads «2025». Resolving them would be a new capability, so they are only
RECOGNISED here, to let ``temporal_noun_anchor`` ask instead of executing a window counted
from today that does not contain the year the person wrote.

Closed forms, nothing else:

* the year 2000-2099 in words: «dos mil», «dos mil veinticinco», «dos mil veinte y cinco»,
  «dos mil treinta y uno» (accent-tolerant);
* the year with a thousands separator: «2.025», «2,025» (also a non-breaking space);
* the year in two digits after «de|del»: «de 25», only right after the window AND only at the end of
  the clause (end of text or punctuation): «… de 25 municipios» and a bare «de 25» elsewhere are
  quantities.

A figure followed by a unit or by another number word is a quantity, never a year
(«2.025 millones», «dos mil quinientos», «de 25 millones», «de 25 años»).

Pure: no I/O, no clock.
"""
from __future__ import annotations

import re

_ACCENTS = {"a": "[aá]", "e": "[eé]", "i": "[ií]", "o": "[oó]", "u": "[uúü]"}


def _word(folded: str) -> str:
    return "".join(_ACCENTS.get(char, char) for char in folded)


def _any(words: tuple[str, ...]) -> str:
    return "|".join(_word(word) for word in sorted(words, key=len, reverse=True))


UNITS = ("un", "uno", "una", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve")
TEENS = ("diez", "once", "doce", "trece", "catorce", "quince", "dieciseis", "diecisiete",
         "dieciocho", "diecinueve")
TWENTIES = ("veintiun", "veintiuno", "veintiuna", "veintidos", "veintitres", "veinticuatro",
            "veinticinco", "veintiseis", "veintisiete", "veintiocho", "veintinueve")
TENS = ("treinta", "cuarenta", "cincuenta", "sesenta", "setenta", "ochenta", "noventa")
HUNDREDS = ("cien", "ciento", "doscientos", "doscientas", "trescientos", "trescientas",
            "cuatrocientos", "cuatrocientas", "quinientos", "quinientas", "seiscientos",
            "seiscientas", "setecientos", "setecientas", "ochocientos", "ochocientas",
            "novecientos", "novecientas")
# What follows a figure when it is a quantity, not a year.
_QUANTITY_AFTER = (
    r"(?!\s*(?:\d|mil\b|mill|mm\b|miles\b|peso|cop\b|smm|smlm|salario|uvt|usd|dolar|d[oó]lar|"
    r"euro|%|por\s+ciento|a[nñ]os?\b|mes(?:es)?\b|d[ií]as?\b|semanas?\b|"
    rf"(?:{_any(HUNDREDS)})\b))"
)

_BELOW_TWENTY = _any(UNITS + TEENS)
_TENS_AND_UNITS = rf"(?:{_any(TENS)})(?:\s+y\s+(?:{_any(UNITS)}))?"
_VEINTE = rf"veinte(?:\s+y\s+(?:{_any(UNITS)}))?"
_REST = rf"(?:{_any(TWENTIES)}|{_VEINTE}|{_TENS_AND_UNITS}|{_BELOW_TWENTY})"
_WORDS = rf"{_word('dos')}\s+mil(?:\s+(?:y\s+)?{_REST})?(?![a-záéíóúñ])"
_SEPARATED = r"(?<![\d.,])2[.,  ]0\d{2}(?![\d]|[.,]\d)"
# Two digits are a year only where the context is unmistakably temporal: nothing but the end of the
# text or punctuation follows them («… últimos 3 meses de 25», «… de 25.»), never «de 25 municipios».
_END_OF_CLAUSE = r"(?=\s*(?:$|[.,;:!?)\]]))"
_TWO_DIGITS = rf"(?<![\d.,])\d{{2}}(?![\d]|[.,]\d){_END_OF_CLAUSE}"

WORDS = re.compile(rf"{_WORDS}{_QUANTITY_AFTER}", re.IGNORECASE)
SEPARATED = re.compile(rf"{_SEPARATED}{_QUANTITY_AFTER}", re.IGNORECASE)
TWO_DIGITS = re.compile(rf"{_TWO_DIGITS}{_QUANTITY_AFTER}", re.IGNORECASE)

# A year is introduced by «de|del» (and, optionally, «el año»): the caller anchors the match.
_INTRO = re.compile(r"(?:de|del)\s+(?:(?:el\s+)?a[nñ]o\s+)?", re.IGNORECASE)
_YEAR_FORMS = (WORDS, SEPARATED)
# «más de 2.000», «menos de dos mil», «mayor de 25»: the figure measures something, it is not a year.
_COMPARED = re.compile(
    r"(?:m[aá]s|menos|cerca|alrededor|aproximadamente|casi|mayor(?:es)?|menor(?:es)?|"
    r"superior(?:es)?|inferior(?:es)?|igual(?:es)?|unos|unas|exactamente)\s+$",
    re.IGNORECASE,
)


def _compared(text: str, intro: re.Match[str]) -> bool:
    return _COMPARED.search(text[:intro.start()]) is not None


def find(text: str) -> tuple[int, int] | None:
    """The first non-canonical year of ``text`` introduced by «de|del», as ``(start, end)``."""
    best = None
    for intro in _INTRO.finditer(text):
        if _compared(text, intro):
            continue
        for form in _YEAR_FORMS:
            hit = form.match(text, intro.end())
            if hit and (best is None or hit.start() < best[0]):
                best = hit.span()
    return best


def find_adjacent(text: str, position: int) -> tuple[int, int] | None:
    """A year in any closed form right after ``position`` («<window> de 25»), if any."""
    gap = re.compile(r"\s*,?\s*", re.IGNORECASE).match(text, position)
    intro = _INTRO.match(text, gap.end() if gap else position)
    if not intro or _compared(text, intro):
        return None
    for form in _YEAR_FORMS + (TWO_DIGITS,):
        hit = form.match(text, intro.end())
        if hit:
            return hit.span()
    return None
