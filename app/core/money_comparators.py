"""Un solo léxico de comparadores de dinero (M1-D, VALUE).

Antes había dos: ``money_polarity._UP/_DOWN`` conocía «mínimo», «máximo» y
«rebasan»; ``foundation_value_direction._MARKERS`` no, y ninguno distinguía el
límite inclusivo del exclusivo con la misma tabla. Aquí vive la ÚNICA tabla de
frases; los dos consumidores derivan de ella:

* ``money_polarity`` toma ``UP_PATTERN`` / ``DOWN_PATTERN`` (regex tolerantes a
  tildes, sobre texto crudo) y los sufijos «o más», «en adelante»…
* ``foundation_value_direction`` toma ``read_comparators`` (texto plegado) y
  recibe familia, negación y posición de cada comparador.

Familias: ``GT`` ``GTE`` ``LT`` ``LTE`` (más ``EQ`` y ``BETWEEN``, que ningún
operador VALUE admite). La negación invierte el LÍMITE y la inclusividad:
``no (> x)`` = ``<= x``; ``no (>= x)`` = ``< x``. Una frase léxicamente
inclusiva («al menos», «hasta», «mayor o igual») NO se niega: «no al menos»
se lee como ambiguo (``family=None``), no se adivina.

Módulo puro: sin I/O, sin estado, sin conversión de montos.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from app.utils.money import normalize_text

GT, GTE, LT, LTE, EQ, BETWEEN = "GT", "GTE", "LT", "LTE", "EQ", "BETWEEN"

OPERATOR_FAMILY = {
    "greater_than": GT,
    "greater_or_equal": GTE,
    "less_than": LT,
    "less_or_equal": LTE,
    # Alias vistos en sondas externas; el contrato M1.0 usa los de arriba.
    "greater_than_or_equal": GTE,
    "less_than_or_equal": LTE,
}
_NEGATED = {GT: LTE, GTE: LT, LT: GTE, LTE: GT}
_BOUND = {GT: "min", GTE: "min", LT: "max", LTE: "max"}
_INCLUSIVE = {GT: False, GTE: True, LT: False, LTE: True}


def bound_of(family: str) -> str | None:
    """«min» para GT/GTE, «max» para LT/LTE; None para EQ/BETWEEN."""
    return _BOUND.get(family)


def is_inclusive(family: str) -> bool | None:
    return _INCLUSIVE.get(family)


def negate(family: str) -> str | None:
    return _NEGATED.get(family)


@dataclass(frozen=True)
class _Head:
    phrase: str          # plegado (sin tildes, minúsculas)
    family: str
    needs_link: bool = False   # exige «de/a/que» detrás («más de», «mayor a»)
    o_igual: bool = False      # admite «… o igual (a)» → inclusivo
    negatable: bool = True     # una frase léxicamente inclusiva no se niega


def _heads() -> tuple[_Head, ...]:
    rows: list[_Head] = []

    def add(family: str, phrases: str, **kw) -> None:
        rows.extend(_Head(p, family, **kw) for p in phrases.split("|"))

    # Exclusivos: necesitan enlace («más DE», «mayor A»); admiten «o igual».
    add(GT, "mayor|mayores|superior|superiores", needs_link=True, o_igual=True)
    add(GT, "mas", needs_link=True)
    add(GT, "por encima|encima", needs_link=True)
    add(GT, "pase de|pasen de|pasa de|pasan de|pasar de")
    add(GT, "supera|superan|supere|superen|superar|excede|exceden|exceda|excedan|exceder|"
            "rebasa|rebasan|rebase|rebasen|rebasar|sobrepasa|sobrepasan|sobrepase|sobrepasen|sobrepasar")
    add(LT, "menor|menores|inferior|inferiores", needs_link=True, o_igual=True)
    add(LT, "menos", needs_link=True)
    add(LT, "por debajo|debajo", needs_link=True)
    add(LT, "baje|bajen|baja|bajan|bajar", needs_link=True)
    # Inclusivos léxicos.
    add(GTE, "minimo|minima|al menos|por lo menos|desde|a partir de|igual o mayor|igual o superior",
        negatable=False)
    add(LTE, "maximo|maxima|hasta|a lo sumo|tope|igual o menor|igual o inferior|"
             "sin superar|sin exceder|sin rebasar|sin sobrepasar|sin pasar de", negatable=False)
    # Igualdad: ningún operador VALUE la admite; se lee para rechazarla.
    add(EQ, "exactamente|igual a", negatable=False)
    return tuple(rows)


HEADS = _heads()
_SYMBOLS = {">=": GTE, "<=": LTE, "≥": GTE, "≤": LTE, ">": GT, "<": LT}

# Sufijos tras la cifra: «300 millones o más». Inclusivos.
_SUFFIXES = (
    *((p, GTE) for p in ("o mas", "y mas", "o mayores", "y mayores", "o superiores", "y superiores",
                         "en adelante", "para arriba", "para mas")),
    *((p, LTE) for p in ("o menos", "y menos", "o menores", "y menores", "o inferiores", "y inferiores",
                         "para abajo", "para menos")),
)

_ACCENTED = {"a": "[aá]", "e": "[eé]", "i": "[ií]", "o": "[oó]", "u": "[uúü]"}


def _accent_tolerant(phrase: str) -> str:
    out = []
    for char in phrase:
        if char == " ":
            out.append(r"\s+")
        else:
            out.append(_ACCENTED.get(char, re.escape(char)))
    return "".join(out)


def _alternation(phrases) -> str:
    return "|".join(_accent_tolerant(p) for p in sorted(set(phrases), key=lambda p: (-len(p), p)))


def _literal(phrase: str) -> str:
    return r"\s+".join(re.escape(word) for word in phrase.split(" "))


def _head_pattern(families: set[str]) -> str:
    """Cabezas de frase (sin enlace) de las familias dadas, para ``money_polarity``."""
    words = _alternation(h.phrase for h in HEADS if h.family in families)
    symbols = sorted((re.escape(s) for s, f in _SYMBOLS.items() if f in families), key=lambda s: -len(s))
    return f"(?:{words}|{'|'.join(symbols)})"


UP_PATTERN = _head_pattern({GT, GTE})
DOWN_PATTERN = _head_pattern({LT, LTE})
SUFFIX_UP_PATTERN = _alternation(p for p, f in _SUFFIXES if f == GTE)
SUFFIX_DOWN_PATTERN = _alternation(p for p, f in _SUFFIXES if f == LTE)


# ── lectura sobre texto plegado ──────────────────────────────────────────

# Words that may sit between «no» and the comparator: «no SEA mayor», «no DEBEN superar».
NEGATION_FILLER = r"(?:sea|sean|es|son|este|esten|deba|deban|debe|deben|pueda|puedan|puede|pueden|vaya|vayan)\s+(?:a\s+)?"
_WORD_HEAD = "|".join(_literal(h.phrase) for h in sorted(HEADS, key=lambda h: (-len(h.phrase), h.phrase)))
_SYMBOL_HEAD = "|".join(re.escape(s) for s in sorted(_SYMBOLS, key=lambda s: -len(s)))
_READING_RE = re.compile(
    rf"(?P<sym>{_SYMBOL_HEAD})"
    rf"|(?<![a-z0-9])(?P<neg>(?:que\s+)?no\s+(?:{NEGATION_FILLER})?)?(?P<head>{_WORD_HEAD})"
    rf"(?P<eq>\s+o\s+igual(?:es)?)?(?P<link>\s+(?:de|a|que)(?:\s+(?:los|las|el|la))?)?(?![a-z0-9])"
)
_SUFFIX_RE = re.compile(
    rf"(?<![a-z0-9])(?:{'|'.join(_literal(p) for p, _ in _SUFFIXES)})(?![a-z0-9])"
)
_SUFFIX_FAMILY = dict(_SUFFIXES)
_BETWEEN_RE = re.compile(r"\bentre\b.+\by\b")
_NEGATION_TOKEN = re.compile(r"(?<![a-z0-9])(?:no|sin|ni|tampoco|nunca|jamas)(?![a-z0-9])")
_BY_PHRASE = {h.phrase: h for h in HEADS}


@dataclass(frozen=True)
class ComparatorReading:
    family: str | None   # None = negación de una frase inclusiva: no se adivina
    phrase: str
    negated: bool
    start: int
    end: int


@dataclass(frozen=True)
class ComparatorScan:
    folded: str
    readings: tuple[ComparatorReading, ...]
    between: bool
    unread_negation: bool

    @property
    def families(self) -> tuple[str | None, ...]:
        """Familias distintas, en orden de aparición."""
        return tuple(dict.fromkeys(r.family for r in self.readings))

    def amount_text(self) -> str:
        """El texto sin comparadores: lo único que debe ver un conversor."""
        chars = list(self.folded)
        for reading in self.readings:
            chars[reading.start:reading.end] = " " * (reading.end - reading.start)
        return re.sub(r"\s+", " ", "".join(chars)).strip()


def read_comparators(text: str) -> ComparatorScan:
    """Cada comparador que nombra ``text``, con su negación ya aplicada."""
    folded = f" {normalize_text(text)} "
    readings: list[ComparatorReading] = []
    for match in _READING_RE.finditer(folded):
        if match.group("sym"):
            readings.append(ComparatorReading(_SYMBOLS[match.group("sym")], match.group("sym"), False,
                                              match.start(), match.end()))
            continue
        head = _BY_PHRASE[re.sub(r"\s+", " ", match.group("head"))]
        if head.needs_link and not match.group("link") and not match.group("eq"):
            continue  # «más» o «mayor» sin enlace no es un comparador de monto
        family = head.family
        if match.group("eq"):
            if not head.o_igual:
                continue
            family = GTE if family == GT else LTE
        negated = bool(match.group("neg"))
        if negated:
            family = negate(family) if (head.negatable and not match.group("eq")) else None
        readings.append(ComparatorReading(family, match.group().strip(), negated, match.start(), match.end()))
    masked = list(folded)
    for reading in readings:
        masked[reading.start:reading.end] = " " * (reading.end - reading.start)
    leftover = "".join(masked)
    for match in _SUFFIX_RE.finditer(leftover):
        readings.append(ComparatorReading(_SUFFIX_FAMILY[re.sub(r"\s+", " ", match.group())],
                                          match.group(), False, match.start(), match.end()))
        masked[match.start():match.end()] = " " * (match.end() - match.start())
    unread = bool(_NEGATION_TOKEN.search("".join(masked)))
    readings.sort(key=lambda r: r.start)
    return ComparatorScan(folded, tuple(readings), bool(_BETWEEN_RE.search(folded)), unread)
