"""A temporal noun or a non-canonical year next to a relative window is an anchor too (one responsibility).

``temporal_semantics`` only counts as «another period» what its own lexicon resolves. «los últimos
3 meses de la vigencia anterior», «… del gobierno anterior», «… de dos mil veinticinco», «… de
2.025», «… de 25» name a period it cannot place, so the window used to be counted from today and
executed silently, disjoint from what was asked (the base found no period at all: the history
contained the request). The authority fails closed instead: same error and same question as an
explicit anchor (``temporal_window_anchor``).

Finite and declared here, not inferred:

* ``STRONG`` nouns are always a period (vigencia, periodo, año, cuatrienio, semestre, trimestre,
  bimestre, cuatrimestre, quincena, pandemia…): an article/possessive/demonstrative before them, or
  a temporal adjective after them, makes the phrase an anchor («de la vigencia», «del año»,
  «de vigencia anterior»).
* ``ENTITY_OFFICE`` nouns (gobierno, administración, gestión) are ALSO part of buyer names in the repo's
  catalogs («Gobierno Departamental del Tolima», «Administración Municipal de Galapa»): they anchor ONLY
  with an explicit temporal qualifier (anterior, pasado/a, saliente, entrante, actual, previo/a), never
  bare and never with a possessive. A test reads the buyer catalogs and fails if a noun of the lexicon
  appears in a buyer name and is not in this class.
* ``OFFICE`` nouns that are not in buyer names (ejercicio, mandato) anchor with a temporal adjective
  or a possessive/demonstrative («de su mandato», «del ejercicio fiscal»).
* ``PERSON`` nouns (alcalde, gobernador, presidente…) need a temporal adjective («del alcalde anterior»).

Nothing else is temporal: «de la Alcaldía», «de la Gobernación», «de aseo», «del Atlántico»,
«de Barranquilla», «de la entidad», «del Gobierno del Tolima», «de la Administración Municipal de
Galapa» keep the window.

The noun phrase may stand before or after the window; the two-digit year («de 25») only right after it
(elsewhere it is a quantity). Pure: no I/O, no clock; ``temporal_semantics`` hands in its own
window patterns.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Iterable

from app.core import non_canonical_year
from app.core.temporal_window_anchor import AnchoredWindow

STRONG = {
    "vigencia": "vigencias", "periodo": "periodos", "ano": "anos", "cuatrienio": "cuatrienios",
    "semestre": "semestres", "trimestre": "trimestres", "bimestre": "bimestres",
    "cuatrimestre": "cuatrimestres", "quincena": "quincenas", "pandemia": "pandemias",
    "legislatura": "legislaturas", "bienio": "bienios", "trienio": "trienios",
    "decada": "decadas", "temporada": "temporadas",
}
ENTITY_OFFICE = {
    "gobierno": "gobiernos", "administracion": "administraciones", "gestion": "gestiones",
}
OFFICE = {"ejercicio": "ejercicios", "mandato": "mandatos"}
PERSON = {
    "alcalde": "alcaldes", "alcaldesa": "alcaldesas", "gobernador": "gobernadores",
    "gobernadora": "gobernadoras", "presidente": "presidentes", "presidenta": "presidentas",
}
# What qualifies a noun that is also part of a buyer name as a PERIOD (a closed, explicit set).
PERIOD_QUALIFIERS = (
    "anterior", "anteriores", "pasado", "pasada", "pasados", "pasadas", "saliente", "salientes",
    "entrante", "entrantes", "actual", "actuales", "previo", "previa", "previos", "previas",
)
# «año» / «período»: the accented spellings are folded below.
ADJECTIVES = (
    "anterior", "anteriores", "pasado", "pasada", "pasados", "pasadas", "actual", "actuales",
    "siguiente", "siguientes", "proximo", "proxima", "proximos", "proximas", "vigente",
    "vigentes", "fiscal", "fiscales", "previo", "previa", "previos", "previas", "saliente",
    "salientes", "entrante", "entrantes", "presente", "presentes", "corriente", "corrientes",
)
EXTRA_ADJECTIVES = ("en curso",)
# Before the noun, between the determiner and it: «el primer semestre», «los dos años anteriores».
MODIFIERS = (
    "primer", "primero", "primera", "primeros", "primeras", "segundo", "segunda", "segundos",
    "segundas", "tercer", "tercero", "tercera", "terceros", "terceras", "cuarto", "cuarta",
    "cuartos", "cuartas", "quinto", "quinta", "sexto", "sexta", "ultimo", "ultima", "ultimos",
    "ultimas", "penultimo", "penultima", "mismo", "misma", "mismos", "mismas",
    "un", "una", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve", "diez",
)
PLAIN_DETERMINERS = (
    "la", "las", "los", "el", "todo el", "toda la", "todos los", "todas las",
)
POSSESSIVE_DETERMINERS = (
    "su", "sus", "este", "esta", "estos", "estas", "ese", "esa", "esos", "esas", "aquel",
    "aquella", "aquellos", "aquellas", "mi", "mis", "nuestro", "nuestra", "nuestros",
    "nuestras", "tu", "tus", "dicho", "dicha", "dichos", "dichas",
)
PREPOSITIONS = (
    "de", "en", "durante", "para", "dentro de", "desde", "hasta", "entre", "por",
    "a lo largo de",
)

_ACCENTS = {"a": "[aá]", "e": "[eé]", "i": "[ií]", "o": "[oó]", "u": "[uúü]", "n": "[nñ]"}


def fold(text: str) -> str:
    """Lower-case, without accents («período» -> «periodo», «año» -> «ano»)."""
    plain = unicodedata.normalize("NFKD", text.lower())
    return " ".join("".join(ch for ch in plain if not unicodedata.combining(ch)).split())


def _word(folded: str) -> str:
    return r"\s+".join(
        "".join(_ACCENTS.get(char, char) for char in part) for part in folded.split(" ")
    )


def _any(words: Iterable[str]) -> str:
    return "|".join(_word(word) for word in sorted(set(words), key=len, reverse=True))


def _class_of() -> dict[str, str]:
    table: dict[str, str] = {}
    for kind, nouns in (
        ("strong", STRONG), ("entity_office", ENTITY_OFFICE), ("office", OFFICE), ("person", PERSON),
    ):
        for singular, plural in nouns.items():
            table[singular] = table[plural] = kind
    return table


_NOUN_CLASS = _class_of()
_ADJECTIVE_SET = frozenset(ADJECTIVES) | frozenset(EXTRA_ADJECTIVES) | frozenset(PERIOD_QUALIFIERS)
_QUALIFIER_SET = frozenset(PERIOD_QUALIFIERS)
_POSSESSIVE_SET = frozenset(POSSESSIVE_DETERMINERS)
_PLAIN_SET = frozenset(PLAIN_DETERMINERS)

_NOUN_PHRASE = re.compile(
    rf"(?<![\wáéíóúñÁÉÍÓÚÑ])"
    rf"(?:{_any(PREPOSITIONS)})(?:\s+(?P<det>{_any(PLAIN_DETERMINERS + POSSESSIVE_DETERMINERS)}))?"
    rf"\s+(?P<np>(?:(?:{_any(MODIFIERS)})\s+)*"
    rf"(?P<noun>{_any(_NOUN_CLASS)})"
    rf"(?:\s+(?:(?:inmediatamente|recien)\s+)?(?P<adj>{_any(_ADJECTIVE_SET)}))?)"
    rf"(?![\wáéíóúñÁÉÍÓÚÑ])",
    re.IGNORECASE,
)
# «del» is already «de + el»: the phrase starts at «del» itself.
_CONTRACTED_NOUN_PHRASE = re.compile(
    rf"(?<![\wáéíóúñÁÉÍÓÚÑ])(?P<np>del\s+(?:(?:{_any(MODIFIERS)})\s+)*"
    rf"(?P<noun>{_any(_NOUN_CLASS)})"
    rf"(?:\s+(?:(?:inmediatamente|recien)\s+)?(?P<adj>{_any(_ADJECTIVE_SET)}))?)"
    rf"(?![\wáéíóúñÁÉÍÓÚÑ])",
    re.IGNORECASE,
)


def is_anchor(kind: str, determiner: str | None, has_adjective: bool) -> bool:
    """The closed decision: does this noun phrase name a period?

    ``determiner`` is ``None`` (no article), ``"plain"`` (article, «del») or ``"possessive"``
    (possessive or demonstrative). ``has_adjective`` is whether the adjective after the noun is one
    that qualifies THIS class of noun as a period (see ``_qualifies``).
    """
    if kind == "strong":
        return determiner is not None or has_adjective
    if kind == "office":
        return has_adjective or determiner == "possessive"
    # ENTITY_OFFICE and PERSON: only an explicit temporal qualifier makes them a period.
    return has_adjective


def _qualifies(kind: str, adjective: str | None) -> bool:
    """Is ``adjective`` (as written) a temporal qualifier for a noun of ``kind``?"""
    if adjective is None:
        return False
    folded = fold(adjective)
    if kind in ("entity_office", "person"):
        return folded in _QUALIFIER_SET
    return folded in _ADJECTIVE_SET


def _determiner_kind(word: str | None, contracted: bool) -> str | None:
    if contracted:
        return "plain"
    if word is None:
        return None
    return "possessive" if fold(word) in _POSSESSIVE_SET else "plain"


def _overlaps(span: tuple[int, int], others: Iterable[tuple[int, int]]) -> bool:
    return any(span[0] < end and span[1] > start for start, end in others)


def _noun_phrases(text: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    for pattern in (_NOUN_PHRASE, _CONTRACTED_NOUN_PHRASE):
        for hit in pattern.finditer(text):
            contracted = pattern is _CONTRACTED_NOUN_PHRASE
            kind = _NOUN_CLASS[fold(hit.group("noun"))]
            determiner = _determiner_kind(hit.groupdict().get("det"), contracted)
            if is_anchor(kind, determiner, _qualifies(kind, hit.group("adj"))):
                spans.append(hit.span("np"))
    return spans


def find(
    text: str,
    window_patterns: Iterable[re.Pattern[str]],
    protected_spans: tuple[tuple[int, int], ...] = (),
) -> AnchoredWindow | None:
    """The first relative window of ``text`` with a temporal noun phrase or a year the authority
    does not read, or ``None``.

    Phrases that overlap a window («los dos últimos años») or a protected span (a name, a norm
    citation) are not anchors.
    """
    windows = sorted(
        (hit for pattern in window_patterns for hit in pattern.finditer(text)),
        key=lambda hit: hit.start(),
    )
    if not windows:
        return None
    window_spans = [hit.span() for hit in windows]
    candidates = _noun_phrases(text)
    year = non_canonical_year.find(text)
    if year is not None:
        candidates.append(year)
    for hit in windows:
        adjacent = non_canonical_year.find_adjacent(text, hit.end())
        if adjacent is not None:
            candidates.append(adjacent)
    candidates = [
        span for span in candidates
        if not _overlaps(span, window_spans) and not _overlaps(span, protected_spans)
    ]
    if not candidates:
        return None
    anchor = min(candidates, key=lambda span: span[0])
    first = windows[0]
    return AnchoredWindow(
        window=first.group(0),
        anchor=text[anchor[0]:anchor[1]].strip(),
        span=(min(first.start(), anchor[0]), max(first.end(), anchor[1])),
    )
