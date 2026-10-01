"""Qué calificadores temporales cercanos puede ver una cita (una responsabilidad).

Léxico CERRADO de meses, períodos parciales y cotas, y dos lecturas puras:

* ``window_qualifier(left, right)``: ¿los tokens plegados que rodean la cita (la
  ventana de evidencia, ≤ ``WINDOW_TOKENS`` por lado) contienen un calificador
  que la cita no lleva («antes de» + «2024»)? El contexto sólo puede RETIRAR
  autoridad, nunca añadirla.
* ``unparsed_qualifier(text)``: ¿queda dentro de la propia cita, fuera del span
  que resolvió la autoridad, una palabra del léxico («finales de 2025»)?

GARANTÍA LIMITADA, a propósito: es una guarda de frontera, no un parser. Se parte
del borde de la cita y se camina hacia afuera sólo por palabras *pegamento*
(artículos, ordinales, números pequeños); una palabra del léxico o un año en esa
caminata es calificador, y cualquier otra palabra detiene la caminata
(«hasta 500 millones en 2025» sigue siendo un año simple). Límites conocidos:
sobre-pregunta con nombres propios («Avenida Primero de Mayo 2025») y no ve un
calificador más lejos que la ventana ni fuera del léxico. Los tokens plegados no
conservan separadores, así que «15/03/2025» sólo se detecta si va dentro de la
cita (ver ``numeric_date_in``).
"""
from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence

WINDOW_TOKENS = 4  # «a partir de el año» es la cadena calificadora cerrada más larga

_YEAR = re.compile(r"(?:19|20)\d{2}")
_TOKEN = re.compile(r"[a-z0-9]+")
_SMALL_NUMBER = re.compile(r"\d{1,3}")
_QUARTER = re.compile(r"[qthsb][1-4]|[1-4][qthsb]")
_NUMERIC_DATE = re.compile(
    r"(?<!\d)(?:\d{1,2}\s?[/.\-]\s?\d{1,2}\s?[/.\-]\s?\d{2,4}"  # 15/03/2025
    r"|\d{4}\s?[/.\-]\s?\d{1,2}(?!\d)"  # 2025-03, 2025/3
    r"|\d{4}\s?[/\-]\s?(?:i{1,3}|iv)\b)",  # 2025-I
    re.IGNORECASE,
)
_ISO_DAY = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")

MONTHS = frozenset({
    "enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre",
    "setiembre", "octubre", "noviembre", "diciembre", "ene", "feb", "mar", "abr", "may", "jun",
    "jul", "ago", "sep", "sept", "oct", "nov", "dic",
})
PERIODS = frozenset({
    "semestre", "trimestre", "bimestre", "cuatrimestre", "quincena", "semana", "mes", "mitad",
    "dia", "semestres", "trimestres", "bimestres", "cuatrimestres", "quincenas", "semanas",
    "meses", "mitades", "dias", "inicio", "inicios", "comienzo", "comienzos", "principio",
    "principios", "mediados", "final", "finales", "fines", "fin", "cierre",
})
LEADING_BOUNDS = frozenset({
    "desde", "hasta", "entre", "antes", "despues", "tras", "partir", "previo", "previa", "previos",
    "posterior", "posteriores", "anterior", "anteriores", "pre", "post",
})
TRAILING_BOUNDS = frozenset({
    "adelante", "atras", "posterior", "posteriores", "anterior", "anteriores", "siguientes", "hoy",
    "fecha", "antes", "despues", "ahora", "actualidad", "presente", "aca",
})
ORDINALS = frozenset({
    "primer", "primero", "primera", "primeros", "primeras", "segundo", "segunda", "segundos",
    "segundas", "tercer", "tercero", "tercera", "terceros", "terceras", "cuarto", "cuarta",
    "cuartos", "cuartas", "ultimo", "ultima", "ultimos", "ultimas", "un", "una", "dos", "tres",
    "cuatro", "cinco", "seis", "siete", "ocho", "nueve", "diez", "once", "doce",
})
_GLUE = frozenset({"de", "del", "el", "la", "los", "las", "ano", "anos", "y", "e", "o", "u", "a",
                   "al"}) | ORDINALS
_TRAILING_GLUE = _GLUE | {"en", "hacia", "para", "hasta"}
_LEADING_LEXICON = MONTHS | PERIODS | LEADING_BOUNDS
_TRAILING_LEXICON = MONTHS | PERIODS | TRAILING_BOUNDS
_ANY_LEXICON = MONTHS | PERIODS | LEADING_BOUNDS | TRAILING_BOUNDS


def fold(text: str) -> str:
    """Minúsculas y sin acentos; sólo para leer el léxico cerrado."""
    plain = unicodedata.normalize("NFKD", text.lower())
    return "".join(ch for ch in plain if not unicodedata.combining(ch))


def tokens_of(text: str) -> list[str]:
    return _TOKEN.findall(fold(text))


def _walk(outward: Sequence[str], lexicon: frozenset[str], glue: frozenset[str]) -> bool:
    for token in outward[:WINDOW_TOKENS]:
        if token in lexicon or _YEAR.fullmatch(token) or _QUARTER.fullmatch(token):
            return True
        if token not in glue and not _SMALL_NUMBER.fullmatch(token):
            return False
    return False


def window_qualifier(left: Sequence[str], right: Sequence[str]) -> bool:
    """True si el contexto califica la cita; ``left``/``right`` en orden de lectura."""
    nearest_first = [fold(token) for token in reversed(left)]
    return _walk(nearest_first, _LEADING_LEXICON, _GLUE) or _walk(
        [fold(token) for token in right], _TRAILING_LEXICON, _TRAILING_GLUE,
    )


def unparsed_qualifier(text: str) -> bool:
    """True si ``text`` (resto de la cita) aún nombra mes, período, cota, año o trimestre."""
    return any(
        token in _ANY_LEXICON or _YEAR.fullmatch(token) or _QUARTER.fullmatch(token)
        for token in tokens_of(text)
    )


def numeric_date_in(text: str) -> bool:
    """Fecha numérica distinta de la ISO completa («15/03/2025», «2025-03», «2025-I»)."""
    return _NUMERIC_DATE.search(_ISO_DAY.sub(" ", text)) is not None
