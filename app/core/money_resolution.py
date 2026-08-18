"""Autoridad determinista de REPRESENTACIÓN NUMÉRICA monetaria colombiana.

Money V2 / M2. Responsabilidad única: decidir qué número denota un literal
numérico escrito a la colombiana, distinguiendo agrupación de miles de marca
decimal. NO conoce unidades ("millones", "billón"), NO conoce polaridad, NO
conoce datasets, NO conoce NIT y NO formatea. Esas siguen siendo autoridades de
``app/utils/money.py`` y ``app/core/query_router.py`` respectivamente.

    money_resolution  →  app/utils/money.py::money_to_cop  →  consumidores

Puro, determinista, sin estado, sin I/O y solo stdlib.

Aritmética con ``Decimal``, nunca con ``float``: la representación binaria de
``float`` no puede ser la autoridad de una cifra monetaria. La conversión a int
COP ocurre aguas arriba, en ``money_to_cop``.

POLÍTICA DE SEPARADORES
-----------------------
Es deliberadamente la MISMA que ``app.core.notify.grouping.numeric_value``, que
ya resolvía este problema para el radar. Duplicar la regla con otra semántica
crearía dos autoridades que devuelven valores distintos para el mismo texto, que
es exactamente lo que M0 identificó como el defecto de fondo. La paridad está
fijada por test en ``tests/test_money_resolution.py``; las dos implementaciones
siguen separadas solo porque consolidarlas exigiría tocar un módulo del radar,
fuera del alcance de M2.

M4 añade aquí la composición del subconjunto práctico de cardinales españoles
que precede unidades monetarias. No intenta ser un motor numérico universal.
M6 puede añadir unidades indexadas al año (SMMLV) por encima de esta capa.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

__all__ = [
    "CARDINAL_WORD_PATTERN",
    "find_numeric_literal",
    "resolve_amount",
    "resolve_cardinal_words",
    "resolve_numeric_literal",
]

# Un literal numérico: empieza y termina en dígito; en medio admite dígitos y
# separadores. Se detiene en cualquier otro carácter, de modo que "900.123.456-7"
# entrega "900.123.456" y "1.500 millones" entrega "1.500".
_NUMERIC_LITERAL_RE = re.compile(r"\d(?:[\d.,]*\d)?")

_ALLOWED_CHARS = frozenset("0123456789.,")
_GROUP_SIZE = 3

_UNITS = {
    "un": 1,
    "uno": 1,
    "dos": 2,
    "tres": 3,
    "cuatro": 4,
    "cinco": 5,
    "seis": 6,
    "siete": 7,
    "ocho": 8,
    "nueve": 9,
}
_SMALL = {
    **_UNITS,
    "diez": 10,
    "once": 11,
    "doce": 12,
    "trece": 13,
    "catorce": 14,
    "quince": 15,
    "dieciseis": 16,
    "diecisiete": 17,
    "dieciocho": 18,
    "diecinueve": 19,
    "veinte": 20,
    "veintiun": 21,
    "veintiuno": 21,
    "veintidos": 22,
    "veintitres": 23,
    "veinticuatro": 24,
    "veinticinco": 25,
    "veintiseis": 26,
    "veintisiete": 27,
    "veintiocho": 28,
    "veintinueve": 29,
}
_TENS = {
    "treinta": 30,
    "cuarenta": 40,
    "cincuenta": 50,
    "sesenta": 60,
    "setenta": 70,
    "ochenta": 80,
    "noventa": 90,
}
_HUNDREDS = {
    "cien": 100,
    "ciento": 100,
    "doscientos": 200,
    "trescientos": 300,
    "cuatrocientos": 400,
    "quinientos": 500,
    "seiscientos": 600,
    "setecientos": 700,
    "ochocientos": 800,
    "novecientos": 900,
}
_CARDINAL_WORDS = frozenset({*_SMALL, *_TENS, *_HUNDREDS, "mil", "y"})
CARDINAL_WORD_PATTERN = r"(?<![a-z])(?:" + "|".join(
    sorted(_CARDINAL_WORDS, key=len, reverse=True)
) + r")(?![a-z])"


def find_numeric_literal(text: str) -> str | None:
    """Devuelve el primer literal numérico de ``text``, o None si no hay."""
    match = _NUMERIC_LITERAL_RE.search(text)
    return match.group(0) if match else None


def _is_thousands_grouping(parts: list[str]) -> bool:
    """True si ``parts`` es una agrupación de miles válida.

    Requiere al menos un separador y que todo grupo posterior al primero tenga
    exactamente tres dígitos: "1.500" y "10.500.000" sí; "1.5" y "1.23.456" no.

    El primer grupo no puede empezar en cero: nadie escribe miles como "0.500"
    ni "01.500", así que un cero inicial delata intención decimal y "0.500"
    debe valer medio, no quinientos. Es la única diferencia deliberada con
    ``numeric_value``, que nunca ve esta forma porque recibe cifras ya
    normalizadas por SECOP en vez de texto escrito por una persona.
    """
    if len(parts) < 2 or not parts[0] or parts[0].startswith("0"):
        return False
    return all(len(part) == _GROUP_SIZE for part in parts[1:])


def resolve_numeric_literal(literal: str) -> Decimal | None:
    """Resuelve un literal numérico aislado a su valor.

    Devuelve None si el literal está malformado o es genuinamente ambiguo. No
    se adivina: preferimos no producir cifra a producir una equivocada.

    Reglas, en orden:

    1. Sin separador → entero literal.
    2. Dos tipos de separador → el más a la derecha es la marca decimal y el
       otro agrupa miles ("1.234.567,89" y "1,234,567.89" valen lo mismo).
    3. Un solo tipo de separador:
       a. si todo grupo tras el primero tiene 3 dígitos → agrupación de miles
          ("1.500" → 1500, "1.500.000" → 1500000);
       b. si el último grupo tiene 1 o 2 dígitos → marca decimal
          ("1.5" → 1.5, "1,25" → 1.25);
       c. en otro caso se delega a ``Decimal``, que acepta "1.5000" y rechaza
          formas imposibles como "1.23.456".

    La regla 3a es la que resuelve H1: en una expresión monetaria colombiana,
    un punto seguido de exactamente tres dígitos agrupa miles. Es una
    resolución declarada de una ambigüedad real, no una heurística oculta:
    "1.500" tomado en aislamiento podría ser mil quinientos o uno con quinientos
    milésimas, y aquí se elige lo primero.
    """
    if not literal:
        return None
    if not literal[0].isdigit() or not literal[-1].isdigit():
        return None
    if any(char not in _ALLOWED_CHARS for char in literal):
        return None

    has_dot = "." in literal
    has_comma = "," in literal
    raw = literal

    if has_dot and has_comma:
        decimal_sep = "," if literal.rfind(",") > literal.rfind(".") else "."
        thousands_sep = "." if decimal_sep == "," else ","
        raw = raw.replace(thousands_sep, "")
        if decimal_sep == ",":
            raw = raw.replace(",", ".")
    elif has_dot or has_comma:
        separator = "." if has_dot else ","
        parts = raw.split(separator)
        if _is_thousands_grouping(parts):
            raw = "".join(parts)
        elif len(parts[-1]) in (1, 2):
            raw = "".join(parts[:-1]) + "." + parts[-1]
        elif separator == ",":
            raw = raw.replace(",", ".")

    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _resolve_below_thousand(tokens: list[str]) -> int | None:
    if not tokens:
        return None

    total = 0
    if tokens[0] in _HUNDREDS:
        hundred_word = tokens.pop(0)
        total = _HUNDREDS[hundred_word]
        if hundred_word == "cien" and tokens:
            return None
        if not tokens:
            return total

    if len(tokens) == 1 and tokens[0] in _SMALL:
        return total + _SMALL[tokens[0]]
    if tokens and tokens[0] in _TENS:
        tens = _TENS[tokens.pop(0)]
        if not tokens:
            return total + tens
        if len(tokens) == 2 and tokens[0] == "y" and tokens[1] in _UNITS:
            return total + tens + _UNITS[tokens[1]]
    return None


def _resolve_cardinal_tokens(tokens: list[str]) -> int | None:
    if not tokens or tokens[0] == "y" or tokens[-1] == "y":
        return None
    if tokens.count("mil") > 1:
        return None
    if "mil" not in tokens:
        return _resolve_below_thousand(tokens.copy())

    split = tokens.index("mil")
    prefix = tokens[:split]
    suffix = tokens[split + 1:]
    thousands = _resolve_below_thousand(prefix.copy()) if prefix else 1
    remainder = _resolve_below_thousand(suffix.copy()) if suffix else 0
    if thousands is None or remainder is None:
        return None
    return thousands * 1000 + remainder


def resolve_cardinal_words(text: str) -> Decimal | None:
    """Resuelve un cardinal español práctico contenido en ``text``.

    Admite unidades, 10-29, decenas con ``y``, centenas y una composición con
    ``mil``. Deliberadamente no interpreta ordinales, fracciones ni escalas
    encadenadas; millón/billón siguen siendo unidades de ``money_to_cop``.
    """
    words = re.findall(r"[a-z]+", text.lower())
    for start, word in enumerate(words):
        if word not in _CARDINAL_WORDS or word == "y":
            continue
        end = start
        while end < len(words) and words[end] in _CARDINAL_WORDS:
            end += 1
        for candidate_end in range(end, start, -1):
            value = _resolve_cardinal_tokens(words[start:candidate_end])
            if value is not None:
                return Decimal(value)
    return None


def resolve_amount(text: str) -> Decimal | None:
    """Resuelve la primera cantidad numérica o cardinal presente en ``text``.

    Marcadores de moneda ("$", "pesos", "COP") no alteran el valor; se ignoran
    junto con el resto del texto, que es competencia de la capa de unidades.
    """
    literal = find_numeric_literal(text)
    if literal is not None:
        return resolve_numeric_literal(literal)
    return resolve_cardinal_words(text)
