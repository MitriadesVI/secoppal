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

Extensión prevista: M4 añadirá composición de numerales verbales ("dos mil
quinientos") por encima de esta capa; M6 podría añadir unidades indexadas al
año (SMMLV). Ninguna de las dos pertenece a este módulo hoy.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

__all__ = ["find_numeric_literal", "resolve_numeric_literal", "resolve_amount"]

# Un literal numérico: empieza y termina en dígito; en medio admite dígitos y
# separadores. Se detiene en cualquier otro carácter, de modo que "900.123.456-7"
# entrega "900.123.456" y "1.500 millones" entrega "1.500".
_NUMERIC_LITERAL_RE = re.compile(r"\d(?:[\d.,]*\d)?")

_ALLOWED_CHARS = frozenset("0123456789.,")
_GROUP_SIZE = 3


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


def resolve_amount(text: str) -> Decimal | None:
    """Resuelve la cantidad del primer literal numérico presente en ``text``.

    Marcadores de moneda ("$", "pesos", "COP") no alteran el valor; se ignoran
    junto con el resto del texto, que es competencia de la capa de unidades.
    """
    literal = find_numeric_literal(text)
    if literal is None:
        return None
    return resolve_numeric_literal(literal)
