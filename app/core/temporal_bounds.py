"""Comparadores de cota sobre un período ya resuelto (una responsabilidad).

«después de 2022», «antes de marzo», «a partir de 2023», «de 2024 a la fecha»:
la autoridad temporal resolvía el período («2022», «marzo») e ignoraba el
comparador, así que el filtro ejecutado era el período citado en vez de la cota.
Este módulo sólo lee la familia cerrada de comparadores adyacente al período y
convierte [desde, hasta] en una cota abierta. No resuelve fechas: eso sigue en
``temporal_semantics``. Puro y sin reloj.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as Date, timedelta

FROM, AFTER, BEFORE, UNTIL = "FROM", "AFTER", "BEFORE", "UNTIL"

# Texto plegado (sin acentos, minúsculas): sólo reemplazos 1 a 1 para que los
# índices del texto original sigan siendo válidos.
_FOLD = str.maketrans("áéíóúü", "aeiouu")
_PHRASES: dict[str, tuple[str, ...]] = {
    FROM: ("desde", "a partir"),
    AFTER: ("despues", "posteriores", "posterior", "luego", "tras", "con posterioridad"),
    BEFORE: ("antes", "anteriores", "anterior", "previos", "previo", "con anterioridad"),
    UNTIL: ("hasta",),
}
_CONNECTORS = r"(?:\s+(?:de|del|a|al|el|la|los|las))*\s*$"
_TAIL = {
    kind: re.compile(
        r"(?<!\w)(?:" + "|".join(sorted(phrases, key=len, reverse=True)) + r")" + _CONNECTORS
    )
    for kind, phrases in _PHRASES.items()
}
# Preposiciones que los patrones de período ya consumen dentro de su propio span.
_LEAD = re.compile(r"(?:(?:de|del|el|al|a|la|los|las|en|durante)\s+)+", re.IGNORECASE)
_TRAILING_FROM = re.compile(
    r"\s+(?:en\s+adelante|hacia\s+adelante|a\s+la\s+fecha|al\s+presente|a\s+hoy|"
    r"al\s+dia\s+de\s+hoy|hasta\s+(?:hoy|la\s+fecha|ahora|el\s+presente|la\s+actualidad))\b"
)


@dataclass(frozen=True)
class Bound:
    kind: str
    start: int
    end: int


def _fold(text: str) -> str:
    return text.lower().translate(_FOLD)


def find_bound(text: str, start: int, end: int) -> Bound | None:
    """Comparador que gobierna el período ``text[start:end]``, o None.

    Primero el prefijo («después de», «a partir de»); si no hay, el sufijo
    («en adelante», «a la fecha»). El span devuelto cubre período y comparador.
    """
    lead = _LEAD.match(text, start)
    folded_left = _fold(text[: lead.end() if lead else start])
    for kind, pattern in _TAIL.items():
        if found := pattern.search(folded_left):
            return Bound(kind, found.start(), end)
    if suffix := _TRAILING_FROM.match(_fold(text), end):
        return Bound(FROM, start, suffix.end())
    return None


def open_suffix_end(text: str, end: int) -> int | None:
    """Fin del sufijo «a la fecha» tras una cota ya abierta («desde 2024 a la fecha»)."""
    suffix = _TRAILING_FROM.match(_fold(text), end)
    return suffix.end() if suffix else None


def apply_bound(kind: str, desde: Date, hasta: Date) -> tuple[Date | None, Date | None, str]:
    """(desde, hasta, precision) de la cota abierta; los extremos citados se excluyen en AFTER/BEFORE."""
    if kind == FROM:
        return desde, None, "open_from"
    if kind == AFTER:
        return hasta + timedelta(days=1), None, "open_from"
    if kind == BEFORE:
        return None, desde - timedelta(days=1), "open_until"
    return None, hasta, "open_until"
