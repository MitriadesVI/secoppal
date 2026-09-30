"""Does a VALUE operator follow its own evidence? (ADR-102 §10.4)

The producer proposes the operator and cites the comparative phrase with its
amount (prompt rule V1). The host used to accept any operator, so «más de 300
millones» proposed as less_than inverted the request silently. This is not a
money parser: it only reads the closed family of the cited comparator and checks
it against the operator already proposed. The projection executes two exclusive
bounds (greater_than → ">", less_than → "<"); anything else is asked.
"""

from __future__ import annotations

import re

from app.utils.money import normalize_text

GT, GTE, LT, LTE, EQ, BETWEEN = "GT", "GTE", "LT", "LTE", "EQ", "BETWEEN"
# Longest phrases first: «no más de» must not be read as «más de».
_MARKERS = sorted((
    *((phrase, GTE) for phrase in ("no menos de", "no menos que", "mayor o igual", "igual o mayor",
                                   "al menos", "como minimo", "por lo menos", "desde",
                                   "a partir de")),
    *((phrase, LTE) for phrase in ("no mas de", "no mas que", "menor o igual", "igual o menor",
                                   "a lo sumo", "como maximo", "hasta")),
    *((phrase, GT) for phrase in ("mas de", "mas que", "mayor de", "mayor que", "mayor a", "mayores de",
                                  "mayores que", "mayores a", "superior a", "superiores a",
                                  "por encima de", "supere", "superen", "exceda", "excedan")),
    *((phrase, LT) for phrase in ("menos de", "menos que", "menor de", "menor que", "menor a", "menores de",
                                  "menores que", "menores a", "inferior a", "inferiores a",
                                  "por debajo de")),
    *((phrase, EQ) for phrase in ("exactamente", "igual a")),
), key=lambda item: -len(item[0]))
_BETWEEN = re.compile(r"\bentre\b.+\by\b")
_COMPATIBLE = {"greater_than": GT, "less_than": LT}
_OPPOSITE = {"greater_than": {LT, LTE}, "less_than": {GT, GTE}}

VALUE_OPERATOR_EVIDENCE_MISMATCH = "VALUE_OPERATOR_EVIDENCE_MISMATCH"
VALUE_OPERATOR_BOUNDARY_MISMATCH = "VALUE_OPERATOR_BOUNDARY_MISMATCH"
VALUE_OPERATOR_UNGROUNDED = "VALUE_OPERATOR_UNGROUNDED"
VALUE_OPERATOR_AMBIGUOUS = "VALUE_OPERATOR_AMBIGUOUS"


def directions(text: str) -> tuple[str, ...]:
    """Every comparator family the cited text names, longest phrase first."""
    folded = f" {normalize_text(text)} "
    if _BETWEEN.search(folded):
        return (BETWEEN,)
    found = []
    for phrase, family in _MARKERS:
        pattern = re.compile(rf"(?<!\w){re.escape(phrase)}(?!\w)")
        if pattern.search(folded):
            found.append(family)
            folded = pattern.sub(" ", folded)
    return tuple(dict.fromkeys(found))


def operator_reason(operator: str, text: str) -> str | None:
    """None when the operator is exactly what the cited comparator says."""
    if operator not in _COMPATIBLE:
        return None  # the projection admits no other VALUE operator; it stays UNSUPPORTED there
    named = directions(text)
    if not named:
        return VALUE_OPERATOR_UNGROUNDED
    if len(named) > 1:
        return VALUE_OPERATOR_AMBIGUOUS
    (family,) = named
    if family == _COMPATIBLE[operator]:
        return None
    if family in _OPPOSITE[operator] or family in (EQ, BETWEEN):
        return VALUE_OPERATOR_EVIDENCE_MISMATCH
    return VALUE_OPERATOR_BOUNDARY_MISMATCH
