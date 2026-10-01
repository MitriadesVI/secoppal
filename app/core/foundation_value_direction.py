"""Does a VALUE operator follow its own evidence? (ADR-102 §10.4, M1-D)

The producer proposes the operator and cites the comparative phrase with its
amount (prompt rule V1). The host used to accept any operator, so «más de 300
millones» proposed as less_than inverted the request silently. This is not a
money parser: it reads the comparator the citation names (lexicon in
``money_comparators``, shared with ``money_polarity``) and checks it against the
operator already proposed, for all four VALUE operators. Negation is read
(«que no superen» = LTE, «no menos de» = GTE) and the inclusive bound is part of
the comparison: GT is not GTE, so «al menos» never passes as ``greater_than``.
"""

from __future__ import annotations

from app.core.money_comparators import (
    BETWEEN, EQ, GT, GTE, LT, LTE, OPERATOR_FAMILY, read_comparators,
)

__all__ = [
    "BETWEEN", "EQ", "GT", "GTE", "LT", "LTE", "directions", "operator_reason",
    "VALUE_OPERATOR_EVIDENCE_MISMATCH", "VALUE_OPERATOR_BOUNDARY_MISMATCH",
    "VALUE_OPERATOR_UNGROUNDED", "VALUE_OPERATOR_AMBIGUOUS",
]

_OPPOSITE = {GT: {LT, LTE}, GTE: {LT, LTE}, LT: {GT, GTE}, LTE: {GT, GTE}}

VALUE_OPERATOR_EVIDENCE_MISMATCH = "VALUE_OPERATOR_EVIDENCE_MISMATCH"
VALUE_OPERATOR_BOUNDARY_MISMATCH = "VALUE_OPERATOR_BOUNDARY_MISMATCH"
VALUE_OPERATOR_UNGROUNDED = "VALUE_OPERATOR_UNGROUNDED"
VALUE_OPERATOR_AMBIGUOUS = "VALUE_OPERATOR_AMBIGUOUS"


def directions(text: str) -> tuple[str | None, ...]:
    """Every comparator family the cited text names, in order of appearance.

    ``None`` marks a negated lexically-inclusive phrase («no al menos»): it is
    named but cannot be read, so callers must ask rather than guess.
    """
    scan = read_comparators(text)
    return (BETWEEN,) if scan.between else scan.families


def operator_reason(operator: str, text: str) -> str | None:
    """None when the operator is exactly what the cited comparator says."""
    expected = OPERATOR_FAMILY.get(operator)
    if expected is None:
        return None  # not a VALUE comparison operator; it stays UNSUPPORTED upstream
    scan = read_comparators(text)
    if scan.between:
        return VALUE_OPERATOR_EVIDENCE_MISMATCH
    named = scan.families
    if not named:
        return VALUE_OPERATOR_UNGROUNDED
    if len(named) > 1 or None in named or scan.unread_negation:
        return VALUE_OPERATOR_AMBIGUOUS  # two bounds, an unreadable negation, or a negation left unread
    (family,) = named
    if family == expected:
        return None
    if family in _OPPOSITE[expected] or family == EQ:
        return VALUE_OPERATOR_EVIDENCE_MISMATCH
    return VALUE_OPERATOR_BOUNDARY_MISMATCH
