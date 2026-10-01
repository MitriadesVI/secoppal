"""Autoridad pura de VALUE: de una condición propuesta a un límite de monto (M1-D).

``resolve_value_condition(quote, operator, polarity)`` decide, sin red ni
estado, si una condición VALUE del intérprete puede ejecutarse y como qué límite:

    RESOLVED  bound ∈ {min, max}, inclusive, amount_cop
    CLARIFY   la cita no basta (sin comparador, cifra sin unidad, rango, …)
    BLOCK     el contrato no lo admite (polaridad negativa, operador ajeno)

Reglas que fija (todas fail-closed):

* El operador debe ser EXACTAMENTE el que nombra la cita (negación incluida):
  «que no superen 300 millones» es ``less_or_equal``; ``greater_than`` ahí es una
  inversión y se pregunta. GT y GTE se distinguen con la misma cifra.
* Una cita con dos cifras o dos comparadores («entre X y Y») nunca se convierte:
  ``money_to_cop`` toma la primera cifra y descarta la otra (A2 §5). El intérprete
  debe emitir DOS átomos inclusivos.
* Una cifra sin unidad bajo el piso anti-ruido («más de 5») no es dinero.
* Negativos y cifras ilegibles (SMMLV sin año, «millón y medio») se preguntan.

Entrada (contrato de autoridades C0 §5.5): la CITA es la autoridad; ``left`` y
``right`` son a lo sumo ``WINDOW_TOKENS`` (4) tokens plegados a cada lado. El
contexto sólo puede RETIRAR autoridad, nunca crearla: un RESOLVED con contexto
es siempre el mismo RESOLVED de la cita sola. La única retirada hoy es una
negación («no», «sin», «ni»; se saltan «que», «sea(n)», «deben»…) pegada a la
izquierda de la cita: el productor la dejó fuera y el operador propuesto leyó el
límite sin negar, así que se pregunta (``VALUE_OPERATOR_EVIDENCE_MISMATCH``) en
vez de ejecutar una inversión. NO se «corrige» a LTE: eso crearía autoridad desde
el contexto. ``right`` se acepta por conformidad de interfaz y hoy no retira nada:
«menos de 300 millones y más de 100 millones» son dos átomos legítimos y cada uno
ve al otro en su ventana.

La proyección a SoQL (``>=``/``<=``) no es de este módulo.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from app.core.money_admission import is_admissible_amount
from app.core.money_comparators import (
    OPERATOR_FAMILY, bound_of, is_inclusive, read_comparators,
)
from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.core.foundation_value_direction import operator_reason
from app.utils.money import money_to_cop, normalize_text

WINDOW_TOKENS = 4

AUTHORITY = "value-authority@1"

VALUE_AMOUNT_UNGROUNDED = "VALUE_AMOUNT_UNGROUNDED"
VALUE_AMOUNT_UNREADABLE = "VALUE_AMOUNT_UNREADABLE"
VALUE_AMOUNT_NEGATIVE = "VALUE_AMOUNT_NEGATIVE"
VALUE_AMOUNT_AMBIGUOUS = "VALUE_AMOUNT_AMBIGUOUS"
VALUE_RANGE_NEEDS_TWO_CONDITIONS = "VALUE_RANGE_NEEDS_TWO_CONDITIONS"
VALUE_WITHOUT_COMPARATOR = "VALUE_WITHOUT_COMPARATOR"
VALUE_NEGATIVE_POLARITY = "VALUE_NEGATIVE_POLARITY"
VALUE_OPERATOR_UNSUPPORTED = "VALUE_OPERATOR_UNSUPPORTED"


class ValueDecision(str, Enum):
    RESOLVED = "RESOLVED"
    CLARIFY = "CLARIFY"
    BLOCK = "BLOCK"


_COMPARATOR = {("min", False): ">", ("min", True): ">=", ("max", False): "<", ("max", True): "<="}
_COMPARATORS = frozenset(_COMPARATOR.values())
MONEY_TAG = "money@1"


@dataclass(frozen=True)
class MoneyValue:
    """Executable value for the projection (C0 ``MoneyValue``). ``encode``/``decode``
    round-trip; the projection reads only decoded values."""

    amount_cop: int
    comparator: str

    def __post_init__(self) -> None:
        if type(self.amount_cop) is not int or self.amount_cop <= 0 or self.comparator not in _COMPARATORS:
            raise ValueError("INVALID_MONEY_VALUE")

    def encode(self) -> tuple[str, ...]:
        return (MONEY_TAG, self.comparator, str(self.amount_cop))

    @classmethod
    def decode(cls, encoded: tuple[str, ...]) -> "MoneyValue":
        if len(encoded) != 3 or encoded[0] != MONEY_TAG or not encoded[2].isascii() or not encoded[2].isdigit():
            raise ValueError("INVALID_MONEY_VALUE")
        return cls(int(encoded[2]), encoded[1])


# Granular code (kept: foundation_messages already speaks four of them) -> closed
# C0 reason of the authority interface.
TARGET_REASON = {
    "VALUE_OPERATOR_EVIDENCE_MISMATCH": "OPERATOR_EVIDENCE_MISMATCH:VALUE",
    "VALUE_OPERATOR_BOUNDARY_MISMATCH": "OPERATOR_EVIDENCE_MISMATCH:VALUE",
    "VALUE_OPERATOR_UNGROUNDED": "UNRESOLVED_HOST_DIMENSION:VALUE",
    "VALUE_OPERATOR_AMBIGUOUS": "AMBIGUOUS_HOST_DIMENSION:VALUE",
    "VALUE_AMOUNT_UNGROUNDED": "UNRESOLVED_HOST_DIMENSION:VALUE",
    "VALUE_AMOUNT_UNREADABLE": "UNRESOLVED_HOST_DIMENSION:VALUE",
    "VALUE_AMOUNT_NEGATIVE": "UNRESOLVED_HOST_DIMENSION:VALUE",
    "VALUE_AMOUNT_AMBIGUOUS": "AMBIGUOUS_HOST_DIMENSION:VALUE",
    "VALUE_RANGE_NEEDS_TWO_CONDITIONS": "AMBIGUOUS_HOST_DIMENSION:VALUE",
    "VALUE_WITHOUT_COMPARATOR": "UNRESOLVED_HOST_DIMENSION:VALUE",
    "VALUE_OPERATOR_UNSUPPORTED": "UNRESOLVED_HOST_DIMENSION:VALUE",
    "VALUE_NEGATIVE_POLARITY": "NEGATION_UNSUPPORTED:VALUE",
}


@dataclass(frozen=True)
class ValueResolution:
    decision: ValueDecision
    bound: Literal["min", "max"] | None
    inclusive: bool | None
    amount_cop: int | None
    reason: str | None
    authority: str = AUTHORITY

    @property
    def target_reason(self) -> str | None:
        """The closed C0 reason (``<REASON>:VALUE``) for ``reason``."""
        return TARGET_REASON[self.reason] if self.reason is not None else None

    @property
    def value(self) -> MoneyValue | None:
        if self.decision is not ValueDecision.RESOLVED:
            return None
        return MoneyValue(self.amount_cop, _COMPARATOR[(self.bound, self.inclusive)])


_NEGATIVE_SIGN = re.compile(r"(?<![\w.,])[-−–]\s*\d|\bnegativ[oa]s?\b", re.IGNORECASE)
_AMOUNT_LEAD = re.compile(r"^(?:(?:como|los|las|el|la|de|que|a)\s+)+")
_NEGATORS = frozenset({"no", "sin", "ni"})
# Words that may sit between a «no» and the quote without detaching it.
_SKIPPED = frozenset({
    "que", "a", "los", "las", "el", "la", "sea", "sean", "es", "son", "este", "esten", "deba", "deban",
    "debe", "deben", "pueda", "puedan", "puede", "pueden", "vaya", "vayan",
})


def _negation_just_before(left: tuple[str, ...]) -> bool:
    tokens = [normalize_text(token) for token in left][-WINDOW_TOKENS:]
    while tokens and tokens[-1] in _SKIPPED:
        tokens.pop()
    return bool(tokens) and tokens[-1] in _NEGATORS


def _stop(decision: ValueDecision, reason: str) -> ValueResolution:
    return ValueResolution(decision, None, None, None, reason)


def resolve_value_condition(
    quote: str, operator: str, polarity: object,
    left: tuple[str, ...] = (), right: tuple[str, ...] = (),
) -> ValueResolution:
    """Resolve one proposed VALUE condition from its quote (+ window). Pure."""
    resolved = _resolve_from_quote(quote, operator, polarity)
    if resolved.decision is ValueDecision.RESOLVED and _negation_just_before(left):
        # Context may only withdraw authority: the operator read the bound unnegated.
        return _stop(ValueDecision.CLARIFY, "VALUE_OPERATOR_EVIDENCE_MISMATCH")
    return resolved


def _resolve_from_quote(quote: str, operator: str, polarity: object) -> ValueResolution:
    if str(getattr(polarity, "value", polarity)).upper() != "POSITIVE":
        return _stop(ValueDecision.BLOCK, VALUE_NEGATIVE_POLARITY)
    family = OPERATOR_FAMILY.get(operator)
    if family is None:
        if operator == "value_without_comparator":
            return _stop(ValueDecision.CLARIFY, VALUE_WITHOUT_COMPARATOR)
        return _stop(ValueDecision.BLOCK, VALUE_OPERATOR_UNSUPPORTED)

    scan = read_comparators(quote or "")
    decisions = resolve_money_polarity(quote or "")
    bounds = {bound_of(f) for f in scan.families if f is not None}
    if (scan.between or {"min", "max"} <= bounds
            or any(d.relation is MoneyRelation.RANGE for d in decisions)):
        return _stop(ValueDecision.CLARIFY, VALUE_RANGE_NEEDS_TWO_CONDITIONS)
    if (misdirected := operator_reason(operator, quote or "")) is not None:
        return _stop(ValueDecision.CLARIFY, misdirected)
    if sum(len(d.amount_spans) for d in decisions) > 1:
        return _stop(ValueDecision.CLARIFY, VALUE_AMOUNT_AMBIGUOUS)  # a second figure: never pick one
    if _NEGATIVE_SIGN.search(scan.folded):
        return _stop(ValueDecision.CLARIFY, VALUE_AMOUNT_NEGATIVE)

    amount_text = _AMOUNT_LEAD.sub("", scan.amount_text())
    amount = money_to_cop(amount_text)
    if amount is None or amount <= 0:
        return _stop(ValueDecision.CLARIFY, VALUE_AMOUNT_UNREADABLE)
    if not is_admissible_amount(amount, amount_text):
        return _stop(ValueDecision.CLARIFY, VALUE_AMOUNT_UNGROUNDED)
    return ValueResolution(ValueDecision.RESOLVED, bound_of(family), is_inclusive(family), amount, None)
