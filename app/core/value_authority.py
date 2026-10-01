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

La proyección a SoQL (``>=``/``<=``) no es de este módulo.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Literal

from app.core.money_admission import is_admissible_amount
from app.core.money_comparators import OPERATOR_FAMILY, bound_of, is_inclusive, read_comparators
from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.core.foundation_value_direction import operator_reason
from app.utils.money import money_to_cop

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


@dataclass(frozen=True)
class ValueResolution:
    decision: ValueDecision
    bound: Literal["min", "max"] | None
    inclusive: bool | None
    amount_cop: int | None
    reason: str | None
    authority: str = AUTHORITY


_NEGATIVE_SIGN = re.compile(r"(?<![\w.,])[-−–]\s*\d|\bnegativ[oa]s?\b", re.IGNORECASE)
_AMOUNT_LEAD = re.compile(r"^(?:(?:como|los|las|el|la|de|que|a)\s+)+")


def _stop(decision: ValueDecision, reason: str) -> ValueResolution:
    return ValueResolution(decision, None, None, None, reason)


def resolve_value_condition(quote: str, operator: str, polarity: object) -> ValueResolution:
    """Resolve one proposed VALUE condition from its cited text. Pure."""
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
