"""M1 wave 1 R1 (ZEUS P2-1): the inclusive suffix forms and the arrow.

«X o más», «X o menos», «X o superior», «X en adelante» are INCLUSIVE bounds
(GTE/LTE) whatever follows them («a la fecha», «de valor», «que …»), and «->»
is an arrow, not «>». ZEUS found the guard reading «o más a la fecha» as «más a»
(GT) and the arrow as «>» (GT), so a GTE request passed as ``greater_than``.
The oracle is written by hand from the Spanish meaning.
"""
from __future__ import annotations

import pytest

from app.core.foundation_value_direction import (
    VALUE_OPERATOR_AMBIGUOUS,
    VALUE_OPERATOR_BOUNDARY_MISMATCH,
    VALUE_OPERATOR_EVIDENCE_MISMATCH,
    VALUE_OPERATOR_UNGROUNDED,
    directions,
    operator_reason,
)
from app.core.money_comparators import GT, GTE, LT, LTE
from app.core.money_polarity import MoneyRelation, resolve_money_polarity

OPERATORS = {"greater_than": GT, "greater_or_equal": GTE, "less_than": LT, "less_or_equal": LTE}
BOUND = {GT: "min", GTE: "min", LT: "max", LTE: "max"}

INCLUSIVE_SUFFIX = {
    "300 millones o más": GTE,
    "300 millones o más a la fecha": GTE,
    "300 millones o más de valor": GTE,
    "300 millones o más que eso": GTE,
    "300 millones o mayor": GTE,
    "300 millones o mayores": GTE,
    "300 millones o superior": GTE,
    "300 millones o superiores": GTE,
    "300 millones y más": GTE,
    "300 millones en adelante": GTE,
    "300 millones o menos": LTE,
    "300 millones o menos a la fecha": LTE,
    "300 millones o menos de valor": LTE,
    "300 millones o menos que eso": LTE,
    "300 millones o menor": LTE,
    "300 millones o menores": LTE,
    "300 millones o inferior": LTE,
    "300 millones o inferiores": LTE,
    "300 millones y menos": LTE,
    "300 millones para abajo": LTE,
}


def _expected(operator: str, family: str) -> str | None:
    wanted = OPERATORS[operator]
    if wanted == family:
        return None
    if BOUND[wanted] != BOUND[family]:
        return VALUE_OPERATOR_EVIDENCE_MISMATCH
    return VALUE_OPERATOR_BOUNDARY_MISMATCH  # same bound, other inclusivity


@pytest.mark.parametrize("quote,family", INCLUSIVE_SUFFIX.items(), ids=list(INCLUSIVE_SUFFIX))
def test_an_inclusive_suffix_is_one_inclusive_bound_whatever_follows_it(quote, family):
    assert directions(quote) == (family,), quote
    for operator in OPERATORS:
        assert operator_reason(operator, quote) == _expected(operator, family), (quote, operator)


@pytest.mark.parametrize("quote", ["300 millones o más a la fecha", "300 millones o más de valor"])
def test_a_gte_request_never_executes_as_greater_than(quote):
    """The ZEUS repro: base asked, the candidate executed the exclusive bound."""
    assert operator_reason("greater_than", quote) == VALUE_OPERATOR_BOUNDARY_MISMATCH
    assert operator_reason("greater_or_equal", quote) is None


@pytest.mark.parametrize("quote", ["300 millones o menos a la fecha", "300 millones o menos de valor"])
def test_an_lte_request_never_executes_as_less_than(quote):
    assert operator_reason("less_than", quote) == VALUE_OPERATOR_BOUNDARY_MISMATCH
    assert operator_reason("less_or_equal", quote) is None


@pytest.mark.parametrize("quote", [
    "-> 300 millones", "->300 millones", "=> 300 millones", "<- 300 millones", "<-> 300 millones",
    "--> 300 millones",
])
def test_an_arrow_is_not_a_comparator(quote):
    assert directions(quote) == ()
    for operator in OPERATORS:
        assert operator_reason(operator, quote) == VALUE_OPERATOR_UNGROUNDED, (quote, operator)


@pytest.mark.parametrize("quote,family", [
    ("> 300 millones", GT), (">300 millones", GT), ("< 300 millones", LT), ("<300 millones", LT),
    (">= 300 millones", GTE), ("<= 300 millones", LTE), ("≥ 300 millones", GTE), ("≤ 300 millones", LTE),
])
def test_the_bare_symbols_are_still_read(quote, family):
    assert directions(quote) == (family,)


@pytest.mark.parametrize("quote", ["más o menos 300 millones", "mas o menos 300 millones"])
def test_an_approximation_is_not_an_upper_bound(quote):
    for operator in OPERATORS:
        assert operator_reason(operator, quote) == VALUE_OPERATOR_UNGROUNDED, (quote, operator)


def test_a_second_comparator_with_its_own_amount_is_still_a_second_bound():
    quote = "menos de 100 millones o más de 500 millones"
    assert directions(quote) == (LT, GT)
    for operator in OPERATORS:
        assert operator_reason(operator, quote) == VALUE_OPERATOR_AMBIGUOUS


@pytest.mark.parametrize("text,relation", [
    ("contratos de 300 millones o más a la fecha", MoneyRelation.LOWER_BOUND),
    ("contratos de 300 millones o superior", MoneyRelation.LOWER_BOUND),
    ("contratos de 300 millones o menos de valor", MoneyRelation.UPPER_BOUND),
    ("contratos de 300 millones o inferior", MoneyRelation.UPPER_BOUND),
    ("contratos de 300 millones en adelante", MoneyRelation.LOWER_BOUND),
])
def test_the_parse_side_reads_the_same_inclusive_forms(text, relation):
    (decision,) = resolve_money_polarity(text)
    assert decision.relation is relation


def test_the_parse_side_does_not_read_an_arrow_as_greater_than():
    (decision,) = resolve_money_polarity("contratos -> 300 millones")
    assert decision.relation is MoneyRelation.UNKNOWN


@pytest.mark.parametrize("quote", ["más a 300 millones", "menos a 300 millones"])
def test_mas_and_menos_only_link_with_de_or_que(quote):
    """«más a» is not a comparator («mayor a» is): it is read as nothing, never as a bound."""
    assert directions(quote) == ()
    for operator in OPERATORS:
        assert operator_reason(operator, quote) == VALUE_OPERATOR_UNGROUNDED
