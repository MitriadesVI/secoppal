"""M1-D: one comparator lexicon, a guard that reads negation and inclusive bounds,
and a pure VALUE authority.

The oracle below is written by hand from the Spanish meaning, never from the
implementation's table: a comparator the lexicon learns that this table does
not know is a failure (``test_the_oracle_covers_the_whole_lexicon``).
"""
from __future__ import annotations

import itertools

import pytest
from hypothesis import given, settings, strategies as st

from app.core import value_authority
from app.core.foundation_value_direction import (
    VALUE_OPERATOR_AMBIGUOUS,
    VALUE_OPERATOR_BOUNDARY_MISMATCH,
    VALUE_OPERATOR_EVIDENCE_MISMATCH,
    VALUE_OPERATOR_UNGROUNDED,
    directions,
    operator_reason,
)
from app.core.money_comparators import BETWEEN, EQ, GT, GTE, HEADS, LT, LTE, read_comparators
from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.core.value_authority import ValueDecision, resolve_value_condition

OPERATORS = {"greater_than": GT, "greater_or_equal": GTE, "less_than": LT, "less_or_equal": LTE}
BOUND = {GT: "min", GTE: "min", LT: "max", LTE: "max"}
INCLUSIVE = {GT: False, GTE: True, LT: False, LTE: True}

# phrase placed BEFORE the amount -> the family a Spanish speaker means.
PREFIX_ORACLE = {
    # exclusive lower bound
    **{p: GT for p in (
        "más de", "más que", "mayor a", "mayor de", "mayor que", "mayores a", "superior a", "superiores a",
        "por encima de", "encima de", "supere", "superen", "supera", "superan", "superar", "exceda", "excedan",
        "excede", "exceden", "exceder", "rebase", "rebasen", "rebasan", "rebasar", "sobrepase", "sobrepasen", "sobrepasan", "sobrepasar",
        "pase de", "pasen de", "pasa de", "pasan de", "pasar de", "que superen", "que supere", ">")},
    # exclusive upper bound
    **{p: LT for p in (
        "menos de", "menos que", "menor a", "menor de", "menor que", "menores a", "inferior a", "inferiores a",
        "por debajo de", "debajo de", "baje de", "bajen de", "bajan de", "bajar de", "<")},
    # inclusive lower bound
    **{p: GTE for p in (
        "al menos", "por lo menos", "mínimo", "mínimo de", "mínima", "como mínimo", "desde", "a partir de",
        "mayor o igual a", "mayores o iguales a", "superior o igual a", "igual o mayor a", "igual o superior a",
        ">=", "≥")},
    # inclusive upper bound
    **{p: LTE for p in (
        "hasta", "máximo", "máximo de", "máxima", "como máximo", "a lo sumo", "tope de", "menor o igual a",
        "menores o iguales a", "inferior o igual a", "igual o menor a", "igual o inferior a", "sin superar",
        "sin exceder", "sin rebasar", "sin sobrepasar", "sin pasar de", "<=", "≤")},
    # negation flips the bound AND the inclusivity: no (>x) = <=x ; no (>=x) = <x
    **{p: LTE for p in (
        "que no superen", "que no supere", "no superen", "no supere", "no mayor a", "no mayor que", "no mayores a",
        "no exceda", "no excedan", "no rebase", "que no pase de", "no pasen de", "no más de", "no más que",
        "no superior a", "no superiores a", "no por encima de", "que no sea mayor a", "que no sean mayores a",
        "no es mayor a", "no deben superar", "no debe exceder", "no puede pasar de", "no pueden superar",
        "no sea superior a")},
    **{p: GTE for p in (
        "no menos de", "no menos que", "no menor a", "no menor que", "no menores a", "no inferior a",
        "no inferiores a", "no por debajo de", "que no sea menor que", "que no baje de", "que no bajen de",
        "que no sean inferiores a", "no es menor a")},
    **{p: EQ for p in ("exactamente", "igual a")},
}
# amount first, comparator after.
SUFFIX_ORACLE = {
    "o más": GTE, "y más": GTE, "o mayores": GTE, "o superiores": GTE, "en adelante": GTE,
    "para arriba": GTE, "para más": GTE,
    "o menos": LTE, "y menos": LTE, "o menores": LTE, "o inferiores": LTE, "para abajo": LTE,
    "para menos": LTE,
}
AMOUNT = "300 millones"


def _quotes():
    for phrase, family in PREFIX_ORACLE.items():
        yield f"{phrase} {AMOUNT}", family
    for phrase, family in SUFFIX_ORACLE.items():
        yield f"{AMOUNT} {phrase}", family


QUOTES = tuple(_quotes())


def _expected_reason(expected, family):
    if expected == family:
        return None
    if family == EQ:
        return VALUE_OPERATOR_EVIDENCE_MISMATCH
    if BOUND[expected] != BOUND[family]:
        return VALUE_OPERATOR_EVIDENCE_MISMATCH
    return VALUE_OPERATOR_BOUNDARY_MISMATCH  # same bound, other inclusivity: GT is not GTE


# ── the guard: operator vs. its own evidence ──────────────────────────────

@pytest.mark.parametrize("quote,family", QUOTES, ids=[q for q, _ in QUOTES])
def test_the_guard_admits_only_the_operator_the_quote_names(quote, family):
    for operator, expected in OPERATORS.items():
        assert operator_reason(operator, quote) == _expected_reason(expected, family), (quote, operator)


@pytest.mark.parametrize("quote,family", QUOTES, ids=[q for q, _ in QUOTES])
def test_no_opposite_operator_ever_passes(quote, family):
    if family == EQ:
        passing = [op for op in OPERATORS if operator_reason(op, quote) is None]
        assert passing == []
        return
    for operator, expected in OPERATORS.items():
        if BOUND[expected] != BOUND[family]:
            assert operator_reason(operator, quote) is not None, (quote, operator)


def test_the_oracle_covers_the_whole_lexicon():
    keys = " | ".join(PREFIX_ORACLE).replace("á", "a").replace("í", "i")
    for head in HEADS:
        assert head.phrase in keys or head.phrase in " ".join(SUFFIX_ORACLE), f"unverified comparator: {head.phrase}"


def test_every_oracle_quote_is_read_as_exactly_one_comparator():
    for quote, family in QUOTES:
        assert directions(quote) == (family,), quote


@pytest.mark.parametrize("quote", [
    "que no superen 300 millones", "no mayor a 300 millones", "no más de 300 millones",
])
def test_a_negated_upper_limit_cannot_be_executed_as_a_lower_bound(quote):
    """MONEY-058/059: the inversion that used to pass as greater_than."""
    assert operator_reason("greater_than", quote) == VALUE_OPERATOR_EVIDENCE_MISMATCH
    assert operator_reason("greater_or_equal", quote) == VALUE_OPERATOR_EVIDENCE_MISMATCH
    assert operator_reason("less_or_equal", quote) is None


def test_the_four_operators_are_all_read_not_only_the_exclusive_pair():
    """MONEY-045/046: the guard used to return None without reading the citation."""
    assert operator_reason("greater_or_equal", "menos de 300 millones") == VALUE_OPERATOR_EVIDENCE_MISMATCH
    assert operator_reason("less_or_equal", "más de 300 millones") == VALUE_OPERATOR_EVIDENCE_MISMATCH
    assert operator_reason("greater_or_equal", "300 millones") == VALUE_OPERATOR_UNGROUNDED
    assert operator_reason("less_or_equal", "300 millones") == VALUE_OPERATOR_UNGROUNDED


def test_gt_and_gte_with_the_same_figure_are_distinguished():
    assert operator_reason("greater_than", "al menos 300 millones") == VALUE_OPERATOR_BOUNDARY_MISMATCH
    assert operator_reason("greater_or_equal", "más de 300 millones") == VALUE_OPERATOR_BOUNDARY_MISMATCH
    assert operator_reason("less_than", "hasta 300 millones") == VALUE_OPERATOR_BOUNDARY_MISMATCH
    assert operator_reason("less_or_equal", "menos de 300 millones") == VALUE_OPERATOR_BOUNDARY_MISMATCH


@pytest.mark.parametrize("quote", [
    "no al menos 300 millones",            # negating a lexically inclusive phrase is not guessed
    "no hasta 300 millones",
    "no mayor o igual a 300 millones",     # the negation of an inclusive bound is not guessed either
    "no igual o menor a 300 millones",
    "no se debe pasar de 300 millones",    # a negation the grammar did not attach to the comparator
    "no sé, más de 300 millones",
    "más de 100 y menos de 300 millones",  # two bounds
    "desde 100 hasta 300 millones",
])
def test_an_unreadable_or_unattached_negation_is_asked_never_guessed(quote):
    for operator in OPERATORS:
        assert operator_reason(operator, quote) in {VALUE_OPERATOR_AMBIGUOUS, VALUE_OPERATOR_EVIDENCE_MISMATCH}


def test_a_range_is_never_a_single_bound_for_the_guard():
    for operator in OPERATORS:
        assert operator_reason(operator, "entre 100 y 300 millones") == VALUE_OPERATOR_EVIDENCE_MISMATCH
    assert directions("entre 100 y 300 millones") == (BETWEEN,)


def test_operators_outside_the_value_contract_stay_untouched():
    assert operator_reason("contains", "más de 300 millones") is None


# ── the authority ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("quote,family", QUOTES, ids=[q for q, _ in QUOTES])
def test_the_authority_resolves_only_when_operator_and_quote_agree(quote, family):
    for operator, expected in OPERATORS.items():
        result = resolve_value_condition(quote, operator, "POSITIVE")
        if expected == family:
            assert result.decision is ValueDecision.RESOLVED, (quote, operator, result)
            assert (result.bound, result.inclusive, result.amount_cop) == (
                BOUND[family], INCLUSIVE[family], 300_000_000), (quote, operator)
            assert result.reason is None and result.authority == "value-authority@1"
        else:
            assert result.decision is ValueDecision.CLARIFY, (quote, operator, result)
            assert result.bound is None and result.inclusive is None and result.amount_cop is None
            assert result.reason == _expected_reason(expected, family)


@pytest.mark.parametrize("text,cop", [
    ("300 millones", 300_000_000), ("$300.000.000", 300_000_000), ("300.000.000 pesos", 300_000_000),
    ("1.500 millones", 1_500_000_000), ("1,5 millones", 1_500_000), ("mil millones", 1_000_000_000),
    ("3 mil millones", 3_000_000_000), ("tres mil millones", 3_000_000_000), ("200 palos", 200_000_000),
    ("50 barras", 50_000_000), ("2 billones", 2_000_000_000_000), ("500 mil pesos", 500_000),
    ("mil quinientos millones", 1_500_000_000), ("300 millones de pesos", 300_000_000),
    ("100 mil pesos", 100_000), ("5 pesos", 5),
])
def test_es_co_amounts_convert_through_the_authority(text, cop):
    for quote in (f"más de {text}", f"que no superen {text}", f"{text} o más"):
        result = resolve_value_condition(
            quote, "greater_than" if quote.startswith("más") else
            "less_or_equal" if "no superen" in quote else "greater_or_equal", "POSITIVE")
        assert (result.decision, result.amount_cop) == (ValueDecision.RESOLVED, cop), quote


@pytest.mark.parametrize("quote", ["más de 5", "más de 100 mil", "al menos 2025", "hasta 500 mil", "más de 999999"])
def test_a_bare_figure_under_the_noise_floor_is_not_money(quote):
    operator = "greater_than" if quote.startswith("más") else "greater_or_equal" if "menos" in quote else "less_or_equal"
    result = resolve_value_condition(quote, operator, "POSITIVE")
    assert (result.decision, result.reason) == (ValueDecision.CLARIFY, "VALUE_AMOUNT_UNGROUNDED")


@pytest.mark.parametrize("quote", [
    "entre 100 y 300 millones", "entre 100 millones y 300 millones", "de 100 a 300 millones",
    "desde 100 hasta 300 millones", "más de 100 y menos de 300 millones", "100-300 millones",
    "menos de 300 millones y más de 100 millones",
])
def test_a_range_is_two_conditions_and_is_never_converted(quote, monkeypatch):
    def forbidden(_text):
        raise AssertionError("a quote with two figures must never reach money_to_cop")

    monkeypatch.setattr(value_authority, "money_to_cop", forbidden)
    for operator in OPERATORS:
        result = resolve_value_condition(quote, operator, "POSITIVE")
        assert (result.decision, result.reason) == (ValueDecision.CLARIFY, "VALUE_RANGE_NEEDS_TWO_CONDITIONS"), (
            quote, operator)
        assert result.amount_cop is None


@pytest.mark.parametrize("quote,reason", [
    ("más de -5 millones", "VALUE_AMOUNT_NEGATIVE"),
    ("menos de -300 millones", "VALUE_AMOUNT_NEGATIVE"),
    ("más de 2 SMMLV", "VALUE_AMOUNT_UNREADABLE"),
    ("más de un millón y medio", "VALUE_AMOUNT_UNREADABLE"),
    ("más de", "VALUE_AMOUNT_UNREADABLE"),
    ("más de 0 pesos", "VALUE_AMOUNT_UNREADABLE"),
    ("más de 300 millones en 2025", "VALUE_AMOUNT_AMBIGUOUS"),
])
def test_negative_or_unreadable_amounts_are_asked(quote, reason):
    operator = "less_than" if quote.startswith("menos") else "greater_than"
    result = resolve_value_condition(quote, operator, "POSITIVE")
    assert (result.decision, result.reason) == (ValueDecision.CLARIFY, reason)


def test_negative_polarity_blocks_even_with_a_perfect_quote():
    for polarity in ("NEGATIVE", "negative"):
        result = resolve_value_condition("más de 300 millones", "greater_than", polarity)
        assert (result.decision, result.reason) == (ValueDecision.BLOCK, "VALUE_NEGATIVE_POLARITY")
    result = resolve_value_condition("más de 300 millones", "greater_than", "MAYBE")
    assert result.decision is ValueDecision.BLOCK


def test_polarity_accepts_the_contract_enum_by_value():
    from app.core.interpretation_contract import Polarity

    assert resolve_value_condition("más de 300 millones", "greater_than", Polarity.POSITIVE).decision \
        is ValueDecision.RESOLVED
    assert resolve_value_condition("más de 300 millones", "greater_than", Polarity.NEGATIVE).decision \
        is ValueDecision.BLOCK


def test_operators_outside_the_four_are_blocked_and_a_bare_amount_is_asked():
    assert resolve_value_condition("300 millones", "value_without_comparator", "POSITIVE").reason \
        == "VALUE_WITHOUT_COMPARATOR"
    assert resolve_value_condition("300 millones", "value_without_comparator", "POSITIVE").decision \
        is ValueDecision.CLARIFY
    for operator in ("equals", "contains", "is", ""):
        result = resolve_value_condition("más de 300 millones", operator, "POSITIVE")
        assert (result.decision, result.reason) == (ValueDecision.BLOCK, "VALUE_OPERATOR_UNSUPPORTED")


def test_the_authority_is_pure_and_hashable_evidence():
    first = resolve_value_condition("al menos 300 millones", "greater_or_equal", "POSITIVE")
    assert first == resolve_value_condition("al menos 300 millones", "greater_or_equal", "POSITIVE")
    with pytest.raises(Exception):
        first.amount_cop = 1  # frozen


@settings(max_examples=150, deadline=None)
@given(number=st.integers(min_value=1, max_value=999),
       unit=st.sampled_from([("millones", 10**6), ("mil millones", 10**9), ("palos", 10**6)]),
       quote_family=st.sampled_from(sorted(PREFIX_ORACLE.items())))
def test_property_the_resolved_bound_is_exactly_the_one_the_quote_names(number, unit, quote_family):
    phrase, family = quote_family
    name, scale = unit
    for operator, expected in OPERATORS.items():
        result = resolve_value_condition(f"{phrase} {number} {name}", operator, "POSITIVE")
        if family != EQ and expected == family:
            assert result.decision is ValueDecision.RESOLVED
            assert result.amount_cop == number * scale
            assert (result.bound, result.inclusive) == (BOUND[family], INCLUSIVE[family])
        else:
            assert result.decision is not ValueDecision.RESOLVED


# ── one lexicon for both consumers ────────────────────────────────────────

@pytest.mark.parametrize("quote,family", [(q, f) for q, f in QUOTES if f != EQ], ids=[q for q, f in QUOTES if f != EQ])
def test_money_polarity_and_the_guard_read_the_same_direction(quote, family):
    decisions = resolve_money_polarity(quote)
    assert len(decisions) == 1, quote
    expected = MoneyRelation.LOWER_BOUND if BOUND[family] == "min" else MoneyRelation.UPPER_BOUND
    assert decisions[0].relation is expected, quote


@pytest.mark.parametrize("quote,relation", [
    ("por lo menos 300 millones", MoneyRelation.LOWER_BOUND),    # was UPPER_BOUND: «menos» alone won
    ("no es mayor a 300 millones", MoneyRelation.UPPER_BOUND),   # was LOWER_BOUND: «es» hid the «no»
    ("que no deben superar 300 millones", MoneyRelation.UPPER_BOUND),
    ("sin superar 300 millones", MoneyRelation.UPPER_BOUND),
    ("tope de 300 millones", MoneyRelation.UPPER_BOUND),
    ("300 millones o inferiores", MoneyRelation.UPPER_BOUND),
])
def test_legacy_inversions_the_unified_lexicon_corrects(quote, relation):
    assert resolve_money_polarity(quote)[0].relation is relation


def test_every_lexicon_head_is_reached_by_both_consumers():
    for head in HEADS:
        if head.family == EQ:
            continue
        quote = f"{head.phrase} {'de ' if head.needs_link else ''}300 millones"
        scan = read_comparators(quote)
        assert [r.family for r in scan.readings] == [head.family], quote
        relation = resolve_money_polarity(quote)[0].relation
        assert relation is (MoneyRelation.LOWER_BOUND if BOUND[head.family] == "min" else MoneyRelation.UPPER_BOUND)


def test_the_two_consumers_cannot_drift_apart():
    import app.core.money_polarity as polarity

    from app.core.money_comparators import DOWN_PATTERN, UP_PATTERN

    assert polarity._UP == UP_PATTERN and polarity._DOWN == DOWN_PATTERN
    assert not hasattr(__import__("app.core.foundation_value_direction", fromlist=["x"]), "_MARKERS")
    assert len(list(itertools.chain(*[[h] for h in HEADS]))) == len({h.phrase for h in HEADS})
