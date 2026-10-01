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


# ── one lexicon for the quote route ───────────────────────────────────────
# M1 wave 1 VALUE PARITY: the legacy scanner (``money_polarity``) is byte-identical to ``main`` and no
# longer shares this lexicon; the polarity half of the old «both consumers» tests is the E-VALUE contract
# (docs/audits/m1_wave1/E_VALUE_REGRESSION_CASES.jsonl).

def test_every_lexicon_head_is_read_as_its_own_family():
    for head in HEADS:
        if head.family == EQ:
            continue
        quote = f"{head.phrase} {'de ' if head.needs_link else ''}300 millones"
        scan = read_comparators(quote)
        assert [r.family for r in scan.readings] == [head.family], quote


def test_the_lexicon_has_one_owner_and_no_duplicate_phrase():
    assert not hasattr(__import__("app.core.foundation_value_direction", fromlist=["x"]), "_MARKERS")
    assert len(list(itertools.chain(*[[h] for h in HEADS]))) == len({h.phrase for h in HEADS})


# ── the evidence window (C0 §5.5): context may only withdraw authority ─────

from app.core.value_authority import MONEY_TAG, TARGET_REASON, MoneyValue, WINDOW_TOKENS  # noqa: E402

NEGATING_LEFTS = [("no",), ("que", "no"), ("que", "no", "sean"), ("no", "deben"), ("sin",), ("ni",),
                  ("contratos", "que", "no", "sea")]
_OPERATOR_OF = {GT: "greater_than", GTE: "greater_or_equal", LT: "less_than", LTE: "less_or_equal"}


@pytest.mark.parametrize("left", NEGATING_LEFTS)
def test_a_negation_just_before_the_quote_withdraws_authority(left):
    """The producer left the «no» out of the quote: the unnegated reading is an inversion."""
    for quote, family in QUOTES:
        if family == EQ:
            continue
        operator = _OPERATOR_OF[family]
        assert resolve_value_condition(quote, operator, "POSITIVE").decision is ValueDecision.RESOLVED
        withdrawn = resolve_value_condition(quote, operator, "POSITIVE", left=left)
        assert withdrawn.decision is ValueDecision.CLARIFY and withdrawn.amount_cop is None, (quote, left)
        assert withdrawn.target_reason == "OPERATOR_EVIDENCE_MISMATCH:VALUE"


def test_the_context_never_corrects_the_operator_it_cannot_create_a_resolved():
    """«no mayores a 300 millones» cited without the «no» and proposed less_or_equal: the quote
    alone says GT. Context does not turn that into LTE."""
    for operator in OPERATORS:
        result = resolve_value_condition("mayores a 300 millones", operator, "POSITIVE", left=("que", "no"))
        assert result.decision is not ValueDecision.RESOLVED


@pytest.mark.parametrize("left,right", [
    ((), ("y", "menos", "de", "300")),
    (("de", "100", "millones", "y"), ()),
    (("no", "hay", "contratos", "de"), ()),                 # a «no» that is not just before the quote
    (("contratos", "de", "aseo", "de"), ("en", "bogota")),
    (("no", "x", "y", "z", "w"), ()),                       # only the nearest WINDOW_TOKENS count
    (("no", "que", "sea", "es", "a"), ()),                  # a «no» beyond the window never counts
])
def test_legitimate_context_does_not_withdraw(left, right):
    result = resolve_value_condition("más de 100 millones", "greater_than", "POSITIVE", left, right)
    assert result.decision is ValueDecision.RESOLVED and result.amount_cop == 100_000_000
    both = resolve_value_condition("menos de 300 millones", "less_than", "POSITIVE", ("y",), ("desde", "2023"))
    assert both.decision is ValueDecision.RESOLVED


_TOKENS = st.sampled_from(["no", "sin", "ni", "que", "sea", "más", "de", "menos", "300", "millones", "y", "o",
                           "desde", "hasta", "aseo", "2025", "deben", "superar", "a", "los"])


@settings(max_examples=300, deadline=None)
@given(left=st.lists(_TOKENS, max_size=WINDOW_TOKENS + 2).map(tuple),
       right=st.lists(_TOKENS, max_size=WINDOW_TOKENS + 2).map(tuple),
       item=st.sampled_from(QUOTES), operator=st.sampled_from(sorted(OPERATORS)),
       polarity=st.sampled_from(["POSITIVE", "NEGATIVE"]))
def test_property_context_never_creates_authority(left, right, item, operator, polarity):
    quote, _ = item
    alone = resolve_value_condition(quote, operator, polarity)
    with_context = resolve_value_condition(quote, operator, polarity, left, right)
    if with_context.decision is ValueDecision.RESOLVED:
        assert with_context == alone
    else:
        assert alone.decision is not ValueDecision.RESOLVED or (
            with_context.decision is ValueDecision.CLARIFY and with_context.reason == "VALUE_OPERATOR_EVIDENCE_MISMATCH")


# ── the typed value and the closed reasons ─────────────────────────────────

@pytest.mark.parametrize("quote,operator,comparator", [
    ("más de 300 millones", "greater_than", ">"), ("al menos 300 millones", "greater_or_equal", ">="),
    ("menos de 300 millones", "less_than", "<"), ("que no superen 300 millones", "less_or_equal", "<="),
])
def test_a_resolution_exposes_the_typed_money_value(quote, operator, comparator):
    value = resolve_value_condition(quote, operator, "POSITIVE").value
    assert value == MoneyValue(300_000_000, comparator)
    assert resolve_value_condition(quote, "less_than" if operator == "greater_than" else "greater_than",
                                   "POSITIVE").value is None


@settings(max_examples=200, deadline=None)
@given(amount=st.integers(min_value=1, max_value=10**15), comparator=st.sampled_from([">", ">=", "<", "<="]))
def test_property_money_value_round_trips(amount, comparator):
    value = MoneyValue(amount, comparator)
    assert MoneyValue.decode(value.encode()) == value and value.encode()[0] == MONEY_TAG


@pytest.mark.parametrize("bad", [
    (), ("money@1", ">", "-5"), ("money@1", "==", "5"), ("money@2", ">", "5"), ("money@1", ">", "5.0"),
    ("money@1", ">", "0"), ("money@1", ">", "٣"), ("money@1", ">"), ("money@1", ">", "5", "x"),
])
def test_a_malformed_encoded_money_value_is_rejected(bad):
    with pytest.raises(ValueError):
        MoneyValue.decode(bad)


def test_every_reason_the_authority_can_emit_has_a_closed_target_reason():
    emitted = set()
    for quote, family in QUOTES:
        for operator in OPERATORS:
            for polarity in ("POSITIVE", "NEGATIVE"):
                emitted.add(resolve_value_condition(quote, operator, polarity).reason)
    for quote in ("más de 5", "entre 1 y 2 millones", "más de -5 millones", "más de 2 SMMLV", "más de 3 millones en 2025",
                  "300 millones", "no al menos 300 millones"):
        for operator in (*OPERATORS, "value_without_comparator", "equals"):
            emitted.add(resolve_value_condition(quote, operator, "POSITIVE").reason)
    emitted.discard(None)
    assert emitted and emitted <= set(TARGET_REASON)
    closed = {"OPERATOR_EVIDENCE_MISMATCH:VALUE", "UNRESOLVED_HOST_DIMENSION:VALUE", "AMBIGUOUS_HOST_DIMENSION:VALUE",
              "NEGATION_UNSUPPORTED:VALUE"}
    assert set(TARGET_REASON.values()) == closed
    assert resolve_value_condition("más de 3", "greater_than", "NEGATIVE").target_reason == "NEGATION_UNSUPPORTED:VALUE"
