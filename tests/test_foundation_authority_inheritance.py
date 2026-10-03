"""M2A-3b: the host's separate input for inheritance, rule by rule (contract §15.4, §16.1, §16.3; INV-M2A-07, -17).

``foundation_authority_inheritance`` derives unit, year and previous-TIME inputs from the accepted base ALONE. Every rule is
a finite table and every table is walked in full, from the contract's words (never from the module's code):

* UNIT (§15.4): every pair (lower bound, upper bound) over {absent, each ``Unit``, unrecorded}; PRESENT only with ONE
  active unit; two units are AMBIGUOUS (SF102: MILLONES and MIL_MILLONES are two units, not «peso scale»).
* YEAR (§16.1) and REFERENT (§16.3): every TIME shape (year, month, semester, quarter across a year, three years, a day,
  unaligned ranges, open ends) × every ``YearSource`` × the receipts × the number and polarity of TIME conditions × where
  the TIME sits in the logic (conjunctive, under OR, under NOT).
* The three inputs are discriminated unions: the type refuses every (status, reason, payload) combination that is not one
  of PRESENT / ABSENT / AMBIGUOUS-with-its-own-reason.

The batteries of the mutants live in ``test_foundation_authority_inheritance_mutants``; the generated bases in
``..._properties``; the Gold's DEP-INHERITANCE cases in ``..._gold``. The builders of this file are shared by them.
"""
from __future__ import annotations

import hashlib
import itertools
from dataclasses import dataclass, field
from datetime import date

import pytest

import app.core.foundation_accepted as fa
import app.core.foundation_authority_inheritance as inh
from app.core.foundation_authority_values import (
    BuyerKind, BuyerValue, Comparator, MoneyValue, TimeValue, TopicValue,
)
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import ConditionExpression, ContractError, LogicOperator, Polarity
from app.core.semantic_dimensions import SemanticDimension as D

BASE_TRACE = "t-base"
DATASETS = {Subject.PROCESSES: "p6dx-8zbt", Subject.CONTRACTS: "jbjy-vk9h"}
PRESENT, ABSENT, AMBIGUOUS = inh.Presence.PRESENT, inh.Presence.ABSENT, inh.Presence.AMBIGUOUS
AMB = inh.Ambiguity


# ---- builders (shared with the sibling files) ---------------------------------------------------------------------------
def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass(frozen=True)
class Cond:
    """The spec of one condition. What is NOT meaning (origin, request ids, authority name) is applied at build time from
    ``noise``, so the same spec built under two noises is the same base for everything that matters."""

    cid: str
    dimension: D
    value: object  # a closed typed value (``MoneyValue``, ``TimeValue``, ...)
    provenance: fa.Provenance = field(default_factory=fa.Provenance)
    rules: tuple[str, ...] = ()
    operator: str | None = None


def build_condition(spec: Cond, rids: tuple[str, ...], noise: int) -> fa.AcceptedCondition:
    encoded = spec.value.encode()
    slot = fa.slot_of(spec.dimension, encoded)
    operator = spec.operator
    if operator is None:
        operator = {D.VALUE: lambda: fa.VALUE_OPERATOR_OF[spec.value.comparator], D.TIME: lambda: "between_dates",
                    D.TOPIC: lambda: "contains", D.BUYER: lambda: "is"}[spec.dimension]()
    origin = fa.ConditionOrigin(fa.OriginKind.SPAN, f"tn{noise}-{spec.cid}", sha(f"raw{noise}{spec.cid}"), (noise, noise + 3))
    return fa.AcceptedCondition(spec.cid, tuple(sorted(rids)), slot, spec.dimension, operator, slot.polarity or Polarity.POSITIVE,
                                None, encoded, f"authority{noise}@1", tuple(sorted(spec.rules)), (), origin, spec.provenance)


def build_base(specs, *, dual: bool = False, logic=None, noise: int = 0) -> fa.AcceptedInterpretation:
    subjects = (Subject.PROCESSES, Subject.CONTRACTS) if dual else (Subject.CONTRACTS if noise % 2 == 0 else Subject.PROCESSES,)
    kind = fa.UniverseKind.RECORDS if noise % 3 == 0 else fa.UniverseKind.OPPORTUNITIES
    ids = tuple(f"r{i}" if noise == 0 else f"req{noise}-{i}" for i, _ in enumerate(subjects, 1))
    org = fa.ConditionOrigin(fa.OriginKind.SPAN, f"tn{noise}-u", sha(f"universe{noise}"), (0, 2))
    requests = tuple(fa.AcceptedRequest(rid, f"scope{noise}", subject, kind, org, None, DATASETS[subject] + f"-{noise}")
                     for rid, subject in zip(ids, subjects))
    conditions = tuple(build_condition(spec, ids, noise) for spec in specs)
    return fa.canonical_interpretation(requests, conditions, logic, (), None if noise == 0 else f"bt{noise}", f"root{noise}")


def atom(cid: str) -> ConditionExpression:
    return ConditionExpression(LogicOperator.ATOM, cid)


def group(operator: LogicOperator, *children: ConditionExpression) -> ConditionExpression:
    return ConditionExpression(operator, None, tuple(children))


AND, OR, NOT = LogicOperator.AND, LogicOperator.OR, LogicOperator.NOT


def money(cid: str, comparator: Comparator, unit: fa.Unit | None, amount: int = 200_000_000) -> Cond:
    """A VALUE bound. ``unit=None`` is a bound whose unit the base does not record (§15.2: comparable with nothing)."""
    if unit is None:
        provenance = fa.Provenance()
    elif unit is fa.Unit.SMMLV:
        provenance = fa.Provenance(unit=unit, source_amount="500", unit_source=fa.UnitSource.STATED, year=2025,
                                   year_source=fa.YearSource.STATED)
    else:
        provenance = fa.Provenance(unit=unit, unit_source=fa.UnitSource.STATED)
    return Cond(cid, D.VALUE, MoneyValue(amount, comparator), provenance)


def lower(unit, cid="v1") -> Cond:
    return money(cid, Comparator.GTE, unit)


def upper(unit, cid="v2") -> Cond:
    return money(cid, Comparator.LTE, unit, 900_000_000)


def time_value(start: date | None, end: date | None, excluded: bool = False) -> TimeValue:
    if start is None or end is None:
        label = "open"
    else:
        label = "year" if (start.month, start.day) == (1, 1) and end == date(start.year + 1, 1, 1) else "range"
    return TimeValue(start, end, label, excluded)


def when(start, end, *, cid="c3", excluded=False, source=None, rules=()) -> Cond:
    """A TIME condition; ``source`` is the ``year_source`` of its provenance (None: not recorded)."""
    provenance = fa.Provenance(year_source=source,
                               inherited_from="t-prev" if source is fa.YearSource.PREVIOUS_ACCEPTED_TIME else None)
    return Cond(cid, D.TIME, time_value(start, end, excluded), provenance, tuple(rules))


def topic(cid="c1") -> Cond:
    return Cond(cid, D.TOPIC, TopicValue("aseo", False, False))


def buyer(cid="c2") -> Cond:
    return Cond(cid, D.BUYER, BuyerValue("ICBF", BuyerKind.UNIT, ("ICBF",), None, False))


Y = date  # shorthand for the shapes below
SHAPES = {  # name: (start, end_exclusive)
    "year": (Y(2024, 1, 1), Y(2025, 1, 1)),
    "month": (Y(2024, 3, 1), Y(2024, 4, 1)),
    "semester": (Y(2024, 1, 1), Y(2024, 7, 1)),
    "quarter_across_years": (Y(2024, 11, 1), Y(2025, 2, 1)),
    "three_years": (Y(2022, 1, 1), Y(2025, 1, 1)),
    "day": (Y(2024, 3, 15), Y(2024, 3, 16)),
    "unaligned_range": (Y(2024, 3, 15), Y(2024, 4, 21)),
    "unaligned_end": (Y(2024, 3, 1), Y(2024, 4, 15)),
    "unaligned_start": (Y(2024, 3, 2), Y(2024, 4, 1)),
    "open_from": (Y(2024, 1, 1), None),
    "open_until": (None, Y(2025, 1, 1)),
}
# What the contract says of each shape, written by hand: (YEAR §16.1, REFERENT §16.3).
#   YEAR: PRESENT with the civil year, or the reason; the granularity does not matter for a year (a day lies in one year).
#   REFERENT: PRESENT with (first_year, last_year, whole months), or the reason; a plurianual referent is PRESENT: whether
#   «el año anterior» fits 36 months is the authority's call, with ``months``.
EXPECTED_SHAPE = {
    "year": (("PRESENT", 2024), ("PRESENT", 2024, 2024, 12)),
    "month": (("PRESENT", 2024), ("PRESENT", 2024, 2024, 1)),
    "semester": (("PRESENT", 2024), ("PRESENT", 2024, 2024, 6)),
    "quarter_across_years": (("AMBIGUOUS", AMB.MULTI_YEAR), ("PRESENT", 2024, 2025, 3)),
    "three_years": (("AMBIGUOUS", AMB.MULTI_YEAR), ("PRESENT", 2022, 2024, 36)),
    "day": (("PRESENT", 2024), ("AMBIGUOUS", AMB.INCOMPATIBLE_GRANULARITY)),
    "unaligned_range": (("PRESENT", 2024), ("AMBIGUOUS", AMB.INCOMPATIBLE_GRANULARITY)),
    "unaligned_end": (("PRESENT", 2024), ("AMBIGUOUS", AMB.INCOMPATIBLE_GRANULARITY)),
    "unaligned_start": (("PRESENT", 2024), ("AMBIGUOUS", AMB.INCOMPATIBLE_GRANULARITY)),
    "open_from": (("AMBIGUOUS", AMB.OPEN_INTERVAL), ("AMBIGUOUS", AMB.OPEN_INTERVAL)),
    "open_until": (("AMBIGUOUS", AMB.OPEN_INTERVAL), ("AMBIGUOUS", AMB.OPEN_INTERVAL)),
}


def inputs_of(*specs, **kwargs) -> inh.HostInheritanceInputs:
    return inh.host_inheritance_inputs(build_base(specs, **kwargs), base_trace_id=BASE_TRACE)


def year_view(result: inh.YearInheritance) -> tuple:
    return (result.status.value, result.year) if result.status is PRESENT else (
        result.status.value, result.reason) if result.status is AMBIGUOUS else (result.status.value,)


def referent_view(result: inh.TimeReferent) -> tuple:
    return ((result.status.value, result.first_year, result.last_year, result.months) if result.status is PRESENT
            else (result.status.value, result.reason) if result.status is AMBIGUOUS else (result.status.value,))


# ---- UNIT (§15.4) --------------------------------------------------------------------------------------------------------
UNITS = tuple(fa.Unit)
BOUND = (None, *UNITS, "unrecorded")  # absent, each unit, a bound the base does not record


def bounds_of(lower_state, upper_state) -> list[Cond]:
    out = []
    for state, make in ((lower_state, lower), (upper_state, upper)):
        if state is not None:
            out.append(make(None if state == "unrecorded" else state))
    return out


def expected_unit(lower_state, upper_state) -> tuple:
    """§15.4 («una única unidad activa inequívoca»), as ``(status, unit, reason)`` of the walk below: PRESENT iff every bound
    carries the same known unit; ABSENT iff there is no bound; otherwise AMBIGUOUS, and the first reason is two known units."""
    states = [s for s in (lower_state, upper_state) if s is not None]
    known = {s for s in states if s != "unrecorded"}
    if not states:
        return ABSENT, None, None
    if len(known) > 1:
        return AMBIGUOUS, None, AMB.MULTIPLE_UNITS
    if "unrecorded" in states:
        return AMBIGUOUS, None, AMB.UNIT_UNRECORDED
    return PRESENT, known.pop(), None


@pytest.mark.parametrize("lower_state, upper_state", list(itertools.product(BOUND, repeat=2)))
def test_every_pair_of_bounds_is_one_of_absent_present_or_ambiguous(lower_state, upper_state):
    """8 x 8 states, each judged by ``expected_unit`` and by the facts a PRESENT unit carries."""
    result = inputs_of(topic(), *bounds_of(lower_state, upper_state)).unit
    assert (result.status, result.unit, result.reason) == expected_unit(lower_state, upper_state)
    if result.status is PRESENT:
        assert result.inherited_from == BASE_TRACE
        assert result.source_condition_ids == tuple(c.cid for c in bounds_of(lower_state, upper_state))
    else:
        assert result.inherited_from is None and result.source_condition_ids == ()


NAMED_UNIT_CASES = [  # (lower, upper, expected) — the cases that matter by name, apart from the walk above
    (fa.Unit.MILLONES, None, (PRESENT, fa.Unit.MILLONES, None)),  # SF101: one prior unit
    (None, fa.Unit.MILLONES, (PRESENT, fa.Unit.MILLONES, None)),  # SF183: the unit of an upper bound
    (fa.Unit.MILLONES, fa.Unit.MILLONES, (PRESENT, fa.Unit.MILLONES, None)),  # SF106: both bounds, one unit
    (fa.Unit.SMMLV, None, (PRESENT, fa.Unit.SMMLV, None)),  # SF167: SMMLV is a unit too (A2)
    (fa.Unit.MILLONES, fa.Unit.MIL_MILLONES, (AMBIGUOUS, None, AMB.MULTIPLE_UNITS)),  # SF102: two peso scales
    (fa.Unit.MILLONES, fa.Unit.SMMLV, (AMBIGUOUS, None, AMB.MULTIPLE_UNITS)),  # pesos and SMMLV
    (fa.Unit.PESOS, "unrecorded", (AMBIGUOUS, None, AMB.UNIT_UNRECORDED)),
    ("unrecorded", None, (AMBIGUOUS, None, AMB.UNIT_UNRECORDED)),
    (None, None, (ABSENT, None, None)),
]


@pytest.mark.parametrize("lower_state, upper_state, expected", NAMED_UNIT_CASES)
def test_the_named_unit_cases(lower_state, upper_state, expected):
    result = inputs_of(buyer(), *bounds_of(lower_state, upper_state)).unit
    assert (result.status, result.unit, result.reason) == expected


@pytest.mark.parametrize("logic_of", [
    lambda a, b: group(OR, atom(a), atom(b)),
    lambda a, b: group(AND, atom(a), group(NOT, atom(b))),
    lambda a, b: group(NOT, group(OR, atom(a), atom(b))),
], ids=["or", "and-not", "not-or"])
def test_the_unit_of_a_bound_does_not_depend_on_where_it_sits_in_the_logic(logic_of):
    """The unit is a fact of the amount the user said, not of how the bounds are combined: one unit is PRESENT and two are
    AMBIGUOUS under any logic (counting more bounds can only add ambiguity, never a wrong PRESENT)."""
    one = build_base((lower(fa.Unit.MILLONES), upper(fa.Unit.MILLONES)), logic=logic_of("v1", "v2"))
    two = build_base((lower(fa.Unit.MILLONES), upper(fa.Unit.SMMLV)), logic=logic_of("v1", "v2"))
    assert inh.unit_inheritance(one, base_trace_id=BASE_TRACE).unit is fa.Unit.MILLONES
    assert inh.unit_inheritance(two, base_trace_id=BASE_TRACE).reason is AMB.MULTIPLE_UNITS


def test_a_dual_anchor_reads_the_conditions_both_requests_share():
    result = inputs_of(lower(fa.Unit.BILLONES), dual=True).unit
    assert (result.status, result.unit) == (PRESENT, fa.Unit.BILLONES)


# ---- YEAR (§16.1) and REFERENT (§16.3): one TIME ----------------------------------------------------------------------------
SOURCES = (None, *fa.YearSource)
ABSOLUTE = tuple(s for s in SOURCES if s is not fa.YearSource.RELATIVE_TO_CLOCK)


@pytest.mark.parametrize("shape", sorted(SHAPES))
@pytest.mark.parametrize("source", ABSOLUTE)
def test_every_shape_of_an_absolute_time_year_and_referent(shape, source):
    """The provenance of the year (stated, inferred by the clock, inherited) never changes the reading of an absolute
    TIME: the interval is the fact."""
    result = inputs_of(topic(), when(*SHAPES[shape], source=source))
    expected_year, expected_referent = EXPECTED_SHAPE[shape]
    assert year_view(result.year) == expected_year
    assert referent_view(result.referent) == expected_referent
    if expected_year[0] == "PRESENT":
        assert (result.year.condition_id, result.year.inherited_from) == ("c3", BASE_TRACE)
    if expected_referent[0] == "PRESENT":
        assert (result.referent.condition_id, result.referent.inherited_from) == ("c3", BASE_TRACE)


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_a_window_relative_to_the_clock_is_never_a_referent_nor_a_year(shape):
    """§16.1 and §16.3: «ventana relativa» ⇒ ambiguous, whatever its dates (the shape is irrelevant to the marker)."""
    result = inputs_of(when(*SHAPES[shape], source=fa.YearSource.RELATIVE_TO_CLOCK))
    assert year_view(result.year) == ("AMBIGUOUS", AMB.RELATIVE_WINDOW)
    assert referent_view(result.referent) == ("AMBIGUOUS", AMB.RELATIVE_WINDOW)


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_a_time_resolved_against_the_previous_one_is_a_relative_window_too(shape):
    """M2A3B-D4 (fail-closed): the receipt ``time.relative_to_previous`` marks a window expressed relatively; a chain of
    relatives is not an absolute referent. ``year.inherited`` (a month whose year was inherited) is NOT relative."""
    relative = inputs_of(when(*SHAPES[shape], source=fa.YearSource.PREVIOUS_ACCEPTED_TIME,
                              rules=("time.relative_to_previous",)))
    assert year_view(relative.year) == ("AMBIGUOUS", AMB.RELATIVE_WINDOW)
    assert referent_view(relative.referent) == ("AMBIGUOUS", AMB.RELATIVE_WINDOW)
    inherited = inputs_of(when(*SHAPES[shape], source=fa.YearSource.PREVIOUS_ACCEPTED_TIME, rules=("year.inherited",)))
    assert year_view(inherited.year) == EXPECTED_SHAPE[shape][0]
    assert referent_view(inherited.referent) == EXPECTED_SHAPE[shape][1]


def test_without_a_positive_time_both_inputs_are_absent():
    assert inputs_of(topic()).year.status is ABSENT and inputs_of(topic()).referent.status is ABSENT
    negative = inputs_of(topic(), when(*SHAPES["year"], excluded=True))  # «no en 2024» states no active period
    assert negative.year.status is ABSENT and negative.referent.status is ABSENT


def test_a_negative_time_next_to_one_positive_time_is_ignored():
    """§16.3 «único TIME positivo activo»: the exclusion is not a referent and does not make the positive one ambiguous."""
    result = inputs_of(when(*SHAPES["year"]), when(*SHAPES["month"], cid="c4", excluded=True))
    assert year_view(result.year) == ("PRESENT", 2024) and result.year.condition_id == "c3"
    assert referent_view(result.referent) == ("PRESENT", 2024, 2024, 12)


@pytest.mark.parametrize("shapes", list(itertools.combinations_with_replacement(["year", "month", "three_years"], 2)))
def test_several_positive_times_are_ambiguous_even_inside_one_year(shapes):
    """«Varios referentes» (§1.2, §16.3): never the first, and not even when both lie in the same year."""
    result = inputs_of(when(*SHAPES[shapes[0]], cid="c3"), when(*SHAPES[shapes[1]], cid="c4"))
    assert year_view(result.year) == ("AMBIGUOUS", AMB.MULTIPLE_TIMES)
    assert referent_view(result.referent) == ("AMBIGUOUS", AMB.MULTIPLE_TIMES)


# ---- where the TIME sits in the logic ------------------------------------------------------------------------------------
LOGICS = {  # name: (logic over c1 TOPIC, c2 BUYER, c3 TIME, is the TIME inside an OR/NOT group?)
    "and": (group(AND, atom("c1"), atom("c2"), atom("c3")), False),
    "or_over_others": (group(AND, group(OR, atom("c1"), atom("c2")), atom("c3")), False),
    "or_with_time": (group(OR, atom("c1"), atom("c2"), atom("c3")), True),
    "not_time": (group(AND, atom("c1"), atom("c2"), group(NOT, atom("c3"))), True),
    "or_nested": (group(AND, atom("c1"), group(OR, atom("c2"), atom("c3"))), True),
    "not_others": (group(AND, group(NOT, atom("c1")), atom("c2"), atom("c3")), False),
}


@pytest.mark.parametrize("name", sorted(LOGICS))
@pytest.mark.parametrize("polarity_excluded", [False, True])
def test_a_time_inside_an_or_or_not_group_is_not_an_active_period(name, polarity_excluded):
    """A TIME under NOT or OR is not stated as active (a positive atom under NOT is an exclusion): ambiguous, never a year.
    Either polarity: a negative atom under NOT would be an inclusion, and the base cannot say which without evaluating."""
    logic, inside = LOGICS[name]
    result = inputs_of(topic(), buyer(), when(*SHAPES["year"], excluded=polarity_excluded), logic=logic)
    if inside:
        assert year_view(result.year) == ("AMBIGUOUS", AMB.NONCONJUNCTIVE_TIME)
        assert referent_view(result.referent) == ("AMBIGUOUS", AMB.NONCONJUNCTIVE_TIME)
    elif polarity_excluded:
        assert result.year.status is ABSENT and result.referent.status is ABSENT
    else:
        assert year_view(result.year) == ("PRESENT", 2024) and referent_view(result.referent) == ("PRESENT", 2024, 2024, 12)


def test_the_precedence_of_the_reasons_is_closed():
    """NONCONJUNCTIVE > MULTIPLE > RELATIVE > OPEN > MULTI_YEAR (year) / INCOMPATIBLE_GRANULARITY (referent)."""
    relative = when(*SHAPES["year"], cid="c3", source=fa.YearSource.RELATIVE_TO_CLOCK)
    both = inputs_of(relative, when(*SHAPES["month"], cid="c4"))
    assert both.year.reason is AMB.MULTIPLE_TIMES and both.referent.reason is AMB.MULTIPLE_TIMES
    under_or = inputs_of(topic(), relative, logic=group(OR, atom("c1"), atom("c3")))
    assert under_or.year.reason is AMB.NONCONJUNCTIVE_TIME and under_or.referent.reason is AMB.NONCONJUNCTIVE_TIME
    open_relative = inputs_of(when(*SHAPES["open_from"], source=fa.YearSource.RELATIVE_TO_CLOCK))
    assert open_relative.year.reason is AMB.RELATIVE_WINDOW and open_relative.referent.reason is AMB.RELATIVE_WINDOW


# ---- the referent carries the typed value ----------------------------------------------------------------------------------
@pytest.mark.parametrize("shape", [name for name, (_, referent) in EXPECTED_SHAPE.items() if referent[0] == "PRESENT"])
def test_the_referent_value_is_the_canonical_time_encoding_of_the_base(shape):
    from app.core.foundation_authority_values import decode_value

    spec = when(*SHAPES[shape])
    referent = inputs_of(topic(), spec).referent
    assert referent.value == spec.value.encode()
    assert decode_value(referent.value) == spec.value  # the authority decodes it with its own closed types


# ---- the inputs are separate, derived from the base alone, and never partial ----------------------------------------------------
def test_the_three_inputs_are_derived_together_and_from_the_same_base():
    base = build_base((lower(fa.Unit.MILLONES), when(*SHAPES["month"])))
    together = inh.host_inheritance_inputs(base, base_trace_id=BASE_TRACE)
    assert together == inh.HostInheritanceInputs(inh.unit_inheritance(base, base_trace_id=BASE_TRACE),
                                                 inh.year_inheritance(base, base_trace_id=BASE_TRACE),
                                                 inh.time_referent(base, base_trace_id=BASE_TRACE))


@pytest.mark.parametrize("bad", [None, "", 7, b"t"])
def test_a_base_trace_id_that_is_not_text_is_refused_even_when_nothing_is_inherited(bad):
    base = build_base((topic(),))
    for function in (inh.unit_inheritance, inh.year_inheritance, inh.time_referent, inh.host_inheritance_inputs):
        with pytest.raises(ContractError, match="INHERITANCE_BASE_TRACE_ID"):
            function(base, base_trace_id=bad)


@pytest.mark.parametrize("not_a_base", [None, {}, "base", build_base((topic(),)).to_dict()])
def test_only_an_accepted_interpretation_is_a_base(not_a_base):
    """Not a record, not a dict of the store, not a patch: INV-M2A-07 (the input is derived from the typed base)."""
    for function in (inh.unit_inheritance, inh.year_inheritance, inh.time_referent, inh.host_inheritance_inputs):
        with pytest.raises(ContractError, match="INHERITANCE_BASE_NOT_ACCEPTED"):
            function(not_a_base, base_trace_id=BASE_TRACE)


def test_the_arguments_after_the_base_are_keyword_only():
    base = build_base((topic(),))
    for function in (inh.unit_inheritance, inh.year_inheritance, inh.time_referent, inh.host_inheritance_inputs):
        with pytest.raises(TypeError):
            function(base, BASE_TRACE)


def test_a_time_value_the_module_cannot_read_is_refused_not_guessed(monkeypatch):
    """The two date fields are read from the canonical ``time@1`` encoding and cross-checked against ``time_years``: a
    disagreement (a codec that drifted) is an error, never a different year."""
    base = build_base((when(*SHAPES["year"]),))
    monkeypatch.setattr(inh, "time_years", lambda condition: (1999, 1999))
    with pytest.raises(ContractError, match="TIME_YEARS_DISAGREE"):
        inh.time_referent(base, base_trace_id=BASE_TRACE)
    with pytest.raises(ContractError, match="TIME_YEARS_DISAGREE"):
        inh.year_inheritance(base, base_trace_id=BASE_TRACE)


# ---- the types: every (status, reason, payload) combination -----------------------------------------------------------------------
UNIT_PAYLOAD = dict(unit=fa.Unit.MILLONES, inherited_from="t", source_condition_ids=("v1",))
YEAR_PAYLOAD = dict(year=2024, condition_id="c3", inherited_from="t")
REFERENT_PAYLOAD = dict(condition_id="c3", value=("time@1", "x"), first_year=2024, last_year=2024, months=12, inherited_from="t")
CASES = [(inh.UnitInheritance, UNIT_PAYLOAD, inh.UNIT_AMBIGUITIES),
         (inh.YearInheritance, YEAR_PAYLOAD, inh.YEAR_AMBIGUITIES),
         (inh.TimeReferent, REFERENT_PAYLOAD, inh.REFERENT_AMBIGUITIES)]


def accepted(allowed, status, reason, with_payload) -> bool:
    """The discriminated union, from the contract: PRESENT = whole payload, no reason; ABSENT = nothing; AMBIGUOUS = a
    reason of its own input and no payload."""
    if status is PRESENT:
        return with_payload and reason is None
    if status is ABSENT:
        return not with_payload and reason is None
    return not with_payload and reason in allowed


@pytest.mark.parametrize("kind, payload, allowed", CASES, ids=[c[0].__name__ for c in CASES])
def test_the_type_is_exactly_a_discriminated_union(kind, payload, allowed):
    for status, reason, with_payload in itertools.product(inh.Presence, (None, *inh.Ambiguity), (False, True)):
        fields = {**(payload if with_payload else {}), "status": status, "reason": reason}
        if accepted(allowed, status, reason, with_payload):
            assert kind(**fields).status is status
        else:
            with pytest.raises(ContractError):
                kind(**fields)


@pytest.mark.parametrize("kind, payload, allowed", CASES, ids=[c[0].__name__ for c in CASES])
def test_a_present_input_refuses_every_partial_or_mistyped_payload(kind, payload, allowed):
    kind(status=PRESENT, **payload)  # the whole payload is accepted
    for name in payload:
        with pytest.raises(ContractError):
            kind(status=PRESENT, **{k: v for k, v in payload.items() if k != name})  # one field missing
    mistyped = {"unit": "MILLONES", "inherited_from": 7, "source_condition_ids": ["v1"], "year": True, "condition_id": "",
                "value": ["time@1"], "first_year": "2024", "last_year": 0, "months": False}
    for name in payload:
        if name in mistyped:
            with pytest.raises(ContractError):
                kind(status=PRESENT, **{**payload, name: mistyped[name]})


def test_the_payload_checks_that_are_not_about_types():
    with pytest.raises(ContractError):
        inh.UnitInheritance(PRESENT, fa.Unit.PESOS, "t", ("v1", "v1"))  # a donor listed twice
    with pytest.raises(ContractError):
        inh.UnitInheritance(PRESENT, fa.Unit.PESOS, "t", ())
    with pytest.raises(ContractError):
        inh.TimeReferent(PRESENT, "c3", ("time@1",), 2025, 2024, 12, "t")  # years out of order
    with pytest.raises(ContractError):
        inh.TimeReferent(PRESENT, "c3", ("time@1",), 2024, 2024, 0, "t")  # a referent spans at least one month
    with pytest.raises(ContractError):
        inh.HostInheritanceInputs(inh.UnitInheritance(ABSENT), inh.YearInheritance(ABSENT), None)
    with pytest.raises(ContractError):
        inh.UnitInheritance("PRESENT", **UNIT_PAYLOAD)  # the status is the enum, not its text


def test_the_inputs_are_immutable():
    result = inputs_of(lower(fa.Unit.MILLONES)).unit
    with pytest.raises((AttributeError, TypeError)):
        result.unit = fa.Unit.PESOS
