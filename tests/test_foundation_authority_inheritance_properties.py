"""M2A-3b: properties of the inheritance input over GENERATED bases (Hypothesis, derandomized for the gates).

The tables of ``test_foundation_authority_inheritance`` walk the rules; these generate WORLDS (a spec: bounds by unit, TIME
conditions by dates/polarity/provenance, OR/NOT placement, dual anchor) and judge the module by three independent routes:

* P1 the ORACLE: written on the spec's raw data (units, ``date`` arithmetic, the logic kind), never on the typed base. What
  a relative window is comes from the contract's text (``is_relative_window``: §16 item 4 names its operator), and «sin TIME
  previo» (§16.1) is a base with no TIME at all: a base with only negative TIME is ambiguous (HERA, ZEUS P2-1);
* P2 INV-M2A-07: everything that is not a condition (origins, request ids, scope, datasets, subjects, authority, root and
  base trace of the base, dual or single) is noise: two builds of one world give the same inputs, and ``inherited_from`` is
  the caller's ``base_trace_id`` and nothing the base says;
* P3 INV-M2A-17 as implications over the decoded base: PRESENT never hides two units, several TIME or several years.

A random pair of dates (aligned to months or not, one or several years, open ends) goes through the same oracle, and the
referent's value is decoded with the closed types and checked against calendar arithmetic of its own.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_authority_inheritance as inh
from app.core.foundation_authority_values import TimeValue, decode_value
from tests.test_foundation_authority_inheritance import (
    AND, BASE_TRACE, BOUND, NOT, OR, RELATIVE_OPERATORS, SOURCES, TIME_OPERATORS, atom, bounds_of, build_base, buyer,
    expected_unit, group, is_relative_window, topic, when,
)

PROPS = settings(max_examples=400, deadline=None, derandomize=True,
                 suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
RULES = ((), ("year.inherited",), ("time.relative_to_previous",))
DATES = st.dates(min_value=date(1995, 1, 1), max_value=date(2099, 12, 31))


@dataclass(frozen=True)
class TimeSpec:
    cid: str
    start: date | None
    end: date | None  # exclusive
    excluded: bool
    source: fa.YearSource | None
    rules: tuple[str, ...]
    operator: str = "between_dates"


@dataclass(frozen=True)
class World:
    lower: object
    upper: object
    times: tuple[TimeSpec, ...]
    topics: int
    with_buyer: bool
    logic_kind: str  # none | or | not_first


@st.composite
def time_specs(draw, cid: str) -> TimeSpec:
    start = draw(DATES)
    kind = draw(st.sampled_from(["year", "month", "months", "rolling", "days", "from", "until"]))
    if kind == "rolling":  # as long as whole months, but starting on a day other than the 1st: not calendar months
        start = date(start.year, start.month, draw(st.integers(2, 28)))
        index = start.year * 12 + start.month - 1 + draw(st.integers(1, 40))
        end = date(index // 12, index % 12 + 1, start.day)
    elif kind == "year":
        start, end = date(start.year, 1, 1), date(start.year + draw(st.integers(1, 3)), 1, 1)
    elif kind in ("month", "months"):
        start = date(start.year, start.month, 1)
        count = 1 if kind == "month" else draw(st.integers(2, 40))
        index = start.year * 12 + start.month - 1 + count
        end = date(index // 12, index % 12 + 1, 1)
    elif kind == "days":
        end = start + timedelta(days=draw(st.integers(1, 900)))
    else:
        start, end = (start, None) if kind == "from" else (None, start)
    return TimeSpec(cid, start, end, draw(st.booleans()), draw(st.sampled_from(SOURCES)), draw(st.sampled_from(RULES)),
                    draw(st.sampled_from(TIME_OPERATORS)))


@st.composite
def worlds(draw) -> World:
    times = tuple(draw(time_specs(f"c{3 + i}")) for i in range(draw(st.integers(0, 3))))
    return World(draw(st.sampled_from(BOUND)), draw(st.sampled_from(BOUND)), times, draw(st.integers(0, 2)),
                 draw(st.booleans()), draw(st.sampled_from(["none", "or", "not_first"])))


def specs_of(world: World) -> list:
    specs = [topic(f"p{i}") for i in range(world.topics)] + ([buyer("b1")] if world.with_buyer else [])
    specs += bounds_of(world.lower, world.upper)
    specs += [when(t.start, t.end, cid=t.cid, excluded=t.excluded, source=t.source, rules=t.rules, operator=t.operator)
              for t in world.times]
    return specs


def logic_of(world: World, specs: list):
    ids = [spec.cid for spec in specs]
    if len(ids) < 2 or world.logic_kind == "none":
        return None, set()
    if world.logic_kind == "or":
        return group(OR, *map(atom, ids)), set(ids)
    return group(AND, group(NOT, atom(ids[0])), *map(atom, ids[1:])), {ids[0]}


def base_of_world(world: World, *, noise: int = 0, dual: bool = False):
    specs = specs_of(world)
    logic, inside = logic_of(world, specs)
    return build_base(specs, noise=noise, dual=dual, logic=logic), inside


# ---- the oracle, on the spec -------------------------------------------------------------------------------------------------
def months_between(start: date, end: date) -> int | None:
    if start.day != 1 or end.day != 1:
        return None
    return (end.year - start.year) * 12 + end.month - start.month


def oracle_time(world: World, inside: set[str]) -> tuple[tuple, tuple]:
    """(year view, referent view) from the spec's own dates, by the contract's §16.1/§16.3 clauses in their order."""
    if any(t.cid in inside for t in world.times):
        reason = ("AMBIGUOUS", "NONCONJUNCTIVE_TIME")
        return reason, reason
    if not world.times:  # §16.1 «Sin TIME previo»: the clock decides
        return ("ABSENT",), ("ABSENT",)
    positive = [t for t in world.times if not t.excluded]
    if not positive:  # HERA (ZEUS P2-1): an excluded period is a TIME previo that states no period
        reason = ("AMBIGUOUS", "NEGATIVE_TIME_ONLY")
        return reason, reason
    if len(positive) > 1:
        reason = ("AMBIGUOUS", "MULTIPLE_TIMES")
        return reason, reason
    only = positive[0]
    if is_relative_window(only.operator, only.source is fa.YearSource.RELATIVE_TO_CLOCK,
                          "time.relative_to_previous" in only.rules):
        reason = ("AMBIGUOUS", "RELATIVE_WINDOW")
        return reason, reason
    if only.start is None or only.end is None:
        reason = ("AMBIGUOUS", "OPEN_INTERVAL")
        return reason, reason
    first, last = only.start.year, (only.end - timedelta(days=1)).year
    year = ("PRESENT", first) if first == last else ("AMBIGUOUS", "MULTI_YEAR")
    months = months_between(only.start, only.end)
    referent = ("PRESENT", first, last, months) if months is not None else ("AMBIGUOUS", "INCOMPATIBLE_GRANULARITY")
    return year, referent


def views(result: inh.HostInheritanceInputs) -> tuple:
    year, referent = result.year, result.referent
    year_view = ((year.status.value, year.year) if year.status is inh.Presence.PRESENT else
                 (year.status.value, year.reason.value) if year.reason else (year.status.value,))
    referent_view = ((referent.status.value, referent.first_year, referent.last_year, referent.months)
                     if referent.status is inh.Presence.PRESENT else
                     (referent.status.value, referent.reason.value) if referent.reason else (referent.status.value,))
    return year_view, referent_view


@PROPS
@given(worlds(), st.booleans())
def test_p1_the_inputs_are_what_the_independent_oracle_says(world, dual):
    base, inside = base_of_world(world, dual=dual)
    result = inh.host_inheritance_inputs(base, base_trace_id=BASE_TRACE)
    status, unit, reason = expected_unit(world.lower, world.upper)
    assert (result.unit.status, result.unit.unit, result.unit.reason) == (status, unit, reason)
    if status.value == "PRESENT":  # the donors are exactly the bounds, in the base's order (lower before upper)
        assert result.unit.source_condition_ids == tuple(c.cid for c in bounds_of(world.lower, world.upper))
        assert result.unit.inherited_from == BASE_TRACE
    assert views(result) == oracle_time(world, inside)


@PROPS
@given(worlds(), st.integers(1, 9), st.integers(1, 9), st.booleans(), st.booleans())
def test_p2_nothing_but_the_conditions_reaches_the_inputs(world, noise_a, noise_b, dual_a, dual_b):
    """INV-M2A-07. Two builds of the same world that differ in origins, request ids, scope, datasets, subjects, authority,
    root and base trace, single or dual anchor, give exactly the same inputs: the base's bookkeeping is not an input."""
    first, _ = base_of_world(world, noise=noise_a, dual=dual_a)
    second, _ = base_of_world(world, noise=noise_b, dual=dual_b)
    assert inh.host_inheritance_inputs(first, base_trace_id=BASE_TRACE) == inh.host_inheritance_inputs(
        second, base_trace_id=BASE_TRACE)


@PROPS
@given(worlds(), st.text(alphabet="abcdefghij0123456789-", min_size=1, max_size=12))
def test_p2b_inherited_from_is_the_callers_and_never_the_bases(world, trace):
    """The record a value is inherited from is the one the caller names; no origin, root or chain of the base says it."""
    base, _ = base_of_world(world, noise=3)
    result = inh.host_inheritance_inputs(base, base_trace_id=trace)
    carried = {result.unit.inherited_from, result.year.inherited_from, result.referent.inherited_from} - {None}
    assert carried <= {trace}  # a variant that took it from an origin, the root or the chain would carry another id


@PROPS
@given(worlds(), st.booleans())
def test_p3_a_present_input_never_hides_two_units_several_times_or_several_years(world, dual):
    """INV-M2A-17 read off the DECODED base (a second, independent route): PRESENT only over exactly one of each."""
    base, _ = base_of_world(world, dual=dual)
    result = inh.host_inheritance_inputs(base, base_trace_id=BASE_TRACE)
    bounds = [c for c in base.conditions if c.dimension.value == "VALUE"]
    times = [c for c in base.conditions if c.dimension.value == "TIME"]
    positive = [c for c in times if c.polarity.value == "POSITIVE"]
    if result.unit.status is inh.Presence.PRESENT:
        assert bounds and {c.provenance.unit for c in bounds} == {result.unit.unit} and None not in {
            c.provenance.unit for c in bounds}
    if len({c.provenance.unit for c in bounds}) > 1:
        assert result.unit.status is inh.Presence.AMBIGUOUS
    for item, status in ((result.year, result.year.status), (result.referent, result.referent.status)):
        if status is inh.Presence.PRESENT:
            (only,) = positive  # exactly one positive TIME, and not a relative window by any of its three marks
            assert item.condition_id == only.condition_id and only.provenance.year_source is not (
                fa.YearSource.RELATIVE_TO_CLOCK)
            assert only.operator not in RELATIVE_OPERATORS and "time.relative_to_previous" not in only.receipt_rules
    assert (result.year.status is inh.Presence.ABSENT) == (not times)  # ABSENT is «no TIME at all», nothing less
    if len(positive) > 1:
        assert result.year.status is inh.Presence.AMBIGUOUS and result.referent.status is inh.Presence.AMBIGUOUS
    if result.year.status is inh.Presence.PRESENT:
        assert fa.time_years(positive[0]) == (result.year.year, result.year.year)  # never the first year of several


@PROPS
@given(worlds())
def test_p4_it_is_deterministic_and_never_touches_the_base(world):
    base, _ = base_of_world(world)
    before = base.to_dict()
    first = inh.host_inheritance_inputs(base, base_trace_id=BASE_TRACE)
    assert first == inh.host_inheritance_inputs(base, base_trace_id=BASE_TRACE)
    assert base.to_dict() == before


@PROPS
@given(DATES, DATES, st.sampled_from(["closed", "from", "until"]), st.sampled_from(["free", "aligned", "rolling"]),
       st.sampled_from(TIME_OPERATORS))
def test_p5_a_random_pair_of_dates_against_the_oracle_and_the_closed_codec(first, second, shape, alignment, operator):
    """Any interval (calendar months, whole months from a day other than the 1st, or free; one or several years; open
    ends) through the oracle; and the referent's value decoded with the closed types equals the base's TimeValue, whose
    ``months`` the calendar confirms."""
    if alignment == "aligned":
        first, second = date(first.year, first.month, 1), date(second.year, second.month, 1)
    elif alignment == "rolling":
        day = min(first.day, 28)
        first, second = date(first.year, first.month, day), date(second.year, second.month, day)
    low, high = min(first, second), max(first, second)
    assume(low < high)
    bounds = {"closed": (low, high), "from": (low, None), "until": (None, high)}[shape]
    world = World(None, None, (TimeSpec("c3", *bounds, False, None, (), operator),), 0, False, "none")
    base, inside = base_of_world(world)
    result = inh.host_inheritance_inputs(base, base_trace_id=BASE_TRACE)
    assert views(result) == oracle_time(world, inside)
    if result.referent.status is inh.Presence.PRESENT:
        value = decode_value(result.referent.value)
        assert isinstance(value, TimeValue) and (value.start, value.end_exclusive) == bounds
        index = value.start.year * 12 + value.start.month - 1 + result.referent.months
        assert date(index // 12, index % 12 + 1, 1) == value.end_exclusive  # the months land exactly on the end
        assert (result.referent.first_year, result.referent.last_year) == (
            value.start.year, (value.end_exclusive - timedelta(days=1)).year)
