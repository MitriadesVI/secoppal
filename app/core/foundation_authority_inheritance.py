"""M2A-3b: the host's SEPARATE INPUT for inheritance (contract §15.4, §16.1, §16.3; ADR-103 §4.3 and D10).

``host_inheritance_inputs(base, *, base_trace_id)`` derives, from the accepted base ALONE, the three typed inputs an
authority will receive through the entry only the host may call (never through ``resolve``: INV-M2A-07):

* ``UnitInheritance`` (§15.4, rule ``unit.inherited``): the one active unit of the base's VALUE bounds;
* ``YearInheritance`` (§16.1, rule ``year.inherited``): the one active civil year of the base's TIME;
* ``TimeReferent`` (§16.3, rule ``time.relative_to_previous``): the one active TIME, the typed referent of a period
  arithmetic.

Each input is exactly one of PRESENT (a single value and where it comes from), ABSENT (the base holds nothing to
inherit) or AMBIGUOUS (the base holds something that must NOT be inherited, and the closed reason). Nothing is ever
chosen among several: two units, several TIME conditions or several years are AMBIGUOUS, never «the first»
(INV-M2A-17).

What this module does NOT do (it is not a rule of receipt): it reads neither the turn's text nor the previous record nor
the anchor, calls no authority, reads no clock, converts nothing, does no period arithmetic and does not decide whether
the expression of a new turn fits the referent. ``base_trace_id`` is the id of the record whose effective is the base
(the fact ``compose`` receives too): provenance (``Provenance.inherited_from``), never meaning.

Declared choices (``docs/audits/m2a_conversation/M2A3B_CLOSE.md``, M2A3B-D1..D9): a relative window is a TIME with the
operator ``relative_period`` (§16.4), or whose ``year_source`` is ``RELATIVE_TO_CLOCK``, or whose receipt is
``time.relative_to_previous`` (D4); a TIME inside an OR/NOT group is not an active period, and a base with only
negative TIME is not a base without TIME (D3); a referent fits period arithmetic only as a whole number of calendar
months, and whether it fits the new expression is the authority's, from ``months`` (D6). Values are read only through
the accessors of ``foundation_accepted`` (R1, D8); the one exception is the two date fields of the canonical ``time@1``
encoding, read strictly and cross-checked against ``time_years`` (D7).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum

from app.core.foundation_accepted import (
    AcceptedCondition, AcceptedInterpretation, Unit, YearSource, time_years,
)
from app.core.interpretation_contract import ContractError, Polarity
from app.core.semantic_dimensions import SemanticDimension

RELATIVE_TO_PREVIOUS_RULE = "time.relative_to_previous"
RELATIVE_PERIOD_OPERATOR = "relative_period"  # §16.4: the TIME operator of a window relative to the clock
_TIME_TAG, _START, _END, _OPEN = "time@1", "start=", "end_exclusive=", "!"  # «!»: the codec's open end


class Presence(str, Enum):
    PRESENT = "PRESENT"
    ABSENT = "ABSENT"
    AMBIGUOUS = "AMBIGUOUS"


class Ambiguity(str, Enum):
    """Why the base holds something that must not be inherited (closed; the codes name causes, not user messages)."""

    MULTIPLE_UNITS = "MULTIPLE_UNITS"  # §15.4: two or more active units («una única unidad activa inequívoca»)
    UNIT_UNRECORDED = "UNIT_UNRECORDED"  # §15.4: a VALUE bound whose unit the base does not record
    NONCONJUNCTIVE_TIME = "NONCONJUNCTIVE_TIME"  # a TIME inside an OR/NOT group is not an active period
    NEGATIVE_TIME_ONLY = "NEGATIVE_TIME_ONLY"  # §16.1: it excludes a period and states none: not «sin TIME previo»
    MULTIPLE_TIMES = "MULTIPLE_TIMES"  # §16.1, §16.3: several TIME conditions («varios referentes»)
    RELATIVE_WINDOW = "RELATIVE_WINDOW"  # §16.1, §16.3: the TIME is a relative window («ventana relativa»)
    OPEN_INTERVAL = "OPEN_INTERVAL"  # one end of the TIME is open: no year and no period to shift
    MULTI_YEAR = "MULTI_YEAR"  # §16.1, §15.5: the TIME spans several civil years («plurianual»)
    INCOMPATIBLE_GRANULARITY = "INCOMPATIBLE_GRANULARITY"  # §16.3: not a whole number of calendar months


UNIT_AMBIGUITIES = frozenset({Ambiguity.MULTIPLE_UNITS, Ambiguity.UNIT_UNRECORDED})
_TIME_AMBIGUITIES = frozenset({Ambiguity.NONCONJUNCTIVE_TIME, Ambiguity.NEGATIVE_TIME_ONLY, Ambiguity.MULTIPLE_TIMES,
                               Ambiguity.RELATIVE_WINDOW, Ambiguity.OPEN_INTERVAL})
YEAR_AMBIGUITIES = _TIME_AMBIGUITIES | {Ambiguity.MULTI_YEAR}
REFERENT_AMBIGUITIES = _TIME_AMBIGUITIES | {Ambiguity.INCOMPATIBLE_GRANULARITY}


def _fail(code: str):
    raise ContractError(code)


def _text_ok(value) -> bool:
    return type(value) is str and value != ""


def _int_ok(value, low: int) -> bool:
    return type(value) is int and value >= low


def _shape(status, reason, allowed, payload: bool, complete: bool) -> None:
    """The one discriminated shape of an input: PRESENT carries its whole payload and no reason, ABSENT nothing,
    AMBIGUOUS a reason of its own input and no payload."""
    if type(status) is not Presence:
        _fail("INHERITANCE_INPUT_STATUS")
    if status is Presence.PRESENT:
        if reason is not None or not complete:
            _fail("INHERITANCE_INPUT_PRESENT_SHAPE")
    elif status is Presence.ABSENT:
        if reason is not None or payload:
            _fail("INHERITANCE_INPUT_ABSENT_SHAPE")
    elif type(reason) is not Ambiguity or reason not in allowed or payload:
        _fail("INHERITANCE_INPUT_AMBIGUOUS_SHAPE")


# ---- the canonical ``time@1`` tuple (D7: the one reading beyond the accessors of ``foundation_accepted``) -----------
def _one_date(raw: str) -> date | None:
    try:
        day = None if raw == _OPEN else date.fromisoformat(raw)
    except ValueError:
        _fail("TIME_VALUE_UNREADABLE")
    if day is not None and day.isoformat() != raw:  # only the canonical spelling of the codec
        _fail("TIME_VALUE_UNREADABLE")
    return day


def _time_dates(value) -> tuple[date | None, date | None]:
    """The two dates of a canonical ``time@1`` tuple (``None``: an open end); anything else is unreadable."""
    shape = (type(value) is tuple and len(value) == 5 and all(type(item) is str for item in value)
             and value[0] == _TIME_TAG and value[1].startswith(_START) and value[2].startswith(_END)
             and value[3].startswith("precision=") and value[4] in ("excluded=0", "excluded=1"))
    if not shape:
        _fail("TIME_VALUE_UNREADABLE")
    return _one_date(value[1][len(_START):]), _one_date(value[2][len(_END):])


def _whole_months(first: date, after: date) -> int | None:
    """The calendar months from ``first`` to ``after``, or None unless both fall on the first of a month."""
    if first.day != 1 or after.day != 1:
        return None
    return (after.year - first.year) * 12 + after.month - first.month


def _referent_value_ok(value, first_year, last_year, months) -> bool:
    """A PRESENT referent's ``value`` is a canonical ``time@1`` of an included period whose own dates give exactly the
    years and the whole months the referent states (``months >= 1`` makes the interval non-empty and ordered): its
    numbers cannot disagree with its value."""
    try:
        first, after = _time_dates(value)
    except ContractError:
        return False
    return (first is not None and after is not None and value[4] == "excluded=0"
            and (first.year, (after - timedelta(days=1)).year) == (first_year, last_year)
            and _whole_months(first, after) == months)


@dataclass(frozen=True, slots=True)
class UnitInheritance:
    """§15.4. PRESENT: the one active ``unit`` (``unit_source = PREVIOUS_ACCEPTED_VALUE``), the record it is inherited
    from (``Provenance.inherited_from``) and the VALUE conditions of the base that carry it."""

    status: Presence
    unit: Unit | None = None
    inherited_from: str | None = None
    source_condition_ids: tuple[str, ...] = ()
    reason: Ambiguity | None = None

    def __post_init__(self) -> None:
        ids = self.source_condition_ids
        ids_ok = type(ids) is tuple and bool(ids) and all(_text_ok(i) for i in ids) and len(set(ids)) == len(ids)
        _shape(self.status, self.reason, UNIT_AMBIGUITIES, (self.unit, self.inherited_from, ids) != (None, None, ()),
               type(self.unit) is Unit and _text_ok(self.inherited_from) and ids_ok)


@dataclass(frozen=True, slots=True)
class YearInheritance:
    """§16.1. PRESENT: the one active civil ``year`` of the base's TIME (``year_source = PREVIOUS_ACCEPTED_TIME``), the
    TIME condition that carries it and the record it is inherited from."""

    status: Presence
    year: int | None = None
    condition_id: str | None = None
    inherited_from: str | None = None
    reason: Ambiguity | None = None

    def __post_init__(self) -> None:
        payload = (self.year, self.condition_id, self.inherited_from) != (None, None, None)
        _shape(self.status, self.reason, YEAR_AMBIGUITIES, payload,
               _int_ok(self.year, 1) and _text_ok(self.condition_id) and _text_ok(self.inherited_from))


@dataclass(frozen=True, slots=True)
class TimeReferent:
    """§16.3. PRESENT: the previous TIME as a typed referent. ``value`` is its canonical ``time@1`` encoding (the
    authority decodes it with its own closed types: this module builds no value); ``first_year``/``last_year`` the civil
    years it touches and ``months`` its whole number of calendar months (a year: 12 and ``first_year == last_year``)."""

    status: Presence
    condition_id: str | None = None
    value: tuple[str, ...] = ()
    first_year: int | None = None
    last_year: int | None = None
    months: int | None = None
    inherited_from: str | None = None
    reason: Ambiguity | None = None

    def __post_init__(self) -> None:
        years_ok = _int_ok(self.first_year, 1) and _int_ok(self.last_year, 1) and self.first_year <= self.last_year
        fields = (self.condition_id, self.value, self.first_year, self.last_year, self.months, self.inherited_from)
        payload = fields != (None, (), None, None, None, None)
        _shape(self.status, self.reason, REFERENT_AMBIGUITIES, payload,
               _text_ok(self.condition_id) and years_ok and _int_ok(self.months, 1)
               and _referent_value_ok(self.value, self.first_year, self.last_year, self.months)
               and _text_ok(self.inherited_from))


@dataclass(frozen=True, slots=True)
class HostInheritanceInputs:
    """The separate entry of the host's inputs: the three of one base, derived together from it and nothing else.

    The type refuses to join inputs that say they come from different bases: the PRESENT inputs name the one record they
    are inherited from, and a year and a referent that are both PRESENT are the same TIME condition. Limited guarantee
    (M2A3B-D1): an ABSENT or AMBIGUOUS input carries no origin, so a mix of those with a PRESENT input of another
    base is not detectable here; ``host_inheritance_inputs`` is the way to build all three together."""

    unit: UnitInheritance
    year: YearInheritance
    referent: TimeReferent

    def __post_init__(self) -> None:
        if (type(self.unit) is not UnitInheritance or type(self.year) is not YearInheritance
                or type(self.referent) is not TimeReferent):
            _fail("INHERITANCE_INPUTS_TYPES")
        origins = {part.inherited_from for part in (self.unit, self.year, self.referent)} - {None}
        if len(origins) > 1:
            _fail("INHERITANCE_INPUTS_FROM_DIFFERENT_BASES")
        if (self.year.status is Presence.PRESENT and self.referent.status is Presence.PRESENT
                and self.year.condition_id != self.referent.condition_id):
            _fail("INHERITANCE_INPUTS_FROM_DIFFERENT_TIMES")


# ---- reading the base ---------------------------------------------------------------------------------------------
def _check(base, base_trace_id) -> None:
    if type(base) is not AcceptedInterpretation:
        _fail("INHERITANCE_BASE_NOT_ACCEPTED")
    if not _text_ok(base_trace_id):
        _fail("INHERITANCE_BASE_TRACE_ID")


def _is_relative(condition: AcceptedCondition) -> bool:
    """A relative window (§16.1, §16.3, §16.4, M2A3B-D4): the TIME operator ``relative_period`` (§16.4: what the
    TIME authority emits for «este año», «últimos seis meses»), or a window resolved against the clock or the
    previous TIME."""
    return (condition.operator == RELATIVE_PERIOD_OPERATOR
            or condition.provenance.year_source is YearSource.RELATIVE_TO_CLOCK
            or RELATIVE_TO_PREVIOUS_RULE in condition.receipt_rules)


def _extent(condition: AcceptedCondition) -> tuple[int, int, int | None] | None:
    """``(first_year, last_year, whole_months)`` of a TIME; None if an end is open; ``whole_months`` is None when an end
    does not fall on the first of a month. The years are ``time_years``'; the dates are read only to count months."""
    years = time_years(condition)
    if years is None:
        return None
    first, after = _time_dates(condition.value)
    if first is None or after is None or years != (first.year, (after - timedelta(days=1)).year):
        _fail("TIME_YEARS_DISAGREE")
    return years[0], years[1], _whole_months(first, after)


def _active_time(base: AcceptedInterpretation) -> AcceptedCondition | Ambiguity | None:
    """The base's one active TIME: the condition; ``None`` if the base has no TIME at all; or why there is not one."""
    times = [c for c in base.conditions if c.dimension is SemanticDimension.TIME]
    if base.logic is not None and any(base.logic.has_nonconjunctive_atom(c.condition_id) for c in times):
        return Ambiguity.NONCONJUNCTIVE_TIME
    positive = [c for c in times if c.polarity is Polarity.POSITIVE]
    if len(positive) > 1:
        return Ambiguity.MULTIPLE_TIMES
    if not positive:
        return Ambiguity.NEGATIVE_TIME_ONLY if times else None
    return Ambiguity.RELATIVE_WINDOW if _is_relative(positive[0]) else positive[0]


def unit_inheritance(base: AcceptedInterpretation, *, base_trace_id: str) -> UnitInheritance:
    """§15.4: a bare amount inherits a unit only if the base holds ONE active unit, SMMLV included (A1, A2)."""
    _check(base, base_trace_id)
    bounds = [c for c in base.conditions if c.dimension is SemanticDimension.VALUE]
    if not bounds:
        return UnitInheritance(Presence.ABSENT)
    units = {c.provenance.unit for c in bounds if c.provenance.unit is not None}
    if len(units) > 1:
        return UnitInheritance(Presence.AMBIGUOUS, reason=Ambiguity.MULTIPLE_UNITS)
    if any(c.provenance.unit is None for c in bounds):
        return UnitInheritance(Presence.AMBIGUOUS, reason=Ambiguity.UNIT_UNRECORDED)
    (unit,) = units
    return UnitInheritance(Presence.PRESENT, unit, base_trace_id, tuple(c.condition_id for c in bounds))


def year_inheritance(base: AcceptedInterpretation, *, base_trace_id: str) -> YearInheritance:
    """§16.1: a month without a year inherits the ONE active year of the base's TIME; ABSENT is the host's clock's."""
    _check(base, base_trace_id)
    active = _active_time(base)
    if active is None:
        return YearInheritance(Presence.ABSENT)
    if type(active) is Ambiguity:
        return YearInheritance(Presence.AMBIGUOUS, reason=active)
    extent = _extent(active)
    if extent is None:
        return YearInheritance(Presence.AMBIGUOUS, reason=Ambiguity.OPEN_INTERVAL)
    if extent[0] != extent[1]:
        return YearInheritance(Presence.AMBIGUOUS, reason=Ambiguity.MULTI_YEAR)
    return YearInheritance(Presence.PRESENT, extent[0], active.condition_id, base_trace_id)


def time_referent(base: AcceptedInterpretation, *, base_trace_id: str) -> TimeReferent:
    """§16.3: a relative expression resolves against the ONE active TIME of the base, if period arithmetic applies."""
    _check(base, base_trace_id)
    active = _active_time(base)
    if active is None:
        return TimeReferent(Presence.ABSENT)
    if type(active) is Ambiguity:
        return TimeReferent(Presence.AMBIGUOUS, reason=active)
    extent = _extent(active)
    if extent is None:
        return TimeReferent(Presence.AMBIGUOUS, reason=Ambiguity.OPEN_INTERVAL)
    if extent[2] is None:
        return TimeReferent(Presence.AMBIGUOUS, reason=Ambiguity.INCOMPATIBLE_GRANULARITY)
    return TimeReferent(Presence.PRESENT, active.condition_id, active.value, extent[0], extent[1], extent[2],
                        base_trace_id)


def host_inheritance_inputs(base: AcceptedInterpretation, *, base_trace_id: str) -> HostInheritanceInputs:
    """The separate entry of the host's inputs for one base (contract §15.4, §16.1, §16.3)."""
    return HostInheritanceInputs(unit_inheritance(base, base_trace_id=base_trace_id),
                                 year_inheritance(base, base_trace_id=base_trace_id),
                                 time_referent(base, base_trace_id=base_trace_id))
