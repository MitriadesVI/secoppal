"""M2A-7: ``echo_items`` rule by rule (contract §11, INV-M2A-12, INV-M2A-21), over hand-written typed turns.

Every scenario is a ``Case``: the four arguments of ``echo_items`` (built with the REAL ``compose``, never with the
echo) and the EXACT list of echo items the contract says that turn owes: ``(class, slot key, condition ids)``, in the
order of §11 then §4.2. ``test_every_scenario`` judges the module with them; ``tests/test_foundation_echo_mutants.py``
judges every mutant of the module with the same cases (a mutant must make at least one case fail), and
``tests/test_foundation_echo_properties.py`` repeats the claim over generated turns.

Derived from the types, not by hand: the provenance table is enumerated from ``UnitSource`` / ``YearSource`` /
``OrderDirectionSource`` and the classes from ``EchoClass`` (a new member fails until someone decides it).
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone

import pytest

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_echo as fe
from app.core.foundation_authority_values import (
    BuyerKind, BuyerValue, CardinalityBasis, CardinalityValue, Comparator, MoneyValue, OrderDirection, OrderKey,
    OrderValue, TimeValue, TopicValue,
)
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import ContractError, Polarity, ValueSide
from app.core.semantic_dimensions import SemanticDimension as D

T0 = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
RAW_BASE, RAW_TURN, PLAN = "a" * 64, "b" * 64, "c" * 64
POS, NEG = Polarity.POSITIVE, Polarity.NEGATIVE
LOWER, UPPER = fa.Slot(D.VALUE, side=ValueSide.LOWER), fa.Slot(D.VALUE, side=ValueSide.UPPER)
EXPECTED_STEP = {fa.TurnResolution.NEW_SEARCH: fa.TurnScope.IN_NEW_SEARCH, fa.TurnResolution.PROTOCOL_PAGE: fa.TurnScope.IN_PROTOCOL,
                 fa.TurnResolution.PROTOCOL_EVENT: fa.TurnScope.IN_PROTOCOL}


# ---- typed building blocks -----------------------------------------------------------------------------------------
def span_origin(trace: str, n: int, raw: str = RAW_TURN) -> fa.ConditionOrigin:
    return fa.ConditionOrigin(fa.OriginKind.SPAN, trace, raw, (3 * n, 3 * n + 2))


def event_origin(kind: fa.OriginKind = fa.OriginKind.VIEW_ACTION) -> fa.ConditionOrigin:
    return fa.ConditionOrigin(kind, "t1", event_id="evt-1")


def year(first: int, excluded: bool = False) -> TimeValue:
    return TimeValue(date(first, 1, 1), date(first + 1, 1, 1), "year", excluded)


def operator_of(dimension: D, value) -> str:
    if dimension is D.VALUE:
        return fa.VALUE_OPERATOR_OF[value.comparator]
    if dimension is D.ORDER:
        return f"{value.key.value}_{value.direction.value}"
    return {D.CARDINALITY: "top_n", D.TOPIC: "contains", D.TIME: "during_year"}.get(dimension, "is")


def cond(dimension: D, value, cid: str, *, trace: str = "t0", n: int = 0, provenance=None, rules=(), rids=("r1",),
         origin=None) -> fa.AcceptedCondition:
    encoded = value.encode()
    slot = fa.slot_of(dimension, encoded)
    return fa.AcceptedCondition(
        cid, tuple(sorted(rids)), slot, dimension, operator_of(dimension, value), slot.polarity or POS, None, encoded,
        "test_authority@1", tuple(sorted(rules)), (), origin or span_origin(trace, n, RAW_BASE if trace == "t0" else RAW_TURN),
        provenance or fa.Provenance())


def topic(cid: str, phrase: str, *, excluded: bool = False, **kw) -> fa.AcceptedCondition:
    return cond(D.TOPIC, TopicValue(phrase, False, excluded), cid, **kw)


def buyer(cid: str, name: str = "ICBF", **kw) -> fa.AcceptedCondition:
    return cond(D.BUYER, BuyerValue(name, BuyerKind.UNIT, (name,), None, False), cid, **kw)


def time(cid: str, first: int = 2025, **kw) -> fa.AcceptedCondition:
    return cond(D.TIME, year(first), cid, **kw)


def money(cid: str, millions: int, comparator: Comparator, *, unit_source=fa.UnitSource.STATED, inherited_from=None,
          **kw) -> fa.AcceptedCondition:
    provenance = fa.Provenance(unit=fa.Unit.MILLONES, source_amount=str(millions), unit_source=unit_source,
                               inherited_from=inherited_from)
    return cond(D.VALUE, MoneyValue(millions * 1_000_000, comparator), cid, provenance=provenance, **kw)


def order(cid: str, *, key: OrderKey = OrderKey.VALUE, source=None, rules=(), **kw) -> fa.AcceptedCondition:
    provenance = fa.Provenance(order_direction_source=source)
    return cond(D.ORDER, OrderValue(key, OrderDirection.DESC), cid, provenance=provenance, rules=rules, **kw)


def top(cid: str, n_requested: int = 10, *, rules=(), **kw) -> fa.AcceptedCondition:
    value = CardinalityValue(min(n_requested, 100), CardinalityBasis.FIGURE, n_requested)
    return cond(D.CARDINALITY, value, cid, rules=rules, **kw)


def interp(*conditions: fa.AcceptedCondition, subject: Subject = Subject.CONTRACTS, dual: bool = False,
           trace: str | None = None) -> fa.AcceptedInterpretation:
    subjects = (Subject.PROCESSES, Subject.CONTRACTS) if dual else (subject,)
    requests = tuple(fa.AcceptedRequest(f"r{i}", "s1", s, fa.UniverseKind.RECORDS, span_origin("t0", 0, RAW_BASE), None,
                                        fd.SUBJECT_DATASET[s]) for i, s in enumerate(subjects, 1))
    rids = tuple(r.request_id for r in requests)
    return fa.canonical_interpretation(requests, tuple(replace(c, request_ids=rids) for c in conditions), None, (),
                                       trace, "t-root")


def set_(condition: fa.AcceptedCondition) -> fa.PatchOp:
    return fa.PatchOp(fa.PatchOpKind.SET, condition.slot, condition, None, condition.origin)


def set_order(condition: fa.AcceptedCondition) -> fa.PatchOp:
    return fa.PatchOp(fa.PatchOpKind.SET_ORDER, condition.slot, condition, None, condition.origin)


def clear(slot: fa.Slot) -> fa.PatchOp:
    return fa.PatchOp(fa.PatchOpKind.CLEAR, slot, None, None, span_origin("t1", 9))


def keep(slot: fa.Slot) -> fa.PatchOp:
    return fa.PatchOp(fa.PatchOpKind.KEEP, slot, None, None, span_origin("t1", 8))


def universe(subject: Subject, kind: fa.UniverseKind = fa.UniverseKind.RECORDS) -> fa.PatchOp:
    return fa.PatchOp(fa.PatchOpKind.REPLACE_UNIVERSE, fa.UNIVERSE_SLOT, None, fa.UniverseTarget(subject, kind),
                      span_origin("t1", 7))


def wide(dimension: D) -> fa.Slot:
    return fa.Slot(dimension)


def expiry_pending(frame, patch) -> fa.PendingClarification:
    return fa.PendingClarification("p1", "t1", T0, T0 + timedelta(minutes=30), 1, frame, "t0", None, None,
                                   "CONTEXT_EXPIRED_CONFIRM",
                                   (fa.CandidatePatch("k1", "continue", patch),), frozenset(patch.slots))


def make_record(resolution: fa.TurnResolution, *, patch=None, effective=None, base_trace_id=None, pending=None,
                echo=()) -> fa.AcceptedTurnRecord:
    """A coherent record: the turn executes (an ``ExecutionReceipt``) only if its resolution does."""
    executes = resolution in fe.EXECUTING and effective is not None
    scope = EXPECTED_STEP.get(resolution, fa.TurnScope.IN)
    return fa.AcceptedTurnRecord(
        "t1", "chat-1", "turn-1", T0, T0, "foundation-v5", None if resolution is fa.TurnResolution.PROTOCOL_EVENT else RAW_TURN,
        scope, resolution, None, base_trace_id, patch, effective,
        fa.ExecutionReceipt(True, True, True, (PLAN,)) if executes else None, None, pending, tuple(echo),
        "t1" if executes or resolution is fa.TurnResolution.PROTOCOL_PAGE else "t0")


def summary(items) -> list[tuple]:
    """``(class, slot key, ids)`` per item; the display key must be the class (and the slot's key when there is one)."""
    out = []
    for item in items:
        slot_key = None if item.slot is None else item.slot.key
        assert item.display_key == (item.echo_class.value if slot_key is None else f"{item.echo_class.value}:{slot_key}")
        out.append((item.echo_class.value, slot_key, item.condition_ids))
    return out


def E(echo_class: str, slot: str | None, *ids: str) -> tuple:
    return echo_class, slot, tuple(ids)


@dataclass(frozen=True)
class Case:
    name: str
    args: tuple
    kwargs: dict
    expected: list

    def run(self, echo_items=fe.echo_items) -> list[tuple]:
        return summary(echo_items(*self.args, **self.kwargs))


def delta(name, base, ops, expected, *, expired=False, resolution=fa.TurnResolution.DELTA,
          kind=fa.PatchOriginKind.TEXT) -> Case:
    patch = fa.SemanticPatch(tuple(ops), kind)
    composed = fd.compose(base, patch, base_trace_id="t0")
    assert isinstance(composed, fd.Composed), f"{name}: the reducer refused {composed}"
    record = make_record(resolution, patch=patch, effective=composed.effective, base_trace_id="t0")
    return Case(name, (base, composed.effective, patch, record), {"anchor_expired": expired}, expected)


def fresh(name, effective, expected, *, base=None, expired=False) -> Case:
    record = make_record(fa.TurnResolution.NEW_SEARCH, effective=effective)
    return Case(name, (base, effective, None, record), {"anchor_expired": expired}, expected)


def not_ready(name, base, ops, expected, *, expired=False) -> Case:
    """A DELTA that did not reach READY: its record has a patch and no effective, so it executes nothing (§3.1)."""
    patch = fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT)
    record = make_record(fa.TurnResolution.DELTA, patch=patch, effective=None, base_trace_id="t0")
    return Case(name, (base, None, patch, record), {"anchor_expired": expired}, expected)


def silent(name, resolution, expected, *, base=None, expired=False, pending=None) -> Case:
    record = make_record(resolution, base_trace_id="t0" if resolution is fa.TurnResolution.PROTOCOL_PAGE else None,
                         pending=pending)
    return Case(name, (base, None, None, record), {"anchor_expired": expired}, expected)


# ---- the scenarios ---------------------------------------------------------------------------------------------------
BASE_BT = interp(buyer("c1"), time("c2"), trace="t0")  # ICBF, 2025, contracts
CARRY_BT = [E("CARRIED", "BUYER/POSITIVE", "c1"), E("CARRIED", "TIME/POSITIVE", "c2"), E("CARRIED", "UNIVERSE")]
UNI = E("CARRIED", "UNIVERSE")
RANGE = interp(money("c1", 100, Comparator.GTE), money("c2", 900, Comparator.LT), trace="t0")
ORDERED = interp(order("c1"), top("c2"), trace="t0")
TOPIC_BASE = interp(topic("c1", "aseo"), trace="t0")


def _cases() -> list[Case]:
    out = [
        # CARRIED: what survives untouched, a KEEP included, and the said universe
        delta("carried", BASE_BT, [set_(topic("p1", "aseo", trace="t1", n=1))], [*CARRY_BT]),
        delta("carried_keep", BASE_BT, [keep(fa.Slot(D.TIME, POS)), set_(topic("p1", "aseo", trace="t1", n=1))], [*CARRY_BT]),
        delta("carried_not_the_cleared", BASE_BT, [clear(wide(D.TIME))], [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI]),
        delta("carried_not_the_replaced", BASE_BT, [set_(time("p1", 2026, trace="t1", n=1))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI]),
        delta("carried_each_condition_of_a_slot", interp(topic("c1", "aseo"), topic("c2", "vias"), trace="t0"),
              [set_(buyer("p1", trace="t1", n=1))],
              [E("CARRIED", "TOPIC/POSITIVE", "c1"), E("CARRIED", "TOPIC/POSITIVE", "c2"), UNI]),
        delta("carried_dual_anchor", interp(buyer("c1"), dual=True, trace="t0"), [set_(topic("p1", "aseo", trace="t1", n=1))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI]),
        delta("carried_provenance_was_announced_by_the_turn_that_produced_it", interp(
            money("c1", 100, Comparator.GTE, unit_source=fa.UnitSource.PREVIOUS_ACCEPTED_VALUE, inherited_from="t-0"),
            time("c2", provenance=fa.Provenance(year_source=fa.YearSource.CLOCK_INFERRED)), trace="t0"),
              [set_(topic("p1", "aseo", trace="t1", n=1))],
              [E("CARRIED", "TIME/POSITIVE", "c2"), E("CARRIED", "VALUE/lower", "c1"), UNI]),
        delta("clear_of_a_whole_dimension_covers_its_negative_slot", interp(topic("c1", "obras", excluded=True), trace="t0"),
              [clear(wide(D.TOPIC))], [UNI]),
        # the provenance of what the turn placed
        delta("unit_inherited", RANGE, [clear(wide(D.VALUE)), set_(money("p1", 100, Comparator.GTE, trace="t1", n=1,
              unit_source=fa.UnitSource.PREVIOUS_ACCEPTED_VALUE, inherited_from="t0"))],
              [UNI, E("UNIT_INHERITED", "VALUE/lower", "p1"), E("VALUE_RANGE_REPLACED", "VALUE/upper", "c2", "p1")]),
        delta("unit_shared_in_range", BASE_BT, [
            set_(money("p1", 100, Comparator.GTE, trace="t1", n=1, unit_source=fa.UnitSource.SIBLING_IN_RANGE)),
            set_(money("p2", 900, Comparator.LTE, trace="t1", n=2))],
              [*CARRY_BT, E("UNIT_SHARED_IN_RANGE", "VALUE", "p1", "p2")]),
        delta("unit_stated_is_silent", BASE_BT, [set_(money("p1", 100, Comparator.GTE, trace="t1", n=1))], [*CARRY_BT]),
    ]
    for name, source, rules, classes in (
            ("year_inherited", fa.YearSource.PREVIOUS_ACCEPTED_TIME, (), ["YEAR_INHERITED"]),
            ("year_clock_inferred", fa.YearSource.CLOCK_INFERRED, (), ["YEAR_CLOCK_INFERRED"]),
            ("relative_window_clock", fa.YearSource.RELATIVE_TO_CLOCK, (), ["RELATIVE_WINDOW_RESOLVED"]),
            ("relative_window_previous", fa.YearSource.PREVIOUS_ACCEPTED_TIME, (fe.RULE_RELATIVE_TO_PREVIOUS,),
             ["YEAR_INHERITED", "RELATIVE_WINDOW_RESOLVED"]),
            ("year_stated_is_silent", fa.YearSource.STATED, (), [])):
        inherited = "t0" if source is fa.YearSource.PREVIOUS_ACCEPTED_TIME else None
        placed = time("p1", 2026, trace="t1", n=1, rules=rules,
                      provenance=fa.Provenance(year_source=source, inherited_from=inherited))
        out.append(delta(name, BASE_BT, [set_(placed)], [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI,
                                                         *[E(c, "TIME/POSITIVE", "p1") for c in classes]]))
    out += [
        # VALUE_RANGE_REPLACED: A8.1, or a whole-VALUE CLEAR followed by a SET that does not restore a side
        delta("value_range_a81", interp(money("c1", 500, Comparator.LT), trace="t0"),
              [set_(money("p1", 900, Comparator.GTE, trace="t1", n=1))], [UNI, E("VALUE_RANGE_REPLACED", "VALUE/upper", "c1", "p1")]),
        delta("value_range_clear_set", RANGE, [clear(wide(D.VALUE)), set_(money("p1", 200, Comparator.GTE, trace="t1", n=1))],
              [UNI, E("VALUE_RANGE_REPLACED", "VALUE/upper", "c2", "p1")]),
        delta("value_range_clear_set_one_side_base", interp(money("c1", 100, Comparator.GTE), trace="t0"),
              [clear(wide(D.VALUE)), set_(money("p1", 800, Comparator.LT, trace="t1", n=1))],
              [UNI, E("VALUE_RANGE_REPLACED", "VALUE/lower", "c1", "p1")]),
        delta("value_range_clear_alone_is_not_a_replacement", RANGE, [clear(wide(D.VALUE))], [UNI]),
        delta("value_range_clear_of_one_side_is_not_a_replacement", RANGE,
              [clear(LOWER), set_(money("p1", 800, Comparator.LT, trace="t1", n=1))], [UNI]),
        delta("value_range_both_sides_set", RANGE, [clear(wide(D.VALUE)),
              set_(money("p1", 200, Comparator.GTE, trace="t1", n=1)), set_(money("p2", 800, Comparator.LT, trace="t1", n=2))],
              [UNI]),
        delta("value_range_no_collision_keeps_the_opposite_bound", interp(money("c1", 900, Comparator.LT), trace="t0"),
              [set_(money("p1", 100, Comparator.GTE, trace="t1", n=1))], [E("CARRIED", "VALUE/upper", "c1"), UNI]),
        # TOPIC_REPLACED, POLARITY_INVERTED
        delta("topic_replaced", TOPIC_BASE, [set_(topic("p1", "vias", trace="t1", n=1))],
              [UNI, E("TOPIC_REPLACED", "TOPIC/POSITIVE", "c1", "p1")]),
        delta("topic_restated_is_not_replaced", TOPIC_BASE, [set_(topic("p1", "aseo", trace="t1", n=1))], [UNI]),
        delta("topic_clear_then_set_is_replaced", TOPIC_BASE, [clear(wide(D.TOPIC)), set_(topic("p1", "vias", trace="t1", n=1))],
              [UNI, E("TOPIC_REPLACED", "TOPIC/POSITIVE", "c1", "p1")]),
        delta("topic_negative_is_not_a_replacement", TOPIC_BASE, [set_(topic("p1", "obras", excluded=True, trace="t1", n=1))],
              [E("CARRIED", "TOPIC/POSITIVE", "c1"), UNI]),
        delta("topic_cleared_and_a_negative_set_is_not_a_replacement", TOPIC_BASE,
              [clear(fa.Slot(D.TOPIC, POS)), set_(topic("p1", "obras", excluded=True, trace="t1", n=1))], [UNI]),
        delta("polarity_inverted", TOPIC_BASE, [set_(topic("p1", "aseo", excluded=True, trace="t1", n=1))],
              [UNI, E("POLARITY_INVERTED", "TOPIC/NEGATIVE", "c1", "p1")]),
        delta("polarity_cited_by_a_clear_is_not_an_inversion", TOPIC_BASE,
              [clear(wide(D.TOPIC)), set_(topic("p1", "aseo", excluded=True, trace="t1", n=1))], [UNI]),
        # UNIVERSE_CHANGED (with the carried TIME), and a restated universe
        delta("universe_changed", BASE_BT, [universe(Subject.PROCESSES)],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), E("CARRIED", "TIME/POSITIVE", "c2"), E("UNIVERSE_CHANGED", "UNIVERSE", "c2")]),
        delta("universe_changed_without_time", interp(buyer("c1"), trace="t0"), [universe(Subject.PROCESSES)],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), E("UNIVERSE_CHANGED", "UNIVERSE")]),
        delta("universe_restated_changes_nothing", BASE_BT, [universe(Subject.CONTRACTS)],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), E("CARRIED", "TIME/POSITIVE", "c2")]),
        delta("universe_dual_reduced", interp(buyer("c1"), time("c2"), dual=True, trace="t0"), [universe(Subject.CONTRACTS)],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), E("CARRIED", "TIME/POSITIVE", "c2"), E("UNIVERSE_CHANGED", "UNIVERSE", "c2")]),
        # TOPN_CARRIED (F10), TOPN_CAPPED (P8 receipt), ORDER_DIRECTION_DEFAULTED (F21)
        delta("topn_carried", ORDERED, [set_order(order("p1", key=OrderKey.DATE, trace="t1", n=1))],
              [E("CARRIED", "CARDINALITY", "c2"), UNI, E("TOPN_CARRIED", "CARDINALITY", "c2")]),
        delta("topn_not_carried_when_set_in_the_same_turn", ORDERED,
              [set_order(order("p1", key=OrderKey.DATE, trace="t1", n=1)), set_(top("p2", 5, trace="t1", n=2))], [UNI]),
        delta("topn_not_carried_without_a_set_order", ORDERED, [set_(topic("p1", "aseo", trace="t1", n=1))],
              [E("CARRIED", "ORDER", "c1"), E("CARRIED", "CARDINALITY", "c2"), UNI]),
        delta("topn_capped_by_its_receipt", interp(buyer("c1"), trace="t0"),
              [set_order(order("p1", trace="t1", n=1)), set_(top("p2", 250, rules=(fe.RULE_TOPN_CAPPED,), trace="t1", n=2))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI, E("TOPN_CAPPED", "CARDINALITY", "p2")]),
        delta("topn_capped_without_its_receipt_is_not_announced", interp(buyer("c1"), trace="t0"),
              [set_order(order("p1", trace="t1", n=1)), set_(top("p2", 250, trace="t1", n=2))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI]),
        delta("topn_capped_is_announced_at_every_execution", interp(order("c1"), top("c2", 250, rules=(fe.RULE_TOPN_CAPPED,))),
              [set_(topic("p1", "aseo", trace="t1", n=1))],
              [E("CARRIED", "ORDER", "c1"), E("CARRIED", "CARDINALITY", "c2"), UNI, E("TOPN_CAPPED", "CARDINALITY", "c2")]),
        delta("topn_capped_receipt_on_another_dimension_is_nothing", interp(buyer("c1"), trace="t0"),
              [set_(topic("p1", "aseo", rules=(fe.RULE_TOPN_CAPPED,), trace="t1", n=1))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI]),
        delta("order_default_by_provenance", interp(buyer("c1"), trace="t0"),
              [set_order(order("p1", key=OrderKey.DATE, source=fa.OrderDirectionSource.DEFAULTED, trace="t1", n=1))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI, E("ORDER_DIRECTION_DEFAULTED", "ORDER", "p1")]),
        delta("order_default_by_receipt_alone", interp(buyer("c1"), trace="t0"),
              [set_order(order("p1", key=OrderKey.DATE, rules=(fe.RULE_ORDER_DEFAULTED,), trace="t1", n=1))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI, E("ORDER_DIRECTION_DEFAULTED", "ORDER", "p1")]),
        delta("order_default_is_announced_at_every_execution",
              interp(order("c1", source=fa.OrderDirectionSource.DEFAULTED), trace="t0"), [set_(topic("p1", "aseo", trace="t1", n=1))],
              [E("CARRIED", "ORDER", "c1"), UNI, E("ORDER_DIRECTION_DEFAULTED", "ORDER", "c1")]),
        delta("order_stated_is_silent", interp(buyer("c1"), trace="t0"),
              [set_order(order("p1", key=OrderKey.DATE, source=fa.OrderDirectionSource.STATED, trace="t1", n=1))],
              [E("CARRIED", "BUYER/POSITIVE", "c1"), UNI]),
        # turns with a patch that are not a text delta: a pending's answer and an event
        delta("pending_answer_is_a_delta", BASE_BT, [set_(topic("p1", "aseo", trace="t1", n=1))], [*CARRY_BT],
              resolution=fa.TurnResolution.PENDING_ANSWER),
        # a button event is no delta and no answer: no CARRIED line (Gold SF149-SF152), its effects still apply
        delta("protocol_event_keeps_its_effects", ORDERED, [set_order(order("p1", key=OrderKey.DATE, origin=event_origin()))],
              [E("TOPN_CARRIED", "CARDINALITY", "c2")], resolution=fa.TurnResolution.PROTOCOL_EVENT,
              kind=fa.PatchOriginKind.EVENT),
        delta("protocol_event_order_button_of_sf149", interp(topic("c1", "vigilancia"), buyer("c2"), time("c3"), trace="t0"),
              [set_order(order("p1", key=OrderKey.VALUE, source=fa.OrderDirectionSource.STATED, origin=event_origin()))], [],
              resolution=fa.TurnResolution.PROTOCOL_EVENT, kind=fa.PatchOriginKind.EVENT),
        delta("protocol_event_remove_filter_button_of_sf151", interp(topic("c1", "papeleria"), buyer("c2"), time("c3"), trace="t0"),
              [fa.PatchOp(fa.PatchOpKind.CLEAR, fa.Slot(D.BUYER, POS), None, None, event_origin(fa.OriginKind.VIEW_ACTION))],
              [], resolution=fa.TurnResolution.PROTOCOL_EVENT, kind=fa.PatchOriginKind.EVENT),
        delta("protocol_event_universe_button_changes_the_universe", BASE_BT,
              [fa.PatchOp(fa.PatchOpKind.REPLACE_UNIVERSE, fa.UNIVERSE_SLOT, None,
                          fa.UniverseTarget(Subject.PROCESSES, fa.UniverseKind.RECORDS), event_origin())],
              [E("UNIVERSE_CHANGED", "UNIVERSE", "c2")], resolution=fa.TurnResolution.PROTOCOL_EVENT,
              kind=fa.PatchOriginKind.EVENT),
    ]
    # a NEW_SEARCH: CONTEXT_RESET over a standing anchor, the provenance of its own conditions, nothing by difference
    new_conditions = (topic("n1", "vias", trace="t1", n=1), time("n2", 2026, trace="t1", n=2,
                      provenance=fa.Provenance(year_source=fa.YearSource.CLOCK_INFERRED)))
    new_effective = interp(*new_conditions, trace=None)
    out += [
        fresh("new_search_over_a_standing_anchor", new_effective, [E("YEAR_CLOCK_INFERRED", "TIME/POSITIVE", "n2"),
              E("CONTEXT_RESET", None)], base=BASE_BT),
        fresh("new_search_without_an_anchor", new_effective, [E("YEAR_CLOCK_INFERRED", "TIME/POSITIVE", "n2")]),
        fresh("new_search_replacing_an_expired_anchor", new_effective, [E("YEAR_CLOCK_INFERRED", "TIME/POSITIVE", "n2")],
              base=BASE_BT, expired=True),
        fresh("new_search_has_no_difference_echo", interp(topic("n1", "vias", trace="t1", n=1), trace=None), [E("CONTEXT_RESET", None)],
              base=TOPIC_BASE),
        fresh("new_search_announces_its_plan", interp(order("n1", source=fa.OrderDirectionSource.DEFAULTED, trace="t1", n=1),
              top("n2", 250, rules=(fe.RULE_TOPN_CAPPED,), trace="t1", n=2), trace=None),
              [E("TOPN_CAPPED", "CARDINALITY", "n2"), E("ORDER_DIRECTION_DEFAULTED", "ORDER", "n1")]),
    ]
    # CONTEXT_EXPIRED: the turn found the anchor expired and neither confirms nor replaces it
    patch = fa.SemanticPatch((set_(topic("p1", "aseo", trace="t1", n=1)),), fa.PatchOriginKind.TEXT)
    pending = expiry_pending(BASE_BT, patch)
    gone = [E("CONTEXT_EXPIRED", None)]
    out += [
        silent("expired_clarify_with_its_question", fa.TurnResolution.CLARIFY, gone, base=BASE_BT, expired=True, pending=pending),
        silent("expired_clarify", fa.TurnResolution.CLARIFY, gone, base=BASE_BT, expired=True),
        silent("expired_inform", fa.TurnResolution.INFORM, gone, base=BASE_BT, expired=True),
        silent("expired_reask", fa.TurnResolution.REASK, gone, base=BASE_BT, expired=True),
        silent("expired_without_the_anchor_loaded", fa.TurnResolution.INFORM, gone, expired=True),
        silent("standing_clarify_is_silent", fa.TurnResolution.CLARIFY, [], base=BASE_BT),
        silent("standing_inform_is_silent", fa.TurnResolution.INFORM, [], base=BASE_BT),
        silent("standing_reask_is_silent", fa.TurnResolution.REASK, [], base=BASE_BT),
        silent("a_page_is_the_same_plan_again", fa.TurnResolution.PROTOCOL_PAGE, [], base=BASE_BT),
        not_ready("a_delta_that_is_not_ready_executes_nothing", BASE_BT, [set_(topic("p1", "aseo", trace="t1", n=1))], []),
        not_ready("a_delta_that_is_not_ready_over_an_expired_anchor", BASE_BT, [set_(topic("p1", "aseo", trace="t1", n=1))],
                  gone, expired=True),
        delta("confirmed_anchor_is_not_expired", BASE_BT, [set_(topic("p1", "aseo", trace="t1", n=1))], [*CARRY_BT], expired=True),
    ]
    return out


CASES = _cases()


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_every_scenario(case):
    assert case.run() == case.expected


@pytest.mark.parametrize("case", CASES, ids=lambda case: case.name)
def test_the_echo_is_deterministic_and_the_record_stores_it(case):
    first, second = fe.echo_items(*case.args, **case.kwargs), fe.echo_items(*case.args, **case.kwargs)
    assert first == second
    record = replace(case.args[3], echo=first)  # the type refuses duplicates and bad items
    assert fa.AcceptedTurnRecord.from_dict(record.to_dict()) == record
    ranks = [(fe.CLASS_RANK[item.echo_class], fe._slot_rank(item.slot)) for item in first]
    assert ranks == sorted(ranks)  # the order is the contract's: by class (§11), then by slot (§4.2)


def test_the_scenario_names_are_unique():
    assert len({case.name for case in CASES}) == len(CASES)


# ---- enumerated from the types ---------------------------------------------------------------------------------------
# What each source of provenance owes, written from §11 (a table of the CONTRACT, not read from the module).
SOURCE_OWES = {
    (fa.UnitSource, fa.UnitSource.STATED): [],
    (fa.UnitSource, fa.UnitSource.SIBLING_IN_RANGE): ["UNIT_SHARED_IN_RANGE"],
    (fa.UnitSource, fa.UnitSource.PREVIOUS_ACCEPTED_VALUE): ["UNIT_INHERITED"],
    (fa.YearSource, fa.YearSource.STATED): [],
    (fa.YearSource, fa.YearSource.CLOCK_INFERRED): ["YEAR_CLOCK_INFERRED"],
    (fa.YearSource, fa.YearSource.RELATIVE_TO_CLOCK): ["RELATIVE_WINDOW_RESOLVED"],
    (fa.YearSource, fa.YearSource.PREVIOUS_ACCEPTED_TIME): ["YEAR_INHERITED"],
    (fa.OrderDirectionSource, fa.OrderDirectionSource.STATED): [],
    (fa.OrderDirectionSource, fa.OrderDirectionSource.DEFAULTED): ["ORDER_DIRECTION_DEFAULTED"],
}


def test_every_provenance_source_of_the_types_is_decided():
    for kind in (fa.UnitSource, fa.YearSource, fa.OrderDirectionSource):
        assert {(kind, member) for member in kind} == {key for key in SOURCE_OWES if key[0] is kind}, kind
    announced = {member for table in (fe.UNIT_ECHO, fe.YEAR_ECHO, fe.ORDER_ECHO) for member in table}
    assert announced | fe.SILENT_SOURCES == {member for kind, member in SOURCE_OWES}
    assert not announced & fe.SILENT_SOURCES


def _placed_with(source) -> Case:
    if isinstance(source, fa.UnitSource):
        placed = money("p1", 100, Comparator.GTE, trace="t1", n=1, unit_source=source,
                       inherited_from="t0" if source is fa.UnitSource.PREVIOUS_ACCEPTED_VALUE else None)
        base = BASE_BT
    elif isinstance(source, fa.YearSource):
        placed = time("p1", 2026, trace="t1", n=1, provenance=fa.Provenance(
            year_source=source, inherited_from="t0" if source is fa.YearSource.PREVIOUS_ACCEPTED_TIME else None))
        base = BASE_BT
    else:
        placed = order("p1", key=OrderKey.DATE, source=source, trace="t1", n=1)
        base = interp(buyer("c1"), trace="t0")
    op = set_order(placed) if placed.dimension is D.ORDER else set_(placed)
    return delta(f"source_{source.value}", base, [op], [])


@pytest.mark.parametrize("key", sorted(SOURCE_OWES, key=lambda key: (key[0].__name__, key[1].value)),
                         ids=lambda key: f"{key[0].__name__}.{key[1].value}")
def test_each_provenance_source_owes_exactly_its_class(key):
    kind, source = key
    case = _placed_with(source)
    owed = [item[0] for item in case.run() if item[0] not in {"CARRIED"}]
    assert owed == SOURCE_OWES[key]


# Which scenario produces each class: every class of the closed list is produced by a scenario, or it is declared
# unrepresentable today (the premise is pinned below). A class added to ``EchoClass`` fails here until it is decided.
PRODUCED_BY = {
    "CARRIED": "carried", "UNIT_INHERITED": "unit_inherited", "UNIT_SHARED_IN_RANGE": "unit_shared_in_range",
    "YEAR_INHERITED": "year_inherited", "YEAR_CLOCK_INFERRED": "year_clock_inferred",
    "RELATIVE_WINDOW_RESOLVED": "relative_window_clock", "VALUE_RANGE_REPLACED": "value_range_a81",
    "TOPIC_REPLACED": "topic_replaced", "POLARITY_INVERTED": "polarity_inverted", "UNIVERSE_CHANGED": "universe_changed",
    "TOPN_CARRIED": "topn_carried", "TOPN_CAPPED": "topn_capped_by_its_receipt",
    "ORDER_DIRECTION_DEFAULTED": "order_default_by_provenance", "CONTEXT_RESET": "new_search_over_a_standing_anchor",
    "CONTEXT_EXPIRED": "expired_clarify",
}


def test_every_echo_class_is_produced_or_declared_unrepresentable():
    declared = {echo_class.value for echo_class in fe.NOT_REPRESENTABLE}
    assert set(PRODUCED_BY) | declared == {echo_class.value for echo_class in fa.EchoClass}
    assert not set(PRODUCED_BY) & declared
    by_name = {case.name: case for case in CASES}
    for echo_class, name in PRODUCED_BY.items():
        assert echo_class in {item[0] for item in by_name[name].run()}, (echo_class, name)


def test_the_unrepresentable_classes_are_so_because_the_type_refuses_a_derived_view():
    """VIEW_DERIVED / VIEW_REDERIVED need a derived view: ``subject = unspecified`` and a ``HostViewDecision``. The
    premise is pinned: the day ``AcceptedRequest`` admits a view (wave F; §19 DEP-DERIVED-VIEW) this test fails and the
    echo owes the two classes (a declared red, never a dispensation)."""
    assert fe.NOT_REPRESENTABLE == {fa.EchoClass.VIEW_DERIVED, fa.EchoClass.VIEW_REDERIVED}
    request = fa.AcceptedRequest("r1", "s1", Subject.CONTRACTS, fa.UniverseKind.RECORDS, span_origin("t0", 0, RAW_BASE),
                                 None, "jbjy-vk9h")
    with pytest.raises(ContractError, match="VIEW_TYPE_UNAVAILABLE"):
        replace(request, view="a view")
    with pytest.raises(ContractError, match="VIEW_REQUIRED"):
        replace(request, subject=Subject.UNSPECIFIED)


# ---- the arguments must be ONE coherent turn (negative contract) -----------------------------------------------------
def _delta():
    return delta("d", BASE_BT, [set_(topic("p1", "aseo", trace="t1", n=1))], CARRY_BT)


def _tampered(change) -> tuple:
    base, effective, patch, _ = _delta().args
    effective = change(effective)
    return base, effective, patch, make_record(fa.TurnResolution.DELTA, patch=patch, effective=effective, base_trace_id="t0")


def _refusals() -> list[tuple]:
    """``(name, args, kwargs, code)``: a turn the echo must REFUSE with ``ContractError(code)`` (never echo)."""
    base, effective, patch, record = _delta().args
    other = fa.SemanticPatch((set_(topic("p9", "vias", trace="t1", n=1)),), fa.PatchOriginKind.TEXT)
    pending = expiry_pending(BASE_BT, patch)
    expiry_record = make_record(fa.TurnResolution.CLARIFY, pending=pending)
    foreign = make_record(fa.TurnResolution.DELTA, patch=patch, effective=effective, base_trace_id="t-other")
    extra = interp(buyer("c1"), time("c2"), topic("p1", "aseo", trace="t1", n=1), topic("zz", "vias", trace="t1", n=5),
                   trace="t0")
    only_base = replace(effective, conditions=tuple(c for c in effective.conditions if c.condition_id != "p1"))
    processes = interp(buyer("c1"), time("c2"), topic("p1", "aseo", trace="t1", n=1), subject=Subject.PROCESSES, trace="t0")
    redirect = fa.SemanticPatch((universe(Subject.PROCESSES),), fa.PatchOriginKind.TEXT)
    stays = replace(base, base_trace_id="t0")

    def changed(c):
        return replace(c, provenance=fa.Provenance(year_source=fa.YearSource.CLOCK_INFERRED))

    def tampered_with(base_, patch_, change):
        effective_ = change(fd.compose(base_, patch_, base_trace_id="t0").effective)
        return base_, effective_, patch_, make_record(fa.TurnResolution.DELTA, patch=patch_, effective=effective_,
                                                      base_trace_id="t0")

    def without(*ids):
        return lambda e: replace(e, conditions=tuple(c for c in e.conditions if c.condition_id not in ids))

    topic_patch = fa.SemanticPatch((set_(topic("p1", "aseo", trace="t1", n=1)),), fa.PatchOriginKind.TEXT)
    bounded = interp(buyer("c1"), money("c2", 900, Comparator.LT), trace="t0")
    negated = interp(topic("c1", "aseo"), trace="t0")
    other_value = fa.SemanticPatch((set_(topic("p1", "obras", excluded=True, trace="t1", n=1)),), fa.PatchOriginKind.TEXT)
    return [
        ("no_record", (base, effective, patch, None), {}, "ECHO_NEEDS_A_RECORD"),
        ("record_as_a_dict", (base, effective, patch, record.to_dict()), {}, "ECHO_NEEDS_A_RECORD"),
        ("patch_outside_the_record", (base, effective, other, record), {}, "ECHO_ARGUMENTS_OUTSIDE_THE_RECORD"),
        ("effective_outside_the_record", (base, interp(buyer("c1"), trace="t0"), patch, record), {},
         "ECHO_ARGUMENTS_OUTSIDE_THE_RECORD"),
        ("effective_dropped", (base, None, patch, record), {}, "ECHO_ARGUMENTS_OUTSIDE_THE_RECORD"),
        ("patch_dropped", (base, effective, None, record), {}, "ECHO_ARGUMENTS_OUTSIDE_THE_RECORD"),
        *[(f"anchor_expired_{bad!r}", (base, effective, patch, record), {"anchor_expired": bad}, "ECHO_INVALID_ANCHOR_EXPIRED")
          for bad in (1, None, "yes")],
        ("base_as_a_dict", (base.to_dict(), effective, patch, record), {}, "ECHO_INVALID_BASE"),
        ("composition_without_a_base", (None, effective, patch, record), {}, "ECHO_COMPOSITION_WITHOUT_BASE"),
        ("effective_of_another_base", (base, effective, patch, foreign), {}, "ECHO_EFFECTIVE_OF_ANOTHER_BASE"),
        ("expiry_question_over_a_standing_anchor", (BASE_BT, None, None, expiry_record), {},
         "ECHO_EXPIRED_PENDING_WITHOUT_EXPIRY"),
        ("removal_that_nobody_cited", _tampered(without("c2")), {}, "ECHO_UNCITED_REMOVAL"),
        ("uncited_value_bound_without_an_op_on_the_opposite_side",
         tampered_with(bounded, topic_patch, without("c2")), {}, "ECHO_UNCITED_REMOVAL"),
        ("uncited_positive_while_another_value_is_negated", tampered_with(negated, other_value, without("c1")), {},
         "ECHO_UNCITED_REMOVAL"),
        ("carried_condition_changed", _tampered(lambda e: replace(e, conditions=tuple(
            changed(c) if c.condition_id == "c2" else c for c in e.conditions))), {}, "ECHO_CARRIED_CONDITION_CHANGED"),
        ("condition_no_operation_placed", _tampered(lambda e: extra), {}, "ECHO_PLACED_WITHOUT_OPERATION"),
        ("operation_whose_condition_is_missing", (base, only_base, patch, make_record(
            fa.TurnResolution.DELTA, patch=patch, effective=only_base, base_trace_id="t0")), {},
         "ECHO_PLACED_WITHOUT_OPERATION"),
        ("universe_changed_without_an_operation", (base, processes, patch, make_record(
            fa.TurnResolution.DELTA, patch=patch, effective=processes, base_trace_id="t0")), {},
         "ECHO_REQUESTS_CHANGED_WITHOUT_OPERATION"),
        ("universe_not_the_requested_one", (base, stays, redirect, make_record(
            fa.TurnResolution.DELTA, patch=redirect, effective=stays, base_trace_id="t0")), {},
         "ECHO_UNIVERSE_NOT_THE_REQUESTED"),
    ]


REFUSALS = _refusals()


@pytest.mark.parametrize("refusal", REFUSALS, ids=lambda refusal: refusal[0])
def test_an_incoherent_turn_is_refused_not_echoed(refusal):
    _, args, kwargs, code = refusal
    with pytest.raises(ContractError, match=code):
        fe.echo_items(*args, **kwargs)


def test_every_refusal_has_its_own_name_and_the_codes_are_the_modules():
    assert len({refusal[0] for refusal in REFUSALS}) == len(REFUSALS)
    assert {refusal[3] for refusal in REFUSALS} >= {
        "ECHO_UNCITED_REMOVAL", "ECHO_CARRIED_CONDITION_CHANGED", "ECHO_PLACED_WITHOUT_OPERATION",
        "ECHO_REQUESTS_CHANGED_WITHOUT_OPERATION", "ECHO_UNIVERSE_NOT_THE_REQUESTED", "ECHO_NEEDS_A_RECORD",
        "ECHO_INVALID_BASE", "ECHO_INVALID_ANCHOR_EXPIRED", "ECHO_ARGUMENTS_OUTSIDE_THE_RECORD",
        "ECHO_COMPOSITION_WITHOUT_BASE", "ECHO_EFFECTIVE_OF_ANOTHER_BASE", "ECHO_EXPIRED_PENDING_WITHOUT_EXPIRY"}


def test_an_expiry_question_over_an_expired_anchor_is_the_expired_echo():
    pending = expiry_pending(BASE_BT, _delta().args[2])
    record = make_record(fa.TurnResolution.CLARIFY, pending=pending)
    assert [i.echo_class for i in fe.echo_items(BASE_BT, None, None, record, anchor_expired=True)] == [
        fa.EchoClass.CONTEXT_EXPIRED]


def test_the_echo_never_reads_the_records_own_echo():
    base, effective, patch, record = _delta().args
    stored = fe.echo_items(base, effective, patch, record)
    again = replace(record, echo=stored)
    assert fe.echo_items(base, effective, patch, again) == stored  # the field it computes is not an input
