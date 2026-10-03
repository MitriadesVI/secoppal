"""M2A-7: INV-M2A-12 (eco = ejecución, «exactamente») as properties over GENERATED typed turns (Hypothesis).

The turns are the ones the reducer is proven on (``st_case`` of ``test_foundation_delta_properties``: rich bases with
policies, receipt rules, filled provenance, dual anchors, opportunities, OR groups; patches of up to four operations),
composed by the REAL ``compose`` / ``delta_outcome``. The echo is then judged against a REFERENCE written here from the
contract and from ``compose``'s own typed account (``Composed``: what the patch placed, its effects, whether the
universe changed), not from the module's code:

* completeness: everything the reference owes is in the echo («todo lo arrastrado, cambiado y toda procedencia ... tiene
  su EchoItem»);
* justification: nothing is in the echo that the reference does not owe («ningún EchoItem carece de justificación»);
* the classes that compose reports as effects (POLARITY_INVERTED, TOPIC_REPLACED, UNIVERSE_CHANGED, A8.1) are the echo's;
* INV-M2A-16 as the echo needs it: every condition that left the base was cited or substituted;
* invariance to the permutation of the operations, determinism, the order of §11, the record accepts the echo;
* the turns that execute nothing echo nothing but ``CONTEXT_EXPIRED``; a fresh acceptance echoes its own provenance and
  plan and the ``CONTEXT_RESET`` over a standing anchor, nothing by difference.
"""
from __future__ import annotations

import types
from dataclasses import replace

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
import app.core.foundation_echo as fe
from app.core.interpretation_contract import POLARIZED_SLOT_DIMENSIONS, Polarity
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_foundation_delta_properties import st_base, st_case, st_op
from app.core.foundation_authority_values import Comparator
from app.core.foundation_authority_values import OrderKey
from tests.test_foundation_echo import (
    LOWER, SOURCE_OWES, UPPER, buyer, clear, interp, make_record, money, order, set_, set_order, summary, time, top, topic,
    keep, universe,
)
from app.core.foundation_operations import Subject

PROPS = settings(max_examples=250, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
PATCHED_RESOLUTIONS = (fa.TurnResolution.DELTA, fa.TurnResolution.PROTOCOL_EVENT)  # an answer has its own entry
EFFECT_DERIVED = {"POLARITY_INVERTED", "TOPIC_REPLACED", "UNIVERSE_CHANGED", "VALUE_RANGE_REPLACED"}
EXACT = {"CARRIED", "UNIT_INHERITED", "UNIT_SHARED_IN_RANGE", "YEAR_INHERITED", "YEAR_CLOCK_INFERRED",
         "RELATIVE_WINDOW_RESOLVED", "TOPN_CARRIED", "TOPN_CAPPED", "ORDER_DIRECTION_DEFAULTED"}
RELATIVE_RULE = "time.relative_to_previous"


# ---- the reference: from the contract and from compose's own account, never from the module ------------------------
def contradictory(conditions) -> bool:
    """An ORDER whose receipt says «defaulted» over a direction the user STATED: the echo refuses it (a receipt that
    contradicts the provenance is never echoed). The reducer's generators attach the receipt to every ORDER."""
    return any((c.dimension is D.ORDER and fe.RULE_ORDER_DEFAULTED in c.receipt_rules
                and c.provenance.order_direction_source is fa.OrderDirectionSource.STATED)
               or (fe.RULE_RELATIVE_TO_PREVIOUS in c.receipt_rules
                   and c.provenance.year_source is not fa.YearSource.PREVIOUS_ACCEPTED_TIME) for c in conditions)


def refused_for_its_receipt(call) -> list[str]:
    """What the echo must do with a contradictory effective: refuse with ``ECHO_RECEIPT_CONTRADICTS_PROVENANCE``."""
    try:
        call()
    except Exception as error:  # noqa: BLE001
        return [] if "ECHO_RECEIPT_CONTRADICTS_PROVENANCE" in str(error) else [f"refused for another reason: {error}"]
    return ["echoed a receipt that contradicts the provenance"]


def provenance_owed(placed) -> set[tuple]:
    """§11 per placed condition, from the enumerated table ``SOURCE_OWES`` of the contract."""
    out = set()
    for c in placed:
        for source in (c.provenance.unit_source, c.provenance.year_source):
            if source is None:
                continue
            for echo_class in SOURCE_OWES[(type(source), source)]:
                if echo_class != "UNIT_SHARED_IN_RANGE":
                    out.add((echo_class, c.slot.key, (c.condition_id,)))
        if c.provenance.year_source is fa.YearSource.PREVIOUS_ACCEPTED_TIME and RELATIVE_RULE in c.receipt_rules:
            out.add(("RELATIVE_WINDOW_RESOLVED", c.slot.key, (c.condition_id,)))
    if any(c.provenance.unit_source is fa.UnitSource.SIBLING_IN_RANGE for c in placed):
        out.add(("UNIT_SHARED_IN_RANGE", "VALUE", tuple(c.condition_id for c in placed if c.dimension is D.VALUE)))
    return out


def plan_owed(conditions) -> set[tuple]:
    """INV-M2A-21 over EVERY condition of the effective: a cap (its receipt) and a defaulted direction."""
    out = set()
    for c in conditions:
        if c.dimension is D.CARDINALITY and "topn.capped" in c.receipt_rules:
            out.add(("TOPN_CAPPED", c.slot.key, (c.condition_id,)))
        if c.dimension is D.ORDER and (c.provenance.order_direction_source is fa.OrderDirectionSource.DEFAULTED
                                       or "order.direction_defaulted" in c.receipt_rules):
            out.add(("ORDER_DIRECTION_DEFAULTED", c.slot.key, (c.condition_id,)))
    return out


def exact_owed(base, patch, composed, resolution=fa.TurnResolution.DELTA) -> set[tuple]:
    """The classes whose items the reference can state exactly (``EXACT``). CARRIED is a delta's or an answer's: a button
    event (``PROTOCOL_EVENT``) is neither, and the Gold gives it no CARRIED line (SF149-SF152)."""
    effective = composed.effective
    placed = [c for c in effective.conditions if c.condition_id in composed.placed_ids]
    carried = [c for c in effective.conditions if c.condition_id not in composed.placed_ids]
    ops = patch.ops
    out = set()
    if resolution is not fa.TurnResolution.PROTOCOL_EVENT:
        out = {("CARRIED", c.slot.key, (c.condition_id,)) for c in carried}
        if not any(op.op is fa.PatchOpKind.REPLACE_UNIVERSE for op in ops):
            out.add(("CARRIED", "UNIVERSE", ()))
    if any(op.op is fa.PatchOpKind.SET_ORDER for op in ops):
        out |= {("TOPN_CARRIED", c.slot.key, (c.condition_id,)) for c in carried if c.dimension is D.CARDINALITY}
    return out | provenance_owed(placed) | plan_owed(effective.conditions)


def problems(echo_items, base, patch, composed, resolution=fa.TurnResolution.DELTA) -> list[str]:
    """Every way the echo of this composed turn departs from the reference (empty = exact)."""
    effective = composed.effective
    record = make_record(resolution, patch=patch, effective=effective, base_trace_id="t0")
    if contradictory(effective.conditions):
        return refused_for_its_receipt(lambda: echo_items(base, effective, patch, record, anchor_expired=False))
    try:
        items = echo_items(base, effective, patch, record, anchor_expired=False)
    except Exception as error:  # noqa: BLE001 - a lawful turn is never refused
        return [f"refused a lawful turn: {type(error).__name__}: {error}"]
    got = summary(items)
    out = []
    owed = exact_owed(base, patch, composed, resolution)
    exact_got = {item for item in got if item[0] in EXACT}
    out += [f"missing {item}" for item in sorted(owed - exact_got, key=repr)]
    out += [f"unjustified {item}" for item in sorted(exact_got - owed, key=repr)]
    classes = {item[0] for item in got}
    out += [f"unknown class {name}" for name in sorted(classes - EXACT - EFFECT_DERIVED)]
    effects = {effect.value for effect in composed.effects}
    for name in ("POLARITY_INVERTED", "TOPIC_REPLACED"):
        if (name in effects) != (name in classes):
            out.append(f"{name}: compose reports {name in effects}, the echo {name in classes}")
    if composed.universe_changed != ("UNIVERSE_CHANGED" in classes):
        out.append(f"UNIVERSE_CHANGED: compose {composed.universe_changed}, the echo {'UNIVERSE_CHANGED' in classes}")
    out += value_range_problems(base, patch, composed, got)
    out += item_problems(base, composed, got)
    ranks = [(fe.CLASS_RANK[fa.EchoClass(item[0])], fe._slot_rank(None if item[1] is None else fa.Slot.from_dict(item[1])))
             for item in got]
    if ranks != sorted(ranks):
        out.append("not in the order of §11 then §4.2")
    return out


def value_range_problems(base, patch, composed, got) -> list[str]:
    """VALUE_RANGE_REPLACED: a side the base had and the effective lacks, replaced by A8.1 (compose's own effect) or by a
    whole-VALUE CLEAR followed by a SET (Gold schema §6 (b))."""
    effective = composed.effective
    base_sides = {c.slot: c for c in base.conditions if c.dimension is D.VALUE}
    standing = {c.slot for c in effective.conditions if c.dimension is D.VALUE}
    placed = tuple(c.condition_id for c in effective.conditions
                   if c.condition_id in composed.placed_ids and c.dimension is D.VALUE)
    wide_clear = any(op.op is fa.PatchOpKind.CLEAR and op.slot == fa.Slot(D.VALUE) for op in patch.ops)
    a81 = fd.DeltaEffect.VALUE_RANGE_REPLACED in composed.effects
    addressed = {slot for op in patch.ops for slot in fd.covered(op.slot)} | {
        op.slot for op in patch.ops if op.op in fd.PLACERS}
    owed = set()
    for slot, old in base_sides.items():
        if slot in standing:
            continue
        if (a81 and slot not in addressed) or (wide_clear and placed):
            owed.add(("VALUE_RANGE_REPLACED", slot.key, (old.condition_id, *placed)))
    gotten = {item for item in got if item[0] == "VALUE_RANGE_REPLACED"}
    return [f"missing {item}" for item in sorted(owed - gotten)] + [f"unjustified {item}" for item in sorted(gotten - owed)]


def item_problems(base, composed, got) -> list[str]:
    """Every id of an item names a condition of the base or of the effective, on the slot the item says."""
    effective = composed.effective
    known = {c.condition_id: c for c in (*base.conditions, *effective.conditions)}
    out = []
    for echo_class, slot, ids in got:
        if echo_class not in {"UNIVERSE_CHANGED", "UNIT_SHARED_IN_RANGE"} and slot not in (None, "UNIVERSE"):
            wrong = [cid for cid in ids if known[cid].slot.key != slot and echo_class not in EFFECT_DERIVED]
            out += [f"{echo_class}: {cid} is not on {slot}" for cid in wrong]
        out += [f"{echo_class}: unknown condition {cid}" for cid in ids if cid not in known]
        if echo_class == "UNIVERSE_CHANGED":
            carried_time = tuple(c.condition_id for c in effective.conditions
                                 if c.condition_id not in composed.placed_ids and c.dimension is D.TIME)
            if ids != carried_time:
                out.append(f"UNIVERSE_CHANGED names {ids}, the carried TIME is {carried_time}")
    return out


@st.composite
def st_value_turn(draw):
    """The shapes of A8.1 and of §11's second VALUE_RANGE_REPLACED, which the general generator reaches rarely: a base with
    0-2 VALUE sides (comparable units) and a patch of whole-VALUE / one-side CLEARs and bounds on either side."""
    sides = draw(st.lists(st.sampled_from(["lower", "upper"]), unique=True, max_size=2))
    amount = {side: draw(st.sampled_from([100, 500, 900])) for side in ("lower", "upper")}
    comparator = {"lower": Comparator.GTE, "upper": Comparator.LT}
    base = interp(*[money(f"c{n}", amount[side], comparator[side]) for n, side in enumerate(sides)], topic("ct", "aseo"),
                  trace="t0")
    ops = []
    for n, kind in enumerate(draw(st.lists(st.sampled_from(["clear_wide", "clear_lower", "clear_upper", "set_lower",
                                                            "set_upper"]), unique=True, min_size=1, max_size=3))):
        if kind.startswith("clear"):
            ops.append(clear(fa.Slot(D.VALUE) if kind == "clear_wide" else (LOWER if kind.endswith("lower") else UPPER)))
        else:
            side = kind.removeprefix("set_")
            ops.append(set_(money(f"p{n}", draw(st.sampled_from([100, 500, 900])), comparator[side], trace="t1", n=n + 1)))
    return base, fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT), False


@st.composite
def st_plan_turn(draw):
    """The rules the general generator reaches only with one fixed pairing: a cap and a defaulted direction by receipt
    AND by provenance independently, receipts on conditions that have no business with them, a TIME with every year
    source with and without the relative receipt, a bound with every unit source."""
    pick = lambda options: draw(st.sampled_from(options))  # noqa: E731
    stray = lambda: pick([(), (fe.RULE_TOPN_CAPPED,), (fe.RULE_ORDER_DEFAULTED,), (fe.RULE_RELATIVE_TO_PREVIOUS,)])  # noqa: E731
    sources = [None, fa.OrderDirectionSource.STATED, fa.OrderDirectionSource.DEFAULTED]
    parts = [buyer("c1", rules=stray())]
    if draw(st.booleans()):
        parts.append(order("c2", source=pick(sources), rules=pick([(), (fe.RULE_ORDER_DEFAULTED,)])))
        if draw(st.booleans()):
            parts.append(top("c3", pick([10, 250]), rules=pick([(), (fe.RULE_TOPN_CAPPED,)])))
    base = interp(*parts, trace="t0")
    ops = []
    if draw(st.booleans()):
        ops.append(set_(topic("p1", "aseo", rules=stray(), trace="t1", n=1)))
    if draw(st.booleans()):
        ops.append(set_order(order("p2", key=OrderKey.DATE, source=pick(sources),
                                   rules=pick([(), (fe.RULE_ORDER_DEFAULTED,)]), trace="t1", n=2)))
    if draw(st.booleans()):
        ops.append(set_(top("p3", pick([10, 250]), rules=pick([(), (fe.RULE_TOPN_CAPPED,)]), trace="t1", n=3)))
    if draw(st.booleans()):
        source = pick(list(fa.YearSource))
        ops.append(set_(time("p4", 2026, rules=pick([(), (fe.RULE_RELATIVE_TO_PREVIOUS,)]), trace="t1", n=4,
                             provenance=fa.Provenance(year_source=source, inherited_from=(
                                 "t0" if source is fa.YearSource.PREVIOUS_ACCEPTED_TIME else None)))))
    if draw(st.booleans()):
        unit = pick(list(fa.UnitSource))
        ops.append(set_(money("p5", 500, Comparator.GTE, unit_source=unit, trace="t1", n=5, inherited_from=(
            "t0" if unit is fa.UnitSource.PREVIOUS_ACCEPTED_VALUE else None))))
    if not ops:
        ops.append(set_(topic("p1", "aseo", trace="t1", n=1)))
    return base, fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT), False


@st.composite
def st_answer_turn(draw):
    """A turn that opens the R3-1 pending (``CLEAR(ORDER)`` over an ordered base that keeps a CARDINALITY) and leaves 0-3
    other operations in its FRAME: a TOPIC, a TIME with every year source, bounds with every unit source (A8.1 included),
    a buyer, the clearing of a slot, a change of universe. What the frame did must be announced by the answer."""
    pick = lambda options: draw(st.sampled_from(options))  # noqa: E731
    sources = [None, fa.OrderDirectionSource.STATED, fa.OrderDirectionSource.DEFAULTED]
    base = interp(buyer("c1"), topic("c4", "aseo"), time("c5", 2025), money("c6", 500, Comparator.LT),
                  order("c2", source=pick(sources)), top("c3", pick([10, 250]), rules=pick([(), (fe.RULE_TOPN_CAPPED,)])),
                  trace="t0")
    extras = []
    for n, kind in enumerate(draw(st.lists(st.sampled_from(["topic", "time", "value", "buyer", "clear_time", "universe"]),
                                           unique=True, max_size=3)), start=1):
        if kind == "topic":
            extras.append(set_(topic(f"p{n}", pick(["vias", "aseo"]), trace="t1", n=n)))
        elif kind == "time":
            source = pick(list(fa.YearSource))
            extras.append(set_(time(f"p{n}", 2026, trace="t1", n=n, provenance=fa.Provenance(
                year_source=source, inherited_from="t0" if source is fa.YearSource.PREVIOUS_ACCEPTED_TIME else None))))
        elif kind == "value":
            unit = pick(list(fa.UnitSource))
            extras.append(set_(money(f"p{n}", pick([100, 900]), pick([Comparator.GTE, Comparator.LT]), unit_source=unit,
                                     trace="t1", n=n, inherited_from="t0" if unit is fa.UnitSource.PREVIOUS_ACCEPTED_VALUE
                                     else None)))
        elif kind == "buyer":
            extras.append(set_(buyer(f"p{n}", "VALLE", trace="t1", n=n)))
        elif kind == "clear_time":
            extras.append(clear(fa.Slot(D.TIME)))
        else:
            extras.append(universe(Subject.PROCESSES))
    ops = [clear(fa.Slot(D.ORDER)), *extras]
    return base, fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT), False


@st.composite
def st_difference_turn(draw):
    """The difference classes the general generator reaches rarely: a base with a TOPIC, a TIME, a buyer (single or dual
    anchor) and a patch of replacements, restatements, F16 negations, changes of universe and clearings."""
    pick = lambda options: draw(st.sampled_from(options))  # noqa: E731
    parts = [topic("c1", "aseo"), time("c2", 2025), buyer("c3")]
    if draw(st.booleans()):
        parts.append(topic("c4", "vias"))
    if draw(st.booleans()):
        parts.append(topic("c5", "obras", excluded=True))  # a whole-dimension CLEAR covers the negative slot too
    base = interp(*parts, subject=pick([Subject.CONTRACTS, Subject.PROCESSES]), dual=draw(st.booleans()), trace="t0")
    makers = {
        "topic_other": lambda n: set_(topic(f"p{n}", "vias", trace="t1", n=n)),
        "topic_same": lambda n: set_(topic(f"p{n}", "aseo", trace="t1", n=n)),
        "negate_aseo": lambda n: set_(topic(f"p{n}", "aseo", excluded=True, trace="t1", n=n)),
        "negate_other": lambda n: set_(topic(f"p{n}", "obras", excluded=True, trace="t1", n=n)),
        "universe_processes": lambda n: universe(Subject.PROCESSES),
        "universe_contracts": lambda n: universe(Subject.CONTRACTS),
        "clear_time": lambda n: clear(fa.Slot(D.TIME)),
        "clear_topic": lambda n: clear(fa.Slot(D.TOPIC)),
        "keep_time": lambda n: keep(fa.Slot(D.TIME, Polarity.POSITIVE)),
        "time_other": lambda n: set_(time(f"p{n}", 2026, trace="t1", n=n)),
    }
    kinds = draw(st.lists(st.sampled_from(sorted(makers)), unique=True, min_size=1, max_size=3))
    return base, fa.SemanticPatch(tuple(makers[kind](n) for n, kind in enumerate(kinds, start=1)),
                                  fa.PatchOriginKind.TEXT), False


def not_ready_problems(echo_items, base, patch) -> list[str]:
    """A DELTA that did not reach READY has a patch and no effective (§3.1): it executes nothing."""
    record = make_record(fa.TurnResolution.DELTA, patch=patch, effective=None, base_trace_id="t0")
    out = []
    for expired in (False, True):
        try:
            got = summary(echo_items(base, None, patch, record, anchor_expired=expired))
        except Exception as error:  # noqa: BLE001
            out.append(f"refused: {error}")
            continue
        if got != ([("CONTEXT_EXPIRED", None, ())] if expired else []):
            out.append(f"expired={expired}: {got}")
    return out


def answers_of(case):
    """Every lawful answer to the pending the REAL ``delta_outcome`` builds for this turn: ``(pending, answer, second step)``;
    the frame, its operations and the candidates come from M2A-3, never from this file."""
    base, patch, expired = case
    outcome = fo.delta_outcome(base, patch, base_trace_id="t0", anchor_expired=expired)  # expired: H1, «continuar»
    if outcome.disposition.value != "CLARIFY" or outcome.pending is None:
        return []
    out = []
    for ops in outcome.pending.candidates:
        answer = fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT)
        second = fd.compose(outcome.pending.frame, answer, base_trace_id="t1")
        if isinstance(second, fd.Composed):
            out.append((outcome.pending, answer, second))
    return out


def answer_problems(entry, anchor, pending, answer, second) -> list[str]:
    """The answer read against the ANCHOR, against a reference written from the two ``Composed`` of the reducer: the exact
    classes, and UNIVERSE_CHANGED iff the universes of the anchor and of what ran differ."""
    effective = second.effective
    record = make_record(fa.TurnResolution.PENDING_ANSWER, patch=answer, effective=effective, base_trace_id="t1")
    call = lambda: entry(anchor, pending.frame, pending.frame_ops, effective, record, anchor_expired=False)  # noqa: E731
    if contradictory(effective.conditions):
        return refused_for_its_receipt(call)
    try:
        got = summary(call())
    except Exception as error:  # noqa: BLE001 - a lawful answer is never refused
        return [f"refused a lawful answer: {type(error).__name__}: {error}"]
    frame_placed = (fd.compose_ops(anchor, pending.frame_ops, base_trace_id="t0").placed_ids
                    if pending.frame_ops else frozenset())
    anchor_ids = {c.condition_id for c in anchor.conditions}
    placed = [c for c in effective.conditions if c.condition_id in second.placed_ids or c.condition_id in frame_placed]
    carried = [c for c in effective.conditions if c.condition_id in anchor_ids and c.condition_id not in second.placed_ids]
    ops = (*pending.frame_ops, *answer.ops)
    owed = {("CARRIED", c.slot.key, (c.condition_id,)) for c in carried}
    if not any(op.op is fa.PatchOpKind.REPLACE_UNIVERSE for op in ops):
        owed.add(("CARRIED", "UNIVERSE", ()))
    if any(op.op is fa.PatchOpKind.SET_ORDER for op in ops):
        owed |= {("TOPN_CARRIED", c.slot.key, (c.condition_id,)) for c in carried if c.dimension is D.CARDINALITY}
    owed |= provenance_owed(placed) | plan_owed(effective.conditions)
    exact = {item for item in got if item[0] in EXACT}
    out = [f"missing {item}" for item in sorted(owed - exact, key=repr)] + [
        f"unjustified {item}" for item in sorted(exact - owed, key=repr)]
    universes = lambda i: [(r.subject, r.universe_kind) for r in i.requests]  # noqa: E731
    if (universes(anchor) != universes(effective)) != ("UNIVERSE_CHANGED" in {item[0] for item in got}):
        out.append("UNIVERSE_CHANGED disagrees with the universes of the anchor and of what ran")
    return out


def one_step_of(anchor, pending, answer, second):
    """The same turn as ONE composition over the anchor (the frame's operations and the answer's), when it composes to the
    same effective: then the answer's echo must be the delta's echo (a differential check against the delta entry)."""
    try:
        combined = fa.SemanticPatch((*pending.frame_ops, *answer.ops), fa.PatchOriginKind.TEXT)
    except Exception:  # noqa: BLE001 - mixed origins or no operations: not a delta
        return None
    one = fd.compose(anchor, combined, base_trace_id="t0")
    drop = lambda e: {k: v for k, v in e.to_dict().items() if k != "base_trace_id"}  # noqa: E731
    if isinstance(one, fd.Composed) and drop(one.effective) == drop(second.effective):
        return combined, one
    return None


def composed_of(base, patch):
    composed = fd.compose(base, patch, base_trace_id="t0")
    return composed if isinstance(composed, fd.Composed) else None


@PROPS
@given(st.one_of(st_case(), st_case(rich=False)), st.sampled_from(PATCHED_RESOLUTIONS))
def test_the_echo_is_exactly_the_reference_on_every_composed_turn(case, resolution):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    assert problems(fe.echo_items, base, patch, composed, resolution) == []


@PROPS
@given(st_plan_turn(), st.sampled_from(PATCHED_RESOLUTIONS))
def test_the_echo_is_exactly_the_reference_on_plan_and_provenance_turns(case, resolution):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    assert problems(fe.echo_items, base, patch, composed, resolution) == []


@PROPS
@given(st_difference_turn(), st.sampled_from(PATCHED_RESOLUTIONS))
def test_the_echo_is_exactly_the_reference_on_difference_turns(case, resolution):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    assert problems(fe.echo_items, base, patch, composed, resolution) == []


@PROPS
@given(st_value_turn(), st.sampled_from(PATCHED_RESOLUTIONS))
def test_the_echo_is_exactly_the_reference_on_value_range_turns(case, resolution):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    assert problems(fe.echo_items, base, patch, composed, resolution) == []


@PROPS
@given(st_value_turn())
def test_value_range_turns_reach_a81_and_the_clear_then_set_shape(case):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    record = make_record(fa.TurnResolution.DELTA, patch=patch, effective=composed.effective, base_trace_id="t0")
    classes = {i.echo_class.value for i in fe.echo_items(base, composed.effective, patch, record, anchor_expired=False)}
    assert ("VALUE_RANGE_REPLACED" in classes) == bool(
        {fd.DeltaEffect.VALUE_RANGE_REPLACED} & composed.effects or value_range_problems_owed(base, patch, composed))


def value_range_problems_owed(base, patch, composed) -> bool:
    """Independent of the echo: some VALUE side of the base is gone, after a whole-VALUE CLEAR and a VALUE SET."""
    gone = {c.slot for c in base.conditions if c.dimension is D.VALUE} - {
        c.slot for c in composed.effective.conditions if c.dimension is D.VALUE}
    wide = any(op.op is fa.PatchOpKind.CLEAR and op.slot == fa.Slot(D.VALUE) for op in patch.ops)
    return bool(gone and wide and any(op.op is fa.PatchOpKind.SET for op in patch.ops))


@PROPS
@given(st.one_of(st_case(), st_case(rich=False)))
def test_a_delta_that_is_not_ready_executes_nothing(case):
    base, patch, _ = case
    assert not_ready_problems(fe.echo_items, base, patch) == []


@PROPS
@given(st.one_of(st_case(), st_case(rich=False)))
def test_every_condition_that_left_the_base_was_cited_or_substituted(case):
    """INV-M2A-16, as the echo reads it: ``ranuras(base - efectiva) ⊆ ranuras(patch) ∪ sustituciones``."""
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    standing = {c.condition_id for c in composed.effective.conditions}
    addressed = {slot for op in patch.ops for slot in fd.covered(op.slot)} | {op.slot for op in patch.ops if op.op in fd.PLACERS}
    for old in base.conditions:
        if old.condition_id in standing:
            continue
        substituted = (
            (old.slot.dimension is D.VALUE and fd.DeltaEffect.VALUE_RANGE_REPLACED in composed.effects)
            or (old.slot.polarity is Polarity.POSITIVE and fd.DeltaEffect.POLARITY_INVERTED in composed.effects))
        assert old.slot in addressed or substituted, old.slot


@PROPS
@given(st.one_of(st_case(), st_case(rich=False)), st.randoms(use_true_random=False))
def test_the_permutation_of_the_operations_changes_nothing(case, rng):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    ops = list(patch.ops)
    rng.shuffle(ops)
    shuffled = fa.SemanticPatch(tuple(ops), patch.origin_kind)
    other = composed_of(base, shuffled)
    assert other is not None and other.effective == composed.effective
    if contradictory(composed.effective.conditions):
        return
    first = fe.echo_items(base, composed.effective, patch, make_record(
        fa.TurnResolution.DELTA, patch=patch, effective=composed.effective, base_trace_id="t0"), anchor_expired=False)
    second = fe.echo_items(base, other.effective, shuffled, make_record(
        fa.TurnResolution.DELTA, patch=shuffled, effective=other.effective, base_trace_id="t0"), anchor_expired=False)
    assert first == second


@PROPS
@given(st.one_of(st_case(), st_case(rich=False)))
def test_the_record_accepts_the_echo_and_reads_it_back(case):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    if contradictory(composed.effective.conditions):
        return
    record = make_record(fa.TurnResolution.DELTA, patch=patch, effective=composed.effective, base_trace_id="t0")
    items = fe.echo_items(base, composed.effective, patch, record, anchor_expired=False)
    stored = replace(record, echo=items)
    assert fa.AcceptedTurnRecord.from_dict(stored.to_dict()) == stored
    assert fe.echo_items(base, composed.effective, patch, stored, anchor_expired=False) == items


def silent_problems(echo_items, base, expired, resolution) -> list[str]:
    """A turn that executes nothing echoes nothing but ``CONTEXT_EXPIRED`` (and that only over an expired anchor)."""
    record = make_record(resolution)
    out = []
    for flag in (False, True):
        owed = [("CONTEXT_EXPIRED", None, ())] if flag else []
        try:
            got = summary(echo_items(base, None, None, record, anchor_expired=flag))
        except Exception as error:  # noqa: BLE001
            out.append(f"refused: {error}")
            continue
        if got != owed:
            out.append(f"expired={flag}: {got} != {owed}")
    return out


def fresh_problems(echo_items, effective, anchor, expired, control=None) -> list[str]:
    """A fresh acceptance owes its own provenance and plan, and CONTEXT_RESET over a standing anchor when no pending was
    vigente (``control`` is ``CANCEL_PENDING`` when a pending was); no difference."""
    record = make_record(fa.TurnResolution.NEW_SEARCH, effective=effective, control=control)
    if contradictory(effective.conditions):
        return refused_for_its_receipt(lambda: echo_items(anchor, effective, None, record, anchor_expired=expired))
    try:
        got = summary(echo_items(anchor, effective, None, record, anchor_expired=expired))
    except Exception as error:  # noqa: BLE001
        return [f"refused: {error}"]
    owed = provenance_owed(effective.conditions) | plan_owed(effective.conditions)
    if anchor is not None and not expired and control is None:
        owed.add(("CONTEXT_RESET", None, ()))
    return [] if set(got) == owed and len(got) == len(owed) else [f"{sorted(got, key=repr)} != {sorted(owed, key=repr)}"]


def page_problems(echo_items, effective, expired) -> list[str]:
    record = make_record(fa.TurnResolution.PROTOCOL_PAGE, base_trace_id="t0", effective=effective)
    try:
        got = echo_items(effective, effective, None, record, anchor_expired=expired)
    except Exception as error:  # noqa: BLE001
        return [f"refused: {error}"]
    return [] if got == () else [f"a page echoed {got}"]


@PROPS
@given(st.one_of(st_case(), st_case(rich=False)))
def test_a_turn_that_executes_nothing_echoes_nothing_but_the_expiry(case):
    base, patch, expired = case
    outcome = fo.delta_outcome(base, patch, base_trace_id="t0", anchor_expired=expired)
    if outcome.disposition.value == "EXECUTE":
        return
    resolution = fa.TurnResolution.CLARIFY if outcome.disposition.value == "CLARIFY" else fa.TurnResolution.INFORM
    assert silent_problems(fe.echo_items, base, expired, resolution) == []


@PROPS
@given(st_base(), st.one_of(st.none(), st_base()), st.booleans(), st.sampled_from([None, fa.PendingControl.CANCEL_PENDING]))
def test_a_fresh_acceptance_echoes_its_own_provenance_and_plan_and_the_reset(effective, anchor, expired, control):
    assert fresh_problems(fe.echo_items, effective, anchor, expired, control) == []


@PROPS
@given(st_base(), st.booleans())
def test_a_page_is_the_same_plan_again_and_echoes_nothing(effective, expired):
    assert page_problems(fe.echo_items, effective, expired) == []


ANSWER_TURNS = st.one_of(st_answer_turn(), st_case(), st_case(rich=False))


@PROPS
@given(ANSWER_TURNS)
def test_the_answer_is_exactly_the_reference_against_the_anchor(case):
    base = case[0]
    for pending, answer, second in answers_of(case):
        assert answer_problems(fe.echo_items_for_answer, base, pending, answer, second) == []


def differential_problems(module, case) -> list[str]:
    """For every answer that ONE composition over the anchor reproduces, the answer entry's echo is the delta entry's."""
    base, out = case[0], []
    for pending, answer, second in answers_of(case):
        one = one_step_of(base, pending, answer, second)
        if one is None or contradictory(second.effective.conditions):
            continue
        combined, composed = one
        record = make_record(fa.TurnResolution.DELTA, patch=combined, effective=composed.effective, base_trace_id="t0")
        try:
            as_delta = module.echo_items(base, composed.effective, combined, record, anchor_expired=False)
            as_answer = module.echo_items_for_answer(
                base, pending.frame, pending.frame_ops, second.effective,
                make_record(fa.TurnResolution.PENDING_ANSWER, patch=answer, effective=second.effective, base_trace_id="t1"),
                anchor_expired=False)
        except Exception as error:  # noqa: BLE001
            out.append(f"refused: {type(error).__name__}: {error}")
            continue
        if as_answer != as_delta:
            out.append("the answer's echo is not the delta's over the same effective")
    return out


PROVENANCE_CLASSES = {"UNIT_INHERITED", "UNIT_SHARED_IN_RANGE", "YEAR_INHERITED", "YEAR_CLOCK_INFERRED",
                      "RELATIVE_WINDOW_RESOLVED"}


def no_anchor_problems(module, case) -> list[str]:
    """A pending WITHOUT an anchor (M2A7-D22, D24): the «previo» is the frame, so the answer, and the ``DELTA`` that cancels the
    pending, are read like a delta over that frame (Gold §7 literal) EXCEPT for the provenance, which is P's: the asking turn
    echoed nothing, so every provenance of the effective is announced, as the new search with those conditions would."""
    frame, patch, _ = case
    frame = replace(frame, base_trace_id=None)  # the frame of a first turn was composed over nothing
    composed = composed_of(frame, patch)
    if composed is None or contradictory(composed.effective.conditions):
        return []
    effective, ok = composed.effective, {"anchor_expired": False}
    delta = make_record(fa.TurnResolution.DELTA, patch=patch, effective=effective, base_trace_id="t0")
    out = []
    try:
        as_delta = summary(module.echo_items(frame, effective, patch, delta, **ok))
        want = {item for item in as_delta if item[0] not in PROVENANCE_CLASSES} | provenance_owed(effective.conditions)
        for resolution, control in ((fa.TurnResolution.PENDING_ANSWER, None),
                                    (fa.TurnResolution.DELTA, fa.PendingControl.CANCEL_PENDING)):
            record = make_record(resolution, patch=patch, effective=effective, base_trace_id="t0", control=control)
            got = summary(module.echo_items_for_answer(None, frame, (), effective, record, **ok))
            if set(got) != want or len(got) != len(want):
                out.append(f"{resolution.value}/{control}: {sorted(set(got) ^ want, key=repr)}")
    except Exception as error:  # noqa: BLE001
        out.append(f"refused: {type(error).__name__}: {error}")
    return out


EFFECT_CLASSES = {"VALUE_RANGE_REPLACED", "POLARITY_INVERTED", "TOPIC_REPLACED", "UNIVERSE_CHANGED"}


def effect_oracle(anchor, effective, ops, placed_ids) -> set[tuple]:
    """The effect classes owed for a turn (or two) that took ``anchor`` to ``effective`` by ``ops``, from the STATES and the
    operations alone (Gold §6, contract §11), never from the module's diff. ``placed_ids``: the ids of the conditions the
    operations put that stand in ``effective`` (the reducer's own account). A result that does not stand in ``effective``
    announces nothing: a bound that replaced a side and was then cleared, a negation that inverted a value and was then cleared."""
    standing = list(effective.conditions)
    placed = [c for c in standing if c.condition_id in placed_ids]
    eff_ids = {c.condition_id for c in standing}
    cleared = {slot for op in ops if op.op is fa.PatchOpKind.CLEAR for slot in fd.covered(op.slot)}
    written = {op.slot for op in ops if op.op in fd.PLACERS}
    wide_clear = any(op.op is fa.PatchOpKind.CLEAR and op.slot == fa.Slot(D.VALUE) for op in ops)
    out = set()
    placed_value = tuple(c.condition_id for c in placed if c.dimension is D.VALUE)
    sides = {c.slot for c in standing if c.dimension is D.VALUE}
    for old in anchor.conditions:
        if old.condition_id in eff_ids:
            continue
        if old.dimension is D.VALUE and old.slot not in sides and placed_value:
            uncited = old.slot not in cleared and old.slot not in written
            if uncited or wide_clear:
                out.add(("VALUE_RANGE_REPLACED", old.slot.key, (old.condition_id, *placed_value)))
        if (old.dimension in POLARIZED_SLOT_DIMENSIONS and old.slot.polarity is Polarity.POSITIVE
                and old.slot not in cleared and old.slot not in written):
            negative = fa.Slot(old.dimension, Polarity.NEGATIVE)
            taken = tuple(c.condition_id for c in placed if c.slot == negative
                          and fa.canonical_value(c) == fa.canonical_value(old))
            if taken:
                out.add(("POLARITY_INVERTED", negative.key, (old.condition_id, *taken)))
    positive = fa.Slot(D.TOPIC, Polarity.POSITIVE)
    new_topics = [c for c in placed if c.slot == positive]
    gone = [c for c in anchor.conditions if c.slot == positive and c.condition_id not in eff_ids]
    if new_topics and any(old.value not in {c.value for c in new_topics} for old in gone):
        out.add(("TOPIC_REPLACED", positive.key, tuple(c.condition_id for c in gone) + tuple(c.condition_id for c in new_topics)))
    if [(r.subject, r.universe_kind) for r in anchor.requests] != [(r.subject, r.universe_kind) for r in effective.requests]:
        anchor_ids = {c.condition_id for c in anchor.conditions}
        out.add(("UNIVERSE_CHANGED", "UNIVERSE", tuple(c.condition_id for c in standing
                                                       if c.dimension is D.TIME and c.condition_id in anchor_ids
                                                       and c.condition_id not in placed_ids)))
    return out


@st.composite
def st_undo_step(draw):
    """What a frame did and the turn that executes UNDOES: a frame that inverts a value (F16) or substitutes a bound (A8.1)
    and an answer, or a cancelling delta, that clears the negation or the bound. The result must not be announced."""
    if draw(st.booleans()):
        anchor = interp(topic("c1", draw(st.sampled_from(["aseo", "vias"]))), buyer("c2"), trace="t0")
        frame_ops = (set_(topic("p1", anchor.conditions[0].value[1].split("=")[1], excluded=True, trace="t1", n=1)),)
        clearing = clear(draw(st.sampled_from([fa.Slot(D.TOPIC), fa.Slot(D.TOPIC, Polarity.NEGATIVE)])))
    else:
        anchor = interp(topic("c1", "aseo"), money("c2", 500, Comparator.LT), trace="t0")
        frame_ops = (set_(money("p1", 900, Comparator.GTE, trace="t1", n=1)),)
        clearing = clear(draw(st.sampled_from([fa.Slot(D.VALUE), LOWER])))
    first = fd.compose_ops(anchor, frame_ops, base_trace_id="t0")
    extra = (set_(buyer("p2", "VALLE", trace="t1", n=2)),) if draw(st.booleans()) else ()
    return (anchor, frame_ops, first.effective, first.placed_ids, fa.SemanticPatch((clearing, *extra), fa.PatchOriginKind.TEXT),
            draw(st.booleans()))


@st.composite
def st_arbitrary_two_step(draw):
    """ARBITRARY turns over an anchor, not only the candidates the reducer builds: 0-2 operations that make the frame and 1-3
    that answer it (or, as a cancelling delta, compose over it), drawn with the reducer's own operation generator."""
    anchor = draw(st_base(draw(st.booleans())))
    frame_ops = tuple(draw(st_op(anchor, i)) for i in range(draw(st.integers(0, 2))))
    first = fd.compose_ops(anchor, frame_ops, base_trace_id="t0") if frame_ops else None
    if frame_ops and not isinstance(first, fd.Composed):
        return None
    frame = first.effective if first else anchor
    answer = fa.SemanticPatch(tuple(draw(st_op(frame, 10 + i)) for i in range(draw(st.integers(1, 3)))), fa.PatchOriginKind.TEXT)
    return anchor, frame_ops, frame, (first.placed_ids if first else frozenset()), answer, draw(st.booleans())


def st_two_step():
    return st.one_of(st_arbitrary_two_step(), st_undo_step())


def two_step_problems(module, case) -> list[str]:
    """An answer or a cancelling delta of ANY operations over a frame of ANY operations: the exact classes (the reference of
    ``answer_problems``) and the effect classes judged by ``effect_oracle`` over the executed effective."""
    if case is None:
        return []
    anchor, frame_ops, frame, first_placed, answer, cancelling = case
    second = fd.compose(frame, answer, base_trace_id="t1")
    if not isinstance(second, fd.Composed):
        return []
    effective = second.effective
    record = make_record(fa.TurnResolution.DELTA if cancelling else fa.TurnResolution.PENDING_ANSWER, patch=answer,
                         effective=effective, base_trace_id="t1",
                         control=fa.PendingControl.CANCEL_PENDING if cancelling else None)
    pending = types.SimpleNamespace(frame=frame, frame_ops=frame_ops)
    out = answer_problems(module.echo_items_for_answer, anchor, pending, answer, second)
    if contradictory(effective.conditions):
        return out
    try:
        got = summary(module.echo_items_for_answer(anchor, frame, frame_ops, effective, record, anchor_expired=False))
    except Exception as error:  # noqa: BLE001
        return out + [f"refused: {type(error).__name__}: {error}"]
    standing_ids = {c.condition_id for c in effective.conditions}
    placed_ids = second.placed_ids | (first_placed & (standing_ids - second.placed_ids))
    want = effect_oracle(anchor, effective, (*frame_ops, *answer.ops), placed_ids)
    have = {item for item in got if item[0] in EFFECT_CLASSES}
    return out + [f"effect missing {item}" for item in sorted(want - have, key=repr)] + [
        f"effect unjustified {item}" for item in sorted(have - want, key=repr)]


@PROPS
@given(st_two_step())
def test_an_arbitrary_answer_or_cancelling_delta_is_judged_by_the_states_of_the_executed_effective(case):
    assert two_step_problems(fe, case) == []


def test_the_arbitrary_two_step_generator_reaches_every_effect_class():
    seen: dict[str, int] = {}

    @settings(max_examples=2500, deadline=None, database=None, derandomize=True,
              suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
    @given(st_two_step())
    def count(case):
        if case is None:
            return
        anchor, frame_ops, frame, first_placed, answer, cancelling = case
        second = fd.compose(frame, answer, base_trace_id="t1")
        if isinstance(second, fd.Composed):
            ids = {c.condition_id for c in second.effective.conditions}
            for item in effect_oracle(anchor, second.effective, (*frame_ops, *answer.ops),
                                      second.placed_ids | (first_placed & (ids - second.placed_ids))):
                seen[item[0]] = seen.get(item[0], 0) + 1
            seen["composed"] = seen.get("composed", 0) + 1

    count()
    assert seen.get("composed", 0) >= 300 and all(seen.get(name, 0) >= 3 for name in EFFECT_CLASSES), seen


def cancelling_problems(module, case) -> list[str]:
    """With an anchor, the ``DELTA`` that cancels the pending is read exactly like the answer: two steps against the anchor."""
    base, out = case[0], []
    for pending, answer, second in answers_of(case):
        if contradictory(second.effective.conditions):
            continue
        ok = {"anchor_expired": False}
        args = (base, pending.frame, pending.frame_ops, second.effective)
        try:
            answered = module.echo_items_for_answer(*args, make_record(
                fa.TurnResolution.PENDING_ANSWER, patch=answer, effective=second.effective, base_trace_id="t1"), **ok)
            cancelled = module.echo_items_for_answer(*args, make_record(
                fa.TurnResolution.DELTA, patch=answer, effective=second.effective, base_trace_id="t1",
                control=fa.PendingControl.CANCEL_PENDING), **ok)
        except Exception as error:  # noqa: BLE001
            out.append(f"refused: {type(error).__name__}: {error}")
            continue
        if answered != cancelled:
            out.append("the cancelling delta is not read like the answer")
    return out


@PROPS
@given(st.one_of(st_difference_turn(), st_case(), st_case(rich=False)))
def test_a_pending_without_an_anchor_is_read_against_its_frame(case):
    assert no_anchor_problems(fe, case) == []


@PROPS
@given(ANSWER_TURNS)
def test_the_cancelling_delta_is_read_like_the_answer(case):
    assert cancelling_problems(fe, case) == []


@PROPS
@given(ANSWER_TURNS)
def test_the_answer_is_the_delta_when_one_composition_gives_the_same_effective(case):
    assert differential_problems(fe, case) == []


def test_the_generated_pendings_reach_answers_with_and_without_frame_operations():
    seen = {"answers": 0, "with_frame_ops": 0, "one_step": 0}

    @settings(max_examples=2500, deadline=None, database=None, derandomize=True,
              suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
    @given(ANSWER_TURNS)
    def count(case):
        for pending, answer, second in answers_of(case):
            seen["answers"] += 1
            seen["with_frame_ops"] += bool(pending.frame_ops)
            seen["one_step"] += one_step_of(case[0], pending, answer, second) is not None

    count()
    assert seen["answers"] >= 200 and seen["with_frame_ops"] >= 100 and seen["one_step"] >= 30, seen


# ---- the generator reaches what the claims are about (a property over nothing proves nothing) -----------------------
def test_the_generated_turns_reach_every_representable_class():
    seen: set[str] = set()

    def collect(strategy):
        @settings(max_examples=2500, deadline=None, database=None, derandomize=True,
                  suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
        @given(strategy)
        def run_all(case):
            base, patch, _ = case
            composed = composed_of(base, patch)
            if composed is not None and not contradictory(composed.effective.conditions):
                record = make_record(fa.TurnResolution.DELTA, patch=patch, effective=composed.effective,
                                     base_trace_id="t0")
                seen.update(item.echo_class.value for item in fe.echo_items(
                    base, composed.effective, patch, record, anchor_expired=False))
        run_all()

    for strategy in (st_case(), st_case(rich=False), st_value_turn(), st_plan_turn(), st_difference_turn()):  # each alone
        collect(strategy)
    # CONTEXT_RESET / CONTEXT_EXPIRED need a record, not a composition: the fresh and silent properties reach them
    reachable = {c.value for c in fa.EchoClass} - {c.value for c in fe.NOT_REPRESENTABLE} - {"CONTEXT_RESET", "CONTEXT_EXPIRED"}
    assert reachable - seen == set(), sorted(reachable - seen)


@pytest.mark.parametrize("echo_class", sorted(EXACT | EFFECT_DERIVED))
def test_the_reference_knows_each_class_it_judges(echo_class):
    assert echo_class in {c.value for c in fa.EchoClass}
