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

from dataclasses import replace

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
import app.core.foundation_echo as fe
from app.core.interpretation_contract import Polarity
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_foundation_delta_properties import st_base, st_case
from app.core.foundation_authority_values import Comparator
from app.core.foundation_authority_values import OrderKey
from tests.test_foundation_echo import (
    LOWER, SOURCE_OWES, UPPER, buyer, clear, interp, make_record, money, order, set_, set_order, summary, time, top, topic,
)

PROPS = settings(max_examples=250, deadline=None, suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
PATCHED_RESOLUTIONS = (fa.TurnResolution.DELTA, fa.TurnResolution.PENDING_ANSWER, fa.TurnResolution.PROTOCOL_EVENT)
EFFECT_DERIVED = {"POLARITY_INVERTED", "TOPIC_REPLACED", "UNIVERSE_CHANGED", "VALUE_RANGE_REPLACED"}
EXACT = {"CARRIED", "UNIT_INHERITED", "UNIT_SHARED_IN_RANGE", "YEAR_INHERITED", "YEAR_CLOCK_INFERRED",
         "RELATIVE_WINDOW_RESOLVED", "TOPN_CARRIED", "TOPN_CAPPED", "ORDER_DIRECTION_DEFAULTED"}
RELATIVE_RULE = "time.relative_to_previous"


# ---- the reference: from the contract and from compose's own account, never from the module ------------------------
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
    try:
        items = echo_items(base, effective, patch, record)
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
    classes = {i.echo_class.value for i in fe.echo_items(base, composed.effective, patch, record)}
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
    first = fe.echo_items(base, composed.effective, patch, make_record(
        fa.TurnResolution.DELTA, patch=patch, effective=composed.effective, base_trace_id="t0"))
    second = fe.echo_items(base, other.effective, shuffled, make_record(
        fa.TurnResolution.DELTA, patch=shuffled, effective=other.effective, base_trace_id="t0"))
    assert first == second


@PROPS
@given(st.one_of(st_case(), st_case(rich=False)))
def test_the_record_accepts_the_echo_and_reads_it_back(case):
    base, patch, _ = case
    composed = composed_of(base, patch)
    if composed is None:
        return
    record = make_record(fa.TurnResolution.DELTA, patch=patch, effective=composed.effective, base_trace_id="t0")
    items = fe.echo_items(base, composed.effective, patch, record)
    stored = replace(record, echo=items)
    assert fa.AcceptedTurnRecord.from_dict(stored.to_dict()) == stored
    assert fe.echo_items(base, composed.effective, patch, stored) == items


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


def fresh_problems(echo_items, effective, anchor, expired) -> list[str]:
    """A fresh acceptance owes its own provenance and plan, and CONTEXT_RESET over a standing anchor; no difference."""
    record = make_record(fa.TurnResolution.NEW_SEARCH, effective=effective)
    try:
        got = summary(echo_items(anchor, effective, None, record, anchor_expired=expired))
    except Exception as error:  # noqa: BLE001
        return [f"refused: {error}"]
    owed = provenance_owed(effective.conditions) | plan_owed(effective.conditions)
    if anchor is not None and not expired:
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
@given(st_base(), st.one_of(st.none(), st_base()), st.booleans())
def test_a_fresh_acceptance_echoes_its_own_provenance_and_plan_and_the_reset(effective, anchor, expired):
    assert fresh_problems(fe.echo_items, effective, anchor, expired) == []


@PROPS
@given(st_base(), st.booleans())
def test_a_page_is_the_same_plan_again_and_echoes_nothing(effective, expired):
    assert page_problems(fe.echo_items, effective, expired) == []


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
            if composed is not None:
                record = make_record(fa.TurnResolution.DELTA, patch=patch, effective=composed.effective,
                                     base_trace_id="t0")
                seen.update(item.echo_class.value for item in fe.echo_items(base, composed.effective, patch, record))
        run_all()

    for strategy in (st_case(), st_case(rich=False), st_value_turn(), st_plan_turn()):  # each on its own
        collect(strategy)
    # CONTEXT_RESET / CONTEXT_EXPIRED need a record, not a composition: the fresh and silent properties reach them
    reachable = {c.value for c in fa.EchoClass} - {c.value for c in fe.NOT_REPRESENTABLE} - {"CONTEXT_RESET", "CONTEXT_EXPIRED"}
    assert reachable - seen == set(), sorted(reachable - seen)


@pytest.mark.parametrize("echo_class", sorted(EXACT | EFFECT_DERIVED))
def test_the_reference_knows_each_class_it_judges(echo_class):
    assert echo_class in {c.value for c in fa.EchoClass}
