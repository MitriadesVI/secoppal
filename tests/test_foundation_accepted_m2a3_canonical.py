"""M2A-3 phases 1/1b: interpretation-level invariants and the canonical form shared by every constructor of an
effective (``app/core/foundation_accepted.py``; ``docs/audits/m2a_conversation/M2A3_CANONICAL_FORM.md``; user decisions
2026-10-02).

Invariants proved here:

1. ``conditions``: grouped by slot in the closed, explicit table ``CONDITION_SLOT_ORDER`` (the order contract §4.2
   lists the slots; never alphabetical), each §4.2 condition slot exactly once; inside a slot the acceptance order is
   KEPT (it is observable). The constructor and the reader reject any other order; they never reorder.
2. ``policies``: the split form ``main`` emits — one application (policy, ONE condition) per record; a joint
   multi-condition policy is refused; every cited condition exists (a ``CLEAR`` never leaves a dangling policy); an
   application appears once — ``(p1, p1)`` is REJECTED, never collapsed, whatever the order verdict; one ref has one
   effect; the records are sorted. O1B / ``opportunity-explicit-state`` cite the REQUESTED_UNIVERSE condition, which is
   not a slot: they cannot be persisted (§6.6 inventory, fail-closed).
3. ``logic``: only what contract §3.2 decides — a pure conjunction is ``None``. Any tree with OR/NOT is kept exactly as
   given (structure and order): limited guarantee, «same meaning ⇒ same bytes» is NOT promised inside OR/NOT.
4. ``canonical_interpretation`` is the one constructor path: a cross-slot permutation of the conditions or any
   permutation of the policies gives the same bytes and ``interpretation_hash``; ``requests`` keep their order;
   nothing is deduplicated, merged or dropped.

Every battery runs against MUTANTS of the module (the ``exec`` technique of ``test_foundation_accepted_contract.py``):
a check that does not kill its mutant does not count. The executed evidence for which orders are observable is
``tests/test_m2a3_order_observability.py`` (the real projection).
"""
from __future__ import annotations

import copy
import itertools
import json
import sys
import types
from dataclasses import replace
from datetime import timedelta

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import (
    POLARIZED_SLOT_DIMENSIONS, SLOT_DIMENSIONS, ConditionExpression, ContractError, Effect, HostPolicy, LogicOperator,
    Polarity, ValueSide,
)
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_foundation_accepted_contract import PROPS, SOURCE, T0, VALUES, condition, record, request

AND, OR, NOT, ATOM = LogicOperator.AND, LogicOperator.OR, LogicOperator.NOT, LogicOperator.ATOM


def atom(cid: str) -> ConditionExpression:
    return ConditionExpression(ATOM, cid)


def node(operator: LogicOperator, *children: ConditionExpression) -> ConditionExpression:
    return ConditionExpression(operator, None, tuple(children))


def _code(build) -> str | None:
    """The ``ContractError`` code a build raises, or ``None`` if it is accepted."""
    try:
        build()
    except ContractError as error:
        return str(error)
    return None


# ---- builders (parameterized by the module: a mutant builds its own instances) -------------------------------------
P_TOPN = HostPolicy("topn", "1", ("c4",), Effect.NARROWING)
P_ACTOR = HostPolicy("actor_state", "1", ("c1",), Effect.EQUIVALENT)
P_ORDER = HostPolicy("order_default", "1", ("c3",), Effect.EQUIVALENT)
POLICIES = (P_ACTOR, P_ORDER, P_TOPN)  # canonical: sorted by (policy_id, version, condition_ids, effect)


def conditions(m, rids=("r1",)):
    """TOPIC/POSITIVE, VALUE/lower, ORDER, CARDINALITY: already in ``CONDITION_SLOT_ORDER``."""
    return (condition(m, D.TOPIC, 0, "c1", rids), condition(m, D.VALUE, 0, "c2", rids),
            condition(m, D.ORDER, 0, "c3", rids), condition(m, D.CARDINALITY, 1, "c4", rids))


def build(m, policies=POLICIES, conds=None, logic=None, requests=None):
    return m.AcceptedInterpretation(requests or (request(m),), conditions(m) if conds is None else conds, logic,
                                    policies, None, "root-1")


def flat_conditions(m):
    """Four conditions with no single-slot dimension (any logic tree over them is admissible), in table order."""
    return (condition(m, D.TOPIC, 0, "c1"), condition(m, D.BUYER, 0, "c2"), condition(m, D.GEOGRAPHY, 0, "c3"),
            condition(m, D.TIME, 0, "c4"))


# ---- 1. the slot table -----------------------------------------------------------------------------------------------
# Contract §4.2, in the order its table lists the slots (UNIVERSE is the request-level slot: no condition occupies it).
CONTRACT_4_2_CONDITION_SLOTS = (
    "TOPIC/POSITIVE", "TOPIC/NEGATIVE", "BUYER/POSITIVE", "BUYER/NEGATIVE", "ACTOR/POSITIVE", "ACTOR/NEGATIVE",
    "GEOGRAPHY/POSITIVE", "GEOGRAPHY/NEGATIVE", "TIME/POSITIVE", "TIME/NEGATIVE", "STATE/POSITIVE", "STATE/NEGATIVE",
    "MODALITY/POSITIVE", "MODALITY/NEGATIVE", "CONTRACT_TYPE_OR_NATURE/POSITIVE", "CONTRACT_TYPE_OR_NATURE/NEGATIVE",
    "VALUE/lower", "VALUE/upper", "ORDER", "CARDINALITY",
)


def test_the_slot_table_lists_every_condition_slot_of_4_2_exactly_once():
    keys = [slot.key for slot in fa.CONDITION_SLOT_ORDER]
    assert type(fa.CONDITION_SLOT_ORDER) is tuple and len(keys) == len(set(keys))
    assert tuple(keys) == CONTRACT_4_2_CONDITION_SLOTS  # explicit, in the contract's order (not alphabetical)
    every = {fa.Slot(d, p).key for d in POLARIZED_SLOT_DIMENSIONS for p in Polarity}
    every |= {fa.Slot(D.VALUE, side=s).key for s in ValueSide}
    every |= {fa.Slot(d).key for d in SLOT_DIMENSIONS - POLARIZED_SLOT_DIMENSIONS - {D.VALUE}}
    assert set(keys) == every
    assert fa.UNIVERSE_SLOT not in fa.CONDITION_SLOT_ORDER and keys != sorted(keys)
    # every slot a typed value can occupy has a rank (slot_of never yields a dimension-wide or UNIVERSE slot)
    for dimension, values in VALUES.items():
        for value in values:
            assert fa.slot_of(dimension, value.encode()) in fa.CONDITION_SLOT_ORDER


def condition_order_failures(m) -> list[str]:
    out = []
    t1, t2 = condition(m, D.TOPIC, 0, "t1"), condition(m, D.TOPIC, 0, "t2")
    buyer, time, low = condition(m, D.BUYER, 0, "b"), condition(m, D.TIME, 1, "tn"), condition(m, D.VALUE, 0, "lo")
    up, order, card = condition(m, D.VALUE, 2, "up"), condition(m, D.ORDER, 0, "o"), condition(m, D.CARDINALITY, 1, "n")
    canonical = (t2, t1, buyer, time, low, up, order, card)  # TOPIC/POSITIVE keeps the given t2, t1
    shuffled = (card, time, t2, up, order, buyer, t1, low)
    try:
        made = m.canonical_interpretation((request(m),), shuffled, None, (), None, "root-1")
        if made.conditions != canonical:
            out.append(f"constructor order {[c.condition_id for c in made.conditions]}")
    except ContractError as error:
        out.append(f"constructor refuses a valid set: {error}")
    for label, conds in (("table order", canonical), ("the other in-slot order", (t1, t2, *canonical[2:])),
                         ("the 1a fixture order", conditions(m))):
        if (code := _code(lambda: build(m, (), conds))) is not None:
            out.append(f"type refuses {label}: {code}")
    for label, conds in (("cross-slot permutation", shuffled), ("CARDINALITY before ORDER", (*canonical[:6], card,
                                                                                            order)),
                         ("alphabetical slots", tuple(sorted(canonical, key=lambda c: c.slot.key)))):
        if conds != canonical and _code(lambda: build(m, (), conds)) != "NONCANONICAL_CONDITION_ORDER":
            out.append(f"type accepts {label}")
    try:
        payload = build(m, (), canonical).to_dict()
    except ContractError as error:
        return [*out, f"the canonical document cannot be built: {error}"]
    tampered = copy.deepcopy(payload)
    tampered["conditions"] = [payload["conditions"][i] for i in (2, 0, 1, 3, 4, 5, 6, 7)]
    if _code(lambda: m.AcceptedInterpretation.from_dict(tampered)) is None:
        out.append("reader accepts a cross-slot permutation")
    return out


def test_conditions_are_grouped_by_slot_and_keep_the_acceptance_order_inside_a_slot():
    assert condition_order_failures(fa) == []


# ---- 2. policies -----------------------------------------------------------------------------------------------------
def policy_failures(m) -> list[str]:
    out = []
    effective = build(m)
    if m.AcceptedInterpretation.from_dict(json.loads(json.dumps(effective.to_dict()))) != effective:
        out.append("the canonical policies do not round-trip")
    refused = {
        "(p1, p1)": ((P_ACTOR, P_ACTOR, P_ORDER, P_TOPN), "DUPLICATE_POLICY"),
        "a policy on a missing condition": ((P_ACTOR, HostPolicy("ghost", "1", ("c9",), Effect.EQUIVALENT)),
                                            "FOREIGN_POLICY_CONDITION"),
        "one ref with two effects": ((HostPolicy("x", "1", ("c1",), Effect.EQUIVALENT),
                                      HostPolicy("x", "1", ("c2",), Effect.NARROWING)), "CONFLICTING_POLICY_EFFECT"),
        "two effects behind one projected ref": ((HostPolicy("a", "b@c", ("c1",), Effect.EQUIVALENT),
                                                  HostPolicy("a@b", "c", ("c2",), Effect.NARROWING)),
                                                 "CONFLICTING_POLICY_EFFECT"),
        "unsorted policies": ((P_TOPN, P_ORDER, P_ACTOR), "UNSORTED_POLICIES"),
        "a joint multi-condition policy": ((HostPolicy("x", "1", ("c1", "c2"), Effect.EQUIVALENT),),
                                           "MULTI_CONDITION_POLICY"),
    }
    for label, (policies, code) in refused.items():
        if _code(lambda: build(m, policies)) != code:
            out.append(f"constructor: {label} not refused as {code}")
    if _code(lambda: build(m, conds=conditions(m)[:3])) != "FOREIGN_POLICY_CONDITION":
        out.append("a policy left dangling by a removed condition is accepted")
    payload = effective.to_dict()
    for label, change in (("repeated", lambda items: items[:1] + items), ("reversed", lambda items: items[::-1]),
                          ("merged", lambda items: [dict(items[0], condition_ids=["c1", "c3"])] + items[1:])):
        tampered = copy.deepcopy(payload)
        tampered["policies"] = change(payload["policies"])
        if _code(lambda: m.AcceptedInterpretation.from_dict(tampered)) is None:
            out.append(f"reader accepts {label} policies")
    # positive controls: independent applications stay representable, split
    for label, policies in (("two refs on one condition", (P_ACTOR, HostPolicy("zz", "1", ("c1",), Effect.WIDENING))),
                            ("one ref applied to two conditions",
                             (HostPolicy("x", "1", ("c1",), Effect.EQUIVALENT),
                              HostPolicy("x", "1", ("c2",), Effect.EQUIVALENT)))):
        if (code := _code(lambda: build(m, policies))) is not None:
            out.append(f"positive control {label} refused: {code}")
    return out


def test_policies_are_split_set_like_and_never_duplicated_or_dangling():
    assert policy_failures(fa) == []


def test_the_request_universe_policies_cannot_be_persisted():
    """§6.6 inventory: O1B and ``opportunity-explicit-state`` (``foundation_admission.py:84``) cite the
    REQUESTED_UNIVERSE condition. It is not a slot, so it never becomes an ``AcceptedCondition`` and the policy has
    nothing to cite: fail-closed until its owner gives it a representation (M2A-1b / universe policy)."""
    universe = fa.ConditionOrigin(fa.OriginKind.SPAN, "t1", "a" * 64, (0, 3))
    topic = condition(fa, D.TOPIC, 0, "c1")
    with pytest.raises(ContractError, match="CONDITION_DIMENSION_NOT_A_SLOT"):
        fa.AcceptedCondition("u1", ("r1",), topic.slot, D.RETRIEVAL_INTENT, "is", topic.polarity, None, topic.value,
                             "host-universe", (), (), universe, fa.Provenance())
    for policy_id in ("O1B", "opportunity-explicit-state"):
        with pytest.raises(ContractError, match="FOREIGN_POLICY_CONDITION"):
            build(fa, (HostPolicy(policy_id, "1", ("u1",), Effect.EQUIVALENT),), conds=(topic,))


# ---- 3. logic --------------------------------------------------------------------------------------------------------
PURE_SHAPES = (node(AND, atom("c1"), atom("c2"), atom("c3"), atom("c4")),
               node(AND, atom("c4"), atom("c3"), atom("c2"), atom("c1")),
               node(AND, node(AND, atom("c1"), atom("c2")), node(AND, atom("c4"), atom("c3"))))
OR_TREES = (node(AND, node(OR, atom("c1"), atom("c2")), atom("c4"), atom("c3")),
            node(AND, atom("c3"), atom("c4"), node(OR, atom("c2"), atom("c1"))),
            node(AND, node(AND, atom("c4"), atom("c3")), node(OR, atom("c1"), atom("c2"))),
            node(OR, node(AND, atom("c4"), atom("c3")), atom("c2"), atom("c1")),
            node(AND, node(NOT, node(AND, atom("c2"), atom("c1"))), atom("c4"), atom("c3")))


def logic_failures(m) -> list[str]:
    out = []
    conds = flat_conditions(m)
    for shape in PURE_SHAPES:
        if m.canonical_logic(shape) is not None:
            out.append(f"a pure conjunction is not None: {m.expression_to_dict(shape)}")
        if _code(lambda: build(m, (), conds, shape)) != "NONCANONICAL_LOGIC":
            out.append("the type accepts a pure conjunction written as a tree")
    for tree in OR_TREES:  # kept exactly as given: structure and order (no evidence to rewrite them)
        if m.canonical_logic(tree) != tree:
            out.append(f"canonical_logic rewrites the OR/NOT tree {m.expression_to_dict(tree)}")
        if (code := _code(lambda: build(m, (), conds, tree))) is not None:
            out.append(f"the type refuses the OR/NOT tree {m.expression_to_dict(tree)}: {code}")
    try:
        made = build(m, (), conds, OR_TREES[0])
        again = m.AcceptedInterpretation.from_dict(json.loads(json.dumps(made.to_dict())))
        if again != made or again.logic != OR_TREES[0]:
            out.append("an OR/NOT tree does not round-trip verbatim")
    except ContractError as error:
        out.append(f"an OR/NOT tree is refused: {error}")
    payload = build(m, (), conds, None).to_dict()
    tampered = dict(payload, logic=m.expression_to_dict(PURE_SHAPES[0]))
    if _code(lambda: m.AcceptedInterpretation.from_dict(tampered)) is None:
        out.append("reader accepts a pure conjunction written as a tree")
    return out


def test_logic_has_one_form_only_where_the_contract_decides_it():
    assert logic_failures(fa) == []


# ---- 4. the shared constructor path ----------------------------------------------------------------------------------
def constructor_failures(m) -> list[str]:
    out = []
    reference = m.canonical_interpretation((request(m),), conditions(m), None, POLICIES, None, "root-1")
    expected = m.interpretation_hash(reference)
    for order in itertools.permutations(POLICIES):
        for conds in (conditions(m), conditions(m)[::-1]):
            try:
                made = m.canonical_interpretation((request(m),), conds, None, order, None, "root-1")
            except ContractError as error:
                out.append(f"policies {[p.policy_id for p in order]} refused: {error}")
                continue
            if made.to_dict() != reference.to_dict() or m.interpretation_hash(made) != expected:
                out.append(f"policies {[p.policy_id for p in order]} / conditions change the bytes")
    if _code(lambda: m.canonical_interpretation((request(m),), conditions(m), None, (P_ACTOR, P_ACTOR), None,
                                                "root-1")) != "DUPLICATE_POLICY":
        out.append("the constructor collapses (p1, p1) instead of refusing it")
    conds = flat_conditions(m)
    try:
        for shape in PURE_SHAPES:
            if m.canonical_interpretation((request(m),), conds, shape, (), None, "root-1").logic is not None:
                out.append("the constructor keeps a pure conjunction as a tree")
        for tree in OR_TREES:
            if m.canonical_interpretation((request(m),), conds, tree, (), None, "root-1").logic != tree:
                out.append("the constructor rewrites an OR/NOT tree")
    except ContractError as error:
        out.append(f"the constructor refuses a valid logic: {error}")
    dual = (request(m, "r2", Subject.CONTRACTS), request(m, "r1", Subject.PROCESSES))
    shared = tuple(replace(c, request_ids=("r1", "r2")) for c in conds)
    try:
        if m.canonical_interpretation(dual, shared, None, (), None, "root-1").requests != dual:
            out.append("the constructor reorders the requests of a dual anchor")
    except ContractError as error:
        out.append(f"a contracts-first dual anchor refused: {error}")
    return out


def test_one_constructor_path_orders_only_what_is_unobservable():
    assert constructor_failures(fa) == []


# ---- 5. property: a cross-slot or policy permutation never changes the bytes ---------------------------------------
PAIRS = tuple((dimension, index) for dimension in (D.TOPIC, D.BUYER, D.ACTOR, D.GEOGRAPHY, D.TIME, D.STATE,
                                                    D.MODALITY, D.CONTRACT_TYPE_OR_NATURE)
              for index in range(len(VALUES[dimension])))
REFS = (("topn", "1", Effect.NARROWING), ("actor_state", "1", Effect.EQUIVALENT), ("order_default", "2",
                                                                                     Effect.WIDENING))


@st.composite
def st_tree(draw, cids, operators):
    if len(cids) == 1:
        return atom(cids[0])
    cut = draw(st.integers(1, len(cids) - 1))
    left, right = draw(st_tree(cids[:cut], operators)), draw(st_tree(cids[cut:], operators))
    if NOT in operators and draw(st.booleans()):
        left = node(NOT, left)
    return node(draw(st.sampled_from([op for op in operators if op is not NOT])), left, right)


@st.composite
def st_case(draw):
    """Plain data (materialized per module): slots, a cross-slot interleaving, policies, logic and a variant logic."""
    pairs = draw(st.lists(st.sampled_from(PAIRS), min_size=2, max_size=6))
    cids = [f"c{index}" for index in range(len(pairs))]
    # a random interleaving that keeps the relative order of the conditions of each slot
    slot_of = [fa.slot_of(d, VALUES[d][i].encode()).key for d, i in pairs]
    labels = draw(st.permutations(slot_of))
    queues = {key: [cid for cid, k in zip(cids, slot_of) if k == key] for key in set(slot_of)}
    interleaved = [queues[key].pop(0) for key in labels]
    policies = [HostPolicy(policy_id, version, (cid,), effect) for policy_id, version, effect in REFS
                for cid in sorted(draw(st.sets(st.sampled_from(cids), max_size=len(cids))))]
    kind = draw(st.sampled_from(["none", "pure", "or"]))
    if kind == "none":
        logic = variant = None
    elif kind == "pure":
        logic = draw(st_tree(draw(st.permutations(cids)), (AND,)))
        variant = draw(st_tree(draw(st.permutations(cids)), (AND,)))
    else:
        logic = draw(st_tree(draw(st.permutations(cids)), (AND, OR, NOT)).filter(
            lambda tree: tree.operator is not ATOM and not fa._conjunction_only(tree)))
        variant = logic  # OR/NOT: limited guarantee, only the identical tree is promised the same bytes
    return tuple(pairs), tuple(interleaved), tuple(policies), tuple(draw(st.permutations(policies))), logic, variant


def canonical_violation(m, case) -> str | None:
    pairs, interleaved, policies, permuted, logic, variant = case
    by_id = {f"c{index}": condition(m, d, i, f"c{index}") for index, (d, i) in enumerate(pairs)}
    conds = tuple(by_id.values())
    other = tuple(by_id[cid] for cid in interleaved)

    def make(cs, pol, log):
        return m.canonical_interpretation((request(m),), cs, log, pol, None, "root-1")

    try:
        first, second = make(conds, policies, logic), make(other, permuted, variant)
    except ContractError as error:
        return f"refused: {error}"
    if first.to_dict() != second.to_dict() or m.interpretation_hash(first) != m.interpretation_hash(second):
        return "a cross-slot / policy permutation changed the bytes"
    if m.AcceptedInterpretation.from_dict(json.loads(json.dumps(first.to_dict()))) != first:
        return "the canonical form does not round-trip"
    rank = {slot: position for position, slot in enumerate(CONTRACT_4_2_CONDITION_SLOTS)}
    if first.conditions != tuple(sorted(conds, key=lambda c: rank[c.slot.key])):
        return "conditions are not grouped by slot with the given in-slot order"
    raw = (other, permuted, variant)
    if raw != (first.conditions, first.policies, first.logic) and _code(
            lambda: m.AcceptedInterpretation((request(m),), *raw, None, "root-1")) is None:
        return "the type accepts a non-canonical form"
    return None


@PROPS
@given(st_case())
def test_property_canonical_form_is_invariant_to_unobservable_permutations(case):
    assert canonical_violation(fa, case) is None


# ---- 6. A.1 / A.3 as amended by R3 (phase 1c): what each disposition may NEVER carry ---------------------------------
# The positive side (every A.1/A.3 entry the contract requires) is ``tests/test_m2a_contract_r3_sweep.py``; this battery
# pins the negative side, so a widening of the closed lists is caught, not only a narrowing.
NOT_A_SLOT = ("RETRIEVAL_INTENT", "UNSUPPORTED", "CONTROL_LANGUAGE", "universe", "", "VALUE/lower")


def annex_failures(m) -> list[str]:
    out = []
    allowed, R = m.reason_code_allowed, m.TurnResolution
    dims = sorted(m.DIMENSION_KEYS)
    if set(dims) != {d.value for d in SLOT_DIMENSIONS} | {"UNIVERSE"}:
        out.append(f"<DIM> is not the slot dimensions plus UNIVERSE: {dims}")
    if m.UNSUPPORTED_PROPOSAL_MOTIVES != {"MULTI_REQUEST", "UNSUPPORTED_CONDITION", "UNSUPPORTED_SOURCE",
                                          "INVALID_PROPOSAL", "NO_ACTION"}:
        out.append("<motivo> is not the closed list of §7.1 row 4b")
    expect = {  # (code, resolution) -> allowed?
        **{(f"UNRESOLVED_VALUE:{d}", r): True for d in dims for r in (R.CLARIFY, R.INFORM)},
        **{(f"UNRESOLVED_VALUE:{d}", r): False for d in NOT_A_SLOT for r in (R.CLARIFY, R.INFORM)},
        **{(f"UNSUPPORTED_PROPOSAL:{x}", R.INFORM): True for x in m.UNSUPPORTED_PROPOSAL_MOTIVES},
        **{(f"UNSUPPORTED_PROPOSAL:{x}", R.INFORM): False for x in ("", "WHATEVER", "PROVIDER_ERROR",
                                                                      "OPERATION_CHANGE", "multi_request")},
        **{(f"UNSUPPORTED_PROPOSAL:{x}", R.CLARIFY): False for x in m.UNSUPPORTED_PROPOSAL_MOTIVES},
        **{(code, R.INFORM): False for code in ("CONTEXT_EXPIRED_CONFIRM", "NO_ANCHOR", "ANCHOR_UNREADABLE")},
        **{(code, R.CLARIFY): False for code in (*m.REASK_CODES, *m.INFORM_CODES)},
        **{(code, R.REASK): False for code in (*m.CLARIFY_CODES, *m.INFORM_CODES)},
        **{(code, R.INFORM): True for code in (*m.INFORM_CODES, *m.INFORM_CAPABLE_CLARIFY_CODES)},
        ("UNRESOLVED_VALUE:UNIVERSE", R.CLARIFY): True, ("UNRESOLVED_VALUE:UNIVERSE", R.INFORM): True,  # R3-3
        ("STALE_OPTION", R.INFORM): True, ("STALE_OPTION", R.CLARIFY): False,
        ("DELTA_SLOT_CONFLICT", R.CLARIFY): True, ("KEEP_ON_EMPTY_SLOT", R.CLARIFY): True,
        ("DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", R.CLARIFY): True,
        ("OP_NOT_ACCREDITED:VALUE", R.CLARIFY): False, ("OP_NOT_ACCREDITED:VALUE", R.INFORM): False,  # a refusal,
        ("BASE_INVALID", R.CLARIFY): False, ("DELTA_CONTRADICTS_CARRIED", R.CLARIFY): False,  # never a reason
    }
    for (code, resolution), wanted in expect.items():
        if allowed(code, resolution) is not wanted:
            out.append(f"{code} with {resolution.value}: {'refused' if wanted else 'accepted'}")
    if m.INFORM_CAPABLE_CLARIFY_CODES - m.CLARIFY_CODES:
        out.append("an INFORM-capable code is not a CLARIFY code")
    # an INFORM-only code never leaves a pending (§7.1 row 4b; R3c STALE_OPTION)
    # R4: independent synthetic asking input for a negative reason-code shape probe.
    asking_condition = condition(m, D.BUYER, 0, "synthetic-asking")
    asking_input = (m.PatchOp(m.PatchOpKind.SET, asking_condition.slot, asking_condition,
                             None, asking_condition.origin),)
    for code in ("STALE_OPTION", "UNSUPPORTED_PROPOSAL:MULTI_REQUEST", "CLEAR_ON_EMPTY_SLOT"):
        if _code(lambda: m.PendingClarification("p1", "t1", T0, T0 + timedelta(minutes=30), 1, None, None, None,
                                                None, code, (), frozenset(), frame_ops=(),
                                                asked_ops=asking_input)) != "UNKNOWN_PENDING_REASON":
            out.append(f"a pending accepts the INFORM-only reason {code}")
    # row 4b: OUT_UNSUPPORTED_PROPOSAL is INFORM and makes 0 calls
    scope = m.TurnScope("OUT_UNSUPPORTED_PROPOSAL")
    if m.OUT_SCOPE_RESOLUTION.get(scope) is not R.INFORM:
        out.append("OUT_UNSUPPORTED_PROPOSAL does not lead to INFORM")
    for resolution in (R.CLARIFY, R.NEW_SEARCH):
        if _code(lambda: record(m, resolution, with_patch=False, scope=scope)) != "OUT_SCOPE_RESOLUTION_MISMATCH":
            out.append(f"OUT_UNSUPPORTED_PROPOSAL accepted with {resolution.value}")
    return out


def test_the_a1_a3_closed_lists_never_widen():
    assert annex_failures(fa) == []


# ---- 7. Provenance as the contract writes it (phase 2, narrowed by ZEUS P2-5) -----------------------------------
def _on(m, dimension: D, **fields):
    """A Provenance checked against the dimension of its condition (``check_dimension``)."""
    def build():
        m.Provenance(**fields).check_dimension(dimension)
    return build


def provenance_failures(m) -> list[str]:
    """§3.2, §11, §15.4–§15.5, §16.1. Relaxed in phase 2, and ONLY this: a TIME carries a ``year_source`` without a SMMLV
    ``year``, and an inherited year needs no origin. Kept: a SMMLV vigencia (``year``) travels with its source;
    ``year_source`` and ``inherited_from`` live on VALUE/TIME only; an inherited UNIT names its origin; an origin needs
    an inheritance."""
    out = []
    stated = m.YearSource.STATED
    accepted = {
        "TIME year source without a SMMLV year": _on(m, D.TIME, year_source=stated),
        "TIME year inferred by the clock": _on(m, D.TIME, year_source=m.YearSource.CLOCK_INFERRED),
        "SMMLV year with its source": _on(m, D.VALUE, unit=m.Unit.SMMLV, source_amount="500",
                                          unit_source=m.UnitSource.STATED, year=2025, year_source=stated),
        "inherited year without origin": _on(m, D.TIME, year_source=m.YearSource.PREVIOUS_ACCEPTED_TIME),
        "inherited year with origin": _on(m, D.TIME, year_source=m.YearSource.PREVIOUS_ACCEPTED_TIME,
                                          inherited_from="t0"),
        "inherited unit with origin": _on(m, D.VALUE, unit=m.Unit.MILLONES, source_amount="5",
                                          unit_source=m.UnitSource.PREVIOUS_ACCEPTED_VALUE, inherited_from="t0"),
        "SMMLV year inherited from the TIME": _on(m, D.VALUE, unit=m.Unit.SMMLV, source_amount="500",
                                                  unit_source=m.UnitSource.STATED, year=2025,
                                                  year_source=m.YearSource.PREVIOUS_ACCEPTED_TIME),
    }
    for label, build in accepted.items():
        if (code := _code(build)) is not None:
            out.append(f"refused {label}: {code}")
    refused = {
        "inherited unit without origin": _on(m, D.VALUE, unit=m.Unit.MILLONES, source_amount="5",
                                             unit_source=m.UnitSource.PREVIOUS_ACCEPTED_VALUE),
        "an origin without an inheritance": _on(m, D.TIME, year_source=stated, inherited_from="t0"),
        "SMMLV without its year": _on(m, D.VALUE, unit=m.Unit.SMMLV, source_amount="500",
                                      unit_source=m.UnitSource.STATED),
        "SMMLV year without a year source": _on(m, D.VALUE, unit=m.Unit.SMMLV, source_amount="500",
                                                unit_source=m.UnitSource.STATED, year=2025),
        "a pesos bound with a year and no source": _on(m, D.VALUE, unit=m.Unit.PESOS,
                                                       unit_source=m.UnitSource.STATED, year=2031),
        "a VALUE year source without a year": _on(m, D.VALUE, unit=m.Unit.PESOS, unit_source=m.UnitSource.STATED,
                                                  year_source=stated),
        **{f"a year source on {d.value}": _on(m, d, year_source=source)
           for d in (D.TOPIC, D.BUYER, D.ORDER) for source in (m.YearSource.CLOCK_INFERRED, stated)},
    }
    for label, build in refused.items():
        if _code(build) is None:
            out.append(f"accepted {label}")
    for d in (D.TOPIC, D.BUYER, D.ORDER):  # the origin itself is confined, not only through its year source
        code = _code(_on(m, d, year_source=m.YearSource.PREVIOUS_ACCEPTED_TIME, inherited_from="t-other"))
        if code != "INHERITANCE_OUTSIDE_VALUE_OR_TIME":
            out.append(f"an inherited year on {d.value}: {code}")
    return out


def test_provenance_follows_the_contract():
    assert provenance_failures(fa) == []


def test_the_decoding_surface_reads_the_accredited_value():
    negative = condition(fa, D.TOPIC, 1, "c1")  # vias, token mode, excluded
    positive = condition(fa, D.TOPIC, 0, "c2")  # obras: another value
    assert fa.canonical_value(negative) != negative.value  # the polarity is gone from the identity
    assert fa.canonical_value(negative) != fa.canonical_value(positive)
    lower = condition(fa, D.VALUE, 0, "v")  # >= 500_000_000, MILLONES
    assert fa.money_bound(lower) == (500_000_000, True, True) and fa.unit_class(lower) == "PESOS"
    assert fa.time_years(condition(fa, D.TIME, 0, "t")) == (2025, 2025)
    assert fa.geo_divipola(condition(fa, D.GEOGRAPHY, 1, "g")) == "76001"
    assert fa.state_datasets(condition(fa, D.STATE, 0, "s")) == frozenset()
    with pytest.raises(ContractError, match="NOT_A_MONEY_VALUE"):
        fa.money_bound(negative)


# ---- mutants ---------------------------------------------------------------------------------------------------------
def _mutant(*replacements: tuple[str, str]) -> types.ModuleType:
    source = SOURCE
    for old, new in replacements:
        assert source.count(old) == 1, f"mutation anchor not unique: {old!r}"
        source = source.replace(old, new)
    name = "m2a3_canonical_mutant"
    module = types.ModuleType(name)
    module.__file__ = "<mutant>"
    sys.modules[name] = module
    try:
        exec(compile(source, "<mutant>", "exec"), module.__dict__)
    finally:
        sys.modules.pop(name, None)
    return module


SORT_POLICIES = "    return tuple(sorted(policies, key=_policy_key))"
GROUP = "    return tuple(sorted(conditions, key=lambda condition: _slot_rank(condition.slot)))"
LOGIC = "    return None if _conjunction_only(logic) else logic"
BUILD = "    return AcceptedInterpretation(requests, canonical_conditions(conditions), canonical_logic(logic),"
MUTANTS = {
    # conditions (D-CANON-1)
    "alphabetical_slot_table": (condition_order_failures, (
        "_SLOT_RANK = {slot: rank for rank, slot in enumerate(CONDITION_SLOT_ORDER)}",
        "_SLOT_RANK = {slot: rank for rank, slot in enumerate(sorted(CONDITION_SLOT_ORDER, key=lambda s: s.key))}"),),
    "sorted_inside_a_slot": (condition_order_failures, (
        GROUP, "    return tuple(sorted(conditions, key=lambda c: (_slot_rank(c.slot), c.condition_id)))"),),
    "noncanonical_condition_order_accepted": (condition_order_failures, (
        '        if ranks != sorted(ranks):\n            _fail("NONCANONICAL_CONDITION_ORDER")', '        if False:\n'
        '            _fail("NONCANONICAL_CONDITION_ORDER")'),),
    "type_demands_sorted_ids_inside_a_slot": (condition_order_failures, (
        "        ranks = [_slot_rank(condition.slot) for condition in self.conditions]",
        "        ranks = [(_slot_rank(condition.slot), condition.condition_id) for condition in self.conditions]"),),
    "conditions_kept_in_input_order": (constructor_failures, (GROUP, "    return tuple(conditions)"),),
    "requests_sorted_blindly_by_the_constructor": (constructor_failures, (
        BUILD, "    return AcceptedInterpretation(tuple(sorted(requests, key=lambda r: r.request_id)), "
               "canonical_conditions(conditions), canonical_logic(logic),"),),
    # policies (D-CANON-2 and the phase-1 invariants)
    "multi_condition_policy_accepted": (policy_failures, (
        '    if len(policy.condition_ids) != 1:\n        _fail("MULTI_CONDITION_POLICY")', '    pass'),),
    "duplicate_policy_accepted": (policy_failures, (
        '            if (ref, condition_id) in cited:\n                _fail("DUPLICATE_POLICY")',
        '            pass'),),
    "dangling_policy_accepted": (policy_failures, (
        '        if not set(policy.condition_ids) <= condition_ids:\n            _fail("FOREIGN_POLICY_CONDITION")',
        '        pass'),),
    "two_effects_for_one_ref": (policy_failures, (
        'if effects.setdefault(ref, policy.effect) is not policy.effect:',
        'if effects.setdefault(ref, policy.effect) is not policy.effect and False:'),),
    "ref_is_the_pair_not_the_projected_string": (policy_failures, (
        '        ref = f"{policy.policy_id}@{policy.version}"', '        ref = (policy.policy_id, policy.version)'),),
    "unsorted_policies_accepted": (policy_failures, (
        '    if list(policies) != sorted(policies, key=_policy_key):\n        _fail("UNSORTED_POLICIES")',
        '    pass'),),
    "policies_kept_in_input_order": (constructor_failures, (SORT_POLICIES, "    return tuple(policies)"),),
    "duplicate_policies_collapsed": (constructor_failures, (
        SORT_POLICIES, "    return tuple(sorted(set(policies), key=_policy_key))"),),
    # logic (contract §3.2 only)
    "noncanonical_logic_accepted": (logic_failures, (
        '            if canonical_logic(self.logic) != self.logic:', '            if False:'),),
    "pure_conjunction_kept_as_a_tree": (logic_failures, (LOGIC, "    return logic"),),
    "or_tree_children_sorted_blindly": (logic_failures, (
        LOGIC, "    return None if _conjunction_only(logic) else ConditionExpression(\n"
               "        logic.operator, None, tuple(sorted(logic.children, key=repr)))"),),
    "or_tree_nested_and_flattened": (logic_failures, (
        LOGIC, "    return None if _conjunction_only(logic) else ConditionExpression(logic.operator, None, tuple(\n"
               "        g for c in logic.children for g in (c.children if c.operator is LogicOperator.AND "
               "and logic.operator is LogicOperator.AND else (c,))))"),),
    # Provenance (phase 2, narrowed by ZEUS P2-5)
    "year_coupled_to_its_source_again": (provenance_failures, (
        "            if self.year_source is None:\n                _fail(\"YEAR_WITHOUT_SOURCE\")",
        "            if self.year_source is None:\n                _fail(\"YEAR_WITHOUT_SOURCE\")\n"
        "        if self.year_source is not None and self.year is None:\n            _fail(\"YEAR_WITHOUT_SOURCE\")"),),
    "smmlv_year_without_source_accepted": (provenance_failures, (
        "            if self.year_source is None:\n                _fail(\"YEAR_WITHOUT_SOURCE\")",
        "            pass"),),
    "year_source_on_any_dimension": (provenance_failures, (
        "        if self.year_source is not None and dimension not in (SemanticDimension.VALUE, "
        "SemanticDimension.TIME):",
        "        if False:"),),
    "value_year_source_without_year": (provenance_failures, (
        "        if self.year_source is not None and self.year is None and dimension is SemanticDimension.VALUE:",
        "        if False:"),),
    "inheritance_on_any_dimension": (provenance_failures, (
        "        if self.inherited_from is not None and dimension not in (SemanticDimension.VALUE, "
        "SemanticDimension.TIME):",
        "        if False:"),),
    "inherited_unit_without_origin_accepted": (provenance_failures, (
        "        if self.unit_source is UnitSource.PREVIOUS_ACCEPTED_VALUE and self.inherited_from is None:",
        "        if False:"),),
    "inherited_year_requires_origin_again": (provenance_failures, (
        "        if self.unit_source is UnitSource.PREVIOUS_ACCEPTED_VALUE and self.inherited_from is None:",
        "        if (self.unit_source is UnitSource.PREVIOUS_ACCEPTED_VALUE\n"
        "                or self.year_source is YearSource.PREVIOUS_ACCEPTED_TIME) and self.inherited_from is None:"),),
    "origin_without_inheritance_accepted": (provenance_failures, (
        "        if self.inherited_from is not None and not (self.unit_source is UnitSource.PREVIOUS_ACCEPTED_VALUE",
        "        if False and not (self.unit_source is UnitSource.PREVIOUS_ACCEPTED_VALUE"),),
    # A.1 / A.3 (phase 1c)
    "inform_accepts_every_clarify_code": (annex_failures, (
        "        return (code in INFORM_CODES or code in INFORM_CAPABLE_CLARIFY_CODES or unresolved",
        "        return (code in INFORM_CODES or code in CLARIFY_CODES or unresolved"),),
    "unsupported_proposal_motive_open": (annex_failures, (
        'and head == "UNSUPPORTED_PROPOSAL" and tail in UNSUPPORTED_PROPOSAL_MOTIVES))',
        'and head == "UNSUPPORTED_PROPOSAL" and bool(tail)))'),),
    "dim_without_universe": (annex_failures, (
        'DIMENSION_KEYS = frozenset({dimension.value for dimension in SLOT_DIMENSIONS} | {"UNIVERSE"})',
        'DIMENSION_KEYS = frozenset({dimension.value for dimension in SLOT_DIMENSIONS})'),),
    "dim_open_to_every_dimension": (annex_failures, (
        'unresolved = sep == ":" and head == "UNRESOLVED_VALUE" and tail in DIMENSION_KEYS',
        'unresolved = sep == ":" and head == "UNRESOLVED_VALUE" and tail in DIMENSION_KEYS | {"RETRIEVAL_INTENT"}'),),
    "clarify_accepts_inform_codes": (annex_failures, (
        "        return code in CLARIFY_CODES or unresolved",
        "        return code in CLARIFY_CODES or code in INFORM_CODES or unresolved"),),
    "out_unsupported_proposal_as_clarify": (annex_failures, (
        "    TurnScope.OUT_UNSUPPORTED_PROPOSAL: TurnResolution.INFORM,",
        "    TurnScope.OUT_UNSUPPORTED_PROPOSAL: TurnResolution.CLARIFY,"),),
}


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_m2a3_canonical_mutant_is_killed(name):
    battery, *replacements = MUTANTS[name]
    assert battery(_mutant(*replacements)), f"mutant {name} survived"


PROPERTY_MUTANTS = ("conditions_kept_in_input_order", "policies_kept_in_input_order",
                    "pure_conjunction_kept_as_a_tree")


@pytest.mark.parametrize("name", PROPERTY_MUTANTS)
def test_the_permutation_property_kills_its_mutants(name):
    """The property is not vacuous: Hypothesis finds a case each mutant gets wrong."""
    mutant = _mutant(*MUTANTS[name][1:])
    found = find(st_case(), lambda case: canonical_violation(mutant, case) is not None,
                 settings=settings(max_examples=2000, database=None, derandomize=True,
                                   suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much]))
    assert canonical_violation(mutant, found) is not None
