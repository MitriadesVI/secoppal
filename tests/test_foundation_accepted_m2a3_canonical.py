"""M2A-3 phase 1: the interpretation-level invariants and the canonical form shared by every constructor of an
effective (``app/core/foundation_accepted.py``; ``docs/audits/m2a_conversation/M2A3_CANONICAL_FORM.md``).

Invariants proved here:

1. ``policies``: every cited condition exists (a ``CLEAR`` never leaves a dangling policy); a (policy ref, condition)
   pair is cited once — ``(p1, p1)`` is REJECTED, never collapsed, whatever the order verdict; one ref has one
   effect; the list is sorted. O1B / ``opportunity-explicit-state`` cite the REQUESTED_UNIVERSE condition, which is
   not a slot: they cannot be persisted (§6.6 inventory, fail-closed).
2. ``logic``: a pure conjunction is ``None``; every AND is flat (no AND child of an AND) with sorted children; OR and
   NOT keep their children's order (not proven set-like).
3. ``canonical_interpretation`` is the one constructor path: a permutation of a set-like collection gives the same
   bytes and the same ``interpretation_hash``; ``conditions`` and ``requests`` keep the order given (observable,
   R4); nothing is deduplicated, merged or dropped.

Every battery runs against MUTANTS of the module (the ``exec`` technique of ``test_foundation_accepted_contract.py``):
a check that does not kill its mutant does not count. The executed evidence for which collections are set-like is
``tests/test_m2a3_order_observability.py`` (the real projection).
"""
from __future__ import annotations

import copy
import itertools
import json
import sys
import types
from dataclasses import replace

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import ConditionExpression, ContractError, Effect, HostPolicy, LogicOperator
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_foundation_accepted_contract import PROPS, SOURCE, condition, request

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
    return (condition(m, D.TOPIC, 0, "c1", rids), condition(m, D.VALUE, 0, "c2", rids),
            condition(m, D.ORDER, 0, "c3", rids), condition(m, D.CARDINALITY, 1, "c4", rids))


def build(m, policies=POLICIES, conds=None, logic=None, requests=None):
    return m.AcceptedInterpretation(requests or (request(m),), conditions(m) if conds is None else conds, logic,
                                    policies, None, "root-1")


def flat_conditions(m):
    """Four conditions with no single-slot dimension, so any logic tree over them is admissible."""
    return (condition(m, D.TOPIC, 0, "c1"), condition(m, D.BUYER, 0, "c2"), condition(m, D.GEOGRAPHY, 0, "c3"),
            condition(m, D.TIME, 0, "c4"))


CANONICAL_LOGIC = node(AND, atom("c3"), atom("c4"), node(OR, atom("c1"), atom("c2")))  # atoms sort before groups


# ---- 1. policies -----------------------------------------------------------------------------------------------------
def policy_failures(m) -> list[str]:
    out = []
    effective = build(m)
    if m.AcceptedInterpretation.from_dict(json.loads(json.dumps(effective.to_dict()))) != effective:
        out.append("the canonical policies do not round-trip")
    refused = {
        "(p1, p1)": ((P_ACTOR, P_ACTOR, P_ORDER, P_TOPN), "DUPLICATE_POLICY"),
        "one ref citing a condition twice": ((P_ACTOR, P_ORDER, P_TOPN, HostPolicy("topn", "1", ("c1", "c4"),
                                                                                   Effect.NARROWING)),
                                             "DUPLICATE_POLICY"),
        "a policy on a missing condition": ((P_ACTOR, HostPolicy("ghost", "1", ("c9",), Effect.EQUIVALENT)),
                                            "FOREIGN_POLICY_CONDITION"),
        "one ref with two effects": ((HostPolicy("x", "1", ("c1",), Effect.EQUIVALENT),
                                      HostPolicy("x", "1", ("c2",), Effect.NARROWING)), "CONFLICTING_POLICY_EFFECT"),
        "two effects behind one projected ref": ((HostPolicy("a", "b@c", ("c1",), Effect.EQUIVALENT),
                                                  HostPolicy("a@b", "c", ("c2",), Effect.NARROWING)),
                                                 "CONFLICTING_POLICY_EFFECT"),
        "unsorted policies": ((P_TOPN, P_ORDER, P_ACTOR), "UNSORTED_POLICIES"),
    }
    for label, (policies, code) in refused.items():
        if _code(lambda: build(m, policies)) != code:
            out.append(f"constructor: {label} not refused as {code}")
    # a CLEAR that drops the cited condition and keeps its policy leaves it dangling: refused
    without_c4 = conditions(m)[:3]
    if _code(lambda: build(m, conds=without_c4)) != "FOREIGN_POLICY_CONDITION":
        out.append("a policy left dangling by a removed condition is accepted")
    payload = effective.to_dict()
    for label, change in (("repeated", lambda items: items[:1] + items), ("reversed", lambda items: items[::-1])):
        tampered = copy.deepcopy(payload)
        tampered["policies"] = change(payload["policies"])
        if _code(lambda: m.AcceptedInterpretation.from_dict(tampered)) is None:
            out.append(f"reader accepts {label} policies")
    # positive controls: what is NOT a duplicate stays representable
    for label, policies in (("two refs on one condition", (P_ACTOR, HostPolicy("zz", "1", ("c1",), Effect.WIDENING))),
                            ("one ref split over disjoint conditions",
                             (HostPolicy("x", "1", ("c1",), Effect.EQUIVALENT),
                              HostPolicy("x", "1", ("c2",), Effect.EQUIVALENT)))):
        if (code := _code(lambda: build(m, policies))) is not None:
            out.append(f"positive control {label} refused: {code}")
    return out


def test_policies_are_set_like_and_never_duplicated_or_dangling():
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


# ---- 2. logic --------------------------------------------------------------------------------------------------------
def logic_failures(m) -> list[str]:
    out = []
    conds = flat_conditions(m)
    canonical = build(m, (), conds, CANONICAL_LOGIC)
    if m.AcceptedInterpretation.from_dict(json.loads(json.dumps(canonical.to_dict()))) != canonical:
        out.append("the canonical logic does not round-trip")
    pure = node(AND, atom("c1"), atom("c2"), atom("c3"), atom("c4"))
    if m.canonical_logic(pure) is not None or m.canonical_logic(node(AND, node(AND, atom("c1"), atom("c2")),
                                                                     atom("c3"), atom("c4"))) is not None:
        out.append("a pure conjunction is not None")
    if _code(lambda: build(m, (), conds, pure)) != "NONCANONICAL_LOGIC":
        out.append("the type accepts a pure conjunction written as a tree")
    group = node(OR, atom("c1"), atom("c2"))
    variants = [node(AND, *order) for order in itertools.permutations((atom("c3"), atom("c4"), group))]
    variants += [node(AND, node(AND, atom("c4"), atom("c3")), group),
                 node(AND, group, node(AND, atom("c3"), atom("c4")))]
    for variant in variants:
        if m.canonical_logic(variant) != CANONICAL_LOGIC:
            out.append(f"canonical_logic does not normalize {m.expression_to_dict(variant)}")
        if variant != CANONICAL_LOGIC and _code(lambda: build(m, (), conds, variant)) != "NONCANONICAL_LOGIC":
            out.append(f"the type accepts the non-canonical {m.expression_to_dict(variant)}")
    # OR and NOT are not proven set-like: their order is kept, while an AND below them is still canonical
    swapped = node(AND, atom("c3"), atom("c4"), node(OR, atom("c2"), atom("c1")))
    if m.canonical_logic(swapped) != swapped or _code(lambda: build(m, (), conds, swapped)) is not None:
        out.append("the order of an OR's children is rewritten or refused")
    inner = node(OR, node(AND, atom("c4"), atom("c3")), atom("c2"), atom("c1"))
    if m.canonical_logic(inner) != node(OR, node(AND, atom("c3"), atom("c4")), atom("c2"), atom("c1")):
        out.append("an AND below an OR is not canonical")
    negated = node(AND, node(NOT, node(AND, atom("c2"), atom("c1"))), atom("c4"), atom("c3"))
    if m.canonical_logic(negated) != node(AND, atom("c3"), atom("c4"), node(NOT, node(AND, atom("c1"), atom("c2")))):
        out.append("an AND below a NOT is not canonical")
    payload = canonical.to_dict()
    tampered = copy.deepcopy(payload)
    tampered["logic"]["children"] = payload["logic"]["children"][::-1]
    if _code(lambda: m.AcceptedInterpretation.from_dict(tampered)) is None:
        out.append("reader accepts permuted AND children")
    return out


def test_logic_has_one_form():
    assert logic_failures(fa) == []


# ---- 3. the shared constructor path ----------------------------------------------------------------------------------
def constructor_failures(m) -> list[str]:
    out = []
    reference = m.canonical_interpretation((request(m),), conditions(m), None, POLICIES, None, "root-1")
    expected = m.interpretation_hash(reference)
    for order in itertools.permutations(POLICIES):
        try:
            made = m.canonical_interpretation((request(m),), conditions(m), None, order, None, "root-1")
        except ContractError as error:
            out.append(f"policies {[p.policy_id for p in order]} refused: {error}")
            continue
        if made.to_dict() != reference.to_dict() or m.interpretation_hash(made) != expected:
            out.append(f"policies {[p.policy_id for p in order]} change the bytes")
    if _code(lambda: m.canonical_interpretation((request(m),), conditions(m), None, (P_ACTOR, P_ACTOR), None,
                                                "root-1")) != "DUPLICATE_POLICY":
        out.append("the constructor collapses (p1, p1) instead of refusing it")
    conds = flat_conditions(m)
    try:
        if m.canonical_interpretation((request(m),), conds, node(AND, *(atom(c.condition_id) for c in conds)), (),
                                      None, "root-1").logic is not None:
            out.append("the constructor keeps a pure conjunction as a tree")
        made = m.canonical_interpretation((request(m),), conds, node(AND, node(OR, atom("c1"), atom("c2")),
                                                                     atom("c4"), atom("c3")), (), None, "root-1")
        if made.logic != CANONICAL_LOGIC:
            out.append("the constructor does not put the logic in its one form")
    except ContractError as error:
        out.append(f"the constructor refuses a valid logic: {error}")
    # nothing observable is reordered (R4): conditions and requests keep the order they are given
    for order in (conds[::-1], (conds[2], conds[0], conds[3], conds[1])):
        try:
            if m.canonical_interpretation((request(m),), order, None, (), None, "root-1").conditions != order:
                out.append("the constructor reorders conditions")
        except ContractError as error:
            out.append(f"conditions in acceptance order refused: {error}")
    dual = (request(m, "r2", Subject.CONTRACTS), request(m, "r1", Subject.PROCESSES))
    shared = tuple(replace(c, request_ids=("r1", "r2")) for c in conds)
    try:
        if m.canonical_interpretation(dual, shared, None, (), None, "root-1").requests != dual:
            out.append("the constructor reorders the requests of a dual anchor")
    except ContractError as error:
        out.append(f"a contracts-first dual anchor refused: {error}")
    return out


def test_one_constructor_path_orders_only_what_is_set_like():
    assert constructor_failures(fa) == []


# ---- 4. property: a permutation of a set-like collection never changes the bytes -----------------------------------
DIMS = (D.TOPIC, D.BUYER, D.ACTOR, D.GEOGRAPHY, D.TIME, D.STATE, D.MODALITY, D.CONTRACT_TYPE_OR_NATURE)
REFS = (("topn", "1", Effect.NARROWING), ("actor_state", "1", Effect.EQUIVALENT), ("order_default", "2",
                                                                                     Effect.WIDENING))


@st.composite
def st_tree(draw, cids):
    if len(cids) == 1:
        return atom(cids[0])
    cut = draw(st.integers(1, len(cids) - 1))
    left, right = draw(st_tree(cids[:cut])), draw(st_tree(cids[cut:]))
    if draw(st.booleans()):
        left = node(NOT, left)
    return node(draw(st.sampled_from([AND, OR])), left, right)


@st.composite
def st_and_equivalent(draw, expression):
    """The same expression up to commutativity and associativity of AND (a set-like rewrite)."""
    if expression.operator is ATOM:
        return expression
    children = [draw(st_and_equivalent(child)) for child in expression.children]
    if expression.operator is AND:  # flatten, permute, and maybe regroup two children into a nested AND
        flat = [grandchild for child in children
                for grandchild in (child.children if child.operator is AND else (child,))]
        children = draw(st.permutations(flat))
        if len(children) > 2 and draw(st.booleans()):
            children = [node(AND, *children[:2]), *children[2:]]
    return node(expression.operator, *children)


@st.composite
def st_case(draw):
    """(dimensions, policies, logic, permuted policies, AND-equivalent logic): plain data, materialized per module."""
    dims = draw(st.lists(st.sampled_from(DIMS), min_size=2, max_size=5))
    cids = [f"c{index}" for index in range(len(dims))]
    policies = []
    for policy_id, version, effect in REFS:
        cited = sorted(draw(st.sets(st.sampled_from(cids), max_size=len(cids))))
        if not cited:
            continue
        if len(cited) > 1 and draw(st.booleans()):  # the split form of one ref (declared residual freedom)
            policies += [HostPolicy(policy_id, version, (cid,), effect) for cid in cited]
        else:
            policies.append(HostPolicy(policy_id, version, tuple(cited), effect))
    logic = draw(st.none() | st_tree(draw(st.permutations(cids))))
    variant = None if logic is None else draw(st_and_equivalent(logic))
    return tuple(dims), tuple(policies), logic, tuple(draw(st.permutations(policies))), variant


def canonical_violation(m, case) -> str | None:
    dims, policies, logic, permuted, variant = case
    conds = tuple(condition(m, dimension, 0, f"c{index}") for index, dimension in enumerate(dims))

    def make(pol, log):
        return m.canonical_interpretation((request(m),), conds, log, pol, None, "root-1")

    try:
        first, second = make(policies, logic), make(permuted, variant)
    except ContractError as error:
        return f"refused: {error}"
    if first.to_dict() != second.to_dict() or m.interpretation_hash(first) != m.interpretation_hash(second):
        return "a set-like permutation changed the bytes"
    if m.AcceptedInterpretation.from_dict(json.loads(json.dumps(first.to_dict()))) != first:
        return "the canonical form does not round-trip"
    if first.conditions != conds:
        return "conditions were reordered"
    rewritten = (tuple(permuted), variant)
    if rewritten != (first.policies, first.logic) and _code(
            lambda: m.AcceptedInterpretation((request(m),), conds, variant, permuted, None, "root-1")) is None:
        return "the type accepts a non-canonical form"
    return None


@PROPS
@given(st_case())
def test_property_canonical_form_is_permutation_invariant(case):
    assert canonical_violation(fa, case) is None


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
BUILD = "    return AcceptedInterpretation(requests, conditions, canonical_logic(logic), canonical_policies(policies),"
MUTANTS = {
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
    "noncanonical_logic_accepted": (logic_failures, (
        '            if canonical_logic(self.logic) != self.logic:', '            if False:'),),
    "and_children_unsorted": (logic_failures, (
        '        children = tuple(sorted(flat, key=_expression_key))', '        children = tuple(flat)'),),
    "nested_and_not_flattened": (logic_failures, (
        'for grandchild in (child.children if child.operator is LogicOperator.AND else (child,)))',
        'for grandchild in (child,))'),),
    "pure_conjunction_kept_as_a_tree": (logic_failures, (
        '    return None if _conjunction_only(logic) else _canonical_node(logic)',
        '    return _canonical_node(logic)'),),
    "or_children_sorted_blindly": (logic_failures, (
        "    return ConditionExpression(node.operator, None, children)  # OR and NOT keep their children's order",
        "    return ConditionExpression(node.operator, None, tuple(sorted(children, key=_expression_key)))"),),
    "conditions_sorted_blindly_by_the_constructor": (constructor_failures, (
        BUILD, "    return AcceptedInterpretation(requests, tuple(sorted(conditions, key=lambda c: c.condition_id)), "
               "canonical_logic(logic), canonical_policies(policies),"),),
    "requests_sorted_blindly_by_the_constructor": (constructor_failures, (
        BUILD, "    return AcceptedInterpretation(tuple(sorted(requests, key=lambda r: r.request_id)), conditions, "
               "canonical_logic(logic), canonical_policies(policies),"),),
    "conditions_order_refused_by_the_type": (constructor_failures, (
        '        _check_policies(self.policies, set(condition_ids))',
        '        _check_policies(self.policies, set(condition_ids))\n'
        '        if condition_ids != sorted(condition_ids):\n            _fail("UNSORTED_CONDITIONS")'),),
}


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_m2a3_canonical_mutant_is_killed(name):
    battery, *replacements = MUTANTS[name]
    assert battery(_mutant(*replacements)), f"mutant {name} survived"


PROPERTY_MUTANTS = ("policies_kept_in_input_order", "and_children_unsorted", "nested_and_not_flattened")


@pytest.mark.parametrize("name", PROPERTY_MUTANTS)
def test_the_permutation_property_kills_its_mutants(name):
    """The property is not vacuous: Hypothesis finds a case each ordering mutant gets wrong."""
    mutant = _mutant(*MUTANTS[name][1:])
    found = find(st_case(), lambda case: canonical_violation(mutant, case) is not None,
                 settings=settings(max_examples=2000, database=None, derandomize=True,
                                   suppress_health_check=[HealthCheck.too_slow]))
    assert canonical_violation(mutant, found) is not None
