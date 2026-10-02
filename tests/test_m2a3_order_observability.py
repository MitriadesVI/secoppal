"""M2A-3 R4: which collections of an interpretation have an OBSERVABLE order, executed on the REAL projection.

``docs/audits/m2a_conversation/M2A3_CANONICAL_FORM.md`` cites these pins as its evidence. Only a collection proven
set-like here may get a canonical order in ``foundation_accepted`` (never «sorted blindly»):

* conditions: the soql and ``plan_hash`` classes are EXACTLY the relative order of the TOPIC/POSITIVE conditions
  (the projection's ``foundation_topic_terms`` list); the order across slots is never seen.  ⇒ grouped by slot in
  ``foundation_accepted.CONDITION_SLOT_ORDER``, acceptance order kept inside a slot (user decision D-CANON-1); the
  grouping itself is pinned not to move soql/plan_hash (INV-M2A-06).
* requests (dual anchor): per-request ``plan_hash`` is the same, but the manifest — and so the runtime's groups and
  rows — follow the request order.  ⇒ observable, kept.
* policies: no order is seen, unless one ``policy_id@version`` carries two effects (the projection maps a ref to
  ONE effect: last wins in ``plan_hash``).  ⇒ set-like once that state is refused; sorted.
* AND children (and AND nesting, and ``None`` vs an explicit AND): never seen; OR is not executed at all.
  ⇒ only contract §3.2 is applied (a pure conjunction is ``None``); a tree with OR/NOT is kept as given (limited
  guarantee: nothing executes it, so no rewrite is proven).

If the projection changes so that one of these verdicts flips, the canonical form must be re-decided: these pins
fail first. The acceptances are built in memory; nothing is executed against a source.
"""
from __future__ import annotations

import hashlib
import itertools
from datetime import datetime

import pytest

import app.core.foundation_accepted as fa
from app.core.contract_universe import POLICY_VERSION
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Operation, Subject
from app.core.foundation_projection import project_accepted
from app.core.interpretation_contract import (
    AcceptanceStatus, ConditionExpression, Effect, HostPolicy, HostResolution, InterpretationAcceptance,
    InterpretationProposal, LogicOperator, Polarity, ProposedCondition, ProposedJob, ProposedRelation, ProposedRequest,
    RawSpan, RelationKind, ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension as D
from app.core.soql_builder import SoQLBuilder

NOW = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
RAW = "x" * 64
CONTRACTS = (("q1", Subject.CONTRACTS, SoQLBuilder.CONTRATOS_DATASET),)
DUAL = (("q1", Subject.PROCESSES, SoQLBuilder.PROCESOS_DATASET),
        ("q2", Subject.CONTRACTS, SoQLBuilder.CONTRATOS_DATASET))
AND, OR, ATOM = LogicOperator.AND, LogicOperator.OR, LogicOperator.ATOM

# (dimension, operator, polarity, resolved value, level): every one of them projects on contracts
SPECS = {
    "c1": (D.TOPIC, "contains", Polarity.POSITIVE, ("aseo",), None),
    "c2": (D.TOPIC, "contains", Polarity.POSITIVE, ("papeleria",), None),
    "c3": (D.TOPIC, "whole_token_folded", Polarity.POSITIVE, ("obras",), None),
    "c4": (D.TIME, "during_year", Polarity.POSITIVE, ("2025",), None),
    "c5": (D.VALUE, "less_than", Polarity.POSITIVE, ("200000000",), None),
    "c6": (D.GEOGRAPHY, "is", Polarity.POSITIVE, ("Antioquia",), GeoLevel.DEPARTMENT),
    "c7": (D.MODALITY, "family", Polarity.POSITIVE, ("licitacion_publica", "licitacion_publica_obra"), None),
    "c8": (D.CONTRACT_TYPE_OR_NATURE, "is", Polarity.POSITIVE, ("obra",), None),
    "c9": (D.STATE, "is", Polarity.NEGATIVE, ("Terminado",), None),
}
TOPIC_POSITIVE = frozenset({"c1", "c2", "c3"})
HOST_POLICIES = (HostPolicy("modality-lp-family", "1", ("c7",), Effect.EQUIVALENT),
                 HostPolicy("celebrated-state-complement", POLICY_VERSION, ("c9",), Effect.EQUIVALENT))


def atom(cid: str) -> ConditionExpression:
    return ConditionExpression(ATOM, cid)


def node(operator, *children) -> ConditionExpression:
    return ConditionExpression(operator, None, tuple(children))


def proposed(cid: str, request_ids=("q1",)) -> ProposedCondition:
    dimension, operator, polarity, _, level = SPECS[cid]
    at = int(cid[1:]) * 3
    return ProposedCondition(cid, request_ids, dimension, operator, polarity, "claim", (RawSpan(at, at + 1, "x"),),
                             level=level)


def acceptance(cids, policies=None, *, requests=CONTRACTS, expression=None, owners=None, values=None):
    """A READY acceptance as admission would hand it to the projection (one request, or main's dual shape)."""
    owners = owners or {cid: tuple(r[0] for r in requests) for cid in cids}
    conditions = tuple(proposed(cid, owners[cid]) for cid in cids)
    if policies is None:
        policies = tuple(p for p in HOST_POLICIES if set(p.condition_ids) <= set(cids))

    def expression_of(request_id):
        mine = [cid for cid in cids if request_id in owners[cid]]
        if expression is not None:
            return expression
        return atom(mine[0]) if len(mine) == 1 else node(AND, *(atom(cid) for cid in mine))

    proposal = InterpretationProposal(
        hashlib.sha256(RAW.encode()).hexdigest(), "r4-pin", "1",
        tuple(ProposedRequest(rid, "s1", subject, (RawSpan(0, 1, "x"),), universe_claim="u",
                              universe_evidence=RawSpan(1, 2, "x"), condition_expression=expression_of(rid))
              for rid, subject, _ in requests),
        conditions,
        (ProposedRelation(RelationKind.SHARES_SCOPE, tuple(r[0] for r in requests)),) if len(requests) > 1 else (),
        jobs=(ProposedJob("job", Operation.SEARCH, tuple(r[0] for r in requests)),))
    values = values or {}
    resolutions = [HostResolution(rid, ResolutionDecision.RESOLVED, "host-universe", (dataset,))
                   for rid, _, dataset in requests]
    resolutions += [HostResolution(cid, ResolutionDecision.RESOLVED, "authority", values.get(cid, SPECS[cid][3]))
                    for cid in cids]
    return InterpretationAcceptance(proposal, AcceptanceStatus.READY, tuple(resolutions), tuple(policies))


def plan(accepted) -> tuple:
    """What the order could change: per call, its request, operation, soql and plan_hash, in manifest order."""
    outcome = project_accepted(accepted, reference_instant=NOW)
    assert outcome.status is AcceptanceStatus.READY, outcome.reason
    return tuple((m.request_id, m.operation, m.soql, m.plan_hash) for m in outcome.manifest)


# ---- conditions ------------------------------------------------------------------------------------------------------
def test_condition_order_is_observable_exactly_through_the_topic_positive_terms():
    """Exhaustive over 6 conditions (720 orders): same TOPIC/POSITIVE relative order ⇔ same soql and plan_hash."""
    cids = ("c1", "c2", "c3", "c4", "c6", "c7")
    by_topic_order: dict[tuple, set] = {}
    by_plan: dict[tuple, set] = {}
    for order in itertools.permutations(cids):
        projected = plan(acceptance(order))
        key = tuple(cid for cid in order if cid in TOPIC_POSITIVE)
        by_topic_order.setdefault(key, set()).add(projected)
        by_plan.setdefault(projected, set()).add(key)
    assert len(by_topic_order) == 6
    assert all(len(plans) == 1 for plans in by_topic_order.values())  # cross-slot order is never seen
    assert all(len(keys) == 1 for keys in by_plan.values())  # a different TOPIC/POSITIVE order is always seen


def _slot(cid: str):
    """The M2a slot of a proposed condition (VALUE side from its operator, as the typed comparator would say)."""
    dimension, operator, polarity, _, _ = SPECS[cid]
    if dimension is D.VALUE:
        return fa.Slot(dimension, side=fa.ValueSide.LOWER if operator.startswith("greater") else fa.ValueSide.UPPER)
    return fa.Slot(dimension, polarity)


def test_the_canonical_slot_grouping_never_moves_soql_or_plan_hash():
    """D-CANON-1 against INV-M2A-06: for every order of the acceptance, grouping it by ``CONDITION_SLOT_ORDER``
    (stable inside a slot, as ``canonical_conditions`` does) projects to the same calls."""
    rank = {slot: position for position, slot in enumerate(fa.CONDITION_SLOT_ORDER)}
    for order in itertools.permutations(("c1", "c2", "c3", "c4", "c6", "c7")):
        grouped = tuple(sorted(order, key=lambda cid: rank[_slot(cid)]))
        assert plan(acceptance(grouped)) == plan(acceptance(order))


def test_cross_slot_order_is_unobservable_with_every_projected_dimension():
    cids = ("c1", "c4", "c5", "c6", "c7", "c8", "c9")
    reference = plan(acceptance(cids))
    for order in itertools.islice(itertools.permutations(cids), 0, None, 97):  # 52 spread orders, c1 the only topic
        assert plan(acceptance(order)) == reference


def test_the_concrete_example_two_topics():
    first, swapped, cross = (plan(acceptance(order)) for order in (("c1", "c2", "c4"), ("c2", "c1", "c4"),
                                                                    ("c4", "c1", "c2")))
    assert first[0][2] != swapped[0][2] and first[0][3] != swapped[0][3]
    assert first[0][2].index("%aseo%") < first[0][2].index("%papeleria%")
    assert swapped[0][2].index("%papeleria%") < swapped[0][2].index("%aseo%")
    assert first == cross


# ---- requests (dual anchor) ------------------------------------------------------------------------------------------
def test_request_order_is_the_group_order_of_a_dual_anchor():
    """main's dual shape (corpus NC-002): each request owns its own copy of the condition."""
    owners = {"c1": ("q1",), "c2": ("q2",)}
    values = {"c2": ("aseo",)}
    processes_first = plan(acceptance(("c1", "c2"), (), requests=DUAL, owners=owners, values=values))
    contracts_first = plan(acceptance(("c1", "c2"), (), requests=DUAL[::-1], owners=owners, values=values))
    assert {row[:2]: row[3] for row in processes_first} == {row[:2]: row[3] for row in contracts_first}
    assert [row[0] for row in processes_first] == ["q1", "q1", "q2", "q2"]
    assert [row[0] for row in contracts_first] == ["q2", "q2", "q1", "q1"]


# ---- policies --------------------------------------------------------------------------------------------------------
def test_policy_order_is_unobservable_for_distinct_refs():
    extra = HostPolicy("topic-aseo-morph", "1", ("c1",), Effect.EQUIVALENT)
    plans = {plan(acceptance(("c7", "c9", "c1"), order)) for order in itertools.permutations(HOST_POLICIES + (extra,))}
    assert len(plans) == 1


def test_one_ref_with_two_effects_makes_policy_order_observable():
    """Why ``CONFLICTING_POLICY_EFFECT`` is refused: the projection keeps the LAST effect of a ref in ``plan_hash``."""
    first = HostPolicy("topic-aseo-morph", "1", ("c1",), Effect.EQUIVALENT)
    second = HostPolicy("topic-aseo-morph", "1", ("c2",), Effect.NARROWING)
    forward, backward = (plan(acceptance(("c1", "c2"), order)) for order in ((first, second), (second, first)))
    assert forward[0][2] == backward[0][2] and forward[0][3] != backward[0][3]


def test_split_and_merged_forms_of_one_ref_project_alike():
    """Evidence behind D-CANON-2: merging independent applications would be projection-equivalent, and still M2a
    persists the SPLIT form ``main`` emits (one condition per record; a joint policy is refused by the type)."""
    values = {"c2": ("aseo",)}
    split = plan(acceptance(("c1", "c2"), (HostPolicy("topic-aseo-morph", "1", ("c1",), Effect.EQUIVALENT),
                                           HostPolicy("topic-aseo-morph", "1", ("c2",), Effect.EQUIVALENT)),
                            values=values))
    merged = plan(acceptance(("c1", "c2"), (HostPolicy("topic-aseo-morph", "1", ("c1", "c2"), Effect.EQUIVALENT),),
                             values=values))
    assert split == merged


# ---- AND children ----------------------------------------------------------------------------------------------------
def test_and_shape_and_order_are_unobservable():
    cids = ("c1", "c4", "c6")
    shapes = [node(AND, *(atom(cid) for cid in order)) for order in itertools.permutations(cids)]
    shapes += [node(AND, node(AND, atom("c1"), atom("c4")), atom("c6")),
               node(AND, atom("c6"), node(AND, atom("c4"), atom("c1")))]
    plans = {plan(acceptance(cids, expression=shape)) for shape in shapes}
    assert len(plans) == 1


@pytest.mark.parametrize("expression", [
    node(AND, node(OR, atom("c1"), atom("c4")), atom("c6")),
    node(OR, atom("c1"), atom("c4"), atom("c6")),
])
def test_an_or_is_not_executed_so_its_order_is_not_proven_either_way(expression):
    outcome = project_accepted(acceptance(("c1", "c4", "c6"), expression=expression), reference_instant=NOW)
    assert outcome.status is AcceptanceStatus.UNSUPPORTED and outcome.reason == "NONCONJUNCTIVE_EXPRESSION"
