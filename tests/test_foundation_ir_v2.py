"""IR v2 (spine v5, C1.2): jobs, subject `unspecified`, geographic level, the closed operator table,
ORDER/CARDINALITY, the lossless `lift_v1` and the reader of stored records.

INV-C0-2 is pinned two ways: the invented regression corpus has a fixture produced by the IR v1 code
(``test_the_regression_corpus_gives_the_outcome_the_ir_v1_gave``), and the properties below say what
``lift_v1`` may never lose. Phrases are invented; none is a Gold case.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_resolver import EntityResolver
from app.core.foundation_acceptance_loader import load_acceptance, load_proposal
from app.core.foundation_admission import admit_proposal
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_lift_v1 import lift_v1, v1_dimension
from app.core.foundation_messages import user_message
from app.core.foundation_operations import Aspect, Operation, Subject
from app.core import foundation_operator_table as table
from app.core.foundation_operator_table import OPERATORS, operator_allowed
from app.core.foundation_producers import LinguisticProducer, decode_proposal, host_candidates
from app.core.foundation_projection import project_accepted
from app.core.interpretation_contract import (
    SCHEMA_VERSION, AcceptanceStatus, ConditionExpression, ConditionRole, ContractError, HostResolution,
    InterpretationAcceptance, InterpretationProposal, LogicOperator, Polarity, ProposedCondition,
    ProposedJob, ProposedRelation, ProposedRequest, RawSpan, RelationKind, ResolutionDecision,
)
from app.core.legacy_shadow import DIMENSION_ORDER
from app.core.semantic_dimensions import SemanticDimension as D
from app.core.soql_builder import SoQLBuilder
from tests.foundation_ir_parity import dump_outcome
from tests.test_foundation_product_gates import CORPUS, _proposal

RESOLVER = EntityResolver("app/data/aliases_db.json")
NOW = datetime.fromisoformat("2026-09-26T12:00:00-05:00")
RAW = "contratos de aseo en Cali con valor mayor a 50 millones, los 5 mas grandes del 2025"
FIXTURES = Path(__file__).parent / "fixtures"


def S(text: str, occurrence: int = 1, raw: str = RAW) -> RawSpan:
    at = -1
    for _ in range(occurrence):
        at = raw.index(text, at + 1)
    return RawSpan(at, at + len(text), text)


def atom(cid: str) -> ConditionExpression:
    return ConditionExpression(LogicOperator.ATOM, cid)


def AND(*children) -> ConditionExpression:
    return ConditionExpression(LogicOperator.AND, None, tuple(children))


def request(rid="r1", subject=Subject.CONTRACTS, expression=None, raw=RAW) -> ProposedRequest:
    universe = S("contratos", raw=raw) if subject is not Subject.UNSPECIFIED else None
    return ProposedRequest(rid, rid, subject, (RawSpan(0, len(raw), raw),),
                           universe_claim=universe.quote if universe else None, universe_evidence=universe,
                           condition_expression=expression)


def condition(cid, dimension, operator, quote, *, rid=("r1",), polarity=Polarity.POSITIVE, **extra):
    if dimension is D.GEOGRAPHY:
        extra.setdefault("level", GeoLevel.DEPARTMENT)
    if dimension is D.UNSUPPORTED:
        extra.setdefault("unsupported_label", "label")
    return ProposedCondition(cid, tuple(rid), dimension, operator, polarity, quote, (S(quote),), **extra)


def proposal(requests=(), jobs=None, conditions=(), relations=(), raw=RAW) -> InterpretationProposal:
    import hashlib

    if jobs is None:
        jobs = (ProposedJob("job-search", Operation.SEARCH, tuple(r.request_id for r in requests)),) if requests else ()
    return InterpretationProposal(hashlib.sha256(raw.encode()).hexdigest(), "test", "1", tuple(requests),
                                  tuple(conditions), tuple(relations), (), tuple(jobs))


def codes(build) -> str:
    with pytest.raises(ContractError) as caught:
        build()
    return str(caught.value)


# ── the closed operator table ───────────────────────────────────────────────────────────────────────

EXPECTED_TABLE = {
    D.TOPIC: {"contains", "whole_token_folded"}, D.BUYER: {"is"}, D.ACTOR: {"is"}, D.GEOGRAPHY: {"is"},
    D.TIME: {"during_year", "during_month", "between_dates", "relative_period"},
    D.VALUE: {"greater_than", "greater_or_equal", "less_than", "less_or_equal"},
    D.STATE: {"is", "family"}, D.MODALITY: {"is", "family"}, D.CONTRACT_TYPE_OR_NATURE: {"is", "family"},
    D.ORDER: {"value_desc", "value_asc", "date_desc", "date_asc"}, D.CARDINALITY: {"top_n"},
    D.RETRIEVAL_INTENT: {"is"}, D.CONTROL_LANGUAGE: set(),
}


def test_the_operator_table_is_the_closed_table_of_the_design():
    assert {d: set(ops) for d, ops in OPERATORS.items()} == EXPECTED_TABLE
    assert set(OPERATORS) == set(D) - {D.UNSUPPORTED}


ALL_OPERATORS = sorted({op for ops in EXPECTED_TABLE.values() for op in ops})


@pytest.mark.parametrize("dimension", sorted(set(D) - {D.UNSUPPORTED}, key=lambda d: d.value))
def test_every_dimension_accepts_exactly_its_own_operators(dimension):
    level = {"level": GeoLevel.REGION} if dimension is D.GEOGRAPHY else {}
    role = ({"role": ConditionRole.REQUESTED_UNIVERSE} if dimension is D.RETRIEVAL_INTENT else {})
    for operator in (*ALL_OPERATORS, "bogus", "before", "EQ"):
        build = lambda: ProposedCondition("c1", ("r1",), dimension, operator, Polarity.POSITIVE, "aseo",  # noqa: E731
                                          (S("aseo"),), **level, **role)
        if operator in EXPECTED_TABLE[dimension]:
            assert build().operator == operator
        else:
            assert codes(build) == "OPERATOR_OUTSIDE_DIMENSION", (dimension, operator)


def test_unsupported_keeps_a_free_operator_and_its_label():
    free = ProposedCondition("c1", ("r1",), D.UNSUPPORTED, "unsupported_range", Polarity.POSITIVE, "aseo",
                             (S("aseo"),), unsupported_label="range")
    assert free.operator == "unsupported_range" and operator_allowed(D.UNSUPPORTED, "anything")


def test_a_dimension_missing_from_the_table_admits_no_operator(monkeypatch):
    monkeypatch.setattr(table, "OPERATORS", {D.TOPIC: frozenset({"contains"})})
    assert table.operator_allowed(D.TOPIC, "contains") and not table.operator_allowed(D.TOPIC, "is")
    assert not table.operator_allowed(D.BUYER, "is") and table.operator_allowed(D.UNSUPPORTED, "is")


def test_order_and_cardinality_are_dimensions_the_shadow_knows():
    assert D.ORDER.value == "ORDER" and D.CARDINALITY.value == "CARDINALITY"
    assert {d.value for d in D} - {"UNSUPPORTED"} <= set(DIMENSION_ORDER)


# ── geographic level ────────────────────────────────────────────────────────────────────────────────

def test_a_geography_condition_names_its_level_and_nothing_else_may():
    ok = condition("c1", D.GEOGRAPHY, "is", "Cali", level=GeoLevel.MUNICIPALITY)
    assert ok.level is GeoLevel.MUNICIPALITY
    assert condition("c1", D.GEOGRAPHY, "is", "Cali", level=GeoLevel.UNKNOWN).level is GeoLevel.UNKNOWN
    assert codes(lambda: ProposedCondition("c1", ("r1",), D.GEOGRAPHY, "is", Polarity.POSITIVE, "Cali",
                                           (S("Cali"),))) == "GEOGRAPHY_LEVEL_REQUIRED"
    assert codes(lambda: ProposedCondition("c1", ("r1",), D.GEOGRAPHY, "is", Polarity.POSITIVE, "Cali",
                                           (S("Cali"),), level="DEPARTMENT")) == "INVALID_GEO_LEVEL"
    for dimension, operator in ((D.TOPIC, "contains"), (D.BUYER, "is"), (D.TIME, "during_year"),
                                (D.VALUE, "greater_than"), (D.UNSUPPORTED, "x")):
        extra = {"unsupported_label": "x"} if dimension is D.UNSUPPORTED else {}
        assert codes(lambda: ProposedCondition("c1", ("r1",), dimension, operator, Polarity.POSITIVE, "aseo",
                                               (S("aseo"),), level=GeoLevel.DEPARTMENT, **extra)
                     ) == "LEVEL_OUTSIDE_GEOGRAPHY"


# ── jobs ────────────────────────────────────────────────────────────────────────────────────────────

def test_the_schema_is_version_two_and_a_search_proposal_has_one_job():
    item = proposal((request(),))
    assert SCHEMA_VERSION == 2 and item.schema_version == 2 and not item.is_multi
    assert item.operation_of("r1") is Operation.SEARCH and item.operation_of("missing") is None
    assert codes(lambda: InterpretationProposal(item.raw_sha256, "t", "1", item.requests, (), (), (), item.jobs,
                                                1)) == "UNSUPPORTED_SCHEMA_VERSION"


def test_a_request_without_a_job_is_refused_and_so_is_an_orphan():
    assert codes(lambda: proposal((request(),), jobs=())) == "MISSING_JOBS"
    other = ProposedJob("job-count", Operation.COUNT, (), (S("contratos"),), Aspect.TOTAL)
    assert codes(lambda: proposal((request("r1"), request("r2")), jobs=(
        ProposedJob("job-search", Operation.SEARCH, ("r1",)), other))) == "ORPHAN_REQUEST"


def test_a_request_belongs_to_exactly_one_job():
    count = ProposedJob("job-count", Operation.COUNT, ("r1",), (S("contratos"),), Aspect.TOTAL)
    assert codes(lambda: proposal((request(),), jobs=(
        ProposedJob("job-search", Operation.SEARCH, ("r1",)), count))) == "REQUEST_IN_TWO_JOBS"
    assert codes(lambda: proposal((request(),), jobs=(
        ProposedJob("job-search", Operation.SEARCH, ("r1", "ghost")),))) == "FOREIGN_JOB_REQUEST"


def test_a_job_id_and_an_operation_each_appear_once():
    first = ProposedJob("job-a", Operation.SEARCH, ("r1",))
    again = ProposedJob("job-a", Operation.COUNT, (), (S("contratos"),), Aspect.TOTAL)
    assert codes(lambda: proposal((request(),), jobs=(first, again))) == "DUPLICATE_JOB_ID"
    twice = ProposedJob("job-b", Operation.SEARCH, ("r2",))
    assert codes(lambda: proposal((request("r1"), request("r2")), jobs=(first, twice))) == "DUPLICATE_JOB_OPERATION"


def test_job_shape_rules():
    ev = (S("contratos"),)
    assert codes(lambda: ProposedJob("j", Operation.SEARCH)) == "SEARCH_JOB_WITHOUT_REQUEST"
    assert codes(lambda: ProposedJob("j", Operation.PROFILE_ACTOR)) == "UNGROUNDED_OPERATION"
    assert codes(lambda: ProposedJob("j", Operation.GROUP_BY, (), ev)) == "ASPECT_REQUIRED"
    assert codes(lambda: ProposedJob("j", Operation.PROFILE_ACTOR, (), ev, Aspect.TOTAL)) == "ASPECT_FORBIDDEN"
    assert codes(lambda: ProposedJob("j", Operation.SEARCH, ("r1",), (), Aspect.TOTAL)) == "ASPECT_FORBIDDEN"
    assert codes(lambda: ProposedJob("j", Operation.COUNT, (), ev, Aspect.BY_YEAR)) == "COUNT_ASPECT_NOT_TOTAL"
    assert codes(lambda: ProposedJob("j", Operation.GROUP_BY, (), ev, Aspect.OTHER)) == "ASPECT_LABEL_MISMATCH"
    assert codes(lambda: ProposedJob("j", Operation.GROUP_BY, (), ev, Aspect.BY_YEAR, "x")) == "ASPECT_LABEL_MISMATCH"
    assert ProposedJob("j", Operation.GROUP_BY, (), ev, Aspect.OTHER, "MARKET_ANALYSIS").aspect_label
    assert ProposedJob("j", Operation.COUNT, (), ev).aspect is None
    assert ProposedJob("j", Operation.SUM_VALUE, (), ev, Aspect.BY_BUYER).aspect is Aspect.BY_BUYER
    assert codes(lambda: ProposedJob("j", Operation.SEARCH, ("r1", "r1"))) == "DUPLICATE_JOB_REQUEST_IDS"
    assert codes(lambda: ProposedJob("j", "SEARCH", ("r1",))) == "INVALID_OPERATION"
    assert codes(lambda: ProposedJob("j", Operation.PROFILE_ACTOR, (), ev, referent_evidence="x")
                 ) == "INVALID_REFERENT_EVIDENCE"


def test_a_job_without_requests_may_carry_the_whole_turn_and_is_multi_when_two():
    item = proposal((request(),), jobs=(
        ProposedJob("job-search", Operation.SEARCH, ("r1",)),
        ProposedJob("job-follow", Operation.FOLLOW, (), (S("valor"),))))
    assert item.is_multi
    only = proposal((), jobs=(ProposedJob("job-protocol", Operation.PROTOCOL, (), (S("contratos"),)),))
    assert not only.is_multi and only.requests == ()


def test_job_evidence_is_bound_to_the_raw_text():
    item = proposal((request(),), jobs=(
        ProposedJob("job-search", Operation.SEARCH, ("r1",)),
        ProposedJob("job-count", Operation.COUNT, (), (RawSpan(0, 9, "procesos"),), Aspect.TOTAL)))
    assert codes(lambda: item.validate_raw(RAW)) == "UNBOUND_RAW_SPAN"
    referent = ProposedJob("job-f", Operation.FOLLOWUP_DELTA, (), (S("contratos"),),
                           referent_evidence=RawSpan(0, 9, "procesos"))
    assert codes(lambda: proposal((), jobs=(referent,)).validate_raw(RAW)) == "UNBOUND_RAW_SPAN"


def test_the_empty_interpretation_is_the_only_proposal_without_jobs():
    empty = proposal()
    assert empty.requests == () and empty.jobs == ()
    accepted = admit_proposal(empty, RAW, entity_resolver=RESOLVER)
    assert (accepted.status, accepted.reason) == (AcceptanceStatus.NO_ACTION, "NO_REQUEST")


# ── subject: unspecified ────────────────────────────────────────────────────────────────────────────

def test_a_subject_is_a_closed_value_and_unspecified_has_no_universe_claim():
    assert codes(lambda: ProposedRequest("r1", "r1", "contracts", (S("contratos"),))) == "INVALID_SUBJECT"
    assert request(subject=Subject.UNSPECIFIED).universe_evidence is None
    named = S("contratos")
    assert codes(lambda: ProposedRequest("r1", "r1", Subject.UNSPECIFIED, (named,), universe_claim="contratos",
                                         universe_evidence=named)) == "UNSPECIFIED_WITH_UNIVERSE_EVIDENCE"
    assert codes(lambda: ProposedRequest("r1", "r1", Subject.CONTRACTS, (named,))) == "UNGROUNDED_REQUEST_UNIVERSE"
    assert codes(lambda: ProposedRequest("r1", "r1", Subject.PROCESSES, (named,), universe_claim="contratos")
                 ) == "UNGROUNDED_UNIVERSE_CLAIM"


def test_the_host_does_not_execute_an_unspecified_request_yet():
    item = proposal((request(subject=Subject.UNSPECIFIED),))
    accepted = admit_proposal(item, RAW, entity_resolver=RESOLVER)
    assert (accepted.status, accepted.reason) == (AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_UNIVERSE")


def two_requests_sharing_a_scope(second_operation):
    first, second = request("r1"), request("r2")
    second = ProposedRequest("r2", "r1", Subject.CONTRACTS, second.evidence, universe_claim="contratos",
                             universe_evidence=S("contratos"))
    first = ProposedRequest("r1", "r1", Subject.CONTRACTS, first.evidence, universe_claim="contratos",
                            universe_evidence=S("contratos"))
    jobs = (ProposedJob("job-search", Operation.SEARCH, ("r1",)),
            ProposedJob("job-other", second_operation, ("r2",), (S("contratos"),),
                        Aspect.TOTAL if second_operation is Operation.COUNT else None))
    return proposal((first, second), jobs=jobs, relations=(ProposedRelation(RelationKind.SHARES_SCOPE, ("r1", "r2")),))


def test_a_search_sharing_its_scope_with_a_count_is_a_multi_request_for_the_host():
    item = two_requests_sharing_a_scope(Operation.COUNT)
    accepted = admit_proposal(item, RAW, entity_resolver=RESOLVER)
    assert (accepted.status, accepted.reason) == (AcceptanceStatus.UNSUPPORTED, "MULTI_REQUEST")


def test_the_projection_never_compiles_a_request_whose_job_is_not_a_search():
    resolution = lambda rid: HostResolution(rid, ResolutionDecision.RESOLVED, "host-universe", ("jbjy-vk9h",))  # noqa: E731
    count = proposal((request(),), jobs=(ProposedJob("job-count", Operation.COUNT, ("r1",), (S("contratos"),),
                                                     Aspect.TOTAL),))
    outcome = project_accepted(InterpretationAcceptance(count, AcceptanceStatus.READY, (resolution("r1"),)),
                               reference_instant=NOW)
    assert (outcome.status, outcome.reason) == (AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_OPERATION")
    pair = two_requests_sharing_a_scope(Operation.COUNT)
    outcome = project_accepted(InterpretationAcceptance(pair, AcceptanceStatus.READY,
                                                        (resolution("r1"), resolution("r2"))),
                               reference_instant=NOW)
    assert (outcome.status, outcome.reason) == (AcceptanceStatus.UNSUPPORTED, "MULTI_REQUEST")


# ── ORDER and CARDINALITY ───────────────────────────────────────────────────────────────────────────

def order_cond(cid="o", rid=("r1",), polarity=Polarity.POSITIVE, operator="value_desc"):
    return condition(cid, D.ORDER, operator, "mas grandes", rid=rid, polarity=polarity)


def top_cond(cid="n", rid=("r1",)):
    return condition(cid, D.CARDINALITY, "top_n", "5", rid=rid)


def with_expression(conditions, expression, requests=None):
    req = request(expression=expression)
    return proposal(requests or (req,), conditions=conditions)


def test_order_and_cardinality_are_accepted_together_and_never_executed():
    item = with_expression((order_cond(), top_cond()), AND(atom("o"), atom("n")))
    assert [c.dimension for c in item.conditions] == [D.ORDER, D.CARDINALITY]
    accepted = admit_proposal(item, RAW, entity_resolver=RESOLVER)
    assert accepted.status is AcceptanceStatus.NEEDS_CLARIFICATION
    assert accepted.reason == "UNRESOLVED_HOST_DIMENSION:ORDER"


def test_a_negated_order_is_representable_the_host_decides():
    item = with_expression((order_cond(polarity=Polarity.NEGATIVE),), atom("o"))
    assert item.conditions[0].polarity is Polarity.NEGATIVE


def test_at_most_one_order_and_one_cardinality_per_request():
    two_orders = (order_cond("o1"), order_cond("o2", operator="date_asc"))
    assert codes(lambda: with_expression(two_orders, AND(atom("o1"), atom("o2")))) == "MULTIPLE_ORDER"
    two_tops = (order_cond(), top_cond("n1"), top_cond("n2"))
    assert codes(lambda: with_expression(two_tops, AND(atom("o"), atom("n1"), atom("n2")))) == "MULTIPLE_CARDINALITY"
    shared = (order_cond("o1", rid=("r1", "r2")), order_cond("o2", rid=("r1",), operator="date_asc"))
    requests = (request("r1", expression=AND(atom("o1"), atom("o2"))), request("r2", expression=atom("o1")))
    assert codes(lambda: proposal(requests, conditions=shared)) == "MULTIPLE_ORDER"


def test_a_cardinality_needs_an_order_in_its_search_request_or_a_ranking_job():
    assert codes(lambda: with_expression((top_cond(),), atom("n"))) == "CARDINALITY_WITHOUT_ORDER"
    elsewhere = (order_cond(rid=("r2",)), top_cond(rid=("r1",)))
    requests = (request("r1", expression=atom("n")), request("r2", expression=atom("o")))
    assert codes(lambda: proposal(requests, conditions=elsewhere)) == "CARDINALITY_WITHOUT_ORDER"
    ranking = proposal((request(expression=atom("n")),), conditions=(top_cond(),), jobs=(
        ProposedJob("job-rank", Operation.RANK_SUPPLIERS, ("r1",), (S("grandes"),)),))
    assert ranking.operation_of("r1") is Operation.RANK_SUPPLIERS


def test_an_order_is_only_a_top_level_conjunction_atom():
    for expression in (ConditionExpression(LogicOperator.OR, None, (atom("o"), atom("t"))),
                       ConditionExpression(LogicOperator.NOT, None, (atom("o"),)),
                       AND(atom("t"), ConditionExpression(LogicOperator.OR, None, (atom("o"), atom("u"))))):
        conditions = [order_cond(), condition("t", D.TOPIC, "contains", "aseo")]
        if "u" in str(expression):
            conditions.append(condition("u", D.TOPIC, "contains", "Cali"))
        if expression.operator is LogicOperator.NOT:
            conditions.pop()
        assert codes(lambda: with_expression(tuple(conditions), expression)) == "NONCONJUNCTIVE_ORDER"
    assert with_expression((order_cond(), condition("t", D.TOPIC, "contains", "aseo")), AND(atom("o"), atom("t")))


# ── lift_v1: nothing the v4.2 wire said is lost ─────────────────────────────────────────────────────

def v1_payload(raw, requests, conditions=(), relations=()):
    return {
        "requests": [{"request_id": rid, "scope_id": scope, "operation": op, "subject": subject,
                      "universe_kind": "records", "evidence": [{"start": 0, "end": len(raw), "quote": raw}],
                      "universe_evidence": wire_span(raw, "contratos"),
                      **({"condition_expression": expr} if expr else {})}
                     for rid, scope, op, subject, expr in requests],
        "conditions": [{"condition_id": cid, "request_ids": rids, "dimension": dim, "operator": op,
                        "polarity": pol, "evidence": [wire_span(raw, quote)],
                        **({"unsupported_label": "x"} if dim == "UNSUPPORTED" else {})}
                       for cid, rids, dim, op, pol, quote in conditions],
        "relations": list(relations),
    }


def wire_span(raw, text, occurrence=1):
    at = -1
    for _ in range(occurrence):
        at = raw.index(text, at + 1)
    return {"start": at, "end": at + len(text), "quote": text}


def decode(payload, raw=RAW):
    return decode_proposal(payload, raw, arm="C")


def test_a_count_request_stays_a_count_job_with_total_and_its_own_evidence():
    item = decode(v1_payload(RAW, [("r1", "r1", "COUNT", "contracts", None)]))
    (job,) = item.jobs
    assert (job.operation, job.aspect, job.request_ids) == (Operation.COUNT, Aspect.TOTAL, ("r1",))
    assert job.evidence == item.requests[0].evidence and item.operation_of("r1") is Operation.COUNT


def test_a_sum_request_stays_a_sum_job():
    item = decode(v1_payload(RAW, [("r1", "r1", "SUM_VALUE", "processes", None)]))
    assert item.jobs[0].operation is Operation.SUM_VALUE and item.jobs[0].aspect is Aspect.TOTAL
    assert item.requests[0].subject is Subject.PROCESSES


def test_two_searches_sharing_a_scope_are_one_search_job():
    payload = v1_payload(RAW, [("r1", "s", "SEARCH", "contracts", None), ("r2", "s", "SEARCH", "processes", None)],
                         relations=[{"kind": "SHARES_SCOPE", "members": ["r1", "r2"]}])
    item = decode(payload)
    (job,) = item.jobs
    assert job.operation is Operation.SEARCH and job.request_ids == ("r1", "r2") and not item.is_multi


def test_a_search_and_a_count_are_two_jobs_and_the_host_still_refuses_the_pair():
    item = decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "contracts", None), ("r2", "r2", "COUNT", "contracts", None)]))
    assert [j.operation for j in item.jobs] == [Operation.SEARCH, Operation.COUNT] and item.is_multi
    accepted = admit_proposal(item, RAW, entity_resolver=RESOLVER)
    assert (accepted.status, accepted.reason) == (AcceptanceStatus.UNSUPPORTED, "MULTI_REQUEST")


def test_the_host_refuses_a_lifted_count_exactly_as_it_did_in_v1():
    item = decode(v1_payload(RAW, [("r1", "r1", "COUNT", "contracts", None)]))
    accepted = admit_proposal(item, RAW, entity_resolver=RESOLVER)
    assert (accepted.status, accepted.reason) == (AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_OPERATION")
    assert accepted.proposal is item


def test_a_lifted_geography_is_a_department_and_nothing_else_has_a_level():
    both = {"operator": "AND", "children": [{"operator": "ATOM", "condition_id": "c1"},
                                            {"operator": "ATOM", "condition_id": "c2"}]}
    item = decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "contracts", both)], [
        ("c1", ["r1"], "GEOGRAPHY", "is", "POSITIVE", "Cali"), ("c2", ["r1"], "TOPIC", "contains", "POSITIVE", "aseo")]))
    assert {c.condition_id: c.level for c in item.conditions} == {"c1": GeoLevel.DEPARTMENT, "c2": None}


def test_a_request_outside_the_v42_vocabulary_is_not_lifted_it_is_refused():
    assert codes(lambda: decode(v1_payload(RAW, [("r1", "r1", "RANK_SUPPLIERS", "contracts", None)]))
                 ) == "UNSUPPORTED_V1_OPERATION"
    assert codes(lambda: decode(v1_payload(RAW, [("r1", "r1", "search", "contracts", None)]))
                 ) == "UNSUPPORTED_V1_OPERATION"
    assert codes(lambda: decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "oportunidades", None)]))
                 ) == "UNSUPPORTED_V1_SUBJECT"
    assert codes(lambda: decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "unspecified", None)]))
                 ) == "UNSUPPORTED_V1_SUBJECT"  # the v4.2 wire never says it: only the v5 wire may
    assert codes(lambda: decode(v1_payload(RAW, [("r1", "r1", "", "contracts", None)]))) == "INVALID_OPERATION"


def test_a_v1_operator_outside_the_table_is_refused_by_the_contract_not_executed():
    payload = v1_payload(RAW, [("r1", "r1", "SEARCH", "contracts", {"operator": "ATOM", "condition_id": "c1"})],
                         [("c1", ["r1"], "TIME", "before", "POSITIVE", "2025")])
    assert codes(lambda: decode(payload)) == "OPERATOR_OUTSIDE_DIMENSION"


def test_an_empty_v1_answer_lifts_to_the_empty_interpretation():
    item = decode(v1_payload(RAW, []))
    assert item.requests == () and item.jobs == ()


DIMENSIONS_V1 = [("TOPIC", "contains", "aseo"), ("TOPIC", "whole_token_folded", "aseo"), ("BUYER", "is", "Cali"),
                 ("GEOGRAPHY", "is", "Cali"), ("TIME", "during_year", "2025"), ("VALUE", "greater_than", "50 millones"),
                 ("VALUE", "less_than", "50 millones"), ("STATE", "family", "valor"), ("STATE", "is", "valor"),
                 ("MODALITY", "family", "valor"), ("CONTRACT_TYPE_OR_NATURE", "is", "valor"),
                 ("UNSUPPORTED", "anything-the-producer-said", "grandes")]


@settings(max_examples=150, deadline=None)
@given(
    ops=st.lists(st.sampled_from(["SEARCH", "COUNT", "SUM_VALUE"]), min_size=0, max_size=3),
    subjects=st.lists(st.sampled_from(["processes", "contracts"]), min_size=3, max_size=3),
    picks=st.lists(st.tuples(st.integers(0, 2), st.sampled_from(DIMENSIONS_V1), st.sampled_from(["POSITIVE", "NEGATIVE"])),
                   max_size=5),
)
def test_lift_v1_loses_nothing_the_v1_payload_said(ops, subjects, picks):
    rids = [f"r{i}" for i in range(len(ops))]
    owned = {rid: [] for rid in rids}
    conditions = []
    for at, (owner, (dim, op, quote), polarity) in enumerate(picks):
        if not rids:
            break
        rid = rids[owner % len(rids)]
        owned[rid].append(f"c{at}")
        conditions.append((f"c{at}", [rid], dim, op, polarity, quote))

    def expression(ids):
        atoms = [{"operator": "ATOM", "condition_id": cid} for cid in ids]
        return None if not atoms else atoms[0] if len(atoms) == 1 else {"operator": "AND", "children": atoms}

    requests = [(rid, rid, op, subjects[i], expression(owned[rid])) for i, (rid, op) in enumerate(zip(rids, ops))]
    item = decode(v1_payload(RAW, requests, conditions))
    assert item.schema_version == 2
    # every request is in exactly one job, and that job has the operation the v1 request had
    assert sorted(r for job in item.jobs for r in job.request_ids) == sorted(rids)
    for rid, op in zip(rids, ops):
        assert item.operation_of(rid) is Operation(op)
    assert [j.operation for j in item.jobs] == list(dict.fromkeys(Operation(op) for op in ops))
    assert item.is_multi == (len(set(ops)) > 1)
    for job in item.jobs:
        if job.operation is not Operation.SEARCH:
            assert job.aspect is Aspect.TOTAL and job.evidence
            assert all(span in job.evidence for r in item.requests if r.request_id in job.request_ids
                       for span in r.evidence)
    # subjects, spans, operators and polarity pass through untouched; only a geography gains its level
    assert [r.subject.value for r in item.requests] == subjects[:len(rids)]
    for built, (cid, rids_, dim, op, polarity, quote) in zip(item.conditions, conditions):
        assert (built.condition_id, built.dimension.value, built.operator, built.polarity.value) == (cid, dim, op, polarity)
        assert built.evidence == (S(quote),)
        assert built.level is (GeoLevel.DEPARTMENT if dim == "GEOGRAPHY" else None)


def downgrade(item: InterpretationProposal, operations: dict[str, str]) -> dict:
    """The record the IR v1 would have stored for this proposal (the shape ``lift_v1`` reads)."""
    record = json.loads(json.dumps(item.to_dict()))
    record.pop("jobs")
    record["schema_version"] = 1
    for entry in record["requests"]:
        entry["operation"] = operations[entry["request_id"]]
    for entry in record["conditions"]:
        entry.pop("level")
    return record


def test_the_stored_record_lift_and_the_wire_decoder_say_the_same_thing():
    payload = v1_payload(
        RAW, [("r1", "s", "SEARCH", "contracts", {"operator": "AND", "children": [
            {"operator": "ATOM", "condition_id": "c1"}, {"operator": "ATOM", "condition_id": "c2"}]}),
              ("r2", "s", "SEARCH", "processes", None), ("r3", "r3", "COUNT", "contracts", None)],
        [("c1", ["r1"], "GEOGRAPHY", "is", "POSITIVE", "Cali"), ("c2", ["r1"], "TOPIC", "contains", "NEGATIVE", "aseo")],
        [{"kind": "SHARES_SCOPE", "members": ["r1", "r2"]}])
    item = decode(payload)
    record = downgrade(item, {"r1": "SEARCH", "r2": "SEARCH", "r3": "COUNT"})
    assert load_proposal(record) == item
    assert load_proposal(lift_v1(record)) == item
    assert lift_v1(record)["schema_version"] == 2 and record["schema_version"] == 1  # the input is never mutated


def test_lift_v1_only_reads_a_version_one_record():
    item = decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "contracts", None)]))
    record = downgrade(item, {"r1": "SEARCH"})
    for version in (2, 0, None, True, "1"):
        assert codes(lambda: lift_v1({**record, "schema_version": version})) == "UNSUPPORTED_SCHEMA_VERSION"
    assert codes(lambda: lift_v1("record")) == "INVALID_STORED_RECORD"


# ── load_acceptance: stored records, v1 or v2, never guessed ────────────────────────────────────────

def stored(item, status="UNSUPPORTED", reason="UNSUPPORTED_OPERATION", resolutions=()):
    return json.loads(json.dumps({"proposal": item.to_dict(), "status": status, "resolutions": resolutions,
                                  "policies": [], "reason": reason}))


def test_a_stored_v2_acceptance_round_trips_exactly():
    item = decode(v1_payload(RAW, [("r1", "r1", "COUNT", "contracts", None)]))
    accepted = admit_proposal(item, RAW, entity_resolver=RESOLVER)
    assert load_acceptance(json.loads(json.dumps(accepted.to_dict()))) == accepted


def test_a_stored_ready_acceptance_round_trips_with_its_resolutions_and_policies():
    raw = "contratos de primera infancia"
    payload = {"requests": [{"request_id": "r1", "scope_id": "r1", "operation": "SEARCH", "subject": "contracts",
                             "universe_kind": "opportunities", "evidence": [wire_span(raw, raw)],
                             "universe_evidence": wire_span(raw, "contratos"),
                             "condition_expression": {"operator": "ATOM", "condition_id": "c1"}}],
               "conditions": [{"condition_id": "c1", "request_ids": ["r1"], "dimension": "TOPIC",
                               "operator": "contains", "polarity": "POSITIVE",
                               "evidence": [wire_span(raw, "primera infancia")]}]}
    item = decode_proposal(payload, raw, arm="C")
    accepted = admit_proposal(item, raw, entity_resolver=RESOLVER)
    assert accepted.status is AcceptanceStatus.READY and accepted.policies
    assert load_acceptance(json.loads(json.dumps(accepted.to_dict()))) == accepted


def test_a_stored_v1_acceptance_is_lifted_to_what_the_decoder_would_have_produced():
    item = decode(v1_payload(RAW, [("r1", "r1", "COUNT", "contracts", None)]))
    record = {"proposal": downgrade(item, {"r1": "COUNT"}), "status": "UNSUPPORTED", "resolutions": [],
              "policies": [], "reason": "UNSUPPORTED_OPERATION"}
    loaded = load_acceptance(record)
    assert loaded.proposal == item and loaded.proposal.operation_of("r1") is Operation.COUNT
    assert (loaded.status, loaded.reason) == (AcceptanceStatus.UNSUPPORTED, "UNSUPPORTED_OPERATION")


def test_a_stored_acceptance_without_a_proposal_loads_as_is():
    record = {"proposal": None, "status": "PROVIDER_ERROR", "resolutions": [], "policies": [],
              "reason": "PROVIDER_ERROR:Boom"}
    assert load_acceptance(record) == InterpretationAcceptance(None, AcceptanceStatus.PROVIDER_ERROR,
                                                               reason="PROVIDER_ERROR:Boom")


@pytest.mark.parametrize("version", [0, 3, 99, -1, "1", "2", None, 1.0, 2.0, True, False, [], {}])
def test_any_other_schema_version_is_unsupported(version):
    item = decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "contracts", None)]))
    record = stored(item)
    record["proposal"]["schema_version"] = version
    assert codes(lambda: load_acceptance(record)) == "UNSUPPORTED_SCHEMA_VERSION"
    del record["proposal"]["schema_version"]
    assert codes(lambda: load_acceptance(record)) == "UNSUPPORTED_SCHEMA_VERSION"


def test_the_version_is_the_stored_one_never_sniffed_from_the_shape():
    item = decode(v1_payload(RAW, [("r1", "r1", "COUNT", "contracts", None)]))
    v2_claiming_v1 = stored(item)
    v2_claiming_v1["proposal"]["schema_version"] = 1
    assert codes(lambda: load_acceptance(v2_claiming_v1)) in {"INVALID_OPERATION", "UNSUPPORTED_V1_OPERATION"}
    v1 = {"proposal": downgrade(item, {"r1": "COUNT"}), "status": "UNSUPPORTED", "resolutions": [], "policies": [],
          "reason": "UNSUPPORTED_OPERATION"}
    v1["proposal"]["schema_version"] = 2
    assert codes(lambda: load_acceptance(v1)) == "INVALID_STORED_RECORD"


def test_a_stored_record_is_held_to_the_contract_and_to_a_closed_set_of_keys():
    item = decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "contracts", None)]))
    record = stored(item, status="NO_ACTION", reason="x")
    assert load_acceptance(record).proposal == item
    for mutate, expected in (
        (lambda r: r["proposal"].update(sql="DELETE"), "INVALID_STORED_RECORD"),
        (lambda r: r["proposal"].pop("jobs"), "MISSING_JOBS"),
        (lambda r: r["proposal"]["requests"][0].update(subject="contratos"), "INVALID_STORED_RECORD"),
        (lambda r: r["proposal"]["jobs"][0].update(operation="MULTI"), "INVALID_STORED_RECORD"),
        (lambda r: r["proposal"]["jobs"].append(dict(r["proposal"]["jobs"][0], job_id="again")),
         "DUPLICATE_JOB_OPERATION"),
        (lambda r: r.update(extra=1), "INVALID_STORED_RECORD"),
        (lambda r: r.update(status="DONE"), "INVALID_STORED_RECORD"),
        (lambda r: r["proposal"]["requests"][0].pop("request_id"), "INVALID_STORED_RECORD"),
    ):
        broken = json.loads(json.dumps(record))
        mutate(broken)
        assert codes(lambda: load_acceptance(broken)) == expected
    assert codes(lambda: load_acceptance("not a record")) == "INVALID_STORED_RECORD"
    assert codes(lambda: load_acceptance({**record, "proposal": "x"})) == "INVALID_STORED_RECORD"


# ── INV-C0-2: the invented regression corpus gives the outcome the IR v1 gave ───────────────────────

def test_the_regression_corpus_gives_the_outcome_the_ir_v1_gave():
    expected = json.loads((FIXTURES / "foundation_ir_v1_parity_corpus.json").read_text())
    builder = SoQLBuilder()
    got = {case["id"]: dump_outcome(_proposal(case), case["raw"], resolver=RESOLVER, builder=builder)
           for case in CORPUS["cases"]}
    assert got.keys() == expected.keys()
    for case_id in got:
        assert got[case_id] == expected[case_id], case_id
    assert {v["final_status"] for v in got.values()} == {"READY", "NEEDS_CLARIFICATION", "UNSUPPORTED"}
    assert sum(len(v["projection"]["manifest"]) for v in got.values() if "manifest" in v.get("projection", {})) > 0


# ── foundation_v3 parity: the v4.2 wire never had ORDER or CARDINALITY ──────────────────────────────

ORDER_RAW = "contratos de aseo los 5 más caros"


def _order_payload(*extra):
    """The v4.2 wire (arm B) of ``ORDER_RAW`` with the given ``(dimension, operator, quote)`` extra conditions."""
    candidates = host_candidates(ORDER_RAW)

    def ids(quote):
        return [c["candidate_id"] for c in candidates if c["quote"] == quote]

    conditions = [{"condition_id": "c1", "request_ids": ["r1"], "dimension": "TOPIC", "operator": "contains",
                   "polarity": "POSITIVE", "role": "FILTER", "candidate_ids": ids("aseo")}]
    atoms = [{"operator": "ATOM", "condition_id": "c1"}]
    for at, (dimension, operator, quote) in enumerate(extra):
        conditions.append({"condition_id": f"x{at}", "request_ids": ["r1"], "dimension": dimension,
                           "operator": operator, "polarity": "POSITIVE", "role": "FILTER", "candidate_ids": ids(quote)})
        atoms.append({"operator": "ATOM", "condition_id": f"x{at}"})
    return {"requests": [{"request_id": "r1", "scope_id": "s1", "operation": "SEARCH", "subject": "contracts",
                          "universe_kind": "records", "candidate_ids": ids("contratos"),
                          "universe_evidence_candidate_ids": ids("contratos"),
                          "condition_expression": atoms[0] if len(atoms) == 1 else {"operator": "AND", "children": atoms}}],
            "conditions": conditions, "relations": [], "omissions": []}


@pytest.mark.parametrize("extra", [
    (("ORDER", "value_desc", "más caros"),),
    (("CARDINALITY", "top_n", "5"),),
    (("ORDER", "value_desc", "más caros"), ("CARDINALITY", "top_n", "5")),
])
def test_a_v42_wire_with_order_or_cardinality_is_invalid_exactly_as_in_main(extra):
    """main raised ``ValueError`` (no such member): ``INVALID_PROPOSAL`` / ``MALFORMED_PROPOSAL:ValueError``, with the
    same user message. ``foundation_v3`` keeps it: no decision asks the v4.2 wire to carry these dimensions (D3,
    INV-C0-2, M1_0 §10: they are v5 vocabulary). The IR v2 in memory still holds them."""
    acceptance = LinguisticProducer(lambda system, prompt: json.dumps(_order_payload(*extra))).produce(ORDER_RAW)
    assert acceptance.status is AcceptanceStatus.INVALID_PROPOSAL
    assert acceptance.reason == "MALFORMED_PROPOSAL:ValueError"
    message = user_message(acceptance.status.value, acceptance.reason, acceptance.proposal)
    assert message.text.startswith("No pude interpretar tu consulta con seguridad.") and message.choices == ()


def test_the_v42_decoder_names_only_the_dimensions_main_had():
    for member in D:
        if member in (D.ORDER, D.CARDINALITY):
            with pytest.raises(ValueError, match="is not a valid SemanticDimension") as caught:
                v1_dimension(member.value)
            assert type(caught.value) is ValueError
        else:
            assert v1_dimension(member.value) is member
    assert {m.value for m in D} - {"ORDER", "CARDINALITY"} == {
        "UNSUPPORTED", "RETRIEVAL_INTENT", "TOPIC", "MODALITY", "CONTRACT_TYPE_OR_NATURE", "ACTOR", "BUYER",
        "GEOGRAPHY", "TIME", "VALUE", "STATE", "CONTROL_LANGUAGE"}
    with pytest.raises(ValueError):
        v1_dimension("NOT_A_DIMENSION")
    payload = _order_payload()
    payload["omissions"] = [{"request_id": "r1", "dimension": "TIME", "state": "UNKNOWN"}]
    assert decode_proposal(payload, ORDER_RAW, arm="B", candidates=host_candidates(ORDER_RAW)).omissions
    for dimension in ("ORDER", "CARDINALITY"):
        payload["omissions"] = [{"request_id": "r1", "dimension": dimension, "state": "UNKNOWN"}]
        with pytest.raises(ValueError) as caught:
            decode_proposal(payload, ORDER_RAW, arm="B", candidates=host_candidates(ORDER_RAW))
        assert type(caught.value) is ValueError  # not a ContractError: main's disposition is MALFORMED_PROPOSAL:ValueError


def test_a_stored_v1_record_with_order_is_not_lifted_and_a_v2_record_with_it_is_untouched():
    item = decode(v1_payload(RAW, [("r1", "r1", "SEARCH", "contracts", {"operator": "ATOM", "condition_id": "c1"})],
                             [("c1", ["r1"], "TOPIC", "contains", "POSITIVE", "aseo")]))
    record = downgrade(item, {"r1": "SEARCH"})
    record["conditions"][0]["dimension"] = "ORDER"
    assert codes(lambda: lift_v1(record)) == "INVALID_DIMENSION"
    order = ProposedCondition("o1", ("r1",), D.ORDER, "value_desc", Polarity.POSITIVE, "mas grandes",
                              (RawSpan(RAW.index("mas grandes"), RAW.index("mas grandes") + 11, "mas grandes"),))
    assert order.dimension is D.ORDER  # the IR v2 in memory is unchanged
