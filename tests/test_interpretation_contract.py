"""Contract tests for the inactive Foundation V3 interpretation boundary."""

from dataclasses import FrozenInstanceError
import hashlib
import json

import pytest
from hypothesis import given, strategies as st

from app.core.interpretation_contract import (
    AcceptanceStatus,
    ConditionExpression,
    ConditionDisposition,
    ConditionRole,
    ContractError,
    Disposition,
    Effect,
    HostResolution,
    HostPolicy,
    InterpretationAcceptance,
    InterpretationProjection,
    InterpretationProposal,
    LogicOperator,
    Omission,
    Polarity,
    ProjectionStatus,
    ProposedCondition,
    ProposedOmission,
    ProposedRelation,
    ProposedRequest,
    RawSpan,
    RelationKind,
    ResolutionDecision,
)
from app.core.foundation_operations import Subject
from app.core.semantic_dimensions import SemanticDimension
from tests.foundation_ir import search_jobs


RAW = "contratos de aseo y procesos de obra; ambos en 2025"


def span(text: str, occurrence: int = 0) -> RawSpan:
    start = -1
    for _ in range(occurrence + 1):
        start = RAW.index(text, start + 1)
    return RawSpan(start, start + len(text), text)


def atom(condition_id: str) -> ConditionExpression:
    return ConditionExpression(LogicOperator.ATOM, condition_id)


def proposal(*, requests=None, conditions=(), relations=(), omissions=(), raw=RAW, jobs=None):
    if requests is None:
        requests = (ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (span("contratos"),)),)
    return InterpretationProposal(
        hashlib.sha256(raw.encode()).hexdigest(), "test-producer", "1",
        requests, conditions, relations, omissions,
        search_jobs(requests) if jobs is None else jobs,
    )


def test_proposal_is_immutable_and_json_serializable_without_executable_fields():
    item = proposal()
    item.validate_raw(RAW)
    payload = json.loads(json.dumps(item.to_dict(), allow_nan=False))
    assert payload["requests"][0]["subject"] == "unspecified"
    assert payload["jobs"][0]["operation"] == "SEARCH" and payload["schema_version"] == 2
    assert set(payload) == {
        "raw_sha256", "producer", "producer_version", "requests", "conditions",
        "relations", "omissions", "jobs", "schema_version",
    }
    with pytest.raises(FrozenInstanceError):
        item.producer = "other"
    with pytest.raises(TypeError):
        InterpretationProposal(**{**item.to_dict(), "sql": "DELETE FROM x"})


def test_raw_hash_and_half_open_quote_must_bind_to_same_original_turn():
    item = proposal()
    with pytest.raises(ContractError, match="RAW_HASH_MISMATCH"):
        item.validate_raw(RAW + "!")
    bad = proposal(requests=(ProposedRequest(
        "r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, 8, "procesos"),),
    ),))
    with pytest.raises(ContractError, match="UNBOUND_RAW_SPAN"):
        bad.validate_raw(RAW)
    overrun = proposal(requests=(ProposedRequest(
        "r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, 100, "contratos"),),
    ),))
    with pytest.raises(ContractError, match="UNBOUND_RAW_SPAN"):
        overrun.validate_raw(RAW)


def test_repeated_word_uses_distinct_original_offsets():
    raw = "aseo y aseo"
    requests = (
        ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, 4, "aseo"),)),
        ProposedRequest("r2", "s2", Subject.UNSPECIFIED, (RawSpan(7, 11, "aseo"),)),
    )
    proposal(requests=requests, raw=raw).validate_raw(raw)
    stale_second = proposal(requests=(requests[0], ProposedRequest(
        "r2", "s2", Subject.UNSPECIFIED, (RawSpan(6, 10, "aseo"),),
    )), raw=raw)
    with pytest.raises(ContractError, match="UNBOUND_RAW_SPAN"):
        stale_second.validate_raw(raw)


def test_nfc_quote_is_only_a_view_of_original_span():
    raw = "cafe\u0301"
    request = ProposedRequest(
        "r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, len(raw), "caf\u00e9"),),
    )
    item = proposal(requests=(request,), raw=raw)
    item.validate_raw(raw)
    assert item.raw_sha256 != hashlib.sha256("caf\u00e9".encode()).hexdigest()


def test_scope_members_are_enumerated_exactly_and_foreign_ids_fail():
    requests = (
        ProposedRequest("r1", "shared", Subject.UNSPECIFIED, (span("contratos"),)),
        ProposedRequest("r2", "shared", Subject.UNSPECIFIED, (span("procesos"),)),
        ProposedRequest("r3", "other", Subject.UNSPECIFIED, (span("obra"),)),
    )
    with pytest.raises(ContractError, match="INCOMPLETE_SHARED_SCOPE"):
        proposal(requests=requests)
    with pytest.raises(ContractError, match="INCOMPLETE_SHARED_SCOPE"):
        proposal(requests=requests, relations=(ProposedRelation(
            RelationKind.SHARES_SCOPE, ("r1", "r3"),
        ),))
    with pytest.raises(ContractError, match="FOREIGN_RELATION_MEMBER"):
        proposal(requests=requests, relations=(ProposedRelation(
            RelationKind.SHARES_SCOPE, ("r1", "foreign"),
        ),))
    item = proposal(requests=requests, relations=(ProposedRelation(
        RelationKind.SHARES_SCOPE, ("r1", "r2"),
    ),))
    item.validate_raw(RAW)


def test_result_dependencies_are_acyclic_and_condition_ownership_is_explicit():
    requests = (
        ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (span("contratos"),)),
        ProposedRequest("r2", "s2", Subject.UNSPECIFIED, (span("procesos"),)),
    )
    edge = ProposedRelation(RelationKind.USES_RESULT_OF, ("r2", "r1"))
    proposal(requests=requests, relations=(edge,)).validate_raw(RAW)
    with pytest.raises(ContractError, match="CYCLIC_RESULT_DEPENDENCY"):
        proposal(requests=requests, relations=(
            edge, ProposedRelation(RelationKind.USES_RESULT_OF, ("r1", "r2")),
        ))
    negative = ProposedCondition(
        "c1", ("r2",), SemanticDimension.TIME, "during_year", Polarity.NEGATIVE,
        "2025", (span("2025"),),
    )
    scoped_requests = (requests[0], ProposedRequest(
        "r2", "s2", Subject.UNSPECIFIED, (span("procesos"),),
        condition_expression=atom("c1"),
    ))
    item = proposal(requests=scoped_requests, conditions=(negative,))
    assert item.conditions[0].request_ids == ("r2",)
    with pytest.raises(ContractError, match="FOREIGN_CONDITION_REQUEST"):
        proposal(requests=scoped_requests, conditions=(ProposedCondition(
            "c2", ("foreign",), SemanticDimension.TIME, "during_year", Polarity.NEGATIVE,
            "2025", (span("2025"),),
        ),))


@given(st.integers(min_value=2, max_value=8))
def test_result_dependency_chains_reject_any_closing_cycle(length: int):
    requests = tuple(
        ProposedRequest(f"r{index}", f"s{index}", Subject.UNSPECIFIED, (span("contratos"),))
        for index in range(length)
    )
    chain = tuple(
        ProposedRelation(RelationKind.USES_RESULT_OF, (f"r{index}", f"r{index - 1}"))
        for index in range(1, length)
    )
    proposal(requests=requests, relations=chain).validate_raw(RAW)
    with pytest.raises(ContractError, match="CYCLIC_RESULT_DEPENDENCY"):
        proposal(requests=requests, relations=(
            *chain, ProposedRelation(RelationKind.USES_RESULT_OF, ("r0", f"r{length - 1}")),
        ))


def test_omission_does_not_admit_implicit_delete():
    item = proposal(omissions=(ProposedOmission(
        "r1", SemanticDimension.TIME, Omission.UNKNOWN,
    ),))
    assert item.to_dict()["omissions"][0]["state"] == "UNKNOWN"
    with pytest.raises(ContractError, match="INVALID_OMISSION"):
        ProposedOmission("r1", SemanticDimension.TIME, "DELETE")


def test_acceptance_and_projection_keep_host_decisions_separate_from_claims():
    condition = ProposedCondition(
        "c1", ("r1",), SemanticDimension.TIME, "during_year", Polarity.NEGATIVE,
        "2025", (span("2025"),),
    )
    accepted = InterpretationAcceptance(
        proposal(requests=(ProposedRequest(
            "r1", "s1", Subject.UNSPECIFIED, (span("contratos"),),
            condition_expression=atom("c1"),
        ),), conditions=(condition,)), AcceptanceStatus.READY,
        (HostResolution("c1", ResolutionDecision.RESOLVED, "temporal-resolver", ("2025",)),),
    )
    projected = InterpretationProjection(
        accepted, ("r1",), "processes", "SELECT * WHERE year != 2025",
        (ConditionDisposition(
            "c1", ("r1",), Disposition.CONSUMED, Effect.EQUIVALENT,
            "during_year", Polarity.NEGATIVE, "BLOCK_UNKNOWN", "year != 2025",
        ),),
    )
    assert json.loads(json.dumps(projected.to_dict()))["acceptance"]["status"] == "READY"
    with pytest.raises(ContractError, match="DISPOSITION_CHANGED_CLAIM"):
        InterpretationProjection(accepted, ("r1",), "processes", "SELECT *", (
            ConditionDisposition(
                "c1", ("r1",), Disposition.CONSUMED, Effect.EQUIVALENT,
                "during_year", Polarity.POSITIVE, "BLOCK_UNKNOWN", "year = 2025",
            ),
        ))
    with pytest.raises(ContractError, match="INCOMPLETE_PROJECTION_DISPOSITIONS"):
        InterpretationProjection(accepted, ("r1",), "processes", "SELECT *", ())


def test_nonready_acceptance_cannot_project_or_smuggle_foreign_resolution():
    item = proposal()
    with pytest.raises(ContractError, match="FOREIGN_RESOLUTION_TARGET"):
        InterpretationAcceptance(item, AcceptanceStatus.READY, (
            HostResolution("foreign", ResolutionDecision.RESOLVED, "host"),
        ))
    blocked = InterpretationAcceptance(item, AcceptanceStatus.UNSUPPORTED, reason="MULTI_REQUEST")
    with pytest.raises(ContractError, match="PROJECTION_REQUIRES_READY"):
        InterpretationProjection(blocked, ("r1",), "processes", "SELECT *", ())


def test_projection_covers_only_conditions_owned_by_its_requests():
    requests = (
        ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (span("contratos"),)),
        ProposedRequest("r2", "s2", Subject.UNSPECIFIED, (span("procesos"),),
                        condition_expression=atom("c2")),
    )
    condition = ProposedCondition(
        "c2", ("r2",), SemanticDimension.TIME, "during_year", Polarity.POSITIVE,
        "2025", (span("2025"),),
    )
    accepted = InterpretationAcceptance(
        proposal(requests=requests, conditions=(condition,)), AcceptanceStatus.READY,
    )
    InterpretationProjection(accepted, ("r1",), "contracts", "SELECT *", ())
    with pytest.raises(ContractError, match="INCOMPLETE_PROJECTION_DISPOSITIONS"):
        InterpretationProjection(accepted, ("r2",), "processes", "SELECT *", ())


def test_blocked_projection_has_explicit_error_and_no_query():
    accepted = InterpretationAcceptance(proposal(), AcceptanceStatus.READY)
    blocked = InterpretationProjection(
        accepted, ("r1",), None, None, (),
        ProjectionStatus.BLOCKED, "NO_SAFE_SOURCE_PROJECTION",
    )
    assert blocked.to_dict()["query"] is None
    with pytest.raises(ContractError, match="BLOCKED_PROJECTION_REQUIRES_REASON_AND_NO_QUERY"):
        InterpretationProjection(
            accepted, ("r1",), "contracts", "SELECT *", (),
            ProjectionStatus.BLOCKED, "NO_SAFE_SOURCE_PROJECTION",
        )


def test_request_proposes_source_and_universe_with_original_evidence():
    raw = "contratos adjudicados en SECOP II"
    request = ProposedRequest(
        "r1", "s1", Subject.CONTRACTS, (RawSpan(0, 9, "contratos"),),
        source_claim="SECOP II", source_evidence=RawSpan(25, 33, "SECOP II"),
        universe_claim="adjudicados", universe_evidence=RawSpan(10, 21, "adjudicados"),
    )
    proposal(requests=(request,), raw=raw).validate_raw(raw)
    with pytest.raises(ContractError, match="UNGROUNDED_SOURCE_CLAIM"):
        ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, 9, "contratos"),),
                        source_claim="SECOP II")
    bad = ProposedRequest(
        "r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, 9, "contratos"),),
        source_claim="SECOP II", source_evidence=RawSpan(10, 18, "SECOP II"),
    )
    with pytest.raises(ContractError, match="UNBOUND_RAW_SPAN"):
        proposal(requests=(bad,), raw=raw).validate_raw(raw)


def test_boolean_composition_is_explicit_and_request_scoped():
    conditions = (
        ProposedCondition("c1", ("r1",), SemanticDimension.TOPIC, "contains",
                          Polarity.POSITIVE, "aseo", (span("aseo"),)),
        ProposedCondition("c2", ("r1",), SemanticDimension.TOPIC, "contains",
                          Polarity.POSITIVE, "obra", (span("obra"),)),
        ProposedCondition("c3", ("r1",), SemanticDimension.TIME, "during_year",
                          Polarity.POSITIVE, "2025", (span("2025"),)),
    )
    expression = ConditionExpression(LogicOperator.AND, children=(
        ConditionExpression(LogicOperator.OR, children=(atom("c1"), atom("c2"))),
        ConditionExpression(LogicOperator.NOT, children=(atom("c3"),)),
    ))
    request = ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (span("contratos"),),
                              condition_expression=expression)
    item = proposal(requests=(request,), conditions=conditions)
    item.validate_raw(RAW)
    assert item.to_dict()["requests"][0]["condition_expression"]["operator"] == "AND"
    assert expression.atoms() == ("c1", "c2", "c3")
    with pytest.raises(ContractError, match="INCOMPLETE_CONDITION_EXPRESSION"):
        proposal(conditions=conditions)
    with pytest.raises(ContractError, match="DUPLICATE_CONDITION_ATOM"):
        proposal(requests=(ProposedRequest(
            "r1", "s1", Subject.UNSPECIFIED, (span("contratos"),),
            condition_expression=ConditionExpression(
                LogicOperator.OR, children=(atom("c1"), atom("c1"))),
        ),), conditions=conditions)


def test_boolean_expression_rejects_foreign_and_cross_request_atoms():
    requests = (
        ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (span("contratos"),),
                        condition_expression=atom("c2")),
        ProposedRequest("r2", "s2", Subject.UNSPECIFIED, (span("procesos"),),
                        condition_expression=atom("c2")),
    )
    condition = ProposedCondition("c2", ("r2",), SemanticDimension.TIME, "during_year",
                                  Polarity.POSITIVE, "2025", (span("2025"),))
    with pytest.raises(ContractError, match="FOREIGN_CONDITION_ATOM"):
        proposal(requests=requests, conditions=(condition,))
    with pytest.raises(ContractError, match="INVALID_NOT_ARITY"):
        ConditionExpression(LogicOperator.NOT, children=(atom("c2"), atom("c2")))
    with pytest.raises(ContractError, match="INVALID_GROUP_ARITY"):
        ConditionExpression(LogicOperator.AND, children=(atom("c2"),))


def test_unprojectable_material_is_representable_but_never_ready():
    raw = "contratos con huella de carbono verificada"
    request = ProposedRequest(
        "r1", "s1", Subject.UNSPECIFIED, (RawSpan(0, 9, "contratos"),),
        condition_expression=atom("c1"),
    )
    condition = ProposedCondition(
        "c1", ("r1",), SemanticDimension.UNSUPPORTED, "IS_VERIFIED",
        Polarity.POSITIVE, "huella de carbono verificada",
        (RawSpan(14, 42, "huella de carbono verificada"),),
        unsupported_label="carbon_footprint",
    )
    item = proposal(requests=(request,), conditions=(condition,), raw=raw)
    item.validate_raw(raw)
    accepted = InterpretationAcceptance(
        item, AcceptanceStatus.UNSUPPORTED, reason="UNSUPPORTED_CONDITION",
    )
    assert accepted.to_dict()["proposal"]["conditions"][0]["unsupported_label"] == "carbon_footprint"
    with pytest.raises(ContractError, match="UNSUPPORTED_DIMENSION_READY"):
        InterpretationAcceptance(item, AcceptanceStatus.READY)
    with pytest.raises(ContractError, match="INVALID_DIMENSION"):
        ProposedCondition("c1", ("r1",), "carbon_footprint", "IS_VERIFIED",
                          Polarity.POSITIVE, "huella", (RawSpan(14, 20, "huella"),))
    with pytest.raises(ContractError, match="INVALID_UNSUPPORTED_LABEL"):
        ProposedCondition("c1", ("r1",), SemanticDimension.UNSUPPORTED, "IS_VERIFIED",
                          Polarity.POSITIVE, "huella", (RawSpan(14, 20, "huella"),))


def _opportunity_records():
    raw = "Busca oportunidades de aseo"
    quote = RawSpan(6, 19, "oportunidades")
    request = ProposedRequest(
        "r1", "s1", Subject.PROCESSES, (quote,),
        universe_claim="oportunidades", universe_evidence=quote,
        condition_expression=atom("c1"),
    )
    condition = ProposedCondition(
        "c1", ("r1",), SemanticDimension.RETRIEVAL_INTENT, "is",
        Polarity.POSITIVE, "oportunidades", (quote,),
        role=ConditionRole.REQUESTED_UNIVERSE,
    )
    item = proposal(requests=(request,), conditions=(condition,), raw=raw)
    resolution = HostResolution(
        "r1", ResolutionDecision.RESOLVED, "source-universe-resolver", ("p6dx-8zbt",),
    )
    policy = HostPolicy("O1B", "1", ("c1",), Effect.NARROWING)
    disposition = ConditionDisposition(
        "c1", ("r1",), Disposition.TRANSFORMED, Effect.NARROWING,
        "is", Polarity.POSITIVE, "BLOCK_UNKNOWN", "O1B@1",
        role=ConditionRole.REQUESTED_UNIVERSE,
    )
    return raw, item, resolution, policy, disposition


def test_opportunity_claim_is_request_owned_and_policy_bound_for_each_operation():
    raw, item, resolution, policy, disposition = _opportunity_records()
    item.validate_raw(raw)
    accepted = InterpretationAcceptance(
        item, AcceptanceStatus.READY, (resolution,), (policy,),
    )
    for operation in ("query", "count"):
        projected = InterpretationProjection(
            accepted, ("r1",), "p6dx-8zbt", "SELECT count(*) WHERE 1=0",
            (disposition,), operation=operation,
        )
        assert projected.to_dict()["operation"] == operation
    with pytest.raises(ContractError, match="INVALID_PROJECTION_OPERATION"):
        InterpretationProjection(accepted, ("r1",), "p6dx-8zbt", "SELECT *",
                                 (disposition,), operation="delete")
    with pytest.raises(ContractError, match="REQUEST_UNIVERSE_DATASET_MISMATCH"):
        InterpretationProjection(accepted, ("r1",), "jbjy-vk9h", "SELECT *", (disposition,))
    with pytest.raises(ContractError, match="REQUEST_UNIVERSE_POLICY_MISMATCH"):
        InterpretationProjection(accepted, ("r1",), "p6dx-8zbt", "SELECT *", (
            ConditionDisposition(
                "c1", ("r1",), Disposition.TRANSFORMED, Effect.NARROWING,
                "is", Polarity.POSITIVE, "BLOCK_UNKNOWN", "UNDECLARED@1",
                role=ConditionRole.REQUESTED_UNIVERSE,
            ),
        ))


def test_request_universe_claim_cannot_be_ungrounded_or_disjoined():
    raw, item, resolution, policy, disposition = _opportunity_records()
    request = item.requests[0]
    with pytest.raises(ContractError, match="REQUEST_UNIVERSE_EVIDENCE_MISMATCH"):
        proposal(requests=(ProposedRequest(
            "r1", "s1", Subject.PROCESSES, request.evidence,
            universe_claim="procesos", universe_evidence=request.universe_evidence,
            condition_expression=atom("c1"),
        ),), conditions=item.conditions, raw=raw)
    with pytest.raises(ContractError, match="NONCONJUNCTIVE_REQUEST_UNIVERSE"):
        proposal(requests=(ProposedRequest(
            "r1", "s1", Subject.PROCESSES, request.evidence,
            universe_claim="oportunidades", universe_evidence=request.universe_evidence,
            condition_expression=ConditionExpression(LogicOperator.NOT, children=(atom("c1"),)),
        ),), conditions=item.conditions, raw=raw)
    topic = ProposedCondition(
        "c2", ("r1",), SemanticDimension.TOPIC, "contains", Polarity.POSITIVE,
        "aseo", (RawSpan(23, 27, "aseo"),),
    )
    with pytest.raises(ContractError, match="NONCONJUNCTIVE_REQUEST_UNIVERSE"):
        proposal(requests=(ProposedRequest(
            "r1", "s1", Subject.PROCESSES, request.evidence,
            universe_claim="oportunidades", universe_evidence=request.universe_evidence,
            condition_expression=ConditionExpression(
                LogicOperator.OR, children=(atom("c1"), atom("c2")),
            ),
        ),), conditions=(*item.conditions, topic), raw=raw)
    with pytest.raises(ContractError, match="REQUEST_UNIVERSE_POLICY_REQUIRED"):
        InterpretationAcceptance(item, AcceptanceStatus.READY, (resolution,))
    with pytest.raises(ContractError, match="REQUEST_UNIVERSE_RESOLUTION_REQUIRED"):
        InterpretationAcceptance(item, AcceptanceStatus.READY, policies=(policy,))
    accepted = InterpretationAcceptance(item, AcceptanceStatus.READY, (resolution,), (policy,))
    with pytest.raises(ContractError, match="DISPOSITION_CHANGED_CLAIM"):
        InterpretationProjection(accepted, ("r1",), "p6dx-8zbt", "SELECT *", (
            ConditionDisposition(
                "c1", ("r1",), Disposition.TRANSFORMED, Effect.NARROWING,
                "is", Polarity.POSITIVE, "BLOCK_UNKNOWN", "O1B@1",
                role=ConditionRole.FILTER,
            ),
        ))
    assert disposition.role is ConditionRole.REQUESTED_UNIVERSE


def test_preproposal_failure_has_no_fabricated_proposal_or_projection():
    for status in (AcceptanceStatus.PROVIDER_ERROR, AcceptanceStatus.INVALID_PROPOSAL):
        failed = InterpretationAcceptance(None, status, reason="PRODUCER_FAILURE")
        assert failed.to_dict()["proposal"] is None
        with pytest.raises(ContractError, match="NO_PROPOSAL"):
            failed.validate_raw(RAW)
        with pytest.raises(ContractError, match="PROJECTION_REQUIRES_READY"):
            InterpretationProjection(failed, ("r1",), "p6dx-8zbt", "SELECT *", ())
    with pytest.raises(ContractError, match="PROPOSAL_REQUIRED"):
        InterpretationAcceptance(None, AcceptanceStatus.READY, reason="READY")
    with pytest.raises(ContractError, match="PROPOSAL_REQUIRED"):
        InterpretationAcceptance(None, AcceptanceStatus.PROVIDER_ERROR)


@given(st.integers(min_value=0, max_value=6))
def test_request_universe_atom_stays_conjunctive_at_any_depth(depth: int):
    expression = atom("universe")
    for index in range(depth):
        expression = ConditionExpression(
            LogicOperator.AND, children=(expression, atom(f"filter-{index}")),
        )
    assert not expression.has_nonconjunctive_atom("universe")
    assert ConditionExpression(
        LogicOperator.OR, children=(expression, atom("other")),
    ).has_nonconjunctive_atom("universe")
    assert ConditionExpression(
        LogicOperator.NOT, children=(expression,),
    ).has_nonconjunctive_atom("universe")
