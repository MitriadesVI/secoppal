"""Shared IR contract for the C1.4 host's typed operation handoff."""

import hashlib
import json

import pytest
from hypothesis import given, strategies as st

from app.core.foundation_acceptance_loader import load_acceptance
from app.core.foundation_operations import Aspect, Operation, Subject
from app.core.interpretation_contract import (
    AcceptanceStatus,
    ContractError,
    Effect,
    HostPolicy,
    HostResolution,
    InterpretationAcceptance,
    InterpretationProjection,
    InterpretationProposal,
    ProposedJob,
    ProposedRequest,
    RawSpan,
    ResolutionDecision,
)


RAW = "cuenta contratos"
SPAN = RawSpan(0, len(RAW), RAW)


def count_proposal(*operations: Operation) -> InterpretationProposal:
    jobs = tuple(
        ProposedJob(
            f"job-{index}",
            operation,
            evidence=(SPAN,),
            aspect=(Aspect.BY_YEAR if operation is Operation.GROUP_BY else
                    Aspect.TOTAL if operation is Operation.COUNT else None),
        )
        for index, operation in enumerate(operations, start=1)
    )
    return InterpretationProposal(
        hashlib.sha256(RAW.encode()).hexdigest(), "test-producer", "1",
        (), (), (), (), jobs,
    )


def search_proposal() -> InterpretationProposal:
    request = ProposedRequest("r1", "s1", Subject.UNSPECIFIED, (SPAN,))
    return InterpretationProposal(
        hashlib.sha256(RAW.encode()).hexdigest(), "test-producer", "1",
        (request,), (), (), (),
        (ProposedJob("job-search", Operation.SEARCH, (request.request_id,)),),
    )


def handoff(proposal=None, reason="OPERATION_HANDOFF:COUNT:ANALYTICS", **values):
    return InterpretationAcceptance(
        count_proposal(Operation.COUNT) if proposal is None else proposal,
        AcceptanceStatus.HANDOFF,
        reason=reason,
        **values,
    )


def test_handoff_is_typed_proposal_only_and_round_trips_through_json_loader():
    accepted = handoff(reason="OPERATION_HANDOFF:COUNT:UNREGISTERED_OWNER")
    payload = json.loads(json.dumps(accepted.to_dict(), allow_nan=False))

    assert accepted.status is AcceptanceStatus.HANDOFF
    assert payload["status"] == "HANDOFF"
    assert payload["reason"] == "OPERATION_HANDOFF:COUNT:UNREGISTERED_OWNER"
    assert payload["resolutions"] == [] and payload["policies"] == []
    assert load_acceptance(payload) == accepted


@given(
    operation=st.sampled_from([operation for operation in Operation if operation is not Operation.SEARCH]),
    owner=st.from_regex(r"[A-Z][A-Z0-9_]{0,15}", fullmatch=True),
)
def test_handoff_accepts_each_typed_single_non_search_operation_and_owner_token(operation, owner):
    accepted = handoff(
        proposal=count_proposal(operation),
        reason=f"OPERATION_HANDOFF:{operation.value}:{owner}",
    )

    assert accepted.proposal.jobs[0].operation is operation


@pytest.mark.parametrize("reason", [
    "",
    "OPERATION_HANDOFF:COUNT",
    "OPERATION_HANDOFF:COUNT:OWNER:EXTRA",
    "OPERATION_HANDOFF:UNKNOWN:OWNER",
    "OPERATION_HANDOFF:MULTI:OWNER",
    "OPERATION_HANDOFF:COUNT:lowercase_owner",
    "OPERATION_HANDOFF:COUNT:owner with spaces",
    " OPERATION_HANDOFF:COUNT:OWNER",
    "OPERATION_HANDOFF:COUNT:OWNER\n",
])
def test_handoff_rejects_malformed_reason(reason):
    with pytest.raises(ContractError, match="^INVALID_HANDOFF_REASON$"):
        handoff(reason=reason)


def test_handoff_requires_an_exactly_matching_operation_and_one_job():
    with pytest.raises(ContractError, match="^HANDOFF_OPERATION_MISMATCH$"):
        handoff(reason="OPERATION_HANDOFF:SUM_VALUE:ANALYTICS")

    with pytest.raises(ContractError, match="^HANDOFF_SINGLE_JOB_REQUIRED$"):
        handoff(proposal=count_proposal())

    with pytest.raises(ContractError, match="^HANDOFF_SINGLE_JOB_REQUIRED$"):
        handoff(proposal=count_proposal(Operation.COUNT, Operation.SUM_VALUE))


def test_handoff_rejects_search_and_missing_proposal():
    with pytest.raises(ContractError, match="^HANDOFF_SEARCH_FORBIDDEN$"):
        handoff(proposal=search_proposal(), reason="OPERATION_HANDOFF:SEARCH:FOUNDATION")

    with pytest.raises(ContractError, match="^PROPOSAL_REQUIRED$"):
        InterpretationAcceptance(None, AcceptanceStatus.HANDOFF,
                                 reason="OPERATION_HANDOFF:COUNT:ANALYTICS")


def test_handoff_rejects_resolution_and_policy_values():
    with pytest.raises(ContractError, match="^HANDOFF_HAS_HOST_VALUES$"):
        handoff(resolutions=(HostResolution("target", ResolutionDecision.CLARIFY, "test"),))

    with pytest.raises(ContractError, match="^HANDOFF_HAS_HOST_VALUES$"):
        handoff(policies=(HostPolicy("policy", "1", ("condition",), Effect.EQUIVALENT),))


def test_handoff_status_must_be_the_enum_member_and_cannot_be_projected():
    with pytest.raises(ContractError, match="^INVALID_ACCEPTANCE$"):
        InterpretationAcceptance(count_proposal(Operation.COUNT), "HANDOFF",
                                 reason="OPERATION_HANDOFF:COUNT:ANALYTICS")

    accepted = handoff()
    with pytest.raises(ContractError, match="^PROJECTION_REQUIRES_READY$"):
        InterpretationProjection(accepted, (), None, None, ())


def test_loader_reapplies_handoff_validation_to_stored_reason():
    payload = handoff().to_dict()
    payload["reason"] = "OPERATION_HANDOFF:COUNT:UNKNOWN:ANALYTICS"

    with pytest.raises(ContractError, match="^INVALID_HANDOFF_REASON$"):
        load_acceptance(payload)
