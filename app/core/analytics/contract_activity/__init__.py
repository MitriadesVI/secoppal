"""Narrow one-turn Contract Activity handler."""
from __future__ import annotations

from typing import Any

from .admission import admit_contract_activity
from .models import (
    CONTRACTS_DATASET,
    CONTRACT_ACTIVITY_INTENT,
    AdmissionStatus,
    ContractActivityDecision,
    DecisionKind,
)
from .planning import build_plan
from .presentation import clarification, render_result
from .runtime import ContractActivityUnavailable, execute_plan


def handle_contract_activity(
    user_query: str,
    params: dict,
    secop_client: Any,
) -> ContractActivityDecision:
    admission = admit_contract_activity(user_query, params)
    if admission.status is AdmissionStatus.HANDOFF:
        return ContractActivityDecision(DecisionKind.HANDOFF, reason=admission.reason)
    if admission.status is AdmissionStatus.CLARIFY:
        return ContractActivityDecision(DecisionKind.CLARIFY, reason=admission.reason)
    assert admission.request is not None
    plan = build_plan(admission.request)
    try:
        result = execute_plan(plan, secop_client)
    except ContractActivityUnavailable as exc:
        return ContractActivityDecision(
            DecisionKind.CLARIFY,
            reason=str(exc),
            request=admission.request,
            plan=plan,
        )
    return ContractActivityDecision(
        DecisionKind.ANSWER,
        request=admission.request,
        plan=plan,
        result=result,
    )


def as_analytics_payload(decision: ContractActivityDecision) -> dict | None:
    if decision.kind is DecisionKind.HANDOFF:
        return None
    if decision.kind is DecisionKind.CLARIFY:
        return {
            "response": clarification(decision.reason),
            "needs_clarification": True,
            "clarification_reason": clarification(decision.reason),
            "analytical_intent": CONTRACT_ACTIVITY_INTENT,
            "route_reason": "analytics_contract_activity_clarify",
            "soql_query": decision.plan.scalar_sql if decision.plan else "",
            "dataset_id": CONTRACTS_DATASET,
            "results": [],
            "total_count": None,
        }
    assert decision.request is not None and decision.plan is not None
    assert decision.result is not None
    return {
        "response": render_result(decision.request, decision.result),
        "needs_clarification": False,
        "clarification_reason": "",
        "analytical_intent": CONTRACT_ACTIVITY_INTENT,
        "route_reason": "analytics_contract_activity",
        "soql_query": decision.plan.scalar_sql,
        "dataset_id": CONTRACTS_DATASET,
        "results": [],
        "total_count": decision.result.total,
    }


__all__ = [
    "AdmissionStatus", "ContractActivityDecision", "DecisionKind",
    "as_analytics_payload", "handle_contract_activity",
]
