"""Narrow one-turn Contract Activity handler."""
from __future__ import annotations

from datetime import date as Date
from typing import TYPE_CHECKING
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

if TYPE_CHECKING:
    from app.core.capability_binding.contract_activity_binding import (
        ContractActivityBinding,
    )


def handle_contract_activity(
    user_query: str,
    params: dict,
    secop_client: Any,
    *,
    binding: ContractActivityBinding | None = None,
    today: Date | None = None,
    semantic_evidence: object = None,
) -> ContractActivityDecision:
    receipt = binding.receipt(user_query) if binding is not None else None
    admission = admit_contract_activity(
        user_query,
        params,
        binding=binding,
        today=today,
        semantic_evidence=semantic_evidence,
    )
    if admission.status is AdmissionStatus.HANDOFF:
        if binding is not None:
            return ContractActivityDecision(
                DecisionKind.CLARIFY,
                reason="BOUND_CONTRACT_ACTIVITY_NOT_TERMINAL",
                binding_receipt=receipt,
            )
        return ContractActivityDecision(DecisionKind.HANDOFF, reason=admission.reason)
    if admission.status is AdmissionStatus.CLARIFY:
        return ContractActivityDecision(
            DecisionKind.CLARIFY,
            reason=admission.reason,
            binding_receipt=receipt,
        )
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
            binding_receipt=receipt,
        )
    return ContractActivityDecision(
        DecisionKind.ANSWER,
        request=admission.request,
        plan=plan,
        result=result,
        binding_receipt=receipt,
    )


def as_analytics_payload(decision: ContractActivityDecision) -> dict | None:
    if decision.kind is DecisionKind.HANDOFF:
        return None
    if decision.kind is DecisionKind.CLARIFY:
        payload = {
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
        if decision.binding_receipt is not None:
            payload["contract_activity_binding_receipt"] = decision.binding_receipt
        return payload
    assert decision.request is not None and decision.plan is not None
    assert decision.result is not None
    payload = {
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
    if decision.binding_receipt is not None:
        payload["contract_activity_binding_receipt"] = decision.binding_receipt
    return payload


__all__ = [
    "AdmissionStatus", "ContractActivityDecision", "DecisionKind",
    "as_analytics_payload", "handle_contract_activity",
]
