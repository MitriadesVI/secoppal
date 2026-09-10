"""Result constructors — canonical shape for SecopalWorkflow.run_query outputs.

Why this module exists
----------------------
Today ``run_query`` returns a dict whose key set is duplicated across at
least eight branches (reset, pagination_more, pagination_no_context,
direct_lookup hit/miss/disambig, refinement, normal flow, …). When a UI
component starts depending on a new key — say ``contextual_filters`` —
some branches will set it and others will silently omit it. The bug
shows up downstream as "this works for X but not Y".

These constructors centralize the shape so:

1. Every branch must explicitly opt in or out of every field.
2. Forgetting a required field raises ``TypeError`` at call time,
   not a ``KeyError`` four call-frames deeper.
3. There is one place to evolve when the contract changes.

What's the canonical shape?
---------------------------
The union of keys observed across every branch of run_query as of the
refactor's start. See ``CANONICAL_KEYS`` below — this is the contract
the upstream UI (Streamlit + FastAPI) reads from.

Staging note
------------
Paso 3 only **introduces** these constructors. Paso 5 will migrate each
handler to use them. Until then, ``run_query`` continues to build dicts
by hand; the unit tests in ``tests/test_workflow_results.py`` keep this
module honest in the meantime.
"""
from __future__ import annotations

from typing import Any, TypedDict


# ─────────────────────────────────────────────────────────────────────────────
# Canonical contract
# ─────────────────────────────────────────────────────────────────────────────

# The full set of keys any run_query branch may emit. New branches MUST
# either populate or default every key in this set — that is the whole
# point of routing dict construction through this module.
CANONICAL_KEYS: frozenset[str] = frozenset({
    "response",
    "rows",
    "results",
    "dataset_id",
    "soql_query",
    "parsed_params",
    "resolved_params",
    "needs_llm",
    "needs_clarification",
    "route_reason",
    "degraded",
    "degraded_hint",
    "total_count",
    "universe_insights",
    "suggestions",
    "timeout_suggestions",
    "intent_type",
    "followup",
    "trace_id",
    "narrated",
    "followup_mutation",
    "entity_profile_view",
    # Optional / per-branch additions documented in the constructor
    # signatures below.
    "contextual_filters",
    "total_count_approximate",
    "risk_flag_timeout",
    "direct_lookup_status",
    "direct_lookup_reason",
    "successful_sources",
    "failed_sources",
    "search_attempts",
    "source_partial",
    # Etapa 4: outcome bidimensional + recuperación de refinamiento.
    "outcome",
    "outcome_reason",
    "query_fingerprint",
    "recovery_delta",
    "recall_audit",
    # Etapa 5: telemetría observacional.
    "http_metrics",
    # Etapa 6 H1: estado del conteo no bloqueante.
    "count_status",
    "count_error_type",
    "count_used_in_response",
    "count_completed_before_rows",
    "result_row_count",
    # Etapa 6 H2: deadline total del turno.
    "turn_deadline_ms",
    "turn_elapsed_ms",
    "time_remaining_before_fetch_ms",
    "fetch_budget_ms",
    "fetch_elapsed_ms",
    "fetch_status",
    "deadline_exhausted",
    "deadline_stage",
    "request_attempted",
    "request_skipped_due_to_budget",
    "count_skipped_due_to_budget",
    "source_partial",
    # Etapa 6 H4: fallback degradado sin ORDER BY + contrato de orden.
    "result_order_guarantee",
    "fallback_attempted",
    "fallback_used",
    "fallback_strategy",
    "fallback_rows",
    "fallback_elapsed_ms",
    "substantive_filters_preserved",
    "transformations_applied",
    "primary_query_status",
    "fallback_query_status",
    # Etapa 8B.2: estado explicito del buffer canonico.
    "source_candidate_total",
    "validated_total_known",
    "displayed_so_far",
    "source_total_approximate",
    "pagination_buffer_hit",
    "pagination_validation_ms",
    "candidate_count",
    "validated_count",
    "quarantined_count",
})


class WorkflowResult(TypedDict, total=False):
    """The shape every ``run_query`` branch returns.

    Total=False because branches differ in which optional keys they
    populate; the constructors below decide what gets filled.
    """

    response: str
    rows: list
    results: list[dict]
    dataset_id: str
    soql_query: str
    parsed_params: dict
    resolved_params: dict
    needs_llm: bool
    needs_clarification: bool
    route_reason: str
    degraded: bool
    degraded_hint: str
    total_count: int | None
    universe_insights: Any | None
    suggestions: list
    timeout_suggestions: list
    intent_type: str
    followup: bool
    trace_id: str
    narrated: bool
    followup_mutation: dict
    entity_profile_view: dict | None
    contextual_filters: dict
    total_count_approximate: bool
    risk_flag_timeout: bool
    direct_lookup_status: str
    direct_lookup_reason: str
    successful_sources: list[str]
    failed_sources: list[str]
    search_attempts: list[dict]
    source_partial: bool
    source_candidate_total: int
    validated_total_known: int
    displayed_so_far: int
    source_total_approximate: bool
    pagination_buffer_hit: bool
    pagination_validation_ms: float
    candidate_count: int
    validated_count: int
    quarantined_count: int
    outcome: str
    outcome_reason: str
    query_fingerprint: str
    recovery_delta: dict
    recall_audit: dict
    http_metrics: list[dict]
    count_status: str
    count_error_type: str | None
    count_used_in_response: bool
    count_completed_before_rows: bool
    result_row_count: int
    turn_deadline_ms: float
    turn_elapsed_ms: float
    time_remaining_before_fetch_ms: float
    fetch_budget_ms: float
    fetch_elapsed_ms: float
    fetch_status: str
    deadline_exhausted: bool
    deadline_stage: str
    request_attempted: bool
    request_skipped_due_to_budget: bool
    count_skipped_due_to_budget: bool
    source_partial: bool


# ─────────────────────────────────────────────────────────────────────────────
# Constructors
# ─────────────────────────────────────────────────────────────────────────────

def empty_result(
    *,
    response: str,
    route_reason: str,
    needs_clarification: bool = False,
    intent_type: str = "",
    followup: bool = False,
    parsed_params: dict | None = None,
    resolved_params: dict | None = None,
    soql_query: str = "",
    dataset_id: str = "",
    total_count: int | None = 0,
) -> dict:
    """A result with no rows — for reset, clarifications, and lookup misses.

    Required: ``response`` and ``route_reason``. Everything else has a
    sane default that matches what run_query currently emits for empty
    branches.
    """
    return {
        "response": response,
        "rows": [],
        "results": [],
        "dataset_id": dataset_id,
        "soql_query": soql_query,
        "parsed_params": parsed_params if parsed_params is not None else {},
        "resolved_params": resolved_params if resolved_params is not None else {},
        "needs_llm": False,
        "needs_clarification": needs_clarification,
        "route_reason": route_reason,
        "degraded": False,
        "degraded_hint": "",
        "total_count": total_count,
        "universe_insights": None,
        "suggestions": [],
        "timeout_suggestions": [],
        "intent_type": intent_type,
        "followup": followup,
        "trace_id": "",
        "narrated": False,
    }


def tabular_result(
    *,
    response: str,
    rows: list,
    results: list[dict],
    dataset_id: str,
    soql_query: str,
    parsed_params: dict,
    resolved_params: dict,
    route_reason: str,
    total_count: int | None,
    intent_type: str = "",
    followup: bool = False,
    needs_clarification: bool = False,
    needs_llm: bool = False,
    degraded: bool = False,
    degraded_hint: str = "",
    universe_insights: Any | None = None,
    suggestions: list | None = None,
    timeout_suggestions: list | None = None,
    contextual_filters: dict | None = None,
    total_count_approximate: bool | None = None,
    risk_flag_timeout: bool | None = None,
) -> dict:
    """A result that carries rows — refinement, pagination, direct lookup hit, normal flow.

    Required (keyword-only): the ten fields that downstream renderers
    actually read. Optional flags default to ``False`` / ``None`` and are
    omitted from the dict when their value is ``None``, so the output
    matches the legacy shape for backwards compatibility.
    """
    out: dict = {
        "response": response,
        "rows": rows,
        "results": results,
        "dataset_id": dataset_id,
        "soql_query": soql_query,
        "parsed_params": parsed_params,
        "resolved_params": resolved_params,
        "needs_llm": needs_llm,
        "needs_clarification": needs_clarification,
        "route_reason": route_reason,
        "degraded": degraded,
        "degraded_hint": degraded_hint,
        "total_count": total_count,
        "universe_insights": universe_insights,
        "suggestions": suggestions if suggestions is not None else [],
        "timeout_suggestions": timeout_suggestions if timeout_suggestions is not None else [],
        "intent_type": intent_type,
        "followup": followup,
        "trace_id": "",
        "narrated": False,
    }
    if contextual_filters is not None:
        out["contextual_filters"] = contextual_filters
    if total_count_approximate is not None:
        out["total_count_approximate"] = total_count_approximate
    if risk_flag_timeout is not None:
        out["risk_flag_timeout"] = risk_flag_timeout
    return out
