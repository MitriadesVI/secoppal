"""Translate an Entity Profile turn into the existing analytical response channel."""
from __future__ import annotations

from typing import Any

from burr.core import State

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.presentation import (
    ENTITY_PROFILE_VIEW_KEY,
    project_entity_profile,
)
from app.core.entity_profile.render import render_entity_profile
from app.core.entity_profile.turn import EntityProfileTurn
from app.core.workflow.helpers import _with_timing


ENTITY_PROFILE_INTENT = "entity_profile"


def run_entity_profile_turn(
    turn: EntityProfileTurn,
    *,
    secop_client: Any,
    soql_builder: Any,
) -> dict | None:
    if not turn.detected:
        return None
    if turn.clarification:
        return {
            "needs_clarification": True,
            "clarification_reason": turn.clarification,
            "route_reason": "entity_profile_scope_clarification",
            "resolved_params": turn.params,
        }
    if turn.scope is None or turn.identity is None or turn.focus is None:
        return None
    try:
        profile = build_entity_profile(
            scope=turn.scope,
            identity=turn.identity,
            params=turn.params,
            focus=turn.focus,
            secop_client=secop_client,
            soql_builder=soql_builder,
            plan=turn.plan,
        )
    except Exception:
        return {
            "response": (
                "No pude construir el análisis institucional en este momento. "
                "No interpreto ese fallo como ausencia de actividad."
            ),
            "route_reason": "entity_profile_error",
            "resolved_params": turn.params,
            "partial": True,
            "calls_total": 0,
            "calls_by_aspect": {},
            "latency_by_aspect": {},
            "total_latency_ms": 0.0,
        }
    return {
        "response": render_entity_profile(profile),
        "route_reason": f"entity_profile_{turn.focus}",
        "resolved_params": turn.params,
        "partial": profile.partial,
        "calls_total": profile.source_calls.calls_total,
        "calls_by_aspect": profile.source_calls.calls_by_aspect,
        "latency_by_aspect": profile.source_calls.latency_by_aspect,
        "total_latency_ms": profile.source_calls.total_latency_ms,
        "profile": profile,
        ENTITY_PROFILE_VIEW_KEY: project_entity_profile(profile),
    }


def entity_profile_state(
    state: State,
    outcome: dict,
    *,
    execute_started_at: float,
) -> State:
    """Map the profile result without adding a second rendering pipeline."""
    timings = dict(state.get("timings_ms", {}) or {})
    timings["entity_profile_calls_total"] = float(outcome.get("calls_total", 0))
    timings["entity_profile_total_latency_ms"] = float(
        outcome.get("total_latency_ms", 0.0)
    )
    for aspect, calls in outcome.get("calls_by_aspect", {}).items():
        timings[f"entity_profile_{aspect}_calls"] = float(calls)
    for aspect, latency in outcome.get("latency_by_aspect", {}).items():
        timings[f"entity_profile_{aspect}_ms"] = float(latency)
    state = state.update(timings_ms=timings)
    if outcome.get("needs_clarification"):
        return _with_timing(
            state,
            "execute_total_ms",
            execute_started_at,
            results=[],
            query_error="",
            total_count=0,
            timeout_suggestions=[],
            risk_flag_timeout=False,
            needs_clarification=True,
            clarification_reason=outcome["clarification_reason"],
            analytical_response="",
            entity_profile_view=None,
            analytical_intent=ENTITY_PROFILE_INTENT,
            soql_query="",
            route_reason=outcome["route_reason"],
            resolved_params=outcome["resolved_params"],
        )
    return _with_timing(
        state,
        "execute_total_ms",
        execute_started_at,
        results=[],
        query_error="",
        total_count=0,
        timeout_suggestions=[],
        risk_flag_timeout=False,
        needs_clarification=False,
        clarification_reason="",
        analytical_response=outcome.get("response", ""),
        entity_profile_view=outcome.get(ENTITY_PROFILE_VIEW_KEY),
        analytical_intent=ENTITY_PROFILE_INTENT,
        soql_query="",
        route_reason=outcome.get("route_reason", "entity_profile_error"),
        resolved_params=outcome["resolved_params"],
    )
