"""La ruta de perfil de V1.2, enganchada a la admisión y no al grafo normal.

El perfil de V1.1 vive DENTRO de `execute_query`: para llegar hasta él, un turno
pasa antes por `apply_context`, que fusiona los params del turno anterior. Con
eso, el significado ya venía mezclado cuando el perfil lo leía, y corregirlo al
final —como hacía `_composed_request`— llega tarde.

Aquí el perfil recibe el texto y SUS propios params, y el universo anterior
entra únicamente por el Frame confirmado. Este módulo no decide la ruta (eso es
la admisión) ni publica (eso es `turn_publication`): traduce un
`ProfileTurnOutcome` al contrato de resultado que ya usan los demás handlers.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.core.entity_profile.frame_turn import (
    ProfileTurnOutcome,
    TurnKind,
    run_profile_turn,
)
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.workflow.actions.entity_profile import ENTITY_PROFILE_INTENT
from app.core.workflow.results import empty_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


@dataclass(frozen=True, slots=True)
class ProfileRouteResult:
    result: dict
    outcome: ProfileTurnOutcome


def run_profile_route(
    deps: "WorkflowDeps",
    ctx: "HandlerContext",
    *,
    params: dict,
    state: ProfileConversationState,
    turn_id: str,
) -> ProfileRouteResult | None:
    outcome = run_profile_turn(
        user_query=ctx.user_query,
        params=params,
        state=state,
        turn_id=turn_id,
        soql_builder=deps.soql_builder,
        secop_client=deps.secop_client,
        today=ctx.today,
    )
    if outcome is None:
        return None
    return ProfileRouteResult(_result(outcome), outcome)


def _result(outcome: ProfileTurnOutcome) -> dict:
    result = empty_result(
        response=outcome.response,
        route_reason=outcome.route_reason,
        needs_clarification=outcome.kind is not TurnKind.ANSWER,
    )
    result.update({
        # El perfil es dueño TERMINAL de ejecución y presentación: nada aguas
        # abajo puede consultar la fuente ni reemplazar esta respuesta.
        "analytical_intent": ENTITY_PROFILE_INTENT,
        "analytical_response": (
            outcome.response if outcome.kind is TurnKind.ANSWER else ""
        ),
        "clarification_reason": (
            outcome.response if outcome.kind is not TurnKind.ANSWER else ""
        ),
        "timings_ms": _timings(outcome),
        "partial": bool(
            outcome.evidence is not None and not outcome.evidence.total_failure
            and len(outcome.evidence.succeeded_jobs) < len(outcome.plan.jobs)
        ),
        "profile_planned_jobs": outcome.planned_jobs,
        "profile_attempted_calls": outcome.attempted_calls,
        "profile_issues": list(outcome.issues),
        "entity_profile_view": outcome.profile_view,
    })
    return result


def _timings(outcome: ProfileTurnOutcome) -> dict:
    """`planned_jobs`, `attempted_calls` y métricas acreditadas, por separado.

    `executed_datasets` de V1.1 llamaba «ejecutadas» a las fuentes que sólo
    aparecían en el plan. Aquí las tres cosas tienen nombre propio.
    """
    timings: dict = {
        "entity_profile_planned_jobs": float(outcome.planned_jobs),
        "entity_profile_calls_total": float(outcome.attempted_calls),
    }
    evidence = outcome.evidence
    if evidence is None:
        timings["entity_profile_total_latency_ms"] = 0.0
        return timings
    timings["entity_profile_total_latency_ms"] = evidence.total_latency_ms
    for aspect, calls in evidence.calls_by_aspect().items():
        timings[f"entity_profile_{aspect}_calls"] = float(calls)
    for aspect, latency in evidence.latency_by_aspect().items():
        timings[f"entity_profile_{aspect}_ms"] = latency
    if outcome.record is not None:
        timings["entity_profile_accredited_metrics"] = float(
            outcome.record.accredited_metrics,
        )
    return timings


__all__ = ["ProfileRouteResult", "run_profile_route"]
