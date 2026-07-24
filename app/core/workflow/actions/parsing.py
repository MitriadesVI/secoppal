"""Parsing actions — user_query → parsed_params.

Three Burr nodes that turn the raw text into a structured parameter
dict before any SECOP call:

- ``parse_query``    — deterministic parser (QueryRouter).
- ``llm_parse``      — LLM fallback when the parser flags ``needs_llm``.
- ``apply_context``  — merges previous-turn frame into the current
                      parsed_params for follow-up turns.

Moved here verbatim from ``app/core/orchestrator.py`` in Paso 5B. No
behavior changes; the @action decorators, ``reads``/``writes``
declarations and bodies are byte-for-byte identical.
"""
from __future__ import annotations

import time

from burr.core import State, action

from app.core.followup_engine import FollowupGuards, detect_and_merge
from app.core.llm_handler import LLMHandler
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter
from app.core.workflow.helpers import _with_timing


@action(
    reads=["user_query", "timings_ms"],
    writes=[
        "parsed_params", "needs_llm", "route_reason",
        "modality_interpretation", "timings_ms",
    ],
)
def parse_query(state: State, query_router: QueryRouter) -> State:
    start = time.perf_counter()
    result = query_router.parse(state["user_query"])
    return _with_timing(
        state,
        "parse_ms",
        start,
        parsed_params=result.params,
        needs_llm=result.needs_llm,
        route_reason=result.route_reason,
        modality_interpretation=result.modality_interpretation,
    )


@action(reads=["user_query", "needs_llm", "parsed_params", "timings_ms"], writes=["parsed_params", "timings_ms"])
def llm_parse(state: State, llm_handler: LLMHandler) -> State:
    start = time.perf_counter()
    if not state["needs_llm"]:
        return _with_timing(state, "llm_parse_ms", start)
    parsed = llm_handler.parse(state["user_query"], existing_params=state["parsed_params"])
    return _with_timing(state, "llm_parse_ms", start, parsed_params=parsed)


@action(reads=["parsed_params", "context_params", "followup", "user_query", "timings_ms"], writes=["parsed_params", "needs_llm", "needs_clarification", "clarification_reason", "intent_type", "followup_intent_type", "followup_mutation", "timings_ms"])
def apply_context(state: State) -> State:
    """Fusiona context_params del turno anterior con parsed_params del parse actual.
    Usa followup_engine.detect_and_merge para clasificar la intención granular
    y aplicar políticas de merge por tipo de follow-up.

    Solo corre cuando followup=True.
    """
    start = time.perf_counter()
    if not state.get("followup"):
        return _with_timing(state, "apply_context_ms", start)

    previous_frame = frame_from_params(state.get("context_params", {}))
    mutation_trace: dict = {}
    intent, merged = detect_and_merge(
        text=state.get("user_query", ""),
        current_params=state.get("parsed_params", {}),
        previous_frame=previous_frame,
        mutation_trace=mutation_trace,
    )

    # Guard contra WHERE 1=1 global.
    # Excepción: una opportunity_search sin scope/topic NO es "contexto
    # perdido" — es una oportunidad demasiado amplia que aún necesita rubro.
    # Dejamos que P15 (plan_query) la gobierne, que da una aclaración mucho
    # mejor ("¿qué producto/servicio buscas?") que el genérico de abajo.
    is_opportunity = merged.get("intent_type") == "opportunity_search"
    if not is_opportunity and not FollowupGuards.check_no_where_1_1(merged):
        return _with_timing(
            state,
            "apply_context_ms",
            start,
            parsed_params=merged,
            needs_llm=False,
            needs_clarification=True,
            clarification_reason=(
                "Te entendi como refinamiento, pero perdi el contexto anterior. "
                "Haz una busqueda base primero o usa reset."
            ),
            intent_type=intent,
            followup_intent_type=intent,
            followup_mutation=mutation_trace,
        )

    # Si el merge produjo contexto suficiente, no necesita LLM
    has_context = FollowupGuards.check_no_where_1_1(merged)
    if has_context:
        return _with_timing(state, "apply_context_ms", start, parsed_params=merged, needs_llm=False, intent_type=intent, followup_intent_type=intent, followup_mutation=mutation_trace)

    return _with_timing(state, "apply_context_ms", start, parsed_params=merged, intent_type=intent, followup_intent_type=intent, followup_mutation=mutation_trace)
