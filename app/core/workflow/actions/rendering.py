"""Rendering actions — state → user-facing response.

The final three Burr nodes in the pipeline:

- ``suggester_action`` — compute the suggestion list from current results
                       + universe_insights (no SECOP IO).
- ``format_response``  — assemble the channel-formatted response string +
                       structured rows; handle timeout/4xx/5xx error
                       messaging and degraded/timeout notes.
- ``clarify_query``    — terminal action when any earlier node sets
                       ``needs_clarification``; renders the question.

Moved here verbatim from ``app/core/orchestrator.py`` in Paso 5E. No
behavior changes; this is the most user-visible domain so the golden
suite plus the corpus are the tightest guard against regressions.
"""
from __future__ import annotations

import time

from burr.core import State, action

from app.core.formatter import Formatter
from app.core.response_policy import build_clarification_response
from app.core.suggester import generate_suggestions
from app.core.workflow.helpers import _with_timing


@action(
    reads=["resolved_params", "universe_insights", "total_count", "dataset_id", "results", "timings_ms"],
    writes=["suggestions", "timings_ms"],
)
def suggester_action(state: State) -> State:
    start = time.perf_counter()
    suggestions = generate_suggestions(
        params=state.get("resolved_params", {}),
        universe_insights=state.get("universe_insights"),
        total_count=state.get("total_count") or 0,
        rows=state.get("results", []),
        dataset_id=state.get("dataset_id", ""),
        intent_type=state.get("intent_type", ""),
    )
    return _with_timing(state, "suggest_ms", start, suggestions=suggestions)


@action(reads=["results", "dataset_id", "channel", "query_error", "risk_flag_timeout", "resolved_params", "degraded", "degraded_hint", "total_count", "universe_insights", "suggestions", "timeout_suggestions", "analytical_response", "timings_ms"], writes=["formatted_response", "formatted_rows", "timings_ms"])
def format_response(state: State, formatter: Formatter) -> State:
    start = time.perf_counter()
    # Modo analítico: la respuesta ya viene formateada desde execute_query.
    # No pasar por formatter (no hay rows tabulares que renderizar).
    analytical = state.get("analytical_response")
    if analytical:
        return _with_timing(
            state,
            "format_ms",
            start,
            formatted_response=analytical,
            formatted_rows=[],
        )
    if state.get("query_error"):
        err = str(state.get("query_error") or "")
        err_lower = err.lower()
        # DOGFOOD-ERRMSG-007: distinguir 4xx (bug del builder) de timeout real
        if "400" in err or "bad request" in err_lower or "unexpected character" in err_lower:
            message = (
                "⚠️ Error interno: la consulta SoQL generada fue rechazada por SECOP "
                "(400 Bad Request). Este es un bug del builder, no de SECOP — "
                "el equipo ya tiene el trace para revisar."
            )
        elif "404" in err:
            message = "SECOP devolvió 404 — el dataset no respondió. Reintenta."
        elif "5" in err[:3] or "server error" in err_lower:
            message = "SECOP tuvo un error interno (5xx). Reintenta en unos segundos."
        elif state.get("risk_flag_timeout"):
            message = (
                "⏱️ SECOP no respondió a tiempo. "
                "No puedo confirmar si hay o no resultados."
            )
        else:
            message = "⏱️ SECOP no respondió a tiempo. Intenta de nuevo en unos segundos."
        return _with_timing(
            state,
            "format_ms",
            start,
            formatted_response=message,
            formatted_rows=[],
        )
    params = state.get("resolved_params") or {}
    total_count = state.get("total_count", 0)
    universe_insights = state.get("universe_insights")
    suggestions = state.get("suggestions", [])
    approximate = state.get("total_count_approximate", False)

    # Commit 6D: rendering SOLO presenta. El orden ya lo decidió `rank_results`
    # (acción previa); aquí solo consumimos ranked_results y el formatter corta a 25.
    rows_to_format = state.get("ranked_results", state["results"])

    order_guarantee = state.get("result_order_guarantee", "exact_order")
    response, rows = formatter.format_for_channel(
        rows_to_format, state["dataset_id"], state["channel"],
        params=params, total_count=total_count, universe_insights=universe_insights,
        suggestions=suggestions, total_count_approximate=approximate,
        order_guarantee=order_guarantee,
    )
    # H4: muestra recuperada por el fallback (sin ORDER BY) → avisar que NO hay garantía de
    # orden, para que el usuario no lea las filas como un ranking ("más caros"/"más recientes").
    if order_guarantee == "unordered_sample":
        note = ("⚠️ La búsqueda se demoró; recuperé una muestra de coincidencias, pero "
                "no pude garantizar el orden por fecha ni por valor:\n\n")
        response = note + response
    elif state.get("degraded") and state.get("degraded_hint"):
        note = f"No encontre resultados exactos. Te muestro resultados {state['degraded_hint']}:\n\n"
        response = note + response
    # Timeout retry: avisar que se cambió la estrategia
    if state.get("timeout_suggestions"):
        note = (
            "Entendi la busqueda, pero SECOP se demoro al ordenar todos los historicos por valor. "
            "Cambie a orden por fecha mas reciente.\n\n"
        )
        response = note + response
    return _with_timing(state, "format_ms", start, formatted_response=response, formatted_rows=rows)


@action(reads=["clarification_reason", "timings_ms"], writes=["formatted_response", "formatted_rows", "timings_ms"])
def clarify_query(state: State) -> State:
    start = time.perf_counter()
    plan = state.get("query_plan", {}) or {}
    if plan.get("clarification_question"):
        cr = build_clarification_response(plan)
        msg = cr["response"]
    else:
        msg = state["clarification_reason"] or "Necesito un poco mas de contexto para buscar en SECOP."
    return _with_timing(
        state,
        "clarify_ms",
        start,
        formatted_response=msg,
        formatted_rows=[],
    )
