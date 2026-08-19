"""Execution actions — hit SECOP, degrade on failure, observe the universe.

Three Burr nodes that issue the actual HTTP requests:

- ``execute_query``      — analytical fast path + count/query orchestration
                          + retry-without-ordering + degraded fallback +
                          timeout transparency.
- ``degrade_query``      — if zero rows, try one relaxed query.
- ``observe_universe``   — compute insights when the sample is large enough.

Moved here verbatim from ``app/core/orchestrator.py`` in Paso 5D. No
behavior changes; the @action decorators, ``reads``/``writes`` and
bodies are byte-for-byte identical. Retry/timeout/degraded/count-lazy
semantics MUST remain bit-for-bit identical — the golden suite verifies
this end-to-end and several tests in tests/test_timeout_transparency.py,
tests/test_count_lazy.py, tests/test_degrade_query.py guard the corners.
"""
from __future__ import annotations

import re
import time

from burr.core import State, action

from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.actor_profile.turn import maybe_handle_actor_profile
from app.core.analytics import maybe_handle_analytical_query
from app.core.observer import observe as observe_universe_fn
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder
from app.core.deadline import FALLBACK_FETCH_SECONDS
from app.core.order_semantics import (
    ORDER_EXACT,
    ORDER_SAMPLE,
    ORDER_UNAVAILABLE,
    order_is_presentational,
    sort_page_presentational,
)
from app.core.workflow.helpers import (
    _elapsed_ms,
    _is_timeout_exception,
    _query_first_then_count,
    _relax_params,
    _with_timing,
    _with_timings,
)


# Marca del turno de perfil en el estado y en el feedback. No es un análisis
# de la capa analítica: comparte su canal de transporte, no su naturaleza.
ACTOR_PROFILE_INTENT = "actor_profile"


@action(reads=["resolved_params", "dataset_id", "soql_query", "user_query", "timings_ms"], writes=["results", "query_error", "total_count", "timeout_suggestions", "risk_flag_timeout", "needs_clarification", "clarification_reason", "analytical_response", "analytical_intent", "route_reason", "timings_ms"])
def execute_query(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    """Execute SECOP count + query with graceful error handling.
    On timeout for heavy ordering queries, retry without ordering_signal.
    Guards against WHERE 1=1 — requires at least one scope/topic filter.
    """
    start_total = time.perf_counter()
    params = state.get("resolved_params", {})

    # ── Ruta analítica (aggregate_sum) ─────────────────────────────
    # Si maybe_handle_analytical_query atiende la consulta, NO se ejecuta el
    # camino tabular. La respuesta queda en analytical_response y se propaga
    # intacta por degrade_query/format_response/build_advisor_response.
    analytics = maybe_handle_analytical_query(
        user_query=state.get("user_query", ""),
        params=params,
        dataset_id=state.get("dataset_id", ""),
        secop_client=secop_client,
        soql_builder=soql_builder,
    )
    if analytics:
        return _with_timing(
            state,
            "execute_total_ms",
            start_total,
            results=[],
            query_error="",
            total_count=int(analytics.get("total_count", 0) or 0),
            timeout_suggestions=[],
            risk_flag_timeout=False,
            needs_clarification=bool(analytics.get("needs_clarification", False)),
            clarification_reason=analytics.get("clarification_reason", ""),
            analytical_response=analytics.get("response", ""),
            analytical_intent=analytics.get("analytical_intent", ""),
            route_reason=analytics.get("route_reason", state.get("route_reason", "")),
        )

    # ── Ruta de perfil de actor (AP2) ──────────────────────────────────
    # Después de los analyzers: una pregunta concreta ("cuánto suma lo
    # contratado por el NIT X") merece su respuesta concreta. El perfil
    # atiende la pregunta abierta, que es la que hoy no tiene respuesta
    # correcta. Comparte el canal de respuesta pre-formateada con la capa
    # analítica —el único que format_response y build_advisor_response
    # respetan sin añadirle secciones tabulares—; el `route_reason` mantiene
    # el trace honesto sobre cuál de las dos rutas atendió el turno.
    profile_turn = maybe_handle_actor_profile(
        params=params,
        dataset_id=state.get("dataset_id", ""),
        secop_client=secop_client,
        soql_builder=soql_builder,
    )
    if profile_turn:
        return _with_timing(
            state,
            "execute_total_ms",
            start_total,
            results=[],
            query_error="",
            total_count=int(profile_turn.get("total_count", 0) or 0),
            timeout_suggestions=[],
            risk_flag_timeout=False,
            needs_clarification=False,
            clarification_reason="",
            analytical_response=profile_turn.get("response", ""),
            analytical_intent=ACTOR_PROFILE_INTENT,
            route_reason=profile_turn.get("route_reason", ""),
        )

    # ── Guard anti-WHERE 1=1 ─────────────────────────────────────────────
    # Fuente: app/core/_followup_constants.py (H9: unificada con FollowupGuards)
    has_scope_or_topic = any(
        params.get(k) and params.get(k) not in (None, [], "", {}, False)
        for k in SCOPE_TOPIC_KEYS
    )
    if not has_scope_or_topic:
        return _with_timing(
            state,
            "execute_total_ms",
            start_total,
            results=[], query_error="", total_count=0, timeout_suggestions=[],
            risk_flag_timeout=False,
            needs_clarification=True,
            clarification_reason=(
                "Necesito al menos un filtro de entidad, lugar, tema o contratista "
                "para buscar. Sobre qué quieres saber?"
            ),
        )

    try:
        count_soql = soql_builder.build_count(state["dataset_id"], state["resolved_params"])
        limit = int(state.get("resolved_params", {}).get("limit", 50) or 50)
        total, results, secop_timings, approximate = _query_first_then_count(
            secop_client,
            state["dataset_id"],
            count_soql,
            state["soql_query"],
            limit=limit,
        )
        return _with_timings(
            state,
            {**secop_timings, "execute_total_ms": _elapsed_ms(start_total)},
            results=results, query_error="", total_count=total,
            total_count_approximate=approximate, timeout_suggestions=[],
            risk_flag_timeout=False,
            # H4: la query honró el ORDER BY → orden exacto, sin fallback.
            result_order_guarantee=ORDER_EXACT,
            fallback_attempted=False, fallback_used=False,
            # Commit 6A/6C: observabilidad del pool (distinto del display). `pool_full`
            # solo afirma que se llenó el límite (posible truncamiento), NO que el total
            # lo exceda — para eso habría que comparar contra total_count.
            candidate_count=len(results),
            candidate_pool_full=bool(len(results) >= limit),
        )
    except Exception as exc:
        params = state.get("resolved_params", {})
        # Solo errores RECUPERABLES disparan el fallback (timeout / connErr transitorio).
        # Un 400 de SoQL, esquema, columna inexistente, auth o bug interno NO es recuperable:
        # quitar el ORDER BY ahí solo ocultaría el defecto → camino honesto sin fallback.
        is_timeout = _is_timeout_exception(exc)
        fallback_attempted = False
        fallback_query_status = "not_attempted"

        # ── H4: UN fallback controlado sin ORDER BY ──────────────────────────────
        # SOLO ante timeout Y orden presentacional (default del sistema). Si el usuario
        # pidió un orden explícito ("más caros", "últimos") el orden es SEMÁNTICO: retirarlo
        # cambiaría la pregunta → no se degrada (result_order_guarantee=unavailable). El
        # fallback PRESERVA todos los filtros sustantivos (entidad/territorio/fechas/estado/
        # valor/exclusiones/dataset): solo retira el ORDER BY y recorta LIMIT a ≤50, luego
        # ordena la página localmente (muestra sin garantía de orden global). Máximo UNO.
        if is_timeout and order_is_presentational(params):
            fallback_attempted = True
            fb_start = time.perf_counter()
            try:
                did = state["dataset_id"]
                # El LIMIT efectivo de la principal lo decide el builder (200 con
                # objeto_phrases). Se lee del SoQL ya construido para detectar la reducción.
                m = re.search(r"LIMIT (\d+)", state.get("soql_query", "") or "")
                primary_limit = int(m.group(1)) if m else 50
                fb_params = dict(params)
                fb_params["limit"] = 50  # WIDE_LIMIT: el fallback acota a ≤50
                fb_soql = soql_builder.build(did, fb_params, include_order=False)
                fb_count_soql = soql_builder.build_count(did, fb_params)
                fb_total, fb_results, fb_timings, fb_count_approx = _query_first_then_count(
                    secop_client, did, fb_count_soql, fb_soql,
                    limit=int(fb_params["limit"]),
                    fetch_cap_s=FALLBACK_FETCH_SECONDS,
                )
                if fb_results:
                    spec = soql_builder.SPECS[did]
                    fb_results = sort_page_presentational(
                        fb_results, spec.date, spec.value,
                    )
                    transformations = ["removed_order_by"]
                    if primary_limit > 50:
                        transformations.append("reduced_limit")
                    return _with_timings(
                        state,
                        {**fb_timings, "execute_total_ms": _elapsed_ms(start_total)},
                        results=fb_results, query_error="",
                        # El conteo es un EJE INDEPENDIENTE del orden: si el count terminó,
                        # el total es exacto aunque la muestra no tenga orden garantizado.
                        total_count=fb_total, total_count_approximate=fb_count_approx,
                        timeout_suggestions=[], risk_flag_timeout=False,
                        degraded=True, degraded_hint="recuperado sin orden garantizado",
                        soql_query=fb_soql,
                        # H4 — contrato de orden + telemetría del fallback.
                        result_order_guarantee=ORDER_SAMPLE,
                        fallback_attempted=True,
                        fallback_used=True,
                        fallback_strategy="removed_order_by",
                        fallback_rows=len(fb_results),
                        fallback_elapsed_ms=_elapsed_ms(fb_start),
                        substantive_filters_preserved=True,
                        transformations_applied=transformations,
                        primary_query_status="timed_out",
                        fallback_query_status="completed",
                    )
                fallback_query_status = "empty"
            except Exception as fb_exc:
                fallback_query_status = (
                    "timed_out" if _is_timeout_exception(fb_exc) else "error"
                )

        # ── Sin recuperación: timeout/error honesto ──────────────────────────────
        # Orden semántico no satisfecho, sin presupuesto para el fallback, o el fallback
        # también falló/vacío. result_order_guarantee=unavailable → el formateo NO afirma
        # máximos/mínimos/posiciones sobre una muestra. NO se usa _relax_params (quitar
        # filtros sustantivos cambiaría la pregunta).
        if is_timeout:
            return _with_timing(
                state,
                "execute_total_ms",
                start_total,
                results=[],
                query_error=str(exc),
                total_count=None,
                timeout_suggestions=[],
                risk_flag_timeout=True,
                result_order_guarantee=ORDER_UNAVAILABLE,
                fallback_attempted=fallback_attempted,
                fallback_used=False,
                fallback_query_status=fallback_query_status,
                primary_query_status="timed_out",
            )
        return _with_timing(
            state,
            "execute_total_ms",
            start_total,
            results=[],
            query_error=str(exc),
            total_count=0,
            timeout_suggestions=[],
            risk_flag_timeout=False,
            result_order_guarantee=ORDER_UNAVAILABLE,
            fallback_attempted=False,
            fallback_used=False,
            fallback_query_status="not_attempted",
            primary_query_status="error",
        )


@action(
    reads=["results", "query_error", "resolved_params", "dataset_id", "followup", "analytical_response", "timings_ms"],
    writes=["results", "total_count", "degraded", "degraded_hint", "query_error", "needs_clarification", "clarification_reason", "timings_ms"],
)
def degrade_query(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    """If zero results, try one relaxed query. Marks state degraded=True on success.

    En follow-ups conversacionales, NO relaja automáticamente — sugiere alternativas.
    En modo analítico (analytical_response presente), no degrada nunca.
    """
    start = time.perf_counter()
    # Modo analítico: la respuesta ya está construida, no hay nada que degradar.
    if state.get("analytical_response"):
        return _with_timing(state, "degrade_ms", start, degraded=False, degraded_hint="")
    if state["results"] or state.get("query_error"):
        return _with_timing(state, "degrade_ms", start, degraded=False, degraded_hint="")

    # O1B: a current opportunity search is one normal productive retrieval.
    # Zero rows may be honest; silently removing geography/value would be a
    # semantic Tier 2 and would also create hidden fan-out. The technical H4
    # timeout fallback above remains unchanged because it preserves all filters.
    from app.core.opportunity_retrieval_policy import (
        REFERENCE_INSTANT_PARAM,
        is_current_opportunity_default,
    )
    params = state.get("resolved_params", {})
    if (
        params.get(REFERENCE_INSTANT_PARAM)
        and is_current_opportunity_default(params, state.get("dataset_id", ""))
    ):
        return _with_timing(
            state, "degrade_ms", start, degraded=False, degraded_hint=""
        )

    # En contexto conversacional (follow-up), no relajar filtrar scope/fecha automáticamente
    if state.get("followup"):
        return _with_timing(
            state,
            "degrade_ms",
            start,
            degraded=False, degraded_hint="",
            needs_clarification=True,
            clarification_reason=(
                "No encontre resultados con esos filtros. Puedo intentar:\n"
                "1. quitar el filtro de fecha\n"
                "2. ampliar el tema\n"
                "3. buscar en procesos"
            ),
        )

    relaxed, hint = _relax_params(dict(state["resolved_params"]))
    if relaxed is None:
        return _with_timing(state, "degrade_ms", start, degraded=False, degraded_hint="")

    try:
        soql = soql_builder.build(state["dataset_id"], relaxed)
        count_soql = soql_builder.build_count(state["dataset_id"], relaxed)
        total, results, _timings, _approx = _query_first_then_count(
            secop_client, state["dataset_id"], count_soql, soql
        )
        if results:
            return _with_timing(
                state,
                "degrade_ms",
                start,
                results=results,
                total_count=total,
                degraded=True,
                degraded_hint=hint,
                query_error="",
            )
    except Exception:
        pass

    return _with_timing(state, "degrade_ms", start, degraded=False, degraded_hint="")


@action(
    reads=["results", "total_count", "dataset_id", "resolved_params", "query_error", "timings_ms"],
    writes=["universe_insights", "timings_ms"],
)
def observe_universe(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    start = time.perf_counter()
    # Early exit: no correr observer si la query falló, no tiene resultados, o la muestra es trivial
    if state.get("query_error") or not state.get("results"):
        return _with_timing(state, "observe_ms", start, universe_insights=None)
    if state.get("total_count", 0) < 10:
        return _with_timing(state, "observe_ms", start, universe_insights=None)
    insights = observe_universe_fn(
        results=state["results"],
        total_count=state["total_count"],
        dataset_id=state["dataset_id"],
        params=state["resolved_params"],
        secop_client=secop_client,
        soql_builder=soql_builder,
    )
    return _with_timing(state, "observe_ms", start, universe_insights=insights)
