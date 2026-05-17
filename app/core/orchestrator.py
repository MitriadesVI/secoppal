from __future__ import annotations

import dataclasses
import logging
import time
from concurrent.futures import ThreadPoolExecutor

from burr.core import ApplicationBuilder, State, action, default, expr

from app.config import Settings
from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.conversation_store import ConversationStore, is_followup, is_reset_command, is_pagination_phrase
from app.core.direct_responses import reset_response, pagination_no_history_response, suggestion_invalid_response, suggestion_selection_header
from app.core.entity_resolver import EntityResolver
from app.core.feedback import FeedbackStore
from app.core.formatter import Formatter
from app.core.llm_handler import LLMHandler
from app.core.narrator import NarratorHandler
from app.core.observer import observe as observe_universe_fn, UniverseInsights
from app.core.followup_engine import detect_and_merge, FollowupGuards
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter
from app.core.response_policy import build_advisor_response
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder
from app.core.suggester import generate_suggestions, Suggestion


logger = logging.getLogger(__name__)


def _elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


def _with_timing(state: State, key: str, start: float, **updates) -> State:
    timings = dict(state.get("timings_ms", {}) or {})
    timings[key] = _elapsed_ms(start)
    return state.update(**updates, timings_ms=timings)


def _with_timings(state: State, timings_update: dict[str, float], **updates) -> State:
    timings = dict(state.get("timings_ms", {}) or {})
    timings.update(timings_update)
    return state.update(**updates, timings_ms=timings)


def _count_and_query_parallel(
    secop_client: SecopClient,
    dataset_id: str,
    count_soql: str,
    soql_query: str,
) -> tuple[int, list[dict], dict[str, float]]:
    """Run SECOP count(*) and SELECT concurrently; return total, rows and timings."""
    def _count() -> tuple[int, float]:
        start = time.perf_counter()
        return secop_client.count(dataset_id, count_soql), _elapsed_ms(start)

    def _query() -> tuple[list[dict], float]:
        start = time.perf_counter()
        return secop_client.query(dataset_id, soql_query), _elapsed_ms(start)

    with ThreadPoolExecutor(max_workers=2) as executor:
        count_future = executor.submit(_count)
        query_future = executor.submit(_query)
        total, count_ms = count_future.result()
        results, query_ms = query_future.result()

    return total, results, {"secop_count_ms": count_ms, "secop_query_ms": query_ms}

@action(reads=["user_query", "timings_ms"], writes=["parsed_params", "needs_llm", "route_reason", "timings_ms"])
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
    )


@action(reads=["user_query", "needs_llm", "parsed_params", "timings_ms"], writes=["parsed_params", "timings_ms"])
def llm_parse(state: State, llm_handler: LLMHandler) -> State:
    start = time.perf_counter()
    if not state["needs_llm"]:
        return _with_timing(state, "llm_parse_ms", start)
    parsed = llm_handler.parse(state["user_query"], existing_params=state["parsed_params"])
    return _with_timing(state, "llm_parse_ms", start, parsed_params=parsed)


@action(
    reads=["parsed_params", "timings_ms"],
    writes=["resolved_params", "dataset_id", "needs_clarification", "clarification_reason", "timings_ms"],
)
def resolve_entities(state: State, entity_resolver: EntityResolver) -> State:
    """
    Resolve entities to SECOP-compatible values.
    If gazetteer scan already resolved in parse_query, just pass through.
    Only does fuzzy/LIKE for entities the scan missed (e.g. from LLM).
    """
    start = time.perf_counter()
    params = dict(state["parsed_params"])
    resolved = dict(params)
    resolved.pop("dataset_explicit", None)  # meta-field, not for SECOP
    needs_clarification = False
    clarification_reason = ""

    # Department: only resolve if not already resolved by scan
    if params.get("departamento") and not params.get("departamento_resolved"):
        resolution = entity_resolver.resolve_departamento(str(params["departamento"]))
        resolved["departamento_resolution"] = resolution.to_dict()
        if resolution.value:
            resolved["departamento_resolved"] = resolution.value
        else:
            needs_clarification = True
            clarification_reason = f"No pude ubicar el departamento '{params['departamento']}'."

    # Entity: only resolve if not already resolved by scan
    if params.get("entidad") and not params.get("entidad_resolved"):
        depto_value = resolved.get("departamento_resolved")
        resolution = entity_resolver.resolve_entidad(str(params["entidad"]), departamento=depto_value)
        resolved["entidad_resolution"] = resolution.to_dict()
        if resolution.value:
            resolved["entidad_resolved"] = resolution.value
        elif resolution.like_value:
            resolved["entidad_like"] = resolution.like_value

    dataset_id = SoQLBuilder.dataset_id_for(str(params.get("dataset")))
    return _with_timing(
        state,
        "resolve_ms",
        start,
        resolved_params=resolved,
        dataset_id=dataset_id,
        needs_clarification=needs_clarification,
        clarification_reason=clarification_reason,
    )


@action(reads=["resolved_params", "dataset_id", "timings_ms"], writes=["soql_query", "timings_ms"])
def build_query(state: State, soql_builder: SoQLBuilder) -> State:
    start = time.perf_counter()
    soql = soql_builder.build(state["dataset_id"], state["resolved_params"])
    return _with_timing(state, "build_soql_ms", start, soql_query=soql)


@action(reads=["resolved_params", "dataset_id", "soql_query", "timings_ms"], writes=["results", "query_error", "total_count", "timeout_suggestions", "needs_clarification", "clarification_reason", "timings_ms"])
def execute_query(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    """Execute SECOP count + query with graceful error handling.
    On timeout for heavy ordering queries, retry without ordering_signal.
    Guards against WHERE 1=1 — requires at least one scope/topic filter.
    """
    start_total = time.perf_counter()
    params = state.get("resolved_params", {})

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
            needs_clarification=True,
            clarification_reason=(
                "Necesito al menos un filtro de entidad, lugar, tema o contratista "
                "para buscar. Sobre qué quieres saber?"
            ),
        )

    try:
        count_soql = soql_builder.build_count(state["dataset_id"], state["resolved_params"])
        total, results, secop_timings = _count_and_query_parallel(
            secop_client,
            state["dataset_id"],
            count_soql,
            state["soql_query"],
        )
        return _with_timings(
            state,
            {**secop_timings, "execute_total_ms": _elapsed_ms(start_total)},
            results=results, query_error="", total_count=total, timeout_suggestions=[]
        )
    except Exception as exc:
        params = state.get("resolved_params", {})
        # Retry for heavy ordering queries: quitar ordering_signal
        if params.get("ordering_signal") == "valor_desc" and not params.get("fecha_desde"):
            try:
                relaxed = dict(params)
                relaxed.pop("ordering_signal", None)
                did = state["dataset_id"]
                soql = soql_builder.build(did, relaxed)
                count_soql = soql_builder.build_count(did, relaxed)
                total, results, secop_timings = _count_and_query_parallel(
                    secop_client,
                    did,
                    count_soql,
                    soql,
                )
                if results:
                    timeout_suggestions = _build_timeout_suggestions(params, total)
                    return _with_timings(
                        state,
                        {**secop_timings, "execute_total_ms": _elapsed_ms(start_total)},
                        results=results, query_error="", total_count=total,
                        timeout_suggestions=timeout_suggestions,
                        soql_query=soql,
                        resolved_params=relaxed,
                    )
            except Exception:
                pass
        return _with_timing(state, "execute_total_ms", start_total, results=[], query_error=str(exc), total_count=0, timeout_suggestions=[])


def _relax_params(params: dict) -> tuple[dict | None, str]:
    """Remove the single most restrictive filter and return (relaxed_params, hint).
    Priority (most → least restrictive to drop): estado, fecha, valor, departamento.
    Returns (None, "") if nothing left to relax.
    """
    p = dict(params)

    if "estado" in p:
        del p["estado"]
        return p, "sin filtro de estado"

    if "fecha_desde" in p or "fecha_hasta" in p:
        p.pop("fecha_desde", None)
        p.pop("fecha_hasta", None)
        return p, "sin filtro de fechas"

    if "valor_min" in p or "valor_max" in p:
        p.pop("valor_min", None)
        p.pop("valor_max", None)
        return p, "sin filtro de valor"

    if "departamento_resolved" in p or "departamento" in p:
        p.pop("departamento_resolved", None)
        p.pop("departamento", None)
        p.pop("departamento_resolution", None)
        return p, "en todo el pais"

    return None, ""


def _build_timeout_suggestions(params: dict, total_count: int) -> list[dict]:
    """Generate suggestions for timeout scenarios — filter by year."""
    suggestions = []
    if not params.get("fecha_desde"):
        suggestions.append({
            "label": "Solo 2026",
            "modified_params": {**params, "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31"},
            "reason": "Filtrar solo contratos de 2026 para reducir el universo",
        })
        suggestions.append({
            "label": "Ultimos 3 anos",
            "modified_params": {**params, "fecha_desde": "2023-01-01", "fecha_hasta": "2026-12-31"},
            "reason": "Contratos de los ultimos 3 anos",
        })
    suggestions.append({
        "label": "Sin ordenar por valor",
        "modified_params": {k: v for k, v in params.items() if k != "ordering_signal"},
        "reason": "Quitar el orden por valor para que la busqueda sea mas rapida",
    })
    return suggestions[:3]


@action(
    reads=["results", "query_error", "resolved_params", "dataset_id", "followup", "timings_ms"],
    writes=["results", "total_count", "degraded", "degraded_hint", "query_error", "needs_clarification", "clarification_reason", "timings_ms"],
)
def degrade_query(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    """If zero results, try one relaxed query. Marks state degraded=True on success.

    En follow-ups conversacionales, NO relaja automáticamente — sugiere alternativas.
    """
    start = time.perf_counter()
    if state["results"] or state.get("query_error"):
        return _with_timing(state, "degrade_ms", start, degraded=False, degraded_hint="")

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
        total, results, _timings = _count_and_query_parallel(
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


@action(
    reads=["resolved_params", "universe_insights", "total_count", "dataset_id", "results", "timings_ms"],
    writes=["suggestions", "timings_ms"],
)
def suggester_action(state: State) -> State:
    start = time.perf_counter()
    suggestions = generate_suggestions(
        params=state.get("resolved_params", {}),
        universe_insights=state.get("universe_insights"),
        total_count=state.get("total_count", 0),
        rows=state.get("results", []),
        dataset_id=state.get("dataset_id", ""),
    )
    return _with_timing(state, "suggest_ms", start, suggestions=suggestions)


@action(reads=["results", "dataset_id", "channel", "query_error", "resolved_params", "degraded", "degraded_hint", "total_count", "universe_insights", "suggestions", "timeout_suggestions", "timings_ms"], writes=["formatted_response", "formatted_rows", "timings_ms"])
def format_response(state: State, formatter: Formatter) -> State:
    start = time.perf_counter()
    if state.get("query_error"):
        return _with_timing(
            state,
            "format_ms",
            start,
            formatted_response="SECOP no respondio a tiempo. Intenta de nuevo en unos segundos.",
            formatted_rows=[],
        )
    params = state.get("resolved_params") or {}
    total_count = state.get("total_count", 0)
    universe_insights = state.get("universe_insights")
    suggestions = state.get("suggestions", [])
    response, rows = formatter.format_for_channel(
        state["results"], state["dataset_id"], state["channel"],
        params=params, total_count=total_count, universe_insights=universe_insights,
        suggestions=suggestions,
    )
    if state.get("degraded") and state.get("degraded_hint"):
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


@action(reads=["parsed_params", "context_params", "followup", "user_query", "timings_ms"], writes=["parsed_params", "needs_llm", "needs_clarification", "clarification_reason", "intent_type", "followup_intent_type", "timings_ms"])
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
    intent, merged = detect_and_merge(
        text=state.get("user_query", ""),
        current_params=state.get("parsed_params", {}),
        previous_frame=previous_frame,
    )

    # Guard contra WHERE 1=1 global
    if not FollowupGuards.check_no_where_1_1(merged):
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
        )

    # Si el merge produjo contexto suficiente, no necesita LLM
    has_context = FollowupGuards.check_no_where_1_1(merged)
    if has_context:
        return _with_timing(state, "apply_context_ms", start, parsed_params=merged, needs_llm=False, intent_type=intent, followup_intent_type=intent)

    return _with_timing(state, "apply_context_ms", start, parsed_params=merged, intent_type=intent, followup_intent_type=intent)


@action(reads=["clarification_reason", "timings_ms"], writes=["formatted_response", "formatted_rows", "timings_ms"])
def clarify_query(state: State) -> State:
    start = time.perf_counter()
    return _with_timing(
        state,
        "clarify_ms",
        start,
        formatted_response=state["clarification_reason"] or "Necesito un poco mas de contexto para buscar en SECOP.",
        formatted_rows=[],
    )


class SecopalWorkflow:
    """High-level workflow facade around Burr."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.entity_resolver = EntityResolver(settings.secop_alias_db_path)
        self.query_router = QueryRouter(entity_resolver=self.entity_resolver)
        self.llm_handler = LLMHandler(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        self.soql_builder = SoQLBuilder()
        self.secop_client = SecopClient(
            domain=settings.datos_gov_domain,
            app_token=settings.secop_app_token,
            timeout=settings.secop_timeout_seconds,
        )
        self.formatter = Formatter(max_results=settings.secop_results_limit)
        self.feedback = FeedbackStore(settings.secop_feedback_path)
        self.conv_store = ConversationStore()
        self.narrator = NarratorHandler(
            api_key=settings.deepseek_api_key,
            model=settings.deepseek_model,
        )

    def run_query(self, user_query: str, channel: str = "whatsapp", chat_id: str | None = None) -> dict:
        run_start = time.perf_counter()
        # ── Reset command ────────────────────────────────────────────────
        if chat_id and is_reset_command(user_query):
            self.conv_store.clear(chat_id)
            return {
                "response": reset_response(),
                "rows": [],
                "results": [],
                "dataset_id": "",
                "soql_query": "",
                "parsed_params": {},
                "resolved_params": {},
                "needs_llm": False,
                "needs_clarification": False,
                "route_reason": "reset",
                "degraded": False,
                "degraded_hint": "",
                "total_count": 0,
                "universe_insights": None,
                "suggestions": [],
                "timeout_suggestions": [],
                "intent_type": "",
                "followup": False,
                "trace_id": "",
                "narrated": False,
            }

        # ── Pagination detection ──────────────────────────────────────────
        if chat_id and is_pagination_phrase(user_query):
            history = self.conv_store.get_history(chat_id, last_n=1)
            if history:
                prev_params = dict(history[-1].parsed_params)
                prev_offset = int(prev_params.get("offset", 0))
                prev_limit = int(prev_params.get("limit", 10))
                new_offset = prev_offset + prev_limit
                pag_params = {**prev_params, "offset": new_offset, "limit": prev_limit}
                ds_id = SoQLBuilder.dataset_id_for(pag_params.get("dataset"))
                soql = self.soql_builder.build(ds_id, pag_params)
                try:
                    total, results, _secop_timings = _count_and_query_parallel(
                        self.secop_client,
                        ds_id,
                        self.soql_builder.build_count(ds_id, pag_params),
                        soql,
                    )
                    response, rows = self.formatter.format_for_channel(
                        results, ds_id, channel,
                        params=pag_params, total_count=total,
                    )
                    display_from = new_offset + 1
                    display_to = new_offset + len(results)
                    ordering_label = "de mayor valor" if pag_params.get("ordering_signal") == "valor_desc" else "más recientes"
                    header = f"📄 Resultados {display_from}-{display_to} de {total} (manteniendo orden por {ordering_label})\n\n"
                    result = {
                        "response": header + response,
                        "rows": rows,
                        "results": results,
                        "dataset_id": ds_id,
                        "soql_query": soql,
                        "parsed_params": pag_params,
                        "resolved_params": pag_params,
                        "needs_llm": False,
                        "needs_clarification": False,
                        "route_reason": "pagination_more",
                        "degraded": False, "degraded_hint": "",
                        "total_count": total,
                        "universe_insights": None,
                        "suggestions": [], "timeout_suggestions": [],
                        "intent_type": "pagination_more",
                        "followup": True,
                        "trace_id": "", "narrated": False,
                    }
                    # Persist turn for next pagination
                    result_ids = [
                        rid for rid in (
                            str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
                            for r in results[:10]
                        ) if rid
                    ]
                    trace_id = self.feedback.log_trace(user_query, channel, result)
                    result["trace_id"] = trace_id
                    self.conv_store.append_turn(
                        chat_id=chat_id,
                        user_query=user_query,
                        response=result["response"],
                        parsed_params=pag_params,
                        result_ids=result_ids,
                        trace_id=trace_id,
                    )
                    return result
                except Exception:
                    pass
            # Sin historial – pedir contexto (NO caer a flujo normal WHERE 1=1)
            return {
                "response": pagination_no_history_response(),
                "rows": [], "results": [], "dataset_id": "", "soql_query": "",
                "parsed_params": {}, "resolved_params": {},
                "needs_llm": False, "needs_clarification": True,
                "route_reason": "pagination_no_context",
                "degraded": False, "degraded_hint": "", "total_count": 0,
                "universe_insights": None, "suggestions": [],
                "timeout_suggestions": [], "intent_type": "pagination_more",
                "followup": False, "trace_id": "", "narrated": False,
            }

        # ── Suggestion selection ─────────────────────────────────────────
        suggestion_applied = False
        suggestion_label = ""
        if chat_id:
            q = user_query.strip()
            if q in ("1", "2", "3"):
                last_suggestions = self.conv_store.get_last_suggestions(chat_id)
                if last_suggestions and len(last_suggestions) >= int(q):
                    idx = int(q) - 1
                    sug = last_suggestions[idx]
                    sug_params = sug.get("modified_params", {})
                    suggestion_label = sug.get("label", "")
                    sug_dataset_id = SoQLBuilder.dataset_id_for(sug_params.get("dataset"))
                    # Build SoQL and execute directly
                    soql = self.soql_builder.build(sug_dataset_id, sug_params)
                    try:
                        count_soql = self.soql_builder.build_count(sug_dataset_id, sug_params)
                        total, results, _secop_timings = _count_and_query_parallel(
                            self.secop_client,
                            sug_dataset_id,
                            count_soql,
                            soql,
                        )
                        # Generate insights + suggestions for this new result
                        ui = observe_universe_fn(
                            results=results, total_count=total,
                            dataset_id=sug_dataset_id, params=sug_params,
                            secop_client=self.secop_client,
                            soql_builder=self.soql_builder,
                        )
                        new_suggestions = generate_suggestions(
                            params=sug_params, universe_insights=ui,
                            total_count=total, rows=results, dataset_id=sug_dataset_id,
                        )
                        response, rows = self.formatter.format_for_channel(
                            results, sug_dataset_id, channel,
                            params=sug_params, total_count=total,
                            universe_insights=ui,
                            suggestions=[dataclasses.asdict(s) if hasattr(s, '__dataclass_fields__') else s for s in new_suggestions],
                        )
                        suggestion_applied = True
                        state_for_result = {
                            "formatted_response": response,
                            "formatted_rows": rows,
                            "results": results,
                            "dataset_id": sug_dataset_id,
                            "soql_query": soql,
                            "parsed_params": sug_params,
                            "resolved_params": sug_params,
                            "needs_llm": False,
                            "needs_clarification": False,
                            "route_reason": "suggestion",
                            "degraded": False,
                            "degraded_hint": "",
                            "total_count": total,
                            "universe_insights": ui,
                            "suggestions": new_suggestions,
                        }
                    except Exception as exc:
                        # Fallback: sugerencia no disponible
                        return {
                            "response": f"No pude ejecutar esa sugerencia en este momento: {exc}",
                            "rows": [],
                            "results": [],
                            "dataset_id": "",
                            "soql_query": "",
                            "parsed_params": {},
                            "resolved_params": {},
                            "needs_llm": False,
                            "needs_clarification": False,
                            "route_reason": "suggestion_error",
                            "degraded": False,
                            "degraded_hint": "",
                            "total_count": 0,
                            "universe_insights": None,
                            "suggestions": [],
                            "timeout_suggestions": [],
                            "followup": False,
                            "trace_id": "",
                            "narrated": False,
                        }
                else:
                    # Hay sugerencia pero el numero no coincide
                    return {
                        "response": suggestion_invalid_response(3, q),
                        "rows": [],
                        "results": [],
                        "dataset_id": "",
                        "soql_query": "",
                        "parsed_params": {},
                        "resolved_params": {},
                        "needs_llm": False,
                        "needs_clarification": False,
                        "route_reason": "suggestion_invalid",
                        "degraded": False,
                        "degraded_hint": "",
                        "total_count": 0,
                        "universe_insights": None,
                        "suggestions": [],
                        "timeout_suggestions": [],
                        "followup": False,
                        "trace_id": "",
                        "narrated": False,
                    }

        if suggestion_applied:
            # Construct result from direct execution
            result = state_for_result
            # Add fields that run_query normally computes
            raw_results = result.get("results", [])
            result_ids = [
                rid for rid in (
                    str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
                    for r in raw_results[:10]
                ) if rid
            ]
            trace_id = self.feedback.log_trace(user_query, channel, result)
            result["trace_id"] = trace_id

            # Suggestion response
            result["response"] = suggestion_selection_header(suggestion_label) + result.get("response", "")

            # Persist turn
            if chat_id:
                self.conv_store.append_turn(
                    chat_id=chat_id,
                    user_query=user_query,
                    response=result["response"],
                    parsed_params=result.get("resolved_params", {}),
                    result_ids=result_ids,
                    trace_id=trace_id,
                    suggestions=[dataclasses.asdict(s) if hasattr(s, '__dataclass_fields__') else s for s in result.get("suggestions", [])],
                )
            return result

        # ── Normal flow ──────────────────────────────────────────────────
        # Follow-up detection
        history = self.conv_store.get_history(chat_id, last_n=5) if chat_id else []
        last_turn = history[-1] if history else None
        followup = is_followup(user_query, last_turn)
        context_params = dict(last_turn.parsed_params) if (followup and last_turn) else {}

        app = (
            ApplicationBuilder()
            .with_actions(
                parse_query=parse_query.bind(query_router=self.query_router),
                apply_context=apply_context,
                llm_parse=llm_parse.bind(llm_handler=self.llm_handler),
                resolve_entities=resolve_entities.bind(entity_resolver=self.entity_resolver),
                build_query=build_query.bind(soql_builder=self.soql_builder),
                execute_query=execute_query.bind(secop_client=self.secop_client, soql_builder=self.soql_builder),
                degrade_query=degrade_query.bind(secop_client=self.secop_client, soql_builder=self.soql_builder),
                observe_universe=observe_universe.bind(secop_client=self.secop_client, soql_builder=self.soql_builder),
                suggester_action=suggester_action,
                format_response=format_response.bind(formatter=self.formatter),
                clarify_query=clarify_query,
            )
            .with_transitions(
                ("parse_query", "apply_context", expr("followup")),
                ("parse_query", "llm_parse", expr("not followup and needs_llm")),
                ("parse_query", "resolve_entities", expr("not followup and not needs_llm")),
                ("apply_context", "clarify_query", expr("needs_clarification")),
                ("apply_context", "llm_parse", expr("not needs_clarification and needs_llm")),
                ("apply_context", "resolve_entities", expr("not needs_clarification and not needs_llm")),
                ("llm_parse", "resolve_entities", default),
                ("resolve_entities", "clarify_query", expr("needs_clarification")),
                ("resolve_entities", "build_query", expr("not needs_clarification")),
                ("build_query", "execute_query", default),
                ("execute_query", "clarify_query", expr("needs_clarification")),
                ("execute_query", "degrade_query", default),
                ("degrade_query", "clarify_query", expr("needs_clarification")),
                ("degrade_query", "observe_universe", expr("not needs_clarification")),
                ("observe_universe", "suggester_action", default),
                ("suggester_action", "format_response", default),
            )
            .with_state(
                user_query=user_query,
                channel=channel,
                parsed_params={},
                resolved_params={},
                results=[],
                formatted_rows=[],
                formatted_response="",
                soql_query="",
                dataset_id="",
                route_reason="",
                query_error="",
                needs_llm=False,
                needs_clarification=False,
                clarification_reason="",
                total_count=0,
                degraded=False,
                degraded_hint="",
                followup=followup,
                context_params=context_params,
                universe_insights=None,
                suggestions=[],
                timeout_suggestions=[],
                intent_type="",
                followup_intent_type="",
                timings_ms={},
            )
            .with_entrypoint("parse_query")
            .build()
        )

        *_, state = app.run(halt_after=["format_response", "clarify_query"])

        # Extraer IDs de resultados (prerrequisito de callbacks 2.9)
        raw_results = state.get("results", [])
        result_ids = [
            rid for rid in (
                str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
                for r in raw_results[:10]
            ) if rid
        ]

        result = {
            "response": state["formatted_response"],
            "rows": state["formatted_rows"],
            "results": state["results"],
            "dataset_id": state["dataset_id"],
            "soql_query": state["soql_query"],
            "parsed_params": state["parsed_params"],
            "resolved_params": state["resolved_params"],
            "needs_llm": state["needs_llm"],
            "needs_clarification": state["needs_clarification"],
            "route_reason": state["route_reason"],
            "degraded": state.get("degraded", False),
            "degraded_hint": state.get("degraded_hint", ""),
            "total_count": state.get("total_count", 0),
            "universe_insights": state.get("universe_insights"),
            "suggestions": state.get("suggestions", []),
            "timeout_suggestions": state.get("timeout_suggestions", []),
            "intent_type": state.get("intent_type", ""),
            "followup_intent_type": state.get("followup_intent_type", ""),
            "followup": followup,
            "timings_ms": dict(state.get("timings_ms", {}) or {}),
        }

        # ── Política de respuesta asesora (Bloque 3) ───────────────────────
        advisor_start = time.perf_counter()
        advisor_response = build_advisor_response({
            "user_query": user_query,
            "resolved_params": state.get("resolved_params", {}),
            "total_count": state.get("total_count", 0),
            "rows": state.get("formatted_rows", []),
            "universe_insights": state.get("universe_insights"),
            "suggestions": state.get("suggestions", []),
            "dataset_id": state.get("dataset_id", ""),
            "channel": channel,
            "query_error": state.get("query_error", ""),
            "timeout_suggestions": state.get("timeout_suggestions", []),
            "needs_clarification": state.get("needs_clarification", False),
            "clarification_reason": state.get("clarification_reason", ""),
            "degraded": state.get("degraded", False),
            "degraded_hint": state.get("degraded_hint", ""),
        }, narrator=self.narrator)
        result["response"] = advisor_response
        result["timings_ms"]["advisor_response_ms"] = _elapsed_ms(advisor_start)
        result["timings_ms"]["total_ms"] = _elapsed_ms(run_start)

        trace_id = self.feedback.log_trace(user_query, channel, result)
        result["trace_id"] = trace_id
        logger.info(
            "secoppal_query trace_id=%s route=%s dataset=%s total_count=%s results_count=%s total_ms=%s",
            trace_id,
            result.get("route_reason", ""),
            result.get("dataset_id", ""),
            result.get("total_count", 0),
            len(result.get("results", [])),
            result["timings_ms"].get("total_ms"),
        )

        # Persistir turno conversacional
        if chat_id:
            self.conv_store.append_turn(
                chat_id=chat_id,
                user_query=user_query,
                response=result["response"],
                parsed_params=state.get("resolved_params", {}),
                result_ids=result_ids,
                trace_id=trace_id,
                suggestions=[dataclasses.asdict(s) if hasattr(s, '__dataclass_fields__') else s for s in result.get("suggestions", [])],
            )

        return result

    def rate_query(self, trace_id: str, rating: int, comment: str | None = None) -> bool:
        return self.feedback.rate(trace_id, rating, comment)

    def get_feedback_stats(self) -> dict:
        return self.feedback.get_stats()
