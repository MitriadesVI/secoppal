from __future__ import annotations

import dataclasses

from burr.core import ApplicationBuilder, State, action, default, expr

from app.config import Settings
from app.core.conversation_store import ConversationStore, is_followup, is_reset_command, merge_params
from app.core.entity_resolver import EntityResolver
from app.core.feedback import FeedbackStore
from app.core.formatter import Formatter
from app.core.llm_handler import LLMHandler
from app.core.narrator import NarratorHandler
from app.core.observer import observe as observe_universe_fn, UniverseInsights
from app.core.query_router import QueryRouter
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder
from app.core.suggester import generate_suggestions, Suggestion


@action(reads=["user_query"], writes=["parsed_params", "needs_llm", "route_reason"])
def parse_query(state: State, query_router: QueryRouter) -> State:
    result = query_router.parse(state["user_query"])
    return state.update(
        parsed_params=result.params,
        needs_llm=result.needs_llm,
        route_reason=result.route_reason,
    )


@action(reads=["user_query", "needs_llm", "parsed_params"], writes=["parsed_params"])
def llm_parse(state: State, llm_handler: LLMHandler) -> State:
    if not state["needs_llm"]:
        return state
    parsed = llm_handler.parse(state["user_query"], existing_params=state["parsed_params"])
    return state.update(parsed_params=parsed)


@action(
    reads=["parsed_params"],
    writes=["resolved_params", "dataset_id", "needs_clarification", "clarification_reason"],
)
def resolve_entities(state: State, entity_resolver: EntityResolver) -> State:
    """
    Resolve entities to SECOP-compatible values.
    If gazetteer scan already resolved in parse_query, just pass through.
    Only does fuzzy/LIKE for entities the scan missed (e.g. from LLM).
    """
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
    return state.update(
        resolved_params=resolved,
        dataset_id=dataset_id,
        needs_clarification=needs_clarification,
        clarification_reason=clarification_reason,
    )


@action(reads=["resolved_params", "dataset_id"], writes=["soql_query"])
def build_query(state: State, soql_builder: SoQLBuilder) -> State:
    soql = soql_builder.build(state["dataset_id"], state["resolved_params"])
    return state.update(soql_query=soql)


@action(reads=["resolved_params", "dataset_id", "soql_query"], writes=["results", "query_error", "total_count", "timeout_suggestions"])
def execute_query(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    """Execute SECOP count + query with graceful error handling.
    On timeout for heavy ordering queries, retry without ordering_signal."""
    try:
        count_soql = soql_builder.build_count(state["dataset_id"], state["resolved_params"])
        total = secop_client.count(state["dataset_id"], count_soql)
        results = secop_client.query(state["dataset_id"], state["soql_query"])
        return state.update(results=results, query_error="", total_count=total, timeout_suggestions=[])
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
                total = secop_client.count(did, count_soql)
                results = secop_client.query(did, soql)
                if results:
                    timeout_suggestions = _build_timeout_suggestions(params, total)
                    return state.update(
                        results=results, query_error="", total_count=total,
                        timeout_suggestions=timeout_suggestions,
                        soql_query=soql,
                        resolved_params=relaxed,
                    )
            except Exception:
                pass
        return state.update(results=[], query_error=str(exc), total_count=0, timeout_suggestions=[])


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
    reads=["results", "query_error", "resolved_params", "dataset_id"],
    writes=["results", "degraded", "degraded_hint", "query_error"],
)
def degrade_query(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    """If zero results, try one relaxed query. Marks state degraded=True on success."""
    if state["results"] or state.get("query_error"):
        return state.update(degraded=False, degraded_hint="")

    relaxed, hint = _relax_params(dict(state["resolved_params"]))
    if relaxed is None:
        return state.update(degraded=False, degraded_hint="")

    try:
        soql = soql_builder.build(state["dataset_id"], relaxed)
        results = secop_client.query(state["dataset_id"], soql)
        if results:
            return state.update(results=results, degraded=True, degraded_hint=hint, query_error="")
    except Exception:
        pass

    return state.update(degraded=False, degraded_hint="")


@action(
    reads=["results", "total_count", "dataset_id", "resolved_params", "query_error"],
    writes=["universe_insights"],
)
def observe_universe(state: State, secop_client: SecopClient, soql_builder: SoQLBuilder) -> State:
    # Early exit: no correr observer si la query falló, no tiene resultados, o la muestra es trivial
    if state.get("query_error") or not state.get("results"):
        return state.update(universe_insights=None)
    if state.get("total_count", 0) < 10:
        return state.update(universe_insights=None)
    insights = observe_universe_fn(
        results=state["results"],
        total_count=state["total_count"],
        dataset_id=state["dataset_id"],
        params=state["resolved_params"],
        secop_client=secop_client,
        soql_builder=soql_builder,
    )
    return state.update(universe_insights=insights)


@action(
    reads=["resolved_params", "universe_insights", "total_count", "dataset_id", "results"],
    writes=["suggestions"],
)
def suggester_action(state: State) -> State:
    suggestions = generate_suggestions(
        params=state.get("resolved_params", {}),
        universe_insights=state.get("universe_insights"),
        total_count=state.get("total_count", 0),
        rows=state.get("results", []),
        dataset_id=state.get("dataset_id", ""),
    )
    return state.update(suggestions=suggestions)


@action(reads=["results", "dataset_id", "channel", "query_error", "resolved_params", "degraded", "degraded_hint", "total_count", "universe_insights", "suggestions", "timeout_suggestions"], writes=["formatted_response", "formatted_rows"])
def format_response(state: State, formatter: Formatter) -> State:
    if state.get("query_error"):
        return state.update(
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
    return state.update(formatted_response=response, formatted_rows=rows)


@action(reads=["parsed_params", "context_params", "followup"], writes=["parsed_params", "needs_clarification", "clarification_reason"])
def apply_context(state: State) -> State:
    """Fusiona context_params del turno anterior con parsed_params del parse actual.
    Solo corre cuando followup=True. Usa MERGE_RULES de ConversationStore.

    Si el merge produce solo ordering_signal sin contexto real (objeto, entidad,
    departamento), marca clarification para evitar búsqueda global WHERE 1=1.
    """
    if not state.get("followup"):
        return state
    merged = merge_params(state["context_params"], state["parsed_params"])
    # Guard: si solo queda ordering_signal, el contexto se perdió
    has_context = any(
        k not in ("ordering_signal", "dataset", "dataset_explicit")
        and v not in (None, [], "", {}, False)
        for k, v in merged.items()
    )
    if not has_context and merged.get("ordering_signal"):
        return state.update(
            parsed_params=merged,
            needs_clarification=True,
            clarification_reason=(
                "Te entendi como refinamiento, pero perdi el contexto anterior. "
                "Haz una busqueda base primero o usa reset."
            ),
        )
    return state.update(parsed_params=merged)


@action(reads=["clarification_reason"], writes=["formatted_response", "formatted_rows"])
def clarify_query(state: State) -> State:
    return state.update(
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
        self.formatter = Formatter(max_results=min(settings.secop_results_limit, 10))
        self.feedback = FeedbackStore()
        self.conv_store = ConversationStore()
        self.narrator = NarratorHandler(
            api_key=settings.deepseek_api_key,
            model=settings.deepseek_model,
        )

    def run_query(self, user_query: str, channel: str = "whatsapp", chat_id: str | None = None) -> dict:
        # ── Reset command ────────────────────────────────────────────────
        if chat_id and is_reset_command(user_query):
            self.conv_store.clear(chat_id)
            return {
                "response": "🧹 Historial de la conversacion reiniciado. Pregunta lo que necesites.",
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
                "followup": False,
                "trace_id": "",
                "narrated": False,
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
                        total = self.secop_client.count(sug_dataset_id, count_soql)
                        results = self.secop_client.query(sug_dataset_id, soql)
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
                        "response": "No tengo una sugerencia activa para ese numero. Usa 1, 2 o 3 para seleccionar.",
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
                str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
                for r in raw_results[:10]
            ]
            trace_id = self.feedback.log_trace(user_query, channel, result)
            result["trace_id"] = trace_id

            # Narration
            if raw_results:
                narrative = self.narrator.narrate_with_grounding(
                    rows=raw_results,
                    query=f"Sugerencia: {suggestion_label}",
                    channel=channel,
                    total_count=result.get("total_count", 0),
                    universe_insights=result.get("universe_insights"),
                    suggestions=[dataclasses.asdict(s) if hasattr(s, '__dataclass_fields__') else s for s in result.get("suggestions", [])],
                )
                if narrative:
                    result["response"] = narrative
                    result["narrated"] = True
                else:
                    result["narrated"] = False
            else:
                result["narrated"] = False

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
                ("apply_context", "llm_parse", expr("needs_llm")),
                ("apply_context", "resolve_entities", expr("not needs_llm")),
                ("llm_parse", "resolve_entities", default),
                ("resolve_entities", "clarify_query", expr("needs_clarification")),
                ("resolve_entities", "build_query", expr("not needs_clarification")),
                ("build_query", "execute_query", default),
                ("execute_query", "degrade_query", default),
                ("degrade_query", "observe_universe", default),
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
            )
            .with_entrypoint("parse_query")
            .build()
        )

        *_, state = app.run(halt_after=["format_response", "clarify_query"])

        # Extraer IDs de resultados (prerrequisito de callbacks 2.9)
        raw_results = state.get("results", [])
        result_ids = [
            str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
            for r in raw_results[:10]
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
            "followup": followup,
        }

        trace_id = self.feedback.log_trace(user_query, channel, result)
        result["trace_id"] = trace_id

        # Narración conversacional (2.4) — sustituye formatted_response si LLM disponible
        raw_results = state.get("results", [])
        if raw_results and not state.get("needs_clarification"):
            history_for_narrator = [
                {"role": "user" if i % 2 == 0 else "assistant", "content": t.user_query if i % 2 == 0 else t.response}
                for i, t in enumerate(history[-3:])
            ] if history else []
            narrative = self.narrator.narrate_with_grounding(
                rows=raw_results,
                query=user_query,
                channel=channel,
                history=history_for_narrator,
                total_count=state.get("total_count", 0),
                universe_insights=state.get("universe_insights"),
                suggestions=state.get("suggestions", []),
            )
            if narrative:
                result["response"] = narrative
                result["narrated"] = True
            else:
                result["narrated"] = False
        else:
            result["narrated"] = False

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
