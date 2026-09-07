"""Normal-flow handler — the default pipeline through the Burr graph.

Catches every request that didn't match an earlier handler in the chain
(refinement, reset, pagination, suggestion-selection, direct_lookup).
This is the broadest path and the one that does the full work:

1. **Follow-up detection** — pull last_n=5 history, ask
   ``is_followup()`` whether the current query is a continuation.
2. **Burr application** — ``build_application`` then ``app.run`` until
   format_response or clarify_query halts.
3. **Result canonicalization** — collect the final Burr state into a
   contract-shaped result dict.
4. **Contextual filters** — best-effort post-process for the refinement
   bar (5+ results threshold).
5. **Entity-ecosystem expansion** — Sprint 6 Fase 3.
6. **Relevance scoring** — BM25 re-rank when objeto has ≥3 tokens.
7. **Advisor response** — wrap state in the human-facing response unless
   query_plan blocked execution.
8. **Persistence** — log trace, append conversation turn (with the
   suggestion list so the next turn can select).

Moved here in Paso 7E from the inline ``# ── Normal flow`` block in
``SecopalWorkflow.run_query``. Behavior preserving: the body is the
exact same sequence of steps, with ``self.X`` becoming ``deps.X`` and
the per-request args coming from ``ctx``.

Why not adopt ``persist_trace_and_turn`` here?
----------------------------------------------
The normal-flow turn append passes ``suggestions=`` to the conversation
store so the next turn's suggestion handler can read them back.
``persist_trace_and_turn`` doesn't take that argument (yet). Extending
the persistence helper is out of scope for Paso 7E — preserved
verbatim. TODO follow-up: widen persist_trace_and_turn and migrate.
"""
from __future__ import annotations

import dataclasses
import logging
import time
from typing import TYPE_CHECKING

from app.core.conversation_store import is_followup
from app.core.modality_followup_telemetry import append_event, project_event
from app.core.response_policy import build_advisor_response
from app.core.workflow.graph import build_application
from app.core.workflow.helpers import _elapsed_ms
from app.core.workflow.result_ownership import owns_execution_and_presentation

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


logger = logging.getLogger(__name__)

# Fase 2B: registra el clasificador de etapa de oportunidad en el trace como
# OBSERVACIÓN (shadow). No cambia orden, filtro, respuesta ni Radar.
OPPORTUNITY_STAGE_SHADOW_ENABLED = True


def _initial_pagination_buffer(
    *, group: str, dataset_id: str, params: dict, candidates: list[dict],
    validated: list[dict], displayed: int, source_total: int,
    source_total_approximate: bool = False,
):
    from app.core.pagination_buffer import PaginationBuffer, pagination_fingerprint

    clean = dict(params)
    clean.pop("entidad_ecosystem", None)  # se inyecta post-query; no fue parte del fetch
    limit = int(clean.get("limit", 50) or 50)
    offset = int(clean.get("offset", 0) or 0)
    return PaginationBuffer.create(
        query_fingerprint=pagination_fingerprint(clean),
        group=group,
        dataset_id=dataset_id,
        query_params=clean,
        candidate_buffer=candidates,
        validated_ranked_buffer=validated,
        display_cursor=displayed,
        source_cursor=offset + limit,
        source_candidate_total=max(int(source_total or 0), len(candidates)),
        source_total_approximate=source_total_approximate,
    )


def handle_normal_flow(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict:
    """Run the full Burr pipeline and post-process the resulting state."""
    # Etapa 5: la recolección se inicia/cierra en run_query (por-turno); aquí solo se
    # lee lo capturado.
    # Follow-up detection
    history = deps.conv_store.get_history(ctx.chat_id, last_n=5) if ctx.chat_id else []
    last_turn = history[-1] if history else None
    followup = is_followup(ctx.user_query, last_turn)
    context_params = dict(last_turn.parsed_params) if (followup and last_turn) else {}
    app = build_application(
        deps,
        user_query=ctx.user_query,
        channel=ctx.channel,
        followup=followup,
        context_params=context_params,
        today=ctx.today,
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
        "risk_flag_timeout": state.get("risk_flag_timeout", False),
        "query_error": state.get("query_error", ""),
        "intent_type": state.get("intent_type", ""),
        "followup_intent_type": state.get("followup_intent_type", ""),
        "followup_mutation": state.get("followup_mutation", {}),
        "analytical_intent": state.get("analytical_intent", ""),
        "analytical_response": state.get("analytical_response", ""),
        "clarification_reason": state.get("clarification_reason", ""),
        "total_count_approximate": state.get("total_count_approximate", False),
        # H4 — contrato de orden + telemetría del fallback degradado.
        "result_order_guarantee": state.get("result_order_guarantee", "exact_order"),
        "fallback_attempted": state.get("fallback_attempted", False),
        "fallback_used": state.get("fallback_used", False),
        "fallback_strategy": state.get("fallback_strategy", ""),
        "fallback_query_status": state.get("fallback_query_status", "not_attempted"),
        "primary_query_status": state.get("primary_query_status", ""),
        "transformations_applied": state.get("transformations_applied", []),
        "substantive_filters_preserved": state.get("substantive_filters_preserved", True),
        "query_plan": state.get("query_plan", {}),
        "followup": followup,
        "timings_ms": dict(state.get("timings_ms", {}) or {}),
        "contextual_filters": {},
    }

    # A semantic turn normally invalidates the old result universe. An
    # unsupported/contradictory negation is transactional, however: it changes
    # neither the confirmed frame nor its pagination snapshot.
    negation_block = (result.get("query_plan") or {}).get("reason") in {
        "unsupported_negation",
        "dimensional_negation_contradiction",
    }
    if ctx.chat_id and not negation_block:
        deps.conv_store.invalidate_pagination_buffers(ctx.chat_id)

    # ── Contextual filters (Feature: refinement bar) ──────────────────
    raw_results = result.get("results", [])
    total_count = result.get("total_count", 0)
    threshold = 5
    if raw_results and total_count >= threshold:
        try:
            from app.core.contextual_suggestions import compute_contextual_filters
            computed = compute_contextual_filters(
                raw_results,
                result["dataset_id"],
                result.get("resolved_params", {}),
            )
            result["contextual_filters"] = computed
        except Exception:
            pass  # best-effort, never block the main flow

    # ── Política de respuesta asesora (Bloque 3) ───────────────────────
    advisor_start = time.perf_counter()

    # ADR-074: response context and trace retain the executed buyer scope.
    # Geography cannot inject an institutional set after fetch/count/ranking.

    # NOTA (Commit 6D): el BM25 post-ranking vivía AQUÍ pero corría DESPUÉS de que
    # format_response ya había congelado formatted_rows (bug RELEVANCE-RANKING-AFTER-
    # FORMAT-001) → nunca afectaba el display. El ranking real ahora es la acción
    # `rank_results`, ANTES de format_response. Este bloque se eliminó a propósito.

    advisor_response = build_advisor_response({
        "user_query": ctx.user_query,
        "resolved_params": state.get("resolved_params", {}),
        "total_count": state.get("total_count", 0),
        "total_count_approximate": state.get("total_count_approximate", False),
        "rows": state.get("formatted_rows", []),
        "universe_insights": state.get("universe_insights"),
        "suggestions": state.get("suggestions", []),
        "dataset_id": state.get("dataset_id", ""),
        "channel": ctx.channel,
        "query_error": state.get("query_error", ""),
        "risk_flag_timeout": state.get("risk_flag_timeout", False),
        "timeout_suggestions": state.get("timeout_suggestions", []),
        "needs_clarification": state.get("needs_clarification", False),
        "clarification_reason": state.get("clarification_reason", ""),
        "degraded": state.get("degraded", False),
        "degraded_hint": state.get("degraded_hint", ""),
        "analytical_response": state.get("analytical_response", ""),
        "result_order_guarantee": state.get("result_order_guarantee", "exact_order"),
    }, narrator=deps.narrator)
    qp = result.get("query_plan", {}) or {}
    if qp.get("should_execute") is False:
        result["response"] = state.get("formatted_response", "") or (
            qp.get("clarification_question") or "Necesito más información."
        )
        result["needs_clarification"] = True
    else:
        result["response"] = advisor_response

    # ── Commit 6C-UX: header determinístico + trace honesto para frases ──
    # No pasa por el narrator: la línea de conteo es la autoridad, así el LLM no
    # confunde total_count (universo SECOP) con validated_count (confirmadas).
    phrase_counts = state.get("phrase_counts")
    # H4: en una muestra de fallback (sin orden garantizado) la validación de frases sobre
    # esa muestra no es significativa → el header "N mejor clasificadas" confundiría. El
    # disclaimer del advisor ya explica que es una muestra sin orden.
    is_sample = state.get("result_order_guarantee") == "unordered_sample"
    if phrase_counts and not result.get("needs_clarification") and not is_sample:
        from app.core.phrase_presentation import build_phrase_header, build_phrase_trace
        displayed = len(result.get("formatted_rows", []) or [])
        candidate_count = int(state.get("candidate_count", 0) or 0)
        pool_full = bool(state.get("candidate_pool_full", False))
        secop_total = int(state.get("total_count", 0) or 0)
        approximate = bool(state.get("total_count_approximate", False))
        header = build_phrase_header(phrase_counts, candidate_count, pool_full, secop_total, displayed)
        result["response"] = header + "\n\n" + (result.get("response") or "")
        result["phrase_trace"] = build_phrase_trace(
            phrase_counts, candidate_count, pool_full, secop_total, approximate, displayed
        )

    result["should_execute_decision"] = state.get("should_execute_decision")
    result["timings_ms"]["advisor_response_ms"] = _elapsed_ms(advisor_start)
    result["timings_ms"]["total_ms"] = _elapsed_ms(ctx.run_started_at)

    # ── Dual-dataset (Commit 2+3): ejecuta el hijo secundario, agrupa y PRESENTA.
    # El primario NO se re-ejecuta (sale del estado del pipeline). Best-effort: un
    # fallo aquí nunca rompe la respuesta del dataset primario.
    # Un turno con dueño terminal —Entity Profile, incluidas su aclaración y su
    # error honesto— conserva ejecución y presentación hasta el final. Sin esta
    # guarda el postprocesador dual volvía a decidir desde `dataset_scope`,
    # consultaba fuentes que el plan no autorizó y reemplazaba la respuesta.
    profile_owns_turn = owns_execution_and_presentation(result, state)
    groups = None
    try:
        from app.core.workflow.dual_dataset import build_dataset_groups
        groups = None if profile_owns_turn else build_dataset_groups(deps, result, state)
        if groups is not None:
            response, displayed = deps.formatter.format_grouped(
                groups, ctx.channel, params=state.get("resolved_params"),
            )
            result["response"] = response
            result["dataset_scope"] = list(groups.keys())
            result["dataset_groups"] = {
                name: {
                    "status": g.status,
                    "candidate_count": g.counts.get("candidate_count", 0),
                    "validated_count": g.counts.get("validated_count", 0),
                    "displayed_count": displayed.get(name, 0),
                    "candidate_pool_full": g.counts.get("candidate_pool_full", False),
                    "total_count": g.counts.get("total_count", 0),
                    "error": g.error,
                }
                for name, g in groups.items()
            }
    except Exception as exc:  # best-effort
        logger.warning("dual dataset expansion failed: %s", exc)

    # ── E8B.2: cachear la UNICA salida canonica ya producida ────────────────
    # No revalida ni reordena: solo persiste candidatos + secuencia visible para
    # que "mas" lea del mismo snapshot. En dual se guarda un buffer por grupo.
    try:
        if groups is not None:
            from app.core.workflow.dual_dataset import params_for_dataset

            common = result.get("resolved_params", {}) or {}
            for name, group_result in groups.items():
                if group_result.status != "ok":
                    continue
                group_params = params_for_dataset(common, name)
                displayed = int(
                    (result.get("dataset_groups", {}).get(name, {}) or {}).get("displayed_count", 0)
                )
                buffer = _initial_pagination_buffer(
                    group=name,
                    dataset_id=("p6dx-8zbt" if name == "procesos" else "jbjy-vk9h"),
                    params=group_params,
                    candidates=group_result.candidate_rows,
                    validated=group_result.rows,
                    displayed=displayed,
                    source_total=int(group_result.counts.get("total_count", 0) or 0),
                    source_total_approximate=bool(
                        group_result.counts.get("total_count_approximate", False)
                    ),
                )
                if ctx.chat_id:
                    deps.conv_store.save_pagination_buffer(ctx.chat_id, buffer)
                result["dataset_groups"][name].update({
                    "source_candidate_total": buffer.source_candidate_total,
                    "validated_total_known": buffer.validated_total_known,
                    "displayed_so_far": buffer.displayed_so_far,
                    "source_total_approximate": buffer.source_total_approximate,
                })
        elif (
            result.get("dataset_id")
            and not result.get("needs_clarification")
            and not profile_owns_turn
        ):
            # El perfil no publica un universo tabular paginable: sembrar un
            # buffer aquí haría que un "más" posterior recorriera filas que su
            # plan nunca mostró.
            dataset_id = result["dataset_id"]
            name = "contratos" if dataset_id == "jbjy-vk9h" else "procesos"
            validated = list(state.get("ranked_results") or state.get("results") or [])
            candidates = list(state.get("results") or [])
            buffer = _initial_pagination_buffer(
                group=name,
                dataset_id=dataset_id,
                params=result.get("resolved_params", {}) or {},
                candidates=candidates,
                validated=validated,
                displayed=len(result.get("rows", []) or []),
                source_total=int(result.get("total_count", 0) or 0),
                source_total_approximate=bool(result.get("total_count_approximate", False)),
            )
            if ctx.chat_id:
                deps.conv_store.save_pagination_buffer(ctx.chat_id, buffer)
            result.update({
                "source_candidate_total": buffer.source_candidate_total,
                "validated_total_known": buffer.validated_total_known,
                "displayed_so_far": buffer.displayed_so_far,
                "source_total_approximate": buffer.source_total_approximate,
            })
    except Exception as exc:  # cache best-effort: nunca rompe la primera pagina
        logger.warning("pagination buffer initialization failed: %s", exc)

    # ── Opportunity stage shadow (Fase 2B) — observación, no afecta resultados.
    # Solo procesos + opportunity_search. Best-effort: un fallo aquí nunca
    # rompe la respuesta; no reordena, no filtra, no consulta SECOP.
    if OPPORTUNITY_STAGE_SHADOW_ENABLED:
        try:
            rp = state.get("resolved_params", {}) or {}
            from app.core.soql_builder import SoQLBuilder
            if (state.get("dataset_id") == SoQLBuilder.PROCESOS_DATASET
                    and rp.get("intent_type") == "opportunity_search"):
                from app.core.opportunity_stage import summarize_opportunity_stages
                result["opportunity_stage_shadow"] = summarize_opportunity_stages(
                    state.get("results", []),
                    total_count=result.get("total_count"),
                    displayed_count=len(result.get("formatted_rows", []) or []),
                )
        except Exception as exc:  # best-effort: nunca rompe la respuesta
            logger.warning("opportunity_stage shadow failed: %s", exc)

    # ── Outcome bidimensional + recuperación de refinamiento (Etapa 4) ─────
    # Separa ambigüedad genuina (P15) de "0 tras refinamiento" (las falsas
    # clarificaciones del dogfood). Un count aproximado es PARTIAL, no SUCCESS.
    from app.core.outcome import Outcome, OutcomeReason, classify_outcome
    from app.core.query_fingerprint import query_fingerprint
    from app.core.refinement_recovery import build_recovery_message, refinement_delta

    resolved = state.get("resolved_params", {}) or {}
    qp_should_execute = (result.get("query_plan", {}) or {}).get("should_execute")
    previous_had_results = bool(followup and last_turn and last_turn.had_results())
    rows_returned = len(result.get("results", []) or [])
    # Las consultas analíticas (sum/aggregate) no son tabulares: tienen su propia
    # respuesta y 0 filas NO es vacío. No las pasamos por el modelo de recall.
    if result.get("analytical_response"):
        outcome, outcome_reason = Outcome.SUCCESS, OutcomeReason.RESULTS_FOUND
    else:
        outcome, outcome_reason = classify_outcome(
            # Una aclaración decidida ANTES de planear —contexto perdido, o un
            # reinicio explícito— no deja `query_plan`, así que no se veía aquí
            # y el turno acababa clasificado como "0 tras refinamiento". El
            # mensaje de recuperación decía entonces que un filtro había vaciado
            # la búsqueda: exactamente lo contrario de lo que pasó.
            clarification_requested=(
                qp_should_execute is False or bool(result.get("needs_clarification"))
            ),
            query_failed=bool(result.get("query_error")),
            query_timed_out=bool(result.get("risk_flag_timeout")),
            count_approximate=bool(result.get("total_count_approximate")),
            rows_returned=rows_returned,
            previous_had_results=previous_had_results,
        )
    result["outcome"] = outcome
    result["outcome_reason"] = outcome_reason
    result["query_fingerprint"] = query_fingerprint(resolved)

    # Etapa 5: las métricas HTTP y los timings agregados (fetch/count/pipeline_self)
    # los adjunta run_query de forma CENTRALIZADA (attach_telemetry), para que TODOS
    # los handlers — no solo este — expongan telemetría.

    # zero-after-refinement → honestidad + recuperación, NO falsa clarificación.
    if outcome == Outcome.EMPTY and outcome_reason == OutcomeReason.ZERO_AFTER_REFINEMENT:
        last_ok = deps.conv_store.last_successful_turn(ctx.chat_id) if ctx.chat_id else None
        if last_ok is not None:
            delta = refinement_delta(last_ok.parsed_params, resolved)
            result["recovery_delta"] = delta
            result["response"] = build_recovery_message(delta)
            result["needs_clarification"] = False
            # Auditoría de recall (Etapa 3) on-demand, solo bajo flag: localiza el
            # predicado asesino sin penalizar latencia en producción por defecto.
            import os
            if os.getenv("SECOPPAL_RECALL_AUDIT") == "1":
                try:
                    from app.core.recall_audit import build_predicate_waterfall, waterfall_to_dict
                    result["recall_audit"] = waterfall_to_dict(
                        build_predicate_waterfall(resolved, deps.secop_client)
                    )
                except Exception as exc:  # best-effort: diagnóstico nunca rompe la respuesta
                    logger.warning("recall audit failed: %s", exc)

    trace_id = deps.feedback.log_trace(
        ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
    )
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
    if ctx.chat_id:
        deps.conv_store.append_turn(
            chat_id=ctx.chat_id,
            user_query=ctx.user_query,
            response=result["response"],
            parsed_params=state.get("resolved_params", {}),
            result_ids=result_ids,
            trace_id=trace_id,
            suggestions=[
                dataclasses.asdict(s) if hasattr(s, '__dataclass_fields__') else s
                for s in result.get("suggestions", [])
            ],
            outcome=outcome,
            outcome_reason=outcome_reason,
            result_count=rows_returned,
            query_fingerprint=result["query_fingerprint"],
        )

    # M3C-CG-I: telemetria observacional de follow-ups de modalidad.
    # Ejecuta DESPUES de persistir el trace/turno: un fallo aqui no altera
    # resultado, respuesta, params ni persistencia conversacional.
    try:
        telemetry_event = project_event(
            followup=followup,
            context_params=context_params,
            parsed_params=result.get("parsed_params", {}),
            followup_mutation=result.get("followup_mutation", {}),
            query_plan_reason=(result.get("query_plan") or {}).get("reason", ""),
            needs_clarification=result.get("needs_clarification", False),
        )
        if telemetry_event is not None:
            append_event(deps.settings.modality_followup_telemetry_path, telemetry_event)
    except Exception as exc:
        logger.warning("modality telemetry projection failed: %s", type(exc).__name__)

    return result
