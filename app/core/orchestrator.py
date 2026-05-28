from __future__ import annotations

import dataclasses
import logging
import time
from typing import Any

from app.config import Settings
from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.conversation_store import ConversationStore, is_followup, is_reset_command, is_pagination_phrase
from app.core.entity_resolver import EntityResolver
from app.core.feedback import FeedbackStore
from app.core.formatter import Formatter
from app.core.llm_handler import LLMHandler
from app.core.narrator import NarratorHandler
from app.core.observer import observe as observe_universe_fn
from app.core.query_router import QueryRouter
from app.core.response_policy import build_advisor_response, build_clarification_response
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder
from app.core.suggester import generate_suggestions
from app.core.relevance import RelevanceScorer
from app.core.workflow.context import HandlerContext
from app.core.workflow.deps import WorkflowDeps
from app.core.workflow.graph import build_application
from app.core.workflow.handlers.direct_lookup import handle_direct_lookup
from app.core.workflow.handlers.pagination import handle_pagination
from app.core.workflow.handlers.refinement import handle_refinement
from app.core.workflow.handlers.reset import handle_reset
from app.core.workflow.handlers.suggestion import handle_suggestion_selection
from app.core.workflow.helpers import (
    _build_timeout_suggestions,
    _elapsed_ms,
    _is_timeout_exception,
    _query_first_then_count,
    _relax_params,
    _with_timing,
    _with_timings,
)

# ── Legacy compatibility shim ────────────────────────────────────────────
# These actions moved to app.core.workflow.actions/* in Paso 5B-5E. Pre-
# existing tests still import them from app.core.orchestrator
# (test_degrade_query, test_guard_where_1_1_equivalence). Runtime composition
# (workflow/graph.py) imports actions DIRECTLY from workflow/actions — the
# re-exports below are NOT used by the graph, only by legacy test imports.
#
# tests/test_workflow_architecture.py enforces that graph.py and the action
# modules themselves never import from orchestrator. Keeping these shim
# imports in orchestrator.py does not violate those invariants.
#
# TODO(refactor): migrate test_degrade_query.py and
# test_guard_where_1_1_equivalence.py to import from
# app.core.workflow.actions.{execution,parsing,planning,rendering} and
# remove this block. Tracked as housekeeping after handler extraction
# (Paso 7+) completes.
from app.core.workflow.actions.execution import (  # noqa: E402, F401
    degrade_query,
    execute_query,
    observe_universe,
)
from app.core.workflow.actions.parsing import (  # noqa: E402, F401
    apply_context,
    llm_parse,
    parse_query,
)
from app.core.workflow.actions.planning import (  # noqa: E402, F401
    build_query,
    plan_query,
    resolve_entities,
)
from app.core.workflow.actions.rendering import (  # noqa: E402, F401
    clarify_query,
    format_response,
    suggester_action,
)


logger = logging.getLogger(__name__)

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
        self.feedback = FeedbackStore(
            settings.secop_feedback_path,
            db_path=settings.secop_feedback_db_path,
            sqlite_enabled=settings.secop_feedback_sqlite_enabled,
            jsonl_dual_write=settings.secop_feedback_jsonl_dual_write,
        )
        self.conv_store = ConversationStore()
        self.narrator = NarratorHandler(
            api_key=settings.deepseek_api_key,
            model=settings.deepseek_model,
        )

    @property
    def deps(self) -> "WorkflowDeps":
        """Fresh snapshot of collaborators for handler injection.

        Built on every access so test-time reassignments such as
        ``wf.conv_store = X`` or ``wf.secop_client = X`` are reflected in
        the deps object handed to the next handler call. See
        ``app/core/workflow/deps.py`` for the rationale.
        """
        return WorkflowDeps(
            settings=self.settings,
            entity_resolver=self.entity_resolver,
            query_router=self.query_router,
            llm_handler=self.llm_handler,
            soql_builder=self.soql_builder,
            secop_client=self.secop_client,
            formatter=self.formatter,
            feedback=self.feedback,
            conv_store=self.conv_store,
            narrator=self.narrator,
        )

    def _make_ctx(
        self,
        user_query: str,
        *,
        channel: str = "whatsapp",
        chat_id: str | None = None,
        user_identity: dict[str, Any] | None = None,
        seed_resolved_params: dict | None = None,
        run_started_at: float | None = None,
    ) -> "HandlerContext":
        """Build a HandlerContext from run_query's arguments.

        ``run_started_at`` is accepted for tests/handlers that need to
        share a clock; otherwise the context's default_factory captures
        ``time.perf_counter()`` at construction.
        """
        kwargs: dict[str, Any] = {
            "user_query": user_query,
            "channel": channel,
            "chat_id": chat_id,
            "user_identity": user_identity,
            "seed_resolved_params": seed_resolved_params,
        }
        if run_started_at is not None:
            kwargs["run_started_at"] = run_started_at
        return HandlerContext(**kwargs)

    def run_query(
        self,
        user_query: str,
        channel: str = "whatsapp",
        chat_id: str | None = None,
        user_identity: dict[str, Any] | None = None,
        seed_resolved_params: dict | None = None,
    ) -> dict:
        run_start = time.perf_counter()

        # ── Refinement bar bypass (DOGFOOD-A) ─────────────────────────────
        # Cuando la UI envía params ya resueltos desde un botón de refinamiento,
        # se omite el parser y se ejecuta el SoQL directamente. Esto evita que
        # un texto sintético como "filtrando por X" sea malinterpretado y que
        # el contexto previo se pierda.
        if seed_resolved_params:
            ctx = self._make_ctx(
                user_query,
                channel=channel,
                chat_id=chat_id,
                user_identity=user_identity,
                seed_resolved_params=seed_resolved_params,
            )
            res = handle_refinement(self.deps, ctx, seed_resolved_params)
            if res is not None:
                return res
            # si el handler no pudo (sin scope/topic), caer al flujo normal
        # ── Reset command ────────────────────────────────────────────────
        reset_ctx = self._make_ctx(user_query, channel=channel, chat_id=chat_id, user_identity=user_identity)
        reset_result = handle_reset(self.deps, reset_ctx)
        if reset_result is not None:
            return reset_result

        # ── Pagination detection ──────────────────────────────────────────
        pag_ctx = self._make_ctx(user_query, channel=channel, chat_id=chat_id, user_identity=user_identity)
        pag_result = handle_pagination(self.deps, pag_ctx)
        if pag_result is not None:
            return pag_result

        # ── Suggestion selection ─────────────────────────────────────────
        sug_ctx = self._make_ctx(user_query, channel=channel, chat_id=chat_id, user_identity=user_identity)
        sug_result = handle_suggestion_selection(self.deps, sug_ctx)
        if sug_result is not None:
            return sug_result

        # ── Direct lookup (Feature A1) ────────────────────────────────────
        dl_ctx = self._make_ctx(user_query, channel=channel, chat_id=chat_id, user_identity=user_identity)
        dl_result = handle_direct_lookup(self.deps, dl_ctx)
        if dl_result is not None:
            return dl_result

        # ── Normal flow ──────────────────────────────────────────────────
        # Follow-up detection
        history = self.conv_store.get_history(chat_id, last_n=5) if chat_id else []
        last_turn = history[-1] if history else None
        followup = is_followup(user_query, last_turn)
        context_params = dict(last_turn.parsed_params) if (followup and last_turn) else {}

        app = build_application(
            self.deps,
            user_query=user_query,
            channel=channel,
            followup=followup,
            context_params=context_params,
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
            "analytical_intent": state.get("analytical_intent", ""),
            "analytical_response": state.get("analytical_response", ""),
            "clarification_reason": state.get("clarification_reason", ""),
            "total_count_approximate": state.get("total_count_approximate", False),
            "query_plan": state.get("query_plan", {}),
            "followup": followup,
            "timings_ms": dict(state.get("timings_ms", {}) or {}),
            "contextual_filters": {},
        }

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

        # ── Entity ecosystem expansion (Sprint 6 Fase 3) ─────────────────
        # Si no hay entidad explícita pero sí ciudad/departamento, expandir
        resolved_post = state.get("resolved_params", {})
        if resolved_post and not resolved_post.get("entidad_resolved") and not resolved_post.get("entidad_like"):
            from app.core.entity_ecosystem import get_ecosystem_entities, should_expand_ecosystem
            if should_expand_ecosystem(resolved_post):
                ecosys = get_ecosystem_entities(
                    ciudad=str(resolved_post.get("ciudad", "")),
                    departamento=str(resolved_post.get("departamento_resolved", "")),
                )
                if ecosys:
                    resolved_post = dict(resolved_post)
                    resolved_post["entidad_ecosystem"] = ecosys
                    # Rebuild SoQL with ecosystem entities
                    try:
                        new_soql = self.soql_builder.build(state["dataset_id"], resolved_post)
                        state = state.update(soql_query=new_soql, resolved_params=resolved_post)
                    except Exception:
                        pass  # ecosystem is best-effort

        # ── Relevance scoring (Sprint 6 Fase 1 — BM25 post-ranking) ───
        resolved = state.get("resolved_params", {})
        objeto_terms = resolved.get("objeto", []) or []
        if objeto_terms and len(state.get("results", [])) > 0:
            try:
                scorer = RelevanceScorer()
                topic_flat: list[str] = []
                for t in objeto_terms:
                    if isinstance(t, list):
                        topic_flat.extend(t)
                    else:
                        topic_flat.append(str(t))
                if len(topic_flat) >= 3:
                    field = "objeto_del_contrato" if state.get("dataset_id") == "jbjy-vk9h" else "nombre_del_procedimiento"
                    scored = scorer.score_rows(topic_flat, state["results"], field=field)
                    state = state.update(
                        results=[r for r, _ in scored],
                        relevance_classes=[s.classification for _, s in scored],
                        relevance_scores=[s.bm25_score for _, s in scored],
                    )
            except Exception as exc:
                logger.warning("relevance scoring failed: %s", exc)

        advisor_response = build_advisor_response({
            "user_query": user_query,
            "resolved_params": state.get("resolved_params", {}),
            "total_count": state.get("total_count", 0),
            "total_count_approximate": state.get("total_count_approximate", False),
            "rows": state.get("formatted_rows", []),
            "universe_insights": state.get("universe_insights"),
            "suggestions": state.get("suggestions", []),
            "dataset_id": state.get("dataset_id", ""),
            "channel": channel,
            "query_error": state.get("query_error", ""),
            "risk_flag_timeout": state.get("risk_flag_timeout", False),
            "timeout_suggestions": state.get("timeout_suggestions", []),
            "needs_clarification": state.get("needs_clarification", False),
            "clarification_reason": state.get("clarification_reason", ""),
            "degraded": state.get("degraded", False),
            "degraded_hint": state.get("degraded_hint", ""),
            "analytical_response": state.get("analytical_response", ""),
        }, narrator=self.narrator)
        qp = result.get("query_plan", {}) or {}
        if qp.get("should_execute") is False:
            result["response"] = state.get("formatted_response", "") or (
                qp.get("clarification_question") or "Necesito más información."
            )
            result["needs_clarification"] = True
        else:
            result["response"] = advisor_response
        result["should_execute_decision"] = state.get("should_execute_decision")
        result["timings_ms"]["advisor_response_ms"] = _elapsed_ms(advisor_start)
        result["timings_ms"]["total_ms"] = _elapsed_ms(run_start)

        trace_id = self.feedback.log_trace(user_query, channel, result, user_identity=user_identity)
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
