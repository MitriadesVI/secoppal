from __future__ import annotations

import logging
import time
from typing import Any

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.entity_resolver import EntityResolver
from app.core.feedback import FeedbackStore
from app.core.formatter import Formatter
from app.core.llm_handler import LLMHandler
from app.core.narrator import NarratorHandler
from app.core.query_router import QueryRouter
from app.core.radar.store import RadarStore
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.context import HandlerContext
from app.core.workflow.deps import WorkflowDeps
from app.core.workflow.handlers.direct_lookup import handle_direct_lookup
from app.core.workflow.handlers.actor_selection import handle_actor_selection
from app.core.workflow.handlers.normal_flow import handle_normal_flow
from app.core.workflow.handlers.pagination import handle_pagination
from app.core.workflow.handlers.radar import handle_radar_command
from app.core.workflow.handlers.refinement import handle_refinement
from app.core.workflow.handlers.reset import handle_reset
from app.core.workflow.handlers.suggestion import handle_suggestion_selection
from app.data.actor_alias_resolver import ActorNameResolver

# ── Legacy compatibility shim ────────────────────────────────────────────
# The 12 Burr actions and the private helpers below moved to
# app.core.workflow.{actions,helpers} in Pasos 5 and 1 respectively.
# Pre-existing tests still import them from app.core.orchestrator:
#
#   - test_degrade_query              → _relax_params, degrade_query
#   - test_guard_where_1_1_equivalence → execute_query
#   - test_count_lazy                  → _query_first_then_count
#
# Runtime composition (workflow/graph.py + workflow/handlers/*) imports
# from workflow/* directly — the re-exports below are ONLY consumed by
# those legacy test files. tests/test_workflow_architecture.py enforces
# that graph.py and the workflow modules never import from orchestrator;
# keeping these aliases here does not violate that invariant.
#
# TODO(refactor): migrate the three test files above to import from
# app.core.workflow.{actions.execution,actions.parsing,actions.planning,
# actions.rendering,helpers} and delete this whole block.
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
from app.core.workflow.helpers import (  # noqa: E402, F401
    _query_first_then_count,
    _relax_params,
)


logger = logging.getLogger(__name__)

class SecopalWorkflow:
    """High-level workflow facade around Burr."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.entity_resolver = EntityResolver(settings.secop_alias_db_path)
        self.query_router = QueryRouter(entity_resolver=self.entity_resolver)
        self.actor_name_resolver = ActorNameResolver(settings.secop_actor_alias_index_path)
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
        self.radar_store = RadarStore(settings.secop_feedback_db_path)

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
            actor_name_resolver=self.actor_name_resolver,
            llm_handler=self.llm_handler,
            soql_builder=self.soql_builder,
            secop_client=self.secop_client,
            formatter=self.formatter,
            feedback=self.feedback,
            conv_store=self.conv_store,
            narrator=self.narrator,
            radar_store=self.radar_store,
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
        """Dispatch a request through the handler chain.

        Chain of responsibility (first non-None result wins):

        1. ``handle_refinement`` — when the UI sends pre-resolved params.
        2. ``handle_reset``       — ``/reset`` and synonyms.
        3. ``handle_radar_command`` — guardar/listar/ejecutar radar.
        4. ``handle_pagination``  — "muestrame mas" and synonyms.
        5. ``handle_actor_selection`` — user chose a pending actor candidate.
        6. ``handle_suggestion_selection`` — user picked a numbered chip.
        7. ``handle_direct_lookup`` — query is a SECOP id/URL/entity_ref.
        8. ``handle_normal_flow`` — fallback that runs the Burr pipeline.

        Every handler receives a fresh ``HandlerContext`` and the
        ``self.deps`` snapshot. ``run_started_at`` baked into the
        context is the single authoritative clock for end-to-end timing.
        """
        ctx = self._make_ctx(
            user_query,
            channel=channel,
            chat_id=chat_id,
            user_identity=user_identity,
            seed_resolved_params=seed_resolved_params,
            run_started_at=time.perf_counter(),
        )

        # Etapa 5: la recolección de telemetría es por-turno = por-run_query. begin
        # aquí + end en finally garantiza el cierre del ContextVar en TODA salida
        # (early return de cualquier handler, excepción). El attach es CENTRALIZADO:
        # todos los handlers exponen http_metrics, no solo normal_flow. El token hace
        # segura la anidación (un run_query dentro de otro restaura el buffer externo).
        from app.core.deadline import attach_deadline_telemetry, begin_deadline, end_deadline
        from app.core.instrumentation import attach_telemetry, begin_collection, end_collection
        token = begin_collection()
        dl_tokens = begin_deadline(ctx.run_started_at)  # H2: deadline monotónico del turno
        try:
            result = self._dispatch(ctx, seed_resolved_params)
        except BaseException:
            # El turno abortó sin result: cerramos collector + deadline y NO perdemos la
            # evidencia del error (un timeout es justo donde más importa). El attach no
            # aplica porque no hay result; la telemetría va al log estructurado.
            metrics = end_collection(token)
            end_deadline(dl_tokens)
            if metrics:
                logger.warning(
                    "run_query abortado tras capturar %d métricas HTTP: %s",
                    len(metrics), [dict(m) for m in metrics],
                )
            raise
        metrics = end_collection(token)
        dl_tel = end_deadline(dl_tokens)
        attach_telemetry(result, metrics)
        attach_deadline_telemetry(result, dl_tel, ctx.run_started_at)
        return result

    def _dispatch(self, ctx: HandlerContext, seed_resolved_params: dict | None) -> dict:
        """Chain of responsibility — first non-None handler result wins."""
        if seed_resolved_params:
            res = handle_refinement(self.deps, ctx, seed_resolved_params)
            if res is not None:
                return res
            # If the refinement guard failed (no scope/topic), fall through.

        for handler in (
            handle_reset,
            handle_radar_command,
            handle_pagination,
            handle_actor_selection,
            handle_suggestion_selection,
            handle_direct_lookup,
        ):
            res = handler(self.deps, ctx)
            if res is not None:
                return res

        return handle_normal_flow(self.deps, ctx)

    def rate_query(self, trace_id: str, rating: int, comment: str | None = None) -> bool:
        return self.feedback.rate(trace_id, rating, comment)

    def get_feedback_stats(self) -> dict:
        return self.feedback.get_stats()

    def get_usage_by_user(self) -> list[dict]:
        return self.feedback.get_usage_by_user()
