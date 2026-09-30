from __future__ import annotations

import hashlib
import logging
import time
from datetime import date as Date
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
from app.core.socrata_transport import (
    TransportKind,
    build_transport,
    transport_kind_from_settings,
)
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.context import HandlerContext
from app.core.workflow.deps import WorkflowDeps
from app.core.workflow.handlers.direct_lookup import handle_direct_lookup
from app.core.workflow.handlers.actor_selection import handle_actor_selection
from app.core.workflow.handlers.normal_flow import handle_normal_flow
from app.core.workflow.handlers.natural_follow import handle_bound_follow
from app.core.workflow.handlers.natural_alpha import (
    multi_job_result, note_seam, observed_deps, observe_turn,
)
from app.core.natural_alpha.budget import legacy_budget
from app.core.natural_alpha.turn_receipt import abort_turn, finalize_turn, turn_scope
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
        self.natural_alpha_provider = None  # lazy in eligible turns; injectable in tests
        # C4B: el verificador de residual (B) usa el mismo contrato bounded que
        # Natural Alpha y se construye perezosamente; inyectable en tests.
        self.residual_verifier_provider = None
        self.entity_resolver = EntityResolver(settings.secop_alias_db_path)
        self.query_router = QueryRouter(entity_resolver=self.entity_resolver)
        self.actor_name_resolver = ActorNameResolver(settings.secop_actor_alias_index_path)
        self.llm_handler = LLMHandler(api_key=settings.deepseek_api_key, model=settings.deepseek_model)
        self.soql_builder = SoQLBuilder()
        # Transporte seleccionado por configuración (default: SODA3; classic
        # como rollback explícito). La seleccion vive aqui y solo aqui: nada
        # aguas abajo lee Settings para decidir por donde sale la red. Un
        # valor invalido revienta aqui.
        #
        # El clasico se construye por su camino IMPLICITO, sin `transport=`.
        # No es un detalle de estilo: el timeout clasico esta horneado en el
        # objeto `Socrata`, y un transporte explicito se propaga tal cual por
        # `_respawn`, de modo que `derived_for_timeout(3)` acotaria el atributo
        # pero seguiria emitiendo por el Socrata original. El camino implicito
        # deja que `with_timeout` abra un Socrata propio por llamada -que es lo
        # que el contrato de deadlines y H4 espera- y que reasignar `.client`
        # siga redirigiendo el transporte.
        transport_kind = transport_kind_from_settings(settings)
        explicit_transport = None
        if transport_kind is TransportKind.SODA3:
            explicit_transport = build_transport(
                transport_kind,
                domain=settings.datos_gov_domain,
                app_token=settings.secop_app_token,
                timeout_seconds=settings.secop_timeout_seconds,
            )
        self.secop_client = SecopClient(
            domain=settings.datos_gov_domain,
            app_token=settings.secop_app_token,
            timeout=settings.secop_timeout_seconds,
            transport=explicit_transport,
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
        actor_candidate_selection: dict[str, Any] | None = None,
        profile_action_selection: dict[str, Any] | None = None,
        today: Date | None = None,
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
            "actor_candidate_selection": actor_candidate_selection,
            "profile_action_selection": profile_action_selection,
        }
        if run_started_at is not None:
            kwargs["run_started_at"] = run_started_at
        if today is not None:
            kwargs["today"] = today
        return HandlerContext(**kwargs)

    def run_query(
        self,
        user_query: str,
        channel: str = "whatsapp",
        chat_id: str | None = None,
        user_identity: dict[str, Any] | None = None,
        seed_resolved_params: dict | None = None,
        actor_candidate_selection: dict[str, Any] | None = None,
        profile_action_selection: dict[str, Any] | None = None,
        today: Date | None = None,
    ) -> dict:
        """Dispatch a request through the handler chain.

        Chain of responsibility (first non-None result wins):

        1. ``handle_refinement`` — when the UI sends pre-resolved params.
        2. ``handle_reset``       — ``/reset`` and synonyms.
        3. ``handle_actor_selection`` — user chose a pending actor candidate.
        4. ``handle_radar_command`` — guardar/listar/ejecutar radar.
        5. ``handle_pagination``  — "muestrame mas" and synonyms.
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
            actor_candidate_selection=actor_candidate_selection,
            profile_action_selection=profile_action_selection,
            today=today,
            run_started_at=time.perf_counter(),
        )
        # One coverage receipt spans the whole turn, including coordinate_turn.
        with turn_scope(self.settings, self.feedback, ctx) as receipt:
            try:
                result = self._run_turn(ctx, seed_resolved_params)
            except BaseException:
                abort_turn(receipt, self.feedback)
                raise
            finalize_turn(receipt, self.feedback, ctx, result)
        return result

    def _run_turn(self, ctx: HandlerContext, seed_resolved_params: dict | None) -> dict:
        # Etapa 5: la recolección de telemetría es por-turno = por-run_query. begin
        # aquí + end en finally garantiza el cierre del ContextVar en TODA salida
        # (early return de cualquier handler, excepción). El attach es CENTRALIZADO:
        # todos los handlers exponen http_metrics, no solo normal_flow. El token hace
        # segura la anidación (un run_query dentro de otro restaura el buffer externo).
        from app.core.deadline import attach_deadline_telemetry, begin_deadline, end_deadline
        from app.core.capability_binding.dogfood_telemetry import (
            attach_a2_telemetry, begin_turn, end_turn,
        )
        from app.core.instrumentation import attach_telemetry, begin_collection, end_collection
        from app.core.source_call_receipts import begin_source_calls, end_source_calls
        token = begin_collection()
        a2_token = begin_turn()  # A2 dogfood receipt; inactive unless bound.
        dl_tokens = begin_deadline(ctx.run_started_at)  # H2: deadline monotónico del turno
        source_token = (
            begin_source_calls(hashlib.sha256(
                ctx.user_query.encode("utf-8", errors="surrogatepass")
            ).hexdigest())
            if (getattr(self.settings, "secoppal_foundation_source_receipts_enabled", False) is True
                or getattr(self.settings, "secoppal_foundation_mode", "legacy") == "foundation_v3") else None
        )
        try:
            result = self._admit_and_dispatch(ctx, seed_resolved_params)
        except BaseException:
            # El turno abortó sin result: cerramos collector + deadline y NO perdemos la
            # evidencia del error (un timeout es justo donde más importa). El attach no
            # aplica porque no hay result; la telemetría va al log estructurado.
            metrics = end_collection(token)
            end_turn(a2_token)
            end_deadline(dl_tokens)
            source_calls = end_source_calls(source_token) if source_token is not None else []
            if metrics:
                logger.warning(
                    "run_query abortado tras capturar %d métricas HTTP: %s",
                    len(metrics), [dict(m) for m in metrics],
                )
            if source_calls:
                logger.warning("run_query abortado tras %d llamadas SECOP", len(source_calls))
            raise
        metrics = end_collection(token)
        a2_receipt = end_turn(a2_token)
        dl_tel = end_deadline(dl_tokens)
        source_calls = end_source_calls(source_token) if source_token is not None else None
        attach_telemetry(result, metrics)
        if source_calls is not None:
            result["source_call_receipts"] = source_calls
        attach_a2_telemetry(result, a2_receipt)
        attach_deadline_telemetry(result, dl_tel, ctx.run_started_at)
        from app.core.buyer_groups import BuyerGroupError, group_declaration, group_for_params
        group_params = result.get("resolved_params") or {}
        try:
            group = group_for_params(group_params)
        except BuyerGroupError:
            group = None  # planning already surfaced the invalid reference
        if group:
            result["buyer_group"] = {**group.reference(), "members": list(group.members)}
            result["total_count_metric"] = "physical_rows"
            result["total_count_scope"] = (
                "returned_ranking" if result.get("analytical_intent") == "ranking"
                else "filtered_universe"
            )
            if result.get("analytical_intent") == "value_stats":
                result["total_count_metric"] = "not_available"
            # Entity Analysis already presents the group in its bounded overview.
            # Preserve all members above and the declaration on every other route.
            if result.get("analytical_intent") != "entity_profile":
                result["response"] = (
                    group_declaration(group_params) + "\n\n" + result.get("response", "")
                )
        return result

    def select_actor_candidate(
        self,
        candidate_rank: int,
        pending_token: str,
        *,
        channel: str = "whatsapp",
        chat_id: str | None = None,
        user_identity: dict[str, Any] | None = None,
    ) -> dict:
        """Submit transport-independent ``SELECT_ACTOR_CANDIDATE``.

        A transport supplies only the displayed rank and pending-list token.
        Identity lookup, provenance, context restoration, and execution remain
        in ``handle_actor_selection``, shared with natural text selections.
        """
        from app.core.actor_conversation import ActorConversationOperation

        return self.run_query(
            f"opción {candidate_rank}",
            channel=channel,
            chat_id=chat_id,
            user_identity=user_identity,
            actor_candidate_selection={
                "operation": ActorConversationOperation.SELECT_ACTOR_CANDIDATE,
                "candidate_rank": candidate_rank,
                "pending_token": pending_token,
            },
        )

    def select_entity_profile_action(
        self,
        action_id: object,
        reference: object,
        *,
        channel: str = "whatsapp",
        chat_id: str | None = None,
        user_identity: dict[str, Any] | None = None,
    ) -> dict:
        """Submit opaque UI identity; only admission may materialize meaning."""
        from app.core.entity_profile.executive_actions import action_for_id
        from app.core.workflow.profile_action_admission import PROFILE_ACTION_EVENT_VERSION

        result = self.run_query(
            "Acción de perfil",
            channel=channel,
            chat_id=chat_id,
            user_identity=user_identity,
            profile_action_selection={
                "schema_version": PROFILE_ACTION_EVENT_VERSION,
                "action_id": action_id,
                "reference": reference,
            },
        )
        action = action_for_id(action_id)
        result["profile_action_phrase"] = (
            action.phrase if action else "Acción de perfil no disponible"
        )
        return result

    def _admit_and_dispatch(
        self, ctx: HandlerContext, seed_resolved_params: dict | None,
    ) -> dict:
        from app.core.workflow.turn_coordination import coordinate_turn

        return coordinate_turn(self, ctx, seed_resolved_params)

    def _dispatch(self, ctx: HandlerContext, seed_resolved_params: dict | None,
                  observation=None) -> dict:
        """Chain of responsibility — first non-None handler result wins.

        ``observation`` is the turn's Natural Alpha observation when the
        coordinator already made it before admission (plan V2 8). It is ``None``
        on the lanes that never reach admission, and whenever the pre-observation
        was declined, so the seam observes here instead. The provider is called
        at most once per turn either way, and the seam witness is noted only
        where the seam is actually reached.
        """
        if seed_resolved_params:
            res = handle_refinement(self.deps, ctx, seed_resolved_params)
            if res is not None:
                res["_continuation_owner"] = "TABULAR"
                return res
            # If the refinement guard failed (no scope/topic), fall through.

        for handler in (
            handle_reset,
            handle_actor_selection,
            handle_radar_command,
            handle_pagination,
            handle_suggestion_selection,
            handle_direct_lookup,
        ):
            res = handler(self.deps, ctx)
            if res is not None:
                res["_continuation_owner"] = {
                    handle_actor_selection: "ACTOR", handle_radar_command: "RADAR",
                    handle_pagination: "TABULAR", handle_suggestion_selection: "TABULAR",
                }.get(handler, "NONE")
                return res

        deps = self.deps
        if observation is None:
            observation = observe_turn(deps, ctx, self.natural_alpha_provider)
        if observation is not None:
            note_seam(deps)
            if observation.terminal:
                return multi_job_result(deps, ctx, observation)
        from app.core.capability_binding.registry import capability_for

        binding = capability_for(observation, deps.settings)
        followed = handle_bound_follow(deps, ctx, binding, observation)
        if followed is not None:
            followed["_continuation_owner"] = "RADAR"
            return followed
        with legacy_budget(observation is not None):
            result = handle_normal_flow(observed_deps(deps, observation, binding), ctx)
        params = result.get("resolved_params") or {}
        result["_continuation_owner"] = "ACTOR" if params.get("actor_scope") or params.get("actor_pending") else "TABULAR"
        return result

    def rate_query(self, trace_id: str, rating: int, comment: str | None = None) -> bool:
        return self.feedback.rate(trace_id, rating, comment)

    def get_feedback_stats(self) -> dict:
        return self.feedback.get_stats()

    def get_usage_by_user(self) -> list[dict]:
        return self.feedback.get_usage_by_user()
