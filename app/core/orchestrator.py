from __future__ import annotations

import dataclasses
import logging
import time
from typing import Any

from app.config import Settings
from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.conversation_store import ConversationStore, is_followup, is_reset_command, is_pagination_phrase
from app.core.direct_responses import reset_response, pagination_no_history_response, suggestion_invalid_response, suggestion_selection_header
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
from app.core.workflow.helpers import (
    _build_timeout_suggestions,
    _elapsed_ms,
    _is_timeout_exception,
    _query_first_then_count,
    _relax_params,
    _with_timing,
    _with_timings,
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

    def _run_refinement(
        self,
        user_query: str,
        channel: str,
        chat_id: str | None,
        user_identity: dict[str, Any] | None,
        seed_resolved_params: dict,
    ) -> dict | None:
        """Ejecuta una refinement bar mergeada con contexto previo.

        Toma los params ya resueltos del turno anterior + el filtro recién aplicado,
        construye SoQL y ejecuta sin pasar por el parser. Retorna None si no se
        puede ejecutar (sin scope/topic) para que el caller caiga al flujo normal.
        """
        params = dict(seed_resolved_params)
        # offset/limit deben empezar de cero porque es una nueva búsqueda
        params.pop("offset", None)
        params.setdefault("limit", 50)

        # Guard anti-WHERE 1=1
        has_scope_or_topic = any(
            params.get(k) and params.get(k) not in (None, [], "", {}, False)
            for k in SCOPE_TOPIC_KEYS
        )
        if not has_scope_or_topic:
            return None

        ds_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
        try:
            soql = self.soql_builder.build(ds_id, params)
            count_soql = self.soql_builder.build_count(ds_id, params)
            total, results, _t, approx = _query_first_then_count(
                self.secop_client, ds_id, count_soql, soql,
                limit=int(params.get("limit", 50) or 50),
            )
        except Exception as exc:
            logger.warning("refinement failed: %s", exc)
            return None

        response, rows = self.formatter.format_for_channel(
            results, ds_id, channel,
            params=params, total_count=total,
            total_count_approximate=approx,
        )
        header = "🎚️ Refinamiento aplicado\n\n"
        result = {
            "response": header + response,
            "rows": rows,
            "results": results,
            "dataset_id": ds_id,
            "soql_query": soql,
            "parsed_params": params,
            "resolved_params": params,
            "needs_llm": False,
            "needs_clarification": False,
            "route_reason": "refinement",
            "degraded": False, "degraded_hint": "",
            "total_count": total,
            "total_count_approximate": approx,
            "universe_insights": None,
            "suggestions": [], "timeout_suggestions": [],
            "intent_type": "refinement",
            "followup": True,
            "trace_id": "", "narrated": False,
            "contextual_filters": {},
        }

        # Contextual filters (mismo threshold que flujo normal: ≥5)
        if results and total >= 5:
            try:
                from app.core.contextual_suggestions import compute_contextual_filters
                result["contextual_filters"] = compute_contextual_filters(
                    results, ds_id, params,
                )
            except Exception:
                pass

        trace_id = self.feedback.log_trace(user_query, channel, result, user_identity=user_identity)
        result["trace_id"] = trace_id

        if chat_id:
            result_ids = [
                rid for rid in (
                    str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
                    for r in results[:10]
                ) if rid
            ]
            self.conv_store.append_turn(
                chat_id=chat_id,
                user_query=user_query,
                response=result["response"],
                parsed_params=params,
                result_ids=result_ids,
                trace_id=trace_id,
            )
        return result

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
            res = self._run_refinement(
                user_query=user_query,
                channel=channel,
                chat_id=chat_id,
                user_identity=user_identity,
                seed_resolved_params=seed_resolved_params,
            )
            if res is not None:
                return res
            # si _run_refinement no pudo, caer al flujo normal
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
                    total, results, _secop_timings, _approx = _query_first_then_count(
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
                    trace_id = self.feedback.log_trace(user_query, channel, result, user_identity=user_identity)
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
                        total, results, _secop_timings, _approx = _query_first_then_count(
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
                            intent_type=sug_params.get("intent_type", ""),
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
            trace_id = self.feedback.log_trace(user_query, channel, result, user_identity=user_identity)
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

        # ── Direct lookup (Feature A1) ────────────────────────────────────
        from app.core.direct_lookup import (
            detect_direct_lookup, dataset_for_id, PREFIX_DATASET,
            normalize_entity_ref, build_entity_ref_soql,
        )

        lookup_value, lookup_kind = detect_direct_lookup(user_query)
        if lookup_value is not None:
            try:
                # ── Branch: entity_ref → LIKE query across both datasets ─
                if lookup_kind == "entity_ref":
                    normalized = normalize_entity_ref(lookup_value)
                    datasets_to_search = [SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET]
                    all_results: list[dict] = []
                    soql = ""
                    for ds_id in datasets_to_search:
                        soql = build_entity_ref_soql(ds_id, normalized, limit=5)
                        try:
                            rows = self.secop_client.query(ds_id, soql)
                            for r in rows:
                                r["_dataset_id"] = ds_id
                            all_results.extend(rows)
                        except Exception:
                            continue

                    match_count = len(all_results)

                    # 0 results — friendly message
                    if match_count == 0:
                        return {
                            "response": (
                                f"🔍 No encontré la referencia `{lookup_value}` en SECOP II.\n\n"
                                f"Puede que:\n"
                                f"• La referencia esté mal escrita.\n"
                                f"• El proceso sea de SECOP I (anterior a 2019).\n"
                                f"• El proceso aún no esté publicado en datos.gov.co.\n\n"
                                f"💡 Intenta con el número electrónico CO1.XXX.YYY si lo tienes."
                            ),
                            "rows": [], "results": [], "dataset_id": "", "soql_query": soql,
                            "parsed_params": {}, "resolved_params": {},
                            "needs_llm": False, "needs_clarification": False,
                            "route_reason": "direct_lookup_entity_ref",
                            "degraded": False, "degraded_hint": "", "total_count": 0,
                            "universe_insights": None, "suggestions": [],
                            "timeout_suggestions": [], "intent_type": "direct_lookup",
                            "followup": False, "trace_id": "", "narrated": False,
                        }

                    # 1 result — rich detail (same as id/url lookup)
                    if match_count == 1:
                        hit = all_results[0]
                        ds = hit.pop("_dataset_id")
                        hit_results = [hit]
                        response, rows = self.formatter.format_for_channel(
                            hit_results, ds, channel,
                            params={"intent_type": "direct_lookup", "route_reason": "direct_lookup_entity_ref"},
                            total_count=1,
                        )
                        result = {
                            "response": response, "rows": rows, "results": hit_results,
                            "dataset_id": ds, "soql_query": soql,
                            "parsed_params": {"direct_lookup_ref": lookup_value, "route_reason": "direct_lookup_entity_ref"},
                            "resolved_params": {"direct_lookup_ref": lookup_value},
                            "needs_llm": False, "needs_clarification": False,
                            "route_reason": "direct_lookup_entity_ref",
                            "degraded": False, "degraded_hint": "", "total_count": 1,
                            "universe_insights": None, "suggestions": [],
                            "timeout_suggestions": [], "intent_type": "direct_lookup",
                            "followup": False, "trace_id": "", "narrated": False,
                        }
                        trace_id = self.feedback.log_trace(user_query, channel, result, user_identity=user_identity)
                        result["trace_id"] = trace_id
                        return result

                    # 2+ results — disambiguation list
                    candidates: list[str] = []
                    for i, r in enumerate(all_results[:5], 1):
                        ds = r.get("_dataset_id")
                        id_ = r.get("id_del_proceso") or r.get("id_contrato", "")
                        entidad = (r.get("entidad") or r.get("nombre_entidad", "") or "?")[:60]
                        objeto = (r.get("nombre_del_procedimiento") or r.get("objeto_del_contrato", "") or "Sin titulo")[:80]
                        estado = r.get("estado_de_apertura_del_proceso") or r.get("estado_contrato", "?")
                        tipo = "Contrato" if ds == "jbjy-vk9h" else "Proceso"
                        candidates.append(
                            f"{i}. [{tipo}] `{id_}` — {objeto}\n"
                            f"   🏛️ {entidad} | 📌 {estado}"
                        )

                    disambig_response = (
                        f"🔍 Encontré {match_count} resultados para la referencia `{lookup_value}`:\n\n"
                        + "\n\n".join(candidates)
                        + f"\n\nResponde con el número (1-{min(match_count, 5)}) o pega el ID CO1.XXX.YYY para ver el detalle completo."
                    )

                    result = {
                        "response": disambig_response,
                        "rows": [], "results": [], "dataset_id": "", "soql_query": soql,
                        "parsed_params": {"direct_lookup_ref": lookup_value, "route_reason": "entity_ref_disambiguation"},
                        "resolved_params": {"direct_lookup_ref": lookup_value},
                        "needs_llm": False, "needs_clarification": False,
                        "route_reason": "entity_ref_disambiguation",
                        "degraded": False, "degraded_hint": "", "total_count": match_count,
                        "universe_insights": None, "suggestions": [], "timeout_suggestions": [],
                        "intent_type": "direct_lookup", "followup": False,
                        "trace_id": "", "narrated": False,
                    }
                    trace_id = self.feedback.log_trace(user_query, channel, result, user_identity=user_identity)
                    result["trace_id"] = trace_id
                    return result

                # ── Branch: id or url → equality query ──────────────────
                lookup_dataset = dataset_for_id(lookup_value)
                datasets_to_try: list[str] = (
                    [lookup_dataset] if lookup_dataset
                    else [SoQLBuilder.PROCESOS_DATASET, SoQLBuilder.CONTRATOS_DATASET]
                )

                hit_results: list[dict] = []
                hit_dataset = ""
                soql = ""
                for ds_id in datasets_to_try:
                    spec = SoQLBuilder.SPECS[ds_id]
                    id_field = "id_del_proceso" if ds_id == SoQLBuilder.PROCESOS_DATASET else "id_contrato"
                    fields = ", ".join(spec.select_fields)
                    soql = (
                        f"SELECT {fields} "
                        f"WHERE {id_field} = '{SoQLBuilder._escape(lookup_value)}' "
                        f"LIMIT 1"
                    )
                    try:
                        rows = self.secop_client.query(ds_id, soql)
                        if rows:
                            hit_results = rows
                            hit_dataset = ds_id
                            break
                    except Exception:
                        continue

                route_reason = f"direct_lookup_{lookup_kind}"

                if not hit_results:
                    return {
                        "response": (
                            f"🔍 No encontré el proceso con ID `{lookup_value}` en SECOP II.\n\n"
                            f"Puede que:\n"
                            f"• El ID esté mal escrito.\n"
                            f"• El proceso sea de SECOP I (anterior a 2019).\n"
                            f"• El proceso aún no esté publicado en datos.gov.co."
                        ),
                        "rows": [], "results": [], "dataset_id": "", "soql_query": soql,
                        "parsed_params": {}, "resolved_params": {},
                        "needs_llm": False, "needs_clarification": False,
                        "route_reason": route_reason,
                        "degraded": False, "degraded_hint": "", "total_count": 0,
                        "universe_insights": None, "suggestions": [],
                        "timeout_suggestions": [], "intent_type": "direct_lookup",
                        "followup": False, "trace_id": "", "narrated": False,
                    }

                # Format result with rich detail
                response, rows = self.formatter.format_for_channel(
                    hit_results, hit_dataset, channel,
                    params={"intent_type": "direct_lookup", "route_reason": route_reason},
                    total_count=1,
                )

                result = {
                    "response": response,
                    "rows": rows,
                    "results": hit_results,
                    "dataset_id": hit_dataset,
                    "soql_query": soql,
                    "parsed_params": {"direct_lookup_id": lookup_value, "route_reason": route_reason},
                    "resolved_params": {"direct_lookup_id": lookup_value},
                    "needs_llm": False,
                    "needs_clarification": False,
                    "route_reason": route_reason,
                    "degraded": False, "degraded_hint": "", "total_count": 1,
                    "universe_insights": None,
                    "suggestions": [],
                    "timeout_suggestions": [],
                    "intent_type": "direct_lookup",
                    "followup": False, "trace_id": "", "narrated": False,
                }
                trace_id = self.feedback.log_trace(user_query, channel, result, user_identity=user_identity)
                result["trace_id"] = trace_id
                return result

            except Exception as exc:
                logger.warning("direct_lookup failed for %s: %s", lookup_value, exc)
                # Fall through to normal pipeline

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
