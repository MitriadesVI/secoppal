"""Refinement-bar bypass handler.

The Streamlit refinement bar sends a synthetic ``user_query`` (e.g.
"filtrando por X") together with a fully-resolved param dict carried
over from the previous turn. Routing that synthetic query through the
parser would either misinterpret it as natural language or lose the
prior context.

``handle_refinement`` skips the parser entirely:

1. Take ``seed_resolved_params`` as-is, reset ``offset``, ensure ``limit``.
2. Guard anti-WHERE-1=1: bail if the merged params have no scope/topic.
3. Build SoQL + count, execute, format the response, persist trace and
   conversation turn.

Returns ``None`` when the guard fails, signaling the caller to fall
through to the normal flow.

Moved here verbatim from ``SecopalWorkflow._run_refinement`` in Paso 6B.
``self.X`` references replaced by ``deps.X``; ``user_query``,
``channel``, ``chat_id``, ``user_identity`` now come from
``HandlerContext``. No behavior change — verified by
``tests/test_orchestrator_golden.py::test_golden_refinement_bar``.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import _query_first_then_count

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


logger = logging.getLogger(__name__)


def handle_refinement(
    deps: "WorkflowDeps",
    ctx: "HandlerContext",
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
        soql = deps.soql_builder.build(ds_id, params)
        count_soql = deps.soql_builder.build_count(ds_id, params)
        total, results, _t, approx = _query_first_then_count(
            deps.secop_client, ds_id, count_soql, soql,
            limit=int(params.get("limit", 50) or 50),
        )
    except Exception as exc:
        logger.warning("refinement failed: %s", exc)
        return None

    response, rows = deps.formatter.format_for_channel(
        results, ds_id, ctx.channel,
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

    trace_id = deps.feedback.log_trace(
        ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
    )
    result["trace_id"] = trace_id

    if ctx.chat_id:
        result_ids = [
            rid for rid in (
                str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
                for r in results[:10]
            ) if rid
        ]
        deps.conv_store.append_turn(
            chat_id=ctx.chat_id,
            user_query=ctx.user_query,
            response=result["response"],
            parsed_params=params,
            result_ids=result_ids,
            trace_id=trace_id,
        )
    return result
