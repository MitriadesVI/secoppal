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

from typing import TYPE_CHECKING

from app.core.workflow.helpers import execute_resolved_params

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


def handle_refinement(
    deps: "WorkflowDeps",
    ctx: "HandlerContext",
    seed_resolved_params: dict,
) -> dict | None:
    """Ejecuta una refinement bar mergeada con contexto previo.

    Toma los params ya resueltos del turno anterior + el filtro recién aplicado,
    construye SoQL y ejecuta sin pasar por el parser. Retorna None si no se
    puede ejecutar (sin scope/topic) para que el caller caiga al flujo normal.

    La ejecución de búsqueda (build → guard → query → format) vive ahora en
    ``execute_resolved_params``; este handler añade el header, los filtros
    contextuales y la persistencia conversacional (trace + turno).
    """
    params = dict(seed_resolved_params)
    # offset/limit deben empezar de cero porque es una nueva búsqueda
    params.pop("offset", None)
    params.setdefault("limit", 50)

    execution = execute_resolved_params(deps, params, channel=ctx.channel)
    if execution is None:
        return None

    results = execution.results
    rows = execution.rows
    response = execution.response
    total = execution.total
    soql = execution.soql
    ds_id = execution.dataset_id
    approx = execution.approximate

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
