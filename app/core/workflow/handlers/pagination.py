"""Pagination handler — "muestrame mas" and synonyms.

Two sub-branches share this handler because they're both triggered by
the same predicate (``is_pagination_phrase(user_query)``):

1. **Hit**: history exists for the chat. Re-issue the previous turn's
   SoQL with ``offset += limit`` so the user gets the next page.
   Returns a tabular result with ``route_reason="pagination_more"``.

2. **Miss**: chat has no history. We must NOT fall through to the
   normal flow — a parsed "muestrame mas" with no scope would hit
   WHERE 1=1. Instead return a clarification asking for context.
   ``route_reason="pagination_no_context"``.

Moved here in Paso 7B from the inline branch in
``SecopalWorkflow.run_query``. The hit path uses ``tabular_result`` +
``persist_trace_and_turn`` from the staged modules; the miss path uses
``empty_result``.

Contract
--------
Returns a dict for both sub-branches. Returns ``None`` only when the
predicate doesn't match (no chat_id, or query is not a pagination
phrase) — that signals the caller to keep walking the chain.

If the SoQL execution raises in the hit path, we fall through to the
miss branch's clarification rather than crashing. This mirrors the
pre-refactor behavior (the original code had a bare ``except Exception:
pass`` that effectively did the same.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.conversation_store import is_pagination_phrase
from app.core.direct_responses import pagination_no_history_response
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import _query_first_then_count
from app.core.workflow.persistence import persist_trace_and_turn
from app.core.workflow.results import empty_result, tabular_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


def handle_pagination(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Pagination bypass — next page when there's history, clarification when not."""
    if not ctx.chat_id or not is_pagination_phrase(ctx.user_query):
        return None

    history = deps.conv_store.get_history(ctx.chat_id, last_n=1)
    if history:
        prev_params = dict(history[-1].parsed_params)
        prev_offset = int(prev_params.get("offset", 0))
        prev_limit = int(prev_params.get("limit", 10))
        new_offset = prev_offset + prev_limit
        pag_params = {**prev_params, "offset": new_offset, "limit": prev_limit}
        ds_id = SoQLBuilder.dataset_id_for(pag_params.get("dataset"))
        soql = deps.soql_builder.build(ds_id, pag_params)
        try:
            total, results, _secop_timings, _approx = _query_first_then_count(
                deps.secop_client,
                ds_id,
                deps.soql_builder.build_count(ds_id, pag_params),
                soql,
            )
            response, rows = deps.formatter.format_for_channel(
                results, ds_id, ctx.channel,
                params=pag_params, total_count=total,
            )
            display_from = new_offset + 1
            display_to = new_offset + len(results)
            ordering_label = (
                "de mayor valor"
                if pag_params.get("ordering_signal") == "valor_desc"
                else "más recientes"
            )
            header = (
                f"📄 Resultados {display_from}-{display_to} de {total} "
                f"(manteniendo orden por {ordering_label})\n\n"
            )
            result = tabular_result(
                response=header + response,
                rows=rows,
                results=results,
                dataset_id=ds_id,
                soql_query=soql,
                parsed_params=pag_params,
                resolved_params=pag_params,
                route_reason="pagination_more",
                total_count=total,
                intent_type="pagination_more",
                followup=True,
            )
            return persist_trace_and_turn(deps, ctx, result)
        except Exception:
            # Fall through to the no-history clarification — the previous
            # implementation had an equivalent bare except that swallowed
            # SECOP errors during paginate-more.
            pass

    # No history (or hit branch failed): ask for context, don't WHERE 1=1.
    return empty_result(
        response=pagination_no_history_response(),
        route_reason="pagination_no_context",
        needs_clarification=True,
        intent_type="pagination_more",
    )
