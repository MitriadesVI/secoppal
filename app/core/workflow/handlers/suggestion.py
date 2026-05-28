"""Suggestion-selection handler — user picks "1", "2" or "3" from chips.

When the previous turn surfaced numbered suggestions and the user
replies with one of those digits, this handler skips the parser and
re-executes SECOP using the suggestion's pre-built ``modified_params``.

Three sub-branches:

1. **Hit** — last_suggestions exists, index in range, SECOP call succeeds.
   Returns a tabular result with ``route_reason="suggestion"``.
2. **Error** — SECOP call raises. Returns ``empty_result`` with
   ``route_reason="suggestion_error"``.
3. **Invalid** — last_suggestions missing or index out of range.
   Returns ``empty_result`` with ``route_reason="suggestion_invalid"``.

Moved here in Paso 7C from the inline branch in
``SecopalWorkflow.run_query``.

Quirk preserved on purpose
--------------------------
The legacy success path populates ``formatted_response`` /
``formatted_rows`` keys (matching the Burr-internal state names) rather
than the canonical ``response`` / ``rows``, then prepends the suggestion
header to ``result["response"]`` — which reads ``""`` because the key
isn't present. Net effect: the final ``response`` is JUST the
suggestion header, while the actual formatted text lives in
``formatted_response``. This is anomalous vs. every other handler but
without a golden test for the suggestion path we can't safely
canonicalize the shape — preserved verbatim. See TODO below.

TODO(refactor): write a golden test for suggestion-selection that
seeds last_suggestions in conv_store, then canonicalize response/rows
in a separate commit so this handler matches the rest. Issue tracked
in the workflow refactor backlog.
"""
from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from app.core.direct_responses import (
    suggestion_invalid_response,
    suggestion_selection_header,
)
from app.core.observer import observe as observe_universe_fn
from app.core.soql_builder import SoQLBuilder
from app.core.suggester import generate_suggestions
from app.core.workflow.helpers import _query_first_then_count
from app.core.workflow.results import empty_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


def handle_suggestion_selection(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Run a stored suggestion when the user replies "1", "2" or "3".

    Returns ``None`` to fall through when there is no chat_id or the
    query is not a numeric suggestion pick.
    """
    if not ctx.chat_id:
        return None
    q = ctx.user_query.strip()
    if q not in ("1", "2", "3"):
        return None

    last_suggestions = deps.conv_store.get_last_suggestions(ctx.chat_id)
    if not last_suggestions or len(last_suggestions) < int(q):
        # Hay sugerencia pero el numero no coincide
        return empty_result(
            response=suggestion_invalid_response(3, q),
            route_reason="suggestion_invalid",
        )

    idx = int(q) - 1
    sug = last_suggestions[idx]
    sug_params = sug.get("modified_params", {})
    suggestion_label = sug.get("label", "")
    sug_dataset_id = SoQLBuilder.dataset_id_for(sug_params.get("dataset"))

    # Build SoQL and execute directly
    soql = deps.soql_builder.build(sug_dataset_id, sug_params)
    try:
        count_soql = deps.soql_builder.build_count(sug_dataset_id, sug_params)
        total, results, _secop_timings, _approx = _query_first_then_count(
            deps.secop_client,
            sug_dataset_id,
            count_soql,
            soql,
        )
        # Generate insights + suggestions for this new result
        ui = observe_universe_fn(
            results=results, total_count=total,
            dataset_id=sug_dataset_id, params=sug_params,
            secop_client=deps.secop_client,
            soql_builder=deps.soql_builder,
        )
        new_suggestions = generate_suggestions(
            params=sug_params, universe_insights=ui,
            total_count=total, rows=results, dataset_id=sug_dataset_id,
            intent_type=sug_params.get("intent_type", ""),
        )
        response, rows = deps.formatter.format_for_channel(
            results, sug_dataset_id, ctx.channel,
            params=sug_params, total_count=total,
            universe_insights=ui,
            suggestions=[
                dataclasses.asdict(s) if hasattr(s, '__dataclass_fields__') else s
                for s in new_suggestions
            ],
        )
        # NOTE(refactor): legacy shape — uses formatted_response/formatted_rows
        # instead of canonical response/rows. Preserved verbatim until a
        # golden test exists for the suggestion path.
        result = {
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
        return empty_result(
            response=f"No pude ejecutar esa sugerencia en este momento: {exc}",
            route_reason="suggestion_error",
        )

    # ── Post-execution: trace, header, persist (verbatim from run_query) ──
    raw_results = result.get("results", [])
    result_ids = [
        rid for rid in (
            str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")
            for r in raw_results[:10]
        ) if rid
    ]
    trace_id = deps.feedback.log_trace(
        ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
    )
    result["trace_id"] = trace_id

    # Quirk: result.get("response", "") returns "" because key isn't set,
    # so result["response"] becomes JUST the suggestion_selection_header.
    # See module docstring for why this is preserved.
    result["response"] = suggestion_selection_header(suggestion_label) + result.get("response", "")

    if ctx.chat_id:
        deps.conv_store.append_turn(
            chat_id=ctx.chat_id,
            user_query=ctx.user_query,
            response=result["response"],
            parsed_params=result.get("resolved_params", {}),
            result_ids=result_ids,
            trace_id=trace_id,
            suggestions=[
                dataclasses.asdict(s) if hasattr(s, '__dataclass_fields__') else s
                for s in result.get("suggestions", [])
            ],
        )
    return result
