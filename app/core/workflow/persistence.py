"""Persistence helpers — feedback log + conversation store for run_query.

Why this module exists
----------------------
The current ``orchestrator.py`` makes eight separate ``feedback.log_trace``
calls and four ``conv_store.append_turn`` calls — one per run_query
branch. The same ``referencia_del_proceso or id_contrato`` extraction is
copy-pasted in five places. When the trace schema evolves (new field,
new identifier source), every site has to be updated; in practice
someone always misses one.

These two functions deduplicate the pattern so every branch persists
through the same path. Paso 5 swaps the inline calls for these.

Staging note
------------
Introduced in Paso 3 with full unit-test coverage in
``tests/test_workflow_persistence.py``. ``run_query`` does not consume
them yet — that happens in Paso 5 alongside the handler extractions.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


# How many rows of a result set we surface to the conversation store as
# identifiers — kept at 10 to match the pre-refactor behavior across
# every branch.
_RESULT_IDS_CAP = 10


def extract_result_ids(results: list[dict], *, cap: int = _RESULT_IDS_CAP) -> list[str]:
    """Pull the canonical identifier from each of the first ``cap`` rows.

    Mirrors the inline pattern repeated across run_query:

        str(r.get("referencia_del_proceso") or r.get("id_contrato") or "")

    Empty identifiers are filtered out so the conversation store never
    sees ``""`` as a "result id".
    """
    out: list[str] = []
    for row in results[:cap]:
        rid = str(row.get("referencia_del_proceso") or row.get("id_contrato") or "")
        if rid:
            out.append(rid)
    return out


def persist_trace_and_turn(
    deps: "WorkflowDeps",
    ctx: "HandlerContext",
    result: dict,
) -> dict:
    """Log a feedback trace and (if there's a chat_id) append a conversation turn.

    Mutates ``result`` in place to set ``result["trace_id"]`` and returns
    it for convenience so callers can write ``return persist_trace_and_turn(...)``.

    The trace is always logged. The turn is appended only when the
    request carries a ``chat_id`` — anonymous calls (e.g. one-off API
    requests) get the trace but not a persistent conversation.
    """
    trace_id = deps.feedback.log_trace(
        ctx.user_query,
        ctx.channel,
        result,
        user_identity=ctx.user_identity,
    )
    result["trace_id"] = trace_id

    if ctx.chat_id:
        result_ids = extract_result_ids(result.get("results", []))
        deps.conv_store.append_turn(
            chat_id=ctx.chat_id,
            user_query=ctx.user_query,
            response=result.get("response", ""),
            parsed_params=result.get("parsed_params", {}),
            result_ids=result_ids,
            trace_id=trace_id,
        )

    return result
