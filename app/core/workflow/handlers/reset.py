"""Reset command handler — ``/reset`` and synonyms.

When the user sends a reset command (``/reset``, ``nueva``, ``limpiar`` …)
this handler clears the conversation history and returns an empty
result with ``route_reason="reset"``. No SECOP IO; no trace logged
(reset is a state-management command, not a query).

Moved here in Paso 7A from the inline branch in
``SecopalWorkflow.run_query``. The 22-line dict literal that the
previous code constructed by hand is replaced by ``empty_result(...)``
— the canonical shape constructor staged in Paso 3. The golden test
``tests/test_orchestrator_golden.py::test_golden_reset`` verifies the
output is contract-equivalent.

Contract
--------
Returns the empty result when ``ctx.chat_id`` is set AND
``ctx.user_query`` matches a reset command. Returns ``None`` to fall
through otherwise. Anonymous calls (no chat_id) cannot reset because
there is no conversation to clear.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from app.core.conversation_store import is_reset_command
from app.core.direct_responses import reset_response
from app.core.workflow.results import empty_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


def handle_reset(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Clear the conversation and return the canonical reset response.

    No-op if there is no chat_id (anonymous calls can't reset) or the
    query is not a reset command.
    """
    if not ctx.chat_id or not is_reset_command(ctx.user_query):
        return None
    deps.conv_store.clear(ctx.chat_id)
    return empty_result(
        response=reset_response(),
        route_reason="reset",
    )
