"""Resume a pending actor resolution without losing the original QueryFrame."""
from __future__ import annotations

import re
from copy import copy
from dataclasses import replace
from typing import TYPE_CHECKING

from app.config import actor_profile_enabled
from app.core.actor_resolution import (
    ACTOR_PENDING_VERSION,
    METHOD_USER_SELECTED_CANDIDATE,
    apply_actor_identity,
    candidate_from_payload,
)
from app.core.workflow.results import empty_result

if TYPE_CHECKING:
    from app.core.workflow.context import HandlerContext
    from app.core.workflow.deps import WorkflowDeps


ACTOR_SELECTION_TTL_MINUTES = 30.0
_SELECTION_RE = re.compile(r"^(?:opci[oó]n\s+)?(10|[1-9])$", re.IGNORECASE)


class _SelectedFrameRouter:
    """One-request router that preserves parser metadata but supplies the frame."""

    def __init__(self, delegate, params: dict):
        self._delegate = delegate
        self._params = dict(params)

    def parse(self, user_query: str):
        parsed = self._delegate.parse(user_query)
        return replace(
            parsed,
            params=dict(self._params),
            needs_llm=False,
            route_reason="actor_selection_seed",
        )


class _SelectionConversationStore:
    """Hide pending history from merge while persisting the actual reply text."""

    def __init__(self, delegate, selection_query: str):
        self._delegate = delegate
        self._selection_query = selection_query

    def get_history(self, chat_id: str, last_n: int = 5):
        return []

    def append_turn(self, *args, **kwargs):
        kwargs["user_query"] = self._selection_query
        return self._delegate.append_turn(*args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


class _SelectionFeedback:
    """Keep trace input as the user's selection, not the replayed original text."""

    def __init__(self, delegate, selection_query: str):
        self._delegate = delegate
        self._selection_query = selection_query

    def log_trace(self, user_query, *args, **kwargs):
        return self._delegate.log_trace(self._selection_query, *args, **kwargs)

    def __getattr__(self, name: str):
        return getattr(self._delegate, name)


def handle_actor_selection(deps: "WorkflowDeps", ctx: "HandlerContext") -> dict | None:
    """Apply a numbered candidate to the pending frame, then resume planning."""
    if not actor_profile_enabled() or not ctx.chat_id:
        return None
    match = _SELECTION_RE.fullmatch(ctx.user_query.strip())
    if match is None:
        return None

    history = deps.conv_store.get_history(ctx.chat_id, last_n=1)
    if not history:
        return None
    last_turn = history[-1]
    pending = last_turn.parsed_params.get("actor_pending")
    if not isinstance(pending, dict):
        return None
    if last_turn.age_minutes() > ACTOR_SELECTION_TTL_MINUTES:
        return empty_result(
            response=(
                "Esa selección de actor ya venció. Repite la consulta por nombre "
                "para generar opciones actuales."
            ),
            route_reason="actor_selection_expired",
            needs_clarification=True,
        )
    if pending.get("version") != ACTOR_PENDING_VERSION:
        return empty_result(
            response="No puedo reutilizar esas opciones; repite la consulta por nombre.",
            route_reason="actor_selection_incompatible",
            needs_clarification=True,
        )

    candidates = pending.get("candidates")
    if not isinstance(candidates, list):
        return None
    selected_rank = int(match.group(1))
    selected = next(
        (
            candidate for candidate in candidates
            if isinstance(candidate, dict) and candidate.get("rank") == selected_rank
        ),
        None,
    )
    if selected is None:
        return empty_result(
            response=f"La opción {selected_rank} no está en la lista. Elige un número disponible.",
            route_reason="actor_selection_invalid",
            needs_clarification=True,
        )

    original_params = pending.get("original_params")
    if not isinstance(original_params, dict):
        return empty_result(
            response="No pude recuperar la consulta original; por favor repítela.",
            route_reason="actor_selection_incompatible",
            needs_clarification=True,
        )
    try:
        identity, observed_name = candidate_from_payload(selected)
        selected_params = apply_actor_identity(
            original_params,
            identity,
            method=METHOD_USER_SELECTED_CANDIDATE,
            observed_name=observed_name,
        )
    except (KeyError, TypeError, ValueError):
        return empty_result(
            response="La opción guardada no contiene una identidad válida; repite la consulta.",
            route_reason="actor_selection_incompatible",
            needs_clarification=True,
        )

    if pending.get("origin") == "bare_exact_probe":
        selected_params.pop("objeto", None)
        selected_params.pop("objeto_phrases", None)
    selected_params["actor_selection_rank"] = selected_rank

    from app.core.workflow.handlers.normal_flow import handle_normal_flow

    original_query = str(pending.get("original_query") or last_turn.user_query)
    selected_deps = copy(deps)
    selected_deps.query_router = _SelectedFrameRouter(deps.query_router, selected_params)
    selected_deps.conv_store = _SelectionConversationStore(
        deps.conv_store, ctx.user_query,
    )
    selected_deps.feedback = _SelectionFeedback(deps.feedback, ctx.user_query)
    selection_ctx = replace(ctx, user_query=original_query)
    result = handle_normal_flow(selected_deps, selection_ctx)
    result["followup"] = True
    return result
