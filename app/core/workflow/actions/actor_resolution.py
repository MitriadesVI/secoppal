"""Impure AP3-C orchestration between parse/context and pure QueryPlan."""
from __future__ import annotations

from burr.core import State, action

from app.config import actor_profile_enabled
from app.core.actor_arbitration import (
    detect_bare_actor_probe,
    has_strong_topic_evidence,
    scrub_strong_actor_leaks,
)
from app.core.actor_name import ActorResolutionStatus
from app.core.actor_resolution import (
    METHOD_EXPLICIT_DOCUMENT,
    METHOD_NAME_UNRESOLVED,
    actor_resolution_method,
    project_actor_resolution,
)
from app.data.actor_alias_resolver import ActorNameResolver


@action(reads=["parsed_params", "raw_query"], writes=["parsed_params"])
def resolve_actor_name(state: State, actor_name_resolver: ActorNameResolver) -> State:
    """Resolve strong mentions fully and bare names by exact lookup only."""
    if not actor_profile_enabled():
        return state

    params = dict(state.get("parsed_params", {}) or {})
    raw_query = str(state.get("raw_query", "") or "")
    method = actor_resolution_method(params)

    if method == METHOD_NAME_UNRESOLVED:
        scope = params.get("actor_scope")
        actor_name = scope.get("observed_name") if isinstance(scope, dict) else None
        if not isinstance(actor_name, str) or not actor_name.strip():
            return state
        params = scrub_strong_actor_leaks(params, actor_name)
        # As with AP2's explicit-document door, an implicit dataset must move
        # to contracts, the dataset that publishes contractor documents.
        if not params.get("dataset_explicit"):
            params["dataset"] = "contratos"
        result = actor_name_resolver.resolve(actor_name)
        projected = project_actor_resolution(
            params,
            result,
            actor_name=actor_name,
            original_query=raw_query,
            origin="strong_actor_syntax",
        )
        return state.update(parsed_params=projected)

    # Any already-classified actor (especially an explicit document) bypasses
    # the optional bare-name probe.
    if method:
        if method == METHOD_EXPLICIT_DOCUMENT and not params.get(
            "actor_resolution_method"
        ):
            params["actor_resolution_method"] = METHOD_EXPLICIT_DOCUMENT
            params["actor_resolution_status"] = ActorResolutionStatus.RESOLVED
            return state.update(parsed_params=params)
        return state

    probe = detect_bare_actor_probe(raw_query, params)
    if probe is None:
        return state
    result = actor_name_resolver.resolve_exact(probe.actor_name)
    if result.status in {
        ActorResolutionStatus.UNRESOLVED,
        ActorResolutionStatus.INDEX_UNAVAILABLE,
    }:
        # The probe is optional evidence, not confirmed actor intent.  A miss
        # or unavailable index preserves the exact baseline thematic frame.
        return state

    projected = project_actor_resolution(
        params,
        result,
        actor_name=probe.actor_name,
        original_query=raw_query,
        origin="bare_exact_probe",
    )
    if has_strong_topic_evidence(probe.actor_name, params):
        # Restore baseline topic fields: neither interpretation won.  P17 will
        # ask whether X is the topic or the observed provider.
        for key in ("objeto", "objeto_phrases"):
            if key in params:
                projected[key] = params[key]
        projected["actor_intent_conflict"] = {
            "actor_name": probe.actor_name,
            "resolution_status": result.status,
        }
    return state.update(parsed_params=projected)
