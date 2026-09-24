"""Pre-execution authority mutants for F3 Actor/Buyer product contracts."""
from __future__ import annotations

from app.core import query_router
from app.core.actor_intent import ActorReference
from app.core.actor_name import ActorResolutionStatus, normalize_actor_name
from app.core.actor_resolution import METHOD_EXACT_UNIQUE_ALIAS, apply_actor_identity
from app.core.workflow.actions import actor_resolution


RUNTIME_MUTATIONS = frozenset({
    "force_buyer_as_actor_at_parser",
    "promote_fuzzy_top_at_projection",
})


def install(kind: str, monkeypatch) -> dict[str, int]:
    """Patch the product decision seam before the real workflow executes."""
    hits = {"count": 0}
    if kind == "force_buyer_as_actor_at_parser":
        original = query_router.detect_actor_name

        def force_actor(text, **kwargs):
            if normalize_actor_name(text) == "analiza la alcaldia de barranquilla":
                hits["count"] += 1
                return ActorReference(
                    resolution_method="name_unresolved",
                    observed_name="alcaldia de barranquilla",
                    consumed_spans=("alcaldia de barranquilla",),
                    reason_codes=("ACTOR_ROLE_MARKER",),
                )
            return original(text, **kwargs)

        monkeypatch.setattr(query_router, "detect_actor_name", force_actor)
    elif kind == "promote_fuzzy_top_at_projection":
        original = actor_resolution.project_actor_resolution

        def promote_top(params, result, *, actor_name, original_query, origin):
            if result.status == ActorResolutionStatus.CANDIDATES:
                hits["count"] += 1
                return apply_actor_identity(
                    params, result.candidates[0].identity,
                    method=METHOD_EXACT_UNIQUE_ALIAS, observed_name=actor_name,
                )
            return original(params, result, actor_name=actor_name,
                            original_query=original_query, origin=origin)

        monkeypatch.setattr(actor_resolution, "project_actor_resolution", promote_top)
    else:
        raise ValueError(f"unaccredited runtime mutant {kind!r}")
    return hits
