"""Which existing owner keeps a turn before Foundation may claim it (ADR-102).

Foundation is not a product router. It runs after the structured handlers and
the Natural Alpha seam, and here it asks the existing owners' own detectors,
over the same deterministic parse the admission already uses, whether one of
them claims the turn. It claims only what all of them decline.
"""

from __future__ import annotations

from app.core.conversation_store import is_followup

FOUNDATION_TURN_MARK = "foundation_v3"


def _parse(workflow, ctx):
    return workflow.deps.query_router.parse(ctx.user_query, today=ctx.today)


def specialized_owner(workflow, ctx, parsed) -> str | None:
    """The first existing owner whose detector claims this text, in runtime order."""
    from app.core.analytics.contract_activity.admission import admit_contract_activity
    from app.core.analytics.contract_activity.models import AdmissionStatus
    from app.core.analytics.registry import REGISTRY
    from app.core.entity_profile.turn import detect_entity_profile_turn

    params = parsed.params
    if params.get("actor_scope") or params.get("actor_pending") or params.get("contratista"):
        return "ACTOR"
    if detect_entity_profile_turn(ctx.user_query, params)[0]:
        return "ENTITY_PROFILE"
    evidence = parsed.semantic_evidence.to_state() if parsed.semantic_evidence else None
    if evidence is not None and admit_contract_activity(
            ctx.user_query, params, today=ctx.today, semantic_evidence=evidence,
    ).status is not AdmissionStatus.HANDOFF:
        return "CONTRACT_ACTIVITY"
    if any(analyzer.detect(ctx.user_query, params) for analyzer in REGISTRY):
        return "ANALYTICS"
    return None


def natural_alpha_owner(observation, binding) -> str | None:
    """A capability or a non-SEARCH job that Natural Alpha already grounded."""
    if binding is not None:
        return "CAPABILITY_BINDING"
    if observation is None or observation.mode != "alpha" or observation.status != "MATCHED":
        return None
    if len(observation.jobs) != 1 or observation.jobs[0].action != "SEARCH":
        return "NATURAL_ALPHA_JOB"
    return None


def decline_reason(workflow, ctx, *, observation=None, binding=None) -> str | None:
    """None when Foundation may claim the turn; otherwise the owner that keeps it."""
    owner = natural_alpha_owner(observation, binding)
    if owner is not None:
        return owner
    return specialized_owner(workflow, ctx, _parse(workflow, ctx))


def last_foundation_turn(workflow, ctx):
    """The previous visible turn when Foundation produced it and it is still followable."""
    if not ctx.chat_id:
        return None
    history = workflow.conv_store.get_history(ctx.chat_id, last_n=1)
    last = history[-1] if history else None
    if last is None or last.outcome_reason != FOUNDATION_TURN_MARK or not is_followup(ctx.user_query, last):
        return None
    return last


def continues_foundation_turn(workflow, ctx) -> bool:
    """A turn that legacy would merge into a Foundation result it cannot read.

    A Foundation turn stores no legacy params, so a legacy refinement of it would
    run the fragment alone and drop the earlier conditions without saying so. A
    complete new search, or a turn another owner claims, is not a continuation.
    """
    if last_foundation_turn(workflow, ctx) is None:
        return False
    from app.core._followup_constants import NEW_SEARCH_VERBS
    from app.core.actor_arbitration import is_complete_buyer_search
    from app.core.followup_engine import TextNormalizer
    from app.core.query_frame import frame_from_params, is_complete_new_search

    parsed = _parse(workflow, ctx)
    if specialized_owner(workflow, ctx, parsed) is not None:
        return False
    normalized = TextNormalizer.strip_noise(ctx.user_query)
    explicit = any(normalized.startswith(verb) and normalized[len(verb):].strip() for verb in NEW_SEARCH_VERBS)
    complete = (explicit or is_complete_new_search(frame_from_params(parsed.params))
                or is_complete_buyer_search(ctx.user_query, parsed.params))
    return not complete
