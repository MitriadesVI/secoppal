"""Dispatcher observation seam and exactly-one trace integration."""
from copy import copy

from app.core.direct_lookup import detect_direct_lookup
from app.core.natural_alpha.admission import observe
from app.core.natural_alpha.catalog import LABELS
from app.core.natural_alpha.terminal_publication import mark_terminal_publication
from app.core.natural_alpha.turn_receipt import note_seam_reached
from app.core.radar.commands import parse_radar_command
from app.core.workflow.admission_rules import InputShape, input_shape
from app.core.workflow.results import empty_result


class _AlphaFeedback:
    def __init__(self, delegate, observation):
        self._delegate = delegate
        self._observation = observation

    def log_trace(self, user_query, channel, workflow_result, **kwargs):
        attach_observation(workflow_result, self._observation)
        return self._delegate.log_trace(user_query, channel, workflow_result, **kwargs)

    def __getattr__(self, name):
        return getattr(self._delegate, name)


def attach_observation(result, observation):
    result.update(observation.metadata())
    result["natural_alpha_legacy_route_reason"] = (
        "" if observation.terminal else result.get("route_reason", "")
    )


def observe_turn(deps, ctx, provider=None):
    """The turn's one observation. Witnessing belongs to the seam, not here."""
    return observe(deps.settings, ctx.user_query, ctx.channel, ctx.user_identity, provider)


def note_seam(deps):
    """The turn receipt's only seam witness."""
    note_seam_reached(deps.feedback)


def observes_turn(deps, ctx, seed_resolved_params) -> bool:
    """True only for plain text turns no structured control can pre-claim (plan V2 8).

    Reset, refinement, pagination, ordinal and affirmative inputs are already
    classified by the admission's own shape rule; a radar command and a direct
    lookup are recognised by the very detectors their handlers use, so a turn the
    fast paths can claim is never observed before them and still costs zero
    provider calls. Exact candidate names and lookups that later fall through are
    observed at the seam instead, where the chain has already declined them.
    """
    if ctx.profile_action_selection is not None or ctx.actor_candidate_selection is not None:
        return False
    if input_shape(
        ctx.user_query, seed_resolved_params=seed_resolved_params,
    ) is not InputShape.TEXT:
        return False
    if ctx.chat_id and deps.radar_store is not None and parse_radar_command(ctx.user_query):
        return False
    return detect_direct_lookup(ctx.user_query) is None


def pre_observe(deps, ctx, seed_resolved_params, provider=None):
    """One observation before admission; the dispatcher consumes it, never repeats it.

    With the Natural Alpha flag off, an ineligible identity, or an input shape
    the structured controls can pre-claim, this costs zero provider calls:
    ``observe`` returns ``None`` before building a provider.
    """
    if not observes_turn(deps, ctx, seed_resolved_params):
        return None
    return observe_turn(deps, ctx, provider)


def observed_deps(deps, observation, capability_binding=None):
    if observation is None:
        return deps
    scoped = copy(deps)
    scoped.feedback = _AlphaFeedback(deps.feedback, observation)
    scoped.capability_binding = capability_binding
    return scoped


def multi_job_result(deps, ctx, observation):
    labels = "\n".join(f"{index}. {LABELS[job.aspect]}."
                       for index, job in enumerate(observation.jobs, 1))
    result = empty_result(
        response=("Alfa detectó varias cosas que quieres hacer:\n" + labels
                  + "\n\nPor ahora hazlas por pasos para no mezclar resultados."),
        route_reason="natural_alpha_multi_job_stepwise",
        needs_clarification=True,
    )
    result["_continuation_owner"] = "NONE"
    attach_observation(result, observation)
    result["trace_id"] = deps.feedback.log_trace(
        ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity,
    )
    if ctx.publication_required:
        mark_terminal_publication(result)
    # No pending plan or legacy conversation params. The coordinator consumes
    # the private marker after this one trace and before any public projection.
    return result
