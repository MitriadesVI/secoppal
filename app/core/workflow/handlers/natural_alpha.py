"""Dispatcher observation seam and exactly-one trace integration."""
from copy import copy

from app.core.natural_alpha.admission import observe
from app.core.natural_alpha.catalog import LABELS
from app.core.natural_alpha.terminal_publication import mark_terminal_publication
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


def handle_natural_alpha(deps, ctx, provider=None):
    return observe(deps.settings, ctx.user_query, ctx.channel, ctx.user_identity, provider)


def observed_deps(deps, observation):
    if observation is None:
        return deps
    scoped = copy(deps)
    scoped.feedback = _AlphaFeedback(deps.feedback, observation)
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
