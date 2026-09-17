"""The one canonical seam from an Observation to a CapabilityCredential (plan V2 9, C3).

Nothing else in the codebase may turn an observation into authority: the pair
``(action, aspect)`` is resolved by the declared registry, the rollout decides
whether that capability may claim turns at all, and the evidence must ground
against the very raw text of the turn. Any failure answers ``None`` — the turn
stays legacy, never half-credentialed.
"""

from __future__ import annotations

from app.core.natural_alpha.models import Observation

from .credential import CapabilityCredential, from_grounded_job
from .registry import SUPPLIERS, spec_for_action_aspect


def rollout_enabled(settings, capability: str) -> bool:
    """Temporary rollout policy: one explicit line per capability.

    SUPPLIERS reuses ``SECOPPAL_A2_SUPPLIERS_BINDING_ENABLED`` so the same
    decision cannot be expressed twice with two different answers. A second
    capability needs its own line here; no new flag is created before that.
    """
    if capability != SUPPLIERS:
        return False
    return getattr(settings, "secoppal_a2_suppliers_binding_enabled", False) is True


def credential_from_observation(
    observation: Observation | None, raw: str, *, settings,
) -> CapabilityCredential | None:
    """The credential an eligible observation accredits, or ``None``.

    Eligible means: an Alpha-mode observation that matched, carrying exactly one
    grounded job whose pair a declared spec owns, with rollout on. The evidence
    itself must still land inside ``raw``; an interval that does not correspond
    to this raw text is refused, never clipped.
    """
    if observation is None or observation.mode != "alpha" or observation.status != "MATCHED":
        return None
    if len(observation.jobs) != 1:
        return None
    job = observation.jobs[0]
    spec = spec_for_action_aspect(job.action, job.aspect)
    if spec is None or not rollout_enabled(settings, spec.capability):
        return None
    try:
        return from_grounded_job(raw, job, capability=spec.capability)
    except ValueError:
        return None
