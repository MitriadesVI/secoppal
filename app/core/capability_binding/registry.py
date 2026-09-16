"""Host-owned registry: which Natural Job may open which existing capability.

The model contributes exactly one thing, the ``(action, aspect)`` of a single
grounded job. Evidence, text and every executable parameter stay with the
deterministic authorities behind each capability's input contract.

Deliberately NOT bound yet:

- TOP_ENTITIES → analytics ranking. In A2 X0, Alpha labelled supplier questions
  ("a quién le contrata más la alcaldía de Soledad") as TOP_ENTITIES; binding it
  would send them to an entity ranking. The label alone does not say which
  ranking the person wants.
- VALUES/TOTAL_VALUE, MODALITIES, OVERVIEW, RECENT, TEMPORAL: promoted one by one
  after SUPPLIERS survives dogfood.
"""
from __future__ import annotations

from types import MappingProxyType

SUPPLIERS = "entity_profile.suppliers"

_BINDINGS = MappingProxyType({("ANALYZE", "SUPPLIERS"): SUPPLIERS})


def capability_for(observation, settings) -> str | None:
    """The capability a single Alpha-mode job opens, or ``None``.

    Shadow mode never governs a route, and a multi-job or ungrounded proposal
    binds nothing.
    """
    if getattr(settings, "secoppal_a2_suppliers_binding_enabled", False) is not True:
        return None
    if observation is None or observation.mode != "alpha" or observation.status != "MATCHED":
        return None
    if len(observation.jobs) != 1:
        return None
    job = observation.jobs[0]
    return _BINDINGS.get((job.action, job.aspect))
