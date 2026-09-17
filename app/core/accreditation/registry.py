"""Declared capabilities (plan V2 7): growing the control plane means adding a spec.

Nothing here reads text. The registry answers two questions only: which spec a
capability declares, and which spec — if any — a credential is admissible for.
"""

from __future__ import annotations

from .credential import CapabilityCredential
from .spec import CapabilitySpec, Cardinality, Dimension, Owner

SUPPLIERS = "entity_profile.suppliers"

# The universe a capability serves (plan V2 14). SUPPLIERS ranks contracts.
CONTRACTS_UNIVERSE = "contracts"

_SPECS: tuple[CapabilitySpec, ...] = (
    CapabilitySpec(
        capability=SUPPLIERS,
        owner=Owner.ENTITY_PROFILE,
        focus="suppliers",
        action="ANALYZE",
        aspect="SUPPLIERS",
        universe=CONTRACTS_UNIVERSE,
        required=frozenset({Dimension.BUYER}),
        optional=frozenset({Dimension.TIME, Dimension.RANKING}),
        guarded=frozenset({
            Dimension.ACTOR,
            Dimension.SOURCE,
            Dimension.POLARITY,
            Dimension.CARDINALITY,
        }),
        supported_cardinality=frozenset({Cardinality.NONE, Cardinality.EXACT_5}),
        unsupported=frozenset({
            Dimension.MONEY,
            Dimension.MODALITY,
            Dimension.TOPIC,
            Dimension.GEOGRAPHY,
        }),
    ),
)

_BY_CAPABILITY = {spec.capability: spec for spec in _SPECS}


def capability_spec(capability: object) -> CapabilitySpec | None:
    """The declared spec, or ``None`` when this build cannot serve the capability."""
    return _BY_CAPABILITY.get(capability) if type(capability) is str else None


def spec_for_credential(credential: CapabilityCredential | None) -> CapabilitySpec | None:
    """The spec this credential is admissible for; ``None`` when nothing matches.

    A credential never authorizes a capability on its own: the declared
    action/aspect pair must be the spec's own pair.
    """
    if credential is None:
        return None
    spec = capability_spec(credential.capability)
    if spec is None:
        return None
    return spec if (credential.action, credential.aspect) == (spec.action, spec.aspect) else None


def declared_capabilities() -> tuple[str, ...]:
    return tuple(spec.capability for spec in _SPECS)
