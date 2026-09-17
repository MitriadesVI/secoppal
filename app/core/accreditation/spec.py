"""Declarative capability spec (plan V2 7): what a capability may receive.

The spec names dimensions, cardinalities and its own universe as data. It holds
no grammar, no phrase list and no paraphrase rule: how a dimension is found is
its authority's business, never the registry's.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class Owner(str, Enum):
    ENTITY_PROFILE = "ENTITY_PROFILE"


class Dimension(str, Enum):
    """Executable dimensions: the unit of authority accounting.

    Every dimension the overreach check must respect (plan V2 6) is named here.
    """

    BUYER = "BUYER"
    TIME = "TIME"
    ACTOR = "ACTOR"
    RANKING = "RANKING"
    SOURCE = "SOURCE"
    CARDINALITY = "CARDINALITY"
    POLARITY = "POLARITY"
    MONEY = "MONEY"
    MODALITY = "MODALITY"
    GEOGRAPHY = "GEOGRAPHY"
    TOPIC = "TOPIC"


class Cardinality(Enum):
    """Cardinality claims the capability can serve.

    ``EXACT_5`` carries the view's own number as its value, so a change in the
    view is a change the drift test can see.
    """

    NONE = None
    EXACT_5 = 5


@dataclass(frozen=True, slots=True)
class CapabilitySpec:
    capability: str
    owner: Owner
    focus: str
    action: str
    aspect: str
    universe: str
    required: frozenset[Dimension]
    optional: frozenset[Dimension]
    supported_cardinality: frozenset[Cardinality]
    unsupported: frozenset[Dimension]

    def __post_init__(self) -> None:
        for name in ("capability", "focus", "action", "aspect", "universe"):
            value = getattr(self, name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"INVALID_SPEC_{name.upper()}")
        if type(self.owner) is not Owner:
            raise ValueError("INVALID_SPEC_OWNER")
        for members in (self.required, self.optional, self.unsupported):
            if type(members) is not frozenset or any(
                type(member) is not Dimension for member in members
            ):
                raise ValueError("INVALID_SPEC_DIMENSION")
        if any(type(cardinal) is not Cardinality for cardinal in self.supported_cardinality):
            raise ValueError("INVALID_SPEC_CARDINALITY")
        if self.required & self.optional:
            raise ValueError("SPEC_DIMENSION_OVERLAP")
        if self.unsupported & (self.required | self.optional):
            raise ValueError("SPEC_UNSUPPORTED_OVERLAP")
        if not self.supported_cardinality:
            raise ValueError("SPEC_CARDINALITY_EMPTY")
