"""Registry of dimension authorities (spine v5, C0 §5.5.3): one line per dimension, maturity-gated.

EMPTY in C1.1 and not wired into admission yet: ``foundation_admission`` does not import it.  The
core adds a line per dimension in its E steps (``SHADOW`` -> ``ACTIVE``); ``ACTIVE`` deletes the
in-line branch of that dimension in the same commit (invariant 5).

- ``DECLARED``: the entry exists; nothing is computed.
- ``SHADOW``: the verdict is computed and logged, the current host branch still decides.
- ``ACTIVE``: the verdict decides.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from app.core.foundation_authority_contract import DimensionAuthority
from app.core.interpretation_contract import ContractError
from app.core.semantic_dimensions import SemanticDimension


class Maturity(str, Enum):
    DECLARED = "DECLARED"
    SHADOW = "SHADOW"
    ACTIVE = "ACTIVE"


@dataclass(frozen=True, slots=True)
class AuthorityDeps:
    """Read-only indexes injected into a factory; an authority never fetches its own."""

    entity_resolver: object | None = None
    actor_name_resolver: object | None = None
    territories: object | None = None
    catalogs: object | None = None


@dataclass(frozen=True, slots=True)
class AuthorityEntry:
    dimension: SemanticDimension
    factory: Callable[[AuthorityDeps], DimensionAuthority]
    maturity: Maturity

    def __post_init__(self) -> None:
        if type(self.dimension) is not SemanticDimension:
            raise ContractError("INVALID_REGISTRY_DIMENSION")
        if not callable(self.factory):
            raise ContractError("INVALID_REGISTRY_FACTORY")
        if type(self.maturity) is not Maturity:
            raise ContractError("INVALID_REGISTRY_MATURITY")


REGISTRY: tuple[AuthorityEntry, ...] = ()
