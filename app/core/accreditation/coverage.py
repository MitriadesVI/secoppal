"""Dimension → authority coverage (plan V2 7, C4A.1).

EVERY_SPEC_DIMENSION_HAS_AN_AUTHORITY: a declared dimension without a consumer is
a dimension that silently disappears. This map is the contract the test enforces;
adding a dimension to the spec without naming its authority fails the build.
"""

from __future__ import annotations

from app.core.accreditation.spec import CapabilitySpec, Dimension

# The consumer of each dimension for the declared capabilities. The residual is
# the consumer of TOPIC: it is the only place where material text without an
# authority can be seen, and it stops the turn instead of executing.
AUTHORITY_BY_DIMENSION: dict[str, str] = {
    Dimension.BUYER.value: "buyer_authority",
    Dimension.TIME.value: "time_authority",
    Dimension.RANKING.value: "ranking_authority",
    Dimension.ACTOR.value: "actor_authority + actor_exact_guard",
    Dimension.SOURCE.value: "source_authority",
    Dimension.POLARITY.value: "supplier_guard",
    Dimension.CARDINALITY.value: "supplier_guard",
    Dimension.MODALITY.value: "modality_guard",
    Dimension.MONEY.value: "money_guard",
    Dimension.GEOGRAPHY.value: "geography_guard",
    Dimension.TOPIC.value: "semantic residual",
}


def declared_dimensions(spec: CapabilitySpec) -> frozenset[str]:
    return frozenset(
        dimension.value
        for dimension in (spec.required | spec.optional | spec.guarded | spec.unsupported)
    )


def uncovered_dimensions(spec: CapabilitySpec) -> frozenset[str]:
    """Dimensions the spec declares and no authority consumes."""
    return frozenset(
        name for name in declared_dimensions(spec)
        if not AUTHORITY_BY_DIMENSION.get(name)
    )


def unknown_authority_dimensions() -> frozenset[str]:
    """Authorities named for dimensions no spec can declare (the map drifted)."""
    known = {dimension.value for dimension in Dimension}
    return frozenset(name for name in AUTHORITY_BY_DIMENSION if name not in known)
