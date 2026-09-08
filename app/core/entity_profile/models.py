"""Stable, graph-ready value types for institutional Entity Analysis V1."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


PROCESS_DATASET = "p6dx-8zbt"
CONTRACT_DATASET = "jbjy-vk9h"


class ScopeType(str, Enum):
    BUYER_UNIT = "BUYER_UNIT"
    BUYER_GROUP = "BUYER_GROUP"


class SectionStatus(str, Enum):
    SUPPORTED = "SUPPORTED"
    PARTIAL = "PARTIAL"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class TerritoryContext:
    """Metadata only; it never contributes predicates to a SECOP query."""

    divipola: str
    municipality: str
    department: str
    macro_region: str
    institution_id: str
    institutional_family: str
    knowledge_status: str
    provenance: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EntityProfileScope:
    """The small execution frame owned by Entity Analysis, not a QueryFrame copy."""

    scope_type: ScopeType
    buyer_units: tuple[str, ...]
    buyer_group_id: str | None = None
    territory_context: tuple[TerritoryContext, ...] = ()
    time_from: str | None = None
    time_to: str | None = None
    modality: tuple[str, ...] = ()
    topic: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.buyer_units or len(set(self.buyer_units)) != len(self.buyer_units):
            raise ValueError("EntityProfileScope requires unique BuyerUnits")
        if self.scope_type is ScopeType.BUYER_UNIT and len(self.buyer_units) != 1:
            raise ValueError("BUYER_UNIT scope requires exactly one BuyerUnit")
        if self.scope_type is ScopeType.BUYER_GROUP and not self.buyer_group_id:
            raise ValueError("BUYER_GROUP scope requires buyer_group_id")


@dataclass(frozen=True, slots=True)
class InstitutionReference:
    institution_id: str
    buyer_canonical: str
    territory_divipola: str
    knowledge_status: str
    provenance: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class EntityProfileIdentity:
    display_name: str
    canonical_buyers: tuple[str, ...]
    buyer_group_id: str | None
    institution_refs: tuple[InstitutionReference, ...]
    resolution_authority: str


@dataclass(frozen=True, slots=True)
class EntityProfileFact:
    key: str
    value: Any
    source: str
    unit: str | None = None


@dataclass(frozen=True, slots=True)
class SupplierMetric:
    actor_identity: str
    document_type: str
    document: str
    observed_name: str
    contract_count: int | None
    total_contract_value: float | None
    total_contract_value_status: str = "VALID"


@dataclass(frozen=True, slots=True)
class PeriodMetric:
    period: str
    process_count: int | None
    contract_count: int | None
    contract_value: float | None


@dataclass(frozen=True, slots=True)
class RecentRecord:
    dataset_id: str
    stable_identity: str | None
    identity_status: str
    title: str
    date: str | None
    value: float | None
    url: str | None


@dataclass(frozen=True, slots=True)
class EntityProfileSection:
    status: SectionStatus
    source: tuple[str, ...]
    coverage: tuple[tuple[str, Any], ...] = ()
    facts: tuple[EntityProfileFact, ...] = ()
    warnings: tuple[str, ...] = ()
    attempted: bool = True

    def fact(self, key: str) -> Any:
        for fact in self.facts:
            if fact.key == key:
                return fact.value
        return None


@dataclass(frozen=True, slots=True)
class SourceCall:
    call_id: str
    aspect: str
    dataset_id: str
    operation: str
    status: str
    rows_received: int
    latency_ms: float
    soql: str
    error_type: str | None = None
    attempted: bool = True


@dataclass(frozen=True, slots=True)
class SourceCalls:
    calls: tuple[SourceCall, ...]
    total_latency_ms: float

    @property
    def calls_total(self) -> int:
        return sum(call.attempted for call in self.calls)

    @property
    def calls_by_aspect(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for call in self.calls:
            if not call.attempted:
                continue
            counts[call.aspect] = counts.get(call.aspect, 0) + 1
        return counts

    @property
    def latency_by_aspect(self) -> dict[str, float]:
        totals: dict[str, float] = {}
        for call in self.calls:
            if not call.attempted:
                continue
            totals[call.aspect] = totals.get(call.aspect, 0.0) + call.latency_ms
        return totals


@dataclass(frozen=True, slots=True)
class EntityProfile:
    scope: EntityProfileScope
    identity: EntityProfileIdentity
    activity: EntityProfileSection
    values: EntityProfileSection
    modalities: EntityProfileSection
    suppliers: EntityProfileSection
    temporal: EntityProfileSection
    recent: EntityProfileSection
    source_calls: SourceCalls
    warnings: tuple[str, ...] = ()
    focus: str = "overview"
    # El plan que produjo este perfil. Es lo que permite que el render hable de
    # lo EJECUTADO y no de lo que el foco sugiere; se declara ``Any`` para no
    # invertir la dependencia (``execution_plan`` importa estos modelos).
    plan: Any = None

    @property
    def partial(self) -> bool:
        sections = (
            self.activity,
            self.values,
            self.modalities,
            self.suppliers,
            self.temporal,
            self.recent,
        )
        return any(
            section.attempted and section.status is not SectionStatus.SUPPORTED
            for section in sections
        )

    @property
    def failed_sections(self) -> tuple[str, ...]:
        named = (
            ("activity", self.activity),
            ("values", self.values),
            ("modalities", self.modalities),
            ("suppliers", self.suppliers),
            ("temporal", self.temporal),
            ("recent", self.recent),
        )
        return tuple(
            name
            for name, section in named
            if section.attempted and section.status is not SectionStatus.SUPPORTED
        )


def unavailable_section(*, attempted: bool = False) -> EntityProfileSection:
    reason = "NOT_REQUESTED" if not attempted else "SOURCE_UNAVAILABLE"
    return EntityProfileSection(
        status=SectionStatus.UNAVAILABLE,
        source=(),
        coverage=(("reason", reason),),
        attempted=attempted,
    )
