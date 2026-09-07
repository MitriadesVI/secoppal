"""La ÚNICA autoridad del significado institucional confirmado.

Un Frame contiene todo lo necesario para volver a compilar sin releer
``raw_query``, ``parsed_params``, ``resolved_params`` ni un plan anterior. Esa
es su razón de ser: en V1.1 el significado vivía repartido entre un request, un
diccionario de params y un scope, y las tres copias podían decir cosas
distintas sin que nada fallara.

Lo que este modelo hace estructuralmente imposible:

* **Una fecha global.** Cada fuente tiene su propio evento y su propio periodo.
  "Procesos publicados en julio y contratos firmados en agosto" no cabe en un
  escalar, y forzarlo perdía uno de los dos.
* **Un estado global con dos fuentes.** El estado pertenece al esquema que lo
  define; el de procesos no describe contratos.
* **Un tema plano.** El árbol conserva AND, OR, frase y exclusión.
* **Una fuente sin predicados.** ``source_predicates`` tiene exactamente una
  entrada por fuente solicitada, ni una más ni una menos.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from types import MappingProxyType
from typing import Mapping

from app.core.entity_profile.frame_values import (
    UNBOUNDED_PERIOD,
    UNBOUNDED_VALUE,
    Period,
    PredicateOrigin,
    Selection,
    SourceKind,
    SourceOrigin,
    StateExpression,
    TemporalEventKind,
    TopicNode,
    TopicTrue,
    ValueConstraint,
)


FRAME_SCHEMA_VERSION = 1

FOCUS_VALUES = frozenset(
    {"overview", "suppliers", "values", "modalities", "recent", "temporal"},
)


class FrameSchemaError(ValueError):
    """Una versión desconocida se rechaza; no se reinterpreta por aproximación."""


class BuyerScopeKind(str, Enum):
    BUYER_UNIT = "BUYER_UNIT"
    BUYER_GROUP = "BUYER_GROUP"


@dataclass(frozen=True, slots=True)
class BuyerScope:
    """Identidad institucional acreditada por su propia autoridad."""

    kind: BuyerScopeKind
    display_name: str
    canonical_buyers: tuple[str, ...]
    authority: str
    authority_version: str
    group_id: str | None = None

    def __post_init__(self) -> None:
        if not self.canonical_buyers:
            raise ValueError("a buyer scope needs at least one canonical buyer")
        if self.kind is BuyerScopeKind.BUYER_GROUP and not self.group_id:
            raise ValueError("BUYER_GROUP scope requires a group id")

    def fingerprint(self) -> str:
        return f"buyer:{self.kind.value}:{'|'.join(self.canonical_buyers)}"

    def to_dict(self) -> dict:
        return {
            "kind": self.kind.value, "display_name": self.display_name,
            "canonical_buyers": list(self.canonical_buyers),
            "authority": self.authority, "authority_version": self.authority_version,
            "group_id": self.group_id,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "BuyerScope":
        if not isinstance(raw, dict):
            raise FrameSchemaError("buyer_scope must be an object")
        return cls(
            kind=BuyerScopeKind(raw["kind"]),
            display_name=str(raw["display_name"]),
            canonical_buyers=tuple(str(v) for v in raw["canonical_buyers"]),
            authority=str(raw["authority"]),
            authority_version=str(raw["authority_version"]),
            group_id=raw.get("group_id"),
        )


@dataclass(frozen=True, slots=True)
class TemporalPredicate:
    """El hecho y la ventana de UNA fuente, cada uno con su origen."""

    event: TemporalEventKind = TemporalEventKind.NONE
    period: Period = UNBOUNDED_PERIOD
    event_origin: PredicateOrigin = PredicateOrigin.DEFAULT
    period_origin: PredicateOrigin = PredicateOrigin.DEFAULT

    def fingerprint(self) -> str:
        return f"event:{self.event.value}/{self.period.fingerprint()}"

    def to_dict(self) -> dict:
        return {
            "event": self.event.value, "period": self.period.to_dict(),
            "event_origin": self.event_origin.value,
            "period_origin": self.period_origin.value,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "TemporalPredicate":
        if not isinstance(raw, dict):
            raise FrameSchemaError("temporal predicate must be an object")
        return cls(
            event=TemporalEventKind(raw["event"]),
            period=Period.from_dict(raw["period"]),
            event_origin=PredicateOrigin(raw["event_origin"]),
            period_origin=PredicateOrigin(raw["period_origin"]),
        )


@dataclass(frozen=True, slots=True)
class SourcePredicates:
    """Todo lo que filtra UNA fuente. Nada aquí describe a la otra."""

    source: SourceKind
    temporal: TemporalPredicate = TemporalPredicate()
    current_state: StateExpression = StateExpression()
    topic: TopicNode = field(default_factory=TopicTrue)
    modality: Selection = Selection()
    contract_type: Selection = Selection()
    value: ValueConstraint = UNBOUNDED_VALUE

    def fingerprint(self) -> str:
        """La representación normalizada de la que salen SQL y alcance visible."""
        return "|".join((
            f"source:{self.source.value}",
            self.temporal.fingerprint(),
            self.current_state.fingerprint(),
            f"topic:{self.topic.fingerprint()}",
            f"modality:{self.modality.fingerprint()}",
            f"contract_type:{self.contract_type.fingerprint()}",
            self.value.fingerprint(),
        ))

    def to_dict(self) -> dict:
        return {
            "source": self.source.value,
            "temporal": self.temporal.to_dict(),
            "current_state": self.current_state.to_dict(),
            "topic": self.topic.to_dict(),
            "modality": self.modality.to_dict(),
            "contract_type": self.contract_type.to_dict(),
            "value": self.value.to_dict(),
        }

    @classmethod
    def from_dict(cls, raw: object) -> "SourcePredicates":
        if not isinstance(raw, dict):
            raise FrameSchemaError("source predicates must be an object")
        return cls(
            source=SourceKind(raw["source"]),
            temporal=TemporalPredicate.from_dict(raw["temporal"]),
            current_state=StateExpression.from_dict(raw["current_state"]),
            topic=TopicNode.from_dict(raw["topic"]),
            modality=Selection.from_dict(raw["modality"]),
            contract_type=Selection.from_dict(raw["contract_type"]),
            value=ValueConstraint.from_dict(raw["value"]),
        )


@dataclass(frozen=True, slots=True)
class SourceSelection:
    requested: frozenset[SourceKind]
    origin: SourceOrigin

    def __post_init__(self) -> None:
        if not self.requested:
            raise ValueError("a frame always requests at least one source")

    @property
    def ordered(self) -> tuple[SourceKind, ...]:
        return tuple(s for s in SourceKind if s in self.requested)

    def to_dict(self) -> dict:
        return {
            "requested": [s.value for s in self.ordered],
            "origin": self.origin.value,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "SourceSelection":
        if not isinstance(raw, dict):
            raise FrameSchemaError("source_selection must be an object")
        return cls(
            requested=frozenset(SourceKind(v) for v in raw["requested"]),
            origin=SourceOrigin(raw["origin"]),
        )


@dataclass(frozen=True, slots=True)
class FrameProvenance:
    """De dónde salió este significado: claims, turnos y versiones."""

    claim_ids: tuple[str, ...] = ()
    turn_ids: tuple[str, ...] = ()
    authorities: tuple[tuple[str, str], ...] = ()

    def to_dict(self) -> dict:
        return {
            "claim_ids": list(self.claim_ids), "turn_ids": list(self.turn_ids),
            "authorities": [list(item) for item in self.authorities],
        }

    @classmethod
    def from_dict(cls, raw: object) -> "FrameProvenance":
        if not isinstance(raw, dict):
            return cls()
        return cls(
            claim_ids=tuple(str(v) for v in raw.get("claim_ids") or ()),
            turn_ids=tuple(str(v) for v in raw.get("turn_ids") or ()),
            authorities=tuple(
                (str(item[0]), str(item[1])) for item in raw.get("authorities") or ()
            ),
        )


@dataclass(frozen=True, slots=True)
class EntityProfileFrame:
    """El universo confirmado. Nadie por debajo puede reinterpretarlo."""

    frame_id: str
    revision: int
    buyer_scope: BuyerScope
    focus: str
    source_selection: SourceSelection
    source_predicates: Mapping[SourceKind, SourcePredicates]
    provenance: FrameProvenance = FrameProvenance()
    schema_version: int = FRAME_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FRAME_SCHEMA_VERSION:
            raise FrameSchemaError(f"unknown frame schema: {self.schema_version}")
        if self.focus not in FOCUS_VALUES:
            raise ValueError(f"unknown Entity Profile focus: {self.focus}")
        keys = frozenset(self.source_predicates)
        if keys != self.source_selection.requested:
            raise ValueError(
                "source_predicates must hold exactly one entry per requested source",
            )
        for source, predicates in self.source_predicates.items():
            if predicates.source is not source:
                raise ValueError(f"predicates for {source.value} describe another source")
        object.__setattr__(
            self, "source_predicates", MappingProxyType(dict(self.source_predicates)),
        )

    @property
    def sources(self) -> tuple[SourceKind, ...]:
        return self.source_selection.ordered

    def predicates_for(self, source: SourceKind) -> SourcePredicates:
        return self.source_predicates[source]

    def with_revision(self, **changes) -> "EntityProfileFrame":
        """Una revisión nueva; el objeto base nunca se muta."""
        return replace(self, revision=self.revision + 1, **changes)

    def fingerprint(self) -> str:
        return "&".join((
            self.buyer_scope.fingerprint(),
            f"focus:{self.focus}",
            f"origin:{self.source_selection.origin.value}",
            *(self.source_predicates[s].fingerprint() for s in self.sources),
        ))

    def semantic_equal(self, other: "EntityProfileFrame") -> bool:
        """Igualdad de SIGNIFICADO: ids y procedencia no cambian el universo."""
        return (
            self.schema_version == other.schema_version
            and self.fingerprint() == other.fingerprint()
            and self.source_selection.requested == other.source_selection.requested
            and all(
                self.source_predicates[s].temporal.period
                == other.source_predicates[s].temporal.period
                for s in self.sources
            )
        )

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "frame_id": self.frame_id, "revision": self.revision,
            "buyer_scope": self.buyer_scope.to_dict(),
            "focus": self.focus,
            "source_selection": self.source_selection.to_dict(),
            "source_predicates": {
                source.value: self.source_predicates[source].to_dict()
                for source in self.sources
            },
            "provenance": self.provenance.to_dict(),
        }

    @classmethod
    def from_dict(cls, raw: object) -> "EntityProfileFrame":
        if not isinstance(raw, dict):
            raise FrameSchemaError("frame must be an object")
        version = raw.get("schema_version")
        if version != FRAME_SCHEMA_VERSION:
            raise FrameSchemaError(f"unknown frame schema: {version!r}")
        try:
            predicates = {
                SourceKind(key): SourcePredicates.from_dict(value)
                for key, value in raw["source_predicates"].items()
            }
            return cls(
                frame_id=str(raw["frame_id"]), revision=int(raw["revision"]),
                buyer_scope=BuyerScope.from_dict(raw["buyer_scope"]),
                focus=str(raw["focus"]),
                source_selection=SourceSelection.from_dict(raw["source_selection"]),
                source_predicates=predicates,
                provenance=FrameProvenance.from_dict(raw.get("provenance")),
            )
        except (AttributeError, KeyError, TypeError) as exc:
            raise FrameSchemaError(f"malformed frame payload: {exc}") from exc
