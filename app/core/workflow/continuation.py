"""Qué puede continuar el siguiente turno, y quién responde por ello.

Hoy cada camino de continuación redescubre su autoridad mirando hacia atrás:
`radar._last_search_params` busca cualquier turno con alcance o tema,
`suggestion.py` lee `modified_params`, y la paginación confía en que un buffer
con TTL vigente corresponde al último resultado mostrado. Ninguna de esas tres
cosas prueba que el referente sea el resultado que la persona está viendo.

El contrato REFERENCIA el resultado publicado; no lo copia. No contiene otro
Frame, ni params heredables, ni candidatos de Actor: si contuviera copias,
volvería a haber dos versiones del mismo universo y la pregunta «cuál manda»
regresaría por la puerta de atrás.

`VisibleUniverse` del diseño es este mismo objeto, no un segundo.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Mapping


CONTINUATION_SCHEMA_VERSION = 1

CONTINUATION_KEY = "continuation"


class ContinuationError(ValueError):
    """Un contrato ilegible marca continuidad INVÁLIDA, no continuidad libre."""


class ResultKind(str, Enum):
    TABULAR_SEARCH = "TABULAR_SEARCH"
    ENTITY_PROFILE = "ENTITY_PROFILE"
    ACTOR_PROFILE = "ACTOR_PROFILE"
    RADAR = "RADAR"
    CLARIFICATION = "CLARIFICATION"
    OTHER = "OTHER"


class ContinuationOwner(str, Enum):
    TABULAR = "TABULAR"
    ENTITY_PROFILE = "ENTITY_PROFILE"
    ACTOR = "ACTOR"
    RADAR = "RADAR"
    NONE = "NONE"


@dataclass(frozen=True, slots=True)
class TabularGroup:
    """Un grupo paginable: su cursor, su orden y la generación que lo produjo."""

    group: str
    cursor_id: str
    fingerprint: str
    order_contract: str
    generation: str
    expiration: str | None = None

    def to_dict(self) -> dict:
        return {
            "group": self.group, "cursor_id": self.cursor_id,
            "fingerprint": self.fingerprint,
            "order_contract": self.order_contract,
            "generation": self.generation, "expiration": self.expiration,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "TabularGroup":
        if not isinstance(raw, dict):
            raise ContinuationError("tabular group must be an object")
        try:
            return cls(
                group=str(raw["group"]), cursor_id=str(raw["cursor_id"]),
                fingerprint=str(raw["fingerprint"]),
                order_contract=str(raw["order_contract"]),
                generation=str(raw["generation"]),
                expiration=raw.get("expiration"),
            )
        except (KeyError, TypeError) as exc:
            raise ContinuationError(f"malformed tabular group: {exc}") from exc


@dataclass(frozen=True, slots=True)
class ConfirmedProfileRef:
    """Perfil ejecutado: su Frame, su plan y su evidencia, por referencia."""

    frame_ref: tuple[str, int]
    plan_ref: str
    evidence_ref: str

    def to_dict(self) -> dict:
        return {
            "frame_ref": list(self.frame_ref), "plan_ref": self.plan_ref,
            "evidence_ref": self.evidence_ref,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "ConfirmedProfileRef":
        if not isinstance(raw, dict):
            raise ContinuationError("confirmed profile ref must be an object")
        try:
            ref = raw["frame_ref"]
            return cls(
                frame_ref=(str(ref[0]), int(ref[1])),
                plan_ref=str(raw["plan_ref"]),
                evidence_ref=str(raw["evidence_ref"]),
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise ContinuationError(f"malformed confirmed profile ref: {exc}") from exc


@dataclass(frozen=True, slots=True)
class PendingProfileRef:
    """Aclaración de perfil: la pregunta abierta y su base, sin ejecución."""

    pending_ref: str
    base_frame_ref: tuple[str, int] | None = None

    def to_dict(self) -> dict:
        return {
            "pending_ref": self.pending_ref,
            "base_frame_ref": (
                list(self.base_frame_ref) if self.base_frame_ref else None
            ),
        }

    @classmethod
    def from_dict(cls, raw: object) -> "PendingProfileRef":
        if not isinstance(raw, dict):
            raise ContinuationError("pending profile ref must be an object")
        try:
            base = raw.get("base_frame_ref")
            return cls(
                pending_ref=str(raw["pending_ref"]),
                base_frame_ref=(str(base[0]), int(base[1])) if base else None,
            )
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            raise ContinuationError(f"malformed pending profile ref: {exc}") from exc


@dataclass(frozen=True, slots=True)
class DelegatedRef:
    """Actor y Radar conservan su autoridad: aquí sólo viaja su referencia."""

    reference: str

    def to_dict(self) -> dict:
        return {"reference": self.reference}

    @classmethod
    def from_dict(cls, raw: object) -> "DelegatedRef":
        if not isinstance(raw, dict):
            raise ContinuationError("delegated ref must be an object")
        return cls(reference=str(raw.get("reference") or ""))


@dataclass(frozen=True, slots=True)
class ContinuationContract:
    conversation_revision: int
    visible_turn_id: str
    result_id: str
    result_kind: ResultKind
    owner: ContinuationOwner
    tabular_groups: Mapping[str, TabularGroup] | None = None
    confirmed_profile: ConfirmedProfileRef | None = None
    pending_profile: PendingProfileRef | None = None
    delegated: DelegatedRef | None = None
    export_ref: str | None = None
    schema_version: int = CONTINUATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != CONTINUATION_SCHEMA_VERSION:
            raise ContinuationError(
                f"unknown continuation schema: {self.schema_version}",
            )
        targets = tuple(
            item for item in (
                self.tabular_groups, self.confirmed_profile,
                self.pending_profile, self.delegated,
            ) if item
        )
        profile_pair = (self.owner is ContinuationOwner.ENTITY_PROFILE
                        and self.confirmed_profile and self.pending_profile
                        and not self.tabular_groups and not self.delegated)
        if len(targets) > 1 and not profile_pair:
            raise ContinuationError("a contract names exactly one target")
        object.__setattr__(
            self, "tabular_groups",
            dict(self.tabular_groups) if self.tabular_groups else None,
        )

    @property
    def can_paginate(self) -> bool:
        """Sólo un resultado tabular con grupos publica capacidad de paginar."""
        return (
            self.owner is ContinuationOwner.TABULAR
            and bool(self.tabular_groups)
        )

    def to_dict(self) -> dict:
        return {
            "export_ref": self.export_ref,
            "schema_version": self.schema_version,
            "conversation_revision": self.conversation_revision,
            "visible_turn_id": self.visible_turn_id,
            "result_id": self.result_id,
            "result_kind": self.result_kind.value,
            "owner": self.owner.value,
            "tabular_groups": (
                {name: g.to_dict() for name, g in self.tabular_groups.items()}
                if self.tabular_groups else None
            ),
            "confirmed_profile": (
                self.confirmed_profile.to_dict() if self.confirmed_profile else None
            ),
            "pending_profile": (
                self.pending_profile.to_dict() if self.pending_profile else None
            ),
            "delegated": self.delegated.to_dict() if self.delegated else None,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "ContinuationContract":
        if not isinstance(raw, dict):
            raise ContinuationError("continuation must be an object")
        version = raw.get("schema_version")
        if version != CONTINUATION_SCHEMA_VERSION:
            raise ContinuationError(f"unknown continuation schema: {version!r}")
        try:
            groups_raw = raw.get("tabular_groups")
            confirmed_raw = raw.get("confirmed_profile")
            pending_raw = raw.get("pending_profile")
            delegated_raw = raw.get("delegated")
            return cls(
                export_ref=raw.get("export_ref"),
                conversation_revision=int(raw["conversation_revision"]),
                visible_turn_id=str(raw["visible_turn_id"]),
                result_id=str(raw["result_id"]),
                result_kind=ResultKind(raw["result_kind"]),
                owner=ContinuationOwner(raw["owner"]),
                tabular_groups=(
                    {
                        str(name): TabularGroup.from_dict(value)
                        for name, value in groups_raw.items()
                    } if groups_raw else None
                ),
                confirmed_profile=(
                    ConfirmedProfileRef.from_dict(confirmed_raw)
                    if confirmed_raw else None
                ),
                pending_profile=(
                    PendingProfileRef.from_dict(pending_raw) if pending_raw else None
                ),
                delegated=(
                    DelegatedRef.from_dict(delegated_raw) if delegated_raw else None
                ),
            )
        except (AttributeError, KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, ContinuationError):
                raise
            raise ContinuationError(f"malformed continuation: {exc}") from exc


def clarification_contract(
    *, conversation_revision: int, turn_id: str, result_id: str,
    owner: ContinuationOwner, pending: PendingProfileRef | None = None,
    delegated: DelegatedRef | None = None,
) -> ContinuationContract:
    """Una aclaración referencia a su dueño; NO se vuelve un resultado tabular."""
    return ContinuationContract(
        conversation_revision=conversation_revision,
        visible_turn_id=turn_id,
        result_id=result_id,
        result_kind=ResultKind.CLARIFICATION,
        owner=owner,
        pending_profile=pending,
        delegated=delegated,
    )


__all__ = [
    "CONTINUATION_KEY",
    "CONTINUATION_SCHEMA_VERSION",
    "ConfirmedProfileRef",
    "ContinuationContract",
    "ContinuationError",
    "ContinuationOwner",
    "DelegatedRef",
    "PendingProfileRef",
    "ResultKind",
    "TabularGroup",
    "clarification_contract",
]
