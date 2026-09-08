"""Lo que la conversación del perfil guarda entre turnos, en V1.2.

Reemplaza a `conversation_state.py`, que guardaba dos `EntityProfileSemanticRequest`.
La diferencia no es de nombres: allí el «pendiente» era otra petición completa, y
por tanto otra copia del universo que podía discrepar de la confirmada. Aquí:

    confirmed_frame   el universo que la persona pidió Y el sistema pudo autorizar
    pending           el PARCHE que provocó una aclaración, sobre una revisión
                      concreta, más las alternativas que puede completarlo

Un pendiente es una lista de operaciones, no un Frame. Eso es lo que impide que
una aclaración se convierta en un universo por el hecho de existir: para
ejecutarlo hay que aplicarlo sobre su base y volver a compilar.

El esquema va versionado y se lee ESTRICTAMENTE: una versión desconocida, una
referencia rota o una forma incompleta invalidan el estado entero en vez de
adivinar la mitad que se entiende.
"""
from __future__ import annotations

from dataclasses import dataclass
import uuid

from app.core.entity_profile.frame import EntityProfileFrame, FrameSchemaError
from app.core.entity_profile.patch import EntityProfilePatch, PatchOp
from app.core.entity_profile.patch_codec import (
    operation_from_dict,
    operation_to_dict,
    patch_from_dict,
    patch_to_dict,
)


PROFILE_STATE_VERSION = 1

# La clave del sobre publicado. No viaja en `parsed_params`: los params
# materiales son de la ruta legacy y no adquieren autoridad sobre el Frame.
PROFILE_STATE_KEY = "profile_state"


class ProfileStateError(ValueError):
    """Un estado que no se entiende NO se reinterpreta como uno vacío útil."""


@dataclass(frozen=True, slots=True)
class PendingIssue:
    code: str
    detail: str = ""

    def to_dict(self) -> dict:
        return {"code": self.code, "detail": self.detail}

    @classmethod
    def from_dict(cls, raw: object) -> "PendingIssue":
        if not isinstance(raw, dict):
            raise ProfileStateError("issue must be an object")
        return cls(str(raw["code"]), str(raw.get("detail") or ""))


@dataclass(frozen=True, slots=True)
class Alternative:
    """Una salida ofrecida: su etiqueta y las operaciones que la completan."""

    alternative_id: str
    label: str
    completion_operations: tuple[PatchOp, ...] = ()

    def to_dict(self) -> dict:
        return {
            "alternative_id": self.alternative_id,
            "label": self.label,
            "completion_operations": [
                operation_to_dict(op) for op in self.completion_operations
            ],
        }

    @classmethod
    def from_dict(cls, raw: object) -> "Alternative":
        if not isinstance(raw, dict):
            raise ProfileStateError("alternative must be an object")
        try:
            return cls(
                alternative_id=str(raw["alternative_id"]),
                label=str(raw["label"]),
                completion_operations=tuple(
                    operation_from_dict(item)
                    for item in raw.get("completion_operations") or ()
                ),
            )
        except (FrameSchemaError, KeyError, TypeError, ValueError) as exc:
            raise ProfileStateError(f"malformed alternative: {exc}") from exc


@dataclass(frozen=True, slots=True)
class PendingProfile:
    """La pregunta abierta: qué se pidió cambiar y sobre qué revisión."""

    pending_id: str
    patch: EntityProfilePatch
    question: str
    base_frame_ref: tuple[str, int] | None = None
    issues: tuple[PendingIssue, ...] = ()
    alternatives: tuple[Alternative, ...] = ()

    def alternative(self, alternative_id: str) -> Alternative | None:
        for item in self.alternatives:
            if item.alternative_id == alternative_id:
                return item
        return None

    def to_dict(self) -> dict:
        return {
            "pending_id": self.pending_id,
            "patch": patch_to_dict(self.patch),
            "question": self.question,
            "base_frame_ref": (
                list(self.base_frame_ref) if self.base_frame_ref else None
            ),
            "issues": [issue.to_dict() for issue in self.issues],
            "alternatives": [alt.to_dict() for alt in self.alternatives],
        }

    @classmethod
    def from_dict(cls, raw: object) -> "PendingProfile":
        if not isinstance(raw, dict):
            raise ProfileStateError("pending must be an object")
        try:
            base_ref = raw.get("base_frame_ref")
            return cls(
                pending_id=str(raw["pending_id"]),
                patch=patch_from_dict(raw["patch"]),
                question=str(raw.get("question") or ""),
                base_frame_ref=(
                    (str(base_ref[0]), int(base_ref[1])) if base_ref else None
                ),
                issues=tuple(
                    PendingIssue.from_dict(item) for item in raw.get("issues") or ()
                ),
                alternatives=tuple(
                    Alternative.from_dict(item)
                    for item in raw.get("alternatives") or ()
                ),
            )
        except (FrameSchemaError, IndexError, KeyError, TypeError, ValueError) as exc:
            raise ProfileStateError(f"malformed pending: {exc}") from exc


@dataclass(frozen=True, slots=True)
class ProfileConversationState:
    confirmed_frame: EntityProfileFrame | None = None
    pending: PendingProfile | None = None
    schema_version: int = PROFILE_STATE_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != PROFILE_STATE_VERSION:
            raise ProfileStateError(
                f"unknown profile state schema: {self.schema_version}",
            )
        # Un pendiente que dice basarse en otra revisión que la confirmada NO
        # se «reancla»: aplicarlo sobre una base distinta cambiaría el universo
        # sin que nadie lo pidiera.
        if (
            self.pending is not None
            and self.pending.base_frame_ref is not None
            and self.confirmed_frame is not None
            and self.pending.base_frame_ref
            != (self.confirmed_frame.frame_id, self.confirmed_frame.revision)
        ):
            raise ProfileStateError("pending is anchored to a foreign revision")

    @property
    def has_pending(self) -> bool:
        return self.pending is not None

    def confirm(self, frame: EntityProfileFrame) -> "ProfileConversationState":
        """Un turno autorizado confirma su Frame y cierra lo pendiente."""
        return ProfileConversationState(confirmed_frame=frame)

    def hold(self, pending: PendingProfile) -> "ProfileConversationState":
        """Una aclaración retiene su parche SIN tocar el Frame confirmado."""
        return ProfileConversationState(
            confirmed_frame=self.confirmed_frame, pending=pending,
        )

    def cancel_pending(self) -> "ProfileConversationState":
        return ProfileConversationState(confirmed_frame=self.confirmed_frame)

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "confirmed_frame": (
                self.confirmed_frame.to_dict() if self.confirmed_frame else None
            ),
            "pending": self.pending.to_dict() if self.pending else None,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "ProfileConversationState":
        """Estricta: lo que no se entiende invalida, no se completa a ojo."""
        if raw is None:
            return cls()
        if not isinstance(raw, dict):
            raise ProfileStateError("profile state must be an object")
        version = raw.get("schema_version")
        if version != PROFILE_STATE_VERSION:
            raise ProfileStateError(f"unknown profile state schema: {version!r}")
        confirmed_raw = raw.get("confirmed_frame")
        pending_raw = raw.get("pending")
        try:
            confirmed = (
                EntityProfileFrame.from_dict(confirmed_raw) if confirmed_raw else None
            )
        except FrameSchemaError as exc:
            raise ProfileStateError(f"malformed confirmed frame: {exc}") from exc
        pending = PendingProfile.from_dict(pending_raw) if pending_raw else None
        return cls(confirmed_frame=confirmed, pending=pending)


def new_pending_id() -> str:
    return uuid.uuid4().hex[:12]


__all__ = [
    "PROFILE_STATE_KEY",
    "PROFILE_STATE_VERSION",
    "Alternative",
    "PendingIssue",
    "PendingProfile",
    "ProfileConversationState",
    "ProfileStateError",
    "new_pending_id",
]
