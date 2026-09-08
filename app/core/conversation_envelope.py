"""El sobre versionado de un turno: respuesta, estado, continuación y ejecución.

Un turno de V1.2 publica CUATRO cosas que tienen que ser coherentes entre sí:
lo que se contestó, el Frame confirmado o el pendiente, el contrato que dice qué
puede continuar, y el registro de lo autorizado y observado. Publicarlas por
separado permite un estado intermedio en el que el contrato referencia un plan
que nadie guardó.

Aquí van en un solo registro, con su revisión. Y se leen ESTRICTAMENTE:

* `parse_constant` rechaza NaN e Infinity, que no son JSON y que al imprimirse
  parecerían montos;
* una versión desconocida, una referencia rota o una forma incompleta
  invalidan el sobre entero;
* una cola corrupta marca continuidad INVÁLIDA — no se retrocede en silencio a
  una línea vieja para reactivar su cursor.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json


ENVELOPE_SCHEMA_VERSION = 1


class EnvelopeError(ValueError):
    """Ilegible es ilegible: no hay una lectura parcial «suficientemente buena»."""


class EnvelopeStatus(str, Enum):
    OK = "OK"
    EMPTY = "EMPTY"
    CORRUPT = "CORRUPT"


def _reject_constant(name: str):
    raise EnvelopeError(f"JSON no finito en el sobre: {name}")


@dataclass(frozen=True, slots=True)
class ConversationEnvelope:
    revision: int
    turn_id: str
    result_id: str
    chat_id: str
    user_query: str
    response: str
    timestamp: str
    profile_state: dict | None = None
    continuation: dict | None = None
    execution_record: dict | None = None
    continuity_generation: str | None = None
    schema_version: int = ENVELOPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != ENVELOPE_SCHEMA_VERSION:
            raise EnvelopeError(f"unknown envelope schema: {self.schema_version}")
        if self.revision < 1:
            raise EnvelopeError("a published revision starts at 1")

    def to_dict(self) -> dict:
        return {
            "continuity_generation": self.continuity_generation,
            "schema_version": self.schema_version,
            "revision": self.revision,
            "turn_id": self.turn_id,
            "result_id": self.result_id,
            "chat_id": self.chat_id,
            "user_query": self.user_query,
            "response": self.response,
            "timestamp": self.timestamp,
            "profile_state": self.profile_state,
            "continuation": self.continuation,
            "execution_record": self.execution_record,
        }

    def to_json(self) -> str:
        """JSON estricto: `allow_nan=False` revienta aquí y no en el lector."""
        return json.dumps(self.to_dict(), ensure_ascii=False, allow_nan=False)

    @classmethod
    def from_dict(cls, raw: object) -> "ConversationEnvelope":
        if not isinstance(raw, dict):
            raise EnvelopeError("envelope must be an object")
        version = raw.get("schema_version")
        if version != ENVELOPE_SCHEMA_VERSION:
            raise EnvelopeError(f"unknown envelope schema: {version!r}")
        try:
            return cls(
                continuity_generation=raw.get("continuity_generation"),
                revision=int(raw["revision"]),
                turn_id=str(raw["turn_id"]),
                result_id=str(raw["result_id"]),
                chat_id=str(raw["chat_id"]),
                user_query=str(raw["user_query"]),
                response=str(raw["response"]),
                timestamp=str(raw["timestamp"]),
                profile_state=_optional_object(raw.get("profile_state")),
                continuation=_optional_object(raw.get("continuation")),
                execution_record=_optional_object(raw.get("execution_record")),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise EnvelopeError(f"malformed envelope: {exc}") from exc

    @classmethod
    def from_json(cls, line: str) -> "ConversationEnvelope":
        try:
            payload = json.loads(line, parse_constant=_reject_constant)
        except json.JSONDecodeError as exc:
            raise EnvelopeError(f"unreadable envelope line: {exc}") from exc
        return cls.from_dict(payload)


@dataclass(frozen=True, slots=True)
class EnvelopeRead:
    """El resultado de leer la conversación publicada, con su honestidad."""

    status: EnvelopeStatus
    envelope: ConversationEnvelope | None = None
    revision: int = 0
    reason: str = ""

    @property
    def usable(self) -> bool:
        return self.status is EnvelopeStatus.OK and self.envelope is not None


def _optional_object(value: object) -> dict | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise EnvelopeError("envelope section must be an object or null")
    return value


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


__all__ = [
    "ENVELOPE_SCHEMA_VERSION",
    "ConversationEnvelope",
    "EnvelopeError",
    "EnvelopeRead",
    "EnvelopeStatus",
    "now_iso",
]
