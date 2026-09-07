"""Lo que la conversación del perfil guarda entre turnos, de forma transaccional.

La revisión independiente (F6) probó con `ConversationStore` real y un
`SecopalWorkflow` nuevo por turno —no con un merge simulado— que el candidato:

* perdía el evento: `contratos firmados en agosto` → `proveedores` dejaba
  `event` en `NONE`;
* perdía el foco: `proveedores` → `en 2025` volvía a `overview` y ejecutaba
  cuatro llamadas cuando sólo se pidió cambiar el periodo;
* **guardaba como petición vigente la que había provocado una aclaración**, de
  modo que preguntar destruía el frame confirmado;
* convertía permanentemente el origen EXPLICIT en INHERITED.

Aquí se separan dos cosas que V1 mezclaba en un solo diccionario:

    confirmed    lo último que la persona pidió Y el sistema pudo ejecutar
    pending      la lectura que provocó una aclaración, todavía sin confirmar

Una aclaración NUNCA sustituye a la confirmada. Mientras haya pendiente, la
confirmada sigue siendo la verdad de la conversación, y eso es lo que permite
volver a preguntar en vez de ejecutar una interpretación que nadie aceptó.

El esquema va versionado porque se persiste en disco y lo lee una sesión
posterior: una carga de otra versión se descarta entera en vez de adivinar.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.entity_profile.semantic_request import EntityProfileSemanticRequest


CONVERSATION_STATE_VERSION = 1

STATE_PARAM = "_entity_profile_state"


@dataclass(frozen=True, slots=True)
class EntityProfileConversationState:
    confirmed: EntityProfileSemanticRequest | None = None
    pending: EntityProfileSemanticRequest | None = None
    pending_question: str | None = None

    @property
    def has_pending(self) -> bool:
        return self.pending is not None

    def confirm(
        self, request: EntityProfileSemanticRequest,
    ) -> "EntityProfileConversationState":
        """Un turno ejecutable confirma su lectura y cancela lo pendiente."""
        return EntityProfileConversationState(confirmed=request)

    def hold(
        self, request: EntityProfileSemanticRequest, question: str,
    ) -> "EntityProfileConversationState":
        """Una aclaración retiene su lectura SIN tocar la confirmada."""
        return EntityProfileConversationState(
            confirmed=self.confirmed, pending=request, pending_question=question,
        )

    def to_dict(self) -> dict:
        return {
            "version": CONVERSATION_STATE_VERSION,
            "confirmed": self.confirmed.to_dict() if self.confirmed else None,
            "pending": self.pending.to_dict() if self.pending else None,
            "pending_question": self.pending_question,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "EntityProfileConversationState":
        """Una carga ilegible o de otra versión se descarta; nunca se adivina."""
        if not isinstance(raw, dict):
            return cls()
        if raw.get("version") != CONVERSATION_STATE_VERSION:
            return cls()
        question = raw.get("pending_question")
        return cls(
            confirmed=EntityProfileSemanticRequest.from_dict(raw.get("confirmed")),
            pending=EntityProfileSemanticRequest.from_dict(raw.get("pending")),
            pending_question=question if isinstance(question, str) else None,
        )


def state_from_params(params: dict) -> EntityProfileConversationState:
    return EntityProfileConversationState.from_dict(params.get(STATE_PARAM))


def state_context(state: EntityProfileConversationState) -> dict:
    """Los marcadores JSON-safe que la conversación persiste.

    ``_entity_profile_request`` conserva SIEMPRE la petición **confirmada**: es
    la que leen el fallback del assembler y las auditorías, y publicar allí una
    lectura no confirmada era justamente el defecto.
    """
    context: dict = {STATE_PARAM: state.to_dict()}
    if state.confirmed is not None:
        context["_entity_profile_request"] = state.confirmed.to_dict()
        context["_entity_profile_sources"] = state.confirmed.sources.value
    return context
