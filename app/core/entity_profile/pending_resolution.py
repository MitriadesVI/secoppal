"""Qué hace un turno con una pregunta abierta encima. Sólo eso.

Un pendiente admite exactamente cinco desenlaces, y ninguno de ellos es
«ejecutar de todas formas»:

    CANCELLED     la persona lo canceló; lo confirmado sigue vigente
    REPEATED      un «sí» a secas: se repiten las opciones, cero consultas
    UNRESOLVED    un cambio no relacionado: resolver o cancelar, cero consultas
    SUPERSEDED    un pedido inequívoco sobre otro comprador; empieza otro
    SELECTED      una alternativa elegida, aplicada sobre SU base

Este módulo decide cuál es, y no ejecuta: componer y compilar es de `frame_turn`.
Separarlos evita que «se eligió una alternativa» y «se puede consultar» acaben
siendo la misma condición, que es como una pendiente se aplica sin selección.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date
from enum import Enum
import re
import unicodedata

from app.core.entity_profile.claim_adapters import collect_claims
from app.core.entity_profile.claim_composition import buyer_scope_from, compose_patch
from app.core.entity_profile.frame import BuyerScope, EntityProfileFrame
from app.core.entity_profile.patch import (
    EntityProfilePatch,
    PatchOperation,
    apply_patch,
)
from app.core.entity_profile.profile_state import (
    Alternative,
    PendingProfile,
    ProfileConversationState,
)
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.turn import is_bare_affirmative


_CANCEL = re.compile(
    r"\b(?:cancela(?:r)?|olvida(?:lo)?|dejalo|deja\s+eso|anula(?:r)?|"
    r"no\s+importa)\b",
)

_ORDINAL = re.compile(
    r"^(?:la\s+)?(?:opcion\s+)?(?:(\d+)|(primera|segunda|tercera))$",
)

_ORDINAL_WORDS = {"primera": 1, "segunda": 2, "tercera": 3}


class PendingOutcome(str, Enum):
    CANCELLED = "CANCELLED"
    REPEATED = "REPEATED"
    UNRESOLVED = "UNRESOLVED"
    SUPERSEDED = "SUPERSEDED"
    SELECTED = "SELECTED"


@dataclass(frozen=True, slots=True)
class PendingDecision:
    outcome: PendingOutcome
    state: ProfileConversationState
    candidate: EntityProfileFrame | None = None
    patch: EntityProfilePatch | None = None
    alternative: Alternative | None = None


def resolve_pending(
    *,
    user_query: str,
    params: dict,
    state: ProfileConversationState,
    turn_id: str,
    buyer: BuyerScope | None,
    today: Date | None = None,
) -> PendingDecision:
    """El desenlace de la pendiente; ninguna de sus ramas consulta la fuente."""
    pending = state.pending
    if pending is None:
        raise ValueError("resolve_pending needs a pending question")
    if _CANCEL.search(_fold(user_query)):
        return PendingDecision(PendingOutcome.CANCELLED, state.cancel_pending())
    if is_bare_affirmative(user_query):
        # Un «sí» a secas no elige entre alternativas: elegir por la persona es
        # justo lo que la aclaración existía para evitar.
        return PendingDecision(PendingOutcome.REPEATED, state)
    if buyer is not None and _is_other_buyer(buyer, state):
        # Un pedido inequívoco sobre OTRO comprador abandona la pendiente; no se
        # obliga a contestar una pregunta vieja para empezar otra consulta.
        return PendingDecision(PendingOutcome.SUPERSEDED, state)
    selected = _select(user_query, params, pending, state, turn_id, today)
    if selected is None:
        return PendingDecision(PendingOutcome.UNRESOLVED, state)
    candidate, patch, alternative = selected
    return PendingDecision(
        PendingOutcome.SELECTED, state.cancel_pending(), candidate, patch, alternative,
    )


def _select(user_query, params, pending, state, turn_id, today):
    """Aplica pending.patch sobre su base y después la alternativa elegida."""
    if not pending.alternatives:
        return None
    alternative = _by_ordinal(user_query, pending)
    if alternative is None:
        alternative = _by_own_words(
            user_query, params, pending, state, turn_id, today,
        )
    if alternative is None:
        return None
    applied = apply_patch(state.confirmed_frame, pending.patch)
    if applied.frame is None:
        return None
    completion = EntityProfilePatch(
        patch_id=f"{pending.pending_id}:{alternative.alternative_id}",
        turn_id=turn_id,
        operations=alternative.completion_operations,
        base_frame_ref=(applied.frame.frame_id, applied.frame.revision),
    )
    completed = apply_patch(applied.frame, completion)
    if completed.frame is None:
        return None
    return completed.frame, completion, alternative


def _by_ordinal(user_query: str, pending: PendingProfile) -> Alternative | None:
    match = _ORDINAL.fullmatch(_fold(user_query).strip())
    if match is None:
        return None
    index = (
        int(match.group(1)) if match.group(1)
        else _ORDINAL_WORDS[match.group(2)]
    )
    if 1 <= index <= len(pending.alternatives):
        return pending.alternatives[index - 1]
    return None


def _by_own_words(
    user_query, params, pending, state, turn_id, today,
) -> Alternative | None:
    """La persona describe la salida con sus palabras: «solo contratos».

    Se compone su turno y se compara con lo que cada alternativa CAMBIA. No es
    un emparejamiento de texto: es igualdad de operaciones tipadas, así que una
    frase que pida otra cosa no puede colarse como si fuera la alternativa.
    """
    resolution = resolve_profile_scope(params)
    claims = collect_claims(
        user_query, params, turn_id=turn_id, buyer_resolution=resolution,
        today=today,
    )
    proposal = compose_patch(
        claims, base_frame=state.confirmed_frame,
        buyer_scope=buyer_scope_from(resolution),
    )
    if proposal.patch is None:
        return None
    changes = _sets(proposal.patch.operations)
    if not changes:
        return None
    for alternative in pending.alternatives:
        offered = _sets(alternative.completion_operations)
        if all(offered.get(key) == value for key, value in changes.items()):
            return alternative
    return None


def _sets(operations) -> dict:
    return {
        (op.target.kind, op.target.source): op.value
        for op in operations
        if op.operation is PatchOperation.SET
    }


def _is_other_buyer(buyer: BuyerScope, state: ProfileConversationState) -> bool:
    confirmed = state.confirmed_frame
    if confirmed is None:
        return False
    return buyer.canonical_buyers != confirmed.buyer_scope.canonical_buyers


def _fold(text: str) -> str:
    decomposed = unicodedata.normalize("NFD", text.casefold())
    return "".join(char for char in decomposed if not unicodedata.combining(char))


__all__ = [
    "PendingDecision",
    "PendingOutcome",
    "resolve_pending",
]


def complete_reformulation(user_query, params, buyer, turn_id, builder, today):
    """A standalone, supported request can replace any pending question."""
    from app.core.entity_profile.turn import detect_entity_profile_turn
    from app.core.entity_profile.frame_compiler import compile_profile, PlanStatus

    if buyer is None or not detect_entity_profile_turn(user_query, params)[0]:
        return None
    resolution = resolve_profile_scope(params)
    claims = collect_claims(user_query, params, turn_id=turn_id,
                            buyer_resolution=resolution, today=today)
    proposal = compose_patch(claims, base_frame=None, buyer_scope=buyer)
    if proposal.patch is None:
        return None
    applied = apply_patch(None, proposal.patch)
    if applied.frame is None:
        return None
    decision = compile_profile(applied.frame, soql_builder=builder)
    if decision.status is not PlanStatus.READY:
        return None
    return applied.frame, proposal.patch
