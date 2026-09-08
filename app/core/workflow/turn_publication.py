"""Publicar el turno: respuesta, estado, contrato y ejecución, como UN registro.

Publicarlos por separado deja un estado intermedio en el que el contrato
referencia un plan que nadie guardó, o en el que un cursor nuevo se entrega
como confirmado aunque la escritura fallara. El diseño lo cierra en cuatro
pasos, y este módulo implementa los dos últimos:

    3. asignar el ID de resultado ANTES de crear sus referencias;
    4. sólo el registro completo publicado es autoridad para el siguiente turno.

Un fallo de publicación NO avanza el Frame durable ni entrega un cursor nuevo:
la respuesta se devuelve —la persona ya hizo su pregunta— con el aviso de que
el contexto no pudo conservarse.
"""
from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import TYPE_CHECKING

from app.core.conversation_store import StaleRevisionError, TurnReservation
from app.core.entity_profile.frame_turn import ProfileTurnOutcome, TurnKind
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.store_lock import ConversationLockTimeout
from app.core.workflow.continuation import (
    ConfirmedProfileRef,
    ContinuationContract,
    ContinuationOwner,
    PendingProfileRef,
    ResultKind,
    DelegatedRef,
)

if TYPE_CHECKING:
    from app.core.workflow.deps import WorkflowDeps


logger = logging.getLogger(__name__)

CONTEXT_NOT_PRESERVED = (
    "\n\n(No pude guardar el contexto de este turno, así que la próxima "
    "pregunta no podrá continuarlo.)"
)


@dataclass(frozen=True, slots=True)
class PublishedTurn:
    published: bool
    revision: int
    reason: str = ""


def profile_contract(
    reservation: TurnReservation, outcome: ProfileTurnOutcome,
) -> ContinuationContract:
    """El contrato de un turno de perfil: confirmado o pendiente, nunca los dos."""
    if outcome.kind is TurnKind.ANSWER and outcome.frame and outcome.record:
        return ContinuationContract(
            conversation_revision=reservation.revision,
            visible_turn_id=reservation.turn_id,
            result_id=reservation.result_id,
            result_kind=ResultKind.ENTITY_PROFILE,
            owner=ContinuationOwner.ENTITY_PROFILE,
            confirmed_profile=ConfirmedProfileRef(
                frame_ref=(outcome.frame.frame_id, outcome.frame.revision),
                plan_ref=outcome.plan.plan_id,
                evidence_ref=outcome.record.record_id,
            ),
        )
    pending = outcome.state.pending
    previous = reservation.previous.envelope if reservation.previous.usable else None
    confirmed = None
    if outcome.state.confirmed_frame and previous and previous.continuation:
        confirmed = ContinuationContract.from_dict(previous.continuation).confirmed_profile
    return ContinuationContract(
        conversation_revision=reservation.revision,
        visible_turn_id=reservation.turn_id,
        result_id=reservation.result_id,
        result_kind=ResultKind.CLARIFICATION,
        owner=ContinuationOwner.ENTITY_PROFILE,
        confirmed_profile=confirmed,
        pending_profile=(
            PendingProfileRef(
                pending_ref=pending.pending_id,
                base_frame_ref=pending.base_frame_ref,
            ) if pending else None
        ),
    )


def delegated_contract(reservation: TurnReservation, result: dict, store=None) -> ContinuationContract:
    """The producing handler declares ownership; rows never grant capabilities."""
    owner = ContinuationOwner(result.get("_continuation_owner", "NONE"))
    kinds = {ContinuationOwner.ACTOR: ResultKind.ACTOR_PROFILE,
             ContinuationOwner.RADAR: ResultKind.RADAR,
             ContinuationOwner.TABULAR: ResultKind.TABULAR_SEARCH}
    groups, export_ref = None, None
    if owner is ContinuationOwner.TABULAR and store and not result.get("needs_clarification"):
        from app.core.workflow.tabular_capability import published_groups
        groups = published_groups(store, reservation)
        history = store.get_history(reservation.chat_id, last_n=1)
        if history and history[0].trace_id == result.get("trace_id"):
            export_ref = history[0].turn_id
    return ContinuationContract(
        conversation_revision=reservation.revision,
        visible_turn_id=reservation.turn_id, result_id=reservation.result_id,
        result_kind=ResultKind.CLARIFICATION if result.get("needs_clarification") else kinds.get(owner, ResultKind.OTHER),
        owner=owner, tabular_groups=groups, export_ref=export_ref,
        delegated=DelegatedRef(str(result.get("trace_id") or reservation.turn_id))
        if owner in (ContinuationOwner.ACTOR, ContinuationOwner.RADAR) else None,
    )


def publish(
    deps: "WorkflowDeps",
    reservation: TurnReservation | None,
    result: dict,
    *,
    user_query: str,
    profile_state: ProfileConversationState,
    contract: ContinuationContract | None,
    execution_record: dict | None = None,
    preserve_generation: bool = False,
) -> PublishedTurn:
    """Escribe el registro completo, o informa por qué no hay continuidad nueva."""
    if reservation is None:
        return PublishedTurn(False, 0, "anonymous_turn")
    try:
        envelope = deps.conv_store.publish_turn(
            reservation,
            user_query=user_query,
            response=result.get("response", ""),
            profile_state=profile_state.to_dict(),
            continuation=contract.to_dict() if contract else None,
            execution_record=execution_record,
            preserve_generation=preserve_generation,
        )
    except (ConversationLockTimeout, OSError, StaleRevisionError, ValueError) as exc:
        # El Frame durable NO avanza y no se entrega un cursor nuevo. La
        # respuesta sí se devuelve: la consulta ya se hizo y ocultarla sería
        # otra mentira.
        logger.warning("turn publication failed: %s", exc)
        result["response"] = result.get("response", "") + CONTEXT_NOT_PRESERVED
        result["continuation_published"] = False
        return PublishedTurn(False, reservation.expected_revision, str(exc))
    result["continuation_published"] = True
    result["conversation_revision"] = envelope.revision
    result["result_id"] = envelope.result_id
    result["chat_id"] = envelope.chat_id
    result["continuation"] = contract.to_dict() if contract else None
    return PublishedTurn(True, envelope.revision)


__all__ = [
    "CONTEXT_NOT_PRESERVED",
    "PublishedTurn",
    "delegated_contract",
    "profile_contract",
    "publish",
]


def preserved_contract(reservation, contract):
    from dataclasses import replace

    return replace(contract, conversation_revision=reservation.revision,
                   visible_turn_id=reservation.turn_id, result_id=reservation.result_id,
                   result_kind=ResultKind.CLARIFICATION)
