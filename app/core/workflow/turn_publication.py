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
    TabularGroup,
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
    return ContinuationContract(
        conversation_revision=reservation.revision,
        visible_turn_id=reservation.turn_id,
        result_id=reservation.result_id,
        result_kind=ResultKind.CLARIFICATION,
        owner=ContinuationOwner.ENTITY_PROFILE,
        pending_profile=(
            PendingProfileRef(
                pending_ref=pending.pending_id,
                base_frame_ref=pending.base_frame_ref,
            ) if pending else None
        ),
    )


def delegated_contract(
    reservation: TurnReservation, result: dict,
) -> ContinuationContract:
    """El contrato de un turno que NO es del perfil, leído de su resultado.

    No se inventa un cursor: los grupos declaran de quién es el resultado y con
    qué huella se mostró. La mecánica de paginación sigue siendo del handler,
    que valida cursor, generación y vigencia en su propio sidecar.
    """
    if result.get("needs_clarification"):
        return ContinuationContract(
            conversation_revision=reservation.revision,
            visible_turn_id=reservation.turn_id,
            result_id=reservation.result_id,
            result_kind=ResultKind.CLARIFICATION,
            owner=ContinuationOwner.NONE,
        )
    groups = _tabular_groups(reservation, result)
    if not groups:
        return ContinuationContract(
            conversation_revision=reservation.revision,
            visible_turn_id=reservation.turn_id,
            result_id=reservation.result_id,
            result_kind=ResultKind.OTHER,
            owner=ContinuationOwner.NONE,
        )
    return ContinuationContract(
        conversation_revision=reservation.revision,
        visible_turn_id=reservation.turn_id,
        result_id=reservation.result_id,
        result_kind=ResultKind.TABULAR_SEARCH,
        owner=ContinuationOwner.TABULAR,
        tabular_groups=groups,
    )


def _tabular_groups(
    reservation: TurnReservation, result: dict,
) -> dict[str, TabularGroup]:
    fingerprint = str(result.get("query_fingerprint") or "")
    order = str(result.get("result_order_guarantee") or "exact_order")
    names = list((result.get("dataset_groups") or {}).keys())
    if not names and result.get("rows"):
        dataset_id = str(result.get("dataset_id") or "")
        if dataset_id:
            names = ["contratos" if dataset_id == "jbjy-vk9h" else "procesos"]
    return {
        name: TabularGroup(
            group=name,
            cursor_id=f"{reservation.turn_id}:{name}",
            fingerprint=fingerprint,
            order_contract=order,
            generation=reservation.turn_id,
        )
        for name in names
    }


def publish(
    deps: "WorkflowDeps",
    reservation: TurnReservation | None,
    result: dict,
    *,
    user_query: str,
    profile_state: ProfileConversationState,
    contract: ContinuationContract | None,
    execution_record: dict | None = None,
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
    result["continuation"] = contract.to_dict() if contract else None
    return PublishedTurn(True, envelope.revision)


__all__ = [
    "CONTEXT_NOT_PRESERVED",
    "PublishedTurn",
    "delegated_contract",
    "profile_contract",
    "publish",
]
