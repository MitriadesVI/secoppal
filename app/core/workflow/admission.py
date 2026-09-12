"""La admisión superior: un grafo Burr pequeño ANTES de `_dispatch`.

`_dispatch` es una cadena de responsabilidad: el primer handler que devuelve
algo gana. Eso deja la elección de ruta repartida entre ocho detectores, y el
refinamiento sembrado por la interfaz entra por delante de todos ellos sin
pasar por ninguna comprobación de dueño.

Aquí hay un solo nodo de decisión y tres salidas, y las condiciones de
transición comparan un campo ya calculado (`route`), de modo que son disjuntas
por construcción y no por convención. Burr elige la primera condición
verdadera; que sean excluyentes lo garantiza el clasificador, y lo comprueba su
prueba.

Este grafo no consulta fuentes, no pagina y no duplica la lógica de Actor,
Radar ni sugerencias: decide de quién es el turno y delega.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date as Date
from typing import TYPE_CHECKING, Any

from burr.core import ApplicationBuilder, State, action, expr

from app.core.conversation_envelope import EnvelopeRead, EnvelopeStatus
from app.core.entity_profile.frame_turn import is_profile_turn
from app.core.entity_profile.profile_state import (
    ProfileConversationState,
    ProfileStateError,
)
from app.core.workflow.admission_rules import (
    AdmissionDecision,
    AdmissionRoute,
    InputShape,
    classify,
)
from app.core.workflow.continuation import (
    ContinuationContract,
    ContinuationError,
    ContinuationOwner,
)
from app.core.workflow.profile_action_admission import (
    TrustedProfileAction,
    admit_profile_action,
)

if TYPE_CHECKING:
    from app.core.workflow.deps import WorkflowDeps


@dataclass(frozen=True, slots=True)
class AdmittedTurn:
    """Lo que la admisión entrega: la decisión y el contexto acreditado."""

    decision: AdmissionDecision
    profile_state: ProfileConversationState
    contract: ContinuationContract | None
    envelope_status: EnvelopeStatus
    conversation_revision: int
    params: dict
    effective_user_query: str
    trusted_profile_action: TrustedProfileAction | None = None

    @property
    def route(self) -> AdmissionRoute:
        return self.decision.route


@action(reads=[], writes=["route", "reason", "shape", "owner", "response"])
def classify_turn(state: State, decision: AdmissionDecision) -> State:
    """El único punto donde se decide la ruta del turno."""
    return state.update(
        route=decision.route.value,
        reason=decision.reason,
        shape=decision.shape.value,
        owner=decision.owner.value,
        response=decision.response,
    )


@action(reads=["route"], writes=[])
def admit_profile(state: State) -> State:
    return state


@action(reads=["route"], writes=[])
def admit_delegated(state: State) -> State:
    return state


@action(reads=["route"], writes=[])
def admit_clarification(state: State) -> State:
    return state


_TERMINALS = ("admit_profile", "admit_delegated", "admit_clarification")


def build_admission(decision: AdmissionDecision):
    """Un grafo por turno, como el del flujo normal: la decisión es su entrada."""
    return (
        ApplicationBuilder()
        .with_actions(
            classify_turn=classify_turn.bind(decision=decision),
            admit_profile=admit_profile,
            admit_delegated=admit_delegated,
            admit_clarification=admit_clarification,
        )
        .with_transitions(
            ("classify_turn", "admit_profile", expr("route == 'PROFILE'")),
            ("classify_turn", "admit_delegated", expr("route == 'DELEGATED'")),
            ("classify_turn", "admit_clarification", expr("route == 'CLARIFY'")),
        )
        .with_state(route="", reason="", shape="", owner="", response="")
        .with_entrypoint("classify_turn")
        .build()
    )


def admit(
    deps: "WorkflowDeps",
    *,
    user_query: str,
    chat_id: str | None,
    seed_resolved_params: dict | None,
    today: Date | None = None,
    read: EnvelopeRead | None = None,
    actor_candidate_selection: dict | None = None,
    profile_action_selection: dict | None = None,
) -> AdmittedTurn:
    """Carga el contrato publicado, clasifica y devuelve UNA decisión.

    `read` llega ya cargado cuando el turno se reservó primero, que es el orden
    del diseño: reservar serialmente ANTES de ejecutar, y clasificar sobre la
    misma revisión que después se comprueba al publicar.
    """
    read = read if read is not None else _load(deps, chat_id)
    profile_state, contract, status = _restore(read)
    if profile_action_selection is not None:
        action = admit_profile_action(
            profile_action_selection,
            chat_id=chat_id,
            read=read,
            state=profile_state,
            contract=contract,
        )
        if action.trusted is None:
            decision = AdmissionDecision(
                AdmissionRoute.CLARIFY, action.reason, InputShape.TEXT,
                ContinuationOwner.NONE, action.response,
            )
            return _finish_admission(
                decision=decision,
                profile_state=profile_state,
                contract=contract,
                status=status,
                revision=read.revision,
                params={},
                effective_user_query="",
            )
        effective_query = action.trusted.phrase
        params = deps.query_router.parse(effective_query, today=today).params
        decision = AdmissionDecision(
            AdmissionRoute.PROFILE, "profile_action", InputShape.TEXT,
            ContinuationOwner.ENTITY_PROFILE,
        )
        return _finish_admission(
            decision=decision,
            profile_state=profile_state,
            contract=contract,
            status=status,
            revision=read.revision,
            params=params,
            effective_user_query=effective_query,
            trusted=action.trusted,
        )
    if read.usable and read.envelope.chat_id != chat_id:
        profile_state, contract, status = ProfileConversationState(), None, EnvelopeStatus.CORRUPT
    params = deps.query_router.parse(user_query, today=today).params
    from app.core.workflow.external_admission import external_owner
    external = external_owner(user_query, params, actor_candidate_selection)
    if status is EnvelopeStatus.EMPTY and external is None and not is_profile_turn(
            user_query, params, ProfileConversationState())[0]:
        from app.core.workflow.profile_migration import migrate_current_legacy
        migrated = migrate_current_legacy(deps.conv_store, chat_id)
        if migrated:
            profile_state, error = migrated
            if error:
                status = EnvelopeStatus.CORRUPT
    owns_text = False
    if _text_shaped(user_query, seed_resolved_params):
        owns_text = is_profile_turn(user_query, params, profile_state)[0]
    decision = classify(
        user_query=user_query,
        seed_resolved_params=seed_resolved_params,
        envelope_status=status,
        contract=contract,
        conversation_revision=read.revision,
        profile_owns_text=owns_text,
        profile_has_options=bool(
            profile_state.pending and profile_state.pending.alternatives,
        ),
    )
    if external is not None and not seed_resolved_params:
        decision = AdmissionDecision(AdmissionRoute.DELEGATED, "explicit_external_operation",
                                     InputShape.TEXT, ContinuationOwner(external))
    elif status is EnvelopeStatus.CORRUPT:
        fresh, _ = is_profile_turn(user_query, params, ProfileConversationState())
        if not fresh and not decision.clarifies:
            decision = AdmissionDecision(AdmissionRoute.CLARIFY, "invalid_current_publication",
                                         InputShape.TEXT, ContinuationOwner.NONE,
                                         "No puedo acreditar el contexto actual. Repite la consulta completa.")
    reference = (seed_resolved_params or {}).get("_continuation_ref")
    if isinstance(reference, dict) and reference.get("chat_id", chat_id) != chat_id:
        decision = AdmissionDecision(AdmissionRoute.CLARIFY, "foreign_chat_reference",
                                     InputShape.SEED_REFINEMENT, ContinuationOwner.NONE,
                                     "Ese filtro pertenece a otra conversación.")
    return _finish_admission(
        decision=decision,
        profile_state=profile_state,
        contract=contract,
        status=status,
        revision=read.revision,
        params=params,
        effective_user_query=user_query,
    )


def _finish_admission(
    *, decision, profile_state, contract, status, revision, params,
    effective_user_query, trusted=None,
) -> AdmittedTurn:
    application = build_admission(decision)
    *_, final = application.run(halt_after=list(_TERMINALS))
    if final["route"] != decision.route.value:
        raise RuntimeError("admission graph disagreed with its own decision")
    return AdmittedTurn(
        decision=decision, profile_state=profile_state, contract=contract,
        envelope_status=status, conversation_revision=revision, params=params,
        effective_user_query=effective_user_query,
        trusted_profile_action=trusted,
    )


def _text_shaped(user_query: str, seed: dict | None) -> bool:
    from app.core.workflow.admission_rules import input_shape

    return input_shape(user_query, seed_resolved_params=seed) is InputShape.TEXT


def _load(deps: "WorkflowDeps", chat_id: str | None) -> EnvelopeRead:
    if not chat_id:
        return EnvelopeRead(EnvelopeStatus.EMPTY)
    return deps.conv_store.load_envelope(chat_id)


def _restore(
    read: EnvelopeRead,
) -> tuple[ProfileConversationState, ContinuationContract | None, EnvelopeStatus]:
    """Un sobre que no se entiende NO produce un estado a medias."""
    if not read.usable:
        return ProfileConversationState(), None, read.status
    envelope = read.envelope
    try:
        state = ProfileConversationState.from_dict(envelope.profile_state)
    except ProfileStateError:
        return ProfileConversationState(), None, EnvelopeStatus.CORRUPT
    try:
        contract = (
            ContinuationContract.from_dict(envelope.continuation)
            if envelope.continuation else None
        )
    except ContinuationError:
        return ProfileConversationState(), None, EnvelopeStatus.CORRUPT
    from app.core.workflow.envelope_validation import validate_envelope

    try:
        validate_envelope(envelope, state, contract)
    except (ValueError, KeyError, TypeError, IndexError):
        return ProfileConversationState(), None, EnvelopeStatus.CORRUPT
    return state, contract, EnvelopeStatus.OK


def owner_of(contract: ContinuationContract | None) -> ContinuationOwner:
    return contract.owner if contract else ContinuationOwner.NONE


__all__ = [
    "AdmittedTurn",
    "admit",
    "build_admission",
    "owner_of",
]
