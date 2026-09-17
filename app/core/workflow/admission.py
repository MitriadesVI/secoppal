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
    from app.core.accreditation.assembly import AccreditationAssembly
    from app.core.accreditation.credential import CapabilityCredential
    from app.core.accreditation.spec import CapabilitySpec
    from app.core.natural_alpha.models import Observation
    from app.core.workflow.deps import WorkflowDeps


_ACCREDITED_READY = (
    "Entendí el trabajo solicitado y acredité sus parámetros de ejecución, pero esta "
    "ruta todavía no ejecuta la consulta."
)
_ACCREDITED_OTHER_OWNER = (
    "Entendí que esta pregunta es sobre un actor concreto; esa ruta todavía no está "
    "conectada en este carril."
)


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
    # Plan V2 9 (C3/C4A): la autoridad credenciada que este turno reclamó y el
    # ensamblaje que sus autoridades produjeron. Viajan juntas o ambas son None —
    # nunca una credencial sin la spec que la declaró, ni un ensamblaje sin
    # credencial que lo justifique.
    credential: "CapabilityCredential | None" = None
    capability_spec: "CapabilitySpec | None" = None
    assembly: "AccreditationAssembly | None" = None

    def __post_init__(self) -> None:
        if (self.credential is None) != (self.capability_spec is None):
            raise ValueError("CREDENTIAL_SPEC_MISMATCH")
        if self.assembly is not None and self.credential is None:
            raise ValueError("ASSEMBLY_WITHOUT_CREDENTIAL")

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
    observation: "Observation | None" = None,
) -> AdmittedTurn:
    """Carga el contrato publicado, clasifica y devuelve UNA decisión.

    `read` llega ya cargado cuando el turno se reservó primero, que es el orden
    del diseño: reservar serialmente ANTES de ejecutar, y clasificar sobre la
    misma revisión que después se comprueba al publicar.

    `observation` es la observación única de Natural Alpha, hecha antes de esta
    admisión (plan V2 8). En C2 viaja como input inerte: ninguna decisión de
    aquí la lee todavía. C3 es quien la vuelve credencial y quien puede dejar de
    llamar al parser tabular por ella.
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
    accredited = _accredited_pending(
        deps, observation=observation, user_query=user_query,
        seed_resolved_params=seed_resolved_params, profile_state=profile_state,
        contract=contract, status=status, revision=read.revision, today=today,
    )
    if accredited is not None:
        return accredited
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


def _accredited_pending(
    deps: "WorkflowDeps", *, observation, user_query, seed_resolved_params,
    profile_state, contract, status, revision, today=None,
) -> AdmittedTurn | None:
    """C4A: ensamblar las autoridades sobre el raw y terminar el turno con honestidad.

    Plan V2 9-17: con credencial válida no hay ``query_router.parse``, ni
    external owner sobre params legacy, ni migración, ni caída al dispatcher. Las
    autoridades independientes leen el raw y producen ``accredited_params``; C4A
    todavía no ejecuta el runtime de perfil, así que READY termina en un CLARIFY
    que transporta los parámetros acreditados y sus receipts.
    """
    from app.core.accreditation.assembly import AssemblyStatus, assemble
    from app.core.accreditation.registry import spec_for_credential
    from app.core.accreditation.seam import credential_from_observation
    from app.core.workflow.admission_rules import input_shape

    credential = credential_from_observation(observation, user_query, settings=deps.settings)
    if credential is None:
        return None
    spec = spec_for_credential(credential)
    if spec is None:  # The seam never builds an undeclared capability; fail closed.
        raise ValueError("CREDENTIAL_WITHOUT_DECLARED_SPEC")
    assembly = assemble(user_query, credential, spec,
                        entity_resolver=deps.entity_resolver, today=today,
                        actor_resolver=deps.actor_name_resolver)
    if assembly.status is AssemblyStatus.READY:
        reason, response, params = "accredited_execution_pending", _ACCREDITED_READY, assembly.params
    elif assembly.status is AssemblyStatus.OTHER_OWNER:
        reason, response, params = "other_owner_pending", _ACCREDITED_OTHER_OWNER, {}
    else:
        reason, params = assembly.reason, {}
        response = next((issue.detail for issue in assembly.issues if issue.detail), "")
    return _finish_admission(
        decision=AdmissionDecision(
            AdmissionRoute.CLARIFY, reason,
            input_shape(user_query, seed_resolved_params=seed_resolved_params),
            ContinuationOwner.ENTITY_PROFILE, response,
        ),
        profile_state=profile_state, contract=contract, status=status,
        revision=revision, params=params, effective_user_query=user_query,
        credential=credential, capability_spec=spec, assembly=assembly,
    )


def _finish_admission(
    *, decision, profile_state, contract, status, revision, params,
    effective_user_query, trusted=None, credential=None, capability_spec=None, assembly=None,
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
        credential=credential, capability_spec=capability_spec, assembly=assembly,
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
