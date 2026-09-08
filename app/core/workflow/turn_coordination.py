"""Serial admission/execution/publication with private continuity writes."""
from contextlib import nullcontext
from dataclasses import replace

from app.config import entity_profile_frame_enabled
from app.core.conversation_store import is_reset_command
from app.core.store_lock import conversation_lock
from app.core.workflow.continuity_staging import stage_continuity


def coordinate_turn(workflow, ctx, seed):
    if is_reset_command(ctx.user_query):
        return workflow._dispatch(ctx, None)
    if not entity_profile_frame_enabled():
        from app.core.conversation_envelope import EnvelopeStatus
        from app.core.workflow.results import empty_result
        read = workflow.conv_store.load_envelope(ctx.chat_id) if ctx.chat_id else None
        if read and read.status is not EnvelopeStatus.EMPTY:
            return empty_result(response="Este contexto requiere la versión Frame activa. Inicia una conversación nueva con /reset para buscar.",
                                route_reason="admission_rollback_incompatible", needs_clarification=True)
        return workflow._dispatch(ctx, seed)
    store = workflow.conv_store
    lock = (conversation_lock(store._envelope_path_for(ctx.chat_id).with_suffix('.turn'), timeout=120)
            if ctx.chat_id else nullcontext())
    with lock:
        reservation = store.reserve_turn(ctx.chat_id) if ctx.chat_id else None
        with stage_continuity(store, reservation):
            return _execute(workflow, ctx, seed, reservation)


def _execute(workflow, ctx, seed_resolved_params, reservation):
    from app.core.workflow.admission import admit
    from app.core.workflow.admission_rules import (
        CONTINUATION_REF_KEY,
        AdmissionRoute,
    )
    from app.core.workflow.profile_route import run_profile_route
    from app.core.workflow.results import empty_result
    from app.core.workflow.turn_publication import (
        delegated_contract,
        profile_contract,
        publish,
    )

    deps = workflow.deps
    # Paso 1 del diseño: reservar serialmente el turno ANTES de ejecutar.
    # La misma revisión que se lee aquí es la que se comprueba al publicar.
    admitted = admit(
        deps,
        user_query=ctx.user_query,
        chat_id=ctx.chat_id,
        seed_resolved_params=seed_resolved_params,
        today=ctx.today,
        read=reservation.previous if reservation else None,
        actor_candidate_selection=ctx.actor_candidate_selection,
    )

    # La referencia de continuación es de la ADMISIÓN: ya cumplió su
    # función y no puede seguir viaje hacia el builder como si fuera un
    # filtro material.
    dispatch_seed = (
        {k: v for k, v in seed_resolved_params.items() if k != CONTINUATION_REF_KEY}
        if seed_resolved_params else seed_resolved_params
    )

    from app.core.workflow.continuation import ContinuationOwner
    from app.core.workflow.admission_rules import InputShape, AdmissionDecision
    from app.core.workflow.tabular_capability import validate_tabular, export_params
    from app.core.workflow.handlers.pagination import _requested_group
    from app.core.radar.commands import parse_radar_command, SAVE

    ctx = replace(ctx, publication_required=True)
    command = parse_radar_command(ctx.user_query)
    if command and command.kind == SAVE:
        params = export_params(deps.conv_store, admitted.contract, ctx.chat_id) if admitted.contract else None
        ctx = replace(ctx, export_search_params=params)
        if params is None:
            admitted = replace(admitted, decision=AdmissionDecision(
                AdmissionRoute.CLARIFY, "radar_requires_exportable_search", InputShape.TEXT,
                admitted.contract.owner if admitted.contract else ContinuationOwner.NONE,
                "Este resultado no es una búsqueda tabular exportable. Formula una búsqueda con sus filtros y luego puedes guardarla como Radar."))
    pagination = admitted.decision.shape is InputShape.PAGINATION or _requested_group(ctx.user_query)
    if pagination and admitted.route is not AdmissionRoute.CLARIFY:
        try:
            if not reservation or not admitted.contract or admitted.contract.owner is not ContinuationOwner.TABULAR:
                raise ValueError("No hay un cursor tabular vigente para continuar.")
            referent = validate_tabular(deps.conv_store, reservation, admitted.contract, ctx.user_query)
            if referent is None:
                raise ValueError("Esta búsqueda tiene dos grupos. ¿Quieres más procesos o más contratos?")
            ctx = replace(ctx, tabular_referent=referent)
        except (ValueError, KeyError, TypeError) as exc:
            admitted = replace(admitted, decision=AdmissionDecision(
                AdmissionRoute.CLARIFY, "invalid_tabular_cursor", InputShape.PAGINATION,
                admitted.contract.owner if admitted.contract else ContinuationOwner.NONE, str(exc)))

    outcome = None
    if admitted.route is AdmissionRoute.CLARIFY:
        # Una continuación inválida termina aquí: cero consultas, ningún
        # offset inventado y ninguna caída por defecto al parser tabular.
        result = empty_result(
            response=admitted.decision.response,
            route_reason=f"admission_{admitted.decision.reason}",
            needs_clarification=True,
        )
    elif admitted.route is AdmissionRoute.PROFILE:
        routed = run_profile_route(
            deps, ctx,
            params=admitted.params,
            state=admitted.profile_state,
            turn_id=reservation.turn_id if reservation else "anonymous",
        )
        if routed is None:
            result = workflow._dispatch(ctx, dispatch_seed)
        else:
            result, outcome = routed.result, routed.outcome
    else:
        result = workflow._dispatch(ctx, dispatch_seed)

    if admitted.route is not AdmissionRoute.DELEGATED:
        # Las rutas que la admisión atiende por su cuenta no pasan por
        # ningún handler, así que su traza se registra aquí. NO se añade un
        # turno legacy: el sobre es la autoridad del contexto, y escribir
        # además en el historial dejaría params heredables de un turno cuyo
        # significado ya vive en el Frame.
        result["trace_id"] = deps.feedback.log_trace(
            ctx.user_query, ctx.channel, result,
            user_identity=ctx.user_identity,
        )

    if result.get("route_reason") == "reset":
        # `handle_reset` ya invalidó la continuidad borrando el sobre;
        # publicar encima recrearía un contrato que el reset acaba de negar.
        return result

    if admitted.route is AdmissionRoute.CLARIFY and admitted.contract and reservation:
        from app.core.workflow.turn_publication import preserved_contract
        publish(deps, reservation, result, user_query=ctx.user_query,
                profile_state=admitted.profile_state,
                contract=preserved_contract(reservation, admitted.contract),
                execution_record=reservation.previous.envelope.execution_record, preserve_generation=True)
        return result

    if outcome is not None:
        publish(
            deps, reservation, result,
            user_query=ctx.user_query,
            profile_state=outcome.state,
            contract=profile_contract(reservation, outcome) if reservation else None,
            execution_record=(
                outcome.record.to_dict() if outcome.record else (
                    reservation.previous.envelope.execution_record
                    if reservation and reservation.previous.usable and outcome.state.confirmed_frame else None
                )
            ),
        )
        return result

    from app.core.entity_profile.profile_state import ProfileConversationState

    # Un turno que no es del perfil deja de tener un perfil activo: el
    # universo visible es otro, y conservar el Frame confirmado haría que
    # un «proveedores» posterior profundizara en algo que ya no se ve.
    publish(
        deps, reservation, result,
        user_query=ctx.user_query,
        profile_state=ProfileConversationState(),
        contract=delegated_contract(reservation, result, deps.conv_store) if reservation else None,
    )
    return result

