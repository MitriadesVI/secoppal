"""UN turno de perfil en V1.2: del texto al resultado, sin lectores legacy.

`turn.py` compone una petición y la fusiona con la anterior; después
`build_execution_plan(request, clean)` recibe dos entradas que pueden describir
filtros distintos. Aquí el trayecto es lineal y cada paso tiene una autoridad:

    texto + params propios -> claims -> patch -> Frame -> plan -> evidencia

Nada de esta ruta lee `_entity_profile_request`, `_entity_profile_state` ni el
`state_context` heredado. El universo anterior entra por UNA puerta: el Frame
confirmado que publicó el turno anterior.

Y el pendiente no es un Frame: para ejecutarlo hay que aplicarlo sobre su base,
elegir su alternativa y volver a compilar. Una aclaración nunca altera lo
confirmado.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date as Date
from enum import Enum
import re
from typing import Any

from app.core.entity_profile.claim_adapters import (
    asks_to_clear_period,
    collect_claims,
    consume_clear_period_terms,
)
from app.core.entity_profile.claim_composition import (
    PatchProposal,
    buyer_scope_from,
    compose_patch,
)
from app.core.entity_profile.execution_record import (
    ExecutionRecord,
    build_execution_record,
)
from app.core.entity_profile.frame import BuyerScope, EntityProfileFrame
from app.core.entity_profile.frame_alternatives import capability_alternatives
from app.core.entity_profile.frame_compiler import PlanStatus, ProfilePlan, compile_profile
from app.core.entity_profile.frame_response import render_frame_profile
from app.core.entity_profile.frame_response import build_profile_sections
from app.core.entity_profile.frame_runner import ProfileEvidence, execute_plan
from app.core.entity_profile.presentation import project_entity_profile
from app.core.entity_profile.patch import (
    EntityProfilePatch,
    PatchOp,
    PatchOperation,
    PatchTarget,
    TargetKind,
    apply_patch,
)
from app.core.entity_profile.pending_resolution import PendingOutcome, resolve_pending
from app.core.entity_profile.profile_state import (
    PendingIssue,
    PendingProfile,
    ProfileConversationState,
    new_pending_id,
)
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.turn import (
    GEOGRAPHY_PARAM_KEYS,
    PROFILE_ACTIVE_PARAM,
    detect_entity_profile_turn,
    is_bare_affirmative,
    profile_query_params,
    territory_clarification,
    territory_only_name,
)
from app.core.entity_profile.followup import PROFILE_FOCUS_PARAM


# Razones de resolución que significan «esto no habla de un comprador», no
# «este comprador no existe».
_NO_BUYER = {"BUYER_SCOPE_REQUIRED", "PRODUCTIVE_BUYER_RESOLUTION_REQUIRED"}

_PENDING_UNRESOLVED = (
    "Todavía tengo una pregunta abierta sobre este análisis: {question}\n"
    "Puedes responderla o decirme «cancela ese cambio» para dejar el "
    "análisis como estaba."
)

_CANCELLED = "Listo, cancelé ese cambio. El análisis anterior sigue vigente."
_CANCELLED_EMPTY = "Listo, cancelé ese cambio. No queda un análisis anterior."


class TurnKind(str, Enum):
    ANSWER = "ANSWER"
    CLARIFICATION = "CLARIFICATION"
    INVALID_CONTEXT = "INVALID_CONTEXT"


@dataclass(frozen=True, slots=True)
class ProfileTurnOutcome:
    kind: TurnKind
    response: str
    state: ProfileConversationState
    route_reason: str
    frame: EntityProfileFrame | None = None
    plan: ProfilePlan | None = None
    evidence: ProfileEvidence | None = None
    record: ExecutionRecord | None = None
    profile_view: dict | None = None
    issues: tuple[str, ...] = ()

    @property
    def answered(self) -> bool:
        return self.kind is TurnKind.ANSWER

    @property
    def planned_jobs(self) -> int:
        return len(self.plan.jobs) if self.plan else 0

    @property
    def attempted_calls(self) -> int:
        return self.evidence.attempted_calls if self.evidence else 0


def is_profile_turn(
    user_query: str, params: dict, state: ProfileConversationState,
    *, bound_focus: str | None = None,
) -> tuple[bool, str | None]:
    """¿Este turno lo posee el perfil? El universo activo sale del ESTADO.

    ``bound_focus`` reemplaza SÓLO la detección — el mismo contrato que
    ``classify_entity_profile_turn(bound_focus=...)`` usa en el carril A2: una
    autoridad ya adjudicó el trabajo (SUPPLIERS) y el Frame sigue siendo quien
    compila, ejecuta y presenta. Una pendiente abierta sigue ganando: contestarla
    es lo que el turno significa.
    """
    if state.has_pending:
        return True, _pending_focus(state)
    if bound_focus:
        return True, bound_focus
    if state.confirmed_frame is None:
        return detect_entity_profile_turn(user_query, params)
    from app.core.contract_universe_intent import request
    dependent_filter = (params.get("fecha_desde") or params.get("fecha_hasta")
                        or request(user_query) is not None
                        or (re.match(r"^de\s+", user_query.strip(), re.I) and params.get("objeto")))
    if dependent_filter and not params.get("entidad") and not params.get("contratista"):
        return True, state.confirmed_frame.focus
    if asks_to_clear_period(user_query):
        # «sin filtro de fecha» sobre un perfil activo es un turno del perfil:
        # es la MISMA autoridad que produce el claim CLEAR_PERIOD, y sin esta
        # rama la petición caía al parser tabular y el CLEAR no llegaba nunca
        # al Frame (R7b).
        return True, state.confirmed_frame.focus
    probe = {
        **params,
        PROFILE_ACTIVE_PARAM: True,
        PROFILE_FOCUS_PARAM: state.confirmed_frame.focus,
    }
    return detect_entity_profile_turn(user_query, probe)


def run_profile_turn(
    *,
    user_query: str,
    params: dict,
    state: ProfileConversationState,
    turn_id: str,
    soql_builder: Any,
    secop_client: Any,
    today: Date | None = None,
    bound_focus: str | None = None,
) -> ProfileTurnOutcome | None:
    """El turno completo, o `None` si el perfil no es su dueño."""
    detected, focus = is_profile_turn(user_query, params, state, bound_focus=bound_focus)
    if not detected:
        return None

    clean = consume_clear_period_terms(user_query, profile_query_params(params))
    resolution = resolve_profile_scope(clean)
    buyer = buyer_scope_from(resolution)

    if buyer is None and state.confirmed_frame is None and not state.has_pending:
        blocked = _territory_block(user_query, params, resolution)
        return blocked  # None cuando el turno no habla de un comprador

    if state.has_pending:
        from app.core.entity_profile.pending_resolution import complete_reformulation
        replacement = complete_reformulation(user_query, clean, buyer, turn_id, soql_builder, today)
        if replacement:
            candidate, patch = replacement
            return _compile_and_execute(candidate, patch, state, None, soql_builder,
                                        secop_client, candidate.focus)
        resolved = _resolve_pending(
            user_query, clean, state, turn_id, buyer, soql_builder, secop_client,
            today,
        )
        if resolved is not None:
            return resolved

    base = _base_frame(state, buyer)
    if base is None and buyer is None:
        # Sin comprador propio y sin Frame confirmado no hay universo posible.
        return _territory_block(user_query, params, resolution)

    return _compose_and_run(
        user_query=user_query, clean=clean, base=base, buyer=buyer,
        turn_id=turn_id, state=state, soql_builder=soql_builder,
        secop_client=secop_client, resolution=resolution, focus=focus,
        today=today, bound_focus=bound_focus,
    )


# ── Composición normal ────────────────────────────────────────────────────

def _compose_and_run(
    *, user_query, clean, base, buyer, turn_id, state, soql_builder,
    secop_client, resolution, focus, today, bound_focus=None,
) -> ProfileTurnOutcome:
    claims = collect_claims(
        user_query, clean, turn_id=turn_id, buyer_resolution=resolution,
        today=today,
    )
    proposal = _bind_focus(
        compose_patch(claims, base_frame=base, buyer_scope=buyer), bound_focus,
    )
    if proposal.patch is None:
        if proposal.issues:
            return _clarify(
                state, base,
                tuple(PendingIssue(i.code, _issue_detail(i)) for i in proposal.issues),
                patch=_empty_patch(turn_id, base, buyer),
                alternatives=(),
            )
        return ProfileTurnOutcome(
            TurnKind.INVALID_CONTEXT,
            _invalid_text(proposal.reason), state,
            "entity_profile_invalid_context", issues=(proposal.reason or "",),
        )

    outcome = apply_patch(base, proposal.patch)
    if outcome.frame is None:
        return ProfileTurnOutcome(
            TurnKind.INVALID_CONTEXT, _invalid_text(outcome.reason), state,
            "entity_profile_invalid_context", issues=(outcome.reason or "",),
        )
    return _compile_and_execute(
        outcome.frame, proposal.patch, state, base, soql_builder, secop_client,
        focus,
    )


def _compile_and_execute(
    candidate, patch, state, base, soql_builder, secop_client, focus,
) -> ProfileTurnOutcome:
    decision = compile_profile(candidate, soql_builder=soql_builder)
    if decision.status is PlanStatus.NEEDS_CLARIFICATION:
        return _clarify(
            state, base,
            tuple(PendingIssue(i.code, i.detail) for i in decision.issues),
            patch=patch, alternatives=capability_alternatives(candidate),
        )
    if decision.status is PlanStatus.INVALID_CONTEXT:
        detail = decision.issues[0].detail if decision.issues else ""
        return ProfileTurnOutcome(
            TurnKind.INVALID_CONTEXT, _invalid_text(detail), state,
            "entity_profile_invalid_context",
            issues=tuple(i.code for i in decision.issues),
        )
    # READY: el SIGNIFICADO queda confirmado aquí, antes de tocar la red. Que
    # una fuente falle después no deshace lo que la persona pidió y el sistema
    # autorizó; confundir las dos cosas es lo que borraba el frame en V1.
    plan = decision.plan
    evidence = execute_plan(plan, secop_client)
    record = build_execution_record(plan, evidence, candidate)
    profile = build_profile_sections(candidate, plan, evidence, soql_builder)
    return ProfileTurnOutcome(
        TurnKind.ANSWER,
        render_frame_profile(candidate, plan, evidence, soql_builder, profile=profile),
        state.confirm(candidate),
        f"entity_profile_{plan.focus}",
        frame=candidate, plan=plan, evidence=evidence, record=record,
        profile_view=project_entity_profile(profile),
    )


# ── Pendiente ─────────────────────────────────────────────────────────────

def _resolve_pending(
    user_query, clean, state, turn_id, buyer, soql_builder, secop_client, today,
) -> ProfileTurnOutcome | None:
    """Traduce el desenlace de la pendiente a un resultado del turno.

    Decidir cuál es el desenlace es de `pending_resolution`; ejecutarlo, de
    aquí. La separación existe para que «se eligió una alternativa» y «se puede
    consultar» no acaben siendo la misma condición.
    """
    decision = resolve_pending(
        user_query=user_query, params=clean, state=state, turn_id=turn_id,
        buyer=buyer, today=today,
    )
    pending = state.pending
    if decision.outcome is PendingOutcome.SUPERSEDED:
        return None
    if decision.outcome is PendingOutcome.CANCELLED:
        kept = decision.state.confirmed_frame
        return ProfileTurnOutcome(
            TurnKind.ANSWER if kept else TurnKind.CLARIFICATION,
            _CANCELLED if kept else _CANCELLED_EMPTY,
            decision.state, "entity_profile_pending_cancelled",
        )
    if decision.outcome is PendingOutcome.REPEATED:
        return ProfileTurnOutcome(
            TurnKind.CLARIFICATION, pending.question, state,
            "entity_profile_pending_repeated",
            issues=tuple(issue.code for issue in pending.issues),
        )
    if decision.outcome is PendingOutcome.UNRESOLVED:
        return ProfileTurnOutcome(
            TurnKind.CLARIFICATION,
            _PENDING_UNRESOLVED.format(question=_first_line(pending.question)),
            state, "entity_profile_pending_unresolved",
            issues=tuple(issue.code for issue in pending.issues),
        )
    return _compile_and_execute(
        decision.candidate, decision.patch, decision.state,
        state.confirmed_frame, soql_builder, secop_client,
        decision.candidate.focus,
    )


# ── Utilidades ────────────────────────────────────────────────────────────

def _clarify(state, base, issues, *, patch, alternatives) -> ProfileTurnOutcome:
    pending = PendingProfile(
        pending_id=new_pending_id(), patch=patch,
        question=_question_text(issues, alternatives),
        base_frame_ref=(base.frame_id, base.revision) if base else None,
        issues=issues, alternatives=alternatives,
    )
    return ProfileTurnOutcome(
        TurnKind.CLARIFICATION, pending.question, state.hold(pending),
        "entity_profile_scope_clarification",
        issues=tuple(issue.code for issue in issues),
    )


def _question_text(issues, alternatives) -> str:
    parts = [issue.detail or issue.code for issue in issues]
    question = " ".join(parts).strip() or "Necesito precisar este análisis."
    if alternatives:
        options = "\n".join(
            f"{index}. {alternative.label}"
            for index, alternative in enumerate(alternatives, start=1)
        )
        question = f"{question}\n{options}"
    return question


def _first_line(question: str) -> str:
    return question.splitlines()[0] if question else ""


def _pending_focus(state: ProfileConversationState) -> str | None:
    return state.confirmed_frame.focus if state.confirmed_frame else None


def _empty_patch(turn_id, base, buyer) -> EntityProfilePatch:
    """Un pendiente sin operaciones: la pregunta existe, el cambio todavía no."""
    return EntityProfilePatch(
        patch_id=f"pending:{turn_id}", turn_id=turn_id, operations=(),
        base_frame_ref=(base.frame_id, base.revision) if base else None,
        new_buyer_scope=buyer if base is None else None,
    )


def _bind_focus(proposal: PatchProposal, bound_focus: str | None) -> PatchProposal:
    """C4B: el trabajo ACREDITADO fija el foco del Frame con una operación tipada.

    Una credencial ``entity_profile.suppliers`` ya adjudicó qué trabajo es este
    turno, así que el foco de la capability se declara con la MISMA operación que
    usa el texto —``SET`` sobre ``TargetKind.FOCUS``— y reemplaza la que las
    claims hayan producido: dos ``SET`` sobre el mismo target con valores
    distintos invalidarían el parche (``CONFLICTING_OPERATIONS``), y el foco
    acreditado no puede depender de si la paráfrasis nombró el sustantivo.
    Nada más del parche cambia; compilar, ejecutar y presentar siguen siendo del
    Frame.
    """
    if bound_focus is None or proposal.patch is None:
        return proposal
    operations = tuple(
        op for op in proposal.patch.operations
        if op.target.kind is not TargetKind.FOCUS
    ) + (
        PatchOp(target=PatchTarget(TargetKind.FOCUS), operation=PatchOperation.SET,
                value=bound_focus),
    )
    return replace(proposal, patch=replace(proposal.patch, operations=operations))


def _base_frame(
    state: ProfileConversationState, buyer: BuyerScope | None,
) -> EntityProfileFrame | None:
    """El universo anterior entra por UNA puerta: el Frame confirmado."""
    confirmed = state.confirmed_frame
    if confirmed is None:
        return None
    if buyer is not None and _is_other_buyer(buyer, state):
        # Otro comprador es otra búsqueda: no hereda actor, tema ni filtros.
        return None
    return confirmed


def _is_other_buyer(buyer: BuyerScope, state: ProfileConversationState) -> bool:
    confirmed = state.confirmed_frame
    if confirmed is None:
        return False
    return buyer.canonical_buyers != confirmed.buyer_scope.canonical_buyers


def _territory_block(user_query, params, resolution) -> ProfileTurnOutcome | None:
    territory_only = territory_only_name(user_query)
    has_geography = any(params.get(key) for key in GEOGRAPHY_PARAM_KEYS)
    if (
        resolution.scope is None
        and resolution.reason in _NO_BUYER
        and not territory_only
        and not has_geography
    ):
        return None
    reason = resolution.reason
    detail = (
        reason if reason and reason not in _NO_BUYER
        else territory_clarification(user_query)
    )
    return ProfileTurnOutcome(
        TurnKind.CLARIFICATION, detail,
        ProfileConversationState(), "entity_profile_scope_clarification",
        issues=(reason or "BUYER_SCOPE_REQUIRED",),
    )


def _issue_detail(issue) -> str:
    detail = getattr(issue, "detail", "")
    if detail:
        return detail
    terms = getattr(issue, "terms", ())
    return f"{issue.code}: {', '.join(terms)}" if terms else issue.code


def _invalid_text(reason: str | None) -> str:
    return (
        "No pude conservar el contexto de este análisis"
        + (f" ({reason})." if reason else ".")
        + " Vuelve a decirme qué entidad quieres analizar."
    )


def _fold(text: str) -> str:
    import unicodedata

    decomposed = unicodedata.normalize("NFD", text.casefold())
    return "".join(char for char in decomposed if not unicodedata.combining(char))


__all__ = [
    "ProfileTurnOutcome",
    "TurnKind",
    "is_profile_turn",
    "run_profile_turn",
]
