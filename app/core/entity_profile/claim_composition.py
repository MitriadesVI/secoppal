"""De ocurrencias vinculadas a un parche tipado, sin reasignar sujetos.

Ésta es la pieza donde V1.1 perdía la mitad de la pregunta. Su composición
miraba el TIPO del evento y le asignaba la fuente físicamente compatible:
"publicación" iba a procesos porque procesos tienen fecha de publicación. Con
eso, "contratos publicados" se contestaba con procesos y nadie se enteraba.

Aquí el sujeto sale de la relación que el adaptador acreditó, no de la tabla de
compatibilidad. Si el sujeto expresado y el hecho son incompatibles, el
compilador lo bloquea; la composición conserva lo que la persona dijo.

Un filtro sin destinatario expreso —un tema, un límite de valor, un periodo
que ninguna cláusula reclamó— se distribuye una vez a todas las fuentes del
turno. Distribuir no es inventar: es lo que significa un filtro común.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import uuid

from app.core.entity_profile.claims import (
    ClaimDimension,
    ClaimSet,
    RelationKind,
    SemanticClaim,
    UnresolvedIssue,
)
from app.core.entity_profile.frame import (
    BuyerScope,
    BuyerScopeKind,
    EntityProfileFrame,
    SourcePredicates,
    SourceSelection,
    TemporalPredicate,
)
from app.core.entity_profile.frame_values import (
    UNBOUNDED_PERIOD,
    VALUE_MAGNITUDE,
    Period,
    PredicateOrigin,
    Selection,
    SourceKind,
    SourceOrigin,
    StateExpression,
    StatePredicate,
    TemporalEventKind,
    TopicNode,
    ValueConstraint,
)
from app.core.entity_profile.patch import (
    EntityProfilePatch,
    PatchOp,
    PatchOperation,
    PatchTarget,
    TargetKind,
)


# Una cuestión sin resolver que no puede convertirse en un universo ejecutable.
BLOCKING_ISSUES = frozenset({
    "UNSUPPORTED_EVENT",
    "CONTRACT_UNIVERSE_CONFLICT",
    "SOURCE_CHOICE_AMBIGUOUS",
    "AMBIGUOUS_PERIOD_ANCHOR",
    "EVENT_SUBJECT_UNRESOLVED",
    "CONTRACT_TYPE_EXCLUDE_UNSUPPORTED",
    "LEGACY_STATE_INVALID_CONTEXT",
    "TEXT_OFFSETS_UNMAPPABLE",
})

_DEFAULT_SOURCES = frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS})


@dataclass(frozen=True, slots=True)
class PatchProposal:
    """El parche del turno, o las cuestiones que impiden construirlo."""

    patch: EntityProfilePatch | None
    issues: tuple[UnresolvedIssue, ...] = ()
    reason: str | None = None


def buyer_scope_from(resolution: Any) -> BuyerScope | None:
    """Proyecta la resolución de comprador existente a su valor del Frame."""
    scope = getattr(resolution, "scope", None)
    identity = getattr(resolution, "identity", None)
    if scope is None or identity is None:
        return None
    return BuyerScope(
        kind=(
            BuyerScopeKind.BUYER_GROUP
            if scope.buyer_group_id else BuyerScopeKind.BUYER_UNIT
        ),
        display_name=identity.display_name,
        canonical_buyers=tuple(identity.canonical_buyers),
        authority=identity.resolution_authority,
        authority_version="1",
        group_id=scope.buyer_group_id,
    )


def compose_patch(
    claims: ClaimSet,
    *,
    base_frame: EntityProfileFrame | None = None,
    buyer_scope: BuyerScope | None = None,
) -> PatchProposal:
    """Ocurrencias -> operaciones. Lo que el turno calló no se toca."""
    blocking = tuple(i for i in claims.unresolved if i.code in BLOCKING_ISSUES)
    if blocking:
        return PatchProposal(None, blocking)

    sources = claims.by_dimension(ClaimDimension.SOURCE)
    selection = _selection(sources, base_frame)
    if selection is None:
        return PatchProposal(None, (), "SOURCE_SELECTION_REQUIRED")
    if base_frame is None and buyer_scope is None:
        return PatchProposal(None, (), "BUYER_REQUIRED")

    operations: list[PatchOp] = []
    if sources or base_frame is None:
        operations.append(_op(TargetKind.SOURCE_SELECTION, selection, sources))

    focus = _focus(claims)
    if focus is not None:
        operations.append(_op(TargetKind.FOCUS, focus, ()))

    cleared = _clears(claims)
    for source in selection.ordered:
        operations.extend(_source_operations(claims, source, sources, cleared))

    patch_id = str(uuid.uuid4())
    return PatchProposal(EntityProfilePatch(
        patch_id=patch_id,
        turn_id=claims.turn_id,
        operations=tuple(operations),
        base_frame_ref=(
            (base_frame.frame_id, base_frame.revision) if base_frame else None
        ),
        new_buyer_scope=buyer_scope if base_frame is None else None,
    ))


def _op(kind: TargetKind, value, evidence, source: SourceKind | None = None) -> PatchOp:
    return PatchOp(
        target=PatchTarget(kind, source),
        operation=PatchOperation.SET,
        value=value,
        evidence_refs=tuple(c.id for c in evidence),
    )


def _selection(
    sources: tuple[SemanticClaim, ...], base: EntityProfileFrame | None,
) -> SourceSelection | None:
    if sources:
        return SourceSelection(
            requested=frozenset(c.canonical_value for c in sources),
            origin=SourceOrigin.EXPLICIT,
        )
    if base is not None:
        return base.source_selection
    return SourceSelection(requested=_DEFAULT_SOURCES, origin=SourceOrigin.DEFAULT)


def _focus(claims: ClaimSet) -> str | None:
    found = claims.by_dimension(ClaimDimension.FOCUS)
    return found[0].canonical_value if found else None


def _clears(claims: ClaimSet) -> frozenset[TargetKind]:
    operations = {
        c.canonical_value for c in claims.by_dimension(ClaimDimension.OPERATION)
    }
    return frozenset(
        {TargetKind.TEMPORAL} if "CLEAR_PERIOD" in operations else ()
    )


def _source_operations(
    claims: ClaimSet,
    source: SourceKind,
    sources: tuple[SemanticClaim, ...],
    cleared: frozenset[TargetKind],
) -> list[PatchOp]:
    owner = next((c for c in sources if c.canonical_value is source), None)
    operations: list[PatchOp] = []

    if TargetKind.TEMPORAL in cleared:
        operations.append(PatchOp(
            PatchTarget(TargetKind.TEMPORAL, source), PatchOperation.CLEAR,
        ))
    else:
        temporal = _temporal(claims, owner)
        if temporal is not None:
            operations.append(_op(TargetKind.TEMPORAL, temporal[0], temporal[1], source))

    universe = _single(claims, ClaimDimension.CONTRACT_UNIVERSE)
    if universe is not None and source is SourceKind.CONTRACTS:
        operations.append(_op(TargetKind.CONTRACT_UNIVERSE, universe.canonical_value, (universe,), source))

    state = _state(claims, owner)
    if state is not None:
        operations.append(_op(TargetKind.CURRENT_STATE, state[0], state[1], source))

    topic = _single(claims, ClaimDimension.TOPIC)
    if topic is not None:
        operations.append(_op(TargetKind.TOPIC, topic.canonical_value, (topic,), source))

    modality = _single(claims, ClaimDimension.MODALITY)
    if modality is not None:
        operations.append(
            _op(TargetKind.MODALITY, modality.canonical_value, (modality,), source),
        )

    contract_type = _single(claims, ClaimDimension.CONTRACT_TYPE)
    if contract_type is not None:
        operations.append(_op(
            TargetKind.CONTRACT_TYPE, contract_type.canonical_value,
            (contract_type,), source,
        ))

    value = _single(claims, ClaimDimension.VALUE)
    if value is not None:
        constraint: ValueConstraint = value.canonical_value
        operations.append(_op(
            TargetKind.VALUE,
            ValueConstraint(
                lower=constraint.lower, upper=constraint.upper,
                magnitude=VALUE_MAGNITUDE[source], currency=constraint.currency,
            ),
            (value,), source,
        ))
    return operations


def _temporal(claims: ClaimSet, owner: SemanticClaim | None):
    """El hecho y la ventana que ESTA fuente nombró, o el periodo común."""
    events = [
        c for c in claims.by_dimension(ClaimDimension.EVENT)
        if owner is not None and owner.id in c.related(RelationKind.HAS_SUBJECT)
    ]
    periods = claims.by_dimension(ClaimDimension.PERIOD)
    own_period = None
    event_kind = TemporalEventKind.NONE
    evidence: list[SemanticClaim] = []

    if events:
        event = events[0]
        event_kind = event.canonical_value
        evidence.append(event)
        own_period = next(
            (p for p in periods if event.id in p.related(RelationKind.PERIOD_OF)), None,
        )
    if own_period is None and owner is not None:
        own_period = next(
            (p for p in periods if owner.id in p.related(RelationKind.PERIOD_OF)), None,
        )
    if own_period is None:
        # Un periodo que ninguna cláusula reclamó es un filtro común del turno.
        own_period = next(
            (p for p in periods if not p.related(RelationKind.PERIOD_OF)), None,
        )
    if own_period is not None:
        evidence.append(own_period)
    if not evidence:
        return None
    period: Period = own_period.canonical_value if own_period else UNBOUNDED_PERIOD
    return TemporalPredicate(
        event=event_kind,
        period=period,
        event_origin=(
            PredicateOrigin.EXPLICIT if events else PredicateOrigin.DEFAULT
        ),
        period_origin=(
            PredicateOrigin.EXPLICIT if own_period else PredicateOrigin.DEFAULT
        ),
    ), tuple(evidence)


def _state(claims: ClaimSet, owner: SemanticClaim | None):
    """El estado de hoy que MODIFICA a esta fuente, sobre su propio esquema."""
    if owner is None:
        found = [
            c for c in claims.by_dimension(ClaimDimension.CURRENT_STATE)
            if not c.related(RelationKind.MODIFIES)
        ]
    else:
        found = [
            c for c in claims.by_dimension(ClaimDimension.CURRENT_STATE)
            if owner.id in c.related(RelationKind.MODIFIES)
        ]
    if not found:
        return None
    predicates: list[StatePredicate] = [c.canonical_value for c in found]
    return StateExpression(tuple(predicates)), tuple(found)


def _single(claims: ClaimSet, dimension: ClaimDimension) -> SemanticClaim | None:
    found = claims.by_dimension(dimension)
    return found[0] if len(found) == 1 else None


__all__ = [
    "BLOCKING_ISSUES",
    "PatchProposal",
    "SourcePredicates",
    "Selection",
    "TopicNode",
    "buyer_scope_from",
    "compose_patch",
]
