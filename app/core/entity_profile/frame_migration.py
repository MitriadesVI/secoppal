"""Leer un contexto de V1/V1.1 sin preferir en silencio una de dos copias.

Un turno viejo guardaba el significado dos veces: en el request serializado y
en los params materiales. Cuando las dos copias discrepan —el request dice
agosto y los params dicen julio, o no dicen nada— no hay forma de saber cuál
describe lo que la persona aceptó. V1 elegía una. Ese es el defecto: la
elección no la puede hacer el lector.

Aquí una discrepancia INVALIDA el contexto. Perder el hilo de una conversación
vieja es recuperable; contestar con un universo que nadie pidió no lo es.

Este migrador es PURO: lee y devuelve. No escribe histórico, no toca
`ConversationStore` y ningún lector productivo lo llama todavía.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any
import uuid

from app.core.entity_profile.frame import (
    BuyerScope,
    BuyerScopeKind,
    EntityProfileFrame,
    FrameProvenance,
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
    TopicAll,
    ValueConstraint,
    decimal_or_none,
)
from app.core.entity_profile.legacy_equivalence import (
    legacy_contract_type,
    legacy_modality,
    legacy_topic,
    material_conflict,
)


_NAMESPACE = uuid.UUID("9a1d2f60-77c1-4a5f-8b6a-2c1d3e4f5a6b")

_LEGACY_SOURCES = {
    "PROCESSES": frozenset({SourceKind.PROCESSES}),
    "CONTRACTS": frozenset({SourceKind.CONTRACTS}),
    "BOTH": frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS}),
}

_STATE_FIELDS = {
    SourceKind.PROCESSES: frozenset(
        {"estado_del_procedimiento", "estado_de_apertura_del_proceso"},
    ),
    SourceKind.CONTRACTS: frozenset({"estado_contrato", "estado_contrato_adicional"}),
}


class MigrationStatus(str, Enum):
    MIGRATED = "MIGRATED"
    INVALID_CONTEXT = "INVALID_CONTEXT"
    BLOCKED = "BLOCKED"
    NEEDS_CLARIFICATION = "NEEDS_CLARIFICATION"


@dataclass(frozen=True, slots=True)
class MigrationOutcome:
    status: MigrationStatus
    frame: EntityProfileFrame | None = None
    reason: str | None = None
    blocked_events: tuple[str, ...] = ()


def migrate_legacy_context(request: Any, params: dict) -> MigrationOutcome:
    """Contexto legacy -> Frame, sólo con equivalencia completa acreditada."""
    if not isinstance(request, dict) or not isinstance(params, dict):
        return MigrationOutcome(MigrationStatus.INVALID_CONTEXT, reason="MALFORMED")

    try:
        focus = str(request["focus"])
        sources = _LEGACY_SOURCES[str(request["sources"])]
        origin = _origin(request)
    except (KeyError, TypeError, ValueError):
        return MigrationOutcome(MigrationStatus.INVALID_CONTEXT, reason="MALFORMED")

    spans = _validate_claim_spans(request)
    if spans is not None:
        return MigrationOutcome(MigrationStatus.INVALID_CONTEXT, reason=spans)

    blocked = tuple(
        str(term) for term in request.get("unsupported_events") or () if term
    )
    if not blocked and request.get("unsupported_event"):
        blocked = (str(request["unsupported_event"]),)
    if blocked:
        # Se conserva el bloqueo. Descartarlo y ejecutar contestaría con otra
        # fecha la pregunta que el bloqueo existía para no contestar.
        return MigrationOutcome(MigrationStatus.BLOCKED, blocked_events=blocked)

    period = _period(request, params)
    if isinstance(period, str):
        return MigrationOutcome(MigrationStatus.INVALID_CONTEXT, reason=period)

    conflict = material_conflict(request, params, sources)
    if conflict is not None:
        return MigrationOutcome(MigrationStatus.INVALID_CONTEXT, reason=conflict)

    buyer = _buyer(params)
    if buyer is None:
        return MigrationOutcome(
            MigrationStatus.INVALID_CONTEXT, reason="BUYER_NOT_ACCREDITED",
        )

    state = _state(params, sources)
    if isinstance(state, str):
        return MigrationOutcome(MigrationStatus.NEEDS_CLARIFICATION, reason=state)

    topic = legacy_topic(request)
    events = _events(request)
    selection = SourceSelection(requested=sources, origin=origin)
    predicates = {
        source: SourcePredicates(
            source=source,
            temporal=TemporalPredicate(
                event=events.get(source, TemporalEventKind.NONE),
                period=period,
                event_origin=(
                    PredicateOrigin.EXPLICIT if source in events
                    else PredicateOrigin.DEFAULT
                ),
                period_origin=(
                    PredicateOrigin.EXPLICIT if not period.is_unbounded
                    else PredicateOrigin.DEFAULT
                ),
            ),
            current_state=state.get(source, StateExpression()),
            topic=topic,
            modality=legacy_modality(request),
            contract_type=legacy_contract_type(request),
            value=_value(request, source),
        )
        for source in selection.ordered
    }
    frame_id = str(uuid.uuid5(_NAMESPACE, repr(sorted(request.items(), key=str))))
    try:
        frame = EntityProfileFrame(
            frame_id=frame_id, revision=1, buyer_scope=buyer, focus=focus,
            source_selection=selection, source_predicates=predicates,
            provenance=FrameProvenance(authorities=(("legacy_migration", "1"),)),
        )
    except ValueError as exc:
        return MigrationOutcome(MigrationStatus.INVALID_CONTEXT, reason=str(exc))
    return MigrationOutcome(MigrationStatus.MIGRATED, frame=frame)


def _origin(request: dict) -> SourceOrigin:
    """V1 no guardaba el origen; su provenance del turno era lo único que había."""
    raw = request.get("sources_origin") or request.get("sources_provenance")
    return SourceOrigin.EXPLICIT if raw == "EXPLICIT" else SourceOrigin.DEFAULT


def _validate_claim_spans(request: dict) -> str | None:
    for claim in request.get("temporal_claims") or ():
        if not isinstance(claim, dict):
            return "MALFORMED_CLAIM"
        span = claim.get("span")
        if not isinstance(span, (list, tuple)) or len(span) != 2:
            return "MALFORMED_SPAN"
        for value in span:
            if isinstance(value, bool) or not isinstance(value, int):
                return "MALFORMED_SPAN"
        if span[0] < 0 or span[1] <= span[0]:
            return "MALFORMED_SPAN"
        try:
            TemporalEventKind(claim["event"])
        except (KeyError, ValueError):
            return "MALFORMED_CLAIM"
    return None


def _period(request: dict, params: dict) -> Period | str:
    """Las dos copias del periodo deben decir lo mismo, o no hay contexto."""
    stated = (request.get("time_from"), request.get("time_to"))
    material = (params.get("fecha_desde"), params.get("fecha_hasta"))
    if stated == (None, None) and material == (None, None):
        return UNBOUNDED_PERIOD
    if stated != material:
        return "PERIOD_CONFLICT"
    return Period(lower=stated[0], upper=stated[1])


def _events(request: dict) -> dict[SourceKind, TemporalEventKind]:
    events: dict[SourceKind, TemporalEventKind] = {}
    for claim in request.get("temporal_claims") or ():
        kind = TemporalEventKind(claim["event"])
        if kind.source is not None:
            events[kind.source] = kind
    if not events:
        raw = request.get("event")
        if raw and raw != TemporalEventKind.NONE.value:
            kind = TemporalEventKind(raw)
            if kind.source is not None:
                events[kind.source] = kind
    return events


def _buyer(params: dict) -> BuyerScope | None:
    name = params.get("entidad_resolved") or params.get("entidad")
    if not isinstance(name, str) or not name.strip():
        return None
    group_id = params.get("buyer_group_id")
    return BuyerScope(
        kind=BuyerScopeKind.BUYER_GROUP if group_id else BuyerScopeKind.BUYER_UNIT,
        display_name=name,
        canonical_buyers=(name,),
        authority="legacy_params",
        authority_version="1",
        group_id=group_id,
    )


def _state(params: dict, sources: frozenset[SourceKind]):
    """El estado legacy se proyecta sólo si su esquema es inequívoco."""
    states: dict[SourceKind, StateExpression] = {}
    for source in sources:
        predicates = [
            StatePredicate(field, tuple(_values(params[field])))
            for field in sorted(_STATE_FIELDS[source])
            if params.get(field)
        ]
        if predicates:
            states[source] = StateExpression(tuple(predicates))
    if not params.get("estado"):
        return states
    field = params.get("estado_field")
    if not isinstance(field, str) or not field:
        return "AMBIGUOUS_STATE_FIELD"
    owners = [source for source in sources if field in _STATE_FIELDS[source]]
    if len(owners) != 1:
        return "AMBIGUOUS_STATE_FIELD"
    existing = states.get(owners[0], StateExpression())
    states[owners[0]] = StateExpression(
        existing.predicates + (StatePredicate(field, tuple(_values(params["estado"]))),),
    )
    return states


def _value(request: dict, source: SourceKind) -> ValueConstraint:
    lower = decimal_or_none(request.get("value_min"))
    upper = decimal_or_none(request.get("value_max"))
    if lower is None and upper is None:
        return ValueConstraint()
    return ValueConstraint(lower=lower, upper=upper, magnitude=VALUE_MAGNITUDE[source])


def _values(raw: object) -> list[str]:
    if isinstance(raw, (list, tuple)):
        return [str(v) for v in raw]
    return [str(raw)]
