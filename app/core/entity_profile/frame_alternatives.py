"""Qué salidas se le ofrecen a una aclaración de capacidad, y con qué operaciones.

El diseño es explícito: «Una alternativa es una lista de operaciones; no otra
copia del Frame». Y en «procesos en agosto → proveedores → solo contratos», «la
alternativa publicada debe explicitar que contratos usa fecha de firma y
conserva agosto».

Eso último no sale gratis. Al cambiar de fuente, `_reshape` da predicados
LIMPIOS a la fuente que entra: la fecha de firma de un contrato no describe la
publicación de un proceso, y heredarla por intuición contestaría otra pregunta.
Por eso la transposición del periodo tiene que estar ESCRITA en la alternativa,
como una operación tipada que la persona acepta al elegirla.

Se transponen sólo las dimensiones que no pertenecen al esquema de una fuente
—periodo, tema, modalidad, valor— y sólo cuando las fuentes que salen dicen lo
mismo. El estado actual NO se transpone: `estado_contrato` no existe en procesos.
"""
from __future__ import annotations

from dataclasses import replace

from app.core.entity_profile.frame import (
    EntityProfileFrame,
    SourcePredicates,
    SourceSelection,
    TemporalPredicate,
)
from app.core.entity_profile.focus_labels import focus_label
from app.core.entity_profile.frame_compiler import FOCUS_CAPABILITY
from app.core.entity_profile.frame_render import event_verb, humanize_period
from app.core.entity_profile.frame_values import (
    VALUE_MAGNITUDE,
    PredicateOrigin,
    SourceKind,
    SourceOrigin,
    TemporalEventKind,
)
from app.core.entity_profile.patch import (
    PatchOp,
    PatchOperation,
    PatchTarget,
    TargetKind,
)
from app.core.entity_profile.profile_state import Alternative


# Dimensiones que describen el PEDIDO, no el esquema físico de una fuente.
# `current_state` y `contract_type` quedan fuera a propósito.
_TRANSPOSABLE = (TargetKind.TOPIC, TargetKind.MODALITY, TargetKind.VALUE)


def capability_alternatives(
    candidate: EntityProfileFrame,
) -> tuple[Alternative, ...]:
    """La salida para «este foco no se puede responder sobre esas fuentes»."""
    capable = FOCUS_CAPABILITY.get(candidate.focus)
    if not capable:
        return ()
    requested = candidate.source_selection.requested
    if requested <= capable:
        return ()
    target_sources = frozenset(capable)
    dropped = tuple(s for s in candidate.sources if s not in target_sources)
    incoming = tuple(
        s for s in SourceKind
        if s in target_sources and s not in requested
    )
    operations: list[PatchOp] = [PatchOp(
        target=PatchTarget(TargetKind.SOURCE_SELECTION),
        operation=PatchOperation.SET,
        value=SourceSelection(requested=target_sources, origin=SourceOrigin.EXPLICIT),
    )]
    for source in incoming:
        operations.extend(_transposed(candidate, source, dropped))
    return (Alternative(
        alternative_id="capable_sources",
        label=_label(candidate, target_sources, dropped),
        completion_operations=tuple(operations),
    ),)


def _transposed(
    candidate: EntityProfileFrame, source: SourceKind,
    dropped: tuple[SourceKind, ...],
) -> list[PatchOp]:
    """Lo que la fuente entrante recibe, escrito y no adivinado."""
    donors = [candidate.predicates_for(s) for s in dropped]
    if not donors:
        return []
    operations: list[PatchOp] = []
    period = _agreed(donors, lambda p: p.temporal.period)
    if period is not None and not period.is_unbounded:
        operations.append(PatchOp(
            target=PatchTarget(TargetKind.TEMPORAL, source),
            operation=PatchOperation.SET,
            # El HECHO no se transpone: la fuente entrante usa su propia base
            # temporal, que es lo que la etiqueta declara.
            value=TemporalPredicate(
                event=TemporalEventKind.NONE, period=period,
                event_origin=PredicateOrigin.DEFAULT,
                period_origin=PredicateOrigin.EXPLICIT,
            ),
        ))
    default = SourcePredicates(source)
    for kind in _TRANSPOSABLE:
        value = _agreed(donors, lambda p, k=kind.value: getattr(p, k))
        if value is None or value == getattr(default, kind.value):
            continue
        if kind is TargetKind.VALUE:
            value = replace(value, magnitude=VALUE_MAGNITUDE[source])
        operations.append(PatchOp(
            target=PatchTarget(kind, source),
            operation=PatchOperation.SET, value=value,
        ))
    return operations


def _agreed(donors, read):
    """Un valor sólo se transpone si TODAS las fuentes que salen coinciden."""
    values = [read(donor) for donor in donors]
    first = values[0]
    return first if all(value == first for value in values[1:]) else None


def _label(
    candidate: EntityProfileFrame, target: frozenset[SourceKind],
    dropped: tuple[SourceKind, ...],
) -> str:
    ordered = tuple(s for s in SourceKind if s in target)
    nouns = ", ".join(s.noun for s in ordered)
    bases = "; ".join(f"{s.noun} según fecha de {event_verb(s)}" for s in ordered)
    text = f"Responder {focus_label(candidate.focus)} sobre {nouns} ({bases})"
    periods = {
        humanize_period(candidate.predicates_for(s).temporal.period)
        for s in dropped
    }
    if len(periods) == 1:
        period = periods.pop()
        if period != humanize_period(SourcePredicates(ordered[0]).temporal.period):
            text += f", conservando {period.lower()}"
    return text + "."


__all__ = ["capability_alternatives"]
