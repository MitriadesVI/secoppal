"""Una representación única, JSON-safe, de QUÉ pidió el usuario.

No es otro parser. Cada dimensión sigue perteneciendo a la autoridad que ya la
resuelve —comprador a ``entity_resolver``/``buyer_groups``, rango a
``temporal_semantics``, estado a ``estado_families``, modalidad a
``ModalityResolver``, tema al extractor de objeto— y esta capa las **integra**.

Sólo dos decisiones son nuevas, y las dos existen porque hoy no tienen dueño:

* ``sources`` — qué fuente nombró la persona (``source_request``);
* ``event`` — a qué hecho ancla su periodo (``temporal_event``).

La petición es lo que el usuario PIDIÓ. Lo que finalmente se ejecuta lo decide
``execution_plan`` a partir de ella, y nunca al revés: el foco no reescribe la
fuente solicitada.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from app.core.entity_profile.models import EntityProfileScope
from app.core.entity_profile.source_request import (
    RequestedSources,
    SourceProvenance,
    resolve_requested_sources,
)
from app.core.entity_profile.temporal_event import (
    TemporalEvent,
    TemporalEventResolution,
    current_state_survives_event,
    resolve_temporal_event,
)


PROCESS_STATE_KEYS = (
    "estado_del_procedimiento", "estado_de_apertura_del_proceso",
)
CONTRACT_STATE_KEYS = ("estado_contrato", "estado_contrato_adicional")
SHARED_STATE_KEYS = ("estado", "estado_family", "estado_field", "intent_type")


@dataclass(frozen=True, slots=True)
class EntityProfileSemanticRequest:
    """Qué pidió el usuario, en una forma que sobrevive a la serialización."""

    focus: str
    sources: RequestedSources
    sources_provenance: SourceProvenance
    time_from: str | None = None
    time_to: str | None = None
    event: TemporalEvent = TemporalEvent.NONE
    unsupported_event: str | None = None
    keeps_current_state: bool = True
    topic: tuple[str, ...] = ()
    modality: tuple[str, ...] = ()
    value_min: float | None = None
    value_max: float | None = None

    @property
    def wants_processes(self) -> bool:
        return self.sources in (RequestedSources.PROCESSES, RequestedSources.BOTH)

    @property
    def wants_contracts(self) -> bool:
        return self.sources in (RequestedSources.CONTRACTS, RequestedSources.BOTH)

    @property
    def has_period(self) -> bool:
        return bool(self.time_from or self.time_to)

    def to_dict(self) -> dict:
        """Sólo tipos JSON: la conversación guarda datos, nunca objetos vivos."""
        return {
            "focus": self.focus,
            "sources": self.sources.value,
            "sources_provenance": self.sources_provenance.value,
            "time_from": self.time_from,
            "time_to": self.time_to,
            "event": self.event.value,
            "unsupported_event": self.unsupported_event,
            "keeps_current_state": self.keeps_current_state,
            "topic": list(self.topic),
            "modality": list(self.modality),
            "value_min": self.value_min,
            "value_max": self.value_max,
        }

    @classmethod
    def from_dict(cls, raw: object) -> "EntityProfileSemanticRequest | None":
        """Un contexto ilegible se descarta; nunca se adivina la petición."""
        if not isinstance(raw, dict):
            return None
        try:
            return cls(
                focus=str(raw["focus"]),
                sources=RequestedSources(raw["sources"]),
                sources_provenance=SourceProvenance(raw["sources_provenance"]),
                time_from=_text_or_none(raw.get("time_from")),
                time_to=_text_or_none(raw.get("time_to")),
                event=TemporalEvent(raw.get("event", TemporalEvent.NONE.value)),
                unsupported_event=_text_or_none(raw.get("unsupported_event")),
                keeps_current_state=bool(raw.get("keeps_current_state", True)),
                topic=tuple(str(item) for item in raw.get("topic") or ()),
                modality=tuple(str(item) for item in raw.get("modality") or ()),
                value_min=_number_or_none(raw.get("value_min")),
                value_max=_number_or_none(raw.get("value_max")),
            )
        except (KeyError, TypeError, ValueError):
            return None


def _text_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _number_or_none(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def build_semantic_request(
    *,
    user_query: str,
    params: dict,
    scope: EntityProfileScope,
    focus: str,
) -> EntityProfileSemanticRequest:
    """Integra las autoridades existentes y adjudica fuente y evento."""
    source = resolve_requested_sources(user_query, params)
    resolution = resolve_temporal_event(user_query, params)
    keeps_state = _keeps_current_state(user_query, params, resolution)
    return EntityProfileSemanticRequest(
        focus=focus,
        sources=source.sources,
        sources_provenance=source.provenance,
        time_from=scope.time_from,
        time_to=scope.time_to,
        event=resolution.event,
        unsupported_event=resolution.unsupported_term,
        keeps_current_state=keeps_state,
        topic=scope.topic,
        modality=scope.modality,
        value_min=_number_or_none(params.get("valor_min")),
        value_max=_number_or_none(params.get("valor_max")),
    )


def _keeps_current_state(
    user_query: str, params: dict, resolution: TemporalEventResolution,
) -> bool:
    """Un tramo consumido como evento no filtra además el estado de hoy."""
    if resolution.event is TemporalEvent.NONE:
        return True
    if not _has_current_state(params):
        return True
    dataset = params.get("dataset")
    return current_state_survives_event(
        user_query, resolution, dataset if isinstance(dataset, str) else "procesos",
    )


def _has_current_state(params: dict) -> bool:
    return any(
        params.get(key)
        for key in (*PROCESS_STATE_KEYS, *CONTRACT_STATE_KEYS, "estado")
    )


def inherit_request(
    previous: EntityProfileSemanticRequest,
    current: EntityProfileSemanticRequest,
    *,
    source_is_explicit: bool,
) -> EntityProfileSemanticRequest:
    """Compone un turno sobre el anterior sin ensanchar ninguna dimensión.

    Cambiar sólo el foco no reinicia fuente, tema ni periodo; cambiar sólo el
    periodo no reinicia el foco. Una fuente que el turno actual no nombró se
    hereda con procedencia INHERITED, nunca se reabre al default.
    """
    if source_is_explicit:
        return current
    return replace(
        current,
        sources=previous.sources,
        sources_provenance=SourceProvenance.INHERITED,
    )


def request_context(request: EntityProfileSemanticRequest) -> dict[str, Any]:
    """Marcadores conversacionales JSON-safe de la petición vigente."""
    return {
        "_entity_profile_request": request.to_dict(),
        "_entity_profile_sources": request.sources.value,
    }
