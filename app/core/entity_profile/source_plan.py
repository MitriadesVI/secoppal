"""Proyectar UNA petición semántica a parámetros válidos para CADA dataset.

El perfil pasaba el MISMO diccionario de params a los dos constructores. Medido
sobre `main` 7868664, con "analiza procesos de vías publicados por X en agosto":

    [jbjy-vk9h] ... AND estado_del_procedimiento = 'Publicado' ...
    [p6dx-8zbt] ... AND estado_contrato = 'terminado' ...   (caso hermano)

Un campo exclusivo de procesos viajando al esquema de contratos y viceversa: la
fuga es bidireccional y no la detecta nadie, porque el transporte puede
responder igual.

``workflow/dual_dataset.params_for_dataset`` ya resolvió esto para la ruta
tabular y es la referencia doctrinal. No se copia a ciegas: allí basta con
eliminar lo incompatible, mientras que aquí una incompatibilidad puede ser
EXPLÍCITA del usuario, y entonces la respuesta correcta no es filtrar distinto
sino no contestar esa mitad.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from app.core.entity_profile.immutability import deep_freeze
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET
from app.core.entity_profile.semantic_request import (
    CONTRACT_STATE_KEYS,
    PROCESS_STATE_KEYS,
    SHARED_STATE_KEYS,
    EntityProfileSemanticRequest,
)
from app.core.entity_profile.temporal_event import TemporalEvent


DATASET_NAME = {PROCESS_DATASET: "procesos", CONTRACT_DATASET: "contratos"}

# El campo al que el periodo del usuario ancla en cada esquema. El builder ya
# hace este mapeo; se declara aquí para que el plan y el render puedan decir en
# qué fecha se midió, en vez de afirmar "el periodo" a secas.
DATE_FIELD = {
    PROCESS_DATASET: "fecha_de_publicacion_del",
    CONTRACT_DATASET: "fecha_de_firma",
}

# Dos magnitudes distintas. Un proceso tiene presupuesto estimado; un contrato,
# valor pactado. Un filtro de valor sobre ambas fuentes NO es una sola métrica.
VALUE_FIELD = {
    PROCESS_DATASET: "precio_base",
    CONTRACT_DATASET: "valor_del_contrato",
}

EVENT_DATASET = {
    TemporalEvent.PROCESS_PUBLICATION: PROCESS_DATASET,
    TemporalEvent.CONTRACT_SIGNATURE: CONTRACT_DATASET,
}

# Señales de orquestación, no filtros: nunca deben llegar al builder.
_ORCHESTRATION_KEYS = ("dataset_scope", "dataset_explicit", "offset", "limit")


@dataclass(frozen=True, slots=True)
class SourcePlan:
    dataset_id: str
    should_execute: bool
    params: Mapping[str, object] = field(default_factory=dict)
    event: TemporalEvent = TemporalEvent.NONE
    date_field: str = ""
    value_field: str = ""
    unsupported_dimensions: tuple[str, ...] = ()
    provenance: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "params", deep_freeze(self.params))
        object.__setattr__(
            self, "unsupported_dimensions", tuple(self.unsupported_dimensions),
        )

    @property
    def dataset_name(self) -> str:
        return DATASET_NAME[self.dataset_id]


def _other_schema_state_keys(dataset_id: str) -> tuple[str, ...]:
    if dataset_id == PROCESS_DATASET:
        return CONTRACT_STATE_KEYS
    return PROCESS_STATE_KEYS


def project_source(
    request: EntityProfileSemanticRequest, params: dict, dataset_id: str,
) -> SourcePlan:
    """Devuelve el plan de UNA fuente: nunca predicados del otro esquema."""
    wanted = (
        request.wants_processes
        if dataset_id == PROCESS_DATASET
        else request.wants_contracts
    )
    unsupported: list[str] = []

    # Un evento nombrado pertenece a un solo esquema. Contestar "firmados en
    # agosto" con fechas de publicación sería responder otra pregunta. Con dos
    # reclamaciones —"procesos publicados y contratos firmados"— cada esquema
    # honra la suya y ninguna se pierde.
    own_event = request.event_for(dataset_id)
    for claim in request.temporal_claims:
        if EVENT_DATASET[claim.event] != dataset_id and own_event is TemporalEvent.NONE:
            unsupported.append(claim.event.value)
    for term in request.unsupported_events:
        unsupported.append(f"UNSUPPORTED_EVENT:{term}")

    projected = dict(params)
    projected["dataset"] = DATASET_NAME[dataset_id]
    for key in _ORCHESTRATION_KEYS:
        projected.pop(key, None)
    for key in _other_schema_state_keys(dataset_id):
        projected.pop(key, None)
    if not request.keeps_current_state:
        for key in (*PROCESS_STATE_KEYS, *CONTRACT_STATE_KEYS, *SHARED_STATE_KEYS):
            projected.pop(key, None)
    else:
        _purge_orphan_state_metadata(projected, dataset_id)

    should_execute = wanted and not unsupported
    return SourcePlan(
        dataset_id=dataset_id,
        should_execute=should_execute,
        params=projected,
        event=own_event,
        date_field=DATE_FIELD[dataset_id],
        value_field=VALUE_FIELD[dataset_id],
        unsupported_dimensions=tuple(unsupported),
        provenance=request.sources_provenance.value,
    )


def _purge_orphan_state_metadata(projected: dict, dataset_id: str) -> None:
    """La tupla de estado se proyecta ENTERA o no se proyecta (P2-1).

    Purgar sólo las claves físicas dejaba metadata que describe el otro
    esquema: el plan de contratos quitaba ``estado_del_procedimiento`` y
    conservaba ``estado_field`` apuntando a ese mismo campo, más
    ``estado_family``/``intent_type`` de procesos. Ningún predicado salía mal
    en la inyección observada, pero la metadata afirmaba un estado que este
    plan no aplica, y eso alimenta auditorías y render.
    """
    own_keys = (
        PROCESS_STATE_KEYS if dataset_id == PROCESS_DATASET else CONTRACT_STATE_KEYS
    )
    if any(projected.get(key) for key in own_keys):
        field = projected.get("estado_field")
        if isinstance(field, str) and field not in own_keys:
            # La metadata describe el esquema que acaba de purgarse.
            for key in SHARED_STATE_KEYS:
                projected.pop(key, None)
        return
    # Sin filtro físico propio, ninguna metadata de estado tiene referente.
    for key in SHARED_STATE_KEYS:
        projected.pop(key, None)


def project_sources(
    request: EntityProfileSemanticRequest, params: dict,
) -> tuple[SourcePlan, SourcePlan]:
    """(procesos, contratos) — siempre ambos planes, no siempre ejecutables."""
    return (
        project_source(request, params, PROCESS_DATASET),
        project_source(request, params, CONTRACT_DATASET),
    )


def value_magnitudes_differ(request: EntityProfileSemanticRequest) -> bool:
    """Un filtro de valor sobre las dos fuentes compara cosas distintas."""
    return (
        request.wants_processes
        and request.wants_contracts
        and (request.value_min is not None or request.value_max is not None)
    )
