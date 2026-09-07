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
    params: dict = field(default_factory=dict)
    event: TemporalEvent = TemporalEvent.NONE
    date_field: str = ""
    value_field: str = ""
    unsupported_dimensions: tuple[str, ...] = ()
    provenance: str = ""

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
    # agosto" con fechas de publicación sería responder otra pregunta.
    if request.event is not TemporalEvent.NONE:
        if EVENT_DATASET[request.event] != dataset_id:
            unsupported.append(request.event.value)
    if request.unsupported_event is not None:
        unsupported.append(f"UNSUPPORTED_EVENT:{request.unsupported_event}")

    projected = dict(params)
    projected["dataset"] = DATASET_NAME[dataset_id]
    for key in _ORCHESTRATION_KEYS:
        projected.pop(key, None)
    for key in _other_schema_state_keys(dataset_id):
        projected.pop(key, None)
    if not request.keeps_current_state:
        for key in (*PROCESS_STATE_KEYS, *CONTRACT_STATE_KEYS, *SHARED_STATE_KEYS):
            projected.pop(key, None)
    elif not any(projected.get(key) for key in (
        *PROCESS_STATE_KEYS, *CONTRACT_STATE_KEYS,
    )):
        # El estado del OTRO esquema acaba de purgarse: su metadata compartida
        # ya no describe ningún filtro de esta fuente y dejarla afirmaría un
        # estado que este plan no aplica.
        for key in SHARED_STATE_KEYS:
            projected.pop(key, None)

    should_execute = wanted and not unsupported
    return SourcePlan(
        dataset_id=dataset_id,
        should_execute=should_execute,
        params=projected,
        event=request.event if EVENT_DATASET.get(request.event) == dataset_id
        else TemporalEvent.NONE,
        date_field=DATE_FIELD[dataset_id],
        value_field=VALUE_FIELD[dataset_id],
        unsupported_dimensions=tuple(unsupported),
        provenance=request.sources_provenance.value,
    )


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
