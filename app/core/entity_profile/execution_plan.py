"""La única autoridad de ejecución del perfil: fuentes, jobs y secciones.

Antes había tres listas independientes —los jobs de ``jobs_for_focus``, las
secciones que ``build_sections`` arma por foco y lo que el render decide
mostrar— y nada garantizaba que dijeran lo mismo. Un overview ejecutaba seis
llamadas aunque el usuario hubiera nombrado una sola fuente, y el render
presentaba esos ceros como hechos.

Aquí se decide TODO junto y una sola vez: qué fuentes se ejecutan de verdad,
qué jobs se envían, qué secciones se esperan, cuáles quedan NO SOLICITADAS y
cuándo la pregunta no se puede contestar sin aclarar.

El foco nunca reescribe la fuente solicitada. La fuente efectiva es la
intersección de lo que el usuario pidió con lo que el foco puede responder; si
esa intersección es vacía, se pregunta, no se amplía.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET
from app.core.entity_profile.query_runner import QueryJob, jobs_for_focus
from app.core.entity_profile.semantic_request import EntityProfileSemanticRequest
from app.core.entity_profile.source_plan import SourcePlan, project_sources
from app.core.entity_profile.source_request import RequestedSources


# Qué fuentes puede responder cada foco. No es una preferencia de producto: es
# de dónde salen sus cifras. Los proveedores viven en contratos; las modalidades
# del procedimiento, en procesos.
FOCUS_CAPABILITY: dict[str, frozenset[str]] = {
    "overview": frozenset({PROCESS_DATASET, CONTRACT_DATASET}),
    "recent": frozenset({PROCESS_DATASET, CONTRACT_DATASET}),
    "temporal": frozenset({PROCESS_DATASET, CONTRACT_DATASET}),
    "suppliers": frozenset({CONTRACT_DATASET}),
    "modalities": frozenset({PROCESS_DATASET}),
    # V1 no presenta el presupuesto de un proceso como valor contratado —lo
    # prohíbe el contrato de producto—, así que `values` sólo responde sobre
    # contratos. Ampliarlo exige una agregación acreditada sobre `precio_base`,
    # no una reinterpretación del campo que ya se consulta.
    "values": frozenset({CONTRACT_DATASET}),
}

_JOB_DATASET_BY_ID = {
    "process_activity": PROCESS_DATASET,
    "recent_processes": PROCESS_DATASET,
    "temporal_processes": PROCESS_DATASET,
    "contract_values": CONTRACT_DATASET,
    "supplier_count": CONTRACT_DATASET,
    "supplier_value": CONTRACT_DATASET,
    "recent_contracts": CONTRACT_DATASET,
    "temporal_contracts": CONTRACT_DATASET,
}

_SOURCE_LABEL = {
    PROCESS_DATASET: "procesos",
    CONTRACT_DATASET: "contratos",
}

_FOCUS_LABEL = {
    "suppliers": "los proveedores",
    "values": "los valores contratados",
    "modalities": "las modalidades de contratación",
}


@dataclass(frozen=True, slots=True)
class EntityProfileExecutionPlan:
    request: EntityProfileSemanticRequest
    process_plan: SourcePlan
    contract_plan: SourcePlan
    jobs: tuple[QueryJob, ...] = ()
    clarification: str | None = None

    @property
    def focus(self) -> str:
        return self.request.focus

    @property
    def executed_datasets(self) -> frozenset[str]:
        return frozenset(job.dataset_id for job in self.jobs)

    @property
    def executed_call_ids(self) -> frozenset[str]:
        return frozenset(job.call_id for job in self.jobs)

    @property
    def effective_sources(self) -> RequestedSources | None:
        """Lo que realmente se consulta; None cuando no se consulta nada."""
        datasets = self.executed_datasets
        if PROCESS_DATASET in datasets and CONTRACT_DATASET in datasets:
            return RequestedSources.BOTH
        if PROCESS_DATASET in datasets:
            return RequestedSources.PROCESSES
        if CONTRACT_DATASET in datasets:
            return RequestedSources.CONTRACTS
        return None

    def plan_for(self, dataset_id: str) -> SourcePlan:
        return (
            self.process_plan if dataset_id == PROCESS_DATASET else self.contract_plan
        )


def _requested_datasets(request: EntityProfileSemanticRequest) -> frozenset[str]:
    datasets: set[str] = set()
    if request.wants_processes:
        datasets.add(PROCESS_DATASET)
    if request.wants_contracts:
        datasets.add(CONTRACT_DATASET)
    return frozenset(datasets)


def _unsupported_event_clarification(request: EntityProfileSemanticRequest) -> str:
    return (
        f"No puedo analizar por fecha de «{request.unsupported_event}»: SECOP II "
        "publica fecha de publicación para procesos y fecha de firma para "
        "contratos, no una fecha propia de ese hecho. ¿Quieres el periodo sobre "
        "la fecha de firma, o los que hoy están en ese estado sin acotar fecha?"
    )


def _capability_clarification(
    request: EntityProfileSemanticRequest, capability: frozenset[str],
) -> str:
    asked = ", ".join(
        sorted(_SOURCE_LABEL[dataset] for dataset in _requested_datasets(request))
    )
    available = ", ".join(
        sorted(_SOURCE_LABEL[dataset] for dataset in capability)
    )
    subject = _FOCUS_LABEL.get(request.focus, f"el foco «{request.focus}»")
    return (
        f"Pediste {asked}, y {subject} sólo se puede responder sobre "
        f"{available}. ¿Quieres que analice {available}, o prefieres otra "
        f"vista de {asked}?"
    )


def build_execution_plan(
    request: EntityProfileSemanticRequest, params: dict,
) -> EntityProfileExecutionPlan:
    """Decide fuentes efectivas, jobs y aclaración en una sola pasada."""
    process_plan, contract_plan = project_sources(request, params)
    plans = {
        PROCESS_DATASET: process_plan,
        CONTRACT_DATASET: contract_plan,
    }

    if request.unsupported_event is not None:
        return EntityProfileExecutionPlan(
            request, process_plan, contract_plan,
            clarification=_unsupported_event_clarification(request),
        )

    capability = FOCUS_CAPABILITY.get(request.focus)
    if capability is None:
        raise ValueError(f"unknown Entity Profile focus: {request.focus}")

    effective = _requested_datasets(request) & capability
    if not effective:
        return EntityProfileExecutionPlan(
            request, process_plan, contract_plan,
            clarification=_capability_clarification(request, capability),
        )

    executable = {
        dataset for dataset in effective if plans[dataset].should_execute
    }
    if not executable:
        # La fuente se pidió y el foco la soporta, pero el plan de esa fuente
        # declaró una dimensión que no puede honrar. No se degrada en silencio.
        blocked = sorted(
            dimension
            for dataset in effective
            for dimension in plans[dataset].unsupported_dimensions
        )
        return EntityProfileExecutionPlan(
            request, process_plan, contract_plan,
            clarification=(
                "No puedo responder eso sobre la fuente que pediste sin cambiar "
                f"la pregunta ({', '.join(blocked)})."
            ),
        )

    jobs = tuple(
        replace(job, params=plans[_JOB_DATASET_BY_ID[job.call_id]].params)
        for job in jobs_for_focus(request.focus)
        if _JOB_DATASET_BY_ID[job.call_id] in executable
    )
    return EntityProfileExecutionPlan(request, process_plan, contract_plan, jobs)
