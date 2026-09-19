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
from app.core.entity_profile.source_request import RequestedSources, SourceProvenance
from app.core.entity_profile.supplier_ranking_request import (
    DEFAULT_SUPPLIER_REQUEST,
    SupplierRankingRequest,
    UNSUPPORTED_CARDINALITY_TEXT,
    request_from_params,
)
from app.core.entity_profile.temporal_event import TemporalEvent


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

# Qué llamadas sostiene cada sección, y qué secciones produce cada foco. El
# plan las DECLARA (3D) en vez de dejar que cada consumidor las re-derive.
SECTION_CALLS: dict[str, frozenset[str]] = {
    "activity": frozenset({"process_activity", "contract_values"}),
    "values": frozenset({"contract_values"}),
    "modalities": frozenset({"process_activity"}),
    "suppliers": frozenset({"supplier_count", "supplier_value"}),
    "temporal": frozenset({
        "temporal_processes", "temporal_contracts",
        "process_activity", "contract_values",
    }),
    "recent": frozenset({"recent_processes", "recent_contracts"}),
}

FOCUS_SECTIONS: dict[str, frozenset[str]] = {
    "overview": frozenset({
        "activity", "values", "modalities", "suppliers", "temporal", "recent",
    }),
    "suppliers": frozenset({"suppliers"}),
    "modalities": frozenset({"modalities"}),
    "values": frozenset({"values"}),
    "recent": frozenset({"recent"}),
    "temporal": frozenset({"temporal"}),
}

_SOURCE_LABEL = {
    PROCESS_DATASET: "procesos",
    CONTRACT_DATASET: "contratos",
}

_EVENT_PHRASE = {
    TemporalEvent.PROCESS_PUBLICATION: "publicación",
    TemporalEvent.CONTRACT_SIGNATURE: "firma",
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
    # La petición de proveedores de este plan: métrica y corte exacto. Llega
    # transportada por params (primitivas) y reconstruida validada; sin ella, la
    # vista V1 por defecto.
    supplier_ranking: SupplierRankingRequest = DEFAULT_SUPPLIER_REQUEST

    def __post_init__(self) -> None:
        if type(self.supplier_ranking) is not SupplierRankingRequest:
            raise ValueError("INVALID_SUPPLIER_RANKING_REQUEST")

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

    @property
    def expected_sections(self) -> frozenset[str]:
        """Las secciones que ESTE plan puede sostener con lo que ejecutó."""
        executed = self.executed_call_ids
        return frozenset(
            name for name, call_ids in SECTION_CALLS.items()
            if executed & call_ids and name in FOCUS_SECTIONS[self.focus]
        )

    @property
    def not_requested_sections(self) -> frozenset[str]:
        """Ni cero ni fallo: nadie las pidió."""
        return frozenset(SECTION_CALLS) - self.expected_sections

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
        f"{available}. ¿Quieres que analice {subject} sólo en {available}, o "
        f"prefieres una vista general de {asked}?"
    )


def _incomplete_clarification(
    request: EntityProfileSemanticRequest,
    plans: dict[str, SourcePlan],
    unhonored: list[str],
) -> str:
    """Nombra QUÉ fuente se queda fuera y POR QUÉ, no un texto genérico."""
    names = ", ".join(_SOURCE_LABEL[dataset] for dataset in unhonored)
    events = sorted({
        _EVENT_PHRASE.get(claim.event, claim.event.value)
        for claim in request.temporal_claims
    })
    if events:
        reason = (
            f"SECOP II no publica una fecha de {' ni de '.join(events)} para "
            f"{names}"
        )
    else:
        reason = f"esa dimensión no existe en {names}"
    asked = ", ".join(
        sorted(_SOURCE_LABEL[dataset] for dataset in _requested_datasets(request))
    )
    return (
        f"Pediste {asked}, y {reason}. ¿Quieres que analice sólo las fuentes "
        f"que sí pueden responder, o prefieres otro periodo o filtro para {names}?"
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
    # La petición de proveedores viaja por params ya resuelta (carril A2). Un
    # transporte malformado no cae al default: se aclara con cero jobs.
    supplier_ranking = request_from_params(params)
    if supplier_ranking is None:
        return EntityProfileExecutionPlan(
            request, process_plan, contract_plan,
            clarification=UNSUPPORTED_CARDINALITY_TEXT,
        )

    if request.unsupported_event is not None:
        return EntityProfileExecutionPlan(
            request, process_plan, contract_plan,
            clarification=_unsupported_event_clarification(request),
        )

    capability = FOCUS_CAPABILITY.get(request.focus)
    if capability is None:
        raise ValueError(f"unknown Entity Profile focus: {request.focus}")

    requested = _requested_datasets(request)
    explicit = request.sources_provenance is SourceProvenance.EXPLICIT
    effective = requested & capability

    # EXPLICIT REQUEST COMPLETENESS. Una fuente que la persona NOMBRÓ no
    # desaparece en silencio. Si el foco no la cubre —proveedores no existen en
    # procesos— o su plan declaró una dimensión que no puede honrar —contratos
    # no tienen fecha de publicación—, se pregunta antes de emitir un job.
    #
    # Una fuente que sólo entró por DEFAULT o herencia sí puede acotarse: nadie
    # la pidió, y acotarla no contradice a nadie. Esa es toda la diferencia
    # entre "analiza X" + proveedores, que responde contratos, y "analiza
    # proveedores de procesos y contratos de X", que pregunta.
    if explicit:
        uncovered = sorted(requested - capability)
        if uncovered:
            return EntityProfileExecutionPlan(
                request, process_plan, contract_plan,
                clarification=_capability_clarification(request, capability),
            )
        unhonored = sorted(
            dataset for dataset in effective if not plans[dataset].should_execute
        )
        if unhonored:
            return EntityProfileExecutionPlan(
                request, process_plan, contract_plan,
                clarification=_incomplete_clarification(request, plans, unhonored),
            )
    elif not effective:
        return EntityProfileExecutionPlan(
            request, process_plan, contract_plan,
            clarification=_capability_clarification(request, capability),
        )

    executable = {
        dataset for dataset in effective if plans[dataset].should_execute
    }
    if not executable:
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
        for job in jobs_for_focus(request.focus, supplier_ranking)
        if _JOB_DATASET_BY_ID[job.call_id] in executable
    )
    return EntityProfileExecutionPlan(
        request, process_plan, contract_plan, jobs,
        supplier_ranking=supplier_ranking,
    )
