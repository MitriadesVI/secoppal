"""La única autoridad de lo AUTORIZADO a ejecutar. Recibe Frame, no conversación.

`build_execution_plan(request, params)` recibía dos entradas que podían
describir filtros distintos, y ninguna regla decía cuál mandaba. Aquí sólo
entra el Frame y los catálogos declarados; ni texto, ni params, ni el plan
anterior.

Una decisión es una de tres cosas, nunca dos a la vez:

    READY                plan completo, con TODOS los SQL ya construidos
    NEEDS_CLARIFICATION  cuestiones abiertas y CERO jobs
    INVALID_CONTEXT      el contexto no puede sostener un plan

"Aclaración con jobs ejecutables" no existe como estado: preguntar y consultar
al mismo tiempo es contestar una pregunta que no se hizo. Y los SQL se
construyen ANTES de declarar READY, porque un plan que no puede emitir una de
sus consultas no está listo aunque las demás sí lo estén.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Mapping
import uuid

from app.core.entity_profile.aspects import (
    build_contract_values_query,
    build_process_activity_query,
    build_recent_query,
    build_supplier_query,
    build_temporal_query,
)
from app.core.entity_profile.frame import EntityProfileFrame, SourcePredicates
from app.core.entity_profile.frame_predicates import (
    UnsupportedPredicateError,
    compile_predicates,
)
from app.core.entity_profile.frame_values import (
    DATE_FIELD,
    VALUE_MAGNITUDE,
    SourceKind,
    SourceOrigin,
)
from app.core.entity_profile.immutability import deep_freeze

CATALOG_VERSION = "entity-profile-frame-1"

PROFILE_QUERY_BUDGET = 6

# De dónde salen las cifras de cada foco. No es preferencia de producto: los
# proveedores viven en contratos y las modalidades del procedimiento, en
# procesos. `tests/test_entity_profile_frame_catalog_parity.py` comprueba que
# esta tabla y la de V1.1 dicen lo mismo mientras las dos existan.
FOCUS_CAPABILITY: Mapping[str, frozenset[SourceKind]] = {
    "overview": frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS}),
    "recent": frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS}),
    "temporal": frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS}),
    "suppliers": frozenset({SourceKind.CONTRACTS}),
    "modalities": frozenset({SourceKind.PROCESSES}),
    "values": frozenset({SourceKind.CONTRACTS}),
}

_PROCESS_STATE_FIELDS = frozenset(
    {"estado_del_procedimiento", "estado_de_apertura_del_proceso"},
)
_CONTRACT_STATE_FIELDS = frozenset({"estado_contrato", "estado_contrato_adicional"})

_STATE_FIELDS = {
    SourceKind.PROCESSES: _PROCESS_STATE_FIELDS,
    SourceKind.CONTRACTS: _CONTRACT_STATE_FIELDS,
}


class PlanStatus(str, Enum):
    READY = "READY"
    NEEDS_CLARIFICATION = "NEEDS_CLARIFICATION"
    INVALID_CONTEXT = "INVALID_CONTEXT"


@dataclass(frozen=True, slots=True)
class PlanIssue:
    code: str
    detail: str = ""
    sources: tuple[SourceKind, ...] = ()
    terms: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class JobSpec:
    call_id: str
    aspect: str
    source: SourceKind
    operation: str
    build: Callable[[Any, dict], str]


def _job_catalog() -> Mapping[str, tuple[JobSpec, ...]]:
    process_activity = JobSpec(
        "process_activity", "activity_modalities", SourceKind.PROCESSES,
        "aggregate", build_process_activity_query,
    )
    contract_values = JobSpec(
        "contract_values", "activity_values", SourceKind.CONTRACTS,
        "aggregate", build_contract_values_query,
    )
    supplier_count = JobSpec(
        "supplier_count", "suppliers", SourceKind.CONTRACTS, "aggregate",
        lambda builder, params: build_supplier_query(builder, params, "count"),
    )
    supplier_value = JobSpec(
        "supplier_value", "suppliers", SourceKind.CONTRACTS, "aggregate",
        lambda builder, params: build_supplier_query(builder, params, "value"),
    )
    recent_processes = JobSpec(
        "recent_processes", "recent", SourceKind.PROCESSES, "query",
        lambda builder, params: build_recent_query(
            builder, SourceKind.PROCESSES.dataset_id, params,
        ),
    )
    recent_contracts = JobSpec(
        "recent_contracts", "recent", SourceKind.CONTRACTS, "query",
        lambda builder, params: build_recent_query(
            builder, SourceKind.CONTRACTS.dataset_id, params,
        ),
    )
    temporal_processes = JobSpec(
        "temporal_processes", "temporal", SourceKind.PROCESSES, "aggregate",
        lambda builder, params: build_temporal_query(
            builder, SourceKind.PROCESSES.dataset_id, params,
        ),
    )
    temporal_contracts = JobSpec(
        "temporal_contracts", "temporal", SourceKind.CONTRACTS, "aggregate",
        lambda builder, params: build_temporal_query(
            builder, SourceKind.CONTRACTS.dataset_id, params,
        ),
    )
    return {
        "overview": (
            process_activity, contract_values, supplier_count, supplier_value,
            recent_processes, recent_contracts,
        ),
        "suppliers": (supplier_count, supplier_value),
        "modalities": (process_activity,),
        "values": (contract_values,),
        "recent": (recent_processes, recent_contracts),
        "temporal": (temporal_processes, temporal_contracts),
    }


SECTION_CALLS: Mapping[str, frozenset[str]] = {
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

FOCUS_SECTIONS: Mapping[str, frozenset[str]] = {
    "overview": frozenset({
        "activity", "values", "modalities", "suppliers", "temporal", "recent",
    }),
    "suppliers": frozenset({"suppliers"}),
    "modalities": frozenset({"modalities"}),
    "values": frozenset({"values"}),
    "recent": frozenset({"recent"}),
    "temporal": frozenset({"temporal"}),
}


@dataclass(frozen=True, slots=True)
class CompiledJob:
    job_id: str
    call_id: str
    aspect: str
    source: SourceKind
    dataset_id: str
    operation: str
    sql: str


@dataclass(frozen=True, slots=True)
class CompiledSourcePlan:
    """Una fuente, sus predicados congelados y el SQL que salió de ellos."""

    source: SourceKind
    dataset_id: str
    predicates: SourcePredicates
    where: str
    temporal_basis: str
    value_magnitude: str
    fingerprint: str


@dataclass(frozen=True, slots=True)
class ProfilePlan:
    plan_id: str
    frame_ref: tuple[str, int]
    catalog_version: str
    focus: str
    source_plans: Mapping[SourceKind, CompiledSourcePlan]
    jobs: tuple[CompiledJob, ...]
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "frame_ref", tuple(self.frame_ref))
        object.__setattr__(self, "jobs", tuple(self.jobs))
        object.__setattr__(self, "limitations", tuple(self.limitations))
        source_plans = deep_freeze(self.source_plans)
        for source, source_plan in source_plans.items():
            if source_plan.source is not source:
                raise ValueError("source plan is stored under a foreign source")
            if source_plan.dataset_id != source.dataset_id:
                raise ValueError("source plan names a foreign dataset")
        seen_jobs: set[str] = set()
        for job in self.jobs:
            if job.job_id in seen_jobs:
                raise ValueError("job ids must be unique within a plan")
            seen_jobs.add(job.job_id)
            source_plan = source_plans.get(job.source)
            if source_plan is None or job.dataset_id != source_plan.dataset_id:
                raise ValueError("compiled job is not owned by its source plan")
        object.__setattr__(self, "source_plans", source_plans)

    @property
    def planned_sources(self) -> frozenset[SourceKind]:
        """Fuentes con jobs PLANIFICADOS. No son fuentes ejecutadas."""
        return frozenset(job.source for job in self.jobs)

    @property
    def expected_sections(self) -> frozenset[str]:
        planned = {job.call_id for job in self.jobs}
        return frozenset(
            name for name, call_ids in SECTION_CALLS.items()
            if planned & call_ids and name in FOCUS_SECTIONS[self.focus]
        )

    @property
    def not_requested_sections(self) -> frozenset[str]:
        return frozenset(SECTION_CALLS) - self.expected_sections


@dataclass(frozen=True, slots=True)
class PlanDecision:
    status: PlanStatus
    plan: ProfilePlan | None = None
    issues: tuple[PlanIssue, ...] = ()
    limitations: tuple[str, ...] = ()
    alternatives: tuple[str, ...] = field(default=())

    def __post_init__(self) -> None:
        if self.status is not PlanStatus.READY and self.plan is not None:
            raise ValueError("only a READY decision carries a plan")
        if self.status is PlanStatus.READY and self.plan is None:
            raise ValueError("a READY decision must carry its plan")


def clarification(
    issues, *, alternatives: tuple[str, ...] = (),
) -> PlanDecision:
    """Una aclaración: cuestiones, alternativas y CERO jobs."""
    return PlanDecision(
        status=PlanStatus.NEEDS_CLARIFICATION,
        issues=tuple(
            item if isinstance(item, PlanIssue)
            else PlanIssue(item.code, item.detail, terms=item.terms)
            for item in issues
        ),
        alternatives=alternatives,
    )


def compile_profile(
    frame: EntityProfileFrame,
    *,
    soql_builder,
    capability_catalog: Mapping[str, frozenset[SourceKind]] = FOCUS_CAPABILITY,
) -> PlanDecision:
    """Frame -> plan autorizado, o la pregunta que impide autorizar uno."""
    issues = _validate_predicates(frame)
    if issues:
        return clarification(issues)

    capability = capability_catalog.get(frame.focus)
    if capability is None:
        return PlanDecision(
            PlanStatus.INVALID_CONTEXT,
            issues=(PlanIssue("UNKNOWN_FOCUS", frame.focus),),
        )

    requested = frame.source_selection.requested
    effective = requested & capability
    limitations: tuple[str, ...] = ()

    if frame.source_selection.origin is SourceOrigin.EXPLICIT:
        uncovered = tuple(s for s in frame.sources if s not in capability)
        if uncovered:
            # Una fuente que la persona NOMBRÓ no desaparece en silencio.
            return clarification((PlanIssue(
                "EXPLICIT_SOURCE_INCOMPLETE",
                f"«{frame.focus}» no se puede responder sobre "
                + ", ".join(s.noun for s in uncovered),
                uncovered,
            ),))
    elif not effective:
        return clarification((PlanIssue(
            "FOCUS_HAS_NO_SOURCE", frame.focus, tuple(frame.sources),
        ),))
    elif effective != requested:
        # Nadie pidió las fuentes que sobran: acotar no contradice a nadie,
        # pero se declara.
        narrowed = ", ".join(s.noun for s in frame.sources if s not in capability)
        limitations = (
            f"«{frame.focus}» sólo se puede responder sobre "
            f"{', '.join(sorted(s.noun for s in effective))}; "
            f"se deja fuera {narrowed}.",
        )

    try:
        source_plans = {
            source: _compile_source(soql_builder, frame, source)
            for source in frame.sources
            if source in effective
        }
    except UnsupportedPredicateError as exc:
        return clarification((PlanIssue("UNSUPPORTED_PREDICATE", str(exc)),))

    try:
        jobs = _build_jobs(soql_builder, frame.focus, source_plans)
    except (UnsupportedPredicateError, ValueError, RuntimeError) as exc:
        # `UnsafeGlobalQueryError` y `BuyerCandidateSetExceedsSafeLimitError`
        # son RuntimeError. Un SQL que no se puede construir bloquea el plan
        # entero; dejarlo escapar reventaría la llamada en vez de decidir.
        return PlanDecision(
            PlanStatus.INVALID_CONTEXT,
            issues=(PlanIssue("SQL_BUILD_FAILED", str(exc)),),
        )
    if not jobs:
        return clarification((PlanIssue("NO_EXECUTABLE_JOB", frame.focus),))

    return PlanDecision(
        PlanStatus.READY,
        plan=ProfilePlan(
            plan_id=str(uuid.uuid4()),
            frame_ref=(frame.frame_id, frame.revision),
            catalog_version=CATALOG_VERSION,
            focus=frame.focus,
            source_plans=source_plans,
            jobs=jobs,
            limitations=limitations,
        ),
        limitations=limitations,
    )


def _validate_predicates(frame: EntityProfileFrame) -> tuple[PlanIssue, ...]:
    """Cada fuente honra SU hecho y SU esquema de estado, o se pregunta."""
    issues: list[PlanIssue] = []
    for source in frame.sources:
        predicates = frame.predicates_for(source)
        event = predicates.temporal.event
        if event.source is not None and event.source is not source:
            issues.append(PlanIssue(
                "INCOMPATIBLE_EVENT_SUBJECT",
                f"SECOP II no publica una fecha de {event.value} para "
                f"{source.noun}",
                (source,),
                (event.value,),
            ))
        for predicate in predicates.current_state.predicates:
            if predicate.field not in _STATE_FIELDS[source]:
                issues.append(PlanIssue(
                    "STATE_FIELD_FOREIGN_TO_SOURCE",
                    f"{predicate.field} no existe en {source.noun}",
                    (source,),
                ))
        if predicates.contract_type.exclude:
            issues.append(PlanIssue(
                "CONTRACT_TYPE_EXCLUDE_UNSUPPORTED",
                "el tipo de contrato negativo no tiene proyección acreditada",
                (source,),
            ))
    return tuple(issues)


def _compile_source(builder, frame: EntityProfileFrame, source: SourceKind):
    predicates = frame.predicates_for(source)
    compiled = compile_predicates(builder, source, predicates, frame.buyer_scope)
    return CompiledSourcePlan(
        source=source,
        dataset_id=source.dataset_id,
        predicates=predicates,
        where=compiled.where,
        temporal_basis=DATE_FIELD[source],
        value_magnitude=VALUE_MAGNITUDE[source],
        fingerprint=predicates.fingerprint(),
    )


def _build_jobs(builder, focus: str, source_plans) -> tuple[CompiledJob, ...]:
    """El SQL de cada job, con el WHERE ya congelado por el compilador.

    Los constructores de aspecto siguen siendo los acreditados: aportan la
    forma del SELECT, el GROUP BY y el orden. Lo único que se les sustituye es
    la decisión de WHERE, que aquí ya está tomada. `params` llega vacío a
    propósito: no queda nada que un emisor pueda reinterpretar.
    """
    specs = _job_catalog()[focus]
    if len(specs) > PROFILE_QUERY_BUDGET:
        raise ValueError("Entity Profile query budget exceeded before execution")
    jobs: list[CompiledJob] = []
    for spec in specs:
        plan = source_plans.get(spec.source)
        if plan is None:
            continue
        frozen = _FrozenWhereBuilder(builder, plan.where)
        jobs.append(CompiledJob(
            job_id=f"{spec.call_id}@{plan.fingerprint[:12]}",
            call_id=spec.call_id, aspect=spec.aspect, source=spec.source,
            dataset_id=plan.dataset_id, operation=spec.operation,
            sql=spec.build(frozen, {}),
        ))
    return tuple(jobs)


class _FrozenWhereBuilder:
    """SoQLBuilder con el WHERE ya decidido.

    Es un adaptador de emisión, no un builder nuevo: delega TODO en el builder
    real y sólo intercepta el punto donde los aspectos preguntarían por el
    WHERE. Sin esto habría que reescribir la forma de cada SELECT, y dos copias
    de la misma consulta divergen.
    """

    def __init__(self, builder, where: str) -> None:
        self._builder = builder
        self._where = where
        self.SPECS = builder.SPECS

    def _build_where(self, spec, params, *, allow_global: bool = False) -> str:
        return self._where

    def __getattr__(self, name: str):
        return getattr(self._builder, name)
