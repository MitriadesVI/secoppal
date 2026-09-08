"""Plan + Evidence -> secciones acreditadas. Sin scope global, sin params.

`sections.py` sigue siendo la ruta V1.1: recibe un `EntityProfileScope` y saca
de él un periodo ÚNICO para toda la respuesta. Con dos fuentes y dos ventanas
—«procesos publicados en julio y contratos firmados en agosto»— ese periodo
único no existe, y publicarlo presenta como cohorte compartida algo que no lo es.

Aquí las secciones salen de dos autoridades y de ninguna más:

* del PLAN, qué se pidió (`expected_sections`) y con qué predicados por fuente;
* de la EVIDENCIA, qué se intentó, qué respondió y qué cifra es legible.

Una sección fuera de `expected_sections` no está «vacía»: NO SE PIDIÓ, y eso se
distingue de una que falló. Los parsers, la reconciliación de ceros y los
modelos son los ya acreditados; lo que cambia es de dónde viene su entrada.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Any, Mapping

from app.core.entity_profile.aspects import (
    merge_periods,
    parse_contract_values,
    parse_process_activity,
    parse_recent,
    parse_suppliers,
)
from app.core.entity_profile.aspects.activity import MODALITY_CARDINALITY_LIMIT
from app.core.entity_profile.aspects.temporal import YEAR_LIMIT
from app.core.entity_profile.evidence import section_evidence_calls
from app.core.entity_profile.frame_compiler import ProfilePlan
from app.core.entity_profile.frame_render import humanize_period
from app.core.entity_profile.frame_runner import ProfileEvidence
from app.core.entity_profile.frame_values import SourceKind
from app.core.entity_profile.models import (
    CONTRACT_DATASET,
    PROCESS_DATASET,
    EntityProfileFact,
    EntityProfileSection,
    SectionStatus,
    SourceCall,
    unavailable_section,
)
from app.core.entity_profile.supplier_rankings import accredit_ranking, ranking_warnings


SECTION_ORDER = (
    "activity", "values", "modalities", "suppliers", "temporal", "recent",
)

_DATASET_BY_CALL = {
    "process_activity": PROCESS_DATASET,
    "recent_processes": PROCESS_DATASET,
    "temporal_processes": PROCESS_DATASET,
    "contract_values": CONTRACT_DATASET,
    "supplier_count": CONTRACT_DATASET,
    "supplier_value": CONTRACT_DATASET,
    "recent_contracts": CONTRACT_DATASET,
    "temporal_contracts": CONTRACT_DATASET,
}


def build_frame_sections(
    plan: ProfilePlan, evidence: ProfileEvidence, soql_builder: Any,
) -> tuple[EntityProfileSection, ...]:
    """Las seis secciones, en el orden que espera `EntityProfile`."""
    if evidence.plan_id != plan.plan_id:
        raise ValueError("evidence belongs to another plan")
    rows = _rows_by_call(plan, evidence)
    calls = section_evidence_calls(rows, _calls_by_call(plan, evidence))
    expected = plan.expected_sections
    if expected & plan.not_requested_sections or expected | plan.not_requested_sections != set(SECTION_ORDER):
        raise ValueError("incoherent section coverage")
    coverage = _period_coverage(plan)
    built = {
        "activity": lambda: _activity(rows, calls, coverage),
        "values": lambda: _values(rows, calls),
        "modalities": lambda: _modalities(rows, calls),
        "suppliers": lambda: _suppliers(rows, calls),
        "temporal": lambda: _temporal(plan, rows, calls, coverage),
        "recent": lambda: _recent(rows, calls, soql_builder),
    }
    return tuple(
        unavailable_section() if name in plan.not_requested_sections else built[name]()
        for name in SECTION_ORDER
    )


def _rows_by_call(
    plan: ProfilePlan, evidence: ProfileEvidence,
) -> dict[str, list[dict]]:
    return {
        job.call_id: list(evidence.rows_for(job.job_id)) for job in plan.jobs
    }


def _calls_by_call(
    plan: ProfilePlan, evidence: ProfileEvidence,
) -> dict[str, SourceCall]:
    """Proyecta la evidencia al contrato que la reconciliación ya sabe leer."""
    projected: dict[str, SourceCall] = {}
    for job in plan.jobs:
        observed = evidence.call(job.job_id)
        if observed is None:
            continue
        projected[job.call_id] = SourceCall(
            call_id=job.call_id, aspect=job.aspect, dataset_id=job.dataset_id,
            operation=job.operation, status=observed.status,
            rows_received=len(evidence.rows_for(job.job_id)),
            latency_ms=evidence.latency_ms.get(job.job_id, 0.0),
            soql=observed.sql, attempted=observed.attempted,
        )
    return projected


def _period_coverage(plan: ProfilePlan) -> tuple[tuple[str, Any], ...]:
    """El periodo se declara POR FUENTE; no se funde en una ventana común."""
    return tuple(
        (f"period_{source.value.lower()}", humanize_period(
            plan.source_plans[source].predicates.temporal.period,
        ))
        for source in SourceKind
        if source in plan.source_plans
    )


def _present(call_id: str, calls: Mapping[str, SourceCall]) -> bool:
    call = calls.get(call_id)
    return call is not None and call.attempted


def _status(
    ids: tuple[str, ...], calls: Mapping[str, SourceCall],
) -> SectionStatus:
    successes = sum(
        calls.get(call_id) is not None and calls[call_id].status == "SUCCESS"
        for call_id in ids
    )
    if successes == len(ids):
        return SectionStatus.SUPPORTED
    return SectionStatus.PARTIAL if successes else SectionStatus.UNAVAILABLE


def _warnings(
    label: str, ids: tuple[str, ...], calls: Mapping[str, SourceCall],
) -> tuple[str, ...]:
    failed = [
        call_id for call_id in ids
        if call_id not in calls or calls[call_id].status != "SUCCESS"
    ]
    if not failed:
        return ()
    statuses = {calls[key].status for key in failed if key in calls}
    if "INCONSISTENT_RESPONSE" in statuses:
        return (
            f"La fuente devolvió datos contradictorios para {label}; "
            "no puedo afirmar un cero.",
        )
    if "INVALID_RESPONSE" in statuses:
        return (
            f"No pude confirmar {label}; la fuente devolvió un agregado "
            "incompleto o inválido.",
        )
    return (
        f"No pude calcular {label} en este momento; la fuente no respondió.",
    )


def _ids(candidates: tuple[str, ...], calls: Mapping[str, SourceCall]):
    return tuple(call_id for call_id in candidates if _present(call_id, calls))


def _sources_for(ids: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(_DATASET_BY_CALL[call_id] for call_id in ids))


def _fact(key: str, value: object, source: str, unit: str | None = None):
    return EntityProfileFact(key, value, source, unit)


def _activity(rows, calls, coverage) -> EntityProfileSection:
    ids = _ids(("process_activity", "contract_values"), calls)
    if not ids:
        return unavailable_section(attempted=True)
    facts: list[EntityProfileFact] = []
    warnings = list(_warnings("la actividad", ids, calls))
    if "process_activity" in ids and calls["process_activity"].status == "SUCCESS":
        process = parse_process_activity(rows.get("process_activity", []))
        if process.process_count is None:
            warnings.append(
                "La fuente devolvió un conteo de procesos ilegible; no puedo "
                "afirmar cuántos publicó.",
            )
        else:
            facts.append(_fact(
                "process_count", process.process_count, PROCESS_DATASET, "processes",
            ))
    if "contract_values" in ids and calls["contract_values"].status == "SUCCESS":
        contracts = parse_contract_values(rows.get("contract_values", []))
        # Un conteo válido sobrevive a un monto ilegible: `count(*)` no depende
        # de la columna de valor.
        if contracts is not None and contracts.contract_count is not None:
            facts.append(_fact(
                "contract_count", contracts.contract_count, CONTRACT_DATASET,
                "contracts",
            ))
    return EntityProfileSection(
        _status(ids, calls), _sources_for(ids), coverage, tuple(facts),
        tuple(warnings),
    )


def _values(rows, calls) -> EntityProfileSection:
    ids = _ids(("contract_values",), calls)
    if not ids:
        return unavailable_section(attempted=True)
    parsed = parse_contract_values(rows.get("contract_values", []))
    status = _status(ids, calls)
    warnings = list(_warnings("los valores contractuales", ids, calls))
    facts: list[EntityProfileFact] = []
    if status is SectionStatus.SUPPORTED and parsed is not None:
        if parsed.total_contract_value is not None:
            facts.append(_fact(
                "total_contract_value", parsed.total_contract_value,
                CONTRACT_DATASET, "COP",
            ))
        if parsed.average_contract_value is not None:
            facts.append(_fact(
                "average_contract_value", parsed.average_contract_value,
                CONTRACT_DATASET, "COP",
            ))
        if parsed.total_contract_value is None:
            status = SectionStatus.UNAVAILABLE
            warnings.append(
                "La fuente no reportó el valor de esos contratos; no puedo "
                "afirmar un total, y eso no significa cero.",
            )
    return EntityProfileSection(
        status, (CONTRACT_DATASET,), (("median", "UNAVAILABLE_V1"),),
        tuple(facts), tuple(warnings),
    )


def _modalities(rows, calls) -> EntityProfileSection:
    ids = _ids(("process_activity",), calls)
    if not ids:
        return unavailable_section(attempted=True)
    process = parse_process_activity(rows.get("process_activity", []))
    status = _status(ids, calls)
    warnings = list(_warnings("las modalidades", ids, calls))
    if status is SectionStatus.SUPPORTED and process.truncated:
        status = SectionStatus.PARTIAL
        warnings.append(
            "La fuente alcanzó el límite de modalidades; las participaciones "
            "son parciales.",
        )
    if status is not SectionStatus.UNAVAILABLE and process.process_count is None:
        status = SectionStatus.PARTIAL
        warnings.append(
            "La fuente devolvió un conteo de procesos ilegible; no puedo "
            "afirmar cuántos publicó.",
        )
    facts = () if status is SectionStatus.UNAVAILABLE else (
        _fact("top_modalities", process.modalities, PROCESS_DATASET),
    )
    return EntityProfileSection(
        status, (PROCESS_DATASET,),
        (("group_limit", MODALITY_CARDINALITY_LIMIT),), facts, tuple(warnings),
    )


def _suppliers(rows, calls) -> EntityProfileSection:
    ids = _ids(("supplier_count", "supplier_value"), calls)
    if not ids:
        return unavailable_section(attempted=True)
    facts: list[EntityProfileFact] = []
    warnings = list(_warnings("los proveedores", ids, calls))
    status = _status(ids, calls)
    for call_id, key in (
        ("supplier_count", "top_by_contract_count"),
        ("supplier_value", "top_by_contract_value"),
    ):
        available = call_id in ids and calls[call_id].status == "SUCCESS"
        suppliers = parse_suppliers(rows.get(call_id, [])) if available else ()
        ranking = 'count' if call_id == 'supplier_count' else 'value'
        decision = accredit_ranking(ranking, suppliers, available=available)
        facts.append(_fact(f'{ranking}_ranking_accreditation', decision, CONTRACT_DATASET))
        warnings.extend(ranking_warnings(ranking, decision))
        if available:
            if not decision.amounts_valid or not decision.counts_valid:
                status = SectionStatus.PARTIAL
            facts.append(_fact(key, suppliers, CONTRACT_DATASET))
    return EntityProfileSection(
        status, (CONTRACT_DATASET,),
        (("identity", "typed_supplier_document"),), tuple(facts),
        tuple(dict.fromkeys(warnings)),
    )


def _recent(rows, calls, soql_builder) -> EntityProfileSection:
    ids = _ids(("recent_processes", "recent_contracts"), calls)
    if not ids:
        return unavailable_section(attempted=True)
    facts: list[EntityProfileFact] = []
    for call_id, dataset_id, key in (
        ("recent_processes", PROCESS_DATASET, "processes"),
        ("recent_contracts", CONTRACT_DATASET, "contracts"),
    ):
        if call_id in ids and calls[call_id].status == "SUCCESS":
            facts.append(_fact(
                key,
                parse_recent(
                    rows.get(call_id, []), dataset_id,
                    soql_builder.SPECS[dataset_id],
                ),
                dataset_id,
            ))
    return EntityProfileSection(
        _status(ids, calls), _sources_for(ids),
        (("limit_each", 3), ("ordering", "date_desc_identity_asc")),
        tuple(facts), _warnings("la actividad reciente", ids, calls),
    )


def _temporal(plan, rows, calls, coverage) -> EntityProfileSection:
    """La evolución explícita usa sus propios jobs; en overview refleja actividad."""
    series = _ids(("temporal_processes", "temporal_contracts"), calls)
    if series:
        status = _status(series, calls)
        warnings = list(_warnings("la evolución temporal", series, calls))
        if any(len(rows.get(call_id, [])) >= YEAR_LIMIT for call_id in series):
            status = SectionStatus.PARTIAL
            warnings.append(
                "La serie alcanzó el límite temporal y puede estar truncada.",
            )
        periods = merge_periods(
            rows.get("temporal_processes"), rows.get("temporal_contracts"),
        )
        facts = () if status is SectionStatus.UNAVAILABLE else (
            _fact("periods", periods, "p6dx-8zbt + jbjy-vk9h"),
        )
        return EntityProfileSection(
            status, _sources_for(series), (*coverage, ("year_limit", YEAR_LIMIT)),
            facts, tuple(warnings),
        )
    activity = _activity(rows, calls, coverage)
    if not activity.attempted:
        return unavailable_section()
    values = _values(rows, calls)
    derived = tuple(
        fact for fact in (
            _fact("process_count", activity.fact("process_count"), PROCESS_DATASET)
            if activity.fact("process_count") is not None else None,
            _fact("contract_count", activity.fact("contract_count"), CONTRACT_DATASET)
            if activity.fact("contract_count") is not None else None,
            _fact(
                "contract_value", values.fact("total_contract_value"),
                CONTRACT_DATASET,
            ) if values.fact("total_contract_value") is not None else None,
        ) if fact is not None
    )
    return replace(
        activity,
        coverage=(*coverage, ("granularity", "selected_period")),
        facts=derived,
    )


__all__ = ["SECTION_ORDER", "build_frame_sections"]
