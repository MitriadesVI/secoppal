"""Turn raw call results into independently honest Entity Profile sections."""
from __future__ import annotations

from typing import Any

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
from app.core.entity_profile.models import (
    CONTRACT_DATASET,
    PROCESS_DATASET,
    EntityProfileFact,
    EntityProfileScope,
    EntityProfileSection,
    SectionStatus,
    SourceCall,
    unavailable_section,
)
from app.core.entity_profile.supplier_ranking_request import (
    DEFAULT_SUPPLIER_REQUEST,
    SupplierRankingRequest,
)
from app.core.entity_profile.supplier_rankings import window_ranking


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


def _executed(ids: tuple[str, ...], executed: frozenset[str]) -> tuple[str, ...]:
    """Sólo las llamadas del plan cuentan para el estado de una sección.

    Una llamada que el plan no envió no falló: no se pidió. Contarla como
    ausencia convertiría una fuente no solicitada en un cero.
    """
    return tuple(call_id for call_id in ids if call_id in executed)


def _sources_for(ids: tuple[str, ...]) -> tuple[str, ...]:
    """Una sección declara las fuentes que realmente la sostienen."""
    return tuple(dict.fromkeys(_DATASET_BY_CALL[call_id] for call_id in ids))


def _status(ids: tuple[str, ...], calls: dict[str, SourceCall]) -> SectionStatus:
    successes = sum(
        calls.get(call_id) is not None and calls[call_id].status == "SUCCESS"
        for call_id in ids
    )
    if successes == len(ids):
        return SectionStatus.SUPPORTED
    return SectionStatus.PARTIAL if successes else SectionStatus.UNAVAILABLE


def _warnings(
    label: str, ids: tuple[str, ...], calls: dict[str, SourceCall],
) -> tuple[str, ...]:
    failed = [
        call_id
        for call_id in ids
        if call_id not in calls or calls[call_id].status != "SUCCESS"
    ]
    if not failed:
        return ()
    statuses = {calls[key].status for key in failed if key in calls}
    if "INCONSISTENT_RESPONSE" in statuses:
        return (f"La fuente devolvió datos contradictorios para {label}; no puedo afirmar un cero.",)
    if "INVALID_RESPONSE" in statuses:
        return (f"No pude confirmar {label}; la fuente devolvió un agregado incompleto o inválido.",)
    if statuses and statuses <= {"BUDGET_EXHAUSTED", "BUILD_ERROR"}:
        # Nothing was sent: blaming the source would be false (A2 X0, A06).
        return (f"No alcancé a consultar {label} en este turno; no es una ausencia de datos.",)
    return (f"No pude calcular {label} en este momento; la fuente no respondió.",)


def _fact(
    key: str, value: object, source: str, unit: str | None = None,
) -> EntityProfileFact:
    return EntityProfileFact(key, value, source, unit)


def _period(scope: EntityProfileScope) -> str:
    if scope.time_from and scope.time_to:
        if (
            scope.time_from.endswith("-01-01")
            and scope.time_to.endswith("-12-31")
            and scope.time_from[:4] == scope.time_to[:4]
        ):
            return scope.time_from[:4]
        return f"{scope.time_from} a {scope.time_to}"
    if scope.time_from:
        return f"desde {scope.time_from}"
    if scope.time_to:
        return f"hasta {scope.time_to}"
    return "todo el histórico disponible"


def _values(
    rows: dict[str, list[dict]], calls: dict[str, SourceCall], executed: frozenset[str],
) -> EntityProfileSection:
    ids = _executed(("contract_values",), executed)
    if not ids:
        return unavailable_section()
    parsed = parse_contract_values(rows.get("contract_values", []))
    status = _status(ids, calls)
    warnings = list(_warnings("los valores contractuales", ids, calls))
    facts: list[EntityProfileFact] = []
    if status is SectionStatus.SUPPORTED and parsed is not None:
        # El transporte respondió y el agregado es legible; cada métrica se
        # publica sólo si la fuente la entregó. Una suma ausente deja la
        # sección no disponible SIN convertirse en $0 ni tocar el conteo.
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
        status,
        (CONTRACT_DATASET,),
        (("median", "UNAVAILABLE_V1"),),
        tuple(facts),
        tuple(warnings),
    )


def _modalities(
    rows: dict[str, list[dict]], calls: dict[str, SourceCall], executed: frozenset[str],
) -> EntityProfileSection:
    ids = _executed(("process_activity",), executed)
    if not ids:
        return unavailable_section()
    process = parse_process_activity(rows.get("process_activity", []))
    status = _status(ids, calls)
    warnings = list(_warnings("las modalidades", ids, calls))
    if status is SectionStatus.SUPPORTED and process.truncated:
        status = SectionStatus.PARTIAL
        warnings.append("La fuente alcanzó el límite de modalidades; las participaciones son parciales.")
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
        status,
        (PROCESS_DATASET,),
        (("group_limit", MODALITY_CARDINALITY_LIMIT),),
        facts,
        tuple(warnings),
    )


def _suppliers(
    rows: dict[str, list[dict]], calls: dict[str, SourceCall],
    executed: frozenset[str], limit: int, metric: str = "both",
) -> EntityProfileSection:
    """La vista de proveedores del plan: el corte que su petición autorizó.

    El corte llega del plan (default V1 cuando no hay request), y se aplica
    igual que en Frame: la fila extra que pide el SQL —el testigo de empate—
    nunca es un resultado. Un ranking que la métrica no ejecuta no deja fact:
    no se pidió, no falló.
    """
    ids = _executed(("supplier_count", "supplier_value"), executed)
    if not ids:
        return unavailable_section()
    facts: list[EntityProfileFact] = []
    for call_id, key, ranking in (
        ("supplier_count", "top_by_contract_count", "count"),
        ("supplier_value", "top_by_contract_value", "value"),
    ):
        if metric not in ("both", ranking):
            continue
        if calls.get(call_id) and calls[call_id].status == "SUCCESS":
            window = window_ranking(
                parse_suppliers(rows.get(call_id, [])), limit, ranking=ranking,
            )
            facts.append(_fact(key, window.visible, CONTRACT_DATASET))
            facts.append(_fact(f"{ranking}_window", window, CONTRACT_DATASET))
    return EntityProfileSection(
        _status(ids, calls),
        (CONTRACT_DATASET,),
        (("identity", "typed_supplier_document"),),
        tuple(facts),
        _warnings("los proveedores", ids, calls),
    )


def _recent(
    rows: dict[str, list[dict]], calls: dict[str, SourceCall], soql_builder: Any,
    executed: frozenset[str],
) -> EntityProfileSection:
    ids = _executed(("recent_processes", "recent_contracts"), executed)
    if not ids:
        return unavailable_section()
    facts: list[EntityProfileFact] = []
    for call_id, dataset_id, key in (
        ("recent_processes", PROCESS_DATASET, "processes"),
        ("recent_contracts", CONTRACT_DATASET, "contracts"),
    ):
        if call_id in ids and calls.get(call_id) and calls[call_id].status == "SUCCESS":
            facts.append(_fact(
                key,
                parse_recent(
                    rows.get(call_id, []), dataset_id,
                    soql_builder.SPECS[dataset_id],
                ),
                dataset_id,
            ))
    return EntityProfileSection(
        _status(ids, calls),
        _sources_for(ids),
        (("limit_each", 3), ("ordering", "date_desc_identity_asc")),
        tuple(facts),
        _warnings("la actividad reciente", ids, calls),
    )


def _temporal(
    rows: dict[str, list[dict]], calls: dict[str, SourceCall], executed: frozenset[str],
) -> EntityProfileSection:
    ids = _executed(("temporal_processes", "temporal_contracts"), executed)
    if not ids:
        return unavailable_section()
    status = _status(ids, calls)
    warnings = list(_warnings("la evolución temporal", ids, calls))
    if any(len(rows.get(call_id, [])) >= YEAR_LIMIT for call_id in ids):
        status = SectionStatus.PARTIAL
        warnings.append("La serie alcanzó el límite temporal y puede estar truncada.")
    periods = merge_periods(
        rows.get("temporal_processes"), rows.get("temporal_contracts"),
    )
    facts = () if status is SectionStatus.UNAVAILABLE else (
        _fact("periods", periods, "p6dx-8zbt + jbjy-vk9h"),
    )
    return EntityProfileSection(
        status,
        _sources_for(ids),
        (("year_limit", YEAR_LIMIT),),
        facts,
        tuple(warnings),
    )


def _supplier_request(plan: Any) -> SupplierRankingRequest:
    """El request del plan; un plan duck-typed sin él es la vista V1 por defecto.

    Los planes reales siempre lo declaran (build_execution_plan); las
    estructuras de prueba que sólo describen foco y llamadas ejecutadas siguen
    midiéndose contra la vista por defecto en vez de fallar.
    """
    request = getattr(plan, "supplier_ranking", None)
    return request if isinstance(request, SupplierRankingRequest) else DEFAULT_SUPPLIER_REQUEST


def build_sections(
    plan: Any,
    scope: EntityProfileScope,
    rows: dict[str, list[dict]],
    calls: dict[str, SourceCall],
    soql_builder: Any,
) -> tuple[EntityProfileSection, ...]:
    """Arma sólo las secciones que el plan ejecutó; el resto queda NO SOLICITADA."""
    calls = section_evidence_calls(rows, calls)
    executed = plan.executed_call_ids
    focus = plan.focus
    request = _supplier_request(plan)
    activity = values = modalities = suppliers = temporal = recent = unavailable_section()
    if focus == "overview":
        process = parse_process_activity(rows.get("process_activity", []))
        contracts = parse_contract_values(rows.get("contract_values", []))
        ids = _executed(("process_activity", "contract_values"), executed)
        facts = []
        activity_warnings = list(_warnings("la actividad", ids, calls))
        if "process_activity" in ids and calls.get("process_activity") and (
            calls["process_activity"].status == "SUCCESS"
        ):
            if process.process_count is None:
                activity_warnings.append(
                    "La fuente devolvió un conteo de procesos ilegible; no "
                    "puedo afirmar cuántos publicó.",
                )
            else:
                facts.append(_fact(
                    "process_count", process.process_count, PROCESS_DATASET, "processes",
                ))
        if "contract_values" in ids and calls.get("contract_values") and (
            calls["contract_values"].status == "SUCCESS"
        ):
            facts.append(_fact("contract_count", contracts.contract_count, CONTRACT_DATASET, "contracts"))
        activity = EntityProfileSection(
            _status(ids, calls),
            _sources_for(ids),
            (("period", _period(scope)),),
            tuple(facts),
            tuple(activity_warnings),
        ) if ids else unavailable_section()
        values = _values(rows, calls, executed)
        modalities = _modalities(rows, calls, executed)
        suppliers = _suppliers(
            rows, calls, executed, request.limit, request.metric.value,
        )
        temporal = EntityProfileSection(
            activity.status,
            activity.source,
            (("period", _period(scope)), ("granularity", "selected_period")),
            tuple(fact for fact in (
                _fact("period", _period(scope), "USER_TIME_SCOPE"),
                _fact("process_count", activity.fact("process_count"), PROCESS_DATASET)
                if activity.fact("process_count") is not None else None,
                _fact("contract_count", activity.fact("contract_count"), CONTRACT_DATASET)
                if activity.fact("contract_count") is not None else None,
                _fact("contract_value", values.fact("total_contract_value"), CONTRACT_DATASET)
                if values.fact("total_contract_value") is not None else None,
            ) if fact is not None),
            activity.warnings,
            attempted=activity.attempted,
        ) if ids else unavailable_section()
        recent = _recent(rows, calls, soql_builder, executed)
    elif focus == "suppliers":
        suppliers = _suppliers(
            rows, calls, executed, request.limit, request.metric.value,
        )
    elif focus == "modalities":
        modalities = _modalities(rows, calls, executed)
    elif focus == "values":
        values = _values(rows, calls, executed)
    elif focus == "recent":
        recent = _recent(rows, calls, soql_builder, executed)
    elif focus == "temporal":
        temporal = _temporal(rows, calls, executed)
    return activity, values, modalities, suppliers, temporal, recent
