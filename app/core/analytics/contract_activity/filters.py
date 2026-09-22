"""Existing deterministic authorities adapted to Contract Activity filters."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as Date

from app.core.control_language import locate_control_span
from app.core.contract_universe import CELEBRATED_STATES, fold_state
from app.core.entity_profile.temporal_event import (
    TemporalEvent,
    resolve_temporal_events,
)
from app.core.estado_families import detect_estado_family
from app.core.modality_dataset_projection import project_modality_filter
from app.core.modality_filter import ModalityFilter
from app.core.modality_resolution_models import ResolutionStatus
from app.core.modality_resolver import resolve as resolve_modality
from app.core.money_polarity import MoneyRelation, resolve_money_polarity
from app.core.money_admission import is_admissible_amount
from app.core.temporal_semantics import resolve_temporal_scope
from app.utils.money import money_to_cop, normalize_text

from .models import MoneyConstraint, Operation


_HISTORICAL_STATE = re.compile(
    r"\b(?:estaban|seguian|permanecian|se\s+encontraban|al\s+cierre\s+de)\b"
)
_CURRENT_STATE = re.compile(
    r"\b(?:actualmente|actual|hoy|ahora|figuran|vigente|vigentes|"
    r"observado\s+al\s+consultar|reportado\s+actual)\b"
)
_MONEY_INCLUSIVE_LOWER = re.compile(
    r"(?:>=|igual|al\s+menos|minim|desde|a\s+partir)"
)
_MONEY_INCLUSIVE_UPPER = re.compile(
    r"(?:<=|igual|a\s+lo\s+sumo|maxim|hasta)"
)


@dataclass(frozen=True, slots=True)
class AccreditedFilters:
    date_from: str | None = None
    date_to: str | None = None
    modality_values: tuple[str, ...] = ()
    reported_states: tuple[str, ...] = ()
    money: MoneyConstraint | None = None
    reason: str = ""

    @property
    def ok(self) -> bool:
        return not self.reason


def accredit_filters(
    raw: str,
    params: dict,
    operation: Operation,
    *,
    today: Date | None = None,
) -> AccreditedFilters:
    """Accredit every supported dimension from its existing raw authority."""
    period, reason = _period(raw, params, operation, today=today)
    if reason:
        return AccreditedFilters(reason=reason)
    money, reason = _money(raw, params)
    if reason:
        return AccreditedFilters(reason=reason)
    modalities, reason = _modalities(raw, params, operation)
    if reason:
        return AccreditedFilters(reason=reason)
    states, reason = _states(raw, params, operation)
    if reason:
        return AccreditedFilters(reason=reason)
    reason = _residual_reason(raw, params)
    if reason:
        return AccreditedFilters(reason=reason)
    return AccreditedFilters(
        date_from=period[0] if period else None,
        date_to=period[1] if period else None,
        modality_values=modalities,
        reported_states=states,
        money=money,
    )


def _period(
    raw: str,
    params: dict,
    operation: Operation,
    *,
    today: Date | None,
) -> tuple[tuple[str, str] | None, str | None]:
    text = normalize_text(raw)
    resolution = resolve_temporal_scope(text, today=today)
    if resolution.should_clarify or params.get("_temporal_error"):
        return None, "SIGNATURE_PERIOD_UNRESOLVED"

    # In a state breakdown, "hoy/actualmente" accredits currentness, not a
    # one-day signature period.
    current_only = (
        operation is Operation.BY_REPORTED_STATE
        and resolution.matched
        and bool(_CURRENT_STATE.search(resolution.phrase))
    )
    if current_only:
        return None, None
    if not resolution.matched:
        if params.get("fecha_desde") or params.get("fecha_hasta"):
            return None, "SIGNATURE_PERIOD_NOT_ACCREDITED"
        return None, None
    if resolution.fecha_desde is None or resolution.fecha_hasta is None:
        return None, "SIGNATURE_PERIOD_INCOMPLETE"

    event = resolve_temporal_events(raw, {
        "fecha_desde": resolution.fecha_desde,
        "fecha_hasta": resolution.fecha_hasta,
    })
    if event.is_unsupported or TemporalEvent.PROCESS_PUBLICATION in event.events:
        return None, "SIGNATURE_PERIOD_EVENT_UNSUPPORTED"
    if event.events and event.events != frozenset({TemporalEvent.CONTRACT_SIGNATURE}):
        return None, "SIGNATURE_PERIOD_EVENT_CONFLICT"

    parsed = (params.get("fecha_desde"), params.get("fecha_hasta"))
    accredited = (resolution.fecha_desde, resolution.fecha_hasta)
    if any(parsed) and parsed != accredited:
        return None, "SIGNATURE_PERIOD_AUTHORITY_CONFLICT"
    return accredited, None


def _money(raw: str, params: dict) -> tuple[MoneyConstraint | None, str | None]:
    decisions = resolve_money_polarity(normalize_text(raw))
    material = tuple(
        decision for decision in decisions
        if (decision.comparator_span or decision.relation is MoneyRelation.RANGE)
        and any(
            is_admissible_amount(money_to_cop(amount), amount)
            for amount in decision.amount_spans
        )
    )
    if not material:
        if params.get("valor_min") is not None or params.get("valor_max") is not None:
            return None, "MONEY_FILTER_NOT_ACCREDITED"
        return None, None
    if len(material) != 1:
        return None, "MONEY_FILTER_CONFLICT"
    decision = material[0]
    if decision.relation not in {MoneyRelation.LOWER_BOUND, MoneyRelation.UPPER_BOUND}:
        return None, "MONEY_COMPOSITION_UNSUPPORTED"
    if len(decision.amount_spans) != 1:
        return None, "MONEY_COMPOSITION_UNSUPPORTED"
    amount = money_to_cop(decision.amount_spans[0])
    if amount is None or amount <= 0:
        return None, "MONEY_AMOUNT_INVALID"
    comparator = normalize_text(decision.comparator_span or "")
    if decision.relation is MoneyRelation.LOWER_BOUND:
        operator = ">=" if _MONEY_INCLUSIVE_LOWER.search(comparator) else ">"
        expected = params.get("valor_min")
    else:
        operator = "<=" if _MONEY_INCLUSIVE_UPPER.search(comparator) else "<"
        expected = params.get("valor_max")
    if expected is not None and expected != amount:
        return None, "MONEY_AUTHORITY_CONFLICT"
    return MoneyConstraint(operator, amount), None


def _modalities(
    raw: str,
    params: dict,
    operation: Operation,
) -> tuple[tuple[str, ...], str | None]:
    resolution = resolve_modality(raw, dataset="contratos")
    executable = {ResolutionStatus.RESOLVED, ResolutionStatus.RESOLVED_WITH_ASSUMPTION}
    if resolution.status in executable:
        if len(resolution.includes) != 1 or resolution.excludes or resolution.issues:
            return (), "MODALITY_COMPOSITION_UNSUPPORTED"
        ids = tuple(resolution.includes[0].procedure_ids)
        projection = project_modality_filter(
            ModalityFilter(include_procedure_ids=ids), "contratos"
        )
        if not projection.include_raw_values:
            return (), "MODALITY_NOT_PROJECTABLE"
        parsed = tuple(params.get("modalidad_include_procedure_ids") or ())
        if parsed and set(parsed) != set(ids):
            return (), "MODALITY_AUTHORITY_CONFLICT"
        return tuple(projection.include_raw_values), None
    if resolution.status is not ResolutionStatus.NO_MATCH:
        return (), "MODALITY_UNRESOLVED"
    if params.get("modalidad_include_procedure_ids") or params.get(
        "modalidad_exclude_procedure_ids"
    ):
        return (), "MODALITY_NOT_ACCREDITED"
    return (), None


def _states(
    raw: str,
    params: dict,
    operation: Operation,
) -> tuple[tuple[str, ...], str | None]:
    text = normalize_text(raw)
    if _HISTORICAL_STATE.search(text):
        return (), "HISTORICAL_REPORTED_STATE_UNSUPPORTED"
    if operation is Operation.BY_REPORTED_STATE:
        return (), None
    tokenized = " ".join(re.findall(r"\b\w+\b", text))
    detected = detect_estado_family(tokenized, "contratos")
    parsed_values = tuple(params.get("estado_contrato") or ())
    if detected is None:
        if parsed_values:
            return (), "REPORTED_STATE_NOT_ACCREDITED"
        return (), None
    if not _CURRENT_STATE.search(text):
        return (), "REPORTED_STATE_MUST_BE_CURRENT"
    values = tuple(detected.get("filters", {}).get("estado_contrato") or ())
    celebrated = {fold_state(value) for value in CELEBRATED_STATES}
    if not values or any(fold_state(value) not in celebrated for value in values):
        return (), "REPORTED_STATE_OUTSIDE_CONTRACT_UNIVERSE"
    if parsed_values and set(parsed_values) != set(values):
        return (), "REPORTED_STATE_AUTHORITY_CONFLICT"
    return values, None


def _residual_reason(raw: str, params: dict) -> str | None:
    """Typed dimensions this capability cannot execute.

    Subject matter is not judged here: admission already refused any material
    residual or topic filter from the upstream evidence
    (ANALYTIC_CONTROL_UPSTREAM_V1 removed the per-operation artifact list).
    """
    if params.get("money_clarification"):
        return "MONEY_FILTER_UNRESOLVED"
    ordering = locate_control_span(normalize_text(raw))
    if ordering is not None and ordering.command_led:
        return "ORDERING_COMPOSITION_UNSUPPORTED"
    for key in (
        "contract_type_include", "contract_type_exclude", "entidad_like",
        "entidad_contains_all", "entidad_in_candidates", "entidad_ecosystem",
        "departamento", "municipio",
    ):
        if params.get(key) not in (None, "", (), [], {}):
            return "UNSUPPORTED_MATERIAL_FILTER"
    return None
