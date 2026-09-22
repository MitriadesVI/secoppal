"""Atomic, raw-grounded admission for CONTRACT_ACTIVITY_V1_NARROW."""
from __future__ import annotations

import calendar
import re
from datetime import date as Date
from typing import TYPE_CHECKING

from app.core.buyer_source_names import project_buyer_names
from app.core.contract_universe import CELEBRATED_STATES, fold_state
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.estado_families import detect_estado_family
from app.core.modality_dataset_projection import project_modality_filter
from app.core.modality_filter import ModalityFilter
from app.core.modality_resolution_models import ResolutionStatus
from app.core.modality_resolver import resolve as resolve_modality
from app.utils.money import money_to_cop, normalize_text

from .models import (
    Admission,
    AdmissionStatus,
    ContractActivityRequest,
    MoneyConstraint,
    Operation,
)
from .filters import accredit_filters

if TYPE_CHECKING:
    from app.core.capability_binding.contract_activity_binding import (
        ContractActivityBinding,
    )


_COUNT = re.compile(r"\b(?:cuantos|cuantas|cuente|cuenta|total\s+de)\b")
_SEARCH = re.compile(r"\b(?:lista|listar|liste|muestra|mostrar|enumera|enumerar|enumeralos)\b")
_TOTAL_VALUE = re.compile(r"\b(?:cuanto\s+suman?|valor\s+total|suma\s+de)\b")
_SUPPLIERS = re.compile(r"\b(?:proveedores?|contratistas?)\b")
_RANKING = re.compile(r"\b(?:ordena|ranking|top|con\s+mas|mayores?)\b")
_GROUP_VERB = re.compile(r"\b(?:desglosa|distribuye|agrupa|agrupar|agrupalos)\b")
_GROUP_MODALITY = re.compile(r"\b(?:por\s+modalidad|segun\s+(?:su\s+)?modalidad)\b")
_GROUP_STATE = re.compile(
    r"\b(?:por\s+estado(?:\s+reportado)?|segun\s+(?:su\s+)?estado(?:\s+actual)?)\b"
)
_CURRENT = re.compile(r"\b(?:actualmente|actual|hoy|figuran)\b")
_HISTORICAL_STATE = re.compile(
    r"\b(?:estaban|seguian|permanecian|se\s+encontraban)\b"
)
_PARTIAL_OR_DAY_TIME = re.compile(
    r"\b(?:desde|hasta|entre)\b|\b(?:el\s+)?\d{1,2}\s+de\s+"
    r"(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\b"
)
_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}
_MONTH_YEAR = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\s+(?:de\s+)?((?:19|20)\d{2})\b"
)
_YEAR = re.compile(r"\b((?:19|20)\d{2})\b")
_AMOUNT = (
    r"(?:\d+(?:[.,]\d+)?)\s*"
    r"(?:mil\s+millones|millones?|billones?|pesos?|cop)"
)
_MONEY = re.compile(
    rf"\b(?P<operator>mayor(?:es)?\s+o\s+igual(?:es)?\s+a|"
    rf"menor(?:es)?\s+o\s+igual(?:es)?\s+a|al\s+menos|"
    rf"minimo(?:\s+de)?|a\s+lo\s+sumo|maximo(?:\s+de)?|"
    rf"superior(?:es)?\s+a|mayor(?:es)?\s+a|mas\s+de|por\s+encima\s+de|"
    rf"inferior(?:es)?\s+a|menor(?:es)?\s+a|por\s+debajo\s+de)"
    rf"\s+(?P<amount>{_AMOUNT})\b"
)
_COMPARATOR_CUE = re.compile(
    r"\b(?:superior(?:es)?|mayor(?:es)?|mas\s+de|por\s+encima|"
    r"inferior(?:es)?|menor(?:es)?|por\s+debajo|al\s+menos|"
    r"por\s+lo\s+menos|minimo|maximo|a\s+lo\s+sumo)\b"
)
_MONEY_MARKER = re.compile(r"\b(?:millones?|billones?|pesos?|cop)\b|\$")
_GENERIC_BUYER_WORDS = frozenset({
    "alcaldia", "municipio", "municipal", "gobernacion", "departamento",
    "distrito", "distrital", "entidad", "unidad", "de", "del", "la", "el",
})
_FUNCTIONAL_TOPIC_WORDS = frozenset({
    "actual", "actualmente", "agrupa", "agrupar", "agrupalos", "contrato",
    "contratos", "cuantos", "cuanto", "cuenta", "cuente", "desglosa",
    "distribuye", "estado", "figuran", "firmado", "firmados", "firmo",
    "modalidad", "modalidades", "reportado", "reportados", "segun",
    "suspendido", "suspendidos", "total",
})
_ACTOR_KEYS = (
    "contratista", "actor_name", "actor_document", "documento_proveedor",
    "proveedor_adjudicado",
)


def admit_contract_activity(
    raw: str,
    params: dict,
    *,
    binding: ContractActivityBinding | None = None,
    today: Date | None = None,
) -> Admission:
    """Claim only the narrow envelope; a claimed invalid request is terminal."""
    text = normalize_text(raw)
    if binding is None:
        operation, disposition = _operation(text)
        if disposition is not None:
            return disposition
        assert operation is not None
    else:
        from app.core.capability_binding.contract_activity_binding import (
            accredit_contract_unit,
        )

        unit = accredit_contract_unit(raw, binding)
        if not unit.accredited:
            return _clarify(unit.reason)
        operation = binding.operation

    if any(params.get(key) for key in _ACTOR_KEYS):
        return _clarify("BUYER_ONLY_SCOPE")
    resolution = resolve_profile_scope(params)
    if resolution.scope is None or resolution.identity is None:
        return _clarify("BUYER_SCOPE_REQUIRED")
    if not _buyer_is_present(text, resolution.identity.display_name, resolution.scope.buyer_units):
        return _clarify("ONE_TURN_BUYER_REQUIRED")

    source_names = _source_names(resolution.scope.buyer_units)
    if not source_names:
        return _clarify("BUYER_SOURCE_PROJECTION_REQUIRED")

    if binding is None:
        period, period_error = _period(text)
        if period_error:
            return _clarify(period_error)
        money, money_error = _money(text)
        if money_error:
            return _clarify(money_error)
        modalities, modality_error = _modalities(raw, text, operation)
        if modality_error:
            return _clarify(modality_error)
        states, state_error = _states(text, operation)
        if state_error:
            return _clarify(state_error)
        if _has_unowned_topic(
            params, resolution.identity.display_name, resolution.scope.buyer_units
        ):
            return _clarify("UNSUPPORTED_MATERIAL_FILTER")
    else:
        filters = accredit_filters(raw, params, operation, today=today)
        if not filters.ok:
            return _clarify(filters.reason)
        period = (
            (filters.date_from, filters.date_to)
            if filters.date_from is not None and filters.date_to is not None
            else None
        )
        money = filters.money
        modalities = filters.modality_values
        states = filters.reported_states

    request = ContractActivityRequest(
        raw=raw,
        operation=operation,
        buyer_units=tuple(resolution.scope.buyer_units),
        buyer_display=resolution.identity.display_name,
        source_buyer_names=source_names,
        date_from=period[0] if period else None,
        date_to=period[1] if period else None,
        modality_values=modalities,
        reported_states=states,
        money=money,
    )
    return Admission(AdmissionStatus.READY, request=request)


def _operation(text: str) -> tuple[Operation | None, Admission | None]:
    has_contract = bool(re.search(r"\bcontratos?\b", text))
    ambiguous_unit = bool(re.search(r"\bcontrataciones?\b", text))
    count = bool(_COUNT.search(text))
    if re.search(r"\bprocesos?\b", text) and not has_contract:
        return None, Admission(AdmissionStatus.HANDOFF, reason="PROCESS_NEIGHBOR")
    if not has_contract and not ambiguous_unit:
        return None, Admission(AdmissionStatus.HANDOFF, reason="NOT_CONTRACT_ACTIVITY")
    if ambiguous_unit and not has_contract:
        return None, _clarify("CONTRACT_UNIT_REQUIRED") if count else Admission(
            AdmissionStatus.HANDOFF, reason="AMBIGUOUS_NEIGHBOR"
        )
    search = bool(_SEARCH.search(text))
    total_value = bool(_TOTAL_VALUE.search(text))
    suppliers = bool(_SUPPLIERS.search(text))
    if search or total_value or suppliers:
        if count or _GROUP_VERB.search(text):
            return None, _clarify("MULTIPLE_OPERATIONS")
        reason = "SEARCH_NEIGHBOR" if search else "TOTAL_VALUE_NEIGHBOR"
        if suppliers:
            reason = "SUPPLIERS_NEIGHBOR"
        return None, Admission(AdmissionStatus.HANDOFF, reason=reason)
    if _RANKING.search(text) and re.search(r"\bmodalidades?\b", text):
        return None, _clarify("RANKING_UNSUPPORTED")

    modality_group = bool(_GROUP_MODALITY.search(text)) or bool(
        _GROUP_VERB.search(text) and re.search(r"\bmodalidades?\b", text)
    )
    state_group = bool(_GROUP_STATE.search(text)) or bool(
        _GROUP_VERB.search(text) and re.search(r"\bestado\b", text)
    )
    if re.search(r"\bno\b[^.;]{0,40}\b(?:agrup|desglos|distribu)", text):
        return None, _clarify("NEGATED_OPERATION")
    operation_count = int(count) + int(modality_group) + int(state_group)
    if operation_count > 1:
        return None, _clarify("MULTIPLE_OPERATIONS")
    if modality_group and state_group:
        return None, _clarify("MULTIPLE_BREAKDOWNS")
    if modality_group:
        return Operation.BY_MODALITY, None
    if state_group:
        return Operation.BY_REPORTED_STATE, None
    if count:
        return Operation.TOTAL, None
    return None, Admission(AdmissionStatus.HANDOFF, reason="UNCLAIMED_CONTRACT_NEIGHBOR")


def _period(text: str) -> tuple[tuple[str, str] | None, str | None]:
    period_text = _MONEY.sub(" ", text)
    if _PARTIAL_OR_DAY_TIME.search(period_text):
        return None, "UNSUPPORTED_OR_PARTIAL_SIGNATURE_PERIOD"
    month_matches = tuple(_MONTH_YEAR.finditer(period_text))
    years = tuple(_YEAR.findall(period_text))
    if len(month_matches) > 1 or len(set(years)) > 1:
        return None, "AMBIGUOUS_SIGNATURE_PERIOD"
    if month_matches:
        match = month_matches[0]
        month = _MONTHS[match.group(1)]
        year = int(match.group(2))
        end = calendar.monthrange(year, month)[1]
        return (f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{end:02d}"), None
    if years:
        year = int(years[0])
        return (f"{year:04d}-01-01", f"{year:04d}-12-31"), None
    if any(month in period_text.split() for month in _MONTHS):
        return None, "INCOMPLETE_SIGNATURE_PERIOD"
    return None, None


def _money(text: str) -> tuple[MoneyConstraint | None, str | None]:
    cues = tuple(_COMPARATOR_CUE.finditer(text))
    matches = tuple(_MONEY.finditer(text))
    if len(cues) > 1:
        return None, "MONEY_COMPARATOR_CONFLICT"
    if not matches:
        if cues or _MONEY_MARKER.search(text):
            return None, "MONEY_EVIDENCE_INCOMPLETE"
        return None, None
    if len(matches) != 1 or len(cues) != 1:
        return None, "MONEY_EVIDENCE_INCOMPLETE"
    match = matches[0]
    amount = money_to_cop(match.group("amount"))
    if amount is None or amount <= 0:
        return None, "MONEY_AMOUNT_INVALID"
    surface = match.group("operator")
    if re.match(r"(?:mayor|al\s+menos|minimo)", surface):
        operator = ">=" if re.match(r"(?:mayor.*igual|al\s+menos|minimo)", surface) else ">"
    elif re.match(r"(?:menor.*igual|a\s+lo\s+sumo|maximo)", surface):
        operator = "<="
    elif re.match(r"(?:superior|mas\s+de|por\s+encima)", surface):
        operator = ">"
    else:
        operator = "<"
    return MoneyConstraint(operator, amount), None


def _modalities(raw: str, text: str, operation: Operation) -> tuple[tuple[str, ...], str | None]:
    resolution = resolve_modality(raw, dataset="contratos")
    executable = {
        ResolutionStatus.RESOLVED,
        ResolutionStatus.RESOLVED_WITH_ASSUMPTION,
    }
    if resolution.status in executable:
        if len(resolution.includes) != 1 or resolution.excludes or resolution.issues:
            return (), "MODALITY_COMPOSITION_UNSUPPORTED"
        ids = tuple(resolution.includes[0].procedure_ids)
        projection = project_modality_filter(
            ModalityFilter(include_procedure_ids=ids), "contratos"
        )
        if not projection.include_raw_values:
            return (), "MODALITY_NOT_PROJECTABLE"
        return tuple(projection.include_raw_values), None
    if resolution.status is not ResolutionStatus.NO_MATCH:
        return (), "MODALITY_UNRESOLVED"
    marker = re.search(r"\bmodalidad(?:es)?\b", text)
    if marker and operation is not Operation.BY_MODALITY:
        return (), "UNKNOWN_MODALITY"
    return (), None


def _states(text: str, operation: Operation) -> tuple[tuple[str, ...], str | None]:
    if operation is Operation.BY_REPORTED_STATE:
        return (), None
    tokenized = " ".join(re.findall(r"\b\w+\b", text))
    state = detect_estado_family(tokenized, "contratos")
    if state is None:
        return (), None
    if _HISTORICAL_STATE.search(text) or not _CURRENT.search(text):
        return (), "REPORTED_STATE_MUST_BE_CURRENT"
    values = tuple(state.get("filters", {}).get("estado_contrato") or ())
    celebrated = {fold_state(value) for value in CELEBRATED_STATES}
    if not values or any(fold_state(value) not in celebrated for value in values):
        return (), "REPORTED_STATE_OUTSIDE_CONTRACT_UNIVERSE"
    return values, None


def _source_names(buyer_units: tuple[str, ...]) -> tuple[str, ...]:
    names: list[str] = []
    for buyer in buyer_units:
        projected = project_buyer_names(buyer, "jbjy-vk9h")
        if not projected:
            return ()
        names.extend(projected)
    return tuple(dict.fromkeys(names))


def _buyer_is_present(text: str, display: str, buyer_units: tuple[str, ...]) -> bool:
    authority = normalize_text(" ".join((display, *buyer_units)))
    tokens = {
        token for token in re.findall(r"\b\w+\b", authority)
        if len(token) >= 3 and token not in _GENERIC_BUYER_WORDS
    }
    return any(re.search(rf"\b{re.escape(token)}\b", text) for token in tokens)


def _has_unowned_topic(params: dict, display: str, buyer_units: tuple[str, ...]) -> bool:
    authority = normalize_text(" ".join((display, *buyer_units)))
    buyer_tokens = set(re.findall(r"\b\w+\b", authority)) | set(_GENERIC_BUYER_WORDS)
    values: list[str] = []
    for key in ("objeto", "objeto_or", "objeto_phrases", "objeto_exclude"):
        value = params.get(key) or ()
        if isinstance(value, str):
            values.append(value)
        else:
            for item in value:
                values.extend(item if isinstance(item, list) else (item,))
    for value in values:
        for token in re.findall(r"\b\w+\b", normalize_text(str(value))):
            if token not in buyer_tokens and token not in _FUNCTIONAL_TOPIC_WORDS:
                return True
    return False


def _clarify(reason: str) -> Admission:
    return Admission(AdmissionStatus.CLARIFY, reason=reason)
