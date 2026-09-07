"""Semantic equivalence checks for duplicated legacy profile context."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation

from app.core.entity_profile.claim_filters import topic_expression
from app.core.entity_profile.frame_values import (
    Selection,
    SourceKind,
    TopicAll,
    TopicAny,
    TopicNode,
    TopicNot,
    TopicPhrase,
    TopicTerm,
    TopicTrue,
)
from app.utils.money import normalize_text


_STATE_FIELDS = {
    SourceKind.PROCESSES: frozenset(
        {"estado_del_procedimiento", "estado_de_apertura_del_proceso"},
    ),
    SourceKind.CONTRACTS: frozenset(
        {"estado_contrato", "estado_contrato_adicional"},
    ),
}


def material_conflict(
    request: dict, params: dict, sources: frozenset[SourceKind],
) -> str | None:
    """Return the first material dimension whose two legacy copies diverge."""
    request_topic = _positive_request_topic(request)
    params_without_exclusions = dict(params)
    params_without_exclusions.pop("objeto_exclude", None)
    material_topic = topic_expression(params_without_exclusions) or TopicTrue()
    if _topic_key(request_topic) != _topic_key(material_topic):
        return "TOPIC_CONFLICT"
    if _terms(_request_exclusions(request)) != _terms(params.get("objeto_exclude")):
        return "EXCLUSION_CONFLICT"
    if legacy_modality(request) != Selection(
        include=_terms(params.get("modalidad_include_procedure_ids")),
        exclude=_terms(params.get("modalidad_exclude_procedure_ids")),
    ):
        return "MODALITY_CONFLICT"
    if legacy_contract_type(request) != Selection(
        include=_terms(params.get("contract_type_include")),
        exclude=_terms(params.get("contract_type_exclude")),
    ):
        return "CONTRACT_TYPE_CONFLICT"
    if _number(request.get("value_min")) != _number(params.get("valor_min")):
        return "VALUE_CONFLICT"
    if _number(request.get("value_max")) != _number(params.get("valor_max")):
        return "VALUE_CONFLICT"
    state_conflict = _state_conflict(request, params, sources)
    if state_conflict:
        return state_conflict
    if _params_sources(params) not in (None, sources):
        return "SOURCES_CONFLICT"
    if _params_events(params) not in (None, _request_events(request)):
        return "EVENT_CONFLICT"
    return None


def legacy_topic(request: dict) -> TopicNode:
    """Recover only the topic expression the serialized request can prove."""
    node = _positive_request_topic(request)
    exclusions = tuple(
        TopicNot(TopicTerm(term)) for term in _terms(_request_exclusions(request))
    )
    if not exclusions:
        return node
    positive = () if isinstance(node, TopicTrue) else (node,)
    children = (*positive, *exclusions)
    return children[0] if len(children) == 1 else TopicAll(children)


def _positive_request_topic(request: dict) -> TopicNode:
    stored = request.get("topic_expression")
    if isinstance(stored, dict):
        return TopicNode.from_dict(stored)
    terms = tuple(
        TopicTerm(str(term)) for term in request.get("topic") or () if term
    )
    if not terms:
        return TopicTrue()
    return terms[0] if len(terms) == 1 else TopicAll(terms)


def legacy_modality(request: dict) -> Selection:
    return Selection(
        include=_terms(request.get("modality")),
        exclude=_terms(
            request.get("modality_exclude")
            or request.get("modality_exclude_procedure_ids"),
        ),
    )


def legacy_contract_type(request: dict) -> Selection:
    return Selection(
        include=_terms(
            request.get("contract_type") or request.get("contract_type_include"),
        ),
        exclude=_terms(request.get("contract_type_exclude")),
    )


def _request_exclusions(request: dict):
    return request.get("topic_exclude") or request.get("exclusions")


def _topic_key(node: TopicNode):
    if isinstance(node, TopicTrue):
        return ("TRUE",)
    if isinstance(node, TopicTerm):
        return ("TERM", _text(node.term))
    if isinstance(node, TopicPhrase):
        return ("PHRASE", _text(node.canonical))
    if isinstance(node, TopicNot):
        return ("NOT", _topic_key(node.operand))
    if isinstance(node, (TopicAll, TopicAny)):
        operator = "ALL" if isinstance(node, TopicAll) else "ANY"
        return (operator, tuple(sorted({_topic_key(child) for child in node.children})))
    raise ValueError(f"unsupported legacy topic node: {type(node).__name__}")


def _state_conflict(request: dict, params: dict, sources) -> str | None:
    state_keys = {
        "estado", "estado_field", "estado_family",
        *{field for fields in _STATE_FIELDS.values() for field in fields},
    }
    if not state_keys.intersection(request):
        return None
    stated = _state_snapshot(request, sources)
    material = _state_snapshot(params, sources)
    if stated is None or material is None or stated != material:
        return "CURRENT_STATE_CONFLICT"
    return None


def _state_snapshot(payload: dict, sources):
    allowed = {field for source in sources for field in _STATE_FIELDS[source]}
    state: dict[str, tuple[str, ...]] = {}
    for fields in _STATE_FIELDS.values():
        for field in fields:
            values = _terms(payload.get(field))
            if values:
                if field not in allowed:
                    return None
                state[field] = values
    legacy = _terms(payload.get("estado"))
    if legacy:
        field = payload.get("estado_field")
        if not isinstance(field, str) or field not in allowed:
            return None
        if field in state and state[field] != legacy:
            return None
        state[field] = legacy
    return tuple(sorted(state.items()))


def _params_sources(params: dict) -> frozenset[SourceKind] | None:
    raw = params.get("_entity_profile_sources")
    if isinstance(raw, str):
        if raw == "BOTH":
            return frozenset(SourceKind)
        try:
            return frozenset({SourceKind(raw)})
        except ValueError:
            return frozenset()
    scope = params.get("dataset_scope")
    if not isinstance(scope, (list, tuple, set, frozenset)):
        return None
    translated = {
        SourceKind.PROCESSES if value == "procesos" else SourceKind.CONTRACTS
        for value in scope if value in {"procesos", "contratos"}
    }
    return frozenset(translated)


def _request_events(request: dict) -> tuple[str, ...]:
    events = tuple(
        str(claim.get("event")) for claim in request.get("temporal_claims") or ()
        if isinstance(claim, dict) and claim.get("event")
    )
    raw = request.get("event")
    if not events and raw and raw != "NONE":
        events = (str(raw),)
    return tuple(sorted(set(events)))


def _params_events(params: dict) -> tuple[str, ...] | None:
    if "temporal_claims" not in params and "event" not in params:
        return None
    return _request_events(params)


def _terms(raw: object) -> tuple[str, ...]:
    if raw in (None, "", (), []):
        return ()
    values = raw if isinstance(raw, (list, tuple, set, frozenset)) else (raw,)
    return tuple(sorted({_text(value) for value in values if value not in (None, "")}))


def _text(value: object) -> str:
    return normalize_text(str(value)).strip()


def _number(value: object):
    if value is None:
        return None
    if isinstance(value, bool):
        return ("INVALID", repr(value))
    try:
        number = Decimal(str(value))
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return ("INVALID", repr(value))
    return number if number.is_finite() else ("INVALID", repr(value))
