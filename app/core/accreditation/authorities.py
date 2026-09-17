"""Independent authorities over the RAW QUERY (plan V2 9-17, C4A).

Each function reads the raw turn and answers only its own dimension. None of them
parses a filter out of another authority's text, none of them consults the legacy
parser, and none of them invents vocabulary: every span is produced by the
authority that already owns that dimension, and the span is the receipt.

The supplier guard is the one exception in shape: it reports the *surface* the
person used (a literal supplier noun, a negation operator, a quantity), never an
executable parameter.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date as Date
from typing import Any

from app.core.accreditation.spec import Dimension
from app.core.capability_binding.supplier_request_types import (
    Authorization,
    Polarity,
)
from app.core.entity_profile.text_offsets import normalize_with_offsets
from app.utils.money import normalize_text

# Span dimensions that are surface evidence rather than executable dimensions.
SUPPLIER_MENTION = "SUPPLIER_MENTION"
JOB_EVIDENCE = "JOB_EVIDENCE"

_OK = "OK"
_ABSENT = "ABSENT"
_CLARIFY = "CLARIFY"
_OTHER_OWNER = "OTHER_OWNER"


@dataclass(frozen=True, slots=True)
class AccreditedSpan:
    """One span a host authority accredited over the raw turn."""

    dimension: str
    start: int
    end: int
    authority: str
    canonical_value: str | None = None


@dataclass(frozen=True, slots=True)
class AuthorityReport:
    """What one authority answered about the raw turn, its spans and its claim."""

    authority: str
    status: str
    detail: str = ""
    spans: tuple[AccreditedSpan, ...] = ()
    params: tuple[tuple[str, Any], ...] = ()
    clarification: str = ""

    @property
    def ok(self) -> bool:
        return self.status in (_OK, _ABSENT)


def _entity_params(official: str, method: str, alias: str, kind: str = "buyer_entity") -> dict:
    return {
        "entidad": alias,
        "entidad_resolved": official,
        "entidad_resolution": {
            "value": official,
            "method": method,
            "kind": kind,
            "confidence": "high",
            "like_value": None,
            "metadata": {},
        },
    }


def _alias_span(normalized, alias: str) -> tuple[int, int] | None:
    """The occurrence the resolver itself consumed, mapped back to the raw text.

    ``EntityResolver._scan_against_index`` selects aliases with
    ``re.search(r"\\balias\\b", text)`` and scrubs that same first occurrence; the
    span is derived with the same matcher, never with ``str.find``.
    """
    match = re.search(rf"\b{re.escape(alias)}\b", normalized.text)
    if match is None:
        return None
    return normalized.to_original((match.start(), match.end()))


def buyer_authority(raw: str, *, entity_resolver) -> AuthorityReport:
    """Exact entity or declared group only. Fuzzy needs a clarification, always."""
    authority = "entity_resolver.scan_exact"
    normalized = normalize_with_offsets(raw)
    if not normalized.faithful:
        return AuthorityReport(authority, _CLARIFY, "unfaithful_normalization",
                               clarification="No pude acreditar el comprador con exactitud.")
    scan = entity_resolver.scan_entity(normalized.text)
    if scan is not None and scan.method == "scan_exact":
        span = _alias_span(normalized, scan.matched_alias)
        if span is None:
            return AuthorityReport(authority, _CLARIFY, "buyer_span_unresolved",
                                   clarification="No pude acreditar el comprador con exactitud.")
        return AuthorityReport(
            authority, _OK, "buyer_exact",
            spans=(AccreditedSpan(Dimension.BUYER.value, span[0], span[1], authority,
                                  scan.official_name),),
            params=tuple(_entity_params(scan.official_name, scan.method,
                                        scan.matched_alias).items()),
        )
    from app.core.buyer_groups import BuyerGroupError, consume_group_mention

    try:
        group_params, _residual = consume_group_mention(raw, entity_resolver)
    except BuyerGroupError as exc:
        return AuthorityReport(authority, _CLARIFY, "buyer_group_ambiguous",
                               clarification=str(exc))
    if not group_params:
        return AuthorityReport(authority, _CLARIFY, "buyer_not_accredited",
                               clarification=("Necesito una entidad compradora exacta. "
                                              "Por ejemplo «Alcaldía de Galapa»."))
    span = _group_span(normalized, group_params)
    if span is None:
        return AuthorityReport(authority, _CLARIFY, "buyer_group_span_unresolved",
                               clarification="No pude acreditar el grupo comprador con exactitud.")
    value = group_params.get("entidad_resolved") or group_params.get("buyer_group", {}).get("name", "")
    return AuthorityReport(
        authority, _OK, "buyer_declared_group",
        spans=(AccreditedSpan(Dimension.BUYER.value, span[0], span[1], authority, value or None),),
        params=tuple(group_params.items()),
    )


def _group_span(normalized, group_params: dict) -> tuple[int, int] | None:
    from app.core.buyer_groups import get_groups

    for group in get_groups().values():
        for alias in sorted(group.aliases, key=len, reverse=True):
            match = re.search(r"(?<!\w)" + re.escape(alias) + r"(?!\w)", normalized.text)
            if match is not None:
                return normalized.to_original((match.start(), match.end()))
    return None


def time_authority(raw: str, *, today: Date | None,
                   protected_spans: tuple[tuple[int, int], ...] = ()) -> AuthorityReport:
    """The temporal authority's own resolution; TIME stays optional."""
    from app.core.temporal_semantics import resolve_temporal_scope

    resolution = resolve_temporal_scope(raw, today=today, protected_spans=protected_spans)
    authority = "temporal_semantics.resolve_temporal_scope"
    if resolution.should_clarify:
        return AuthorityReport(authority, _CLARIFY, resolution.error_code or "invalid_temporal",
                               clarification=resolution.clarification or "")
    if not resolution.matched:
        return AuthorityReport(authority, _ABSENT, "no_temporal_expression")
    params = tuple(
        (key, value) for key, value in (
            ("fecha_desde", resolution.fecha_desde), ("fecha_hasta", resolution.fecha_hasta),
        ) if value
    )
    spans = ()
    if resolution.span is not None:
        spans = (AccreditedSpan(Dimension.TIME.value, resolution.span[0], resolution.span[1],
                                authority, resolution.phrase),)
    return AuthorityReport(authority, _OK, resolution.precision, spans=spans, params=params)


def actor_authority(raw: str) -> AuthorityReport:
    """Actor ownership: a strong actor reference means the turn is not ours."""
    from app.core.actor_intent import detect_actor_name

    authority = "actor_intent.detect_actor_name"
    normalized = normalize_with_offsets(raw)
    if not normalized.faithful:
        return AuthorityReport(authority, _ABSENT, "unfaithful_normalization")
    reference = detect_actor_name(normalized.text)
    if reference is None:
        return AuthorityReport(authority, _ABSENT, "no_actor_reference")
    # ``consumed_spans`` are the consumed strings, not offsets: the span is
    # located with the same boundary match the authority used to find them.
    surfaces = tuple(dict.fromkeys(
        item for item in (*reference.consumed_spans, reference.observed_name) if item
    ))
    spans = []
    for surface in surfaces:
        for match in re.finditer(rf"(?<!\w){re.escape(surface)}(?!\w)", normalized.text):
            original = normalized.to_original((match.start(), match.end()))
            if original is not None:
                spans.append(AccreditedSpan(Dimension.ACTOR.value, original[0], original[1],
                                            authority, reference.observed_name))
                break
    return AuthorityReport(authority, _OTHER_OWNER, "actor_subject", spans=tuple(spans))


def ranking_authority(raw: str, *,
                      protected_spans: tuple[tuple[int, int], ...] = ()) -> AuthorityReport:
    """Control Language is the only ranking authority; nothing is re-confirmed."""
    from app.core.control_language import (
        FECHA_ASC,
        FECHA_DESC,
        VALOR_ASC,
        VALOR_DESC,
        interpret_control_language,
        locate_control_span,
    )
    from app.core.query_text_consumption import mask_consumed_spans

    authority = "control_language.interpret_control_language"
    masked = mask_consumed_spans(raw, protected_spans) if protected_spans else raw
    request = interpret_control_language(masked)
    if not request.ordering_signal:
        detail = "top_n_only" if request.top_n is not None else "no_ordering"
        params = (("top_n", request.top_n),) if request.top_n is not None else ()
        return AuthorityReport(authority, _OK, detail, params=params)
    supported = {VALOR_DESC}
    located = locate_control_span(masked)
    spans = ()
    if located is not None:
        spans = (AccreditedSpan(Dimension.RANKING.value, located.start, located.end, authority,
                                request.ordering_signal),)
    if request.ordering_signal not in supported:
        unsupported = {VALOR_ASC, FECHA_DESC, FECHA_ASC}
        detail = "unsupported_ordering" if request.ordering_signal in unsupported else "unknown_ordering"
        return AuthorityReport(authority, _CLARIFY, detail, spans=spans,
                               clarification=("La vista de proveedores ordena por valor descendente; "
                                              "no voy a cambiar ese orden en silencio."))
    return AuthorityReport(authority, _OK, "ordering_supported", spans=spans,
                           params=(("ordering_signal", request.ordering_signal),))


def supplier_guard(raw: str) -> AuthorityReport:
    """Polarity and cardinality witnesses, plus the literal mention spans."""
    from app.core.capability_binding.supplier_request_authority import assess_supplier_request

    authority = "supplier_request_authority.assess_supplier_request"
    evidence = assess_supplier_request(raw)
    spans = [
        AccreditedSpan(SUPPLIER_MENTION, mention.span.original[0], mention.span.original[1],
                       authority, mention.noun.value)
        for mention in evidence.mentions
    ]
    for operator in evidence.cardinality.operator_spans:
        spans.append(AccreditedSpan(Dimension.CARDINALITY.value, operator.original[0],
                                    operator.original[1], authority, "operator"))
    for quantity in evidence.cardinality.quantity_spans:
        spans.append(AccreditedSpan(Dimension.CARDINALITY.value, quantity.original[0],
                                    quantity.original[1], authority, "quantity"))
    if evidence.mentions and evidence.polarity is not Polarity.POSITIVE:
        return AuthorityReport(authority, _CLARIFY, "intent_witness_not_positive",
                               spans=tuple(spans),
                               clarification=("Entendí una mención negativa o incierta de proveedores "
                                              "y no voy a convertirla en una búsqueda positiva."))
    if evidence.mentions and evidence.authorization is not Authorization.EXECUTABLE:
        return AuthorityReport(authority, _CLARIFY, "unsupported_cardinality",
                               spans=tuple(spans),
                               clarification=("La vista de proveedores sirve una cantidad exacta "
                                              "determinada; entendí otra y no la voy a cambiar en silencio."))
    if not evidence.mentions and negated_scope_present(raw):
        return AuthorityReport(authority, _CLARIFY, "negation_scope_unaccredited",
                               spans=tuple(spans),
                               clarification=("Entendí una negación y no puedo acreditar su alcance "
                                              "en esta ruta."))
    return AuthorityReport(authority, _OK, "witnesses_accredited", spans=tuple(spans))


def negated_scope_present(raw: str) -> bool:
    """True when an existing negation operator appears with no accreditable scope."""
    from app.core.modality_text import find_occurrences
    from app.core.negation_markers import (
        DIMENSIONAL_ANTI_NEGATION_PATTERNS,
        DIMENSIONAL_NEGATIVE_OPERATORS,
    )

    folded = normalize_text(raw)
    for anti in DIMENSIONAL_ANTI_NEGATION_PATTERNS:
        if find_occurrences(folded, normalize_text(anti)):
            return False
    return any(
        find_occurrences(folded, normalize_text(operator))
        for operator in DIMENSIONAL_NEGATIVE_OPERATORS
    )


def source_authority(raw: str, *, protected_surfaces: tuple[str, ...] = ()) -> AuthorityReport:
    """The universe the person asked for, decided by its own authority."""
    from app.core.contract_universe import ContractUniverse
    from app.core.contract_universe_intent import request as universe_request

    authority = "contract_universe_intent.request"
    decision = universe_request(raw, protected_surfaces=protected_surfaces)
    if decision is None:
        return AuthorityReport(authority, _ABSENT, "capability_universe")
    span = (decision.span[0], decision.span[1])
    spans = (AccreditedSpan(Dimension.SOURCE.value, span[0], span[1], authority,
                            decision.universe.value),)
    if decision.conflict:
        return AuthorityReport(authority, _CLARIFY, "source_conflict", spans=spans,
                               clarification=("Entendí dos universos distintos en la misma pregunta; "
                                              "no voy a elegir uno por ti."))
    if decision.universe is not ContractUniverse.CELEBRATED:
        return AuthorityReport(authority, _CLARIFY, "source_not_contracts", spans=spans,
                               clarification=("La vista de proveedores trabaja sobre contratos; "
                                              "entendí otra fuente."))
    return AuthorityReport(authority, _OK, "contracts_universe", spans=spans,
                           params=(("contract_universe", decision.universe.value),))
