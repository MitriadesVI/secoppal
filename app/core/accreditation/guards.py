"""Guards for the dimensions a capability cannot apply (plan V2 6, C4A.1).

Each guard answers one question with an existing authority: is there a signal of
this dimension in the raw turn that SUPPLIERS must not swallow? They never apply
the filter, never invent grammar and never consult the legacy parser. The signal
is recorded as provenance and the turn stops.

The residual is not a substitute for a guard: a dimension stops being invisible
because its authority looked at the turn, not because a word happened to be in
the functional closed class.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.accreditation.authorities import AccreditedSpan
from app.core.accreditation.spec import Dimension
from app.core.entity_profile.text_offsets import normalize_with_offsets

_OK = "OK"
_ABSENT = "ABSENT"
_CLARIFY = "CLARIFY"


@dataclass(frozen=True, slots=True)
class GuardReport:
    authority: str
    status: str
    detail: str = ""
    spans: tuple[AccreditedSpan, ...] = ()
    clarification: str = ""


def _pending(authority: str, dimension: str, detail: str, clarification: str,
             spans: tuple[AccreditedSpan, ...] = ()) -> GuardReport:
    return GuardReport(authority, _CLARIFY, detail, spans, clarification)


def modality_guard(raw: str, *, dataset: str = "contratos") -> GuardReport:
    """Any modality the resolver recognises is a filter SUPPLIERS cannot apply."""
    from app.core.modality_resolver import resolve as resolve_modality

    authority = "modality_resolver.resolve"
    resolution = resolve_modality(raw, dataset=dataset)
    status = getattr(resolution.status, "value", str(resolution.status))
    if status == "no_match" and not resolution.includes and not resolution.excludes:
        return GuardReport(authority, _ABSENT, "no_modality")
    spans = _modality_spans(raw, resolution)
    return _pending(
        authority, Dimension.MODALITY.value, "UNSUPPORTED_MODALITY",
        "Entendí una modalidad de contratación y la vista de proveedores no la aplica; "
        "no la voy a quitar en silencio.",
        spans,
    )


def _modality_spans(raw: str, resolution) -> tuple[AccreditedSpan, ...]:
    normalized = normalize_with_offsets(raw)
    if not normalized.faithful or normalized.text != resolution.normalized_text:
        return ()
    spans = []
    for start, end in resolution.consumed_spans:
        original = normalized.to_original((start, end))
        if original is not None:
            spans.append(AccreditedSpan(Dimension.MODALITY.value, original[0], original[1],
                                        "modality_resolver.resolve", raw[original[0]:original[1]]))
    return tuple(spans)


def money_guard(raw: str) -> GuardReport:
    """An admissible monetary amount is a filter, never a supplier ranking.

    Years and bare counts are not money: the amount must carry explicit monetary
    evidence *and* be admissible as an amount before this guard stops the turn.
    """
    from app.core.money_admission import has_explicit_monetary_evidence, is_admissible_amount
    from app.core.money_polarity import resolve_money_polarity
    from app.utils.money import money_to_cop

    authority = "money_polarity.resolve_money_polarity"
    for decision in resolve_money_polarity(raw):
        span_text = raw[decision.start:decision.end]
        if not has_explicit_monetary_evidence(span_text):
            continue
        amount = money_to_cop(span_text)
        if amount is None:
            amount = next((money_to_cop(candidate) for candidate in decision.amount_spans
                           if money_to_cop(candidate) is not None), None)
        if not is_admissible_amount(amount, span_text):
            continue
        return _pending(
            authority, Dimension.MONEY.value, "UNSUPPORTED_MONEY",
            "Entendí un monto y la vista de proveedores no lo filtra; no lo voy a "
            "quitar en silencio.",
            (AccreditedSpan(Dimension.MONEY.value, decision.start, decision.end,
                            authority, span_text),),
        )
    return GuardReport(authority, _ABSENT, "no_monetary_evidence")


def geography_guard(raw: str, *, buyer_span: tuple[int, int] | None = None) -> GuardReport:
    """An independent territory is a filter; the buyer's own name is not."""
    from app.core.entity_resolver import TERRITORY_PHRASES

    authority = "entity_resolver.TERRITORY_PHRASES"
    normalized = normalize_with_offsets(raw)
    if not normalized.faithful:
        return GuardReport(authority, _ABSENT, "unfaithful_normalization")
    words = list(re.finditer(r"\S+", normalized.text))
    tokens = [word.group() for word in words]
    found: list[tuple[tuple[int, int], str]] = []
    index = 0
    while index < len(tokens):
        spans = TERRITORY_PHRASES.spans_in(tuple(tokens[index:]))
        if spans and tuple(tokens[index:index + len(spans[0])]) == spans[0]:
            width = len(spans[0])
            original = normalized.to_original(
                (words[index].start(), words[index + width - 1].end()))
            if original is not None:
                found.append((original, " ".join(tokens[index:index + width])))
            index += width
            continue
        index += 1
    independent = [
        (span, text) for span, text in found
        if not (buyer_span is not None
                and span[0] >= buyer_span[0] and span[1] <= buyer_span[1])
    ]
    if not independent:
        return GuardReport(authority, _ABSENT, "no_independent_territory")
    return _pending(
        authority, Dimension.GEOGRAPHY.value, "UNSUPPORTED_GEOGRAPHY",
        "Entendí un territorio independiente del comprador y la vista de proveedores no "
        "lo combina así; no lo voy a quitar en silencio.",
        tuple(AccreditedSpan(Dimension.GEOGRAPHY.value, span[0], span[1], authority, text)
              for span, text in independent),
    )


def actor_exact_guard(raw: str, *, actor_resolver,
                      protected_spans: tuple[tuple[int, int], ...] = ()) -> GuardReport:
    """An exact actor alias outside every span the host already interpreted.

    Naming somebody is not the same as owning the turn: the strong syntactic gate
    (``detect_actor_name``) decides ownership, this guard only refuses to execute a
    supplier list while a concrete actor is named.

    A token the host already accredited to another authority is not a new actor
    signal — the supplier noun ("proveedores"), the buyer's own name and any other
    accredited span are protected. That is a span rule, not a word list: the
    production index contains company-shaped aliases that collide with them.
    """
    authority = "ActorNameResolver.exact_aliases_in"
    if actor_resolver is None:
        return GuardReport(authority, _ABSENT, "resolver_absent")
    found = actor_resolver.exact_aliases_in(raw)
    independent = [
        entry for entry in found
        if not any(entry[1] < end and start < entry[2]
                   for start, end in protected_spans)
    ]
    if not independent:
        return GuardReport(authority, _ABSENT, "no_exact_actor_alias")
    return _pending(
        authority, Dimension.ACTOR.value, "ACTOR_MENTION_PRESENT",
        "Entendí el nombre de un actor concreto y no voy a listar proveedores como si "
        "no lo hubiera visto.",
        tuple(AccreditedSpan(Dimension.ACTOR.value, start, end, authority, alias)
              for alias, start, end in independent),
    )
