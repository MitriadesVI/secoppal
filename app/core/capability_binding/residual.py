"""Which topic-like residue an existing authority can account for.

A parameter does not enter a capability because the parser produced it; it
enters because an authority accredits its dimension. The parser's topic output
(``objeto``, ``objeto_or``, ``objeto_phrases``) is precisely what the parser
could not place in any structured dimension, so for SUPPLIERS every term must be
accounted for by an authority that already exists:

- profile language and consumed temporal terms (``profile_query_params``);
- the buyer's own surface (``entidad``, ``rewritten_from``);
- a source word accredited by occurrence (``source_request_occurrences``);
- a Control Language span (``interpret_control_language``);
- a municipal name (the territory registry), which makes it territory, not topic;
- the capability's own grammar: ranking criterion and supplier evidence.

Anything else is UNACCREDITED. It is neither dropped nor interpreted, and no word
list lives in this module. Bare numbers follow the same rule.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date as Date

from app.core.capability_binding.ranking_criterion import ranking_criterion_spans
from app.core.capability_binding.supplier_request_types import SupplierRequestEvidence
from app.core.control_language import interpret_control_language
from app.core.entity_profile.turn import profile_query_params
from app.core.municipal_territory import get_municipal_territories
from app.core.query_text_consumption import mask_consumed_spans
from app.core.source_occurrence import source_request_occurrences
from app.core.temporal_semantics import civil_today, resolve_temporal_scope
from app.utils.money import normalize_text

TOPIC_KEYS = ("objeto", "objeto_or", "objeto_phrases")
_WORD = re.compile(r"[a-z0-9ñ]+")
_NUMBER = re.compile(r"\b\d+(?:[.,]\d+)*\b")


@dataclass(frozen=True, slots=True)
class Accounting:
    unaccredited: tuple[str, ...]
    territory_names: tuple[str, ...]
    ordering_accounted: bool


def _words(text: str) -> frozenset[str]:
    return frozenset(_WORD.findall(normalize_text(text)))


def topic_groups(params: dict) -> tuple[tuple[str, ...], ...]:
    """Each topic item with its variants; a nested list is one term's variants."""
    groups: list[tuple[str, ...]] = []
    for key in TOPIC_KEYS:
        value = params.get(key)
        items = [value] if isinstance(value, str) else (value if isinstance(value, (list, tuple)) else [])
        for item in items:
            if isinstance(item, str) and item.strip():
                groups.append((item,))
            elif isinstance(item, (list, tuple)):
                variants = tuple(v for v in item if isinstance(v, str) and v.strip())
                if variants:
                    groups.append(variants)
    return tuple(groups)


def buyer_surfaces(params: dict) -> tuple[str, ...]:
    surfaces = [params.get("entidad")]
    resolution = params.get("entidad_resolution")
    if isinstance(resolution, dict) and isinstance(resolution.get("metadata"), dict):
        surfaces.append(resolution["metadata"].get("rewritten_from"))
        surfaces.append(resolution["metadata"].get("buyer_mention"))
    return tuple(normalize_text(s) for s in surfaces if isinstance(s, str) and s.strip())


def _surface_spans(text: str, surfaces: tuple[str, ...]) -> list[tuple[int, int]]:
    spans = []
    for surface in surfaces:
        start = text.find(surface)
        if start >= 0:
            spans.append((start, start + len(surface)))
    return spans


def _literal_spans(text: str, surface: str) -> tuple[tuple[int, int], ...]:
    """Locate whole literal occurrences without granting by shared vocabulary."""
    tokens = _WORD.findall(normalize_text(surface))
    if not tokens:
        return ()
    pattern = re.compile(
        rf"(?<![a-z0-9ñ]){'\\s+'.join(map(re.escape, tokens))}(?![a-z0-9ñ])",
    )
    return tuple(match.span() for match in pattern.finditer(text))


def _inside(span: tuple[int, int], spans) -> bool:
    return any(a <= span[0] and span[1] <= b for a, b in spans if a is not None)


def account(
    user_query: str,
    params: dict,
    *,
    supplier_evidence: SupplierRequestEvidence,
    today: Date | None = None,
) -> Accounting:
    text = normalize_text(user_query)
    buyer_spans = _surface_spans(text, buyer_surfaces(params))
    free = mask_consumed_spans(text, buyer_spans) if buyer_spans else text

    criterion = ranking_criterion_spans(free)
    control = interpret_control_language(free)
    temporal = resolve_temporal_scope(free, today=civil_today(today))
    territory = get_municipal_territories().name_spans(free)

    accounted_spans: list[tuple[int, int]] = list(buyer_spans)
    accounted_spans.extend(
        occurrence.span for occurrence in source_request_occurrences(free)
    )
    supplier_spans = tuple(span.normalized for span in supplier_evidence.consumed_spans)
    accounted_spans.extend((*criterion, *supplier_spans, *territory))
    for consumed in control.consumed_spans:
        accounted_spans.extend(_literal_spans(free, consumed))

    unaccredited = [
        group[0] for group in topic_groups(profile_query_params(params))
        if not any(
            occurrences
            and all(_inside(occurrence, accounted_spans) for occurrence in occurrences)
            for variant in group
            if (occurrences := _literal_spans(text, variant))
        )
    ]
    protected = [
        *(span.normalized for span in supplier_evidence.cardinality.quantity_spans),
        *([temporal.span] if temporal.span else []),
    ]
    unaccredited.extend(
        match.group(0) for match in _NUMBER.finditer(free)
        if not _inside(match.span(), protected)
    )
    criterion_words = frozenset().union(*(_words(free[a:b]) for a, b in criterion)) if criterion else frozenset()
    ordering_accounted = all(_words(s) <= criterion_words for s in control.consumed_spans)
    return Accounting(
        unaccredited=tuple(dict.fromkeys(unaccredited)),
        territory_names=tuple(free[a:b] for a, b in territory),
        ordering_accounted=ordering_accounted,
    )
