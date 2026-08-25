"""Typed, policy-neutral binding of negative markers to query dimensions.

The binding result is ephemeral turn metadata.  It intentionally does not add
new executable filters to ``QueryFrame``: later R4 sprints own each dimension's
policy and projection.  Its immediate safety contract is narrower and strict:
text governed by a negative marker is masked before positive extractors run.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, replace
from enum import Enum
from typing import Iterable, Mapping, Pattern

from app.core.negation_markers import (
    NegativeMarker,
    find_dimensional_negative_markers,
)
from app.core.query_text_consumption import mask_consumed_spans
from app.data.departamentos import DEPARTAMENTOS
from app.data.estados import ESTADO_SYNONYMS


class NegationDimension(str, Enum):
    OBJECT = "OBJECT"
    CITY = "CITY"
    DEPARTMENT = "DEPARTMENT"
    BUYER = "BUYER"
    ACTOR = "ACTOR"
    MODALITY = "MODALITY"
    CONTRACT_TYPE = "CONTRACT_TYPE"
    STATE = "STATE"
    TIME = "TIME"


class NegationVerdict(str, Enum):
    EXECUTE = "EXECUTE"
    DEFER = "DEFER"
    CLARIFY = "CLARIFY"
    CONTRADICTORY = "CONTRADICTORY"


@dataclass(frozen=True, slots=True)
class ScopeAnchor:
    dimension: NegationDimension
    value: str


@dataclass(frozen=True, slots=True)
class NegationRequest:
    """Minimum typed representation required by ADR-064."""

    dimension: NegationDimension
    value: str
    value_start: int
    value_end: int
    marker: str
    marker_start: int
    marker_end: int
    verdict: NegationVerdict
    scope_anchor: ScopeAnchor | None = None
    context: str | None = None

    def to_state(self) -> dict[str, object]:
        return {
            "dimension": self.dimension.value,
            "value": self.value,
            "value_start": self.value_start,
            "value_end": self.value_end,
            "marker": self.marker,
            "marker_start": self.marker_start,
            "marker_end": self.marker_end,
            "verdict": self.verdict.value,
            "scope_anchor": (
                None
                if self.scope_anchor is None
                else {
                    "dimension": self.scope_anchor.dimension.value,
                    "value": self.scope_anchor.value,
                }
            ),
            "context": self.context,
        }


@dataclass(frozen=True, slots=True)
class NegationBinding:
    requests: tuple[NegationRequest, ...]
    masked_text: str
    masked_spans: tuple[tuple[int, int], ...]

    @property
    def requires_policy(self) -> bool:
        return any(request.verdict is not NegationVerdict.EXECUTE for request in self.requests)


_BUYER_RE = re.compile(
    r"\b(?:de\s+)?(?:la\s+|el\s+)?(?:alcaldia|gobernacion|secretaria|"
    r"municipio|departamento|distrito|entidad)\b",
    re.IGNORECASE,
)
_ACTOR_ROLE_BEFORE_RE = re.compile(
    r"\b(?:proveedor|contratista)(?:\s+adjudicado)?\s*$", re.IGNORECASE,
)
_TIME_RE = re.compile(
    r"\b(?:20\d{2}|enero|febrero|marzo|abril|mayo|junio|julio|agosto|"
    r"septiembre|octubre|noviembre|diciembre)\b",
    re.IGNORECASE,
)
_CONTRACT_TYPE_RE = re.compile(
    r"\b(?:contratos?\s+(?:de\s+)?(?:obra|suministro|arrendamiento|"
    r"prestacion\s+de\s+servicios|compraventa|interadministrativo)|"
    r"convenios?(?:\s+interadministrativos?|\s+solidarios?)?)\b",
    re.IGNORECASE,
)
_STATE_RE = re.compile(
    r"\b(?:estado\s+)?(?:" + "|".join(
        sorted((re.escape(alias) for alias in ESTADO_SYNONYMS), key=len, reverse=True)
    ) + r")\b",
    re.IGNORECASE,
)
_MONEY_POLARITY_RE = re.compile(
    r"\b(?:por\s+)?menos\s+de\s+(?:\$\s*|cop\s+)?"
    r"(?:\d|un\b|una\b|mil\b|millones?\b|billon(?:es)?\b)",
    re.IGNORECASE,
)
_NEGATED_MONEY_COMPARATOR_RE = re.compile(
    r"\bno(?:\s+sean?|\s+sea)?\s+(?:por\s+)?"
    r"(?:mas|menos|mayor(?:es)?|menor(?:es)?|superior(?:es)?|inferior(?:es)?)\s+de\b",
    re.IGNORECASE,
)
_SINO_RE = re.compile(r"\bsino(?:\s+tambien)?\b", re.IGNORECASE)
_STRONG_BOUNDARY_RE = re.compile(r"[.;:?!\n\r]|\bpero\b", re.IGNORECASE)
_NI_RE = re.compile(r"\s*,?\s*\bni\b\s*", re.IGNORECASE)
_LEADING_GRAMMAR_RE = re.compile(
    r"^(?:(?:de|del|en|para|la|el|las|los|al|que)\s+)+", re.IGNORECASE,
)
_ONLY_GRAMMAR_RE = re.compile(
    r"^(?:(?:de|del|en|para|la|el|las|los|al|que|sean|sea)\s*)*$", re.IGNORECASE,
)


def _overlaps(left: tuple[int, int], right: tuple[int, int]) -> bool:
    return left[0] < right[1] and right[0] < left[1]


def _trim_span(
    text: str,
    start: int,
    end: int,
    dimension: NegationDimension,
) -> tuple[int, int]:
    while start < end and (text[start].isspace() or text[start] in ","):
        start += 1
    while end > start and (text[end - 1].isspace() or text[end - 1] in ","):
        end -= 1
    if dimension in {
        NegationDimension.OBJECT,
        NegationDimension.CITY,
        NegationDimension.DEPARTMENT,
        NegationDimension.BUYER,
        NegationDimension.TIME,
    }:
        match = _LEADING_GRAMMAR_RE.match(text[start:end])
        if match:
            start += match.end()
    if dimension is NegationDimension.TIME:
        year = re.search(r"\b20\d{2}\b", text[start:end])
        if year:
            start += year.start()
            end = start + len(year.group(0))
    return start, end


def _department_match(text: str) -> re.Match[str] | None:
    aliases = sorted(
        {alias for alias in DEPARTAMENTOS if " " not in alias or alias.startswith(("san ", "valle ", "norte "))},
        key=len,
        reverse=True,
    )
    return re.search(
        r"\b(?:" + "|".join(re.escape(alias) for alias in aliases) + r")\b",
        text,
        re.IGNORECASE,
    )


def _first_structured_candidate(
    text: str,
    *,
    city_pattern: Pattern[str],
) -> tuple[NegationDimension, int, int] | None:
    candidates: list[tuple[int, int, int, NegationDimension]] = []
    specs = (
        (NegationDimension.BUYER, _BUYER_RE.search(text), 0),
        (NegationDimension.TIME, _TIME_RE.search(text), 1),
        (NegationDimension.CITY, city_pattern.search(text), 2),
        (NegationDimension.DEPARTMENT, _department_match(text), 3),
        (NegationDimension.CONTRACT_TYPE, _CONTRACT_TYPE_RE.search(text), 4),
        (NegationDimension.STATE, _STATE_RE.search(text), 5),
    )
    for dimension, match, priority in specs:
        if match is not None:
            candidates.append((match.start(), priority, match.end(), dimension))
    if not candidates:
        return None
    start, _, end, dimension = min(candidates)
    return dimension, start, end


def _classify_target(
    text: str,
    *,
    marker: NegativeMarker,
    target_start: int,
    target_end: int,
    city_pattern: Pattern[str],
) -> tuple[NegationDimension, int]:
    role_prefix = text[max(0, marker.start - 40):marker.start]
    if _ACTOR_ROLE_BEFORE_RE.search(role_prefix):
        return NegationDimension.ACTOR, target_end

    target = text[target_start:target_end]
    structured = _first_structured_candidate(target, city_pattern=city_pattern)
    if structured is None:
        return NegationDimension.OBJECT, target_end
    dimension, local_start, _ = structured
    prefix = target[:local_start].strip(" ,")
    if not prefix or _ONLY_GRAMMAR_RE.fullmatch(prefix):
        return dimension, target_end
    # A free residual immediately following the marker owns the negation.  A
    # later resolved span starts a positive dimension and is not crossed.
    return NegationDimension.OBJECT, target_start + local_start


def _protected_money_spans(text: str) -> tuple[tuple[int, int], ...]:
    return tuple(
        match.span()
        for pattern in (_MONEY_POLARITY_RE, _NEGATED_MONEY_COMPARATOR_RE)
        for match in pattern.finditer(text)
    )


def _target_end(text: str, marker: NegativeMarker, next_marker_start: int) -> tuple[int, tuple[int, int] | None]:
    end = next_marker_start
    local = text[marker.end:end]
    sino = _SINO_RE.search(local)
    sino_span = None
    if sino is not None:
        end = marker.end + sino.start()
        sino_span = (marker.end + sino.start(), marker.end + sino.end())
    boundary = _STRONG_BOUNDARY_RE.search(text[marker.end:end])
    if boundary is not None:
        end = marker.end + boundary.start()
    return end, sino_span


def _modality_targets(
    text: str,
    marker: NegativeMarker,
    modality_spans: tuple[tuple[int, int], ...],
) -> tuple[tuple[int, int], ...]:
    following = [span for span in modality_spans if span[0] >= marker.end]
    if not following:
        return ()
    selected: list[tuple[int, int]] = []
    cursor = marker.end
    for span in following:
        gap = text[cursor:span[0]]
        allowed = r"\s*(?:(?:de|del|la|el|las|los|y|o|ni)\s*)*"
        if not re.fullmatch(allowed, gap, re.IGNORECASE):
            break
        selected.append(span)
        cursor = span[1]
    return tuple(selected)


def bind_negations(
    text: str,
    *,
    city_pattern: Pattern[str],
    modality_spans: Iterable[tuple[int, int]] = (),
    protected_spans: Iterable[tuple[int, int]] = (),
) -> NegationBinding:
    """Bind every resolvable external marker and isolate its governed text."""
    modality = tuple(sorted(modality_spans))
    protected = (
        *modality,
        *tuple(protected_spans),
        *_protected_money_spans(text),
    )
    markers = find_dimensional_negative_markers(text, protected_spans=protected)
    requests: list[NegationRequest] = []
    mask_spans: list[tuple[int, int]] = []

    for index, marker in enumerate(markers):
        next_start = markers[index + 1].start if index + 1 < len(markers) else len(text)
        modality_targets = _modality_targets(text, marker, modality)
        if modality_targets:
            for start, end in modality_targets:
                requests.append(NegationRequest(
                    dimension=NegationDimension.MODALITY,
                    value=text[start:end].strip(),
                    value_start=start,
                    value_end=end,
                    marker=marker.text,
                    marker_start=marker.start,
                    marker_end=marker.end,
                    verdict=NegationVerdict.EXECUTE,
                    context="resolved_alias",
                ))
            mask_spans.append((marker.start, modality_targets[-1][1]))
            continue

        end, sino_span = _target_end(text, marker, next_start)
        raw_start = marker.end
        if not text[raw_start:end].strip(" ,"):
            continue
        dimension, governed_end = _classify_target(
            text,
            marker=marker,
            target_start=raw_start,
            target_end=end,
            city_pattern=city_pattern,
        )
        end = governed_end
        if end <= raw_start or not text[raw_start:end].strip(" ,"):
            continue

        pieces: list[tuple[int, int, str | None]] = []
        if dimension is NegationDimension.OBJECT:
            cursor = raw_start
            for separator in _NI_RE.finditer(text[raw_start:end]):
                split_start = raw_start + separator.start()
                split_end = raw_start + separator.end()
                pieces.append((cursor, split_start, None if not pieces else "ni"))
                cursor = split_end
            pieces.append((cursor, end, None if not pieces else "ni"))
        else:
            pieces.append((raw_start, end, None))

        for piece_start, piece_end, context in pieces:
            value_start, value_end = _trim_span(
                text, piece_start, piece_end, dimension,
            )
            if value_start >= value_end:
                continue
            requests.append(NegationRequest(
                dimension=dimension,
                value=text[value_start:value_end],
                value_start=value_start,
                value_end=value_end,
                marker=marker.text,
                marker_start=marker.start,
                marker_end=marker.end,
                verdict=(
                    NegationVerdict.EXECUTE
                    if dimension is NegationDimension.OBJECT
                    and marker.text in {"excluye", "excluir", "excepto"}
                    else NegationVerdict.DEFER
                ),
                context=context,
            ))
        if requests and requests[-1].marker_start == marker.start:
            mask_spans.append((marker.start, end))
            if sino_span is not None:
                mask_spans.append(sino_span)

    merged_mask_spans = tuple(sorted(set(mask_spans)))
    return NegationBinding(
        requests=tuple(requests),
        masked_text=mask_consumed_spans(text, merged_mask_spans),
        masked_spans=merged_mask_spans,
    )


def attach_scope_anchors(
    requests: Iterable[NegationRequest],
    params: Mapping[str, object],
) -> tuple[NegationRequest, ...]:
    """Attach positive containing scope once ordinary extraction has finished."""
    anchored: list[NegationRequest] = []
    for request in requests:
        anchor = request.scope_anchor
        if request.dimension is NegationDimension.CITY:
            if params.get("departamento_resolved"):
                anchor = ScopeAnchor(
                    NegationDimension.DEPARTMENT,
                    str(params["departamento_resolved"]),
                )
            elif params.get("region"):
                anchor = ScopeAnchor(
                    NegationDimension.DEPARTMENT,
                    str(params["region"]),
                )
        anchored.append(replace(request, scope_anchor=anchor))
    return tuple(anchored)


def requests_require_policy(requests: Iterable[NegationRequest | Mapping[str, object]]) -> bool:
    """True when at least one request cannot execute in the current sprint."""
    for request in requests:
        verdict = request.verdict.value if isinstance(request, NegationRequest) else request.get("verdict")
        if verdict != NegationVerdict.EXECUTE.value:
            return True
    return False


def deferred_negation_question(
    requests: Iterable[NegationRequest | Mapping[str, object]],
) -> str:
    """Build a value-safe generic clarification for deferred dimensions."""
    dimensions: set[str] = set()
    for request in requests:
        if isinstance(request, NegationRequest):
            if request.verdict is not NegationVerdict.EXECUTE:
                dimensions.add(request.dimension.value)
        elif request.get("verdict") != NegationVerdict.EXECUTE.value:
            dimension = request.get("dimension")
            if isinstance(dimension, str) and dimension:
                dimensions.add(dimension)
    label = ", ".join(sorted(dimensions)) or "la dimensión indicada"
    return (
        f"Identifiqué una exclusión sobre {label}, pero esa política todavía "
        "no está habilitada de forma segura. ¿Quieres reformular la búsqueda?"
    )
