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

from app.core.control_language import command_boundary
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

    @classmethod
    def from_state(cls, state: Mapping[str, object]) -> "NegationRequest":
        """Restore the JSON-native ephemeral form transported by Burr."""
        anchor_state = state.get("scope_anchor")
        anchor = None
        if isinstance(anchor_state, Mapping):
            anchor = ScopeAnchor(
                NegationDimension(str(anchor_state["dimension"])),
                str(anchor_state["value"]),
            )
        return cls(
            dimension=NegationDimension(str(state["dimension"])),
            value=str(state.get("value", "")),
            value_start=int(state.get("value_start", 0) or 0),
            value_end=int(state.get("value_end", 0) or 0),
            marker=str(state.get("marker", "")),
            marker_start=int(state.get("marker_start", 0) or 0),
            marker_end=int(state.get("marker_end", 0) or 0),
            verdict=NegationVerdict(str(state.get("verdict", NegationVerdict.DEFER.value))),
            scope_anchor=anchor,
            context=(
                str(state["context"])
                if state.get("context") is not None
                else None
            ),
        )


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
    r"\b(?:proveedor|contratista)(?:\s+adjudicado)?"
    r"(?P<contrast>\s+pero)?\s*$",
    re.IGNORECASE,
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


def _actor_contrast_start(text: str, marker: NegativeMarker) -> int | None:
    """Return the start of ``pero`` when it joins an ACTOR role to ``no``."""
    prefix_start = max(0, marker.start - 40)
    match = _ACTOR_ROLE_BEFORE_RE.search(text[prefix_start:marker.start])
    if match is None or match.group("contrast") is None:
        return None
    return prefix_start + match.start("contrast")


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
    # HCA-R6: a command terminates the negated target. Without this, "sin
    # alimentación ordenados por mayor valor" excludes the object AND the word
    # "ordenados", and the ordering never reaches the builder. Only explicitly
    # command-led spans cut: a bare adjective ("sin obras nuevas") stays part of
    # what is being excluded.
    command = command_boundary(text[marker.end:end])
    if command is not None:
        end = marker.end + command
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
                    else NegationVerdict.DEFER
                ),
                context=context,
            ))
        if requests and requests[-1].marker_start == marker.start:
            mask_start = marker.start
            if dimension is NegationDimension.ACTOR:
                contrast_start = _actor_contrast_start(text, marker)
                if contrast_start is not None:
                    mask_start = contrast_start
            mask_spans.append((mask_start, end))
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
        if verdict not in {NegationVerdict.EXECUTE, NegationVerdict.EXECUTE.value}:
            return True
    return False


def _request_field(
    request: NegationRequest | Mapping[str, object], field: str,
) -> object:
    return getattr(request, field) if isinstance(request, NegationRequest) else request.get(field)


def blocking_negation_reason(
    requests: Iterable[NegationRequest | Mapping[str, object]],
) -> tuple[str, str]:
    """Return stable QueryPlan reason/risk for a fail-closed request set."""
    if any(
        _request_field(request, "verdict")
        in {NegationVerdict.CONTRADICTORY, NegationVerdict.CONTRADICTORY.value}
        for request in requests
    ):
        return (
            "dimensional_negation_contradiction",
            "DIMENSIONAL_NEGATION_CONTRADICTION",
        )
    return "unsupported_negation", "UNSUPPORTED_NEGATION"


def negation_clarification_question(
    requests: Iterable[NegationRequest | Mapping[str, object]],
) -> str:
    """Explain that the exclusion was understood, never that parsing failed."""
    blocked = [
        request for request in requests
        if _request_field(request, "verdict")
        not in {NegationVerdict.EXECUTE, NegationVerdict.EXECUTE.value}
    ]
    contradictory = [
        request for request in blocked
        if _request_field(request, "verdict")
        in {NegationVerdict.CONTRADICTORY, NegationVerdict.CONTRADICTORY.value}
    ]
    if contradictory:
        value = str(_request_field(contradictory[0], "value") or "ese valor").strip()
        return (
            f"La búsqueda incluye y excluye '{value}' al mismo tiempo. "
            "¿Cuál de las dos condiciones quieres conservar?"
        )

    if not blocked:
        return "Necesito que reformules la exclusión para poder aplicarla con seguridad."

    request = blocked[0]
    value = str(_request_field(request, "value") or "ese valor").strip()
    raw_dimension = _request_field(request, "dimension")
    dimension = (
        raw_dimension.value
        if isinstance(raw_dimension, NegationDimension)
        else str(raw_dimension or "")
    )
    messages = {
        NegationDimension.CITY.value: (
            f"Entendí que quieres excluir '{value}', pero todavía no puedo aplicar "
            "esa exclusión municipal con suficiente precisión."
        ),
        NegationDimension.TIME.value: (
            f"Entendí que quieres excluir '{value}', pero aún no puedo representar "
            "huecos temporales de forma segura."
        ),
        NegationDimension.BUYER.value: (
            f"Entendí que quieres excluir la entidad compradora '{value}', pero "
            "todavía no puedo ejecutar esa exclusión con precisión."
        ),
        NegationDimension.ACTOR.value: (
            f"Entendí que quieres excluir al proveedor o contratista '{value}', "
            "pero todavía no puedo ejecutar esa exclusión con precisión."
        ),
        NegationDimension.DEPARTMENT.value: (
            f"Entendí que quieres excluir el departamento '{value}', pero sólo "
            "puedo hacerlo cuando la búsqueda ya define un conjunto regional "
            "finito y seguro."
        ),
        NegationDimension.CONTRACT_TYPE.value: (
            f"Entendí que quieres excluir el tipo contractual '{value}', pero "
            "esa exclusión todavía no es ejecutable de forma segura."
        ),
        NegationDimension.STATE.value: (
            f"Entendí que quieres excluir el estado '{value}', pero esa exclusión "
            "todavía no es ejecutable de forma segura."
        ),
    }
    if len(blocked) == 1 and dimension in messages:
        return messages[dimension]
    dimensions = sorted({
        raw.value if isinstance(raw, NegationDimension) else str(raw or "")
        for raw in (_request_field(item, "dimension") for item in blocked)
    })
    return (
        "Entendí las exclusiones solicitadas sobre "
        f"{', '.join(item for item in dimensions if item)}, pero no puedo "
        "ejecutarlas juntas de forma segura todavía."
    )


# Compatibility alias for R4-S1 callers; S3 resolves DEFER into CLARIFY before
# workflow planning, so new code should use the truthful name above.
deferred_negation_question = negation_clarification_question
