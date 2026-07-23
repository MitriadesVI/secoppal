"""Clasificación pura de alternativas coordinadas entre qualifiers.

Opera sobre protocolos estructurales para no depender del módulo de composición,
del catálogo M1 ni de los modelos públicos. La matriz canónica llega validada
por el lexicón.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, Sequence

from app.core.modality_text import (
    comma_count,
    contains_phrase,
    find_occurrences,
    token_distance,
)

_STRENGTH_RANK = {
    "exact_legal": 0,
    "legacy_raw": 1,
    "exact_common": 2,
    "contextual": 3,
    "colloquial": 4,
}

_COORDINATORS = ("y o", "o", "y")
_CONTRASTS = ("pero", "sino", "sin embargo", "en cambio")
_EXTERNAL_NEGATIVE_OPERATORS = (
    "que no sean", "que no sea", "diferentes de", "diferente de",
    "distintos de", "distinto de", "excluyendo", "excepto", "salvo",
    "no", "sin",
)
_ANTI_NEGATION_PATTERNS = (
    "no solo", "sin descartar", "sin excluir",
    "no quiero excluir", "no busco excluir",
)


class RuleLike(Protocol):
    rule_id: str
    target_kind: str
    target_id: str | None
    qualifier_type: str | None
    qualifier_value: str | None
    qualifier_values: tuple[str, ...]
    allowed_retrieval_families: tuple[str, ...]
    attachment_direction: str | None
    max_attachment_tokens: int | None
    match_strength: str


class Occurrence(Protocol):
    norm_start: int
    norm_end: int
    orig_start: int
    orig_end: int
    clause_index: int
    rule: RuleLike
    phrase: str


@dataclass(frozen=True)
class AlternativeSelection:
    primary: Occurrence
    family: str
    procedure_ids: tuple[str, ...]
    norm_start: int
    norm_end: int
    orig_start: int
    orig_end: int
    qualifier_rule_ids: tuple[str, ...]


@dataclass(frozen=True)
class AlternativeConflict:
    primary: Occurrence
    target_ids: tuple[str, ...]
    norm_start: int
    norm_end: int
    orig_start: int
    orig_end: int
    code: str
    explanation: str


@dataclass(frozen=True)
class AlternativeAnalysis:
    selections: tuple[AlternativeSelection, ...]
    conflicts: tuple[AlternativeConflict, ...]

    @property
    def consumed_norm_spans(self) -> tuple[tuple[int, int], ...]:
        return tuple(sorted(
            [(item.norm_start, item.norm_end) for item in self.selections]
            + [(item.norm_start, item.norm_end) for item in self.conflicts]
        ))


def has_explicit_coordinator(gap: str, *, first_gap: str = "") -> bool:
    if any(contains_phrase(gap, marker) for marker in _COORDINATORS):
        return True
    return contains_phrase(gap, "como") and contains_phrase(first_gap, "tanto")


def has_external_negative_operator(norm_gap: str) -> bool:
    """Detecta operadores negativos externos a un efecto lingüístico.

    El llamador entrega únicamente el texto comprendido entre el primario o el
    efecto anterior y el inicio del efecto actual. Por tanto, un ``sin``/``no``
    que forma parte del propio alias técnico nunca aparece aquí. Las
    anti-negaciones se neutralizan primero, preservando longitud y límites.
    """
    chars = list(norm_gap)
    for phrase in _ANTI_NEGATION_PATTERNS:
        for start, end in find_occurrences(norm_gap, phrase):
            chars[start:end] = " " * (end - start)
    operator_gap = "".join(chars)
    return any(contains_phrase(operator_gap, phrase) for phrase in _EXTERNAL_NEGATIVE_OPERATORS)


def attachment_gap(
    primary: Occurrence, effect: Occurrence,
) -> tuple[int, int, int, int] | None:
    """Gap normalizado/original respetando la dirección del efecto."""
    direction = effect.rule.attachment_direction
    if direction == "after":
        return None if primary.norm_end > effect.norm_start else (
            primary.norm_end, effect.norm_start, primary.orig_end, effect.orig_start,
        )
    if direction == "before":
        return None if effect.norm_end > primary.norm_start else (
            effect.norm_end, primary.norm_start, effect.orig_end, primary.orig_start,
        )
    if primary.norm_end <= effect.norm_start:
        return primary.norm_end, effect.norm_start, primary.orig_end, effect.orig_start
    if effect.norm_end <= primary.norm_start:
        return effect.norm_end, primary.norm_start, effect.orig_end, primary.orig_start
    return None


def crosses_intervening_primary(
    primary: Occurrence, effect: Occurrence, primaries: Sequence[Occurrence],
) -> bool:
    """Impide que attachment/refinement atraviese otra mención primaria."""
    low = min(primary.norm_end, effect.norm_end)
    high = max(primary.norm_start, effect.norm_start)
    return any(
        other is not primary
        and other is not effect
        and low <= other.norm_start
        and other.norm_end <= high
        for other in primaries
    )


def structural_attachment_cost(
    text: str, norm: str, gap: tuple[int, int, int, int],
) -> int:
    norm_start, norm_end, orig_start, orig_end = gap
    return token_distance(norm, norm_start, norm_end) + comma_count(text[orig_start:orig_end])


def span_hull(
    primary: Occurrence, effects: Sequence[Occurrence],
) -> tuple[int, int, int, int]:
    return (
        min(primary.norm_start, *(item.norm_start for item in effects)),
        max(primary.norm_end, *(item.norm_end for item in effects)),
        min(primary.orig_start, *(item.orig_start for item in effects)),
        max(primary.orig_end, *(item.orig_end for item in effects)),
    )


def spans_overlap(span: tuple[int, int], others: Sequence[tuple[int, int]]) -> bool:
    return any(span[0] < end and start < span[1] for start, end in others)


def analyze_alternatives(
    *,
    text: str,
    norm: str,
    candidates: Sequence[Occurrence],
    family_by_procedure: Mapping[str, str],
    qualifier_targets: Mapping[tuple[str, str, str], str],
    qualifier_set_targets: Mapping[tuple[str, str, tuple[str, ...]], tuple[str, ...]],
) -> AlternativeAnalysis:
    primaries = tuple(sorted(
        (item for item in candidates if _is_primary(item)),
        key=_occurrence_key,
    ))
    family_primaries = tuple(
        item for item in primaries if item.rule.target_kind == "retrieval_family"
    )
    effects = tuple(sorted(
        (item for item in candidates if _is_effect(item)),
        key=_occurrence_key,
    ))
    selections: list[AlternativeSelection] = []
    conflicts: list[AlternativeConflict] = []
    consumed: list[tuple[int, int]] = []

    for effect in (item for item in effects if item.rule.target_kind == "qualifier_set"):
        primary = _nearest_primary(
            text, norm, primaries, primaries, effect, family_by_procedure,
        )
        if primary is None:
            continue
        gap = attachment_gap(primary, effect)
        if gap is None or has_external_negative_operator(norm[gap[0]:gap[1]]):
            # Un qualifier_set negado externamente no se interpreta como unión
            # positiva. Se deja sin consumir para que el flujo conservador
            # preserve la familia y el texto residual.
            continue
        family = _family(primary, family_by_procedure)
        hull = span_hull(primary, (effect,))
        if spans_overlap(hull[:2], consumed):
            continue
        if family not in effect.rule.allowed_retrieval_families:
            conflicts.append(_family_conflict(
                primary, family, effect, hull,
                qualifier_targets, qualifier_set_targets,
            ))
        else:
            key = (
                family,
                effect.rule.qualifier_type or "",
                tuple(sorted(effect.rule.qualifier_values)),
            )
            procedure_ids = qualifier_set_targets.get(key, ())
            if procedure_ids:
                selections.append(AlternativeSelection(
                    primary, family, procedure_ids, *hull,
                    (effect.rule.rule_id,),
                ))
        consumed.append(hull[:2])

    ordinary = _select_non_overlapping_ordinary_effects([
        effect for effect in effects
        if effect.rule.target_kind == "qualifier"
        and not spans_overlap((effect.norm_start, effect.norm_end), consumed)
    ])
    attached: dict[Occurrence, list[Occurrence]] = {}
    for effect in ordinary:
        primary = _nearest_primary(
            text, norm, family_primaries, primaries, effect, family_by_procedure,
        )
        if primary is not None:
            attached.setdefault(primary, []).append(effect)

    for primary, local_effects in sorted(attached.items(), key=lambda item: _occurrence_key(item[0])):
        family = _family(primary, family_by_procedure)
        local_effects.sort(key=_occurrence_key)
        index = 0
        while index < len(local_effects) - 1:
            first = local_effects[index]
            chain = [first]
            cursor = index + 1
            first_gap = norm[primary.norm_end:first.norm_start]
            while cursor < len(local_effects):
                current = local_effects[cursor]
                previous = chain[-1]
                if current.rule.qualifier_type != first.rule.qualifier_type:
                    break
                if not has_explicit_coordinator(
                    norm[previous.norm_end:current.norm_start], first_gap=first_gap,
                ):
                    break
                chain.append(current)
                cursor += 1
            if len(chain) == 1:
                index += 1
                continue
            eligible: list[Occurrence] = []
            previous: Occurrence | None = None
            for member in chain:
                if previous is None:
                    prefix_start = primary.norm_end
                else:
                    prefix_start = previous.norm_end
                prefix = norm[prefix_start:member.norm_start]
                if not has_external_negative_operator(prefix):
                    eligible.append(member)
                previous = member

            # La cadena estructural existe aunque uno o más miembros hayan sido
            # negados externamente. Si todos quedan bloqueados, no se consume:
            # el flujo ordinario conserva familia y residual.
            if not eligible:
                index = cursor
                continue
            hull = span_hull(primary, tuple(chain))
            if spans_overlap(hull[:2], consumed):
                index = cursor
                continue
            if any(family not in item.rule.allowed_retrieval_families for item in eligible):
                targets = {
                    target
                    for item in eligible
                    for target in _effect_targets(
                        item, qualifier_targets, qualifier_set_targets,
                    )
                }
                conflicts.append(AlternativeConflict(
                    primary, tuple(sorted({primary.rule.target_id or family, *targets})),
                    *hull, "MODALITY_QUALIFIER_FAMILY_CONFLICT",
                    f"La coordinación no es compatible con retrieval_family={family}.",
                ))
            else:
                procedure_ids = tuple(sorted({
                    qualifier_targets[(
                        family,
                        item.rule.qualifier_type or "",
                        item.rule.qualifier_value or "",
                    )]
                    for item in eligible
                }))
                selections.append(AlternativeSelection(
                    primary, family, procedure_ids, *hull,
                    tuple(item.rule.rule_id for item in chain),
                ))
            consumed.append(hull[:2])
            index = cursor

    for primary in family_primaries:
        if spans_overlap((primary.norm_start, primary.norm_end), consumed):
            continue
        family = _family(primary, family_by_procedure)
        compatible = sorted((
            item for item in ordinary
            if family in item.rule.allowed_retrieval_families
            and item.norm_start >= primary.norm_end
            and not crosses_intervening_primary(primary, item, primaries)
        ), key=_occurrence_key)
        for left, right in zip(compatible, compatible[1:]):
            if left.rule.qualifier_type != right.rule.qualifier_type:
                continue
            if not any(contains_phrase(
                norm[left.norm_end:right.norm_start], marker,
            ) for marker in _CONTRASTS):
                continue
            left_target = _single_target(family, left, qualifier_targets)
            right_target = _single_target(family, right, qualifier_targets)
            if not left_target or not right_target or left_target == right_target:
                continue
            hull = span_hull(primary, (left, right))
            if spans_overlap(hull[:2], consumed):
                continue
            conflicts.append(AlternativeConflict(
                primary, tuple(sorted({left_target, right_target})), *hull,
                "MODALITY_QUALIFIER_VALUE_CONFLICT",
                f"El contraste expresa valores incompatibles para retrieval_family={family}.",
            ))
            consumed.append(hull[:2])
            break

    return AlternativeAnalysis(
        tuple(sorted(selections, key=lambda item: (
            item.norm_start, item.norm_end, item.procedure_ids,
        ))),
        tuple(sorted(conflicts, key=lambda item: (
            item.norm_start, item.norm_end, item.code,
        ))),
    )



def _select_non_overlapping_ordinary_effects(
    effects: Sequence[Occurrence],
) -> list[Occurrence]:
    """Resolve aliases anidados antes de agrupar alternativas.

    La selección se limita a qualifiers ordinarios: no compite con primarios,
    fast paths ni qualifier_set. El span más específico (más largo) gana; el
    resto del orden hace el resultado total y determinístico.
    """
    ordered = sorted(
        effects,
        key=lambda effect: (
            -(effect.norm_end - effect.norm_start),
            _STRENGTH_RANK[effect.rule.match_strength],
            effect.norm_start,
            effect.rule.rule_id,
            effect.phrase,
        ),
    )
    accepted: list[Occurrence] = []
    for effect in ordered:
        if any(
            effect.norm_start < other.norm_end
            and other.norm_start < effect.norm_end
            for other in accepted
        ):
            continue
        accepted.append(effect)
    return sorted(accepted, key=_occurrence_key)

def _nearest_primary(
    text: str,
    norm: str,
    choices: Sequence[Occurrence],
    all_primaries: Sequence[Occurrence],
    effect: Occurrence,
    family_by_procedure: Mapping[str, str],
) -> Occurrence | None:
    candidates: list[tuple[int, int, int, int, Occurrence]] = []
    for primary in choices:
        gap = attachment_gap(primary, effect)
        if gap is None or crosses_intervening_primary(primary, effect, all_primaries):
            continue
        ns, ne, _, _ = gap
        cost = structural_attachment_cost(text, norm, gap)
        limit = effect.rule.max_attachment_tokens or 0
        if cost > limit:
            continue
        family_rank = 0 if primary.rule.target_kind == "retrieval_family" else 1
        candidates.append((cost, family_rank, ne - ns, -primary.norm_end, primary))
    return min(candidates, key=lambda item: item[:4])[4] if candidates else None


def _family(item: Occurrence, family_by_procedure: Mapping[str, str]) -> str:
    target = item.rule.target_id or ""
    return target if item.rule.target_kind == "retrieval_family" else family_by_procedure[target]


def _single_target(
    family: str,
    effect: Occurrence,
    targets: Mapping[tuple[str, str, str], str],
) -> str | None:
    return targets.get((
        family,
        effect.rule.qualifier_type or "",
        effect.rule.qualifier_value or "",
    ))


def _effect_targets(
    effect: Occurrence,
    targets: Mapping[tuple[str, str, str], str],
    set_targets: Mapping[tuple[str, str, tuple[str, ...]], tuple[str, ...]],
) -> tuple[str, ...]:
    if effect.rule.target_kind == "qualifier_set":
        values = tuple(sorted(effect.rule.qualifier_values))
        return tuple(sorted({
            target
            for family in effect.rule.allowed_retrieval_families
            for target in set_targets.get((
                family, effect.rule.qualifier_type or "", values,
            ), ())
        }))
    return tuple(sorted({
        target
        for family in effect.rule.allowed_retrieval_families
        if (target := _single_target(family, effect, targets)) is not None
    }))


def _family_conflict(
    primary: Occurrence,
    family: str,
    effect: Occurrence,
    hull: tuple[int, int, int, int],
    targets: Mapping[tuple[str, str, str], str],
    set_targets: Mapping[tuple[str, str, tuple[str, ...]], tuple[str, ...]],
) -> AlternativeConflict:
    effect_targets = _effect_targets(effect, targets, set_targets)
    return AlternativeConflict(
        primary,
        tuple(sorted({primary.rule.target_id or family, *effect_targets})),
        *hull,
        "MODALITY_QUALIFIER_FAMILY_CONFLICT",
        f"El qualifier_set no es compatible con retrieval_family={family}.",
    )


def _is_primary(item: Occurrence) -> bool:
    return item.rule.target_kind in {"retrieval_family", "procedure"} and item.rule.target_id is not None


def _is_effect(item: Occurrence) -> bool:
    return item.rule.target_kind in {"qualifier", "qualifier_set"}


def _occurrence_key(item: Occurrence) -> tuple[int, int, str]:
    return item.norm_start, item.norm_end, item.rule.rule_id
