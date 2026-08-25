"""Fail-closed policy for contradictory OBJECT retrieval predicates.

``objeto_exclude`` remains the only negative runtime authority.  This module
only asks whether the current positive and negative authorities would project
incompatible predicates; it stores no alternative query representation.
"""
from __future__ import annotations

import unicodedata
from collections.abc import Iterable

from app.core.soql_builder import project_object_term_variants


OBJECT_NEGATION_CONTRADICTION_QUESTION = (
    "El mismo tema quedó incluido y excluido en la búsqueda. "
    "¿Quieres buscarlo o excluirlo?"
)


def _projection_key(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value.casefold())
    return "".join(char for char in folded if not unicodedata.combining(char))


def _project(term: object) -> frozenset[str]:
    if not isinstance(term, str) or not term:
        return frozenset()
    return frozenset(
        _projection_key(variant)
        for variant in project_object_term_variants(term)
    )


def _alternatives(value: object) -> tuple[object, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return (value,)


def _fully_blocked_alternatives(
    alternatives: Iterable[object],
    negative: frozenset[str],
) -> frozenset[str]:
    intersections = [_project(term) & negative for term in alternatives]
    if not intersections or not all(intersections):
        return frozenset()
    return frozenset().union(*intersections)


def object_negation_overlap(params: dict) -> frozenset[str]:
    """Return projected variants that make an OBJECT predicate contradictory.

    A scalar entry in ``objeto`` is required (AND), while a nested entry and
    ``objeto_or`` are alternatives (OR).  An OR predicate conflicts only when
    every alternative is blocked, avoiding false contradictions such as
    ``alimentación o transporte, sin alimentación``.
    """
    negative = frozenset().union(*(
        _project(term) for term in params.get("objeto_exclude") or []
    ))
    if not negative:
        return frozenset()

    overlap: set[str] = set()
    for item in params.get("objeto") or []:
        alternatives = _alternatives(item)
        if len(alternatives) == 1:
            overlap.update(_project(alternatives[0]) & negative)
        else:
            overlap.update(_fully_blocked_alternatives(alternatives, negative))

    objeto_or = tuple(params.get("objeto_or") or [])
    if objeto_or:
        overlap.update(_fully_blocked_alternatives(objeto_or, negative))

    # Phrase retrieval requires every ordered anchor in the same field.  If an
    # exclusion blocks one of those required anchors, the phrase is impossible.
    from app.core.phrase_catalog import spec_for_canonical

    for canonical in params.get("objeto_phrases") or []:
        phrase_spec = spec_for_canonical(str(canonical))
        if phrase_spec:
            for anchor in phrase_spec.ordered_anchors:
                overlap.update({_projection_key(anchor)} & negative)

    return frozenset(overlap)
