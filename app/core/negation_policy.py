"""Capability policy for bound dimensional negations.

This module deliberately stays small: binding owns what the user negated,
while this policy decides whether the already-bound request can execute with
the query capabilities that exist today.  It never invents a negative SoQL
predicate.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Iterable, Mapping

from app.core.modality_text import normalize_phrase
from app.core.negation_binding import (
    NegationDimension,
    NegationRequest,
    NegationVerdict,
)
from app.data.departamentos import DEPARTAMENTOS, DEPARTAMENTOS_OFICIALES


@dataclass(frozen=True, slots=True)
class NegationPolicyResult:
    """Transaction-like result of evaluating all requests in one turn."""

    params: dict[str, object]
    requests: tuple[NegationRequest, ...]

    @property
    def blocks_execution(self) -> bool:
        return any(
            request.verdict is not NegationVerdict.EXECUTE
            for request in self.requests
        )


_OFFICIAL_DEPARTMENTS = frozenset(DEPARTAMENTOS_OFICIALES)


def _canonical_department(value: str) -> str | None:
    return DEPARTAMENTOS.get(normalize_phrase(value))


def _canonical_department_set(value: object) -> list[str] | None:
    if not isinstance(value, list) or not value:
        return None
    if not all(type(item) is str and item in _OFFICIAL_DEPARTMENTS for item in value):
        return None
    # Preserve the existing canonical order and reject duplicate/corrupt sets.
    if len(value) != len(set(value)):
        return None
    return list(value)


def _time_is_direct_contradiction(request: NegationRequest, params: Mapping[str, object]) -> bool:
    value = normalize_phrase(request.value)
    if len(value) != 4 or not value.isdigit():
        return False
    return (
        params.get("fecha_desde") == f"{value}-01-01"
        and params.get("fecha_hasta") == f"{value}-12-31"
    )


def _city_is_direct_contradiction(request: NegationRequest, params: Mapping[str, object]) -> bool:
    city = params.get("ciudad_resolved") or params.get("ciudad")
    return isinstance(city, str) and normalize_phrase(city) == normalize_phrase(request.value)


def _remove_contradictory_positive(
    params: dict[str, object], request: NegationRequest,
) -> None:
    """Do not persist one side of an unresolved direct contradiction."""
    if request.dimension is NegationDimension.TIME:
        params.pop("fecha_desde", None)
        params.pop("fecha_hasta", None)
    elif request.dimension is NegationDimension.CITY:
        params.pop("ciudad", None)
        params.pop("ciudad_resolved", None)
        entity_resolution = params.get("entidad_resolution")
        if (
            isinstance(entity_resolution, Mapping)
            and entity_resolution.get("method") == "city_to_entity"
        ):
            params.pop("entidad", None)
            params.pop("entidad_resolved", None)
            params.pop("entidad_resolution", None)
    elif request.dimension is NegationDimension.DEPARTMENT:
        for key in (
            "departamento", "departamento_resolved", "departamento_resolution",
            "departamentos_in", "region", "region_resolution",
        ):
            params.pop(key, None)
        if params.get("actor_conversation_operation") == "SET_TERRITORY":
            params.pop("actor_conversation_operation", None)
            params.pop("actor_conversation_construction", None)


def apply_negation_policy(
    requests: Iterable[NegationRequest],
    params: Mapping[str, object],
) -> NegationPolicyResult:
    """Resolve EXECUTE/CLARIFY/CONTRADICTORY without partial mutation.

    DEPARTMENT is executable only as subtraction from an already materialized,
    finite and canonical ``departamentos_in`` set.  Every other unsupported
    dimension fails closed.  If any request blocks, tentative set subtraction
    is rolled back; only directly contradictory positive fields are scrubbed
    from the unconfirmed turn frame.
    """
    original = dict(params)
    working = dict(original)
    decided: list[NegationRequest] = []

    for request in requests:
        if request.dimension in {
            NegationDimension.OBJECT,
            NegationDimension.MODALITY,
        }:
            decided.append(replace(request, verdict=NegationVerdict.EXECUTE))
            continue

        if request.dimension is NegationDimension.DEPARTMENT:
            canonical = _canonical_department(request.value)
            positive_scalar = working.get("departamento_resolved")
            if canonical and positive_scalar == canonical:
                decided.append(replace(
                    request,
                    verdict=NegationVerdict.CONTRADICTORY,
                    context="same_positive_negative",
                ))
                continue

            positive_set = _canonical_department_set(working.get("departamentos_in"))
            if canonical and positive_set is not None:
                remaining = [item for item in positive_set if item != canonical]
                if not remaining:
                    decided.append(replace(
                        request,
                        verdict=NegationVerdict.CONTRADICTORY,
                        context="empty_department_set",
                    ))
                else:
                    working["departamentos_in"] = remaining
                    decided.append(replace(
                        request,
                        verdict=NegationVerdict.EXECUTE,
                        context="department_set_subtraction",
                    ))
                continue

        if request.dimension is NegationDimension.TIME and _time_is_direct_contradiction(
            request, working,
        ):
            decided.append(replace(
                request,
                verdict=NegationVerdict.CONTRADICTORY,
                context="same_positive_negative",
            ))
            continue

        if request.dimension is NegationDimension.CITY and _city_is_direct_contradiction(
            request, working,
        ):
            decided.append(replace(
                request,
                verdict=NegationVerdict.CONTRADICTORY,
                context="same_positive_negative",
            ))
            continue

        decided.append(replace(request, verdict=NegationVerdict.CLARIFY))

    if all(request.verdict is NegationVerdict.EXECUTE for request in decided):
        return NegationPolicyResult(working, tuple(decided))

    # Roll back every tentative executable mutation. A blocked turn cannot
    # partially apply a supported exclusion while another exclusion is unsafe.
    blocked_params = dict(original)
    for request in decided:
        if request.verdict is NegationVerdict.CONTRADICTORY:
            _remove_contradictory_positive(blocked_params, request)
    return NegationPolicyResult(blocked_params, tuple(decided))
