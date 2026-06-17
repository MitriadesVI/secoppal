"""Opportunity scope policy — block opportunity searches that are too broad.

OPP-SCOPE-001. Pure, deterministic, no I/O. Decides — BEFORE touching
SECOP — whether an ``intent_type="opportunity_search"`` query carries
enough commercial scope to be useful, or whether it would resolve to a
near ``WHERE estado=abierto`` dump (the class of query that drives the
timeout/ranking bottleneck).

Key design choice: territory alone (departamento/ciudad) does NOT count
as a qualifier. P13's ``has_substantial_filter`` already treats territory
as sufficient, which is exactly why a query like "presentarme en bolívar"
executes today. This policy is stricter on purpose, and only for
opportunity searches.

Unlike the original OPP-SCOPE plan, bidder/browse are NOT distinguished
here: any opportunity search without a qualifier blocks and asks for a
topic. This also covers the empty case ("licitaciones abiertas" with no
place) which P13 would only flag, never block.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date as Date

# Value floor (COP) above which an opportunity is specific enough to run
# even without a topic. A billion-peso floor already excludes the long
# tail of small open processes.
MIN_BROAD_OPPORTUNITY_VALUE_COP = 1_000_000_000

# A date window only counts as a qualifier when it is genuinely narrow.
# A full year is not a "recent window".
MAX_NARROW_WINDOW_DAYS = 60

# Topic carriers.
_TOPIC_KEYS = ("objeto", "objeto_or")

# Concrete-entity carriers. Any one of these means the user consciously
# scoped the universe to specific entities.
_ENTITY_KEYS = (
    "entidad",
    "entidad_resolved",
    "entidad_like",
    "entidad_ecosystem",
    "entidad_contains_all",
    "entidad_in_candidates",
)

_DEFAULT_CLARIFICATION = (
    "¿Qué producto, servicio u obra buscas? "
    "Por ejemplo: mantenimiento, alimentación, dotaciones, "
    "construcción o consultoría."
)


@dataclass(frozen=True)
class OpportunityScopeDecision:
    """Result of evaluate_opportunity_scope. Never mutates input params."""

    should_execute: bool
    missing_slots: tuple[str, ...] = ()
    risk_flags: tuple[str, ...] = ()
    clarification_question: str | None = None
    reason: str = ""


def _truthy(value: object) -> bool:
    """A param key 'counts' only when it holds a non-empty value."""
    return value not in (None, "", [], {}, (), False)


def _has_topic(params: dict) -> bool:
    return any(_truthy(params.get(k)) for k in _TOPIC_KEYS)


def _has_entity(params: dict) -> bool:
    return any(_truthy(params.get(k)) for k in _ENTITY_KEYS)


def _has_modality(params: dict) -> bool:
    return _truthy(params.get("modalidad"))


def _has_narrow_date_range(params: dict) -> bool:
    """True when both bounds exist and span <= MAX_NARROW_WINDOW_DAYS days."""
    desde = params.get("fecha_desde")
    hasta = params.get("fecha_hasta")
    if not desde or not hasta:
        return False
    try:
        d0 = Date.fromisoformat(str(desde))
        d1 = Date.fromisoformat(str(hasta))
    except ValueError:
        return False
    return 0 <= (d1 - d0).days <= MAX_NARROW_WINDOW_DAYS


def _has_high_value_floor(params: dict) -> bool:
    """True when valor_min is at or above the broad-opportunity floor."""
    raw = params.get("valor_min")
    if raw in (None, "", False):
        return False
    try:
        return int(raw) >= MIN_BROAD_OPPORTUNITY_VALUE_COP
    except (TypeError, ValueError):
        return False


def _has_qualifier(params: dict) -> bool:
    return any((
        _has_topic(params),
        _has_entity(params),
        _has_modality(params),
        _has_narrow_date_range(params),
        _has_high_value_floor(params),
    ))


def evaluate_opportunity_scope(
    raw_query: str,
    params: dict,
    context: dict | None = None,
) -> OpportunityScopeDecision:
    """Decide whether an opportunity search has enough scope to execute.

    Pure function. ``params`` is read, never mutated. ``raw_query`` and
    ``context`` are accepted for signature symmetry with the rest of the
    QueryPlan pipeline; this policy needs neither, because follow-up
    context inheritance happens upstream (apply_context) before P15 runs.

    Fail-open: anything that is not an opportunity search executes.
    """
    if params.get("intent_type") != "opportunity_search":
        return OpportunityScopeDecision(
            should_execute=True,
            reason="not_opportunity_search",
        )

    if _has_qualifier(params):
        return OpportunityScopeDecision(
            should_execute=True,
            reason="opportunity_scope_sufficient",
        )

    return OpportunityScopeDecision(
        should_execute=False,
        missing_slots=("topic",),
        risk_flags=("opportunity_scope_too_broad",),
        clarification_question=_DEFAULT_CLARIFICATION,
        reason="opportunity_scope_too_broad",
    )
