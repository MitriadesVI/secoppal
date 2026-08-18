"""O1B current-opportunity retrieval policy.

Pure and deterministic: no I/O, no clock and no backend access.  The caller
must inject one timezone-aware reference instant for the whole query.  This
module decides applicability and exposes the state/time facts needed by the
SoQL seam; it never filters rows after LIMIT.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


PROCESS_DATASET_ID = "p6dx-8zbt"
REFERENCE_INSTANT_PARAM = "_opportunity_reference_instant"
DEFAULT_STATES = ("Publicado", "Abierto")
DEFAULT_DISCOVERY_WINDOW_WHEN_DEADLINE_UNKNOWN_DAYS = 90
CALENDAR_TIMEZONE_NAME = "America/Bogota"
_CALENDAR_TIMEZONE = ZoneInfo(CALENDAR_TIMEZONE_NAME)

# These are the only procedure-state bundles derived automatically by the
# current parser for opportunity_search.  Any other bundle is treated as an
# explicit caller/user restriction and therefore wins over O1B.
_AUTOMATIC_PROCEDURE_STATE_BUNDLES = frozenset({
    frozenset(("Publicado", "Borrador", "Abierto")),
    frozenset(("Publicado", "Borrador", "En aprobación", "Aprobado", "Abierto")),
})


@dataclass(frozen=True, slots=True)
class OpportunityRetrievalDecision:
    applies: bool
    reason: str
    allowed_states: tuple[str, ...] = ()
    reference_instant: datetime | None = None
    current_colombia_date: date | None = None
    unknown_deadline_publication_since: date | None = None


def _truthy(value: object) -> bool:
    return value not in (None, "", [], {}, (), False)


def has_explicit_time(params: dict[str, Any]) -> bool:
    """Structured time bounds are authoritative, regardless of their origin."""
    return _truthy(params.get("fecha_desde")) or _truthy(params.get("fecha_hasta"))


def _procedure_states(params: dict[str, Any]) -> frozenset[str]:
    raw = params.get("estado_del_procedimiento")
    if raw in (None, "", [], (), False):
        return frozenset()
    values = raw if isinstance(raw, (list, tuple, set, frozenset)) else (raw,)
    return frozenset(str(value) for value in values if _truthy(value))


def has_explicit_state(params: dict[str, Any]) -> bool:
    """Distinguish parser-derived opportunity state from a user restriction.

    QueryRouter emits legacy ``estado``/``estado_field`` only when it consumed
    state language.  Opening/contract fields are never opportunity defaults.
    Direct structured callers are also protected when they provide a procedure
    state bundle other than the two known automatic bundles.
    """
    if _truthy(params.get("estado")) or _truthy(params.get("estado_field")):
        return True
    if any(_truthy(params.get(key)) for key in (
        "estado_de_apertura_del_proceso",
        "estado_contrato",
        "estado_contrato_adicional",
    )):
        return True
    family = params.get("estado_family")
    if _truthy(family) and family != "oferta_abierta":
        return True
    states = _procedure_states(params)
    return bool(states and states not in _AUTOMATIC_PROCEDURE_STATE_BUNDLES)


def is_current_opportunity_default(
    params: dict[str, Any], dataset_id: str,
) -> bool:
    """True only for O1B's exact retrieval scope, independent of the clock."""
    return bool(
        dataset_id == PROCESS_DATASET_ID
        and params.get("intent_type") == "opportunity_search"
        and not has_explicit_time(params)
        and not has_explicit_state(params)
    )


def parse_reference_instant(value: object) -> datetime | None:
    """Parse internal JSON-safe reference metadata without reading a clock."""
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        instant = value
    elif isinstance(value, str):
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError("opportunity reference instant must be datetime or ISO string")
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("opportunity reference instant must be timezone-aware")
    return instant.astimezone(timezone.utc)


def evaluate_current_opportunity_policy(
    params: dict[str, Any],
    dataset_id: str,
    *,
    reference_instant: datetime | None,
) -> OpportunityRetrievalDecision:
    """Return O1B state/time facts; never mutates ``params``."""
    if dataset_id != PROCESS_DATASET_ID:
        return OpportunityRetrievalDecision(False, "not_processes_dataset")
    if params.get("intent_type") != "opportunity_search":
        return OpportunityRetrievalDecision(False, "not_opportunity_search")
    if has_explicit_time(params):
        return OpportunityRetrievalDecision(False, "user_explicit_time")
    if has_explicit_state(params):
        return OpportunityRetrievalDecision(False, "user_explicit_state")
    if reference_instant is None:
        return OpportunityRetrievalDecision(False, "missing_reference_instant")
    if reference_instant.tzinfo is None or reference_instant.utcoffset() is None:
        raise ValueError("reference_instant must be timezone-aware")

    instant = reference_instant.astimezone(timezone.utc)
    current_colombia_date = instant.astimezone(_CALENDAR_TIMEZONE).date()
    return OpportunityRetrievalDecision(
        applies=True,
        reason="current_opportunity_default",
        allowed_states=DEFAULT_STATES,
        reference_instant=instant,
        current_colombia_date=current_colombia_date,
        unknown_deadline_publication_since=(
            current_colombia_date
            - timedelta(days=DEFAULT_DISCOVERY_WINDOW_WHEN_DEADLINE_UNKNOWN_DAYS)
        ),
    )


def format_socrata_calendar_date(value: date) -> str:
    """Format a Colombia civil-date boundary for Socrata calendar_date."""
    return f"{value.isoformat()}T00:00:00.000"
