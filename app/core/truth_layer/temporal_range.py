"""SECOP Truth Layer T1B-4 — temporal relations and requested ranges.

One responsibility: comparing an already-validated temporal value against the
reference instant and against a requested range. No parsing, no DST folds, no
assessments, no domain axes — those live in ``temporal_value``,
``local_wall_time`` and ``temporal_resolver`` respectively.

Two rules carry most of the weight here:

  * a range expressed in CIVIL DAYS is inclusive of its final day, which is
    represented as a half-open interval ending at the start of the next day;
    a range that carries a time of day is half-open as written;
  * an INVALID range is not an empty range. A reversed or degenerate interval
    yields ``unknown`` membership, never ``outside`` — a malformed query must
    not be answered with a confident "no results".

Range bounds are anchored in ``context.range_timezone`` (the query's calendar),
never in the source zone (the row's calendar). Those are different facts and
the contract keeps them apart.

Pure and deterministic: no clock, no network, no filesystem, no environment.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

from app.core.truth_layer.models import TemporalQueryContext
from app.core.truth_layer.temporal_value import (
    TemporalValueResolution,
    parse_iso_text,
    localize_naive,
    resolve_zone,
    start_of_day,
)

__all__ = [
    "TemporalRelation",
    "RangeMembership",
    "TemporalEvent",
    "END_OF_DAY_EVENTS",
    "RangeBounds",
    "effective_instant",
    "resolve_range_bounds",
    "resolve_relation",
    "resolve_membership",
]


# Frozen by SEMANTIC_CONTRACT section 8. "today" is this contract's name for
# the present.
TemporalRelation = Literal["future", "today", "past", "unknown", "not_applicable"]

RangeMembership = Literal["inside", "outside", "unknown", "not_requested"]

# Frozen by SEMANTIC_CONTRACT section 8.
TemporalEvent = Literal[
    "process_publication",
    "offer_deadline",
    "adjudication",
    "contract_signature",
    "contract_start",
    "contract_end",
    "liquidation_start",
    "liquidation_end",
    "snapshot_reference",
    "unspecified",
]

VALID_RELATIONS: frozenset[str] = frozenset(
    {"future", "today", "past", "unknown", "not_applicable"}
)
VALID_MEMBERSHIP: frozenset[str] = frozenset(
    {"inside", "outside", "unknown", "not_requested"}
)
VALID_EVENTS: frozenset[str] = frozenset(
    {
        "process_publication",
        "offer_deadline",
        "adjudication",
        "contract_signature",
        "contract_start",
        "contract_end",
        "liquidation_start",
        "liquidation_end",
        "snapshot_reference",
        "unspecified",
    }
)

# A deadline expressed as a bare civil date runs to the END of that day.
END_OF_DAY_EVENTS: frozenset[str] = frozenset({"offer_deadline", "contract_end"})


@dataclass(frozen=True, slots=True)
class RangeBounds:
    """The resolved bounds of a requested range, plus why they may be unusable."""

    range_start: str | None
    range_end_exclusive: str | None
    start_instant: datetime | None
    end_exclusive_instant: datetime | None
    requested: bool
    usable: bool
    reason_codes: tuple[str, ...]


def _bound_instant(
    text: str, zone_name: str, *, exclusive_end: bool
) -> tuple[datetime | None, str | None, bool]:
    """Resolve one bound. Returns (instant, canonical text, resolvable)."""
    zone = resolve_zone(zone_name)
    kind, parsed = parse_iso_text(text.strip())
    if kind == "civil_date":
        if exclusive_end:
            # Inclusive final civil day -> half-open interval that ends at the
            # start of the next day.
            next_day = parsed + timedelta(days=1)  # type: ignore[operator]
            status, instant = start_of_day(next_day, zone)
            return (instant, next_day.isoformat(), status == "valid")
        status, instant = start_of_day(parsed, zone)  # type: ignore[arg-type]
        return (instant, text if status == "valid" else None, status == "valid")
    if kind == "naive":
        status, instant = localize_naive(parsed, zone)  # type: ignore[arg-type]
        return (instant, text if status == "valid" else None, status == "valid")
    if kind == "aware":
        return (parsed, text, True)  # type: ignore[return-value]
    return (None, None, False)


def resolve_range_bounds(context: TemporalQueryContext) -> RangeBounds:
    """Derive the half-open bounds of the requested range in the query zone."""
    start_text = context.requested_range_start
    end_text = context.requested_range_end
    if start_text is None and end_text is None:
        return RangeBounds(None, None, None, None, False, False, ())

    zone_name = context.range_timezone
    reasons: list[str] = []
    start_instant: datetime | None = None
    range_start: str | None = None
    end_instant: datetime | None = None
    range_end_exclusive: str | None = None
    usable = True

    if start_text is not None:
        start_instant, range_start, resolvable = _bound_instant(
            start_text, zone_name, exclusive_end=False
        )
        if not resolvable:
            usable = False
            reasons.append("TEMPORAL_RANGE_START_INVALID")

    if end_text is not None:
        end_instant, range_end_exclusive, resolvable = _bound_instant(
            end_text, zone_name, exclusive_end=True
        )
        if not resolvable:
            usable = False
            reasons.append("TEMPORAL_RANGE_END_INVALID")

    if (
        usable
        and start_instant is not None
        and end_instant is not None
        and start_instant >= end_instant
    ):
        # A reversed or degenerate interval is a malformed query, not an empty
        # one. Answering "outside" would turn a user error into a confident
        # zero.
        usable = False
        reasons.append("TEMPORAL_RANGE_REVERSED")

    return RangeBounds(
        range_start=range_start,
        range_end_exclusive=range_end_exclusive,
        start_instant=start_instant,
        end_exclusive_instant=end_instant,
        requested=True,
        usable=usable,
        reason_codes=tuple(reasons),
    )


def effective_instant(
    value: TemporalValueResolution, requested_event: str
) -> datetime | None:
    """The instant a comparison should use for this value and this event.

    A bare civil date used as a deadline means the END of that day, per the
    binding contract. Every other bare civil date keeps the start of its day
    and is compared as a civil day, not as a wall-clock instant.
    """
    if not value.is_valid:
        return None
    if value.civil_date is not None and requested_event in END_OF_DAY_EVENTS:
        zone = resolve_zone(value.source_timezone)
        status, instant = start_of_day(value.civil_date + timedelta(days=1), zone)
        return instant if status == "valid" else None
    return value.parsed_instant


def resolve_relation(
    value: TemporalValueResolution,
    context: TemporalQueryContext,
    requested_event: str,
    applied_field: str | None,
) -> tuple[TemporalRelation, tuple[str, ...]]:
    """Place a validated value in the past, the present or the future."""
    if applied_field is None:
        return "not_applicable", ("TEMPORAL_NO_FIELD_APPLIED",)
    if not value.is_valid:
        return "unknown", ("TEMPORAL_RELATION_UNDECIDABLE",)

    effective = effective_instant(value, requested_event)
    if effective is None:
        return "unknown", ("TEMPORAL_RELATION_UNDECIDABLE",)

    if value.civil_date is not None and requested_event not in END_OF_DAY_EVENTS:
        # A bare civil date is compared as a civil day in the interpretive
        # zone: claiming an hour it never carried would be an invention.
        local_day = context.local_reference.date()
        if value.civil_date == local_day:
            return "today", ("TEMPORAL_RELATION_SAME_CIVIL_DAY",)
        if value.civil_date < local_day:
            return "past", ("TEMPORAL_RELATION_PAST_CIVIL_DAY",)
        return "future", ("TEMPORAL_RELATION_FUTURE_CIVIL_DAY",)

    if effective < context.reference_instant:
        return "past", ("TEMPORAL_RELATION_PAST_INSTANT",)
    if effective > context.reference_instant:
        return "future", ("TEMPORAL_RELATION_FUTURE_INSTANT",)
    return "today", ("TEMPORAL_RELATION_EQUAL_INSTANT",)


def resolve_membership(
    value: TemporalValueResolution,
    context: TemporalQueryContext,
    requested_event: str,
    applied_field: str | None,
) -> tuple[RangeMembership, RangeBounds, tuple[str, ...]]:
    """Decide whether a validated value falls inside the requested range."""
    bounds = resolve_range_bounds(context)

    if not bounds.requested:
        return "not_requested", bounds, ()
    if not bounds.usable:
        return "unknown", bounds, bounds.reason_codes
    if applied_field is None or not value.is_valid:
        return "unknown", bounds, bounds.reason_codes

    effective = effective_instant(value, requested_event)
    if effective is None:
        return "unknown", bounds, bounds.reason_codes

    if bounds.start_instant is not None and effective < bounds.start_instant:
        return "outside", bounds, ("TEMPORAL_RANGE_BEFORE",)
    if (
        bounds.end_exclusive_instant is not None
        and effective >= bounds.end_exclusive_instant
    ):
        return "outside", bounds, ("TEMPORAL_RANGE_AFTER",)
    return "inside", bounds, ("TEMPORAL_RANGE_INSIDE",)
