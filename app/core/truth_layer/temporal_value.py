"""SECOP Truth Layer T1B-4 — temporal value validation.

One responsibility: deciding whether an observed scalar IS a date, and if so
which instant it denotes. No ranges, no relations, no assessments, no domain
axes.

The load-bearing distinction:

    the PRESENCE of a raw date is not yet a VALID date

T1A says whether a scalar was observed. It does not say whether that scalar is
a date. ``"pendiente"``, ``""``, ``True`` and ``0`` can all be *observed* and
none of them supports a temporal claim. Absent, present-but-unusable and valid
stay three different things, and none decays into another.

Source-zone policy: the binding contract freezes none, so it is declared here.
A SECOP timestamp WITHOUT an offset is read in ``SECOP_SOURCE_TIMEZONE``
(``America/Bogota``) — documented, testable and overridable per call, never a
silent assumption that naive means UTC. A timestamp WITH an explicit offset
keeps its own absolute instant.

This module also owns the primitive that turns a naive wall time into an
instant (``localize_naive``), because both the value parser and the
user-facing wall-time boundary need it and a second copy would be a second
source of truth. The USER-FACING semantics of a wall time — explicit folds,
clarification — live in ``local_wall_time.py``, not here.

Pure and deterministic: no clock, no network, no filesystem, no environment.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R2.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R3.md
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.core.truth_layer.models import FieldObservation

__all__ = [
    "SECOP_SOURCE_TIMEZONE",
    "TemporalValueStatus",
    "IsoTextKind",
    "LocalizationStatus",
    "TemporalValueResolution",
    "resolve_zone",
    "parse_iso_text",
    "localize_naive",
    "resolve_temporal_value",
]


# The declared interpretation zone for SECOP timestamps that carry no offset.
# A decision, not a discovery: documented, testable and overridable per call.
SECOP_SOURCE_TIMEZONE = "America/Bogota"

_UTC = timezone.utc

# Presence (T1A) and validity (T1B-4) are different questions, so both absence
# states are carried through verbatim rather than merged into "no date".
TemporalValueStatus = Literal[
    "valid",
    "missing",
    "not_observed",
    "invalid_type",
    "invalid_format",
    "nonexistent_local_time",
    "ambiguous_local_time",
]

IsoTextKind = Literal["civil_date", "naive", "aware", "invalid_format"]
LocalizationStatus = Literal["valid", "nonexistent_local_time", "ambiguous_local_time"]

_VALID_VALUE_STATUS: frozenset[str] = frozenset(
    {
        "valid",
        "missing",
        "not_observed",
        "invalid_type",
        "invalid_format",
        "nonexistent_local_time",
        "ambiguous_local_time",
    }
)


def resolve_zone(name: str) -> ZoneInfo:
    """Return the IANA zone named ``name``, rejecting anything else."""
    if not isinstance(name, str):
        raise TypeError("timezone must be a string")
    try:
        return ZoneInfo(name)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise ValueError(f"invalid IANA timezone: {name!r}") from exc


def parse_iso_text(text: str) -> tuple[IsoTextKind, date | datetime | None]:
    """Classify an ISO-8601 text as a civil date, a naive time or an instant.

    No format is guessed and no locale is applied: an unparseable string is
    reported as such, never coerced and never treated as absent. Civil dates
    are tried first so that a bare day is not silently promoted to midnight.
    """
    try:
        return "civil_date", date.fromisoformat(text)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return "invalid_format", None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return "naive", parsed
    return "aware", parsed


def localize_naive(
    naive: datetime, zone: ZoneInfo
) -> tuple[LocalizationStatus, datetime | None]:
    """Attach ``zone`` to a naive wall time, reporting gaps and folds.

    A naive local time is not always an instant: during a DST spring-forward
    gap it denotes nothing, and during a fall-back overlap it denotes two
    instants. Neither is resolved here by quietly choosing a fold.
    """
    candidate = naive.replace(tzinfo=zone)
    if candidate.replace(fold=0).utcoffset() == candidate.replace(fold=1).utcoffset():
        return "valid", candidate.replace(fold=0)
    roundtrip = candidate.replace(fold=0).astimezone(_UTC).astimezone(zone)
    if roundtrip.replace(tzinfo=None) != naive:
        return "nonexistent_local_time", None
    return "ambiguous_local_time", None


def start_of_day(civil: date, zone: ZoneInfo) -> tuple[LocalizationStatus, datetime | None]:
    """The instant a civil day starts in ``zone``, gaps and folds reported."""
    return localize_naive(datetime.combine(civil, time.min), zone)


def _derive(
    observation: FieldObservation, source_timezone: str
) -> tuple[TemporalValueStatus, datetime | None, date | None, bool, tuple[str, ...]]:
    zone = resolve_zone(source_timezone)

    if observation.presence == "missing":
        return "missing", None, None, False, ("TEMPORAL_VALUE_MISSING",)
    if observation.presence == "not_observed":
        return "not_observed", None, None, False, ("TEMPORAL_VALUE_NOT_OBSERVED",)

    raw = observation.raw_value
    # bool is checked first: it is a subclass of int, and a flag is never a
    # date. int and float are rejected too — an epoch-like number is a guess,
    # not an observation.
    if isinstance(raw, bool) or not isinstance(raw, str):
        return "invalid_type", None, None, False, ("TEMPORAL_VALUE_NOT_A_DATE_TYPE",)

    if not raw.strip():
        # "" and whitespace are observed scalars that state nothing. They are
        # NOT missing: the source did answer, with an unusable answer.
        return "invalid_format", None, None, False, ("TEMPORAL_VALUE_BLANK_TEXT",)

    kind, parsed = parse_iso_text(raw)
    if kind == "invalid_format":
        return "invalid_format", None, None, False, ("TEMPORAL_VALUE_UNPARSABLE",)

    if kind == "civil_date":
        civil = parsed  # type: ignore[assignment]
        status, instant = start_of_day(civil, zone)
        if status != "valid":
            return status, None, civil, False, (f"TEMPORAL_VALUE_{status.upper()}",)
        return "valid", instant, civil, False, ("TEMPORAL_VALUE_CIVIL_DATE",)

    if kind == "naive":
        status, instant = localize_naive(parsed, zone)  # type: ignore[arg-type]
        if status != "valid":
            return status, None, None, True, (f"TEMPORAL_VALUE_{status.upper()}",)
        return (
            "valid",
            instant,
            None,
            True,
            ("TEMPORAL_VALUE_NAIVE_READ_IN_SOURCE_ZONE",),
        )

    return "valid", parsed, None, True, ("TEMPORAL_VALUE_EXPLICIT_OFFSET",)  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class TemporalValueResolution:
    """A raw observation seen as a date — or as a reason it is not one.

    ``raw_observation`` is the very object supplied by T1A (identity, not an
    equal copy) and is never rewritten. ``parsed_instant`` is a SEPARATE
    representation that exists only when the raw text really is a date.

    The conclusion is derived in ``__post_init__``, so no public constructor —
    ``dataclasses.replace`` included — can mint a resolution whose own inputs
    contradict it.
    """

    raw_observation: FieldObservation
    source_timezone: str
    status: TemporalValueStatus
    parsed_instant: datetime | None
    civil_date: date | None
    has_time_of_day: bool
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.raw_observation, FieldObservation):
            raise TypeError("raw_observation must be a FieldObservation")
        if self.status not in _VALID_VALUE_STATUS:
            raise ValueError(f"invalid TemporalValueStatus: {self.status!r}")
        status, instant, civil, has_time, reasons = _derive(
            self.raw_observation, self.source_timezone
        )
        if self.status != status:
            raise ValueError(
                f"status={self.status!r} contradicts the raw observation, which "
                f"yields {status!r}"
            )
        if self.parsed_instant != instant:
            raise ValueError("parsed_instant must be derived from the raw scalar")
        if self.civil_date != civil:
            raise ValueError("civil_date must be derived from the raw scalar")
        if self.has_time_of_day != has_time:
            raise ValueError("has_time_of_day must be derived from the raw scalar")
        if self.reason_codes != reasons:
            raise ValueError("reason_codes must be derived from the raw scalar")

    @property
    def is_valid(self) -> bool:
        return self.status == "valid"


def resolve_temporal_value(
    observation: FieldObservation,
    *,
    source_timezone: str = SECOP_SOURCE_TIMEZONE,
) -> TemporalValueResolution:
    """Validate one temporal ``FieldObservation`` without altering it."""
    if not isinstance(observation, FieldObservation):
        raise TypeError("resolve_temporal_value requires a FieldObservation")
    status, instant, civil, has_time, reasons = _derive(observation, source_timezone)
    return TemporalValueResolution(
        raw_observation=observation,
        source_timezone=source_timezone,
        status=status,
        parsed_instant=instant,
        civil_date=civil,
        has_time_of_day=has_time,
        reason_codes=reasons,
    )
