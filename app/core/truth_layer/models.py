from __future__ import annotations

import math
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Literal, TypeAlias
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


RawScalar: TypeAlias = str | int | float | bool | None
ObservationPresence: TypeAlias = Literal[
    "observed",
    "missing",
    "not_observed",
]

_PRESENCE_VALUES = frozenset({"observed", "missing", "not_observed"})
_RAW_VALUE_TYPES = (str, int, float, bool)


@dataclass(frozen=True, slots=True)
class FieldObservation:
    presence: ObservationPresence
    raw_value: RawScalar

    def __post_init__(self) -> None:
        if self.presence not in _PRESENCE_VALUES:
            raise ValueError(f"unsupported observation presence: {self.presence!r}")

        if self.presence == "observed":
            if self.raw_value is None:
                raise ValueError("observed fields require a non-null raw value")
            if type(self.raw_value) not in _RAW_VALUE_TYPES:
                raise TypeError("raw value must be a scalar")
            if type(self.raw_value) is float and not math.isfinite(self.raw_value):
                raise ValueError("raw float value must be finite")
            return

        if self.raw_value is not None:
            raise ValueError(
                f"{self.presence} fields require raw_value=None"
            )


def _validate_observation_fields(instance: object) -> None:
    for model_field in fields(instance):
        if not isinstance(getattr(instance, model_field.name), FieldObservation):
            raise TypeError(f"{model_field.name} must be a FieldObservation")


@dataclass(frozen=True, slots=True)
class ProcessLifecycleSignals:
    raw_procedure_state: FieldObservation
    raw_procedure_state_id: FieldObservation
    raw_opening_state: FieldObservation
    raw_phase: FieldObservation
    raw_summary_state: FieldObservation
    raw_adjudicated: FieldObservation
    raw_adjudication_date: FieldObservation
    raw_adjudication_id: FieldObservation
    raw_portfolio_id: FieldObservation
    raw_modality: FieldObservation

    def __post_init__(self) -> None:
        _validate_observation_fields(self)


@dataclass(frozen=True, slots=True)
class ContractLifecycleSignals:
    raw_contract_state: FieldObservation
    raw_signature_date: FieldObservation
    raw_start_date: FieldObservation
    raw_end_date: FieldObservation
    raw_liquidation_flag: FieldObservation
    raw_liquidation_start_date: FieldObservation
    raw_liquidation_end_date: FieldObservation

    def __post_init__(self) -> None:
        _validate_observation_fields(self)


@dataclass(frozen=True, slots=True)
class ProcessContractReference:
    process_portfolio_id: FieldObservation
    procurement_portfolio_id: FieldObservation

    def __post_init__(self) -> None:
        _validate_observation_fields(self)


@dataclass(frozen=True, slots=True)
class PublicationVisibilitySignals:
    raw_url: FieldObservation
    normalized_url: FieldObservation
    raw_process_id: FieldObservation
    normalization_reason: FieldObservation

    def __post_init__(self) -> None:
        _validate_observation_fields(self)


@dataclass(frozen=True, slots=True)
class TemporalQueryContext:
    """An absolute instant plus the zone used to interpret it.

    Timezone policy (T1B-0C, binding):
    ``OPTION_B_ABSOLUTE_INSTANT_PLUS_INTERPRETIVE_ZONE``.

    ``reference_instant`` is ANY timezone-aware datetime denoting an absolute
    instant; ``reference_timezone`` is the IANA zone used to read that instant
    as a local civil time. The two are deliberately independent: a UTC
    representation and ``America/Bogota`` describe the same instant seen from
    two vantage points, so their offsets need NOT match, and requiring them to
    match would reject a perfectly valid equivalent representation.

    ``requested_range_timezone`` is the zone in which a requested civil range
    is anchored. It defaults to ``reference_timezone`` but is a SEPARATE datum:
    "signed in 2025" is a claim about civil days in some jurisdiction's
    calendar, and that calendar is not necessarily the one used to read the
    reference instant. Leaving it implicit would let a cohort silently shift
    by one day at the boundary.

    This type never interprets a raw local wall time supplied by a user: that
    is a separate, explicit boundary (``LocalWallTimeResolver``), because a
    wall time can be nonexistent or ambiguous while an instant never is. No
    clock is read here — ``reference_instant`` always arrives from the caller.
    """

    reference_instant: datetime
    reference_timezone: str
    requested_range_start: str | None
    requested_range_end: str | None
    requested_event_raw: str | None
    requested_range_timezone: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.reference_instant, datetime):
            raise TypeError("reference_instant must be a datetime")
        if (
            self.reference_instant.tzinfo is None
            or self.reference_instant.utcoffset() is None
        ):
            raise ValueError("reference_instant must be timezone-aware")
        if not isinstance(self.reference_timezone, str):
            raise TypeError("reference_timezone must be a string")
        try:
            ZoneInfo(self.reference_timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError("reference_timezone must be a valid IANA zone") from exc

        for field_name in (
            "requested_range_start",
            "requested_range_end",
            "requested_event_raw",
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field_name} must be a string or None")

        if self.requested_range_timezone is not None:
            if not isinstance(self.requested_range_timezone, str):
                raise TypeError(
                    "requested_range_timezone must be a string or None"
                )
            try:
                ZoneInfo(self.requested_range_timezone)
            except (ValueError, ZoneInfoNotFoundError) as exc:
                raise ValueError(
                    "requested_range_timezone must be a valid IANA zone"
                ) from exc

    @property
    def range_timezone(self) -> str:
        """The zone a requested civil range is anchored in.

        Explicit when declared; otherwise the interpretive zone. Never the
        source zone: the range comes from the query, not from the row.
        """
        if self.requested_range_timezone is not None:
            return self.requested_range_timezone
        return self.reference_timezone

    @property
    def local_reference(self) -> datetime:
        """The same instant read in ``reference_timezone``.

        Pure and derived on every access: ``reference_instant`` is never
        mutated nor replaced, and the two remain the same point in time.
        """
        return self.reference_instant.astimezone(ZoneInfo(self.reference_timezone))
