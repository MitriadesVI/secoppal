"""SECOP Truth Layer T1B-0C — the local wall time boundary.

A raw local wall time supplied by a USER is not an instant. During a DST
spring-forward gap it denotes nothing; during a fall-back overlap it denotes
two instants. This module is the explicit boundary where that is decided —
deliberately separate from ``TemporalQueryContext``, which already receives an
absolute instant and must never be handed a wall time to guess at.

It refuses to pick fold 0 or fold 1 for anybody: an ambiguous wall time either
arrives with an explicit fold or comes back asking for one.

The ISO parsing and the naive-to-instant primitive are imported from
``temporal_value``: they are one implementation shared by the two callers, not
two copies that could drift.

Pure and deterministic: no clock, no network, no filesystem, no environment.

Binding documents, read in full before edit:
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_ARCHITECTURE.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_SEMANTIC_CONTRACT.md
    docs/planes/2026-07-28_TRUTH_LAYER_T1B_EXECUTION_ROADMAP.md
    docs/audits/2026-07-28_TRUTH_LAYER_T1B_DESIGN_RECONCILIATION_R1.md
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from typing import Literal

from app.core.truth_layer.temporal_value import (
    localize_naive,
    parse_iso_text,
    resolve_zone,
)

__all__ = [
    "LocalWallTimeStatus",
    "LocalWallTimeResolution",
    "LocalWallTimeResolver",
]


LocalWallTimeStatus = Literal[
    "resolved", "nonexistent", "ambiguous", "clarification_required"
]

_VALID_WALL_STATUS: frozenset[str] = frozenset(
    {"resolved", "nonexistent", "ambiguous", "clarification_required"}
)


def _derive(
    local_wall_time_raw: str, timezone_name: str, fold: int | None
) -> tuple[LocalWallTimeStatus, datetime | None, tuple[str, ...]]:
    zone = resolve_zone(timezone_name)
    if not isinstance(local_wall_time_raw, str):
        raise TypeError("local_wall_time_raw must be a string")
    if fold not in (None, 0, 1):
        raise ValueError("fold must be 0, 1 or None")

    text = local_wall_time_raw.strip()
    if not text:
        return "clarification_required", None, ("LOCAL_WALL_TIME_BLANK",)

    kind, parsed = parse_iso_text(text)
    if kind == "invalid_format":
        return "clarification_required", None, ("LOCAL_WALL_TIME_UNPARSABLE",)
    if kind == "aware":
        # An offset-bearing input is already an instant; reading it as a wall
        # time would silently discard the offset the user wrote.
        return (
            "clarification_required",
            None,
            ("LOCAL_WALL_TIME_CARRIES_EXPLICIT_OFFSET",),
        )
    naive = (
        datetime.combine(parsed, time.min)  # type: ignore[arg-type]
        if kind == "civil_date"
        else parsed
    )

    status, instant = localize_naive(naive, zone)  # type: ignore[arg-type]
    if status == "valid":
        return (
            "resolved",
            naive.replace(tzinfo=zone, fold=fold or 0),  # type: ignore[union-attr]
            ("LOCAL_WALL_TIME_UNAMBIGUOUS",),
        )
    if status == "nonexistent_local_time":
        return "nonexistent", None, ("LOCAL_WALL_TIME_NONEXISTENT",)
    if fold is None:
        return (
            "ambiguous",
            None,
            ("LOCAL_WALL_TIME_AMBIGUOUS", "LOCAL_WALL_TIME_FOLD_REQUIRED"),
        )
    return (
        "resolved",
        naive.replace(tzinfo=zone, fold=fold),  # type: ignore[union-attr]
        ("LOCAL_WALL_TIME_AMBIGUOUS", "LOCAL_WALL_TIME_FOLD_SUPPLIED"),
    )


@dataclass(frozen=True, slots=True)
class LocalWallTimeResolution:
    """The result of reading a user-supplied local wall time.

    ``instant`` exists only when the wall time denotes exactly one instant.
    A gap yields ``nonexistent`` and an overlap without an explicit fold
    yields ``ambiguous`` — never a quietly chosen fold.
    """

    local_wall_time_raw: str
    timezone: str
    status: LocalWallTimeStatus
    fold: Literal[0, 1] | None
    instant: datetime | None
    reason_codes: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.status not in _VALID_WALL_STATUS:
            raise ValueError(f"invalid LocalWallTimeStatus: {self.status!r}")
        status, instant, reasons = _derive(
            self.local_wall_time_raw, self.timezone, self.fold
        )
        if self.status != status:
            raise ValueError(
                f"status={self.status!r} contradicts the wall time input, which "
                f"yields {status!r}"
            )
        if self.instant != instant:
            raise ValueError("instant must be derived from the wall time input")
        if self.reason_codes != reasons:
            raise ValueError("reason_codes must be derived from the wall time input")


class LocalWallTimeResolver:
    """Pure resolver for raw local wall times (binding contract section 7).

    Stateless by construction: it holds no clock, no default zone and no
    remembered fold, so two calls with the same arguments always agree.
    """

    def resolve(
        self,
        local_wall_time_raw: str,
        timezone: str,
        *,
        fold: Literal[0, 1] | None = None,
    ) -> LocalWallTimeResolution:
        status, instant, reasons = _derive(local_wall_time_raw, timezone, fold)
        return LocalWallTimeResolution(
            local_wall_time_raw=local_wall_time_raw,
            timezone=timezone,
            status=status,
            fold=fold,
            instant=instant,
            reason_codes=reasons,
        )
