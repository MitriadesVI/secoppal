"""Effective analytic operation of a turn, derived from its accredited controls.

Pure function over :class:`~app.core.analytic_control.AnalyticControlSpan`.
It lives outside QueryRouter on purpose: the parser records evidence, and each
consumer (Contract Activity, the no-silent-downgrade guard) derives the same
operation from it without knowing who else reads it.

The derivation never collapses evidence it cannot reconcile: contradictory or
multiple breakdowns come back as a typed ``reason`` so the owner can clarify.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Iterable

from app.core.analytic_control import (
    AnalyticControlKind,
    AnalyticControlSpan,
    AnalyticDimension,
    ControlPolarity,
    is_count_imperative,
)


class AnalyticOperationKind(str, Enum):
    NONE = "NONE"
    COUNT = "COUNT"
    GROUP = "GROUP"


@dataclass(frozen=True, slots=True)
class AnalyticOperation:
    kind: AnalyticOperationKind = AnalyticOperationKind.NONE
    dimension: AnalyticDimension | None = None
    # Non-empty when the controls cannot be reconciled into one operation.
    reason: str = ""

    @property
    def requested(self) -> bool:
        """The turn asked for an analytic result (or asked in a way that needs one)."""
        return self.kind is not AnalyticOperationKind.NONE or bool(self.reason)


NO_OPERATION = AnalyticOperation()


def derive_operation(controls: Iterable[AnalyticControlSpan]) -> AnalyticOperation:
    items = tuple(controls)
    if not items:
        return NO_OPERATION
    positive_groups = {
        c.dimension for c in items
        if c.kind is AnalyticControlKind.GROUP
        and c.polarity is ControlPolarity.POSITIVE
        and c.dimension is not None
    }
    negative_groups = {
        c.dimension for c in items
        if c.kind is AnalyticControlKind.GROUP
        and c.polarity is ControlPolarity.NEGATIVE
        and c.dimension is not None
    }
    count = any(
        c.kind is AnalyticControlKind.COUNT and c.polarity is ControlPolarity.POSITIVE
        for c in items
    )
    count_command = any(
        is_count_imperative(c) and c.polarity is ControlPolarity.POSITIVE
        for c in items
    )
    group_command = any(
        c.kind is AnalyticControlKind.GROUP
        and c.polarity is ControlPolarity.POSITIVE
        and c.dimension is None
        for c in items
    )
    not_only_total = any(c.kind is AnalyticControlKind.TOTAL_ONLY for c in items)
    not_listed = any(c.kind is AnalyticControlKind.LIST for c in items)

    if positive_groups & negative_groups:
        return AnalyticOperation(reason="CONTRADICTORY_ANALYTIC_CONTROL")
    if len(positive_groups) > 1:
        return AnalyticOperation(reason="MULTIPLE_BREAKDOWNS")
    if positive_groups and count_command and group_command:
        # Two coordinated imperatives ("cuenta los contratos y agrúpalos por
        # modalidad") are two operations.  One imperative with a distributive
        # criterion ("cuenta por modalidad", "¿cuántos hay en cada modalidad?")
        # is a single grouped count.
        return AnalyticOperation(reason="MULTIPLE_OPERATIONS")
    if positive_groups:
        (dimension,) = positive_groups
        return AnalyticOperation(AnalyticOperationKind.GROUP, dimension)
    if not_only_total:
        return AnalyticOperation(reason="BREAKDOWN_DIMENSION_REQUIRED")
    if count:
        return AnalyticOperation(AnalyticOperationKind.COUNT)
    if not_listed:
        # "sin listar los contratos" and nothing else: the user excluded the
        # only thing a search can do without saying what to do instead.
        return AnalyticOperation(reason="ANALYTIC_OPERATION_REQUIRED")
    return NO_OPERATION


__all__ = [
    "AnalyticOperation",
    "AnalyticOperationKind",
    "NO_OPERATION",
    "derive_operation",
]
