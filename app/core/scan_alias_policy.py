"""Declarative policy separating exact alias ownership from text scanning.

The gazetteer owns the evidence.  This module only normalizes and applies its
two optional policy fields; it deliberately contains no entity-name denylist
and no lexical heuristic.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from app.utils.money import normalize_text

SCAN_DISABLED_FIELD = "scan_disabled_aliases"
RESOLUTION_BLOCKED_FIELD = "resolution_blocked_aliases"


def normalize_alias_policy(values: Iterable[object] | None) -> frozenset[str]:
    """Return non-empty normalized aliases from one declarative policy list."""
    if values is None:
        return frozenset()
    if isinstance(values, (str, bytes)):
        raise TypeError("alias policy fields must be lists of strings")

    invalid = [value for value in values if not isinstance(value, str)]
    if invalid:
        raise TypeError("alias policy fields must contain only strings")

    return frozenset(
        normalized
        for value in values
        if (normalized := normalize_text(value))
    )


def record_alias_policy(record: Mapping[str, Any]) -> tuple[frozenset[str], frozenset[str]]:
    """Read ``(scan-disabled, resolution-blocked)`` policy from a record."""
    return (
        normalize_alias_policy(record.get(SCAN_DISABLED_FIELD)),
        normalize_alias_policy(record.get(RESOLUTION_BLOCKED_FIELD)),
    )


def is_scan_eligible_alias(alias: str, scan_disabled_aliases: frozenset[str]) -> bool:
    """Whether an owned exact alias may also match inside longer text."""
    return normalize_text(alias) not in scan_disabled_aliases
