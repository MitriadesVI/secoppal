"""Transitively freeze public plan inputs without changing their values."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class FrozenDict(dict):
    """A ``dict``-compatible mapping that rejects every mutation API."""

    @staticmethod
    def _immutable(*args, **kwargs):
        del args, kwargs
        raise TypeError("frozen plan mappings cannot be mutated")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


def deep_freeze(value: Any) -> Any:
    """Return an immutable snapshot of JSON-like plan data.

    Plans are authorization records.  Keeping a read-only outer mapping is not
    enough when one of its values still points at a caller-owned list or dict.
    """
    if isinstance(value, Mapping):
        return FrozenDict({
            deep_freeze(key): deep_freeze(item) for key, item in value.items()
        })
    if isinstance(value, (list, tuple)):
        return tuple(deep_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(deep_freeze(item) for item in value)
    return value
