"""Transitively freeze public plan inputs without changing their values."""
from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any


class FrozenMapping(Mapping):
    """Read-only mapping backed by an owned copy, never by a ``dict`` subtype."""

    __slots__ = ("_values",)

    def __init__(self, values: Mapping) -> None:
        if hasattr(self, "_values"):
            raise TypeError("frozen plan mappings cannot be reinitialized")
        object.__setattr__(self, "_values", MappingProxyType(dict(values)))

    def __getitem__(self, key):
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __setattr__(self, name, value) -> None:
        del name, value
        raise TypeError("frozen plan mappings cannot be mutated")

    def __deepcopy__(self, memo):
        del memo
        return self

    def __repr__(self) -> str:
        return repr(self._values)


def deep_freeze(value: Any) -> Any:
    """Return an immutable snapshot of JSON-like plan data.

    Plans are authorization records.  Keeping a read-only outer mapping is not
    enough when one of its values still points at a caller-owned list or dict.
    """
    if isinstance(value, Mapping):
        return FrozenMapping({
            deep_freeze(key): deep_freeze(item) for key, item in value.items()
        })
    if isinstance(value, list):
        return tuple(deep_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(deep_freeze(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(deep_freeze(item) for item in value)
    return value


def mutable_copy(value: Any) -> Any:
    """Materialize fresh built-in containers at a mutable consumer boundary."""
    if isinstance(value, Mapping):
        return {
            mutable_copy(key): mutable_copy(item) for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [mutable_copy(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return {mutable_copy(item) for item in value}
    return value
