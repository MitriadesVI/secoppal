"""Source-boundary hooks for a host-admitted Foundation SEARCH plan (a leaf).

``secop_client`` reaches these hooks lazily; legacy must reach NOTHING of the Foundation vocabulary through them.
The guard object itself (``ExecutionGuard`` in ``foundation_execution_guard``) is only held here as an opaque
value: this module imports no Foundation module and no IR type (ADR-103 enm. 4, D1(e)/D13).
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar

from app.core.source_call_receipts import current_source_scope


_GUARD: ContextVar[object | None] = ContextVar("foundation_execution_guard", default=None)
# A Foundation turn that queries outside a grant (the basic fallback) still publishes evidence.
_RAW_ROWS: ContextVar[bool] = ContextVar("foundation_raw_source_rows", default=False)


@contextmanager
def execution_guard(guard: object):
    token = _GUARD.set(guard)
    try:
        yield guard
    finally:
        _GUARD.reset(token)


def verify_effective_source_call(operation: str, dataset: str, soql: str) -> None:
    guard = _GUARD.get()
    if guard is not None:
        guard.check(operation, dataset, soql)


def foundation_source_active() -> bool:
    """Identify the admitted source execution that publishes raw source fields."""
    return _GUARD.get() is not None


@contextmanager
def raw_source_rows():
    """Rows returned inside this block are the source's own, not the legacy normalization."""
    token = _RAW_ROWS.set(True)
    try:
        yield
    finally:
        _RAW_ROWS.reset(token)


def source_rows_raw() -> bool:
    """Whether a query must return the transport rows untouched (grant or fallback evidence)."""
    return _GUARD.get() is not None or _RAW_ROWS.get()


def validate_foundation_count_total(value: int) -> None:
    """A source total cannot contradict the page already received for its request."""
    guard = _GUARD.get()
    if guard is not None and value < guard.states[current_source_scope().request_id].page_size:
        raise ValueError("FOUNDATION_COUNT_BELOW_PAGE_SIZE")
