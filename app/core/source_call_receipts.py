"""Observational receipts for logical SECOP calls within one workflow turn."""

from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import Executor, Future
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import asdict, dataclass, replace
from threading import Lock
from typing import Any, Callable


UNKNOWN = "UNKNOWN"


@dataclass(frozen=True, slots=True)
class CallScope:
    reason: str = "legacy_unattributed"
    request_id: str = UNKNOWN
    scope_id: str = UNKNOWN
    parent_call_id: str | None = None
    interpretation_hash: str = UNKNOWN
    plan_hash: str = UNKNOWN


@dataclass(slots=True)
class CallReceipt:
    turn_hash: str
    call_id: str
    parent_call_id: str | None
    request_id: str
    scope_id: str
    operation: str
    dataset: str
    soql: str
    reason: str
    interpretation_hash: str
    plan_hash: str
    status: str = "pending"
    attempts: int = 0
    rows_received: int | None = None
    count_value: int | None = None
    error_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SourceCallCollector:
    def __init__(self, turn_hash: str) -> None:
        self.turn_hash = turn_hash
        self._lock = Lock()
        self._next_id = 0
        self._calls: list[CallReceipt] = []

    def new_call(self, operation: str, dataset: str, soql: str, scope: CallScope) -> CallReceipt:
        with self._lock:
            self._next_id += 1
            call_id = f"call-{self._next_id}"
        return CallReceipt(
            self.turn_hash, call_id, scope.parent_call_id, scope.request_id, scope.scope_id,
            operation, dataset, soql, scope.reason, scope.interpretation_hash, scope.plan_hash,
        )

    def finish(self, receipt: CallReceipt) -> None:
        with self._lock:
            self._calls.append(receipt)

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            calls = sorted(self._calls, key=lambda call: int(call.call_id.split("-")[1]))
            return [call.to_dict() for call in calls]


_CURRENT: ContextVar[SourceCallCollector | None] = ContextVar("foundation_source_calls", default=None)
_SCOPE: ContextVar[CallScope] = ContextVar("foundation_source_scope", default=CallScope())
_LAST_CALL_ID: ContextVar[str | None] = ContextVar("foundation_last_source_call", default=None)
_ACTIVE_CALL: ContextVar[CallReceipt | None] = ContextVar("foundation_active_source_call", default=None)


def active_source_call_metadata() -> dict[str, Any]:
    """Correlation metadata for an independent transport observer, never an authority."""
    receipt = _ACTIVE_CALL.get()
    if receipt is None:
        return {}
    return {key: getattr(receipt, key) for key in (
        "turn_hash", "call_id", "parent_call_id", "request_id", "scope_id",
        "reason", "interpretation_hash", "plan_hash", "attempts",
    )}


@dataclass(frozen=True, slots=True)
class SourceCallTokens:
    current: Token[SourceCallCollector | None]
    scope: Token[CallScope]
    last_call_id: Token[str | None]


def begin_source_calls(turn_hash: str) -> SourceCallTokens:
    return SourceCallTokens(
        _CURRENT.set(SourceCallCollector(turn_hash)),
        _SCOPE.set(CallScope()),
        _LAST_CALL_ID.set(None),
    )


def end_source_calls(tokens: SourceCallTokens) -> list[dict[str, Any]]:
    collector = _CURRENT.get()
    try:
        return collector.snapshot() if collector is not None else []
    finally:
        _LAST_CALL_ID.reset(tokens.last_call_id)
        _SCOPE.reset(tokens.scope)
        _CURRENT.reset(tokens.current)


def source_calls_snapshot() -> list[dict[str, Any]] | None:
    collector = _CURRENT.get()
    return collector.snapshot() if collector is not None else None


def last_call_id() -> str | None:
    return _LAST_CALL_ID.get() if _CURRENT.get() is not None else None


def current_source_scope() -> CallScope:
    return _SCOPE.get()


@contextmanager
def source_call_scope(**changes: Any) -> Iterator[None]:
    scope = replace(_SCOPE.get(), **changes)
    token = _SCOPE.set(scope)
    try:
        yield
    finally:
        _SCOPE.reset(token)


@contextmanager
def logical_source_call(operation: str, dataset: str, soql: str) -> Iterator[CallReceipt | None]:
    collector = _CURRENT.get()
    if collector is None:
        yield None
        return
    receipt = collector.new_call(operation, dataset, soql, _SCOPE.get())
    active_token = _ACTIVE_CALL.set(receipt)
    try:
        yield receipt
    except BaseException as exc:
        receipt.status = "error"
        receipt.error_type = receipt.error_type or type(exc).__name__
        raise
    else:
        receipt.status = "success"
    finally:
        _ACTIVE_CALL.reset(active_token)
        collector.finish(receipt)
        _LAST_CALL_ID.set(receipt.call_id)


def submit_with_source_context(executor: Executor, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Future:
    """Propagate only receipt state; unrelated turn contexts keep their old behavior."""
    collector = _CURRENT.get()
    if collector is None:
        return executor.submit(fn, *args, **kwargs)
    scope = _SCOPE.get()
    last = _LAST_CALL_ID.get()

    def run() -> Any:
        collector_token = _CURRENT.set(collector)
        scope_token = _SCOPE.set(scope)
        last_token = _LAST_CALL_ID.set(last)
        try:
            return fn(*args, **kwargs)
        finally:
            _LAST_CALL_ID.reset(last_token)
            _SCOPE.reset(scope_token)
            _CURRENT.reset(collector_token)

    return executor.submit(run)
