"""One-shot coverage receipt, owned by one handler invocation and executor."""
from asyncio import current_task
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import current_thread

from .admission import eligible, mode
from .coverage_metadata import coverage_metadata


def _task():
    try:
        return current_task()
    except RuntimeError:  # Synchronous Streamlit execution has no event loop.
        return None


@dataclass
class _Receipt:
    store: object
    user_query: str
    channel: str
    identity: object
    classification: str
    thread: object = field(default_factory=current_thread)
    task: object = field(default_factory=_task)
    active: bool = True
    consumed: bool = False
    metadata: dict = field(default_factory=dict)
    persisted_trace_id: str | None = None

    def owned_here(self):
        return self.active and self.thread is current_thread() and self.task is _task()


_CURRENT: ContextVar[_Receipt | None] = ContextVar("natural_alpha_coverage", default=None)


@contextmanager
def coverage_scope(deps, ctx, *, excluded_route_class="FAST_PATH_BYPASS"):
    receipt = None
    if mode(deps.settings) == "alpha" and eligible(deps.settings, ctx.channel, ctx.user_identity):
        receipt = _Receipt(deps.feedback, ctx.user_query, ctx.channel, ctx.user_identity, excluded_route_class)
    # Even an ineligible nested invocation must mask an eligible outer one.
    token = _CURRENT.set(receipt)
    try:
        yield receipt
    finally:
        if receipt is not None:
            receipt.active = False  # Also revoke any inherited/copied context.
        _CURRENT.reset(token)


def current_receipt(store, user_query, channel, identity):
    receipt = _CURRENT.get()
    if (receipt is not None and receipt.owned_here() and receipt.store is store
            and receipt.user_query == user_query and receipt.channel == channel and receipt.identity is identity):
        return receipt
    return None


def consume_receipt(store, user_query, channel, identity, result):
    receipt = current_receipt(store, user_query, channel, identity)
    if receipt is None or receipt.consumed:
        return None
    receipt.consumed = True
    receipt.metadata = coverage_metadata({
        "natural_alpha_coverage_class": receipt.classification,
        "natural_alpha_excluded_by_route": result.get("route_reason"),
        "route_reason": result.get("route_reason"),
    })
    return receipt


def confirm_persistence(receipt, trace_id):
    # Called only after an existing sink returns successfully (SQLite commit or
    # closed JSONL append). The receipt never enters a response or stored JSON.
    if receipt is not None and receipt.owned_here() and receipt.metadata:
        receipt.persisted_trace_id = trace_id
