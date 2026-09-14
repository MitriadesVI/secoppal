"""One opaque coverage receipt per ``run_query`` turn, consumed by its trace sink.

The receipt identifies the turn by object identity and an opaque ``turn_id``,
never by query text, route or email: Profile and actor selection legitimately
rewrite the text they trace. Only a confirmed SQLite commit credits the turn.
"""
from asyncio import current_task
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import logging
from threading import current_thread
import uuid

from .admission import eligible, mode
from .coverage_classification import ACTOR_CANDIDATE, BYPASS_CLASSES, PROFILE_ACTION, classify_turn

logger = logging.getLogger(__name__)


def _task():
    try:
        return current_task()
    except RuntimeError:  # Synchronous Streamlit execution has no event loop.
        return None


@dataclass(eq=False)
class TurnReceipt:
    store: object
    structured: frozenset
    turn_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    thread: object = field(default_factory=current_thread)
    task: object = field(default_factory=_task)
    active: bool = True
    seam_reached: bool = False
    trace_attempted: bool = False
    sqlite_persisted: bool = False

    def owned_here(self, store):
        return (self.active and self.store is store
                and self.thread is current_thread() and self.task is _task())


@dataclass(frozen=True)
class TraceClaim:
    receipt: TurnReceipt
    metadata: dict
    primary: bool


_CURRENT: ContextVar[TurnReceipt | None] = ContextVar("natural_alpha_turn_receipt", default=None)


@contextmanager
def turn_scope(settings, store, ctx):
    receipt = None
    if mode(settings) == "alpha" and eligible(settings, ctx.channel, ctx.user_identity):
        receipt = TurnReceipt(store, frozenset(
            name for name in (PROFILE_ACTION, ACTOR_CANDIDATE) if getattr(ctx, name) is not None
        ))
    # Even an ineligible nested turn must mask an eligible outer one.
    token = _CURRENT.set(receipt)
    try:
        yield receipt
    finally:
        if receipt is not None:
            receipt.active = False  # Also revoke any inherited/copied context.
        _CURRENT.reset(token)


def _owned(store):
    receipt = _CURRENT.get()
    return receipt if receipt is not None and receipt.owned_here(store) else None


def note_seam_reached(store):
    receipt = _owned(store)
    if receipt is not None:
        receipt.seam_reached = True


def claim_trace(store, result: dict) -> TraceClaim | None:
    """Classify the turn at its first trace attempt; later traces are secondary."""
    receipt = _owned(store)
    if receipt is None:
        return None
    metadata = {"natural_alpha_coverage_turn_id": receipt.turn_id}
    if receipt.trace_attempted:
        return TraceClaim(receipt, metadata, primary=False)
    receipt.trace_attempted = True
    classification = classify_turn(seam_reached=receipt.seam_reached,
                                   structured=receipt.structured, result=result)
    metadata["natural_alpha_coverage_class"] = classification
    if classification in BYPASS_CLASSES:
        metadata["natural_alpha_excluded_by_route"] = result["route_reason"]
    return TraceClaim(receipt, metadata, primary=True)


def confirm_sqlite(claim: TraceClaim | None, store) -> None:
    # Called only after the SQLite transaction exits cleanly. JSONL is no ack.
    if claim is not None and claim.primary and claim.receipt.owned_here(store):
        claim.receipt.sqlite_persisted = True


def abort_turn(receipt: TurnReceipt | None, store) -> None:
    """A turn that raised has no result to classify: signal it, invent nothing."""
    if receipt is not None and receipt.owned_here(store) and not receipt.sqlite_persisted:
        logger.error("natural_alpha_coverage_turn_aborted turn_id=%s stop_dogfood=true", receipt.turn_id)


def finalize_turn(receipt: TurnReceipt | None, store, ctx, result: dict) -> None:
    """Write at most one trace-only record; never retry or disguise a failure."""
    if receipt is None or not receipt.owned_here(store):
        return
    if not receipt.trace_attempted:
        if result.get("trace_id"):
            logger.error("natural_alpha_coverage_unaccredited_trace turn_id=%s", receipt.turn_id)
            return
        try:
            store.log_trace(ctx.user_query, ctx.channel, result, user_identity=ctx.user_identity)
        except Exception as exc:
            # Observability must not change a completed turn.
            logger.error("natural_alpha_coverage_persistence_failed turn_id=%s error_type=%s stop_dogfood=true",
                         receipt.turn_id, type(exc).__name__)
            return
    if not receipt.sqlite_persisted:
        logger.error("natural_alpha_coverage_persistence_failed turn_id=%s stop_dogfood=true", receipt.turn_id)
