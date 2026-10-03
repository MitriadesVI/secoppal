"""M2A-4: ``CANCEL_PENDING`` (contract §17, INV-M2A-23, user decision U27 for the expired-confirmation anchor).

``CANCEL_PENDING`` is the ONE representation of a pending that stops existing without being answered (§17.1). It lives in
``AcceptedTurnRecord.control`` and is explicit: an empty list of operations never means «cancel» (that is
``PENDING_UNANSWERED``, §8.3 row 8; nothing here reads an empty patch).

The five causes of §17.2, each a closed value:

* ``DIRECTIVE`` (a): P carries the ``CANCEL_PENDING`` directive (a cited rejection, F23);
* ``NEW_SEARCH`` (b): P is a new search with a pending vigente (A7), written by the host even without a directive;
* ``RESET`` (c): «reset» before a ``CONTEXT_EXPIRED_CONFIRM`` (the button of §10.1);
* ``EXHAUSTED`` (d): ``asks`` reached the cap and the answer failed again (§8.3);
* ``EXPIRED`` (e): the plazo ran out; it is written at the NEXT turn (§17.2).

Base after a cancellation with a fragment (§17.3): the pending's ``frame``, never a turn older than the one that asked
(A7). Exception: a cancelled ``CONTEXT_EXPIRED_CONFIRM`` has no base (its ``frame`` is the expired anchor) and a pending
without ``frame`` has none either: ``fragment_base`` says so by returning ``None``.

Anchor (M2A4-D3, user decision U27 read literally): EVERY cause (a–e) that cancels a ``CONTEXT_EXPIRED_CONFIRM`` removes
the expired anchor. This module states it in the result (``eliminates_expired_anchor``); writing
``anchor_trace_id = None`` in the record is M2A-6's. Any other cancellation leaves the anchor as it was (§17.4).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from app.core.foundation_accepted import AcceptedInterpretation, PendingClarification, PendingControl
from app.core.interpretation_contract import ContractError

EXPIRED_CONFIRM = "CONTEXT_EXPIRED_CONFIRM"  # the A.3 code of the pending of §6 H1 (§10.1)
PENDING_CANCELLED = "PENDING_CANCELLED"  # annex A.4: an effect, with no echo of its own (§11)


class CancelCause(str, Enum):
    DIRECTIVE = "DIRECTIVE"
    NEW_SEARCH = "NEW_SEARCH"
    RESET = "RESET"
    EXHAUSTED = "EXHAUSTED"
    EXPIRED = "EXPIRED"


def holds_expired_anchor(pending: PendingClarification) -> bool:
    """Whether this pending is the confirmation of an expired anchor (§10.1): its ``frame`` IS that anchor."""
    return pending.reason == EXPIRED_CONFIRM


@dataclass(frozen=True, slots=True)
class Cancellation:
    """A pending that stops existing, with the cause that is written."""

    cause: CancelCause
    pending_id: str
    asked_trace_id: str
    eliminates_expired_anchor: bool

    @property
    def control(self) -> PendingControl:
        return PendingControl.CANCEL_PENDING

    @property
    def effects(self) -> frozenset[str]:
        return frozenset({PENDING_CANCELLED})


def cancel_pending(pending: PendingClarification, cause: CancelCause) -> Cancellation:
    """The cancellation of ``pending`` by ``cause`` (§17.2). ``RESET`` exists only before a ``CONTEXT_EXPIRED_CONFIRM``
    (§10.1); there is no cancel button for another pending in the contract."""
    if type(pending) is not PendingClarification or type(cause) is not CancelCause:
        raise ContractError("INVALID_CANCELLATION_INPUT")
    if cause is CancelCause.RESET and not holds_expired_anchor(pending):
        raise ContractError("RESET_OUTSIDE_EXPIRED_CONFIRM")
    return Cancellation(cause, pending.pending_id, pending.asked_trace_id, holds_expired_anchor(pending))


def fragment_base(pending: PendingClarification) -> AcceptedInterpretation | None:
    """§17.3: what the rest of a turn that cancels this pending composes on: its ``frame``; ``None`` (no base: the rest is
    a new search or ``NO_ANCHOR``) for a ``CONTEXT_EXPIRED_CONFIRM`` and for a pending without ``frame``."""
    if type(pending) is not PendingClarification:
        raise ContractError("INVALID_PENDING")
    if holds_expired_anchor(pending) or pending.frame is None:
        return None
    return pending.frame
