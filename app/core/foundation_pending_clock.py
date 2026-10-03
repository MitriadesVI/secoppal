"""M2A-4: the plazo of a pending (contract §8.2, U26) and its expiry (§17.2 e), over an INJECTED instant.

A pending lives 30 minutes from its CREATION (not from its last re-ask: a ``REASK`` keeps the plazo, §8.3). The instant
is always an argument: nothing here reads the system clock, so a replay with the stored instants is deterministic
(``reference_instant`` of the record, §3.1).

* ``expires_at(created)`` is the plazo a new pending takes: ``created + PENDING_TTL``.
* ``pending_expired(pending, now)`` is true from the deadline ON (``now >= deadline``): the same boundary the oracle of
  the anchor uses (a gap of 30 minutes is expired, ``tests/test_m2a3_oracle_link.py``). The deadline is the EARLIER of the
  persisted ``expires_at_utc`` and ``created + PENDING_TTL``: a record that says more than the contract's 30 minutes
  never outlives them, and one that says less is honoured (fail closed).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.core.foundation_accepted import PendingClarification
from app.core.interpretation_contract import ContractError

PENDING_TTL = timedelta(minutes=30)


def _instant(value: object, label: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is not timezone.utc:
        raise ContractError(f"INVALID_{label}")
    return value


def expires_at(created_at_utc: datetime) -> datetime:
    """The plazo of a pending created at ``created_at_utc`` (§8.2: 30 minutes from its creation)."""
    return _instant(created_at_utc, "PENDING_CREATED_AT") + PENDING_TTL


def deadline(pending: PendingClarification) -> datetime:
    if type(pending) is not PendingClarification:
        raise ContractError("INVALID_PENDING")
    return min(pending.expires_at_utc, expires_at(pending.created_at_utc))


def pending_expired(pending: PendingClarification, now: datetime) -> bool:
    """§17.2 e: the pending is no longer vigente at ``now`` (injected). The deadline itself is already expired."""
    return _instant(now, "NOW") >= deadline(pending)
