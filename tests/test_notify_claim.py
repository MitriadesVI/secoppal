"""Tests de app/core/notify/claim.py — exclusión mutua efímera sobre un
batch de notificación (P1B-2)."""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from app.core.db import connect_sqlite
from app.core.notify import claim
from app.core.notify.outbox_store import BatchEventRef, OutboxStore

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
DURATION = timedelta(seconds=60)


@pytest.fixture
def db_path(tmp_path):
    path = tmp_path / "claim_test.db"
    OutboxStore(path)  # asegura el schema
    return path


def _seed_batch(db_path, **row_overrides) -> str:
    store = OutboxStore(db_path)
    res = store.create_batch(
        principal_id="p1", endpoint_id="e1", radar_id="r1",
        payload_json={}, events=[BatchEventRef("ev1", 1)],
        created_at="2026-07-15T00:00:00+00:00",
        expires_at=row_overrides.pop("expires_at", "2099-01-01T00:00:00+00:00"),
        not_before=row_overrides.pop("not_before", None),
    )
    nid = res.notification_id
    if row_overrides:
        conn = connect_sqlite(db_path)
        try:
            assignments = ", ".join(f"{k} = ?" for k in row_overrides)
            conn.execute(
                f"UPDATE radar_notification_batches SET {assignments} "
                f"WHERE notification_id = ?",
                (*row_overrides.values(), nid),
            )
        finally:
            conn.close()
    return nid


def test_claim_of_pending_wins(db_path):
    """22 · claim de pending gana."""
    nid = _seed_batch(db_path)
    conn = connect_sqlite(db_path)
    token = claim.try_claim(conn, nid, NOW, DURATION)
    conn.close()
    assert token is not None


def test_second_claim_before_expiry_loses(db_path):
    """23 · segundo claim antes de vencer pierde."""
    nid = _seed_batch(db_path)
    conn = connect_sqlite(db_path)
    tok1 = claim.try_claim(conn, nid, NOW, DURATION)
    tok2 = claim.try_claim(conn, nid, NOW + timedelta(seconds=1), DURATION)
    conn.close()
    assert tok1 is not None
    assert tok2 is None


def test_two_concurrent_claims_exactly_one_wins(tmp_path):
    """24 · dos threads concurrentes → exactamente uno gana."""
    db_path = tmp_path / "race.db"
    nid = _seed_batch(db_path)

    barrier = threading.Barrier(2)
    results: dict = {}
    lock = threading.Lock()

    def attempt(label: str) -> None:
        conn = connect_sqlite(db_path)
        barrier.wait()
        try:
            token = claim.try_claim(conn, nid, NOW, DURATION)
        finally:
            conn.close()
        with lock:
            results[label] = token

    threads = [threading.Thread(target=attempt, args=(f"t{i}",)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads)

    winners = [v for v in results.values() if v is not None]
    assert len(winners) == 1


def test_fifty_iterations_concurrent_claim_exactly_one_winner(tmp_path):
    """Repetición de 24, 50 corridas."""
    for i in range(50):
        db_path = tmp_path / f"race_{i}.db"
        nid = _seed_batch(db_path)
        barrier = threading.Barrier(4)
        results: dict = {}
        lock = threading.Lock()

        def attempt(label: str) -> None:
            conn = connect_sqlite(db_path)
            barrier.wait()
            try:
                token = claim.try_claim(conn, nid, NOW, DURATION)
            finally:
                conn.close()
            with lock:
                results[label] = token

        threads = [threading.Thread(target=attempt, args=(f"t{j}",)) for j in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        assert not any(t.is_alive() for t in threads)

        winners = [v for v in results.values() if v is not None]
        assert len(winners) == 1, f"iteración {i}: {results}"


def test_expired_claim_is_recovered_with_different_token(db_path):
    """25 · claim vencido es recuperado con token distinto."""
    nid = _seed_batch(db_path)
    conn = connect_sqlite(db_path)
    tok1 = claim.try_claim(conn, nid, NOW, timedelta(seconds=10))
    later = NOW + timedelta(seconds=11)
    tok2 = claim.try_claim(conn, nid, later, DURATION)
    conn.close()
    assert tok1 is not None
    assert tok2 is not None
    assert tok2 != tok1


def test_stale_token_does_not_release_new_claim(db_path):
    """26 · token viejo no libera el claim nuevo."""
    nid = _seed_batch(db_path)
    conn = connect_sqlite(db_path)
    tok1 = claim.try_claim(conn, nid, NOW, timedelta(seconds=10))
    later = NOW + timedelta(seconds=11)
    tok2 = claim.try_claim(conn, nid, later, DURATION)
    released = claim.release(conn, nid, tok1)  # token viejo
    owned_by_new = claim.is_owned(conn, nid, tok2, later)
    conn.close()
    assert released is False
    assert owned_by_new is True


def test_stale_token_is_not_owner(db_path):
    """27 · token viejo no es owner."""
    nid = _seed_batch(db_path)
    conn = connect_sqlite(db_path)
    tok1 = claim.try_claim(conn, nid, NOW, timedelta(seconds=10))
    later = NOW + timedelta(seconds=11)
    claim.try_claim(conn, nid, later, DURATION)
    owned_by_old = claim.is_owned(conn, nid, tok1, later)
    conn.close()
    assert owned_by_old is False


def test_future_not_before_is_not_claimable(db_path):
    """28 · not_before futuro no reclamable."""
    future = (NOW + timedelta(days=1)).isoformat()
    nid = _seed_batch(db_path, not_before=future)
    conn = connect_sqlite(db_path)
    token = claim.try_claim(conn, nid, NOW, DURATION)
    conn.close()
    assert token is None


def test_expired_expires_at_is_not_claimable(db_path):
    """29 · expires_at vencido no reclamable."""
    past = (NOW - timedelta(days=1)).isoformat()
    nid = _seed_batch(db_path, expires_at=past)
    conn = connect_sqlite(db_path)
    token = claim.try_claim(conn, nid, NOW, DURATION)
    conn.close()
    assert token is None


@pytest.mark.parametrize("status", ["sent", "ambiguous", "dead"])
def test_terminal_statuses_are_not_claimable(db_path, status):
    """30 · sent/ambiguous/dead no reclamables."""
    nid = _seed_batch(db_path, status=status)
    conn = connect_sqlite(db_path)
    token = claim.try_claim(conn, nid, NOW, DURATION)
    conn.close()
    assert token is None


def test_currently_claimed_is_not_claimable(db_path):
    """31 · claimed vigente no reclamable."""
    nid = _seed_batch(db_path)
    conn = connect_sqlite(db_path)
    claim.try_claim(conn, nid, NOW, DURATION)
    second = claim.try_claim(conn, nid, NOW + timedelta(seconds=1), DURATION)
    conn.close()
    assert second is None


def test_valid_release_returns_to_pending_and_clears_token(db_path):
    """32 · release válido vuelve a pending y limpia token."""
    nid = _seed_batch(db_path)
    conn = connect_sqlite(db_path)
    token = claim.try_claim(conn, nid, NOW, DURATION)
    released = claim.release(conn, nid, token)
    row = conn.execute(
        "SELECT status, claim_token, claim_until FROM radar_notification_batches "
        "WHERE notification_id = ?",
        (nid,),
    ).fetchone()
    conn.close()
    assert released is True
    assert row["status"] == "pending"
    assert row["claim_token"] is None
    assert row["claim_until"] is None
