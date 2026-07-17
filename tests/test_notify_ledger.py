"""Tests de app/core/notify/ledger_store.py — intentos de entrega,
write-ahead antes/después del envío (P1B-2)."""
from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app.core.db import connect_sqlite
from app.core.notify import claim
from app.core.notify.ledger_store import (
    PHASE_AFTER_ATTEMPT_UPDATE,
    PHASE_AFTER_BATCH_UPDATE,
    PHASE_AFTER_BEGIN,
    PHASE_BEFORE_COMMIT,
    AttemptNotDispatchingError,
    ClaimLostError,
    LedgerError,
    LedgerStore,
)
from app.core.notify.outbox_store import BatchEventRef, OutboxStore

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc)
DURATION = timedelta(seconds=60)


@pytest.fixture
def claimed(tmp_path):
    """Batch recién creado y reclamado. Devuelve (db_path, notification_id, token)."""
    db_path = tmp_path / "ledger_test.db"
    ostore = OutboxStore(db_path)
    res = ostore.create_batch(
        principal_id="p1", endpoint_id="e1", radar_id="r1",
        payload_json={}, events=[BatchEventRef("ev1", 1)],
        created_at="2026-07-15T00:00:00+00:00",
        expires_at="2099-01-01T00:00:00+00:00",
    )
    conn = connect_sqlite(db_path)
    token = claim.try_claim(conn, res.notification_id, NOW, DURATION)
    conn.close()
    assert token is not None
    return db_path, res.notification_id, token


@pytest.fixture
def ledger(claimed):
    db_path, _, _ = claimed
    return LedgerStore(db_path)


def _batch_row(db_path, notification_id):
    conn = connect_sqlite(db_path)
    try:
        return conn.execute(
            "SELECT * FROM radar_notification_batches WHERE notification_id = ?",
            (notification_id,),
        ).fetchone()
    finally:
        conn.close()


def test_begin_attempt_creates_dispatching_with_null_outcome(claimed, ledger):
    """33 · begin_attempt crea dispatching con outcome NULL."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)
    a = ledger.get_attempt(attempt_id)
    assert a.phase == "dispatching"
    assert a.outcome is None
    assert a.attempt_number == 1


def test_begin_attempt_increments_attempt_number_without_duplicates(claimed, ledger):
    """34 · begin_attempt incrementa attempt_number sin duplicados."""
    db_path, nid, token = claimed
    a1 = ledger.begin_attempt(nid, token, now=NOW)
    ledger.finish_attempt(
        a1, nid, token, outcome="retry",
        next_not_before="2026-01-01T00:00:00+00:00",  # ya pasado: reclamable
        now=NOW,
    )
    # re-reclamar para el segundo intento
    conn = connect_sqlite(db_path)
    later = NOW + timedelta(seconds=1)
    token2 = claim.try_claim(conn, nid, later, DURATION)
    conn.close()
    a2 = ledger.begin_attempt(nid, token2, now=later)
    attempts = ledger.list_attempts(nid)
    assert [a.attempt_number for a in attempts] == [1, 2]
    assert a1 != a2


def test_begin_attempt_with_stale_token_fails_without_inserting(claimed, ledger):
    """35 · begin_attempt con token viejo falla sin insertar."""
    db_path, nid, token = claimed
    with pytest.raises(ClaimLostError):
        ledger.begin_attempt(nid, "token-viejo-invalido", now=NOW)
    assert ledger.list_attempts(nid) == []


def test_begin_attempt_rejects_expired_claim_without_writing(claimed, ledger):
    """begin_attempt rechaza un claim vencido aunque el token siga siendo el
    correcto y nadie lo haya reclamado todavía. "Vigente" exige token exacto
    **y** no vencido — igual que ``claim.is_owned`` —, no solo "mismo token";
    de lo contrario un worker que se atrasó más allá de su propio
    ``claim_until`` seguiría escribiendo aunque la política ya lo trate como
    reemplazable."""
    db_path, nid, token = claimed
    expired_now = NOW + DURATION + timedelta(seconds=1)  # claim_until ya pasado
    with pytest.raises(ClaimLostError):
        ledger.begin_attempt(nid, token, now=expired_now)
    assert ledger.list_attempts(nid) == []
    row = _batch_row(db_path, nid)
    assert row["attempt_count"] == 0


def test_dispatching_with_non_null_outcome_fails_check(claimed):
    """36 · dispatching con outcome no NULL falla por CHECK."""
    db_path, nid, token = claimed
    conn = connect_sqlite(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO radar_delivery_attempts "
                "(attempt_id, notification_id, attempt_number, phase, "
                " started_at, outcome) VALUES (?, ?, 1, 'dispatching', ?, 'sent')",
                ("bad-1", nid, "x"),
            )
    finally:
        conn.close()


def test_finished_with_null_outcome_fails_check(claimed):
    """37 · finished con outcome NULL falla por CHECK."""
    db_path, nid, token = claimed
    conn = connect_sqlite(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO radar_delivery_attempts "
                "(attempt_id, notification_id, attempt_number, phase, "
                " started_at, outcome) VALUES (?, ?, 1, 'finished', ?, NULL)",
                ("bad-2", nid, "x"),
            )
    finally:
        conn.close()


def test_finish_sent_leaves_attempt_finished_and_batch_sent(claimed, ledger):
    """38 · finish sent deja attempt finished y batch sent."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)
    ledger.finish_attempt(
        attempt_id, nid, token, outcome="sent", provider_message_id="mid-1",
        now=NOW,
    )
    a = ledger.get_attempt(attempt_id)
    assert a.phase == "finished"
    assert a.outcome == "sent"
    row = _batch_row(db_path, nid)
    assert row["status"] == "sent"
    assert row["claim_token"] is None
    assert row["claim_until"] is None


def test_sent_without_attempt_fails(claimed, ledger):
    """39 · intentar sent sin attempt falla."""
    db_path, nid, token = claimed
    with pytest.raises(AttemptNotDispatchingError):
        ledger.finish_attempt("no-existe", nid, token, outcome="sent", now=NOW)
    row = _batch_row(db_path, nid)
    assert row["status"] == "claimed"  # nada cambió


def test_ambiguous_leaves_batch_ambiguous_not_pending(claimed, ledger):
    """40 · ambiguous deja batch ambiguous, no pending."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)
    ledger.finish_attempt(attempt_id, nid, token, outcome="ambiguous", now=NOW)
    row = _batch_row(db_path, nid)
    assert row["status"] == "ambiguous"
    assert row["claim_token"] is None


def test_retry_leaves_pending_and_applies_caller_next_not_before(claimed, ledger):
    """41 · retry deja pending y aplica next_not_before aportado."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)
    ledger.finish_attempt(
        attempt_id, nid, token, outcome="retry",
        next_not_before="2026-07-15T13:00:00+00:00",
        now=NOW,
    )
    row = _batch_row(db_path, nid)
    assert row["status"] == "pending"
    assert row["claim_token"] is None
    assert row["not_before"] == "2026-07-15T13:00:00+00:00"


def test_dead_requires_dead_reason(claimed, ledger):
    """42 · dead exige dead_reason."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)
    with pytest.raises(LedgerError):
        ledger.finish_attempt(attempt_id, nid, token, outcome="dead")
    with pytest.raises(LedgerError):
        ledger.finish_attempt(attempt_id, nid, token, outcome="dead", dead_reason="no_es_valido")
    # con motivo válido sí funciona
    ledger.finish_attempt(
        attempt_id, nid, token, outcome="dead", dead_reason="expired", now=NOW,
    )
    row = _batch_row(db_path, nid)
    assert row["status"] == "dead"
    assert row["dead_reason"] == "expired"


def test_unique_notification_attempt_number(claimed):
    """43 · UNIQUE(notification_id, attempt_number)."""
    db_path, nid, token = claimed
    conn = connect_sqlite(db_path)
    try:
        conn.execute(
            "INSERT INTO radar_delivery_attempts "
            "(attempt_id, notification_id, attempt_number, phase, started_at) "
            "VALUES (?, ?, 1, 'dispatching', ?)",
            ("dup-a", nid, "x"),
        )
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO radar_delivery_attempts "
                "(attempt_id, notification_id, attempt_number, phase, started_at) "
                "VALUES (?, ?, 1, 'dispatching', ?)",
                ("dup-b", nid, "y"),
            )
    finally:
        conn.close()


def test_delete_batch_with_attempt_fails_by_restrict(claimed, ledger):
    """44 · borrar batch con attempt falla por RESTRICT."""
    db_path, nid, token = claimed
    ledger.begin_attempt(nid, token, now=NOW)
    conn = connect_sqlite(db_path)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "DELETE FROM radar_notification_batches WHERE notification_id = ?",
                (nid,),
            )
    finally:
        conn.close()


def test_stale_token_at_finish_changes_neither_attempt_nor_batch(claimed, ledger):
    """45 · token viejo al terminar no cambia attempt ni batch."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)
    before = ledger.get_attempt(attempt_id)
    before_batch = _batch_row(db_path, nid)
    with pytest.raises(ClaimLostError):
        ledger.finish_attempt(attempt_id, nid, "token-viejo", outcome="sent", now=NOW)
    after = ledger.get_attempt(attempt_id)
    after_batch = _batch_row(db_path, nid)
    assert after == before
    assert dict(after_batch) == dict(before_batch)


def test_finish_attempt_rejects_claim_expired_after_dispatching(claimed, ledger):
    """finish_attempt rechaza cerrar un intento si el claim venció mientras
    estaba en dispatching: el intento queda abierto y el batch intacto — nada
    se cierra con un claim que la política ya no reconoce como vigente."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)  # claim vigente al abrir
    before_attempt = ledger.get_attempt(attempt_id)
    before_batch = _batch_row(db_path, nid)

    expired_now = NOW + DURATION + timedelta(seconds=1)  # venció mientras dispatching
    with pytest.raises(ClaimLostError):
        ledger.finish_attempt(attempt_id, nid, token, outcome="sent", now=expired_now)

    after_attempt = ledger.get_attempt(attempt_id)
    after_batch = _batch_row(db_path, nid)
    assert after_attempt == before_attempt
    assert dict(after_batch) == dict(before_batch)


@pytest.mark.parametrize("phase", [
    PHASE_AFTER_BEGIN, PHASE_AFTER_ATTEMPT_UPDATE, PHASE_AFTER_BATCH_UPDATE,
    PHASE_BEFORE_COMMIT,
])
def test_fault_between_attempt_and_batch_update_rolls_back_everything(
    claimed, ledger, phase,
):
    """46 · fallo entre actualizar attempt y batch hace rollback completo."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)
    before_attempt = ledger.get_attempt(attempt_id)
    before_batch = _batch_row(db_path, nid)

    class _Boom(RuntimeError):
        pass

    def hook(current):
        if current == phase:
            raise _Boom(phase)

    with pytest.raises(_Boom):
        ledger.finish_attempt(
            attempt_id, nid, token, outcome="sent", _fault_hook=hook, now=NOW,
        )

    after_attempt = ledger.get_attempt(attempt_id)
    after_batch = _batch_row(db_path, nid)
    assert after_attempt == before_attempt
    assert dict(after_batch) == dict(before_batch)


# ══ ADR-053 · Vocabulario ampliado del ledger ════════════════════════════════════

@pytest.mark.parametrize("dead_reason", [
    "render_error", "retries_exhausted", "permanent_provider_error",
])
def test_53_the_new_dead_reasons_close_a_batch(claimed, ledger, dead_reason):
    """Los tres registran una causa que antes no se podía expresar. Sin ellos, un
    payload corrupto o unos reintentos agotados solo podían cerrarse mintiendo:
    'expired' (el TTL no venció) o 'invalid_address' (la dirección está bien)."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)

    ledger.finish_attempt(
        attempt_id, nid, token, outcome="dead", dead_reason=dead_reason, now=NOW,
    )

    row = _batch_row(db_path, nid)
    assert row["status"] == "dead"
    assert row["dead_reason"] == dead_reason


def test_53_render_error_is_a_valid_error_class(claimed, ledger):
    """``render_error`` y ``provider_unavailable`` son clases de error del sistema, no
    del proveedor: en un caso nunca se llegó a construir el mensaje, en el otro no
    había a quién llamar."""
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)

    ledger.finish_attempt(
        attempt_id, nid, token, outcome="dead", dead_reason="render_error",
        error_class="render_error", error_message_safe="payload_render_failed",
        now=NOW,
    )

    assert ledger.get_attempt(attempt_id).error_class == "render_error"


def test_53_provider_unavailable_is_a_valid_error_class_on_retry(claimed, ledger):
    db_path, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)

    ledger.finish_attempt(
        attempt_id, nid, token, outcome="retry", error_class="provider_unavailable",
        next_not_before="2026-07-15T12:30:00+00:00", now=NOW,
    )

    attempt = ledger.get_attempt(attempt_id)
    assert (attempt.outcome, attempt.error_class) == ("retry", "provider_unavailable")
    assert _batch_row(db_path, nid)["status"] == "pending"


def test_53_invented_dead_reason_is_still_rejected(claimed, ledger):
    """Ensanchar el vocabulario no es abrirlo."""
    _, nid, token = claimed
    attempt_id = ledger.begin_attempt(nid, token, now=NOW)

    with pytest.raises(LedgerError, match="dead_reason inválido"):
        ledger.finish_attempt(
            attempt_id, nid, token, outcome="dead", dead_reason="inventado", now=NOW,
        )
