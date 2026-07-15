"""Tests de app/core/radar/store.py — persistencia del Radar."""
from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import pytest

from app.core.radar.lease import (
    LEASE_ACQUIRED,
    LEASE_ALREADY_RUNNING,
    LEASE_DURATION,
    LEASE_RADAR_NOT_FOUND,
)
from app.core.radar.store import RadarExistsError, RadarStore


@pytest.fixture
def store(tmp_path):
    return RadarStore(tmp_path / "radar_test.db")


def _frame_json():
    return json.dumps({"dataset": "contratos", "scope": {"departamento_resolved": "ATLÁNTICO"},
                       "topic": ["adulto_mayor"]})


def test_create_and_get(store):
    rid = store.create_radar(
        name="Radar A", owner="u1", query_frame_json=_frame_json(),
        dataset="contratos", created_from_trace_id="tr-1",
    )
    radar = store.get_radar(rid)
    assert radar is not None
    assert radar.name == "Radar A"
    assert radar.owner == "u1"
    assert radar.is_enabled
    assert radar.created_from_trace_id == "tr-1"
    assert radar.frame_dict()["scope"]["departamento_resolved"] == "ATLÁNTICO"


def test_unique_owner_name(store):
    store.create_radar(name="Dup", owner="u1", query_frame_json=_frame_json())
    with pytest.raises(RadarExistsError):
        store.create_radar(name="Dup", owner="u1", query_frame_json=_frame_json())


def test_same_name_different_owner_ok(store):
    store.create_radar(name="Shared", owner="u1", query_frame_json=_frame_json())
    # mismo nombre, otro dueño → permitido
    rid = store.create_radar(name="Shared", owner="u2", query_frame_json=_frame_json())
    assert store.get_radar(rid) is not None


def test_get_by_name_case_insensitive(store):
    store.create_radar(name="Radar PAE", owner="u1", query_frame_json=_frame_json())
    assert store.get_radar_by_name("u1", "radar pae") is not None


def test_list_scoped_to_owner(store):
    store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.create_radar(name="B", owner="u1", query_frame_json=_frame_json())
    store.create_radar(name="C", owner="u2", query_frame_json=_frame_json())
    assert len(store.list_radars("u1")) == 2
    assert len(store.list_radars("u2")) == 1


def test_set_enabled(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.set_enabled(rid, False)
    assert not store.get_radar(rid).is_enabled
    store.set_enabled(rid, True)
    assert store.get_radar(rid).is_enabled


def test_record_run_and_get(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    run_id = store.record_run(
        radar_id=rid, started_at="2026-06-11T00:00:00+00:00", status="ok",
        total_count=12, new_count=3, changed_count=1,
        digest_json=json.dumps({"text": "hola"}), soql="SELECT 1",
        params_json="{}", dataset_id="jbjy-vk9h", approx_total=None,
        execution_ms=42, outcome="radar_ok", reason="ok",
    )
    runs = store.get_runs(rid)
    assert len(runs) == 1
    assert runs[0].id == run_id
    assert runs[0].new_count == 3
    assert runs[0].soql == "SELECT 1"
    assert runs[0].execution_ms == 42
    assert runs[0].outcome == "radar_ok"
    assert runs[0].reason == "ok"
    assert runs[0].digest()["text"] == "hola"


def test_record_error(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_error(rid, "boom")
    radar = store.get_radar(rid)
    assert radar.last_error_message == "boom"
    assert radar.last_error_at


def test_update_last_run_clears_error(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_failure(
        rid, reason="socrata_timeout", message="boom",
        next_run_after="2026-06-12T00:00:00+00:00",
    )
    store.update_last_run(rid)
    radar = store.get_radar(rid)
    assert radar.last_run_at
    assert radar.last_error_message is None
    assert radar.failure_count == 0
    assert radar.last_failure_reason is None
    assert radar.next_run_after is None


def test_record_failure_and_backoff_filter(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    count = store.record_failure(
        rid, reason="socrata_timeout", message="timeout",
        next_run_after="2999-01-01T00:00:00+00:00",
    )
    radar = store.get_radar(rid)
    assert count == 1
    assert radar.failure_count == 1
    assert radar.permanent_failure_streak == 0
    assert radar.last_failure_reason == "socrata_timeout"
    assert radar.next_run_after == "2999-01-01T00:00:00+00:00"
    assert store.list_all_enabled_radars() == []


# ── P0D.1 · racha de fallos permanentes vs. failure_count general ─────────
#
# Bug: la pausa automática dependía de failure_count (cualquier fallo), no de
# una racha de fallos *permanentes* consecutivos. Estos tests fijan el
# contrato correcto directamente contra RadarStore.record_failure.

def test_transient_transient_permanent_does_not_pause(store):
    """Caso central del bug: dos transitorios + un permanente NO deben pausar.

    failure_count llega a 3 (cualquier fallo cuenta), pero solo hubo UN
    fallo permanente, así que permanent_failure_streak == 1 y el radar sigue
    habilitado.
    """
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_failure(rid, reason="socrata_timeout", message="t1", permanent=False)
    store.record_failure(rid, reason="socrata_query_error", message="t2", permanent=False)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p1", permanent=True)

    radar = store.get_radar(rid)
    assert radar.failure_count == 3
    assert radar.permanent_failure_streak == 1
    assert radar.needs_attention == 0
    assert radar.is_enabled


def test_three_consecutive_permanent_failures_pause(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())

    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p1", permanent=True)
    r1 = store.get_radar(rid)
    assert (r1.failure_count, r1.permanent_failure_streak, r1.is_enabled) == (1, 1, True)

    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p2", permanent=True)
    r2 = store.get_radar(rid)
    assert (r2.failure_count, r2.permanent_failure_streak, r2.is_enabled) == (2, 2, True)

    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p3", permanent=True)
    r3 = store.get_radar(rid)
    assert r3.failure_count == 3
    assert r3.permanent_failure_streak == 3
    assert r3.needs_attention == 1
    assert not r3.is_enabled


def test_permanent_streak_reset_by_transient_between_permanents(store):
    """permanente, transitorio, permanente, permanente → racha == 2, sin pausa."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p1", permanent=True)
    store.record_failure(rid, reason="socrata_timeout", message="t1", permanent=False)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p2", permanent=True)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p3", permanent=True)

    radar = store.get_radar(rid)
    assert radar.failure_count == 4
    assert radar.permanent_failure_streak == 2
    assert radar.is_enabled
    assert radar.needs_attention == 0


def test_success_resets_failure_count_and_permanent_streak(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p1", permanent=True)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p2", permanent=True)

    store.update_last_run(rid)

    radar = store.get_radar(rid)
    assert radar.failure_count == 0
    assert radar.permanent_failure_streak == 0
    assert radar.last_failure_reason is None
    assert radar.next_run_after is None
    assert radar.needs_attention == 0


def test_transient_resets_streak_not_failure_count(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p1", permanent=True)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p2", permanent=True)

    store.record_failure(rid, reason="socrata_timeout", message="t1", permanent=False)

    radar = store.get_radar(rid)
    assert radar.failure_count == 3
    assert radar.permanent_failure_streak == 0
    assert radar.is_enabled


def test_success_does_not_silently_reenable_disabled_radar(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p1", permanent=True)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p2", permanent=True)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p3", permanent=True)
    assert not store.get_radar(rid).is_enabled

    store.update_last_run(rid)

    radar = store.get_radar(rid)
    assert radar.failure_count == 0
    assert radar.permanent_failure_streak == 0
    assert radar.needs_attention == 0
    assert not radar.is_enabled  # update_last_run no reactiva silenciosamente


def test_permanent_streak_counts_regardless_of_differing_reasons(store):
    """Tres permanentes consecutivos pausan aunque el reason varíe entre ellos."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_failure(rid, reason="invalid_query_frame_json", message="p1", permanent=True)
    store.record_failure(rid, reason="guard_no_scope_or_topic", message="p2", permanent=True)
    store.record_failure(rid, reason="soql_build_error", message="p3", permanent=True)

    radar = store.get_radar(rid)
    assert radar.permanent_failure_streak == 3
    assert radar.needs_attention == 1
    assert not radar.is_enabled


def test_upsert_seen_idempotent_keeps_first_seen(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    fields = {"title": "T", "entity": "E", "value": "100", "status": "Abierto",
              "urlproceso": "u", "referencia": "R", "contractor": "ACME"}
    store.upsert_seen(radar_id=rid, result_key="K1", dataset="contratos",
                      fields=fields, snapshot_hash="h1", run_id="run-1",
                      seen_at="2026-01-01T00:00:00+00:00")
    # segunda corrida: cambia estado/snapshot
    fields2 = {**fields, "status": "Terminado", "value": "200"}
    store.upsert_seen(radar_id=rid, result_key="K1", dataset="contratos",
                      fields=fields2, snapshot_hash="h2", run_id="run-2",
                      seen_at="2026-02-01T00:00:00+00:00")
    seen = store.get_seen(rid)
    assert set(seen) == {"K1"}
    rec = seen["K1"]
    assert rec["first_seen_at"] == "2026-01-01T00:00:00+00:00"  # preservado
    assert rec["last_seen_at"] == "2026-02-01T00:00:00+00:00"   # actualizado
    assert rec["first_run_id"] == "run-1"
    assert rec["last_run_id"] == "run-2"
    assert rec["snapshot_hash"] == "h2"
    assert rec["status"] == "Terminado"


def test_delete_cascades(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_run(radar_id=rid, started_at="t", status="ok")
    store.upsert_seen(radar_id=rid, result_key="K1", dataset="c",
                      fields={}, snapshot_hash="h", run_id="r")
    store.delete_radar(rid)
    assert store.get_radar(rid) is None
    assert store.get_runs(rid) == []
    assert store.get_seen(rid) == {}


# ── run lease: exclusión mutua interproceso ───────────────────────────────

def _t(seconds: int = 0) -> datetime:
    """Reloj determinista: nada de sleeps, el tiempo se inyecta."""
    return datetime(2026, 7, 14, 12, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=seconds)


def test_lease_acquire_on_free_radar(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    claim = store.try_acquire_run_lease(rid, now=_t())

    assert claim.status == LEASE_ACQUIRED
    assert claim.acquired
    assert claim.token
    radar = store.get_radar(rid)
    assert radar.run_lease_token == claim.token
    assert radar.run_lease_acquired_at == _t().isoformat()
    assert radar.run_lease_expires_at == (_t() + LEASE_DURATION).isoformat()


def test_lease_second_attempt_before_expiry_is_already_running(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    first = store.try_acquire_run_lease(rid, now=_t())
    second = store.try_acquire_run_lease(rid, now=_t(60))

    assert first.acquired
    assert second.status == LEASE_ALREADY_RUNNING
    assert second.token is None
    # el lease del primero queda intacto
    assert store.get_radar(rid).run_lease_token == first.token


def test_lease_recovered_after_expiry(store):
    """El proceso dueño murió sin liberar: el lease vence y otro lo recupera."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    dead = store.try_acquire_run_lease(rid, now=_t())
    after = int(LEASE_DURATION.total_seconds()) + 1
    recovered = store.try_acquire_run_lease(rid, now=_t(after))

    assert recovered.acquired
    assert recovered.token != dead.token
    assert store.get_radar(rid).run_lease_token == recovered.token


def test_lease_acquire_on_missing_radar(store):
    claim = store.try_acquire_run_lease("no-existe", now=_t())
    assert claim.status == LEASE_RADAR_NOT_FOUND
    assert claim.token is None


def test_lease_release_requires_owner_token(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    claim = store.try_acquire_run_lease(rid, now=_t())

    assert store.release_run_lease(rid, "token-ajeno") is False
    assert store.get_radar(rid).run_lease_token == claim.token  # sigue tomado

    assert store.release_run_lease(rid, claim.token) is True
    radar = store.get_radar(rid)
    assert radar.run_lease_token is None
    assert radar.run_lease_acquired_at is None
    assert radar.run_lease_expires_at is None


def test_stale_token_cannot_release_new_lease(store):
    """Fencing: A vence, B toma el lease, y el release tardío de A no lo pisa."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    a = store.try_acquire_run_lease(rid, now=_t())
    after = int(LEASE_DURATION.total_seconds()) + 1
    b = store.try_acquire_run_lease(rid, now=_t(after))

    assert store.release_run_lease(rid, a.token) is False
    assert store.get_radar(rid).run_lease_token == b.token


def test_lease_renew_with_owner_token(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    claim = store.try_acquire_run_lease(rid, now=_t())

    assert store.renew_run_lease(rid, claim.token, now=_t(60)) is True
    assert store.get_radar(rid).run_lease_expires_at == (_t(60) + LEASE_DURATION).isoformat()


def test_lease_renew_with_wrong_token_fails(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    claim = store.try_acquire_run_lease(rid, now=_t())
    before = store.get_radar(rid).run_lease_expires_at

    assert store.renew_run_lease(rid, "token-ajeno", now=_t(60)) is False
    assert store.get_radar(rid).run_lease_expires_at == before


def test_lease_renew_after_losing_lease_fails(store):
    """A no puede resucitar su lease vencido ni robarle la ventana a B."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    a = store.try_acquire_run_lease(rid, now=_t())
    after = int(LEASE_DURATION.total_seconds()) + 1
    b = store.try_acquire_run_lease(rid, now=_t(after))
    b_expires = store.get_radar(rid).run_lease_expires_at

    assert store.renew_run_lease(rid, a.token, now=_t(after + 1)) is False
    assert store.get_radar(rid).run_lease_token == b.token
    assert store.get_radar(rid).run_lease_expires_at == b_expires


def test_lease_renew_of_expired_own_lease_fails(store):
    """Aun sin sucesor, un lease vencido no se renueva: hay que re-adquirirlo."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    claim = store.try_acquire_run_lease(rid, now=_t())
    after = int(LEASE_DURATION.total_seconds()) + 1

    assert store.renew_run_lease(rid, claim.token, now=_t(after)) is False


def test_lease_is_owned(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    claim = store.try_acquire_run_lease(rid, now=_t())
    after = int(LEASE_DURATION.total_seconds()) + 1

    assert store.is_run_lease_owned(rid, claim.token, now=_t(60)) is True
    assert store.is_run_lease_owned(rid, "token-ajeno", now=_t(60)) is False
    assert store.is_run_lease_owned(rid, claim.token, now=_t(after)) is False


def test_lease_is_per_radar(store):
    """La exclusión es por radar: tomar uno no bloquea al otro."""
    a = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    b = store.create_radar(name="B", owner="u1", query_frame_json=_frame_json())

    assert store.try_acquire_run_lease(a, now=_t()).acquired
    assert store.try_acquire_run_lease(b, now=_t()).acquired


def test_lease_does_not_touch_scheduling_policy(store):
    """El lease decide *quién* corre; enabled/next_run_after deciden *si toca*."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    before = store.get_radar(rid)
    claim = store.try_acquire_run_lease(rid, now=_t())
    store.release_run_lease(rid, claim.token)
    after = store.get_radar(rid)

    assert (after.enabled, after.needs_attention) == (before.enabled, before.needs_attention)
    assert after.next_run_after == before.next_run_after
    assert after.failure_count == before.failure_count
    assert after.permanent_failure_streak == before.permanent_failure_streak
    assert after.last_run_at == before.last_run_at


def test_concurrent_acquire_from_two_connections_has_one_winner(tmp_path):
    """Dos conexiones SQLite independientes compitiendo: exactamente un ganador.

    Reproduce el caso real (scheduler systemd vs. «Ejecutar ahora»): procesos
    distintos, conexiones distintas, mismo archivo. Una Barrier sincroniza el
    arranque — sin sleeps ni ventanas de tiempo que adivinar.
    """
    db_path = tmp_path / "concurrent.db"
    seed = RadarStore(db_path)
    rid = seed.create_radar(name="A", owner="u1", query_frame_json=_frame_json())

    workers = 8
    barrier = threading.Barrier(workers)
    results: list = []
    errors: list = []
    lock = threading.Lock()

    def contend() -> None:
        # Cada worker con su propio RadarStore => su propia conexión SQLite.
        store = RadarStore(db_path)
        try:
            barrier.wait()
            claim = store.try_acquire_run_lease(rid, now=_t())
        except Exception as exc:  # incluye OperationalError / database is locked
            with lock:
                errors.append(exc)
            return
        with lock:
            results.append(claim)

    threads = [threading.Thread(target=contend) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == [], f"errores de concurrencia sin manejar: {errors}"
    assert len(results) == workers
    winners = [c for c in results if c.status == LEASE_ACQUIRED]
    losers = [c for c in results if c.status == LEASE_ALREADY_RUNNING]
    assert len(winners) == 1
    assert len(losers) == workers - 1
    # el token persistido es el del único ganador
    assert seed.get_radar(rid).run_lease_token == winners[0].token


def test_concurrent_release_and_acquire_never_double_owns(tmp_path):
    """Ciclo tomar→liberar desde conexiones distintas: nunca hay dos dueños."""
    db_path = tmp_path / "cycle.db"
    seed = RadarStore(db_path)
    rid = seed.create_radar(name="A", owner="u1", query_frame_json=_frame_json())

    workers = 6
    barrier = threading.Barrier(workers)
    acquired: list = []
    lock = threading.Lock()

    def cycle() -> None:
        store = RadarStore(db_path)
        barrier.wait()
        for _ in range(5):
            claim = store.try_acquire_run_lease(rid, now=_t())
            if claim.acquired:
                with lock:
                    acquired.append(claim.token)
                # solo el dueño puede liberar
                assert store.release_run_lease(rid, claim.token) is True

    threads = [threading.Thread(target=cycle) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert len(acquired) == len(set(acquired)), "un token se entregó dos veces"
    assert seed.get_radar(rid).run_lease_token is None


# ── migración aditiva sobre bases pre-P1A ─────────────────────────────────

_PRE_P1A_RADARS = """
CREATE TABLE radars (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL,
    description            TEXT NOT NULL DEFAULT '',
    query_frame_json       TEXT NOT NULL,
    dataset                TEXT,
    frequency              TEXT NOT NULL DEFAULT 'manual',
    channel                TEXT NOT NULL DEFAULT 'web',
    enabled                INTEGER NOT NULL DEFAULT 1,
    owner                  TEXT NOT NULL,
    created_at             TEXT NOT NULL,
    last_run_at            TEXT,
    created_from_trace_id  TEXT,
    last_error_at          TEXT,
    last_error_message     TEXT,
    UNIQUE (owner, name)
);
"""

_LEASE_COLUMNS = {"run_lease_token", "run_lease_acquired_at", "run_lease_expires_at"}


def _columns(db_path, table="radars") -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    finally:
        conn.close()


def _seed_legacy_db(db_path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(_PRE_P1A_RADARS)
        conn.execute(
            "INSERT INTO radars (id, name, query_frame_json, owner, created_at, "
            "last_run_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("old-1", "Radar viejo", _frame_json(), "u1", "2026-01-01T00:00:00+00:00",
             "2026-01-02T00:00:00+00:00"),
        )
        conn.commit()
    finally:
        conn.close()


def test_legacy_db_gets_lease_columns_without_losing_data(tmp_path):
    db_path = tmp_path / "legacy.db"
    _seed_legacy_db(db_path)
    assert not (_LEASE_COLUMNS & _columns(db_path))

    store = RadarStore(db_path)

    assert _LEASE_COLUMNS <= _columns(db_path)
    radar = store.get_radar("old-1")
    assert radar is not None
    assert radar.name == "Radar viejo"
    assert radar.last_run_at == "2026-01-02T00:00:00+00:00"
    assert radar.run_lease_token is None


def test_migration_is_idempotent(tmp_path):
    """Abrir la base N veces no falla ni recrea la tabla."""
    db_path = tmp_path / "legacy_twice.db"
    _seed_legacy_db(db_path)

    RadarStore(db_path)
    RadarStore(db_path)
    store = RadarStore(db_path)

    assert _LEASE_COLUMNS <= _columns(db_path)
    assert store.get_radar("old-1") is not None


def test_legacy_radar_can_take_lease_after_migration(tmp_path):
    db_path = tmp_path / "legacy_lease.db"
    _seed_legacy_db(db_path)
    store = RadarStore(db_path)

    claim = store.try_acquire_run_lease("old-1", now=_t())
    assert claim.acquired
    assert store.try_acquire_run_lease("old-1", now=_t(60)).status == LEASE_ALREADY_RUNNING


# ── migración aditiva: permanent_failure_streak (P0D.1) ───────────────────

def test_legacy_db_gets_permanent_failure_streak_without_losing_data(tmp_path):
    db_path = tmp_path / "legacy_streak.db"
    _seed_legacy_db(db_path)
    assert "permanent_failure_streak" not in _columns(db_path)

    store = RadarStore(db_path)

    assert "permanent_failure_streak" in _columns(db_path)
    radar = store.get_radar("old-1")
    assert radar is not None
    assert radar.name == "Radar viejo"
    assert radar.permanent_failure_streak == 0


def test_legacy_radar_can_record_failure_after_migration(tmp_path):
    db_path = tmp_path / "legacy_failure.db"
    _seed_legacy_db(db_path)
    store = RadarStore(db_path)

    count = store.record_failure(
        "old-1", reason="socrata_timeout", message="boom",
        next_run_after="2999-01-01T00:00:00+00:00",
    )
    radar = store.get_radar("old-1")
    assert count == 1
    assert radar.failure_count == 1
    assert radar.permanent_failure_streak == 0
