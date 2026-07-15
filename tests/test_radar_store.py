"""Tests de app/core/radar/store.py — persistencia del Radar."""
from __future__ import annotations

import ast
import dataclasses
import inspect
import json
import sqlite3
import textwrap
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core.radar import store as store_module
from app.core.radar.lease import (
    LEASE_ACQUIRED,
    LEASE_ALREADY_RUNNING,
    LEASE_DURATION,
    LEASE_RADAR_NOT_FOUND,
)
from app.core.radar.store import (
    PHASE_AFTER_BEGIN,
    PHASE_AFTER_EVENT_INSERTS,
    PHASE_AFTER_FAILURE_UPDATE,
    PHASE_AFTER_LEASE_FENCE,
    PHASE_AFTER_RADAR_UPDATE,
    PHASE_AFTER_RUN_INSERT,
    PHASE_AFTER_SEEN_UPSERTS,
    PHASE_BEFORE_COMMIT,
    EventRecord,
    FailureRecord,
    RadarExistsError,
    RadarStore,
    RunRecord,
    SeenRecord,
    new_run_id,
)


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


# ── P1B-Preflight 1 · materialización atómica de la corrida ──────────────
#
# El bug original: run, seen, events y estado del radar se escribían en
# conexiones autocommit separadas. Un crash entre los upsert_seen y los
# INSERT de eventos dejaba los resultados marcados como vistos sin haber
# anunciado su novedad — y como la corrida siguiente ya los ve "vistos", esa
# novedad se pierde para siempre. Estos tests verifican el estado con sqlite3
# crudo (no con el store) para no leer una caché ni una transacción abierta.

class _Boom(RuntimeError):
    """Fallo inyectado en una fase concreta de la transacción."""


def _hook_at(phase: str):
    """Hook de fault injection: revienta exactamente en ``phase``."""
    def hook(current: str) -> None:
        if current == phase:
            raise _Boom(f"fallo inyectado en {phase}")
    return hook


def _read_state(db_path, radar_id: str) -> dict:
    """Estado real en disco, leído por fuera del store."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        radar = conn.execute(
            "SELECT * FROM radars WHERE id = ?", (radar_id,)
        ).fetchone()
        return {
            "runs": [
                dict(r) for r in conn.execute(
                    "SELECT * FROM radar_runs WHERE radar_id = ?", (radar_id,)
                )
            ],
            "seen": {
                r["result_key"]: dict(r) for r in conn.execute(
                    "SELECT * FROM radar_seen_results WHERE radar_id = ?", (radar_id,)
                )
            },
            "events": [
                dict(r) for r in conn.execute(
                    "SELECT * FROM radar_events WHERE radar_id = ?", (radar_id,)
                )
            ],
            "radar": dict(radar) if radar else None,
        }
    finally:
        conn.close()


def _run(**over) -> RunRecord:
    base = dict(
        id=new_run_id(), started_at="2026-07-15T00:00:00+00:00", status="ok",
        total_count=2, new_count=1, changed_count=1, digest_json='{"text": "d"}',
        soql="SELECT 1", params_json="{}", dataset_id="jbjy-vk9h",
        execution_ms=10, outcome="radar_ok", reason="ok",
    )
    return RunRecord(**{**base, **over})


def _seen_rec(key: str, *, status: str = "Abierto", snap: str = "h2") -> SeenRecord:
    return SeenRecord(
        result_key=key, dataset="procesos",
        fields={"title": f"T-{key}", "entity": "ALCALDIA", "value": "100",
                "status": status, "urlproceso": "u", "referencia": "R",
                "contractor": "ACME"},
        snapshot_hash=snap, last_snapshot_json="{}",
    )


def _event(*, kind: str = "new", key: str = "K-new") -> EventRecord:
    """Sin radar_id/run_id/owner/radar_name: EventRecord no los expone."""
    return EventRecord(
        kind=kind, ts="2026-07-15T00:00:00+00:00", result_key=key,
        dataset="procesos", title="T", entity="E", value="100", status="Abierto",
    )


@pytest.fixture
def leased(store):
    """Radar con un seen previo y el lease ya tomado por nosotros."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.upsert_seen(
        radar_id=rid, result_key="K-old", dataset="procesos",
        fields={"status": "Abierto", "value": "100"}, snapshot_hash="h1",
        run_id="run-seed", seen_at="2026-01-01T00:00:00+00:00",
    )
    claim = store.try_acquire_run_lease(rid)
    assert claim.acquired
    return rid, claim.token


def _materialize_success(store, rid, token, *, hook=None, now=None):
    run = _run()
    return store.materialize_successful_run(
        radar_id=rid, token=token, run=run,
        seen=[_seen_rec("K-old", status="Adjudicado"), _seen_rec("K-new")],
        events=[_event(kind="changed_state", key="K-old"),
                _event(kind="new", key="K-new")],
        now=now, _fault_hook=hook,
    )


# 1 · éxito feliz

def test_materialize_success_persists_run_seen_events_and_state(store, leased):
    rid, token = leased
    store.record_failure(rid, reason="socrata_timeout", message="previo")

    result = _materialize_success(store, rid, token)

    assert result.lease_lost is False
    assert result.run_id
    state = _read_state(store.db_path, rid)
    assert len(state["runs"]) == 1
    assert state["runs"][0]["outcome"] == "radar_ok"
    assert set(state["seen"]) == {"K-old", "K-new"}
    assert state["seen"]["K-old"]["status"] == "Adjudicado"
    assert state["seen"]["K-old"]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert {e["kind"] for e in state["events"]} == {"changed_state", "new"}
    # estado limpio
    assert state["radar"]["last_run_at"]
    assert state["radar"]["failure_count"] == 0
    assert state["radar"]["permanent_failure_streak"] == 0
    assert state["radar"]["last_error_message"] is None
    assert state["radar"]["next_run_after"] is None
    # el lease sigue siendo nuestro hasta que el caller lo libere
    assert state["radar"]["run_lease_token"] == token


def test_materialize_success_extends_lease_window(store, leased):
    """El fence no solo valida: extiende la ventana para la fase de escritura.

    ``renew_at`` se deriva del ``run_lease_acquired_at`` real (no de un reloj
    fijo ajeno a cuándo se tomó el lease), así el test es autoconsistente sin
    importar cuándo corre de verdad.
    """
    rid, token = leased
    radar_before = store.get_radar(rid)
    before = radar_before.run_lease_expires_at
    acquired_at = datetime.fromisoformat(radar_before.run_lease_acquired_at)
    renew_at = acquired_at + timedelta(seconds=60)

    _materialize_success(store, rid, token, now=renew_at)

    after = store.get_radar(rid).run_lease_expires_at
    assert after > before
    assert after == (renew_at + LEASE_DURATION).isoformat()


def test_materialize_success_clears_needs_attention_characterization(store, leased):
    """Caracterización: hoy una corrida OK limpia needs_attention.

    Preflight 3 cambiará esto (una corrida OK no debería borrar la necesidad
    de revisión humana). El test congela la semántica actual para que ese
    cambio sea deliberado y visible, no un efecto colateral de este refactor.
    """
    rid, token = leased
    for _ in range(3):
        store.record_failure(rid, reason="guard_no_scope_or_topic",
                             message="p", permanent=True)
    assert store.get_radar(rid).needs_attention == 1
    store.set_enabled(rid, True)
    claim = store.try_acquire_run_lease(rid)  # re-tomar: record_failure no toca lease
    token = claim.token if claim.acquired else token

    _materialize_success(store, rid, token)

    radar = store.get_radar(rid)
    assert radar.needs_attention == 0        # semántica actual, cambia en Preflight 3
    assert radar.is_enabled                  # no se toca enabled aquí


# ── identidad de eventos: derivada de la transacción, no del caller ───────
#
# materialize_successful_run valida el lease de radar_id, pero antes del fix
# insertaba eventos confiando en radar_id/run_id/owner/radar_name aportados
# dentro de cada dict — una llamada autorizada sobre A podía declarar un
# evento como perteneciente a B. Ahora EventRecord no expone esos campos: la
# operación los deriva del lease ya renovado y de la fila de radars leída
# justo después del fence.

def test_event_record_does_not_expose_identity_fields():
    """EventRecord no puede llevar radar_id/run_id/owner/radar_name."""
    fields = {f.name for f in dataclasses.fields(EventRecord)}
    assert not fields & {"radar_id", "run_id", "owner", "radar_name"}


def test_materialized_events_carry_the_real_identity_of_radar_a(store, leased):
    """Los eventos insertados llevan radar_id/run_id/owner/radar_name reales."""
    rid, token = leased
    radar_a = store.get_radar(rid)

    result = _materialize_success(store, rid, token)

    state = _read_state(store.db_path, rid)
    assert state["events"]
    for ev in state["events"]:
        assert ev["radar_id"] == radar_a.id
        assert ev["run_id"] == result.run_id
        assert ev["owner"] == radar_a.owner
        assert ev["radar_name"] == radar_a.name


def test_event_identity_cannot_be_forged_via_the_record(store, leased):
    """EventRecord no tiene dónde poner una identidad ajena — ni por error.

    Antes del fix, un dict de evento con ``radar_id``/``owner`` propios de otro
    radar se insertaba tal cual. Ahora ni siquiera es representable: construir
    un EventRecord con esos kwargs falla en el constructor, y lo que sí se
    persiste ignora cualquier campo que no sea parte del dataclass.
    """
    rid, token = leased
    radar_a = store.get_radar(rid)
    other = store.create_radar(name="B", owner="u-ajeno", query_frame_json=_frame_json())

    with pytest.raises(TypeError):
        EventRecord(kind="new", result_key="K-forged", radar_id=other,
                   owner="u-ajeno", radar_name="B", run_id="run-ajeno")

    # y la vía real (constructor válido) igual termina con la identidad de A.
    result = _materialize_success(store, rid, token)
    state = _read_state(store.db_path, rid)
    assert all(ev["radar_id"] == radar_a.id for ev in state["events"])
    assert all(ev["owner"] == radar_a.owner for ev in state["events"])
    assert store.get_seen(other) == {}          # el radar B, intacto


# 2-5 · rollback total en cada fase

@pytest.mark.parametrize("phase", [
    PHASE_AFTER_BEGIN,
    PHASE_AFTER_LEASE_FENCE,
    PHASE_AFTER_RUN_INSERT,
    PHASE_AFTER_SEEN_UPSERTS,
    PHASE_AFTER_EVENT_INSERTS,
    PHASE_AFTER_RADAR_UPDATE,
    PHASE_BEFORE_COMMIT,
])
def test_exception_in_any_phase_rolls_back_everything(store, leased, phase):
    rid, token = leased
    before = _read_state(store.db_path, rid)

    with pytest.raises(_Boom):
        _materialize_success(store, rid, token, hook=_hook_at(phase))

    after = _read_state(store.db_path, rid)
    assert after["runs"] == []
    assert after["events"] == []
    assert after["seen"] == before["seen"]
    assert after["radar"] == before["radar"]


def test_exception_after_seen_upserts_does_not_lose_novelty(store, leased):
    """El bug original, aislado: seen no puede adelantarse a los eventos.

    Si el upsert de K-old (Abierto → Adjudicado) sobreviviera al crash sin su
    evento, la corrida siguiente vería el snapshot ya actualizado, no
    detectaría cambio, y el cambio de estado no se anunciaría nunca.
    """
    rid, token = leased

    with pytest.raises(_Boom):
        _materialize_success(store, rid, token, hook=_hook_at(PHASE_AFTER_SEEN_UPSERTS))

    state = _read_state(store.db_path, rid)
    assert set(state["seen"]) == {"K-old"}                  # K-new no entró
    assert state["seen"]["K-old"]["snapshot_hash"] == "h1"  # seen NO se adelantó
    assert state["seen"]["K-old"]["status"] == "Abierto"
    assert state["events"] == []                            # ningún evento perdido
    assert state["runs"] == []                              # sin run parcial


def test_autocommit_mutation_proves_the_test_would_catch_the_bug(store, leased):
    """Mutación: con las llamadas autocommit separadas, el estado sí se parte.

    Reproduce la secuencia anterior al refactor (record_run + upsert_seen y
    luego un fallo antes de los eventos) para demostrar que el test de arriba
    no pasa por vacuidad: detecta exactamente la diferencia entre escribir por
    separado y escribir en una transacción.
    """
    rid, _token = leased

    store.record_run(radar_id=rid, started_at="t", status="ok", outcome="radar_ok")
    store.upsert_seen(
        radar_id=rid, result_key="K-old", dataset="procesos",
        fields={"status": "Adjudicado", "value": "100"}, snapshot_hash="h2",
        run_id="run-x",
    )
    # aquí caía el proceso, antes de record_events

    state = _read_state(store.db_path, rid)
    assert len(state["runs"]) == 1                          # run huérfano persistido
    assert state["seen"]["K-old"]["snapshot_hash"] == "h2"  # seen adelantado
    assert state["events"] == []                            # y la novedad, perdida


# 6 · crash después del COMMIT

def test_crash_after_commit_leaves_complete_state(store, leased):
    """El caller muere justo después de materializar: nada queda a medias."""
    rid, token = leased

    result = _materialize_success(store, rid, token)
    with pytest.raises(_Boom):
        raise _Boom("el proceso muere después del commit")

    state = _read_state(store.db_path, rid)
    assert len(state["runs"]) == 1
    assert state["runs"][0]["id"] == result.run_id
    assert set(state["seen"]) == {"K-old", "K-new"}
    assert len(state["events"]) == 2
    assert state["radar"]["last_run_at"]


# 7-8 · fencing del lease dentro de la transacción

def test_stolen_lease_before_materialize_writes_nothing(store, leased):
    """B se queda con el radar mientras A tenía datos en la mano."""
    rid, token = leased
    before = _read_state(store.db_path, rid)
    expired = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
    b = store.try_acquire_run_lease(rid, now=expired)
    assert b.acquired

    result = _materialize_success(store, rid, token)

    assert result.lease_lost is True
    assert result.run_id is None
    after = _read_state(store.db_path, rid)
    assert after["runs"] == []
    assert after["events"] == []
    assert after["seen"] == before["seen"]
    assert after["radar"]["failure_count"] == before["radar"]["failure_count"]
    assert after["radar"]["last_run_at"] == before["radar"]["last_run_at"]
    assert after["radar"]["run_lease_token"] == b.token   # el token de B, intacto
    # y A tampoco le corrió la ventana a B
    assert after["radar"]["run_lease_expires_at"] == (expired + LEASE_DURATION).isoformat()


def test_expired_owner_without_successor_cannot_materialize(store, leased):
    """Sin sucesor, el dueño vencido tampoco escribe: no se auto-renueva."""
    rid, token = leased
    before = _read_state(store.db_path, rid)
    too_late = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)

    result = _materialize_success(store, rid, token, now=too_late)

    assert result.lease_lost is True
    after = _read_state(store.db_path, rid)
    assert after["runs"] == []
    assert after["events"] == []
    assert after["seen"] == before["seen"]
    assert after["radar"] == before["radar"]   # ni siquiera se extendió el lease


# 9 · dos materializadores concurrentes

def test_two_concurrent_materializers_only_owner_writes(tmp_path):
    """Conexiones distintas, arranque sincronizado por Barrier: un solo ganador."""
    db_path = tmp_path / "materialize_race.db"
    seed = RadarStore(db_path)
    rid = seed.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    owner = seed.try_acquire_run_lease(rid)
    assert owner.acquired

    barrier = threading.Barrier(2)
    results: dict = {}
    lock = threading.Lock()

    def materialize(label: str, token: str) -> None:
        store = RadarStore(db_path)          # su propia conexión
        run = _run()
        barrier.wait()
        res = store.materialize_successful_run(
            radar_id=rid, token=token, run=run, seen=[_seen_rec(f"K-{label}")],
            events=[_event(key=f"K-{label}")],
        )
        with lock:
            results[label] = res

    threads = [
        threading.Thread(target=materialize, args=("owner", owner.token)),
        threading.Thread(target=materialize, args=("stale", "token-vencido")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert results["owner"].lease_lost is False
    assert results["stale"].lease_lost is True
    state = _read_state(db_path, rid)
    assert len(state["runs"]) == 1                     # una sola corrida
    assert set(state["seen"]) == {"K-owner"}           # sin escrituras del perdedor
    assert [e["result_key"] for e in state["events"]] == ["K-owner"]


# 10-11 · materialización de fallo

def _materialize_failure(store, rid, token, *, permanent: bool, hook=None, now=None):
    run = _run(status="error", outcome="radar_timeout", reason="socrata_timeout",
               error="timeout", total_count=0, new_count=0, changed_count=0)
    return store.materialize_failed_run(
        radar_id=rid, token=token, run=run,
        failure=FailureRecord(
            reason="socrata_timeout" if not permanent else "guard_no_scope_or_topic",
            message="boom", next_run_after="2999-01-01T00:00:00+00:00",
            permanent=permanent, pause_after=3,
        ),
        now=now, _fault_hook=hook,
    )


def test_materialize_failure_persists_run_and_backoff_together(store, leased):
    rid, token = leased

    result = _materialize_failure(store, rid, token, permanent=False)

    assert result.lease_lost is False
    state = _read_state(store.db_path, rid)
    assert len(state["runs"]) == 1
    assert state["runs"][0]["outcome"] == "radar_timeout"
    assert state["radar"]["failure_count"] == 1
    assert state["radar"]["permanent_failure_streak"] == 0
    assert state["radar"]["last_failure_reason"] == "socrata_timeout"
    assert state["radar"]["next_run_after"] == "2999-01-01T00:00:00+00:00"
    assert state["radar"]["enabled"] == 1


@pytest.mark.parametrize("phase", [
    PHASE_AFTER_BEGIN,
    PHASE_AFTER_LEASE_FENCE,
    PHASE_AFTER_RUN_INSERT,
    PHASE_AFTER_FAILURE_UPDATE,
    PHASE_BEFORE_COMMIT,
])
def test_exception_in_failure_phase_rolls_back_run_and_counters(store, leased, phase):
    """Ni la corrida ni el backoff quedan sin el otro."""
    rid, token = leased
    before = _read_state(store.db_path, rid)

    with pytest.raises(_Boom):
        _materialize_failure(store, rid, token, permanent=False, hook=_hook_at(phase))

    after = _read_state(store.db_path, rid)
    assert after["runs"] == []
    assert after["radar"] == before["radar"]   # sin contador, sin backoff


def test_permanent_failure_run_and_streak_are_written_together(store, leased):
    rid, token = leased
    for _ in range(2):
        store.record_failure(rid, reason="guard_no_scope_or_topic",
                             message="p", permanent=True)
    assert store.get_radar(rid).permanent_failure_streak == 2

    result = _materialize_failure(store, rid, token, permanent=True)

    assert result.lease_lost is False
    state = _read_state(store.db_path, rid)
    assert len(state["runs"]) == 1                          # el 3er run existe…
    assert state["radar"]["permanent_failure_streak"] == 3  # …junto a su racha
    assert state["radar"]["needs_attention"] == 1
    assert state["radar"]["enabled"] == 0                   # pausa de P0D.1 intacta


def test_permanent_failure_rollback_leaves_streak_untouched(store, leased):
    rid, token = leased
    for _ in range(2):
        store.record_failure(rid, reason="guard_no_scope_or_topic",
                             message="p", permanent=True)

    with pytest.raises(_Boom):
        _materialize_failure(store, rid, token, permanent=True,
                             hook=_hook_at(PHASE_BEFORE_COMMIT))

    state = _read_state(store.db_path, rid)
    assert state["runs"] == []
    assert state["radar"]["permanent_failure_streak"] == 2  # no avanzó a 3
    assert state["radar"]["needs_attention"] == 0
    assert state["radar"]["enabled"] == 1


def test_stolen_lease_on_failure_path_writes_nothing(store, leased):
    rid, token = leased
    before = _read_state(store.db_path, rid)
    expired = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
    b = store.try_acquire_run_lease(rid, now=expired)

    result = _materialize_failure(store, rid, token, permanent=True)

    assert result.lease_lost is True
    after = _read_state(store.db_path, rid)
    assert after["runs"] == []
    assert after["radar"]["failure_count"] == before["radar"]["failure_count"]
    assert after["radar"]["permanent_failure_streak"] == 0
    assert after["radar"]["next_run_after"] is None
    assert after["radar"]["run_lease_token"] == b.token


# 14 · la materialización no puede hacer red

@pytest.mark.parametrize("name", ["materialize_successful_run", "materialize_failed_run"])
def test_materializers_take_no_network_dependencies(name):
    """Estructural: la transacción no puede recibir ni alcanzar la red.

    Si un materializador aceptara deps/secop_client, alguien podría hacer un
    lookup con la transacción abierta y sostener el lock de escritura de
    SQLite durante un timeout de Socrata.
    """
    method = getattr(RadarStore, name)
    params = set(inspect.signature(method).parameters)
    assert not params & {"deps", "secop_client", "client", "http", "session"}

    tree = ast.parse(textwrap.dedent(inspect.getsource(method)))
    called = {
        node.func.id for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    } | {
        node.func.attr for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert "fetch_current_rows_by_keys" not in called
    assert not called & {"query", "count", "get", "post", "sleep", "request"}
    assert not any(isinstance(n, (ast.Await, ast.AsyncFor)) for n in ast.walk(tree))


def test_store_module_imports_no_http_client():
    """El store no conoce la red: ni cliente Socrata, ni HTTP, ni el runner."""
    tree = ast.parse(Path(store_module.__file__).read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
            imported.add(node.module)
    forbidden = {"requests", "httpx", "urllib", "http", "aiohttp", "socket", "asyncio"}
    assert not imported & forbidden
    assert "app.core.radar.follows" not in imported
    assert "app.core.radar.runner" not in imported
