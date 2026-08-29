"""Tests de app/core/radar/scheduler.py — lógica de 'due' y orquestación.

`is_due` es pura. `run_due_radars` se prueba con un stub de `run_radar`
(monkeypatch) para no tocar red: validamos qué radares corre, cuáles salta y
que un radar que falla no detiene a los demás.
"""
from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.core.radar import follows, scheduler
from app.core.radar.scheduler import is_due, run_due_radars
from app.core.radar.store import Radar, RadarStore
from app.core.soql_builder import SoQLBuilder

NOW = datetime(2026, 6, 19, 12, 0, 0, tzinfo=timezone.utc)


def _radar(frequency="diaria", last_run_at=None, *, rid="r", owner="u", enabled=1):
    return Radar(
        id=rid, name=f"radar-{rid}", description="", query_frame_json="{}",
        dataset=None, frequency=frequency, channel="web", enabled=enabled,
        owner=owner, created_at=NOW.isoformat(), last_run_at=last_run_at,
    )


def _iso(dt: datetime) -> str:
    return dt.isoformat()


# ── is_due ─────────────────────────────────────────────────────────────────
def test_manual_never_due():
    assert is_due(_radar(frequency="manual", last_run_at=None), NOW) is False
    assert is_due(_radar(frequency="manual", last_run_at=_iso(NOW - timedelta(days=30))), NOW) is False


def test_unknown_frequency_never_due():
    assert is_due(_radar(frequency="cada_minuto"), NOW) is False


def test_never_run_is_due():
    for freq in ("cada_6h", "diaria", "semanal"):
        assert is_due(_radar(frequency=freq, last_run_at=None), NOW) is True


def test_diaria_due_after_a_day():
    assert is_due(_radar("diaria", _iso(NOW - timedelta(hours=2))), NOW) is False
    assert is_due(_radar("diaria", _iso(NOW - timedelta(hours=25))), NOW) is True


def test_cada_6h_respects_interval():
    assert is_due(_radar("cada_6h", _iso(NOW - timedelta(hours=1))), NOW) is False
    assert is_due(_radar("cada_6h", _iso(NOW - timedelta(hours=7))), NOW) is True


def test_tolerance_at_boundary():
    # 5h55m < 6h pero dentro de la tolerancia de 10 min → debe correr.
    assert is_due(_radar("cada_6h", _iso(NOW - timedelta(hours=5, minutes=55))), NOW) is True


def test_naive_timestamp_treated_as_utc():
    naive = (NOW - timedelta(days=2)).replace(tzinfo=None).isoformat()
    assert is_due(_radar("diaria", naive), NOW) is True


# ── run_due_radars ─────────────────────────────────────────────────────────
@pytest.fixture
def store(tmp_path):
    return RadarStore(tmp_path / "sched.db")


def _ok_result(new=0, changed=0):
    return SimpleNamespace(
        status="ok",
        diff=SimpleNamespace(new_count=new, changed_count=changed),
        digest={"has_novelty": bool(new or changed)},
        error=None,
    )


def _make(store, name, freq, last_run=None, owner="u", enabled=True):
    rid = store.create_radar(name=name, owner=owner, query_frame_json="{}",
                             frequency=freq)
    if last_run is not None:
        store.update_last_run(rid, last_run)
    if not enabled:
        store.set_enabled(rid, False)
    return rid


def test_runs_only_due_radars(store, monkeypatch):
    _make(store, "due-nuevo", "diaria", last_run=None)
    _make(store, "no-due", "diaria", last_run=_iso(NOW - timedelta(hours=1)))
    _make(store, "manual", "manual", last_run=None)
    _make(store, "deshabilitado", "diaria", last_run=None, enabled=False)

    called = []
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar, **kwargs:
                        called.append((radar.name, kwargs.get("run_trigger")))
                        or _ok_result(new=2))

    summary = run_due_radars(deps=None, store=store, now=NOW)

    assert called == [("due-nuevo", "scheduler")]  # solo el due, con trigger formal
    assert summary.checked == 3               # los 3 enabled (el deshabilitado no entra)
    assert summary.ran == 1
    assert summary.skipped == 2               # manual + no-due
    assert summary.new_total == 2
    assert store.get_scheduler_state().last_pass_status == "completed"


def test_force_runs_all_enabled(store, monkeypatch):
    _make(store, "a", "manual", last_run=None)
    _make(store, "b", "diaria", last_run=_iso(NOW - timedelta(minutes=5)))

    called = []
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar, **kwargs:
                        called.append(radar.name) or _ok_result())

    summary = run_due_radars(deps=None, store=store, now=NOW, force=True)
    assert sorted(called) == ["a", "b"]
    assert summary.ran == 2 and summary.skipped == 0


def test_one_failure_does_not_stop_others(store, monkeypatch):
    _make(store, "bueno-1", "diaria", last_run=None)
    _make(store, "malo", "diaria", last_run=None)
    _make(store, "bueno-2", "diaria", last_run=None)

    def fake_run(deps, st, radar, **kwargs):
        if radar.name == "malo":
            raise RuntimeError("boom")
        return _ok_result(new=1)

    monkeypatch.setattr(scheduler, "run_radar", fake_run)
    summary = run_due_radars(deps=None, store=store, now=NOW)

    assert summary.ran == 2
    assert summary.errors == 1
    assert summary.new_total == 2
    bad = [o for o in summary.outcomes if o.status == "error"]
    assert len(bad) == 1 and bad[0].radar.name == "malo"
    assert store.get_scheduler_state().last_pass_status == "completed_with_errors"


def test_run_error_status_counts_as_error(store, monkeypatch):
    _make(store, "x", "diaria", last_run=None)
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar, **kwargs: SimpleNamespace(
                            status="error", diff=None, digest={}, error="sin alcance"))
    summary = run_due_radars(deps=None, store=store, now=NOW)
    assert summary.errors == 1 and summary.ran == 0


def test_summary_line_format(store, monkeypatch):
    _make(store, "a", "diaria", last_run=None)
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar, **kwargs: _ok_result(new=3, changed=1))
    summary = run_due_radars(deps=None, store=store, now=NOW)
    line = summary.as_line()
    assert "corridos=1" in line and "nuevos=3" in line and "cambios=1" in line


def test_pass_without_due_radars_uses_real_operational_clock(store, monkeypatch):
    start_calls = []
    finish_calls = []
    original_start = store.start_scheduler_pass
    original_finish = store.finish_scheduler_pass

    def start_spy(*args, **kwargs):
        start_calls.append((args, kwargs))
        return original_start(*args, **kwargs)

    def finish_spy(*args, **kwargs):
        finish_calls.append((args, kwargs))
        return original_finish(*args, **kwargs)

    monkeypatch.setattr(store, "start_scheduler_pass", start_spy)
    monkeypatch.setattr(store, "finish_scheduler_pass", finish_spy)

    before = datetime.now(timezone.utc)
    summary = run_due_radars(deps=None, store=store, now=NOW)
    after = datetime.now(timezone.utc)

    state = store.get_scheduler_state()
    heartbeat = datetime.fromisoformat(state.last_heartbeat_at)
    started = datetime.fromisoformat(state.last_pass_started_at)
    finished = datetime.fromisoformat(state.last_pass_finished_at)

    assert summary.checked == 0
    assert start_calls == [((), {})]
    assert finish_calls == [(('completed',), {})]
    assert before <= started <= after
    assert before <= finished <= after
    assert finished >= started
    assert heartbeat == finished
    assert NOW.isoformat() not in {
        state.last_heartbeat_at,
        state.last_pass_started_at,
        state.last_pass_finished_at,
    }
    assert state.last_pass_status == "completed"


# ── P1B-Preflight 2 · el scheduler distingue omisión de fallo ──────────────

def _follows_radar(store, owner="u"):
    return follows.get_or_create_follows_radar(store, owner)


@pytest.mark.parametrize("status", sorted(follows.REFRESH_SKIPPED))
def test_follows_skipped_by_concurrency_is_not_an_operational_error(
    store, monkeypatch, status
):
    """Que otro proceso tenga el lease es normal, no una alarma.

    Si contara como error, dos schedulers solapados harían que el radar de
    seguidos pareciera roto en cada pasada.
    """
    _follows_radar(store)
    monkeypatch.setattr(follows, "refresh_follows",
                        lambda deps, st, owner: follows._summary(status))

    summary = run_due_radars(deps=None, store=store, now=NOW)

    assert summary.errors == 0
    assert summary.skipped == 1
    assert summary.follows == 0
    assert [o.status for o in summary.outcomes] == ["skipped"]


def test_follows_refreshed_counts_as_refreshed(store, monkeypatch):
    _follows_radar(store)
    monkeypatch.setattr(
        follows, "refresh_follows",
        lambda deps, st, owner: follows._summary(
            follows.REFRESH_REFRESHED, checked=2, changed_state=1, changed_value=1),
    )

    summary = run_due_radars(deps=None, store=store, now=NOW)

    assert summary.errors == 0 and summary.skipped == 0
    assert summary.follows == 1
    assert summary.changed_total == 2


def test_follows_exception_still_counts_as_error(store, monkeypatch):
    """Un fallo real sigue siendo error: el skip no lo enmascara."""
    _follows_radar(store)

    def boom(deps, st, owner):
        raise RuntimeError("socrata caída")

    monkeypatch.setattr(follows, "refresh_follows", boom)
    summary = run_due_radars(deps=None, store=store, now=NOW)

    assert summary.errors == 1 and summary.skipped == 0


def test_not_follows_is_an_error_not_a_skip(store, monkeypatch):
    """Que el radar deje de ser follows es un invariante roto, no concurrencia.

    Contarlo como omisión haría que una watchlist corrupta pareciera sana en
    cada pasada del scheduler.
    """
    _follows_radar(store)

    def integrity_violation(deps, st, owner):
        raise follows.FollowsRadarIntegrityError("ya no es follows")

    monkeypatch.setattr(follows, "refresh_follows", integrity_violation)
    summary = run_due_radars(deps=None, store=store, now=NOW)

    assert summary.errors == 1
    assert summary.skipped == 0
    assert [o.status for o in summary.outcomes] == ["error"]


def test_not_follows_is_not_in_the_skipped_set():
    """Congela la decisión: REFRESH_SKIPPED es solo concurrencia legítima."""
    assert follows.REFRESH_SKIPPED == {follows.REFRESH_ALREADY_RUNNING,
                                       follows.REFRESH_LEASE_LOST}
    assert not hasattr(follows, "REFRESH_NOT_FOLLOWS")


def test_two_concurrent_schedulers_do_not_duplicate_follows_events(tmp_path):
    """Dos pasadas del scheduler a la vez: un solo evento de seguidos.

    Es el bug N15 end-to-end, por la puerta por la que entra de verdad
    (``run_due_radars``), no llamando a ``refresh_follows`` a mano. La
    sincronización es por Event: el scheduler B arranca cuando A ya está en la
    red, así la ventana se reproduce sin sleeps.
    """
    db_path = tmp_path / "sched_race.db"
    seed = RadarStore(db_path)
    card = {"titulo": "PAE", "entidad": "ALC", "valor": "1000000",
            "estado": "Abierto", "url": "u", "id_del_proceso": "P-1",
            "referencia_del_proceso": "REF-P-1",
            "dataset_id": SoQLBuilder.PROCESOS_DATASET}
    follows.follow(seed, "u", card)

    def _raw(estado):
        return {"id_del_proceso": "P-1", "referencia_del_proceso": "REF-P-1",
                "nombre_del_procedimiento": "PAE", "entidad": "ALC",
                "precio_base": "1000000", "estado_de_apertura_del_proceso": estado,
                "urlproceso": "u"}

    def _deps(client):
        return SimpleNamespace(soql_builder=SoQLBuilder(), secop_client=client)

    # baseline
    run_due_radars(
        deps=_deps(SimpleNamespace(query=lambda d, s: [_raw("Abierto")])),
        store=seed, now=NOW)

    a_in_network = threading.Event()
    b_done = threading.Event()

    class GateClient:
        def query(self, dataset_id, soql):
            a_in_network.set()
            b_done.wait(timeout=15)
            return [_raw("Adjudicado")]

    class BClient:
        calls = 0

        def query(self, dataset_id, soql):
            BClient.calls += 1
            return [_raw("Adjudicado")]

    summaries: dict = {}

    def run_a():
        summaries["a"] = run_due_radars(
            deps=_deps(GateClient()), store=RadarStore(db_path), now=NOW)

    def run_b():
        assert a_in_network.wait(timeout=15)
        summaries["b"] = run_due_radars(
            deps=_deps(BClient()), store=RadarStore(db_path), now=NOW)
        b_done.set()

    ta, tb = threading.Thread(target=run_a), threading.Thread(target=run_b)
    ta.start(); tb.start()
    ta.join(timeout=60); tb.join(timeout=60)

    conn = sqlite3.connect(db_path)
    events = conn.execute(
        "SELECT kind FROM radar_events WHERE kind = 'changed_state'"
    ).fetchall()
    conn.close()

    assert len(events) == 1, "dos schedulers duplicaron el evento de seguidos"
    assert BClient.calls == 0, "el scheduler omitido consultó SECOP"
    assert summaries["b"].errors == 0, "un skip por concurrencia no es un error"
    assert summaries["b"].skipped == 1
    assert summaries["a"].follows == 1
