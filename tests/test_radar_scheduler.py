"""Tests de app/core/radar/scheduler.py — lógica de 'due' y orquestación.

`is_due` es pura. `run_due_radars` se prueba con un stub de `run_radar`
(monkeypatch) para no tocar red: validamos qué radares corre, cuáles salta y
que un radar que falla no detiene a los demás.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.core.radar import scheduler
from app.core.radar.scheduler import is_due, run_due_radars
from app.core.radar.store import Radar, RadarStore

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
                        lambda deps, st, radar: called.append(radar.name) or _ok_result(new=2))

    summary = run_due_radars(deps=None, store=store, now=NOW)

    assert called == ["due-nuevo"]            # solo el due (manual/no-due no; deshabilitado ni se lista)
    assert summary.checked == 3               # los 3 enabled (el deshabilitado no entra)
    assert summary.ran == 1
    assert summary.skipped == 2               # manual + no-due
    assert summary.new_total == 2


def test_force_runs_all_enabled(store, monkeypatch):
    _make(store, "a", "manual", last_run=None)
    _make(store, "b", "diaria", last_run=_iso(NOW - timedelta(minutes=5)))

    called = []
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar: called.append(radar.name) or _ok_result())

    summary = run_due_radars(deps=None, store=store, now=NOW, force=True)
    assert sorted(called) == ["a", "b"]
    assert summary.ran == 2 and summary.skipped == 0


def test_one_failure_does_not_stop_others(store, monkeypatch):
    _make(store, "bueno-1", "diaria", last_run=None)
    _make(store, "malo", "diaria", last_run=None)
    _make(store, "bueno-2", "diaria", last_run=None)

    def fake_run(deps, st, radar):
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


def test_run_error_status_counts_as_error(store, monkeypatch):
    _make(store, "x", "diaria", last_run=None)
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar: SimpleNamespace(
                            status="error", diff=None, digest={}, error="sin alcance"))
    summary = run_due_radars(deps=None, store=store, now=NOW)
    assert summary.errors == 1 and summary.ran == 0


def test_summary_line_format(store, monkeypatch):
    _make(store, "a", "diaria", last_run=None)
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar: _ok_result(new=3, changed=1))
    summary = run_due_radars(deps=None, store=store, now=NOW)
    line = summary.as_line()
    assert "corridos=1" in line and "nuevos=3" in line and "cambios=1" in line
