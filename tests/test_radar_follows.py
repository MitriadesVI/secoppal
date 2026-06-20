"""Tests de app/core/radar/follows.py — watchlist de procesos individuales.

SQLite temporal + SecopClient fake (no red), patrón de test_radar_runner.py.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.formatter import Formatter
from app.core.radar import follows, scheduler
from app.core.radar.store import RadarStore
from app.core.soql_builder import SoQLBuilder

PROCESOS = SoQLBuilder.PROCESOS_DATASET  # p6dx-8zbt
OWNER = "email:ana@x.com"


class FakeClient:
    def __init__(self, rows):
        self.rows = rows

    def query(self, dataset_id, soql):
        return list(self.rows)

    def count(self, dataset_id, soql):
        return len(self.rows)


def _deps(rows):
    return SimpleNamespace(
        soql_builder=SoQLBuilder(), secop_client=FakeClient(rows), formatter=Formatter(),
    )


@pytest.fixture
def store(tmp_path):
    return RadarStore(tmp_path / "follows.db")


def _card(pid="P-1", estado="Abierto", valor="$1.000.000"):
    """Fila de tarjeta enriquecida (lo que produce el formatter)."""
    return {
        "titulo": "Suministro PAE escolar", "entidad": "ALCALDIA DE BARRANQUILLA",
        "valor": valor, "estado": estado, "url": f"http://secop/{pid}",
        "id_del_proceso": pid, "referencia_del_proceso": f"REF-{pid}",
        "dataset_id": PROCESOS,
    }


def _raw(pid="P-1", estado="Abierto", precio="1000000"):
    """Fila cruda SECOP (lo que devolvería el cliente al refrescar)."""
    return {
        "id_del_proceso": pid, "referencia_del_proceso": f"REF-{pid}",
        "nombre_del_procedimiento": "Suministro PAE escolar",
        "entidad": "ALCALDIA DE BARRANQUILLA", "precio_base": precio,
        "estado_de_apertura_del_proceso": estado, "urlproceso": f"http://secop/{pid}",
        "fecha_de_publicacion_del": "2026-05-01",
    }


# ── follow / unfollow / list ────────────────────────────────────────────────
def test_follow_persists_and_is_idempotent(store):
    assert follows.follow(store, OWNER, _card("P-1")) is True
    assert follows.is_following(store, OWNER, "P-1") is True
    seguidos = follows.list_follows(store, OWNER)
    assert len(seguidos) == 1 and seguidos[0]["result_key"] == "P-1"

    # Re-seguir no duplica ni emite otro evento "followed".
    assert follows.follow(store, OWNER, _card("P-1")) is True
    assert len(follows.list_follows(store, OWNER)) == 1
    radar = follows.get_or_create_follows_radar(store, OWNER)
    feed = store.get_feed(OWNER)
    assert sum(1 for e in feed if e["kind"] == "followed") == 1


def test_follow_without_key_returns_false(store):
    row = {"titulo": "sin id", "dataset_id": PROCESOS}  # sin id ni referencia
    assert follows.follow(store, OWNER, row) is False
    assert follows.list_follows(store, OWNER) == []


def test_unfollow_removes(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.unfollow(store, OWNER, "P-1")
    assert follows.is_following(store, OWNER, "P-1") is False
    assert follows.list_follows(store, OWNER) == []


def test_follow_isolated_by_owner(store):
    follows.follow(store, OWNER, _card("P-1"))
    assert follows.list_follows(store, "email:beto@x.com") == []


# ── refresh_follows ──────────────────────────────────────────────────────────
def test_refresh_establishes_baseline_silently(store):
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    deps = _deps([_raw("P-1", estado="Abierto")])
    summary = follows.refresh_follows(deps, store, OWNER)
    assert summary["baseline"] == 1
    assert summary["changed_state"] == 0 and summary["changed_value"] == 0
    # No emite evento de cambio en la línea base.
    feed = store.get_feed(OWNER)
    assert all(e["kind"] == "followed" for e in feed)


def test_refresh_detects_state_change(store):
    follows.follow(store, OWNER, _card("P-1", estado="Abierto"))
    follows.refresh_follows(_deps([_raw("P-1", estado="Abierto")]), store, OWNER)  # baseline
    summary = follows.refresh_follows(_deps([_raw("P-1", estado="Adjudicado")]), store, OWNER)
    assert summary["changed_state"] == 1
    feed = store.get_feed(OWNER)
    ev = [e for e in feed if e["kind"] == "changed_state"]
    assert len(ev) == 1 and ev[0]["prev_status"] == "Abierto" and ev[0]["status"] == "Adjudicado"


def test_refresh_detects_value_change(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1", precio="1000000")]), store, OWNER)  # baseline
    summary = follows.refresh_follows(_deps([_raw("P-1", precio="5000000")]), store, OWNER)
    assert summary["changed_value"] == 1


def test_refresh_no_change_no_events(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)  # baseline
    summary = follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)  # igual
    assert summary["changed_state"] == 0 and summary["changed_value"] == 0


def test_refresh_marks_missing(store):
    follows.follow(store, OWNER, _card("P-1"))
    follows.refresh_follows(_deps([_raw("P-1")]), store, OWNER)
    summary = follows.refresh_follows(_deps([]), store, OWNER)  # SECOP no lo devuelve
    assert summary["missing"] == 1


# ── integración con el scheduler ──────────────────────────────────────────────
def test_scheduler_skips_follows_radar_on_query_side(store, monkeypatch):
    # Un radar de búsqueda + un proceso seguido (crea el radar sintético).
    store.create_radar(name="Búsqueda PAE", owner=OWNER, query_frame_json="{}",
                       frequency="diaria")
    follows.follow(store, OWNER, _card("P-1"))

    ran = []
    monkeypatch.setattr(scheduler, "run_radar",
                        lambda deps, st, radar: ran.append(radar.name) or SimpleNamespace(
                            status="ok",
                            diff=SimpleNamespace(new_count=0, changed_count=0),
                            digest={}, error=None))

    summary = scheduler.run_due_radars(_deps([_raw("P-1")]), store, force=True)

    # run_radar solo para la búsqueda; el radar de seguidos va por refresh_follows.
    assert ran == ["Búsqueda PAE"]
    assert summary.follows == 1
    assert follows.FOLLOWS_NAME not in ran
