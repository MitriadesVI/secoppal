"""Tests e2e de app/core/radar/runner.py con un SecopClient fake.

Cubre el ciclo del MVP: 1ª corrida = todo nuevo; 2ª = 0 novedades;
3ª = cambio de estado detectado. Más la bandera de ventana (radar amplio).
"""
from __future__ import annotations

import dataclasses
import json
from types import SimpleNamespace

import pytest

from app.core.formatter import Formatter
from app.core.query_frame import frame_from_params
from app.core.radar.runner import WINDOW_LIMIT, run_radar
from app.core.radar.store import RadarStore
from app.core.soql_builder import SoQLBuilder


class FakeClient:
    """SecopClient fake: devuelve filas controlables, no toca red."""

    def __init__(self, rows, count_value=None):
        self.rows = rows
        self.count_value = count_value

    def query(self, dataset_id, soql):
        return list(self.rows)

    def count(self, dataset_id, soql):
        return self.count_value if self.count_value is not None else len(self.rows)


def _deps(client):
    return SimpleNamespace(
        soql_builder=SoQLBuilder(),
        secop_client=client,
        formatter=Formatter(),
    )


def _proc(pid, estado="Abierto", valor="1000000", objeto="Suministro PAE escolar"):
    return {
        "id_del_proceso": pid,
        "referencia_del_proceso": f"REF-{pid}",
        "nombre_del_procedimiento": objeto,
        "entidad": "ALCALDIA DE BARRANQUILLA",
        "departamento_entidad": "ATLÁNTICO",
        "precio_base": valor,
        "estado_de_apertura_del_proceso": estado,
        "urlproceso": f"http://secop/{pid}",
        "fecha_de_publicacion_del": "2026-05-01",
    }


@pytest.fixture
def store(tmp_path):
    return RadarStore(tmp_path / "runner.db")


def _make_radar(store):
    params = {"objeto": ["alimentacion_escolar"],
              "departamento_resolved": "ATLÁNTICO"}
    frame_json = json.dumps(dataclasses.asdict(frame_from_params(params)))
    rid = store.create_radar(
        name="Radar PAE Atlántico", owner="u1",
        query_frame_json=frame_json, dataset="procesos",
    )
    return store.get_radar(rid)


def test_first_run_all_new(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1"), _proc("P-2")])
    res = run_radar(_deps(client), store, radar)

    assert res.status == "ok"
    assert res.diff.new_count == 2
    assert res.diff.changed_count == 0
    assert res.digest["has_novelty"] is True
    # seen poblado
    assert set(store.get_seen(radar.id)) == {"P-1", "P-2"}
    # run registrado con auditoría
    runs = store.get_runs(radar.id)
    assert len(runs) == 1
    assert runs[0].soql and runs[0].params_json
    assert runs[0].new_count == 2


def test_second_run_no_novelty(store):
    radar = _make_radar(store)
    rows = [_proc("P-1"), _proc("P-2")]
    deps = _deps(FakeClient(rows))
    run_radar(deps, store, radar)            # 1ª corrida
    res = run_radar(deps, store, radar)      # 2ª corrida, mismas filas

    assert res.diff.new_count == 0
    assert res.diff.changed_count == 0
    assert res.digest["has_novelty"] is False
    assert "Sin novedades" in res.digest["text"]


def test_third_run_detects_state_change(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", estado="Abierto"), _proc("P-2")])
    run_radar(_deps(client), store, radar)   # 1ª
    run_radar(_deps(client), store, radar)   # 2ª, sin cambios

    # 3ª: P-1 cambia de estado
    client.rows = [_proc("P-1", estado="Adjudicado"), _proc("P-2")]
    res = run_radar(_deps(client), store, radar)

    assert res.diff.new_count == 0
    assert len(res.diff.changed_state) == 1
    assert res.diff.changed_state[0].key == "P-1"
    assert res.diff.changed_state[0].prev_status == "Abierto"
    # snapshot persistido se actualizó
    assert store.get_seen(radar.id)["P-1"]["status"] == "Adjudicado"


def test_value_change_detected(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", valor="1000000")])
    run_radar(_deps(client), store, radar)
    client.rows = [_proc("P-1", valor="9000000")]
    res = run_radar(_deps(client), store, radar)
    assert len(res.diff.changed_value) == 1
    assert not res.diff.changed_state


def test_wide_radar_risk_flag(store):
    radar = _make_radar(store)
    # 100 filas (== ventana) fuerza el count; count=250 > ventana → bandera.
    rows = [_proc(f"P-{i}") for i in range(WINDOW_LIMIT)]
    client = FakeClient(rows, count_value=250)
    res = run_radar(_deps(client), store, radar)
    assert res.total == 250
    assert any("Radar amplio" in f for f in res.digest["risk_flags"])


# ── Feed events ──────────────────────────────────────────────────────────

def test_first_run_emits_baseline_only(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1"), _proc("P-2")])
    run_radar(_deps(client), store, radar)
    feed = store.get_feed(radar.owner)
    # Primera corrida: NO inunda con 2 "nuevos", solo 1 baseline.
    assert len(feed) == 1
    assert feed[0]["kind"] == "baseline"
    assert feed[0]["radar_name"] == radar.name


def test_second_run_emits_new_events(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1")])
    run_radar(_deps(client), store, radar)            # baseline
    client.rows = [_proc("P-1"), _proc("P-2"), _proc("P-3")]
    run_radar(_deps(client), store, radar)            # 2 nuevos reales
    feed = store.get_feed(radar.owner)
    kinds = [e["kind"] for e in feed]
    assert kinds.count("new") == 2
    assert kinds.count("baseline") == 1
    new_keys = {e["result_key"] for e in feed if e["kind"] == "new"}
    assert new_keys == {"P-2", "P-3"}


def test_state_change_emits_changed_event(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", estado="Abierto")])
    run_radar(_deps(client), store, radar)            # baseline
    client.rows = [_proc("P-1", estado="Adjudicado")]
    run_radar(_deps(client), store, radar)
    feed = store.get_feed(radar.owner)
    changed = [e for e in feed if e["kind"] == "changed_state"]
    assert len(changed) == 1
    assert changed[0]["prev_status"] == "Abierto"
    assert changed[0]["status"] == "Adjudicado"


def test_no_novelty_emits_no_events(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1")])
    run_radar(_deps(client), store, radar)            # baseline (1 evento)
    run_radar(_deps(client), store, radar)            # sin cambios → 0 eventos
    feed = store.get_feed(radar.owner)
    assert len(feed) == 1  # solo el baseline
