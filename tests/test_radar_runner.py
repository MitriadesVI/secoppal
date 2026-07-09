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
from app.core.radar.runner import MAX_SEEN_REFRESH, WINDOW_LIMIT, run_radar
from app.core.radar.store import RadarStore
from app.core.soql_builder import SoQLBuilder


class FakeClient:
    """SecopClient fake: devuelve filas controlables, no toca red."""

    def __init__(self, rows, count_value=None, lookup_rows=None, lookup_error=False):
        self.rows = rows
        self.count_value = count_value
        self.lookup_rows = lookup_rows if lookup_rows is not None else []
        self.lookup_error = lookup_error
        self.queries = []

    def query(self, dataset_id, soql):
        self.queries.append((dataset_id, soql))
        if " in(" in soql and (
            "id_del_proceso in(" in soql or "id_contrato in(" in soql
        ):
            if self.lookup_error:
                raise RuntimeError("lookup timeout")
            return list(self.lookup_rows)
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


def test_window_churn_does_not_open_novelty_or_feed_events(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1"), _proc("P-2")])
    run_radar(_deps(client), store, radar)            # baseline

    client.rows = [_proc("P-2")]                      # P-1 salió de la ventana
    client.lookup_rows = []                           # no confirma desaparición
    res = run_radar(_deps(client), store, radar)

    assert res.diff.new_count == 0
    assert res.diff.changed_count == 0
    assert len(res.diff.window_churn) == 1
    assert not res.diff.disappeared
    assert res.digest["has_novelty"] is False
    assert res.digest["disappeared_count"] == 0
    assert res.digest["window_churn_count"] == 1
    assert "desaparecid" not in res.digest["text"].lower()
    assert "Sin novedades" in res.digest["text"]
    feed = store.get_feed(radar.owner)
    assert [e["kind"] for e in feed] == ["baseline"]


def test_out_of_window_refresh_detects_state_change(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", estado="Abierto"), _proc("P-2")])
    run_radar(_deps(client), store, radar)            # baseline

    client.rows = [_proc("P-2")]
    client.lookup_rows = [_proc("P-1", estado="Adjudicado")]
    res = run_radar(_deps(client), store, radar)

    assert len(res.diff.changed_state) == 1
    assert res.diff.changed_state[0].key == "P-1"
    assert len(res.diff.window_churn) == 1
    assert not res.diff.disappeared
    assert res.digest["has_novelty"] is True
    assert "1 cambios de estado" in res.digest["text"]
    assert "desaparecid" not in res.digest["text"].lower()
    assert store.get_seen(radar.id)["P-1"]["status"] == "Adjudicado"
    changed = [e for e in store.get_feed(radar.owner) if e["kind"] == "changed_state"]
    assert len(changed) == 1
    assert changed[0]["result_key"] == "P-1"


def test_out_of_window_refresh_detects_value_change(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", valor="1000000"), _proc("P-2")])
    run_radar(_deps(client), store, radar)            # baseline

    client.rows = [_proc("P-2")]
    client.lookup_rows = [_proc("P-1", valor="9000000")]
    res = run_radar(_deps(client), store, radar)

    assert len(res.diff.changed_value) == 1
    assert res.diff.changed_value[0].key == "P-1"
    assert len(res.diff.window_churn) == 1
    assert res.digest["has_novelty"] is True
    assert "1 cambios de valor" in res.digest["text"]
    assert store.get_seen(radar.id)["P-1"]["value"] == "9000000"


def test_out_of_window_refresh_unchanged_stays_silent(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1"), _proc("P-2")])
    run_radar(_deps(client), store, radar)            # baseline

    client.rows = [_proc("P-2")]
    client.lookup_rows = [_proc("P-1")]
    res = run_radar(_deps(client), store, radar)

    assert len(res.diff.unchanged) == 2               # P-2 ventana + P-1 refresh
    assert len(res.diff.window_churn) == 1
    assert res.digest["has_novelty"] is False
    assert not res.diff.disappeared
    assert [e["kind"] for e in store.get_feed(radar.owner)] == ["baseline"]


def test_lookup_error_does_not_confirm_disappeared(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1"), _proc("P-2")])
    run_radar(_deps(client), store, radar)            # baseline

    client.rows = [_proc("P-2")]
    client.lookup_error = True
    res = run_radar(_deps(client), store, radar)

    assert len(res.diff.window_churn) == 1
    assert not res.diff.disappeared
    assert res.digest["has_novelty"] is False


def test_window_key_is_not_refreshed_by_lookup(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1")])
    run_radar(_deps(client), store, radar)

    client.rows = [_proc("P-1")]
    client.lookup_rows = [_proc("P-1", estado="Adjudicado")]
    res = run_radar(_deps(client), store, radar)

    lookup_queries = [q for _ds, q in client.queries if "id_del_proceso in(" in q]
    assert lookup_queries == []
    assert res.diff.changed_count == 0


def test_refresh_change_does_not_duplicate_events_on_next_run(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", estado="Abierto"), _proc("P-2")])
    run_radar(_deps(client), store, radar)

    client.rows = [_proc("P-2")]
    client.lookup_rows = [_proc("P-1", estado="Adjudicado")]
    run_radar(_deps(client), store, radar)
    run_radar(_deps(client), store, radar)

    changed = [e for e in store.get_feed(radar.owner) if e["kind"] == "changed_state"]
    assert len(changed) == 1


def test_duplicate_lookup_rows_do_not_duplicate_events(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", estado="Abierto"), _proc("P-2")])
    run_radar(_deps(client), store, radar)

    client.rows = [_proc("P-2")]
    client.lookup_rows = [
        _proc("P-1", estado="Adjudicado"),
        _proc("P-1", estado="Adjudicado"),
    ]
    res = run_radar(_deps(client), store, radar)

    assert len(res.diff.changed_state) == 1
    changed = [e for e in store.get_feed(radar.owner) if e["kind"] == "changed_state"]
    assert len(changed) == 1


def test_saved_valor_ordering_runs_with_recency_window(store):
    params = {
        "objeto": ["alimentacion_escolar"],
        "departamento_resolved": "ATLÁNTICO",
        "dataset": "procesos",
        "ordering_signal": "valor_desc",
    }
    frame_json = json.dumps(dataclasses.asdict(frame_from_params(params)))
    rid = store.create_radar(
        name="Radar PAE Valor", owner="u1",
        query_frame_json=frame_json, dataset="procesos",
    )
    radar = store.get_radar(rid)
    client = FakeClient([_proc("P-1")])

    run_radar(_deps(client), store, radar)

    soql = client.queries[0][1]
    assert "ORDER BY fecha_de_publicacion_del DESC, precio_base DESC" in soql
    assert "ORDER BY precio_base DESC, fecha_de_publicacion_del DESC" not in soql
    run_params = json.loads(store.get_runs(radar.id)[0].params_json)
    assert "ordering_signal" not in run_params
    assert run_params["limit"] == WINDOW_LIMIT


def test_seen_refresh_limit_is_explicit(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-0")])
    run_radar(_deps(client), store, radar)
    for i in range(MAX_SEEN_REFRESH + 5):
        store.upsert_seen(
            radar_id=radar.id, result_key=f"P-old-{i}", dataset="procesos",
            fields={
                "title": f"Old {i}", "entity": "ALCALDIA", "value": "1",
                "status": "Abierto", "urlproceso": "", "referencia": "",
                "contractor": "",
            },
            snapshot_hash="old", run_id="seed",
        )

    client.rows = [_proc("P-0")]
    res = run_radar(_deps(client), store, radar)

    assert res.digest["seen_refresh_checked"] == MAX_SEEN_REFRESH
    assert res.digest["seen_refresh_limited"] is True
    assert any("límite" in flag for flag in res.digest["risk_flags"])
