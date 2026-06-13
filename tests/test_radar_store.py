"""Tests de app/core/radar/store.py — persistencia del Radar."""
from __future__ import annotations

import json

import pytest

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
        execution_ms=42,
    )
    runs = store.get_runs(rid)
    assert len(runs) == 1
    assert runs[0].id == run_id
    assert runs[0].new_count == 3
    assert runs[0].soql == "SELECT 1"
    assert runs[0].execution_ms == 42
    assert runs[0].digest()["text"] == "hola"


def test_record_error(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_error(rid, "boom")
    radar = store.get_radar(rid)
    assert radar.last_error_message == "boom"
    assert radar.last_error_at


def test_update_last_run_clears_error(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_error(rid, "boom")
    store.update_last_run(rid)
    radar = store.get_radar(rid)
    assert radar.last_run_at
    assert radar.last_error_message is None


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
