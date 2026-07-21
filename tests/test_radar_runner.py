"""Tests e2e de app/core/radar/runner.py con un SecopClient fake.

Cubre el ciclo del MVP: 1ª corrida = todo nuevo; 2ª = 0 novedades;
3ª = cambio de estado detectado. Más la bandera de ventana (radar amplio).
"""
from __future__ import annotations

import ast
import dataclasses
import inspect
import json
import textwrap
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.core.formatter import Formatter
from app.core.query_frame import frame_from_params
from app.core.radar import runner as runner_module
from app.core.radar import scheduler
from app.core.radar.diff import classify
from app.core.radar.lease import LEASE_DURATION
from app.core.radar.runner import (
    MAX_SEEN_REFRESH,
    RADAR_ALREADY_RUNNING,
    RADAR_EMPTY,
    RADAR_GUARD_BLOCKED,
    RADAR_INVALID_FRAME,
    RADAR_LEASE_LOST,
    RADAR_NOT_FOUND,
    RADAR_OK,
    RADAR_PERMANENT_ERROR,
    RADAR_TIMEOUT,
    RADAR_TRANSIENT_ERROR,
    WINDOW_LIMIT,
    run_radar,
)
from app.core.radar.store import RadarStore
from app.core.soql_builder import SoQLBuilder


class FakeClient:
    """SecopClient fake: devuelve filas controlables, no toca red."""

    def __init__(
        self, rows, count_value=None, lookup_rows=None, lookup_error=False,
        query_error=None,
    ):
        self.rows = rows
        self.count_value = count_value
        self.lookup_rows = lookup_rows if lookup_rows is not None else []
        self.lookup_error = lookup_error
        self.query_error = query_error
        self.queries = []

    def query(self, dataset_id, soql):
        self.queries.append((dataset_id, soql))
        if " in(" in soql and (
            "id_del_proceso in(" in soql or "id_contrato in(" in soql
        ):
            if self.lookup_error:
                raise RuntimeError("lookup timeout")
            return list(self.lookup_rows)
        if self.query_error:
            raise self.query_error
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


def _make_phrase_radar(store):
    params = {
        "objeto": [["elaboracion", "actualizacion"]],
        "objeto_phrases": ["manual de contratacion"],
        "departamento_resolved": "ATLÁNTICO",
        "dataset": "procesos",
    }
    frame_json = json.dumps(dataclasses.asdict(frame_from_params(params)))
    rid = store.create_radar(
        name="Radar Manuales", owner="u1",
        query_frame_json=frame_json, dataset="procesos",
    )
    return store.get_radar(rid)


def test_first_run_all_new(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1"), _proc("P-2")])
    res = run_radar(_deps(client), store, radar)

    assert res.status == "ok"
    assert res.outcome == RADAR_OK
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


def test_empty_radar_run_is_outcome_not_error(store):
    radar = _make_radar(store)
    client = FakeClient([])
    res = run_radar(_deps(client), store, radar)

    assert res.status == "ok"
    assert res.outcome == RADAR_EMPTY
    assert res.reason == "no_results"
    assert res.diff.new_count == 0
    assert res.digest["has_novelty"] is False
    run = store.get_runs(radar.id)[0]
    assert run.outcome == RADAR_EMPTY
    assert run.reason == "no_results"
    stored = store.get_radar(radar.id)
    assert stored.failure_count == 0
    assert stored.last_failure_reason is None


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


# ── PhraseSpec validation in radar runner ─────────────────────────────────

def test_phrase_aware_radar_does_not_alert_unvalidated_window_candidates(store):
    radar = _make_phrase_radar(store)
    client = FakeClient([
        _proc("P-valid", objeto="Manual de contratacion municipal"),
        _proc("P-noise", objeto="Pavimentacion de vias urbanas"),
    ])

    res = run_radar(_deps(client), store, radar)

    assert {item.key for item in res.diff.new} == {"P-valid"}
    assert "P-noise" not in store.get_seen(radar.id)
    assert res.digest["new_count"] == 1


def test_phrase_aware_radar_keeps_validated_candidates(store):
    radar = _make_phrase_radar(store)
    client = FakeClient([
        _proc("P-direct", objeto="Actualizacion del manual de contratacion"),
    ])

    res = run_radar(_deps(client), store, radar)

    assert res.diff.new_count == 1
    assert res.diff.new[0].key == "P-direct"
    assert set(store.get_seen(radar.id)) == {"P-direct"}


def test_phrase_aware_refresh_does_not_introduce_semantic_noise(store):
    radar = _make_phrase_radar(store)
    client = FakeClient([
        _proc("P-old", estado="Abierto", objeto="Manual de contratacion municipal"),
    ])
    run_radar(_deps(client), store, radar)

    client.rows = []
    client.lookup_rows = [
        _proc("P-old", estado="Adjudicado", objeto="Pavimentacion de vias urbanas"),
    ]
    res = run_radar(_deps(client), store, radar)

    assert res.diff.changed_count == 0
    assert len(res.diff.window_churn) == 1
    assert res.digest["has_novelty"] is False
    changed = [
        e for e in store.get_feed(radar.owner)
        if e["kind"] in {"changed_state", "changed_value"}
    ]
    assert changed == []


def test_phrase_aware_refresh_detects_valid_seen_change(store):
    radar = _make_phrase_radar(store)
    client = FakeClient([
        _proc("P-old", estado="Abierto", objeto="Manual de contratacion municipal"),
    ])
    run_radar(_deps(client), store, radar)

    client.rows = []
    client.lookup_rows = [
        _proc("P-old", estado="Adjudicado", objeto="Manual de contratacion municipal"),
    ]
    res = run_radar(_deps(client), store, radar)

    assert len(res.diff.changed_state) == 1
    assert res.diff.changed_state[0].key == "P-old"
    assert store.get_seen(radar.id)["P-old"]["status"] == "Adjudicado"


# ── Outcome / Reason + backoff ───────────────────────────────────────────

def test_timeout_outcome_sets_backoff_without_events_or_seen_loss(store):
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1")])
    run_radar(_deps(client), store, radar)
    before_seen = store.get_seen(radar.id)
    before_feed = store.get_feed(radar.owner)

    timeout_client = FakeClient([], query_error=TimeoutError("timed out"))
    res = run_radar(_deps(timeout_client), store, store.get_radar(radar.id))

    assert res.status == "error"
    assert res.outcome == RADAR_TIMEOUT
    assert res.reason == "socrata_timeout"
    assert store.get_seen(radar.id) == before_seen
    assert store.get_feed(radar.owner) == before_feed
    stored = store.get_radar(radar.id)
    assert stored.failure_count == 1
    assert stored.last_failure_reason == "socrata_timeout"
    assert stored.next_run_after
    assert stored.is_enabled
    run = store.get_runs(radar.id)[0]
    assert run.outcome == RADAR_TIMEOUT
    assert run.reason == "socrata_timeout"


def test_transient_error_outcome_sets_backoff(store):
    radar = _make_radar(store)
    client = FakeClient([], query_error=RuntimeError("Socrata 503"))
    res = run_radar(_deps(client), store, radar)

    assert res.status == "error"
    assert res.outcome == RADAR_TRANSIENT_ERROR
    assert res.reason == "socrata_query_error"
    stored = store.get_radar(radar.id)
    assert stored.failure_count == 1
    assert stored.next_run_after
    assert stored.is_enabled


def test_invalid_frame_outcome_counts_permanent_failure(store):
    rid = store.create_radar(
        name="Roto", owner="u1", query_frame_json="{no-json", dataset="procesos",
    )
    radar = store.get_radar(rid)
    res = run_radar(_deps(FakeClient([])), store, radar)

    assert res.status == "error"
    assert res.outcome == RADAR_INVALID_FRAME
    assert res.reason == "invalid_query_frame_json"
    stored = store.get_radar(rid)
    assert stored.failure_count == 1
    assert stored.last_failure_reason == "invalid_query_frame_json"
    assert stored.next_run_after


def test_guard_blocked_outcome_counts_permanent_failure(store):
    rid = store.create_radar(name="Global", owner="u1", query_frame_json="{}")
    radar = store.get_radar(rid)
    res = run_radar(_deps(FakeClient([])), store, radar)

    assert res.status == "error"
    assert res.outcome == RADAR_GUARD_BLOCKED
    assert res.reason == "guard_no_scope_or_topic"
    stored = store.get_radar(rid)
    assert stored.failure_count == 1
    assert stored.last_failure_reason == "guard_no_scope_or_topic"


def test_repeated_permanent_failures_mark_attention_and_pause(store):
    rid = store.create_radar(name="Global", owner="u1", query_frame_json="{}")

    for _ in range(3):
        run_radar(_deps(FakeClient([])), store, store.get_radar(rid))

    stored = store.get_radar(rid)
    assert stored.failure_count == 3
    assert stored.permanent_failure_streak == 3
    assert stored.needs_attention == 1
    assert not stored.is_enabled


# ── P1B-Preflight 3 · recuperación técnica ≠ reconocimiento humano ────────
#
# El ciclo completo por run_radar, no solo la sentencia: tres permanentes
# alertan y pausan; el operador reactiva a mano; la corrida siguiente limpia
# los contadores pero deja la alerta en pie, porque nadie confirmó todavía que
# la causa se entendiera.


def _broken_builder_deps(client):
    """Deps cuyo SoQL revienta: fallo *permanente* (soql_build_error).

    Un error del cliente nunca es permanente (timeout/503 son transitorios),
    así que la vía para alternar permanente↔éxito sobre el mismo radar —sin
    romper su frame, que lo dejaría permanentemente roto— es el builder.
    """
    deps = _deps(client)
    deps.soql_builder = SimpleNamespace(
        build=_raise_build_error, build_count=_raise_build_error,
    )
    return deps


def _raise_build_error(*_args, **_kwargs):
    raise ValueError("builder roto")


def _alert_and_pause(store, rid) -> None:
    for _ in range(3):
        run_radar(_broken_builder_deps(FakeClient([])), store, store.get_radar(rid))
    stored = store.get_radar(rid)
    assert (stored.needs_attention, stored.is_enabled) == (1, False)
    assert stored.permanent_failure_streak == 3


def test_empty_run_after_reactivation_preserves_attention(store):
    """Un radar_empty tampoco reconoce la alerta: no encontró nada, y ya."""
    radar = _make_radar(store)
    _alert_and_pause(store, radar.id)
    store.set_enabled(radar.id, True)

    res = run_radar(_deps(FakeClient([])), store, store.get_radar(radar.id))

    stored = store.get_radar(radar.id)
    assert res.outcome == RADAR_EMPTY
    assert stored.needs_attention == 1
    assert stored.is_enabled
    assert stored.failure_count == 0
    assert stored.permanent_failure_streak == 0
    assert stored.next_run_after is None


def test_permanent_failure_after_recovery_restarts_streak_from_zero(store):
    """La racha se reinicia con el éxito, pero la alerta no vuelve a cero.

    Un permanente aislado tras recuperarse no debe re-pausar el radar: el
    umbral cuenta desde el último éxito, no desde la última alerta.
    """
    radar = _make_radar(store)
    _alert_and_pause(store, radar.id)
    store.set_enabled(radar.id, True)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, store.get_radar(radar.id))
    assert store.get_radar(radar.id).permanent_failure_streak == 0

    run_radar(_broken_builder_deps(FakeClient([])), store, store.get_radar(radar.id))

    stored = store.get_radar(radar.id)
    assert stored.failure_count == 1
    assert stored.permanent_failure_streak == 1
    assert stored.needs_attention == 1       # nunca se limpió
    assert stored.is_enabled                 # 1 < 3: no alcanzó el umbral otra vez


def test_transient_failure_after_attention_does_not_clear_it(store):
    """Ningún fallo reconoce la alerta; tampoco el que corta la racha."""
    radar = _make_radar(store)
    _alert_and_pause(store, radar.id)
    store.set_enabled(radar.id, True)

    run_radar(_deps(FakeClient([], query_error=RuntimeError("Socrata 503"))),
              store, store.get_radar(radar.id))

    stored = store.get_radar(radar.id)
    assert stored.needs_attention == 1
    assert stored.permanent_failure_streak == 0   # el transitorio corta la racha
    assert stored.failure_count == 4


def test_success_resets_failure_state(store):
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([], query_error=RuntimeError("Socrata 503"))), store, radar)
    failed = store.get_radar(radar.id)
    assert failed.failure_count == 1

    res = run_radar(_deps(FakeClient([_proc("P-1")])), store, failed)

    assert res.status == "ok"
    stored = store.get_radar(radar.id)
    assert stored.failure_count == 0
    assert stored.permanent_failure_streak == 0
    assert stored.last_failure_reason is None
    assert stored.next_run_after is None
    assert stored.needs_attention == 0


def test_empty_outcome_resets_failure_state_after_prior_failure(store):
    """radar_empty también reinicia ambos contadores (Política C del contrato)."""
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([], query_error=RuntimeError("Socrata 503"))), store, radar)
    failed = store.get_radar(radar.id)
    assert failed.failure_count == 1

    res = run_radar(_deps(FakeClient([])), store, failed)

    assert res.outcome == RADAR_EMPTY
    stored = store.get_radar(radar.id)
    assert stored.failure_count == 0
    assert stored.permanent_failure_streak == 0
    assert stored.last_failure_reason is None
    assert stored.next_run_after is None
    assert stored.needs_attention == 0


# ── P1A · exclusión mutua interproceso (run lease) ────────────────────────

class ReentrantClient(FakeClient):
    """Dispara un segundo ejecutor *mientras* el primero está en Socrata.

    Es el escenario real (scheduler vs. «Ejecutar ahora») hecho determinista:
    la reentrada ocurre dentro de la ventana del lease del primero, sin sleeps
    ni carreras de reloj.
    """

    def __init__(self, rows, *, on_first_query):
        super().__init__(rows)
        self._on_first_query = on_first_query
        self.reentrant_result = None
        self._fired = False

    def query(self, dataset_id, soql):
        if not self._fired:
            self._fired = True
            self.reentrant_result = self._on_first_query()
        return super().query(dataset_id, soql)


def _acquire_foreign_lease(store, radar_id):
    """Otro proceso ya corriendo este radar."""
    claim = store.try_acquire_run_lease(radar_id)
    assert claim.acquired
    return claim.token


def test_already_running_skips_without_touching_socrata(store):
    radar = _make_radar(store)
    _acquire_foreign_lease(store, radar.id)
    client = FakeClient([_proc("P-1")])

    res = run_radar(_deps(client), store, radar)

    assert res.status == "skipped"
    assert res.outcome == RADAR_ALREADY_RUNNING
    assert res.reason == "already_running"
    assert client.queries == []          # no consultó SECOP
    assert res.digest["has_novelty"] is False


def test_already_running_leaves_no_trace(store):
    """No es un error: ni corrida, ni fallo, ni backoff, ni seen, ni eventos."""
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)  # baseline real
    before = store.get_radar(radar.id)
    before_seen = store.get_seen(radar.id)
    before_feed = store.get_feed(radar.owner)
    before_runs = len(store.get_runs(radar.id))

    _acquire_foreign_lease(store, radar.id)
    res = run_radar(_deps(FakeClient([_proc("P-1"), _proc("P-2")])), store, radar)

    assert res.outcome == RADAR_ALREADY_RUNNING
    after = store.get_radar(radar.id)
    assert len(store.get_runs(radar.id)) == before_runs   # sin radar_run
    assert after.failure_count == before.failure_count    # sin failure_count
    assert after.permanent_failure_streak == before.permanent_failure_streak
    assert after.next_run_after == before.next_run_after  # sin backoff
    assert after.last_run_at == before.last_run_at        # sin last_run_at
    assert after.needs_attention == before.needs_attention
    assert after.is_enabled
    assert store.get_seen(radar.id) == before_seen        # seen intacto
    assert store.get_feed(radar.owner) == before_feed     # feed intacto


def test_second_executor_during_first_run_gets_already_running(store):
    """Dos ejecutores sobre el mismo radar: solo uno llega a Socrata."""
    radar = _make_radar(store)
    second_client = FakeClient([_proc("P-1")])

    def second_executor():
        return run_radar(_deps(second_client), store, store.get_radar(radar.id))

    first_client = ReentrantClient([_proc("P-1")], on_first_query=second_executor)
    first = run_radar(_deps(first_client), store, radar)
    second = first_client.reentrant_result

    assert first.status == "ok"
    assert second.status == "skipped"
    assert second.outcome == RADAR_ALREADY_RUNNING
    assert second_client.queries == []              # el segundo no tocó SECOP
    assert len(store.get_runs(radar.id)) == 1       # una sola corrida
    assert store.get_radar(radar.id).failure_count == 0
    # un solo evento baseline: el segundo no duplicó nada
    feed = store.get_feed(radar.owner)
    assert [e["kind"] for e in feed] == ["baseline"]


def test_concurrent_executors_do_not_duplicate_events(store):
    """El segundo ejecutor no reemite los 'new' que el primero está calculando."""
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)  # baseline

    second_client = FakeClient([_proc("P-1"), _proc("P-2")])

    def second_executor():
        return run_radar(_deps(second_client), store, store.get_radar(radar.id))

    first_client = ReentrantClient(
        [_proc("P-1"), _proc("P-2")], on_first_query=second_executor,
    )
    run_radar(_deps(first_client), store, radar)

    assert first_client.reentrant_result.outcome == RADAR_ALREADY_RUNNING
    new_events = [e for e in store.get_feed(radar.owner) if e["kind"] == "new"]
    assert len(new_events) == 1                     # P-2 anunciado una sola vez
    assert new_events[0]["result_key"] == "P-2"


def test_deleted_radar_yields_not_found_not_already_running(store):
    """Carrera legítima: el llamador ya resolvió el Radar y luego se borró.

    No hay ningún dueño con quien competir, así que no puede confundirse con
    already_running: outcome, status y reason propios.
    """
    radar = _make_radar(store)
    stale = store.get_radar(radar.id)   # snapshot antes del borrado
    store.delete_radar(radar.id)
    client = FakeClient([_proc("P-1")])

    res = run_radar(_deps(client), store, stale)

    assert res.status == "not_found"
    assert res.outcome == RADAR_NOT_FOUND
    assert res.reason == RADAR_NOT_FOUND
    assert res.outcome != RADAR_ALREADY_RUNNING
    assert client.queries == []                    # no llamó a Socrata
    assert store.get_radar(radar.id) is None        # sigue borrado, nada resucitó


# ── P1B-1 · un radar archivado no corre, ni siquiera a mitad de vuelo ─────

def test_archived_radar_does_not_run_end_to_end(store):
    """Extremo a extremo: archivar basta para que nadie lo ejecute.

    El radar se archiva *después* de resolverse, que es el caso real: el
    scheduler ya tiene su lista en memoria cuando el usuario archiva desde la UI.
    """
    radar = _make_radar(store)
    stale = store.get_radar(radar.id)
    store.archive_radar(radar.id)
    client = FakeClient([_proc("P-1")])

    res = run_radar(_deps(client), store, stale)

    assert res.outcome == RADAR_NOT_FOUND
    assert client.queries == []                     # no gastó una llamada a SECOP
    assert store.get_runs(radar.id) == []


def test_archiving_during_the_run_prevents_materialization(store):
    """La carrera que motiva el fence, extremo a extremo.

    El ejecutor ya tiene el lease y está dentro de Socrata cuando otro proceso
    archiva. Un chequeo previo no habría visto nada: cuando miró, el radar estaba
    vivo. Solo el predicado dentro del UPDATE de renovación lo detiene.
    """
    radar = _make_radar(store)
    client = ReentrantClient(
        [_proc("P-1")], on_first_query=lambda: store.archive_radar(radar.id)
    )

    res = run_radar(_deps(client), store, store.get_radar(radar.id))

    assert res.outcome == RADAR_LEASE_LOST           # no disponible, sin taxonomía nueva
    assert store.get_runs(radar.id) == []            # ni un run huérfano
    assert store.get_seen(radar.id) == {}            # ni seen adelantado
    assert store.get_feed(radar.owner) == []         # ni eventos a nadie


def test_lease_released_after_successful_run(store):
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)

    stored = store.get_radar(radar.id)
    assert stored.run_lease_token is None
    assert stored.run_lease_expires_at is None


def test_lease_free_after_run_allows_next_executor(store):
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)
    res = run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)

    assert res.status == "ok"           # la segunda corrida secuencial sí corre
    assert res.outcome in (RADAR_OK, RADAR_EMPTY)


def _lease_is_free(store, radar_id) -> bool:
    return store.get_radar(radar_id).run_lease_token is None


def test_lease_released_on_unexpected_exception(store, monkeypatch):
    radar = _make_radar(store)

    def boom(*a, **kw):
        raise RuntimeError("fallo inesperado en el diff")

    monkeypatch.setattr("app.core.radar.runner.classify", boom)
    with pytest.raises(RuntimeError):
        run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)

    assert _lease_is_free(store, radar.id)


def test_lease_released_on_permanent_error(store, monkeypatch):
    radar = _make_radar(store)
    deps = _deps(FakeClient([_proc("P-1")]))

    def boom(*a, **kw):
        raise ValueError("campo desconocido")

    monkeypatch.setattr(deps.soql_builder, "build", boom)
    res = run_radar(deps, store, radar)

    assert res.outcome == RADAR_PERMANENT_ERROR
    assert _lease_is_free(store, radar.id)


@pytest.mark.parametrize("scenario", ["ok", "empty", "invalid_frame", "guard_blocked",
                                      "timeout", "transient_error"])
def test_lease_released_on_every_outcome(store, scenario):
    """Pase lo que pase, el radar queda libre para el siguiente ejecutor."""
    expected = {
        "ok": RADAR_OK,
        "empty": RADAR_EMPTY,
        "invalid_frame": RADAR_INVALID_FRAME,
        "guard_blocked": RADAR_GUARD_BLOCKED,
        "timeout": RADAR_TIMEOUT,
        "transient_error": RADAR_TRANSIENT_ERROR,
    }[scenario]

    if scenario == "invalid_frame":
        rid = store.create_radar(name="Roto", owner="u1", query_frame_json="{no-json")
        radar, client = store.get_radar(rid), FakeClient([])
    elif scenario == "guard_blocked":
        rid = store.create_radar(name="Global", owner="u1", query_frame_json="{}")
        radar, client = store.get_radar(rid), FakeClient([])
    else:
        radar = _make_radar(store)
        client = {
            "ok": FakeClient([_proc("P-1")]),
            "empty": FakeClient([]),
            "timeout": FakeClient([], query_error=TimeoutError("timed out")),
            "transient_error": FakeClient([], query_error=RuntimeError("Socrata 503")),
        }[scenario]

    res = run_radar(_deps(client), store, radar)

    assert res.outcome == expected
    assert _lease_is_free(store, radar.id)


def test_lost_lease_aborts_without_persisting(store):
    """A pierde el lease durante el fetch: no escribe nada y no pisa a B."""
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)  # baseline
    before = store.get_radar(radar.id)
    before_seen = store.get_seen(radar.id)
    before_feed = store.get_feed(radar.owner)
    before_runs = len(store.get_runs(radar.id))
    stolen: dict = {}

    def steal_lease():
        """B recupera el lease vencido de A mientras A está en Socrata."""
        expired = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
        claim = store.try_acquire_run_lease(radar.id, now=expired)
        assert claim.acquired
        stolen["token"] = claim.token
        return claim

    client = ReentrantClient([_proc("P-1"), _proc("P-2")], on_first_query=steal_lease)
    res = run_radar(_deps(client), store, radar)

    assert res.status == "skipped"
    assert res.outcome == RADAR_LEASE_LOST
    assert res.reason == "lease_lost"
    # nada persistido por el dueño vencido
    after = store.get_radar(radar.id)
    assert len(store.get_runs(radar.id)) == before_runs
    assert store.get_seen(radar.id) == before_seen
    assert store.get_feed(radar.owner) == before_feed
    assert after.failure_count == before.failure_count
    assert after.permanent_failure_streak == before.permanent_failure_streak
    assert after.next_run_after == before.next_run_after
    assert after.last_run_at == before.last_run_at
    # y el lease de B sigue siendo de B: el finally de A no lo liberó
    assert after.run_lease_token == stolen["token"]


def test_lost_lease_on_failure_path_does_not_record_failure(store):
    """Un dueño vencido tampoco escribe fallos ni backoff sobre el radar de B."""
    radar = _make_radar(store)
    stolen: dict = {}

    def steal_lease():
        expired = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
        claim = store.try_acquire_run_lease(radar.id, now=expired)
        stolen["token"] = claim.token
        raise TimeoutError("timed out")   # el fetch de A falla tras perder el lease

    client = ReentrantClient([], on_first_query=steal_lease)
    res = run_radar(_deps(client), store, radar)

    assert res.outcome == RADAR_LEASE_LOST
    after = store.get_radar(radar.id)
    assert store.get_runs(radar.id) == []
    assert after.failure_count == 0
    assert after.permanent_failure_streak == 0
    assert after.next_run_after is None
    assert after.run_lease_token == stolen["token"]


def test_lost_lease_on_invalid_frame_path_does_not_persist(store, monkeypatch):
    """El path de QueryFrame inválido también debe revalidar antes de escribir.

    No hay red en este path, pero la validación es responsabilidad de un
    llamador (aquí, un monkeypatch) — el mismo invariante de fencing aplica:
    si A perdió el lease antes de llegar a record_run/record_failure, no
    puede persistir ni pisar el lease de B.
    """
    rid = store.create_radar(name="Roto", owner="u1", query_frame_json="{no-json")
    radar = store.get_radar(rid)
    stolen: dict = {}
    original_validate = runner_module._validate_frame_json

    def steal_lease_then_validate(radar_arg):
        """B recupera el lease vencido de A durante la validación del frame."""
        expired = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
        claim = store.try_acquire_run_lease(radar_arg.id, now=expired)
        assert claim.acquired
        stolen["token"] = claim.token
        return original_validate(radar_arg)

    monkeypatch.setattr(runner_module, "_validate_frame_json", steal_lease_then_validate)

    res = run_radar(_deps(FakeClient([])), store, radar)

    assert res.outcome == RADAR_LEASE_LOST
    assert res.status == "skipped"
    after = store.get_radar(radar.id)
    assert store.get_runs(radar.id) == []          # sin radar_run
    assert after.failure_count == 0
    assert after.permanent_failure_streak == 0
    assert after.next_run_after is None
    assert after.needs_attention == 0
    assert after.run_lease_token == stolen["token"]  # el lease de B sigue siendo de B
    assert after.is_enabled


def test_force_manual_run_cannot_bypass_lease(store):
    """force salta frecuencia/backoff, nunca la exclusión mutua."""
    radar = _make_radar(store)
    store.record_failure(
        radar.id, reason="socrata_timeout", message="boom",
        next_run_after="2999-01-01T00:00:00+00:00",
    )
    _acquire_foreign_lease(store, radar.id)
    client = FakeClient([_proc("P-1")])

    summary = scheduler.run_due_radars(_deps(client), store, force=True)

    assert client.queries == []
    assert summary.ran == 0


def test_scheduler_and_manual_share_the_same_claim(store):
    """El scheduler y «Ejecutar ahora» compiten por el mismo mecanismo."""
    radar = _make_radar(store)
    manual_client = FakeClient([_proc("P-1")])

    def manual_executor():
        return run_radar(_deps(manual_client), store, store.get_radar(radar.id))

    scheduler_client = ReentrantClient([_proc("P-1")], on_first_query=manual_executor)
    summary = scheduler.run_due_radars(_deps(scheduler_client), store, force=True)

    assert summary.ran == 1                        # el scheduler corrió
    assert scheduler_client.reentrant_result.outcome == RADAR_ALREADY_RUNNING
    assert manual_client.queries == []             # el manual no tocó SECOP
    assert len(store.get_runs(radar.id)) == 1


# ── P1B-Preflight 1 · la corrida se materializa entera o no se materializa ──

class _Boom(RuntimeError):
    """Fallo inyectado dentro de la transacción de materialización."""


def _fail_at(store, method: str, phase: str, monkeypatch):
    """Hace reventar la materialización en ``phase``, con el store real."""
    original = getattr(store, method)

    def hook(current: str) -> None:
        if current == phase:
            raise _Boom(f"crash en {phase}")

    def patched(**kwargs):
        return original(**kwargs, _fault_hook=hook)

    monkeypatch.setattr(store, method, patched)


def _all_state(store, radar):
    return (
        len(store.get_runs(radar.id)),
        store.get_seen(radar.id),
        store.get_feed(radar.owner),
        dataclasses.asdict(store.get_radar(radar.id)),
    )


@pytest.mark.parametrize("phase", [
    "after_run_insert", "after_seen_upserts", "after_event_inserts",
    "after_radar_update", "before_commit",
])
def test_crash_while_materializing_run_leaves_no_trace(store, monkeypatch, phase):
    """Una corrida real que revienta a mitad de la escritura no deja nada."""
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", estado="Abierto")])
    run_radar(_deps(client), store, radar)          # baseline sólido
    before = _all_state(store, radar)

    client.rows = [_proc("P-1", estado="Adjudicado"), _proc("P-2")]
    _fail_at(store, "materialize_successful_run", phase, monkeypatch)
    with pytest.raises(_Boom):
        run_radar(_deps(client), store, store.get_radar(radar.id))

    assert _all_state(store, radar) == before
    # y el lease quedó libre: el finally sigue haciendo su trabajo
    assert store.get_radar(radar.id).run_lease_token is None


def test_crash_on_empty_run_preserves_alerted_state_and_frees_lease(store, monkeypatch):
    """Preflight 1 × 3, extremo a extremo: el caso más fácil de romper.

    Un ``radar_empty`` casi no escribe —ni seen ni eventos, solo el run y el
    estado— así que es donde un UPDATE de éxito suelto fuera de la transacción
    pasaría inadvertido: no habría novedad perdida que lo delatara, solo una
    alerta humana borrada por una corrida que ni siquiera llegó a commitear.
    El radar está reactivado (``enabled=1``) y alertado (``needs_attention=1``),
    que es el estado exacto donde ambos ejes pueden corromperse por separado.
    """
    radar = _make_radar(store)
    _alert_and_pause(store, radar.id)
    store.set_enabled(radar.id, True)
    before = _all_state(store, radar)
    assert before[3]["needs_attention"] == 1
    assert before[3]["enabled"] == 1
    assert before[3]["permanent_failure_streak"] == 3

    _fail_at(store, "materialize_successful_run", "before_commit", monkeypatch)
    with pytest.raises(_Boom):
        run_radar(_deps(FakeClient([])), store, store.get_radar(radar.id))

    after = _all_state(store, radar)
    assert after == before                       # rollback total: ni el run vacío
    assert after[3]["needs_attention"] == 1      # la alerta no se fue con el crash
    assert after[3]["enabled"] == 1              # ni la reactivación del operador
    assert after[3]["failure_count"] == 3        # ni los contadores se limpiaron
    assert store.get_radar(radar.id).run_lease_token is None  # lease libre


def test_crash_after_seen_does_not_swallow_the_novelty(store, monkeypatch):
    """El bug original visto desde el runner: la novedad sobrevive al crash.

    Tras el rollback, la corrida siguiente vuelve a ver P-1 como cambio de
    estado y lo anuncia. Con escrituras autocommit separadas, el seen habría
    quedado en 'Adjudicado' y ese cambio no se habría anunciado nunca.
    """
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1", estado="Abierto")])
    run_radar(_deps(client), store, radar)

    client.rows = [_proc("P-1", estado="Adjudicado")]
    _fail_at(store, "materialize_successful_run", "after_seen_upserts", monkeypatch)
    with pytest.raises(_Boom):
        run_radar(_deps(client), store, store.get_radar(radar.id))

    assert store.get_seen(radar.id)["P-1"]["status"] == "Abierto"   # no se adelantó
    monkeypatch.undo()

    res = run_radar(_deps(client), store, store.get_radar(radar.id))

    assert len(res.diff.changed_state) == 1                         # se vuelve a ver
    assert res.diff.changed_state[0].key == "P-1"
    changed = [e for e in store.get_feed(radar.owner) if e["kind"] == "changed_state"]
    assert len(changed) == 1                                        # y se anuncia


@pytest.mark.parametrize("phase", ["after_run_insert", "after_failure_update",
                                   "before_commit"])
def test_crash_while_materializing_failure_leaves_no_trace(store, monkeypatch, phase):
    """Ni la corrida fallida ni su backoff sobreviven por separado."""
    radar = _make_radar(store)
    client = FakeClient([], query_error=TimeoutError("timed out"))
    _fail_at(store, "materialize_failed_run", phase, monkeypatch)

    with pytest.raises(_Boom):
        run_radar(_deps(client), store, radar)

    stored = store.get_radar(radar.id)
    assert store.get_runs(radar.id) == []
    assert stored.failure_count == 0
    assert stored.last_failure_reason is None
    assert stored.next_run_after is None
    assert stored.run_lease_token is None


def test_successful_run_is_materialized_in_one_transaction(store):
    """Run, seen, eventos y estado quedan consistentes entre sí."""
    radar = _make_radar(store)
    client = FakeClient([_proc("P-1")])
    run_radar(_deps(client), store, radar)                # baseline
    client.rows = [_proc("P-1"), _proc("P-2")]

    res = run_radar(_deps(client), store, store.get_radar(radar.id))

    runs = store.get_runs(radar.id)
    assert len(runs) == 2
    seen = store.get_seen(radar.id)
    assert set(seen) == {"P-1", "P-2"}
    # el run_id del evento y el del seen nuevo son el de esta misma corrida
    new_events = [e for e in store.get_feed(radar.owner) if e["kind"] == "new"]
    assert [e["run_id"] for e in new_events] == [res.run_id]
    assert seen["P-2"]["first_run_id"] == res.run_id
    assert store.get_radar(radar.id).last_run_at


def test_all_network_finishes_before_the_transaction_opens(store):
    """Ninguna consulta a SECOP ocurre con la transacción abierta.

    Si quedara red dentro, un timeout de Socrata sostendría el lock de
    escritura de SQLite y bloquearía a todos los demás radares.
    """
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)   # baseline + seen

    client = FakeClient([_proc("P-2")], lookup_rows=[_proc("P-1", estado="Adjudicado")])
    original = store.materialize_successful_run
    seen_at_materialize: dict = {}

    def spy(**kwargs):
        seen_at_materialize["queries"] = len(client.queries)
        return original(**kwargs)

    store.materialize_successful_run = spy
    run_radar(_deps(client), store, store.get_radar(radar.id))

    assert client.queries                                     # hubo red…
    assert seen_at_materialize["queries"] == len(client.queries)  # …toda antes


def test_build_events_is_pure(store):
    """La construcción de eventos no toca el store, radar ni run_id.

    No recibe ``radar`` ni ``run_id``: los EventRecord que produce no llevan
    identidad de radar/corrida/dueño, así que este caller no tiene ni cómo
    aportarla — la deriva únicamente la transacción de materialización.
    """
    params = set(inspect.signature(runner_module._build_events).parameters)
    assert not params & {"store", "conn", "deps", "secop_client", "radar", "run_id"}

    tree = ast.parse(textwrap.dedent(inspect.getsource(runner_module._build_events)))
    called = {
        node.func.attr for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }
    assert not called & {"record_events", "record_run", "upsert_seen", "execute"}

    # y de hecho devuelve los eventos en vez de escribirlos
    radar = _make_radar(store)
    events = runner_module._build_events(
        "procesos", "jbjy-vk9h", classify([], {}, "jbjy-vk9h"), [], 0, is_first_run=True,
    )
    assert [e.kind for e in events] == ["baseline"]
    assert store.get_feed(radar.owner) == []          # nada persistido


@pytest.mark.parametrize("state", ["already_running", "lease_lost", "not_found"])
def test_control_states_persist_nothing(store, state):
    """Los estados de control no son corridas: no dejan rastro alguno."""
    radar = _make_radar(store)
    run_radar(_deps(FakeClient([_proc("P-1")])), store, radar)   # baseline real
    before = _all_state(store, radar)

    if state == "already_running":
        _acquire_foreign_lease(store, radar.id)
        res = run_radar(_deps(FakeClient([_proc("P-2")])), store, radar)
        expected = RADAR_ALREADY_RUNNING
    elif state == "lease_lost":
        def steal():
            expired = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
            return store.try_acquire_run_lease(radar.id, now=expired)
        client = ReentrantClient([_proc("P-1"), _proc("P-2")], on_first_query=steal)
        res = run_radar(_deps(client), store, radar)
        expected = RADAR_LEASE_LOST
    else:
        store.delete_radar(radar.id)
        res = run_radar(_deps(FakeClient([_proc("P-2")])), store, radar)
        expected = RADAR_NOT_FOUND

    assert res.outcome == expected
    assert res.run_id == ""
    if state == "not_found":
        assert store.get_radar(radar.id) is None     # el borrado no resucita nada
        return
    runs, seen, feed, radar_state = _all_state(store, radar)
    assert (runs, seen, feed) == before[:3]
    for field_name in ("failure_count", "permanent_failure_streak", "next_run_after",
                       "last_run_at", "needs_attention", "enabled"):
        assert radar_state[field_name] == before[3][field_name]


# ── proyección inmutable del evento (R2B-1B1) ──────────────────────────────
# Cada evento congela los datos de la fila TAL COMO SE OBSERVÓ al emitirlo. La
# notificación los leerá de aquí y nunca de radar_seen_results, que para cuando
# el mensaje se arma ya describe el estado de HOY, no el del evento.

_MODALIDAD = "Licitación pública"
_CIERRE = "2026-08-20T17:00:00.000"


def _proc_meta(pid, estado="Abierto", valor="1000000",
               modalidad=_MODALIDAD, cierre=_CIERRE):
    """Proceso con la metadata que SECOP II expone para procesos."""
    row = _proc(pid, estado=estado, valor=valor)
    row["modalidad_de_contratacion"] = modalidad
    row["fecha_de_recepcion_de"] = cierre
    return row


def _cont(cid, estado="En ejecución", valor="5000000",
          contratista="UNION TEMPORAL PAE 2026", firma="2026-03-11T00:00:00.000"):
    """Fila cruda de contratos (jbjy-vk9h)."""
    return {
        "id_contrato": cid,
        "referencia_del_contrato": f"RC-{cid}",
        "objeto_del_contrato": "Suministro PAE escolar",
        "nombre_entidad": "ALCALDIA DE BARRANQUILLA",
        "departamento": "ATLÁNTICO",
        "valor_del_contrato": valor,
        "estado_contrato": estado,
        "modalidad_de_contratacion": "Contratación directa",
        "proveedor_adjudicado": contratista,
        "fecha_de_firma": firma,
        "urlproceso": f"http://secop/{cid}",
    }


def _make_contratos_radar(store):
    params = {"objeto": ["alimentacion_escolar"],
              "departamento_resolved": "ATLÁNTICO", "dataset": "contratos"}
    frame_json = json.dumps(dataclasses.asdict(frame_from_params(params)))
    rid = store.create_radar(
        name="Radar contratos PAE", owner="u1",
        query_frame_json=frame_json, dataset="contratos",
    )
    return store.get_radar(rid)


def _event_by(feed, kind, key):
    hits = [e for e in feed if e["kind"] == kind and e["result_key"] == key]
    assert len(hits) == 1, f"esperaba 1 evento {kind}/{key}, hubo {len(hits)}"
    return hits[0]


def test_new_event_persists_modality_and_closing_date(store):
    """15 · un 'new' de procesos congela modalidad y cierre."""
    radar = _make_radar(store)
    client = FakeClient([_proc_meta("P-1")])
    run_radar(_deps(client), store, radar)              # baseline
    client.rows = [_proc_meta("P-1"), _proc_meta("P-2")]
    run_radar(_deps(client), store, radar)              # P-2 es nuevo

    ev = _event_by(store.get_feed(radar.owner), "new", "P-2")
    assert ev["modality"] == _MODALIDAD
    assert ev["closing_date"] == _CIERRE
    # Procesos no tiene estos dos hechos: se guardan como "no sabemos".
    assert ev["contractor"] is None
    assert ev["signature_date"] is None


def test_changed_state_event_persists_current_metadata_and_prev_status(store):
    """16 · el cambio de estado conserva prev_status y trae la metadata actual."""
    radar = _make_radar(store)
    client = FakeClient([_proc_meta("P-1", estado="Abierto")])
    run_radar(_deps(client), store, radar)
    client.rows = [_proc_meta("P-1", estado="Adjudicado", cierre="2026-09-30T17:00:00.000")]
    run_radar(_deps(client), store, radar)

    ev = _event_by(store.get_feed(radar.owner), "changed_state", "P-1")
    assert ev["prev_status"] == "Abierto"
    assert ev["status"] == "Adjudicado"
    # La metadata describe la fila observada AHORA, no la anterior.
    assert ev["closing_date"] == "2026-09-30T17:00:00.000"
    assert ev["modality"] == _MODALIDAD


def test_changed_value_event_persists_current_metadata_and_prev_value(store):
    """17 · el cambio de valor conserva prev_value y trae la metadata actual."""
    radar = _make_radar(store)
    client = FakeClient([_proc_meta("P-1", valor="1000000")])
    run_radar(_deps(client), store, radar)
    client.rows = [_proc_meta("P-1", valor="1200000")]
    run_radar(_deps(client), store, radar)

    ev = _event_by(store.get_feed(radar.owner), "changed_value", "P-1")
    assert ev["prev_value"] == "1000000"
    assert ev["value"] == "1200000"
    assert ev["modality"] == _MODALIDAD
    assert ev["closing_date"] == _CIERRE


def test_contratos_event_persists_contractor_and_signature_date(store):
    """18 · un evento de contratos congela contratista y fecha de firma."""
    radar = _make_contratos_radar(store)
    client = FakeClient([_cont("C-1")])
    run_radar(_deps(client), store, radar)              # baseline
    client.rows = [_cont("C-1"), _cont("C-2", contratista="CONSORCIO ANDINO",
                                       firma="2026-04-02T00:00:00.000")]
    run_radar(_deps(client), store, radar)

    ev = _event_by(store.get_feed(radar.owner), "new", "C-2")
    assert ev["contractor"] == "CONSORCIO ANDINO"
    assert ev["signature_date"] == "2026-04-02T00:00:00.000"
    assert ev["modality"] == "Contratación directa"
    # Un contrato ya no recibe ofertas: no hay cierre que congelar.
    assert ev["closing_date"] is None


def test_metadata_only_change_emits_no_event(store):
    """23 · dos filas iguales en estado y valor, distintas solo en los cuatro
    campos de presentación, NO producen changed_state ni changed_value: describir
    mejor un proceso no es lo mismo que moverlo."""
    radar = _make_radar(store)
    client = FakeClient([_proc_meta("P-1", modalidad="Licitación pública",
                                    cierre="2026-08-20T17:00:00.000")])
    run_radar(_deps(client), store, radar)              # baseline

    client.rows = [_proc_meta("P-1", modalidad="Selección abreviada",
                              cierre="2026-12-31T17:00:00.000")]
    run_radar(_deps(client), store, radar)

    kinds = [e["kind"] for e in store.get_feed(radar.owner)]
    assert kinds == ["baseline"]                       # nada más entró al feed


def test_contractor_only_change_emits_no_event_in_contratos(store):
    """23b · cambiar solo el contratista tampoco finge un changed_value."""
    radar = _make_contratos_radar(store)
    client = FakeClient([_cont("C-1", contratista="UT PAE")])
    run_radar(_deps(client), store, radar)

    client.rows = [_cont("C-1", contratista="CONSORCIO OTRO",
                         firma="2027-01-01T00:00:00.000")]
    run_radar(_deps(client), store, radar)

    kinds = [e["kind"] for e in store.get_feed(radar.owner)]
    assert kinds == ["baseline"]


def test_snapshot_hash_ignores_event_metadata(store):
    """24 · el hash mantiene su semántica: solo estado y valor lo mueven."""
    from app.core.radar.keys import snapshot_hash

    base = _proc_meta("P-1")
    only_meta = _proc_meta("P-1", modalidad="Otra", cierre="2027-01-01")
    real_change = _proc_meta("P-1", valor="9999999")

    assert snapshot_hash(base, SoQLBuilder.PROCESOS_DATASET) == snapshot_hash(only_meta, SoQLBuilder.PROCESOS_DATASET)
    assert snapshot_hash(base, SoQLBuilder.PROCESOS_DATASET) != snapshot_hash(real_change, SoQLBuilder.PROCESOS_DATASET)


def test_real_change_still_fires_and_carries_latest_metadata(store):
    """25 · un cambio real sigue generando evento y lleva la metadata más
    reciente, aunque la metadata haya cambiado en la misma corrida."""
    radar = _make_radar(store)
    client = FakeClient([_proc_meta("P-1", valor="1000000",
                                    modalidad="Licitación pública")])
    run_radar(_deps(client), store, radar)

    client.rows = [_proc_meta("P-1", valor="7000000",
                              modalidad="Selección abreviada",
                              cierre="2026-11-11T17:00:00.000")]
    run_radar(_deps(client), store, radar)

    ev = _event_by(store.get_feed(radar.owner), "changed_value", "P-1")
    assert ev["prev_value"] == "1000000" and ev["value"] == "7000000"
    assert ev["modality"] == "Selección abreviada"
    assert ev["closing_date"] == "2026-11-11T17:00:00.000"


def test_no_new_event_kinds_were_introduced(store):
    """Los kinds del feed siguen siendo los de siempre: la metadata no creó
    changed_deadline/changed_modality/changed_contractor."""
    radar = _make_radar(store)
    client = FakeClient([_proc_meta("P-1", estado="Abierto", valor="1000000")])
    run_radar(_deps(client), store, radar)
    client.rows = [_proc_meta("P-1", estado="Adjudicado", valor="2000000",
                              modalidad="Otra", cierre="2027-01-01"),
                   _proc_meta("P-2")]
    run_radar(_deps(client), store, radar)

    kinds = {e["kind"] for e in store.get_feed(radar.owner)}
    assert kinds <= {"baseline", "new", "changed_state", "changed_value"}
