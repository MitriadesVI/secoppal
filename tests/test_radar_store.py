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
    FOLLOWS_FREQUENCY,
    FOLLOWS_RADAR_NAME,
    PHASE_AFTER_BEGIN,
    PHASE_AFTER_EVENT_INSERTS,
    PHASE_AFTER_FAILURE_UPDATE,
    PHASE_AFTER_LEASE_FENCE,
    PHASE_AFTER_MEMBERSHIP_INSERT,
    PHASE_AFTER_RADAR_UPDATE,
    PHASE_AFTER_RUN_INSERT,
    PHASE_AFTER_SEEN_UPDATES,
    PHASE_AFTER_SEEN_UPSERTS,
    PHASE_BEFORE_COMMIT,
    EventRecord,
    FailureRecord,
    FollowsSeenUpdate,
    RadarExistsError,
    RadarReservedNameError,
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


# El escenario "update_last_run sobre radar pausado" vive ahora en el bloque de
# Preflight 3 (test_update_last_run_preserves_attention_and_enabled): mismo
# montaje, y además cubre la alerta, los errores y el backoff.


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


def test_delete_radar_archives_and_keeps_history(store):
    """P1B-1: ``delete_radar`` archiva; el CASCADE ya no se dispara.

    Sustituye a ``test_delete_cascades``, que congelaba el contrato anterior
    —borrar el radar destruía runs y seen por ``ON DELETE CASCADE``—. El cambio
    es deliberado: el historial es el activo del producto, y "quitar un radar de
    en medio" nunca quiso decir "borrar todo lo que se supo por él".
    """
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_run(radar_id=rid, started_at="t", status="ok")
    store.upsert_seen(radar_id=rid, result_key="K1", dataset="c",
                      fields={}, snapshot_hash="h", run_id="r")

    store.delete_radar(rid)

    assert store.get_radar(rid) is None          # invisible, como antes
    assert len(store.get_runs(rid)) == 1         # pero el historial sigue ahí
    assert set(store.get_seen(rid)) == {"K1"}


# ── P1B-1 · archivado: retirar de circulación sin destruir ────────────────
#
# radar_runs/radar_events/radar_seen_results cuelgan de radars con ON DELETE
# CASCADE, así que el viejo DELETE se llevaba el historial entero. Archivar deja
# la fila donde está: invisible para todo lector, inelegible para el lease, y
# recuperable con su pasado intacto.


def _raw_radar(store, radar_id: str) -> sqlite3.Row | None:
    """La fila cruda, saltándose el filtro de archivado de los lectores."""
    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT * FROM radars WHERE id = ?", (radar_id,)
        ).fetchone()
    finally:
        conn.close()


def test_archive_hides_the_radar_from_every_active_listing(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())

    store.archive_radar(rid)

    assert store.get_radar(rid) is None
    assert store.get_radar_by_name("u1", "A") is None
    assert store.list_radars("u1") == []
    assert store.list_radars_by_frequency("u1", "manual") == []
    assert [r.id for r in store.list_all_enabled_radars()] == []


def test_archive_keeps_the_row_and_its_history(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_run(radar_id=rid, started_at="t", status="ok")
    store.upsert_seen(radar_id=rid, result_key="K1", dataset="c",
                      fields={}, snapshot_hash="h", run_id="r")
    store.record_events([{
        "radar_id": rid, "owner": "u1", "radar_name": "A", "run_id": "r",
        "kind": "new", "result_key": "K1", "title": "t", "dataset": "c",
    }])

    store.archive_radar(rid)

    assert _raw_radar(store, rid) is not None          # la fila no se fue
    assert len(store.get_runs(rid)) == 1
    assert set(store.get_seen(rid)) == {"K1"}
    assert len(store.get_feed("u1")) == 1


def test_archive_is_idempotent_and_keeps_the_first_date(store):
    """La fecha original dice desde cuándo dejó de operar."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.archive_radar(rid, ts="2026-01-01T00:00:00+00:00")

    store.archive_radar(rid, ts="2026-06-01T00:00:00+00:00")

    assert _raw_radar(store, rid)["archived_at"] == "2026-01-01T00:00:00+00:00"


def test_rearchiving_repairs_a_residual_lease_without_moving_the_date(store, leased):
    """Archivar de nuevo repara una fila archivada que arrastra un token.

    La fila se archiva por SQL directo —como haría una versión anterior o una
    migración—, dejando el lease intacto. Si ``archive_radar`` filtrara por
    ``archived_at IS NULL``, esa fila rota no tendría forma de arreglarse por esta
    puerta: la segunda llamada no haría nada.
    """
    rid, token = leased
    conn = sqlite3.connect(store.db_path)
    try:
        conn.execute(
            "UPDATE radars SET archived_at = '2026-01-01T00:00:00+00:00' WHERE id = ?",
            (rid,),
        )
        conn.commit()
    finally:
        conn.close()
    assert _raw_radar(store, rid)["run_lease_token"] == token   # estado roto

    store.archive_radar(rid, ts="2026-06-01T00:00:00+00:00")

    row = _raw_radar(store, rid)
    assert row["run_lease_token"] is None                        # reparado
    assert row["archived_at"] == "2026-01-01T00:00:00+00:00"     # sin mover la fecha


def test_archived_radar_still_reserves_its_name(store):
    """Si liberara el nombre, restaurarlo chocaría con su reemplazo."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.archive_radar(rid)

    with pytest.raises(RadarExistsError):
        store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())


def test_another_owner_can_still_use_that_name(store):
    """La reserva es por owner, no global."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.archive_radar(rid)

    other = store.create_radar(name="A", owner="u2", query_frame_json=_frame_json())

    assert store.get_radar(other) is not None


def test_restore_brings_back_the_same_row_with_its_history(store):
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_run(radar_id=rid, started_at="t", status="ok")
    store.upsert_seen(radar_id=rid, result_key="K1", dataset="c",
                      fields={}, snapshot_hash="h", run_id="r")
    store.archive_radar(rid)

    store.restore_radar(rid)

    radar = store.get_radar(rid)
    assert radar is not None
    assert radar.id == rid                        # la misma fila, no una nueva
    assert len(store.get_runs(rid)) == 1
    assert set(store.get_seen(rid)) == {"K1"}
    assert [r.id for r in store.list_radars("u1")] == [rid]


def test_archive_does_not_acknowledge_attention_nor_change_enabled(store):
    """Archivar es ortogonal a los ejes de Preflight 3.

    Si archivar limpiara needs_attention, sería un reconocimiento humano por la
    puerta de atrás; y restaurar debe devolver el radar tal como estaba, no
    "arreglado".
    """
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    for _ in range(3):
        store.record_failure(rid, reason="guard_no_scope_or_topic",
                             message="p", permanent=True)

    store.archive_radar(rid)
    row = _raw_radar(store, rid)
    assert (row["needs_attention"], row["enabled"]) == (1, 0)

    store.restore_radar(rid)
    radar = store.get_radar(rid)
    assert radar.needs_attention == 1
    assert radar.enabled == 0
    assert radar.permanent_failure_streak == 3


# ── fence de archivado en el lease ────────────────────────────────────────

def test_archived_radar_cannot_acquire_a_lease(store):
    """Sin esto, archivar no impide correr: el lease no miraba archived_at."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.archive_radar(rid)

    claim = store.try_acquire_run_lease(rid)

    assert not claim.acquired
    assert claim.status == LEASE_RADAR_NOT_FOUND   # no disponible, sin taxonomía nueva


def test_archiving_mid_run_stops_the_old_token_from_materializing(store, leased):
    """La carrera exacta: el ejecutor ya tenía el lease cuando se archivó.

    Es el caso que un chequeo previo del runner no puede cerrar —entre el SELECT
    y la escritura cabe el archivado—, y por eso el predicado vive en el UPDATE
    del lease.
    """
    rid, token = leased

    store.archive_radar(rid)

    result = _materialize_success(store, rid, token)
    assert result.lease_lost is True              # el fence de renew lo frena
    assert _read_state(store.db_path, rid)["runs"] == []


def test_archived_radar_is_not_owned_even_with_a_live_token(store, leased):
    rid, token = leased
    assert store.is_run_lease_owned(rid, token)

    store.archive_radar(rid)

    assert not store.is_run_lease_owned(rid, token)


def test_archive_revokes_the_lease_in_the_same_statement(store, leased):
    """Archivar y quedarse sin dueño son un solo hecho, luego una sola escritura.

    Con dos UPDATEs, entre uno y otro cabe la renovación del ejecutor en vuelo:
    marcaríamos archivado y él volvería a extender el lease.
    """
    rid, token = leased
    assert _raw_radar(store, rid)["run_lease_token"] == token

    store.archive_radar(rid)

    row = _raw_radar(store, rid)
    assert row["archived_at"] is not None
    assert row["run_lease_token"] is None
    assert row["run_lease_acquired_at"] is None
    assert row["run_lease_expires_at"] is None


def test_releasing_after_archive_is_a_harmless_no_op(store, leased):
    """El ejecutor viejo llega a su ``finally`` y suelta un lease que ya no tiene.

    Devuelve False —no había nada suyo que soltar— y no revienta ni toca la fila.
    ``_RELEASE_SQL`` sigue sin filtrar por ``archived_at`` a propósito: soltar lo
    propio nunca es peligroso, y el filtro solo serviría para dejar leases
    clavados si alguna ruta archivara sin revocar.
    """
    rid, token = leased
    store.archive_radar(rid)

    assert store.release_run_lease(rid, token) is False
    assert _raw_radar(store, rid)["run_lease_token"] is None


def test_restore_on_a_live_radar_does_not_touch_a_running_lease(store, leased):
    """Restaurar un radar que nunca se archivó no puede desalojar a quien corre.

    Es el reverso de la limpieza defensiva: arrancarle el lease a un ejecutor
    vivo dejaría que un segundo lo adquiriera en paralelo —dos corridas sobre el
    mismo radar, duplicando eventos—, que es exactamente lo que el lease impide.
    Limpiar solo es seguro cuando el dueño ya no puede estar vivo, y eso solo lo
    garantiza el archivado.
    """
    rid, token = leased
    before = _raw_radar(store, rid)

    store.restore_radar(rid)                       # radar activo, no archivado

    after = _raw_radar(store, rid)
    assert after["run_lease_token"] == token       # A sigue siendo dueño
    assert after["run_lease_acquired_at"] == before["run_lease_acquired_at"]
    assert after["run_lease_expires_at"] == before["run_lease_expires_at"]
    assert store.is_run_lease_owned(rid, token)
    assert not store.try_acquire_run_lease(rid).acquired   # B no puede entrar


def test_restore_twice_does_not_evict_the_new_owner(store):
    """La segunda llamada llega cuando el radar ya está vivo y tomado por otro."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.archive_radar(rid)
    store.restore_radar(rid)
    claim = store.try_acquire_run_lease(rid)       # un ejecutor lo toma
    assert claim.acquired

    store.restore_radar(rid)                       # restore repetido

    assert _raw_radar(store, rid)["run_lease_token"] == claim.token


def test_restore_clears_a_residual_lease(store, leased):
    """Defensivo: restaurar no puede confiar en cómo se archivó la fila.

    Se archiva por SQL directo, sin revocar —como haría una versión anterior, una
    migración o una mano en la consola—, para probar que ``restore_radar`` no
    devuelve el radar con un dueño fantasma que lo bloquee hasta vencer.
    """
    rid, token = leased
    conn = sqlite3.connect(store.db_path)
    try:
        conn.execute(
            "UPDATE radars SET archived_at = '2026-01-01T00:00:00+00:00' WHERE id = ?",
            (rid,),
        )
        conn.commit()
    finally:
        conn.close()
    assert _raw_radar(store, rid)["run_lease_token"] == token   # lease residual

    store.restore_radar(rid)

    assert _raw_radar(store, rid)["run_lease_token"] is None
    assert store.try_acquire_run_lease(rid).acquired            # tomable de nuevo


def test_archive_then_restore_leaves_the_old_token_powerless(store, leased):
    """El ciclo completo: el token de antes del archivado no revive al restaurar.

    Es el caso peligroso —el radar vuelve a estar vivo y sin fence—, así que lo
    único que detiene al ejecutor viejo es haber perdido el lease. Si materializara
    aquí, escribiría una corrida con datos de SECOP anteriores al archivado.
    """
    rid, token = leased
    store.archive_radar(rid)

    store.restore_radar(rid)

    assert not store.is_run_lease_owned(rid, token)
    result = _materialize_success(store, rid, token)
    assert result.lease_lost is True
    assert _read_state(store.db_path, rid)["runs"] == []


def test_lease_fence_holds_even_if_a_row_is_archived_without_revoking(store, leased):
    """El fence del lease es defensa en profundidad, y se prueba aparte.

    ``archive_radar`` revoca el token, así que en el camino normal el ejecutor
    viejo ya falla por token. Este test archiva por SQL directo —dejando el token
    intacto— para que lo único que pueda detenerlo sea ``archived_at IS NULL`` en
    el UPDATE de renovación. Sin él, este radar correría.
    """
    rid, token = leased
    conn = sqlite3.connect(store.db_path)
    try:
        conn.execute(
            "UPDATE radars SET archived_at = '2026-01-01T00:00:00+00:00' WHERE id = ?",
            (rid,),
        )
        conn.commit()
    finally:
        conn.close()

    result = _materialize_success(store, rid, token)

    assert result.lease_lost is True
    assert _read_state(store.db_path, rid)["runs"] == []


def test_restored_radar_can_run_again(store):
    """Restaurar devuelve la capacidad de correr, no solo la visibilidad."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.archive_radar(rid)
    assert not store.try_acquire_run_lease(rid).acquired

    store.restore_radar(rid)

    assert store.try_acquire_run_lease(rid).acquired


# ── migración sobre base heredada ─────────────────────────────────────────

def test_migration_on_a_legacy_db_is_idempotent_and_lossless(tmp_path):
    """Una base sin archived_at migra sin perder datos y sin cambiar de conducta.

    Se construye la tabla vieja a mano —sin la columna— porque el riesgo real es
    una base en producción creada antes de este bloque, no una recién nacida.
    """
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE radars (
                id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT DEFAULT '',
                query_frame_json TEXT NOT NULL, dataset TEXT,
                frequency TEXT NOT NULL DEFAULT 'manual',
                channel TEXT NOT NULL DEFAULT 'web',
                enabled INTEGER NOT NULL DEFAULT 1, owner TEXT NOT NULL,
                created_at TEXT NOT NULL, last_run_at TEXT,
                created_from_trace_id TEXT, last_error_at TEXT,
                last_error_message TEXT, UNIQUE (owner, name)
            )""")
        conn.execute(
            "INSERT INTO radars (id, name, query_frame_json, owner, created_at) "
            "VALUES ('viejo', 'Heredado', '{}', 'u1', '2026-01-01T00:00:00+00:00')"
        )
        conn.commit()
    finally:
        conn.close()

    store = RadarStore(db_path)          # migra al construir
    RadarStore(db_path)                  # y es idempotente

    radar = store.get_radar("viejo")
    assert radar is not None             # el dato sobrevivió
    assert radar.name == "Heredado"
    assert radar.archived_at is None     # NULL = no archivado: misma conducta
    assert store.list_radars("u1")[0].id == "viejo"
    assert store.try_acquire_run_lease("viejo").acquired   # y puede correr


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


# ── P1B-Preflight 3 · needs_attention es sticky ───────────────────────────
#
# Un éxito prueba recuperación *técnica*: que SECOP respondió y la consulta
# corrió. No prueba que alguien entendiera ni corrigiera la causa que exigió
# revisión. Por eso el camino de éxito reinicia contadores/errores/backoff y
# deja intactos los dos ejes que son decisión humana: needs_attention (la
# alerta) y enabled (la autorización a correr solo).


def _alerted_and_paused(store, rid) -> None:
    """Deja el radar en el estado del contrato: 3 permanentes → alerta + pausa."""
    for i in range(3):
        store.record_failure(
            rid, reason="guard_no_scope_or_topic", message=f"p{i}", permanent=True,
            next_run_after="2999-01-01T00:00:00+00:00",
        )
    radar = store.get_radar(rid)
    assert (radar.needs_attention, radar.enabled) == (1, 0)
    assert (radar.failure_count, radar.permanent_failure_streak) == (3, 3)


def _release_and_reacquire(store, rid, token: str) -> str:
    """record_failure no toca el lease, pero sí lo consume el fixture."""
    claim = store.try_acquire_run_lease(rid)
    return claim.token if claim.acquired else token


def test_materialize_success_preserves_needs_attention_on_paused_radar(store, leased):
    """Contrato central: éxito sobre radar pausado NO reactiva ni reconoce.

    Antes de Preflight 3 la corrida OK borraba needs_attention, así que el
    radar quedaba pausado *sin* el indicador que explicaba por qué. Ahora el
    éxito limpia solo el estado técnico.
    """
    rid, token = leased
    _alerted_and_paused(store, rid)
    token = _release_and_reacquire(store, rid, token)

    _materialize_success(store, rid, token)

    radar = store.get_radar(rid)
    assert radar.needs_attention == 1        # la alerta humana sobrevive al éxito
    assert radar.enabled == 0                # sin reactivación silenciosa
    assert radar.failure_count == 0
    assert radar.permanent_failure_streak == 0
    assert radar.last_error_at is None
    assert radar.last_error_message is None
    assert radar.last_failure_reason is None
    assert radar.next_run_after is None
    assert radar.last_run_at


def test_materialize_success_after_explicit_reactivation_keeps_attention(store, leased):
    """Reactivar y reconocer son decisiones distintas.

    El operador habilitó el radar a propósito; eso no dice que haya revisado
    la causa. La alerta sigue en pie hasta que exista una acción explícita que
    la reconozca (deuda de P1B).
    """
    rid, token = leased
    _alerted_and_paused(store, rid)
    store.set_enabled(rid, True)
    token = _release_and_reacquire(store, rid, token)

    _materialize_success(store, rid, token)

    radar = store.get_radar(rid)
    assert radar.enabled == 1                # lo puso el operador, no el éxito
    assert radar.needs_attention == 1        # y el éxito tampoco lo reconoce
    assert radar.failure_count == 0
    assert radar.permanent_failure_streak == 0
    assert radar.last_failure_reason is None
    assert radar.next_run_after is None


def test_materialize_success_leaves_unalerted_radar_unalerted(store, leased):
    """La contracara: preservar no es *encender*. Sin alerta previa, sigue en 0."""
    rid, token = leased
    store.record_failure(rid, reason="socrata_timeout", message="t", permanent=False)
    assert store.get_radar(rid).needs_attention == 0
    token = _release_and_reacquire(store, rid, token)

    _materialize_success(store, rid, token)

    radar = store.get_radar(rid)
    assert radar.needs_attention == 0
    assert radar.enabled == 1
    assert radar.failure_count == 0


def test_update_last_run_preserves_attention_and_enabled(store):
    """update_last_run comparte la sentencia, luego comparte la semántica.

    Es la otra puerta a _mark_success; si divergiera, existirían dos
    definiciones de "corrida OK" y una de ellas borraría la alerta.
    """
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    _alerted_and_paused(store, rid)

    store.update_last_run(rid)

    radar = store.get_radar(rid)
    assert radar.needs_attention == 1
    assert radar.enabled == 0            # tampoco reactiva silenciosamente
    assert radar.failure_count == 0
    assert radar.permanent_failure_streak == 0
    assert radar.last_error_message is None
    assert radar.last_failure_reason is None
    assert radar.next_run_after is None
    assert radar.last_run_at


def test_transient_failure_after_attention_does_not_clear_it(store):
    """Un fallo no reconoce la alerta: solo el transitorio corta la racha."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    _alerted_and_paused(store, rid)

    store.record_failure(rid, reason="socrata_timeout", message="t", permanent=False)

    radar = store.get_radar(rid)
    assert radar.needs_attention == 1
    assert radar.permanent_failure_streak == 0   # el transitorio sí corta la racha
    assert radar.failure_count == 4


def test_mark_success_sql_writes_neither_attention_nor_enabled():
    """Estructural: la sentencia de éxito no menciona los ejes humanos.

    El test de estado prueba el resultado; este prueba que no hay forma de
    llegar a él por accidente —ni con un CASE condicionado a enabled, que
    haría que un radar reactivado se auto-reconociera.
    """
    sql = store_module._MARK_SUCCESS_SQL
    assert "needs_attention" not in sql
    assert "enabled" not in sql
    assert "permanent_failure_streak = 0" in sql


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


def test_rollback_of_success_leaves_alerted_state_untouched(store, leased):
    """Preflight 1 + 3: el todo-o-nada tampoco puede tocar los ejes humanos.

    El parametrizado de arriba corre sobre un radar limpio, donde preservar y
    revertir se ven igual. Aquí el estado previo es el del contrato (alerta +
    pausa + racha), así que un rollback parcial sería visible.
    """
    rid, token = leased
    _alerted_and_paused(store, rid)
    token = _release_and_reacquire(store, rid, token)
    before = _read_state(store.db_path, rid)

    with pytest.raises(_Boom):
        _materialize_success(store, rid, token, hook=_hook_at(PHASE_BEFORE_COMMIT))

    after = _read_state(store.db_path, rid)
    assert after["runs"] == []
    assert after["events"] == []
    assert after["radar"] == before["radar"]              # racha y alerta intactas
    assert after["radar"]["needs_attention"] == 1
    assert after["radar"]["enabled"] == 0
    assert after["radar"]["permanent_failure_streak"] == 3


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

@pytest.mark.parametrize("name", ["materialize_successful_run", "materialize_failed_run",
                                  "materialize_follows_refresh"])
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


# ── P1B-Preflight 2 · materializador del refresco de seguidos ─────────────
#
# El refresco de follows no es una corrida: no inserta radar_run, no toca
# contadores de fallo ni needs_attention. Solo snapshot + eventos, atómicos, y
# con UPDATE en vez de UPSERT para no resucitar lo que unfollow borró.

# first_seen_at del fixture: es la "versión" de la membresía que los updates
# declaran esperar. Un update con otro valor apunta a una encarnación distinta.
_FIRST_SEEN = "2026-01-01T00:00:00+00:00"


@pytest.fixture
def follows_leased(store):
    """Radar sintético con dos seguidos y el lease tomado por nosotros."""
    rid = store.create_radar(
        name=FOLLOWS_RADAR_NAME, owner="u1", query_frame_json="{}",
        frequency=FOLLOWS_FREQUENCY,
    )
    for key in ("K-1", "K-2"):
        store.upsert_seen(
            radar_id=rid, result_key=key, dataset="procesos",
            fields={"status": "Abierto", "value": "100", "title": f"T-{key}"},
            snapshot_hash="h1", run_id="follow_manual",
            seen_at=_FIRST_SEEN,
        )
    claim = store.try_acquire_run_lease(rid)
    assert claim.acquired
    return rid, claim.token


def _follows_update(key: str, *, status="Adjudicado", snap="h2", with_event=True,
                    first_seen=_FIRST_SEEN):
    events = ()
    if with_event:
        events = (EventRecord(
            kind="changed_state", ts="2026-07-15T00:00:00+00:00", result_key=key,
            dataset="procesos", title=f"T-{key}", status=status,
            prev_status="Abierto",
        ),)
    return FollowsSeenUpdate(
        result_key=key, expected_first_seen_at=first_seen,
        fields={"status": status, "value": "100", "title": f"T-{key}",
                "entity": "ALC", "urlproceso": "u", "referencia": "R",
                "contractor": ""},
        snapshot_hash=snap, events=events,
    )


def test_follows_refresh_persists_snapshot_and_events(store, follows_leased):
    rid, token = follows_leased

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    assert (res.updated, res.events, res.lease_lost) == (1, 1, False)
    state = _read_state(store.db_path, rid)
    assert state["seen"]["K-1"]["status"] == "Adjudicado"
    assert state["seen"]["K-1"]["snapshot_hash"] == "h2"
    assert state["seen"]["K-1"]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert state["seen"]["K-2"]["status"] == "Abierto"       # intacto
    assert [e["kind"] for e in state["events"]] == ["changed_state"]


def test_follows_refresh_writes_no_run_and_no_radar_state(store, follows_leased):
    """No es una corrida: no hay radar_run ni limpieza de contadores.

    Es la razón de existir de este materializador: reusar el de la corrida
    obligaría a inventar un radar_run y a aplicar semántica de radar de
    búsqueda que follows nunca tuvo.
    """
    rid, token = follows_leased
    store.record_failure(rid, reason="socrata_timeout", message="previo")
    before = _read_state(store.db_path, rid)["radar"]
    claim = store.try_acquire_run_lease(rid)
    token = claim.token if claim.acquired else token

    store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    after = _read_state(store.db_path, rid)
    assert after["runs"] == []                                  # sin radar_run ficticio
    assert after["radar"]["failure_count"] == before["failure_count"]
    assert after["radar"]["needs_attention"] == before["needs_attention"]
    assert after["radar"]["last_run_at"] == before["last_run_at"]
    assert after["radar"]["next_run_after"] == before["next_run_after"]


def test_follows_refresh_does_not_apply_search_radar_success(store, follows_leased):
    """Regresión de Preflight 3: follows sigue sin tocar el estado del radar.

    El test de arriba parte de un radar sin alerta, donde "no aplicar la
    semántica de éxito" y "aplicarla" difieren poco. Con alerta y pausa puestas,
    un _MARK_SUCCESS_SQL colado aquí se vería en failure_count y last_run_at.
    """
    rid, token = follows_leased
    _alerted_and_paused(store, rid)
    before = _read_state(store.db_path, rid)["radar"]
    token = _release_and_reacquire(store, rid, token)

    result = store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    assert result.lease_lost is False
    after = _read_state(store.db_path, rid)["radar"]
    assert after["failure_count"] == before["failure_count"] == 3
    assert after["permanent_failure_streak"] == before["permanent_failure_streak"] == 3
    assert after["needs_attention"] == before["needs_attention"] == 1
    assert after["enabled"] == before["enabled"] == 0
    assert after["last_run_at"] == before["last_run_at"]
    assert after["next_run_after"] == before["next_run_after"]


def test_follows_refresh_events_use_null_run_id_but_seen_gets_the_sentinel(
    store, follows_leased
):
    """Dos identidades, no una: run_id NULL en el evento, centinela en seen.

    Antes se escribía ``radar.id`` como run_id de ambos — un identificador que
    no resuelve a ninguna fila de radar_runs. Y el fix inicial de esa mentira
    volcó el mismo parámetro (``None``) a los dos campos, dejando
    ``radar_seen_results.last_run_id`` en NULL cuando debía llevar el
    centinela: sin él, dos refrescos consecutivos son indistinguibles en el
    historial de la fila. Ver la investigación, §F.2.
    """
    rid, token = follows_leased

    store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
        seen_run_id="follows_refresh",
    )

    state = _read_state(store.db_path, rid)
    assert state["events"][0]["run_id"] is None, "el evento no debe llevar run_id"
    assert state["events"][0]["radar_id"] == rid
    assert state["seen"]["K-1"]["last_run_id"] == "follows_refresh", (
        "seen.last_run_id debe llevar el centinela, no NULL"
    )


def test_follows_refresh_seen_run_id_defaults_to_the_sentinel(store, follows_leased):
    """El default del parámetro es el mismo centinela que usa follows.py."""
    rid, token = follows_leased

    store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    assert _read_state(store.db_path, rid)["seen"]["K-1"]["last_run_id"] == "follows_refresh"


def test_follows_refresh_update_does_not_resurrect_unfollowed_key(store, follows_leased):
    """El bug de §E: la clave que unfollow borró no revive ni emite evento."""
    rid, token = follows_leased
    store.delete_seen(rid, "K-1")            # el owner dejó de seguirlo

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token,
        updates=[_follows_update("K-1"), _follows_update("K-2")],
    )

    state = _read_state(store.db_path, rid)
    assert "K-1" not in state["seen"], "el refresco resucitó una fila borrada"
    assert res.updated == 1
    # ni un evento de la clave muerta: los eventos van atados a su UPDATE.
    assert [e["result_key"] for e in state["events"]] == ["K-2"]


def test_follows_refresh_upsert_mutation_proves_the_test_would_catch_it(store, follows_leased):
    """Contraprueba: con UPSERT en vez de UPDATE, el test anterior falla.

    Sin esto, ``test_..._does_not_resurrect_...`` podría pasar por accidente.
    """
    rid, token = follows_leased
    store.delete_seen(rid, "K-1")

    # Simula el comportamiento viejo: upsert autocommit de la clave borrada.
    store.upsert_seen(
        radar_id=rid, result_key="K-1", dataset="procesos",
        fields={"status": "Adjudicado", "value": "100"}, snapshot_hash="h2",
        run_id=rid,
    )

    state = _read_state(store.db_path, rid)
    assert "K-1" in state["seen"], "el UPSERT debería resucitarla (comportamiento viejo)"


def test_follows_refresh_rejects_a_search_radar(store):
    """Defensa en profundidad: no materializa sobre un radar que no es follows."""
    rid = store.create_radar(name="Búsqueda", owner="u1",
                             query_frame_json=_frame_json(), frequency="diaria")
    store.upsert_seen(radar_id=rid, result_key="K-1", dataset="procesos",
                      fields={"status": "Abierto"}, snapshot_hash="h1", run_id="r")
    claim = store.try_acquire_run_lease(rid)

    res = store.materialize_follows_refresh(
        radar_id=rid, token=claim.token, updates=[_follows_update("K-1")],
    )

    assert res.not_follows is True
    state = _read_state(store.db_path, rid)
    assert state["seen"]["K-1"]["status"] == "Abierto"    # sin tocar
    assert state["events"] == []


def test_follows_refresh_stolen_lease_writes_nothing(store, follows_leased):
    """El lease se perdió durante la red: no se escribe y no se pisa al sucesor."""
    rid, token = follows_leased
    before = _read_state(store.db_path, rid)
    # B recupera el lease (el de A ya venció).
    future = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
    b = store.try_acquire_run_lease(rid, now=future)
    assert b.acquired

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    assert res.lease_lost is True
    assert (res.updated, res.events) == (0, 0)
    after = _read_state(store.db_path, rid)
    assert after["seen"] == before["seen"]
    assert after["events"] == []
    assert after["radar"]["run_lease_token"] == b.token       # token de B intacto


def test_follows_refresh_release_by_stale_owner_does_not_free_successor(store, follows_leased):
    """El finally de A no puede liberar el lease de B: release es token-fenced."""
    rid, token = follows_leased
    future = datetime.now(timezone.utc) + LEASE_DURATION + timedelta(seconds=1)
    b = store.try_acquire_run_lease(rid, now=future)

    assert store.release_run_lease(rid, token) is False       # A intenta liberar
    assert store.get_radar(rid).run_lease_token == b.token


_FOLLOWS_PHASES = [
    PHASE_AFTER_BEGIN,
    PHASE_AFTER_LEASE_FENCE,
    PHASE_AFTER_SEEN_UPDATES,
    PHASE_AFTER_EVENT_INSERTS,
    PHASE_BEFORE_COMMIT,
]


@pytest.mark.parametrize("phase", _FOLLOWS_PHASES)
def test_follows_refresh_exception_in_any_phase_rolls_back_everything(
    store, follows_leased, phase
):
    rid, token = follows_leased
    before = _read_state(store.db_path, rid)

    with pytest.raises(_Boom):
        store.materialize_follows_refresh(
            radar_id=rid, token=token, updates=[_follows_update("K-1")],
            _fault_hook=_hook_at(phase),
        )

    after = _read_state(store.db_path, rid)
    assert after["seen"] == before["seen"], f"snapshot movido pese al fallo en {phase}"
    assert after["events"] == []
    assert after["runs"] == []


def test_follows_refresh_crash_between_seen_and_events_loses_no_change(store, follows_leased):
    """El defecto de §D: si el snapshot avanza sin su evento, el cambio se pierde.

    Tras el rollback el hash sigue siendo el viejo, así que el refresco
    siguiente vuelve a detectar el cambio y lo emite **exactamente una vez**.
    """
    rid, token = follows_leased

    with pytest.raises(_Boom):
        store.materialize_follows_refresh(
            radar_id=rid, token=token, updates=[_follows_update("K-1")],
            _fault_hook=_hook_at(PHASE_AFTER_SEEN_UPDATES),
        )

    mid = _read_state(store.db_path, rid)
    assert mid["seen"]["K-1"]["snapshot_hash"] == "h1"   # el cambio sigue pendiente
    assert mid["events"] == []

    store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    end = _read_state(store.db_path, rid)
    assert end["seen"]["K-1"]["snapshot_hash"] == "h2"
    assert [e["kind"] for e in end["events"]] == ["changed_state"]  # una sola vez


def test_follows_refresh_crash_after_commit_leaves_complete_state(store, follows_leased):
    """Un fallo posterior al COMMIT no deshace nada: seen y eventos completos."""
    rid, token = follows_leased
    store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    state = _read_state(store.db_path, rid)
    assert state["seen"]["K-1"]["snapshot_hash"] == "h2"
    assert len(state["events"]) == 1


def test_follows_refresh_events_identity_cannot_be_forged(store, follows_leased):
    """La identidad la deriva la transacción, no el caller."""
    rid, token = follows_leased
    intruder = store.create_radar(name="Otro", owner="atacante",
                                  query_frame_json=_frame_json())

    store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    ev = _read_state(store.db_path, rid)["events"][0]
    assert ev["radar_id"] == rid and ev["radar_id"] != intruder
    assert ev["owner"] == "u1"
    assert ev["radar_name"] == FOLLOWS_RADAR_NAME
    assert _read_state(store.db_path, intruder)["events"] == []


def test_two_concurrent_follows_materializers_only_owner_writes(tmp_path):
    """Conexiones distintas, Barrier: el dueño escribe, el vencido no."""
    db_path = tmp_path / "follows_race.db"
    seed = RadarStore(db_path)
    rid = seed.create_radar(name=FOLLOWS_RADAR_NAME, owner="u1",
                            query_frame_json="{}", frequency=FOLLOWS_FREQUENCY)
    seed.upsert_seen(radar_id=rid, result_key="K-1", dataset="procesos",
                     fields={"status": "Abierto"}, snapshot_hash="h1", run_id="m",
                     seen_at=_FIRST_SEEN)
    owner = seed.try_acquire_run_lease(rid)
    assert owner.acquired

    barrier = threading.Barrier(2)
    results: dict = {}
    lock = threading.Lock()

    def materialize(label: str, token: str) -> None:
        store = RadarStore(db_path)
        barrier.wait()
        res = store.materialize_follows_refresh(
            radar_id=rid, token=token, updates=[_follows_update("K-1")],
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
    assert len(state["events"]) == 1          # sin duplicado del perdedor


# ── alta de membresía atómica e idempotente ───────────────────────────────

def _follow_args(key="K-9", *, status="Abierto", snap=""):
    return dict(
        seen=SeenRecord(
            result_key=key, dataset="procesos", snapshot_hash=snap,
            fields={"title": f"T-{key}", "entity": "ALC", "value": "100",
                    "status": status, "urlproceso": "u", "referencia": "R",
                    "contractor": ""},
        ),
        event=EventRecord(
            kind="followed", ts="2026-07-15T00:00:00+00:00", result_key=key,
            dataset="procesos", title=f"T-{key}", status=status,
        ),
        run_id="follow_manual",
    )


def test_follow_if_absent_creates_membership_and_event(store, follows_leased):
    rid, _ = follows_leased

    res = store.materialize_follow_if_absent(radar_id=rid, **_follow_args())

    assert res.created is True
    state = _read_state(store.db_path, rid)
    assert state["seen"]["K-9"]["snapshot_hash"] == ""     # baseline pendiente
    assert [e["kind"] for e in state["events"]] == ["followed"]
    assert state["events"][0]["radar_id"] == rid
    assert state["events"][0]["owner"] == "u1"


def test_refollow_is_idempotent_and_preserves_the_baseline(store, follows_leased):
    """Re-seguir no puede borrar la línea base ya fijada.

    Con el UPSERT anterior, ``follow`` reescribía snapshot_hash a '': el
    refresco siguiente veía baseline y **silenciaba el cambio real**.
    """
    rid, token = follows_leased
    store.materialize_follow_if_absent(radar_id=rid, **_follow_args())
    # el refresco fija la línea base
    store.materialize_follow_if_absent(radar_id=rid, **_follow_args())  # ruido
    conn = sqlite3.connect(store.db_path)
    conn.execute(
        "UPDATE radar_seen_results SET snapshot_hash='h-base', "
        "first_seen_at='2026-01-01T00:00:00+00:00' WHERE result_key='K-9'"
    )
    conn.commit()
    conn.close()

    res = store.materialize_follow_if_absent(radar_id=rid, **_follow_args())

    assert res.created is False
    state = _read_state(store.db_path, rid)
    assert state["seen"]["K-9"]["snapshot_hash"] == "h-base", "re-follow borró la baseline"
    assert state["seen"]["K-9"]["first_seen_at"] == "2026-01-01T00:00:00+00:00"
    assert len([e for e in state["events"] if e["kind"] == "followed"]) == 1


def test_follow_if_absent_rejects_a_search_radar(store):
    rid = store.create_radar(name="Búsqueda", owner="u1",
                             query_frame_json=_frame_json(), frequency="diaria")

    res = store.materialize_follow_if_absent(radar_id=rid, **_follow_args())

    assert res.not_follows is True
    state = _read_state(store.db_path, rid)
    assert state["seen"] == {} and state["events"] == []


@pytest.mark.parametrize("phase", [PHASE_AFTER_BEGIN, PHASE_AFTER_MEMBERSHIP_INSERT,
                                   PHASE_AFTER_EVENT_INSERTS, PHASE_BEFORE_COMMIT])
def test_follow_if_absent_rolls_back_everything_on_any_phase(store, follows_leased, phase):
    """Crash tras la membresía y antes del evento: rollback total.

    Media alta dejaría un seguido que el feed nunca explicó.
    """
    rid, _ = follows_leased

    with pytest.raises(_Boom):
        store.materialize_follow_if_absent(
            radar_id=rid, **_follow_args(), _fault_hook=_hook_at(phase),
        )

    state = _read_state(store.db_path, rid)
    assert "K-9" not in state["seen"]
    assert state["events"] == []


def test_eight_concurrent_follows_create_one_row_and_one_event(tmp_path):
    """Ocho clics simultáneos: una fila, un evento. Lo decide la PRIMARY KEY."""
    db_path = tmp_path / "follow_race.db"
    seed = RadarStore(db_path)
    rid = seed.create_radar(name=FOLLOWS_RADAR_NAME, owner="u1",
                            query_frame_json="{}", frequency=FOLLOWS_FREQUENCY)

    workers = 8
    barrier = threading.Barrier(workers)
    created: list = []
    errors: list = []
    lock = threading.Lock()

    def contend() -> None:
        store = RadarStore(db_path)
        try:
            barrier.wait()
            res = store.materialize_follow_if_absent(radar_id=rid, **_follow_args())
        except Exception as exc:
            with lock:
                errors.append(exc)
            return
        with lock:
            created.append(res.created)

    threads = [threading.Thread(target=contend) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert errors == [], f"la carrera levantó excepciones: {errors}"
    assert sum(created) == 1, "más de un alta ganó la carrera"
    state = _read_state(db_path, rid)
    assert list(state["seen"]) == ["K-9"]
    assert len(state["events"]) == 1


def test_follow_if_absent_rejects_an_event_of_another_key(store, follows_leased):
    """El evento y la membresía son dos caras del mismo hecho."""
    rid, _ = follows_leased
    args = _follow_args("K-9")
    args["event"] = EventRecord(kind="followed", result_key="K-OTRA",
                                dataset="procesos", title="T")

    with pytest.raises(ValueError, match="K-OTRA"):
        store.materialize_follow_if_absent(radar_id=rid, **args)

    assert _read_state(store.db_path, rid)["seen"].get("K-9") is None


def test_follow_if_absent_rejects_an_event_of_another_dataset(store, follows_leased):
    rid, _ = follows_leased
    args = _follow_args("K-9")
    args["event"] = EventRecord(kind="followed", result_key="K-9",
                                dataset="contratos", title="T")

    with pytest.raises(ValueError, match="contratos"):
        store.materialize_follow_if_absent(radar_id=rid, **args)


def test_follow_if_absent_rejects_a_non_followed_event(store, follows_leased):
    rid, _ = follows_leased
    args = _follow_args("K-9")
    args["event"] = EventRecord(kind="changed_state", result_key="K-9",
                                dataset="procesos", title="T")

    with pytest.raises(ValueError, match="followed"):
        store.materialize_follow_if_absent(radar_id=rid, **args)


@pytest.mark.parametrize("mutate,label", [
    ("DELETE FROM radars WHERE id = ?", "borrado"),
    ("UPDATE radars SET frequency='diaria' WHERE id = ?", "ya no es follows"),
    ("UPDATE radars SET name='otro' WHERE id = ?", "nombre no canónico"),
])
def test_follow_if_absent_refuses_a_radar_that_is_not_a_canonical_watchlist(
    store, follows_leased, mutate, label
):
    """Se valida dentro de la transacción: existir, ser follows y ser canónico."""
    rid, _ = follows_leased
    conn = sqlite3.connect(store.db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute(mutate, (rid,))
    conn.commit()
    conn.close()

    res = store.materialize_follow_if_absent(radar_id=rid, **_follow_args("K-9"))

    assert res.not_follows is True, label
    assert res.created is False


@pytest.mark.parametrize("mutate,label", [
    ("UPDATE radars SET frequency='diaria' WHERE id = ?", "ya no es follows"),
    ("UPDATE radars SET name='otro' WHERE id = ?", "nombre no canónico"),
])
def test_follows_refresh_refuses_a_radar_that_is_not_canonical(
    store, follows_leased, mutate, label
):
    rid, token = follows_leased
    conn = sqlite3.connect(store.db_path)
    conn.execute(mutate, (rid,))
    conn.commit()
    conn.close()

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )

    assert res.not_follows is True, label
    state = _read_state(store.db_path, rid)
    assert state["seen"]["K-1"]["status"] == "Abierto"    # sin tocar
    assert state["events"] == []


# ── fence de versión de la membresía ──────────────────────────────────────

def test_update_skips_a_row_recreated_during_the_fetch(store, follows_leased):
    """Misma clave, otra encarnación: el UPDATE no la toca.

    (radar_id, result_key) identifica el proceso; first_seen_at identifica
    **esta** membresía. Sin el fence, el refresco pisaría la fila nueva.
    """
    rid, token = follows_leased
    # el owner deja de seguir K-1 y lo vuelve a seguir: fila nueva, misma clave
    store.delete_seen(rid, "K-1")
    store.upsert_seen(
        radar_id=rid, result_key="K-1", dataset="procesos",
        fields={"status": "Abierto", "value": "100"}, snapshot_hash="",
        run_id="follow_manual", seen_at="2026-06-06T00:00:00+00:00",
    )

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token,
        updates=[_follows_update("K-1"), _follows_update("K-2")],
    )

    state = _read_state(store.db_path, rid)
    assert res.updated == 1                       # solo K-2
    assert state["seen"]["K-1"]["snapshot_hash"] == "", "pisó la membresía nueva"
    assert state["seen"]["K-1"]["first_seen_at"] == "2026-06-06T00:00:00+00:00"
    assert [e["result_key"] for e in state["events"]] == ["K-2"]


def test_update_without_version_fence_would_hit_the_new_row(store, follows_leased):
    """Contraprueba: si el update declara el first_seen_at NUEVO, sí acierta.

    Prueba que el test anterior no pasa por accidente (p. ej. porque el UPDATE
    fallara por otra razón).
    """
    rid, token = follows_leased
    store.delete_seen(rid, "K-1")
    store.upsert_seen(
        radar_id=rid, result_key="K-1", dataset="procesos",
        fields={"status": "Abierto", "value": "100"}, snapshot_hash="",
        run_id="follow_manual", seen_at="2026-06-06T00:00:00+00:00",
    )

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token,
        updates=[_follows_update("K-1", first_seen="2026-06-06T00:00:00+00:00")],
    )

    assert res.updated == 1
    assert _read_state(store.db_path, rid)["seen"]["K-1"]["snapshot_hash"] == "h2"


# ── conteos materializados, no construidos ────────────────────────────────

def test_refresh_counts_come_from_what_was_written(store, follows_leased):
    """Una clave descartada por unfollow no puede contar como cambio."""
    rid, token = follows_leased
    store.delete_seen(rid, "K-1")

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token,
        updates=[_follows_update("K-1"), _follows_update("K-2")],
    )

    assert res.updated == 1
    assert res.changed_state == 1        # solo K-2, no K-1
    assert res.events == 1


def test_refresh_counts_baseline_from_live_updates(store, follows_leased):
    rid, token = follows_leased
    store.delete_seen(rid, "K-1")
    base_1 = FollowsSeenUpdate(result_key="K-1", expected_first_seen_at=_FIRST_SEEN,
                               fields={"status": "Abierto"},
                               snapshot_hash="h9", is_baseline=True)
    base_2 = FollowsSeenUpdate(result_key="K-2", expected_first_seen_at=_FIRST_SEEN,
                               fields={"status": "Abierto"},
                               snapshot_hash="h9", is_baseline=True)

    res = store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[base_1, base_2],
    )

    assert res.baseline == 1             # K-1 ya no existe
    assert res.updated == 1 and res.events == 0


# ── nombre reservado del radar sintético ──────────────────────────────────

def test_create_radar_rejects_the_reserved_name_for_a_search_radar(store):
    """Un radar de búsqueda no puede llamarse como la watchlist.

    Si pudiera, ``get_or_create_follows_radar`` lo resolvería como la watchlist
    del owner y los seguidos entrarían en los radar_seen_results de esa
    búsqueda, marcando como visto lo que nunca encontró.
    """
    with pytest.raises(RadarReservedNameError):
        store.create_radar(name=FOLLOWS_RADAR_NAME, owner="u1",
                           query_frame_json=_frame_json(), frequency="diaria")
    assert store.list_radars("u1") == []


def test_create_radar_rejects_case_variants_of_the_reserved_name(store):
    """El UNIQUE(owner,name) es binario y el lookup NOCASE: la reserva cubre el hueco."""
    variant = FOLLOWS_RADAR_NAME.replace("Procesos", "procesos")
    with pytest.raises(RadarReservedNameError):
        store.create_radar(name=variant, owner="u1",
                           query_frame_json=_frame_json(), frequency="manual")


def test_create_radar_allows_the_reserved_name_for_the_synthetic_radar(store):
    """La reserva protege el nombre; no impide que el dueño legítimo lo use."""
    rid = store.create_radar(name=FOLLOWS_RADAR_NAME, owner="u1",
                             query_frame_json="{}", frequency=FOLLOWS_FREQUENCY)
    assert store.get_radar(rid).frequency == FOLLOWS_FREQUENCY


def test_list_radars_by_frequency_is_scoped_by_owner(store):
    store.create_radar(name="Búsqueda", owner="u1",
                       query_frame_json=_frame_json(), frequency="diaria")
    rid = store.create_radar(name=FOLLOWS_RADAR_NAME, owner="u1",
                             query_frame_json="{}", frequency=FOLLOWS_FREQUENCY)

    assert [r.id for r in store.list_radars_by_frequency("u1", FOLLOWS_FREQUENCY)] == [rid]
    assert store.list_radars_by_frequency("u2", FOLLOWS_FREQUENCY) == []


def test_list_radars_by_frequency_does_not_hide_duplicates(store):
    """La lectura debe **delatar** la cardinalidad, no elegir una fila.

    Con LIMIT 1 la corrupción sería invisible: devolvería una watchlist
    cualquiera y el owner vería la mitad de sus seguidos.
    """
    conn = sqlite3.connect(store.db_path)
    for i in (1, 2):
        conn.execute(
            "INSERT INTO radars (id, name, description, query_frame_json, frequency,"
            " channel, enabled, owner, created_at) VALUES (?,?,?,?,?,?,1,?,?)",
            (f"dup-{i}", f"seguidos-{i}", "", "{}", FOLLOWS_FREQUENCY, "web", "u1",
             f"2026-01-0{i}T00:00:00+00:00"),
        )
    conn.commit()
    conn.close()

    rows = store.list_radars_by_frequency("u1", FOLLOWS_FREQUENCY, limit=2)
    assert [r.id for r in rows] == ["dup-1", "dup-2"]


def test_create_radar_rejects_follows_with_a_non_canonical_name(store):
    """Un follows con otro nombre pasaría el UNIQUE y sería un segundo follows."""
    with pytest.raises(RadarReservedNameError):
        store.create_radar(name="Mis seguidos", owner="u1",
                           query_frame_json="{}", frequency=FOLLOWS_FREQUENCY)
    with pytest.raises(RadarReservedNameError):
        store.create_radar(name=FOLLOWS_RADAR_NAME.replace("Procesos", "procesos"),
                           owner="u1", query_frame_json="{}",
                           frequency=FOLLOWS_FREQUENCY)
    assert store.list_radars("u1") == []


# ── event_seq (P1B-2) ──────────────────────────────────────────────────────

def test_first_new_event_gets_event_seq(store, leased):
    """1 · primer evento nuevo recibe event_seq."""
    rid, token = leased
    _materialize_success(store, rid, token)
    state = _read_state(store.db_path, rid)
    seqs = [e["event_seq"] for e in state["events"]]
    assert seqs and all(s is not None and s > 0 for s in seqs)


def test_multiple_events_in_one_run_get_distinct_increasing_seqs(store, leased):
    """2 · varios eventos de una misma corrida reciben secuencias distintas y
    crecientes."""
    rid, token = leased
    _materialize_success(store, rid, token)  # dos eventos: K-old y K-new
    state = _read_state(store.db_path, rid)
    seqs = [e["event_seq"] for e in state["events"]]
    assert len(seqs) == 2
    assert len(set(seqs)) == 2
    assert max(seqs) > min(seqs)


def test_follows_refresh_and_manual_follow_events_get_event_seq(store, follows_leased):
    """3 · follows refresh y follow manual también reciben event_seq."""
    rid, token = follows_leased
    store.materialize_follows_refresh(
        radar_id=rid, token=token, updates=[_follows_update("K-1")],
    )
    refresh_state = _read_state(store.db_path, rid)
    assert refresh_state["events"][0]["event_seq"] is not None

    res = store.materialize_follow_if_absent(radar_id=rid, **_follow_args(key="K-9"))
    assert res.created is True
    state = _read_state(store.db_path, rid)
    followed = [e for e in state["events"] if e["kind"] == "followed"]
    assert followed and followed[0]["event_seq"] is not None
    # secuencias del refresh y del alta manual, todas distintas
    all_seqs = [e["event_seq"] for e in state["events"]]
    assert len(all_seqs) == len(set(all_seqs))


def _seed_legacy_events_db(db_path) -> None:
    """Base 'pre-P1B-2': sin event_seq ni radar_event_seq, con eventos ya
    existentes. Simula lo que RadarStore encontrará en producción al migrar."""
    conn = sqlite3.connect(db_path)
    conn.executescript(_PRE_P1A_RADARS)  # esquema completo pre-P1A de radars
    conn.execute("""
        CREATE TABLE radar_events (
            id TEXT PRIMARY KEY, radar_id TEXT NOT NULL, owner TEXT NOT NULL,
            radar_name TEXT, run_id TEXT, ts TEXT NOT NULL, kind TEXT NOT NULL,
            result_key TEXT, dataset TEXT, title TEXT, entity TEXT, value TEXT,
            status TEXT, prev_status TEXT, prev_value TEXT, urlproceso TEXT,
            referencia TEXT
        )
    """)
    conn.execute(
        "INSERT INTO radars (id, name, description, query_frame_json, owner, "
        "created_at) VALUES ('r1', 'A', '', '{}', 'u1', 'x')"
    )
    for eid in ("ev-legacy-1", "ev-legacy-2"):
        conn.execute(
            "INSERT INTO radar_events (id, radar_id, owner, ts, kind) "
            "VALUES (?, 'r1', 'u1', 'x', 'new')",
            (eid,),
        )
    conn.commit()
    conn.close()


def test_legacy_rows_keep_event_seq_null_after_migration(tmp_path):
    """4 · eventos heredados quedan NULL tras migrar."""
    db_path = tmp_path / "legacy.db"
    _seed_legacy_events_db(db_path)

    RadarStore(db_path)  # dispara la migración aditiva

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, event_seq FROM radar_events ORDER BY id"
    ).fetchall()
    conn.close()
    assert {r["id"]: r["event_seq"] for r in rows} == {
        "ev-legacy-1": None, "ev-legacy-2": None,
    }


def test_new_event_after_migration_gets_a_sequence(tmp_path):
    """5 · nuevo evento después de migrar recibe secuencia."""
    db_path = tmp_path / "legacy_new.db"
    _seed_legacy_events_db(db_path)

    store = RadarStore(db_path)
    rid = store.create_radar(name="B", owner="u1", query_frame_json=_frame_json())
    claim = store.try_acquire_run_lease(rid)
    result = store.materialize_successful_run(
        radar_id=rid, token=claim.token, run=_run(),
        seen=[_seen_rec("K-post")], events=[_event(key="K-post")],
    )
    assert result.lease_lost is False
    state = _read_state(store.db_path, rid)
    assert state["events"][0]["event_seq"] is not None
    assert state["events"][0]["event_seq"] > 0


def test_legacy_db_migration_preserves_all_existing_rows_and_columns(tmp_path):
    """48 · base heredada con radar_events existentes conserva todos los datos."""
    db_path = tmp_path / "legacy_preserve.db"
    _seed_legacy_events_db(db_path)

    RadarStore(db_path)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM radar_events ORDER BY id").fetchall()
    conn.close()
    assert [r["id"] for r in rows] == ["ev-legacy-1", "ev-legacy-2"]
    assert all(r["kind"] == "new" and r["radar_id"] == "r1" for r in rows)
    assert all(r["event_seq"] is None for r in rows)


def test_deleting_event_does_not_reuse_its_seq(store, leased):
    """6 · borrar directamente un radar_event y crear otro no reutiliza el
    número."""
    rid, token = leased
    _materialize_success(store, rid, token)
    state = _read_state(store.db_path, rid)
    max_before = max(e["event_seq"] for e in state["events"])

    conn = sqlite3.connect(store.db_path)
    conn.execute("DELETE FROM radar_events WHERE radar_id = ?", (rid,))
    conn.commit()
    conn.close()

    _materialize_success(store, rid, token)
    state2 = _read_state(store.db_path, rid)
    assert all(e["event_seq"] > max_before for e in state2["events"])


def test_materializer_rollback_leaves_no_visible_event(store, leased):
    """7 · rollback del materializador no deja radar_event visible; el hueco
    de secuencia consumido es aceptable y no bloquea corridas siguientes."""
    rid, token = leased

    with pytest.raises(_Boom):
        _materialize_success(store, rid, token, hook=_hook_at(PHASE_AFTER_EVENT_INSERTS))
    state = _read_state(store.db_path, rid)
    assert state["events"] == []

    result = _materialize_success(store, rid, token)
    assert result.lease_lost is False
    state2 = _read_state(store.db_path, rid)
    assert len(state2["events"]) == 2
    assert all(e["event_seq"] is not None for e in state2["events"])


def test_record_events_ignores_caller_supplied_event_seq(store):
    """8 · ningún INSERT productivo permite event_seq aportado por caller."""
    rid = store.create_radar(name="A", owner="u1", query_frame_json=_frame_json())
    store.record_events([{
        "radar_id": rid, "owner": "u1", "radar_name": "A", "run_id": "r",
        "kind": "new", "result_key": "K1", "title": "t", "dataset": "c",
        "event_seq": 999999,
    }])
    state = _read_state(store.db_path, rid)
    assert state["events"][0]["event_seq"] != 999999
    assert state["events"][0]["event_seq"] is not None


def test_event_record_does_not_expose_event_seq_field():
    """8 (complemento) · EventRecord no expone event_seq: el caller no tiene
    ni el campo para intentar aportarlo."""
    field_names = {f.name for f in dataclasses.fields(EventRecord)}
    assert "event_seq" not in field_names


def test_radar_store_generates_event_seq_without_any_notify_store(store, leased):
    """50 · RadarStore puede generar event_seq sin haber instanciado ningún
    Notify Store. Este módulo de test no importa app.core.notify en absoluto."""
    rid, token = leased
    result = _materialize_success(store, rid, token)
    assert result.lease_lost is False
    state = _read_state(store.db_path, rid)
    assert all(e["event_seq"] is not None for e in state["events"])


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


# ── proyección inmutable del evento (R2B-1B1) ──────────────────────────────
# Las cuatro columnas son aditivas y nullable: congelan el dato observado al
# emitir el evento para que la notificación no tenga que ir a buscarlo a
# radar_seen_results (estado de HOY) para completar un hecho de AYER.

_EVENT_META_COLUMNS = {"modality", "closing_date", "contractor", "signature_date"}


def _event_columns(db_path) -> set[str]:
    conn = sqlite3.connect(db_path)
    try:
        return {r[1] for r in conn.execute("PRAGMA table_info(radar_events)")}
    finally:
        conn.close()


def test_fresh_db_has_the_four_event_metadata_columns(store):
    """9 · base nueva trae las cuatro columnas."""
    assert _EVENT_META_COLUMNS <= _event_columns(store.db_path)


def test_legacy_db_gets_the_columns_with_null_on_old_rows(tmp_path):
    """10 · base heredada migra y las filas viejas quedan en NULL (sin backfill:
    rellenarlas inventaría un pasado que nadie observó)."""
    db_path = tmp_path / "legacy_meta.db"
    _seed_legacy_events_db(db_path)
    assert not (_EVENT_META_COLUMNS & _event_columns(db_path))

    RadarStore(db_path)

    assert _EVENT_META_COLUMNS <= _event_columns(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, modality, closing_date, contractor, signature_date "
        "FROM radar_events ORDER BY id"
    ).fetchall()
    conn.close()
    assert len(rows) == 2
    for r in rows:
        assert (r["modality"], r["closing_date"],
                r["contractor"], r["signature_date"]) == (None, None, None, None)


def test_event_metadata_migration_is_idempotent(tmp_path):
    """11 · abrir dos veces no falla ni duplica columnas."""
    db_path = tmp_path / "legacy_meta_twice.db"
    _seed_legacy_events_db(db_path)

    RadarStore(db_path)
    RadarStore(db_path)
    store = RadarStore(db_path)

    assert _EVENT_META_COLUMNS <= _event_columns(db_path)
    assert store.get_radar("r1") is not None


def test_insert_event_persists_the_four_metadata_fields(store, leased):
    """12 · _INSERT_EVENT_SQL persiste los cuatro campos de punta a punta."""
    rid, token = leased
    event = dataclasses.replace(
        _event(kind="new", key="K-meta"),
        modality="Licitación pública",
        closing_date="2026-08-20T17:00:00.000",
        contractor="UT PAE 2026",
        signature_date="2026-03-11T00:00:00.000",
    )
    result = store.materialize_successful_run(
        radar_id=rid, token=token, run=_run(),
        seen=[_seen_rec("K-meta")], events=[event],
    )
    assert result.lease_lost is False

    state = _read_state(store.db_path, rid)
    persisted = [e for e in state["events"] if e["result_key"] == "K-meta"]
    assert len(persisted) == 1
    row = persisted[0]
    assert row["modality"] == "Licitación pública"
    assert row["closing_date"] == "2026-08-20T17:00:00.000"
    assert row["contractor"] == "UT PAE 2026"
    assert row["signature_date"] == "2026-03-11T00:00:00.000"


def test_event_metadata_defaults_to_null_when_not_supplied(store, leased):
    """Un EventRecord sin metadata persiste NULL, no cadena vacía: 'no sabemos'
    y 'sabemos que está vacío' no deben distinguirse aguas abajo."""
    rid, token = leased
    store.materialize_successful_run(
        radar_id=rid, token=token, run=_run(),
        seen=[_seen_rec("K-bare")], events=[_event(kind="new", key="K-bare")],
    )
    row = [e for e in _read_state(store.db_path, rid)["events"]
           if e["result_key"] == "K-bare"][0]
    assert (row["modality"], row["closing_date"],
            row["contractor"], row["signature_date"]) == (None, None, None, None)


def test_record_events_still_ignores_caller_supplied_event_seq(store):
    """13 · la metadata nueva no abre una rendija: event_seq lo sigue reservando
    el store, no el caller."""
    rid = store.create_radar(name="C", owner="u1", query_frame_json=_frame_json())
    store.record_events([{
        "radar_id": rid, "owner": "u1", "kind": "new", "result_key": "K-x",
        "ts": "2026-07-16T00:00:00+00:00", "event_seq": 999_999,
        "modality": "Licitación pública",
    }])
    row = _read_state(store.db_path, rid)["events"][0]
    assert row["event_seq"] != 999_999
    assert row["modality"] == "Licitación pública"


def test_event_record_still_has_no_transactional_identity_fields():
    """14 · EventRecord sigue sin identidad: la metadata no la introdujo."""
    field_names = {f.name for f in dataclasses.fields(EventRecord)}
    assert not (field_names & {"radar_id", "owner", "radar_name", "run_id", "event_seq"})
    assert _EVENT_META_COLUMNS <= field_names
