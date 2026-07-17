from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
import threading

import pytest

import app.core.notify.selector as selector_module
from app.core.db import connect_sqlite
from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.grouping import SelectableRadarEvent
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker
from app.core.notify.outbox_store import BatchEventRef, OutboxStore
from app.core.notify.preference_store import PreferenceStore
from app.core.notify.render import render_payload
from app.core.notify.selector import (
    EligibilityFlags,
    NotificationSelector,
    PHASE_AFTER_ASSOCIATIONS,
    PHASE_AFTER_BATCH_INSERT,
    PHASE_AFTER_CURSOR_UPDATE,
    PHASE_AFTER_WINDOW_READ,
    PHASE_BEFORE_COMMIT,
    PHASE_BEFORE_CURSOR_UPDATE,
    SelectorError,
    expires_at_for_frequency,
    is_event_eligible,
    quiet_hours_not_before,
)
from app.core.radar.store import RadarStore


NOW = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc)


@pytest.fixture
def env(tmp_path):
    db = tmp_path / "selector.db"
    radars = RadarStore(db)
    identity = IdentityStore(db)
    endpoints = EndpointStore(db, masker=Masker.from_key("selector-test-key"))
    preferences = PreferenceStore(db)
    owner = "email:ana@example.com"
    principal = identity.create_principal("ana@example.com")
    identity.link_owner_key(owner, principal)
    endpoint = endpoints.create_endpoint(
        principal_id=principal, channel="telegram", address="ana-chat"
    )
    endpoints.mark_verified(endpoint, external_recipient_id="chat-ana")
    radar = radars.create_radar(
        name="Radar PAE", owner=owner, query_frame_json="{}"
    )
    preference = preferences.create_preference(radar_id=radar, endpoint_id=endpoint)
    preferences.set_enabled(preference, True)
    return {
        "db": db, "radars": radars, "identity": identity,
        "endpoints": endpoints, "preferences": preferences,
        "principal": principal, "endpoint": endpoint, "radar": radar,
        "preference": preference, "owner": owner,
        "selector": NotificationSelector(db), "outbox": OutboxStore(db),
    }


def _events(env, kinds, *, ts="2026-07-15T10:00:00+00:00", start=1,
            same_key=False, values=None, extra=None):
    values = values or {}
    rows = []
    for offset, kind in enumerate(kinds):
        number = start + offset
        row = {
            "radar_id": env["radar"], "owner": env["owner"],
            "radar_name": "Radar PAE", "run_id": "run", "ts": ts,
            "kind": kind, "result_key": "SAME" if same_key else f"K-{number}",
            "dataset": "procesos", "title": f"Proceso {number}",
            "entity": "Entidad", "value": values.get(number, str(number * 100)),
            "status": f"S-{number}", "prev_status": f"S-{number - 1}",
            "prev_value": str((number - 1) * 100),
            "urlproceso": f"https://secop/{number}", "referencia": f"REF-{number}",
        }
        row.update(extra or {})
        rows.append(row)
    env["radars"].record_events(rows)
    conn = connect_sqlite(env["db"])
    try:
        return conn.execute(
            "SELECT * FROM radar_events WHERE radar_id=? ORDER BY event_seq DESC LIMIT ?",
            (env["radar"], len(rows)),
        ).fetchall()[::-1]
    finally:
        conn.close()


def _cursor(env, endpoint=None, radar=None):
    conn = connect_sqlite(env["db"])
    try:
        row = conn.execute(
            "SELECT last_examined_seq FROM radar_notification_cursors "
            "WHERE endpoint_id=? AND radar_id=?",
            (endpoint or env["endpoint"], radar or env["radar"]),
        ).fetchone()
        return row["last_examined_seq"] if row else 0
    finally:
        conn.close()


@pytest.mark.parametrize(
    "kind,flags,confirmed,expected",
    [
        ("new", EligibilityFlags(), False, True),
        ("changed_state", EligibilityFlags(), False, True),
        ("changed_value", EligibilityFlags(include_value_changes=True), False, True),
        ("changed_value", EligibilityFlags(include_value_changes=False), False, False),
        ("baseline", EligibilityFlags(), False, False),
        ("followed", EligibilityFlags(), False, False),
        ("window_churn", EligibilityFlags(), False, False),
        ("disappeared", EligibilityFlags(include_disappeared=True), False, False),
        ("disappeared", EligibilityFlags(include_disappeared=True), True, True),
        ("unknown", EligibilityFlags(), False, False),
        ("radar_timeout", EligibilityFlags(), False, False),
        ("already_running", EligibilityFlags(), False, False),
        ("lease_lost", EligibilityFlags(), False, False),
        ("radar_not_found", EligibilityFlags(), False, False),
    ],
)
def test_closed_kind_matrix(kind, flags, confirmed, expected):
    event = SelectableRadarEvent(
        event_id="e", event_seq=1, kind=kind,
        disappeared_confirmed=confirmed,
    )
    assert is_event_eligible(event, flags) is expected


def test_twenty_same_timestamps_use_event_seq_and_associate_all(env):
    rows = _events(env, ["new"] * 20)
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert (result.examined, result.eligible, result.associated) == (20, 20, 20)
    assert [r.event_seq for r in env["outbox"].list_batch_events(result.notification_id)] == [
        row["event_seq"] for row in rows
    ]
    assert result.cursor_after == rows[-1]["event_seq"]


def test_order_uses_seq_not_timestamp(env):
    first = _events(env, ["new"], ts="2099-01-01T00:00:00+00:00")[0]
    second = _events(env, ["new"], ts="2000-01-01T00:00:00+00:00", start=2)[0]
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert [r.event_seq for r in env["outbox"].list_batch_events(result.notification_id)] == [
        first["event_seq"], second["event_seq"],
    ]


def test_already_associated_is_not_reselected_when_cursor_is_rewound(env):
    _events(env, ["new"])
    first = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(
            "UPDATE radar_notification_cursors SET last_examined_seq=0 "
            "WHERE endpoint_id=? AND radar_id=?", (env["endpoint"], env["radar"])
        )
    finally:
        conn.close()
    second = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert first.created is True
    assert second.created is False and second.skipped_reason == "empty_window"
    assert env["outbox"].count_batches() == 1


def test_existing_idempotent_batch_requires_exact_event_id_and_seq(env, monkeypatch):
    event = _events(env, ["new"])[0]
    batch = env["outbox"].create_batch(
        principal_id=env["principal"], endpoint_id=env["endpoint"],
        radar_id=env["radar"], payload_json={},
        events=[BatchEventRef(event["id"], event["event_seq"])],
        created_at=NOW.isoformat(), expires_at="2099-01-01T00:00:00+00:00",
    )
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(
            "UPDATE radar_notification_batch_events SET event_seq = event_seq + 100 "
            "WHERE notification_id = ?", (batch.notification_id,),
        )
    finally:
        conn.close()

    monkeypatch.setattr(
        selector_module,
        "_WINDOW_SQL",
        "SELECT e.* FROM radar_events e "
        "WHERE e.radar_id=? AND e.event_seq IS NOT NULL AND e.event_seq>? "
        "AND ? IS NOT NULL ORDER BY e.event_seq ASC LIMIT ?",
    )
    with pytest.raises(SelectorError, match="no representa"):
        env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)


def test_legacy_null_seq_is_ignored_and_baseline_window_advances(env):
    rows = _events(env, ["baseline", "baseline"])
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(
            "INSERT INTO radar_events (id,radar_id,owner,ts,kind,event_seq) "
            "VALUES ('legacy',?,?,?,'new',NULL)",
            (env["radar"], env["owner"], "1999-01-01T00:00:00+00:00"),
        )
    finally:
        conn.close()
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert (result.examined, result.eligible, result.created) == (2, 0, False)
    assert result.cursor_after == rows[-1]["event_seq"]
    assert env["outbox"].count_batches() == 0


@pytest.mark.parametrize("phase", [
    PHASE_AFTER_WINDOW_READ, PHASE_AFTER_BATCH_INSERT, PHASE_AFTER_ASSOCIATIONS,
    PHASE_BEFORE_CURSOR_UPDATE, PHASE_AFTER_CURSOR_UPDATE, PHASE_BEFORE_COMMIT,
])
def test_fault_at_every_phase_rolls_back_batch_associations_and_cursor(env, phase):
    _events(env, ["new", "changed_state"])
    class Boom(RuntimeError):
        pass
    def hook(current):
        if current == phase:
            raise Boom(phase)
    with pytest.raises(Boom):
        env["selector"].select_pair(
            env["endpoint"], env["radar"], now=NOW, _fault_hook=hook
        )
    assert _cursor(env) == 0
    assert env["outbox"].count_batches() == 0


def test_failure_on_second_association_rolls_back_everything(env):
    rows = _events(env, ["new", "new"])
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(
            f"CREATE TRIGGER fail_second BEFORE INSERT ON radar_notification_batch_events "
            f"WHEN NEW.event_id='{rows[1]['id']}' BEGIN SELECT RAISE(ABORT,'boom'); END"
        )
    finally:
        conn.close()
    with pytest.raises(Exception):
        env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert _cursor(env) == 0
    assert env["outbox"].count_batches() == 0


def test_scan_limit_processes_consecutive_windows_without_loss(env):
    rows = _events(env, ["new"] * 9)
    batches = []
    for _ in range(3):
        result = env["selector"].select_pair(
            env["endpoint"], env["radar"], now=NOW, scan_limit=3
        )
        batches.append(result.notification_id)
        assert (result.examined, result.associated) == (3, 3)
    assert _cursor(env) == rows[-1]["event_seq"]
    assert sum(len(env["outbox"].list_batch_events(batch)) for batch in batches) == 9
    assert len({ref.event_id for batch in batches for ref in env["outbox"].list_batch_events(batch)}) == 9


def test_max_items_only_limits_render_and_payload_keeps_all_groups(env):
    _events(env, ["new"] * 10)
    env["preferences"].update_preference(env["preference"], max_items=3)
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    batch = env["outbox"].get_batch(result.notification_id)
    payload = json.loads(batch.payload_json)
    # Todos los eventos quedan asociados y congelados aunque el render muestre
    # menos: cuántos se ven depende también del presupuesto del canal.
    assert len(env["outbox"].list_batch_events(result.notification_id)) == 10
    assert (len(payload["items"]), payload["total_items"], payload["max_items"]) == (10, 10, 3)
    rendered = render_payload(payload)
    assert rendered.item_count <= 3
    assert rendered.item_count + rendered.truncated_count == 10


def test_process_event_materializes_v2_with_frozen_source_metadata(env):
    _events(env, ["new"], ts="2026-07-16T15:30:00+00:00", extra={
        "dataset": "procesos", "modality": "Licitación pública",
        "closing_date": "2026-08-20T17:00:00",
    })
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    payload = json.loads(env["outbox"].get_batch(result.notification_id).payload_json)
    assert payload["version"] == "p1b-payload-v2"
    item = payload["items"][0]
    assert item["dataset"] == "procesos"
    assert item["modality"] == "Licitación pública"
    assert item["closing_date"] == "2026-08-20T17:00:00"
    assert item["observed_at"] == "2026-07-16T15:30:00+00:00"
    text = render_payload(payload).text
    assert "Recepción de ofertas reportada: 20 ago 2026, 17:00" in text
    assert "Observado: 16 jul 2026, 10:30" in text


def test_contract_event_materializes_contractor_and_signature(env):
    _events(env, ["changed_value"], extra={
        "dataset": "contratos", "contractor": "UNION TEMPORAL ALIMENTAR 2026",
        "signature_date": "2026-03-11", "prev_value": "900000000",
        "value": "1350000000",
    })
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    payload = json.loads(env["outbox"].get_batch(result.notification_id).payload_json)
    item = payload["items"][0]
    assert item["dataset"] == "contratos"
    assert item["contractor"] == "UNION TEMPORAL ALIMENTAR 2026"
    assert item["signature_date"] == "2026-03-11"
    text = render_payload(payload).text
    assert "Contratista reportado: UNION TEMPORAL ALIMENTAR 2026" in text
    assert "Firma reportada: 11 mar 2026" in text
    assert "Recepción de ofertas reportada" not in text


def test_preference_timezone_is_frozen_into_the_payload(env):
    _events(env, ["new"])
    env["preferences"].update_preference(env["preference"], timezone="America/New_York")
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    payload = json.loads(env["outbox"].get_batch(result.notification_id).payload_json)
    assert payload["timezone"] == "America/New_York"


def test_render_failure_rolls_back_batch_associations_and_cursor(env, monkeypatch):
    _events(env, ["new"] * 3)

    def boom(*args, **kwargs):
        raise ValueError("render v2 roto")

    monkeypatch.setattr(selector_module, "render_payload", boom)
    with pytest.raises(ValueError):
        env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert env["outbox"].count_batches() == 0
    assert _cursor(env) == 0
    conn = connect_sqlite(env["db"])
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM radar_notification_batch_events"
        ).fetchone()["c"] == 0
    finally:
        conn.close()


def test_same_result_key_is_one_payload_item_but_all_events_associated(env):
    _events(env, ["changed_state"] * 3, same_key=True)
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    payload = json.loads(env["outbox"].get_batch(result.notification_id).payload_json)
    assert len(payload["items"]) == 1
    assert payload["items"][0]["event_seqs"] == [1, 2, 3]
    assert len(env["outbox"].list_batch_events(result.notification_id)) == 3


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("UPDATE radar_notification_preferences SET enabled=0", "preference_disabled"),
        ("UPDATE notification_endpoints SET verified_at=NULL", "endpoint_unverified"),
        ("UPDATE notification_endpoints SET external_recipient_id=''", "endpoint_missing_recipient"),
        ("UPDATE notification_endpoints SET enabled=0", "endpoint_disabled"),
        ("UPDATE notification_endpoints SET revoked_at='x'", "endpoint_revoked"),
        ("UPDATE principals SET status='suspended'", "principal_suspended"),
        ("UPDATE radars SET archived_at='x'", "radar_archived"),
        ("DELETE FROM principal_owner_keys", "owner_not_linked"),
    ],
)
def test_ineligible_target_creates_nothing(env, mutation, reason):
    _events(env, ["new"])
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(mutation)
    finally:
        conn.close()
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert result.skipped_reason == reason
    assert result.created is False and env["outbox"].count_batches() == 0


def test_cross_principal_fails_closed(env):
    other = env["identity"].create_principal("beto@example.com")
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(
            "UPDATE notification_endpoints SET principal_id=? WHERE endpoint_id=?",
            (other, env["endpoint"]),
        )
    finally:
        conn.close()
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert result.skipped_reason == "principal_mismatch"


@pytest.mark.parametrize("column,value", [
    ("needs_attention", 1), ("next_run_after", "2999-01-01T00:00:00+00:00"),
    ("enabled", 0),
])
def test_operational_radar_state_does_not_block_materialized_events(env, column, value):
    _events(env, ["new"])
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(f"UPDATE radars SET {column}=? WHERE id=?", (value, env["radar"]))
    finally:
        conn.close()
    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert result.created is True and result.associated == 1


def test_two_endpoints_and_two_radars_have_independent_cursors(env):
    endpoint2 = env["endpoints"].create_endpoint(
        principal_id=env["principal"], channel="email", address="ana@example.com"
    )
    env["endpoints"].mark_verified(endpoint2, external_recipient_id="email-ana")
    pref2 = env["preferences"].create_preference(radar_id=env["radar"], endpoint_id=endpoint2)
    env["preferences"].set_enabled(pref2, True)
    radar2 = env["radars"].create_radar(
        name="Radar 2", owner=env["owner"], query_frame_json="{}"
    )
    pref3 = env["preferences"].create_preference(radar_id=radar2, endpoint_id=env["endpoint"])
    env["preferences"].set_enabled(pref3, True)
    _events(env, ["new"])
    env2 = dict(env, radar=radar2)
    _events(env2, ["new"], start=99)
    for endpoint, radar in ((env["endpoint"], env["radar"]), (endpoint2, env["radar"]),
                            (env["endpoint"], radar2)):
        result = env["selector"].select_pair(endpoint, radar, now=NOW)
        assert result.created
        assert _cursor(env, endpoint, radar) > 0
    assert env["outbox"].count_batches() == 3


def test_invalid_endpoint_a_does_not_block_endpoint_b(env):
    endpoint_b = env["endpoints"].create_endpoint(
        principal_id=env["principal"], channel="email", address="b@example.com"
    )
    env["endpoints"].mark_verified(endpoint_b, external_recipient_id="email-b")
    preference_b = env["preferences"].create_preference(
        radar_id=env["radar"], endpoint_id=endpoint_b
    )
    env["preferences"].set_enabled(preference_b, True)
    env["endpoints"].revoke(env["endpoint"])
    _events(env, ["new"])

    skipped = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    selected = env["selector"].select_pair(endpoint_b, env["radar"], now=NOW)

    assert skipped.skipped_reason == "endpoint_revoked"
    assert selected.created is True and selected.associated == 1


def test_two_concurrent_selectors_exactly_one_materializes_repeated_fifty_times(env):
    for iteration in range(50):
        _events(env, ["new"], start=iteration + 1)
        barrier = threading.Barrier(2)
        results, errors = [], []
        lock = threading.Lock()
        def run():
            selector = NotificationSelector(env["db"])
            barrier.wait()
            try:
                result = selector.select_pair(env["endpoint"], env["radar"], now=NOW)
                with lock:
                    results.append(result)
            except Exception as exc:  # pragma: no cover - diagnostic on failure
                with lock:
                    errors.append(exc)
        threads = [threading.Thread(target=run), threading.Thread(target=run)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        assert not any(thread.is_alive() for thread in threads)
        assert errors == []
        assert sum(result.created for result in results) == 1
    assert env["outbox"].count_batches() == 50


@pytest.mark.parametrize(
    "now,start,end,zone,expected",
    [
        (datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc), None, None, "UTC", None),
        (datetime(2026, 7, 15, 2, 0, tzinfo=timezone.utc), "01:00", "07:00", "UTC", "2026-07-15T07:00:00+00:00"),
        (datetime(2026, 7, 15, 8, 0, tzinfo=timezone.utc), "01:00", "07:00", "UTC", None),
        (datetime(2026, 7, 15, 22, 0, tzinfo=timezone.utc), "22:00", "07:00", "UTC", "2026-07-16T07:00:00+00:00"),
        (datetime(2026, 7, 16, 3, 0, tzinfo=timezone.utc), "22:00", "07:00", "UTC", "2026-07-16T07:00:00+00:00"),
        (datetime(2026, 7, 15, 7, 0, tzinfo=timezone.utc), "22:00", "07:00", "UTC", None),
        (datetime(2026, 7, 16, 2, 0, tzinfo=timezone.utc), "20:00", "22:00", "America/Bogota", "2026-07-16T03:00:00+00:00"),
    ],
)
def test_quiet_hours_boundaries_and_timezones(now, start, end, zone, expected):
    assert quiet_hours_not_before(now, start=start, end=end, timezone_name=zone) == expected


def test_equal_quiet_hours_disable_window_and_invalid_configuration_is_explicit():
    assert quiet_hours_not_before(NOW, start="07:00", end="07:00", timezone_name="UTC") is None
    with pytest.raises(SelectorError):
        quiet_hours_not_before(NOW, start="07:00", end=None, timezone_name="UTC")
    with pytest.raises(SelectorError):
        quiet_hours_not_before(NOW, start="7:00", end="08:00", timezone_name="UTC")
    with pytest.raises(SelectorError):
        quiet_hours_not_before(NOW, start="07:00", end="08:00", timezone_name="Mars/Olympus")


def test_invalid_timezone_rolls_back_batch_and_cursor(env):
    _events(env, ["new"])
    env["preferences"].update_preference(env["preference"], timezone="Mars/Olympus")
    with pytest.raises(SelectorError):
        env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert _cursor(env) == 0 and env["outbox"].count_batches() == 0


def test_expiration_map_uses_real_frequency_names():
    """R2B-1A · Solo ``per_run`` tiene expiración, porque solo ``per_run`` existe.

    Antes este test afirmaba las tres frecuencias, y era la prueba de que la
    frecuencia **solo** tocaba el TTL: no había ninguna otra aserción sobre ``daily``
    en toda la suite. El mapa de expiración era la única implementación de la
    cadencia diaria, que es como no tener ninguna.
    """
    assert expires_at_for_frequency(NOW, "per_run") == "2026-07-16T03:00:00+00:00"
    for deferred in ("daily", "weekly"):
        with pytest.raises(SelectorError):
            expires_at_for_frequency(NOW, deferred)
    with pytest.raises(SelectorError):
        expires_at_for_frequency(NOW, "cada_6h")


# ── R2B-1A · fail closed ──────────────────────────────────────────────────

def _force_column(env, column, value):
    """Escribe por SQL directo lo que el store ya no acepta.

    Es la única forma de construir la fila heredada que este bloque debe defender:
    el store la rechaza desde R2B-1A, así que sin esta puerta trasera el caso sería
    inconstruible y el fail-closed no tendría prueba.
    """
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(
            f"UPDATE radar_notification_preferences SET {column} = ? "
            f"WHERE preference_id = ?",
            (value, env["preference"]),
        )
    finally:
        conn.close()


def _cursor_of(env):
    conn = connect_sqlite(env["db"])
    try:
        row = conn.execute(
            "SELECT last_examined_seq FROM radar_notification_cursors "
            "WHERE endpoint_id = ? AND radar_id = ?",
            (env["endpoint"], env["radar"]),
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else row["last_examined_seq"]


def test_legacy_include_disappeared_skips_without_advancing_cursor(env):
    """12 · Preferencia heredada con include_disappeared=1 → ni batch ni cursor.

    El peligro no es entregar una desaparición —``is_event_eligible`` ya las descarta
    una a una—: es entregar **el resto** de los eventos y avanzar el cursor como si la
    preferencia se hubiera cumplido. Eso enterraría para siempre la diferencia entre
    "no hubo desapariciones" y "no sabemos verlas".
    """
    _events(env, ["new", "changed_state"])
    _force_column(env, "include_disappeared", 1)
    cursor_before = _cursor_of(env)

    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)

    assert result.skipped_reason == "include_disappeared_unsupported"
    assert (result.created, result.notification_id) == (False, None)
    assert result.associated == 0
    assert _cursor_of(env) == cursor_before
    assert env["outbox"].count_batches(radar_id=env["radar"]) == 0


def test_legacy_include_disappeared_resumes_once_cleared(env):
    """El skip es por estado, no una lápida: apagarlo devuelve el flujo normal."""
    _events(env, ["new"])
    _force_column(env, "include_disappeared", 1)
    assert env["selector"].select_pair(
        env["endpoint"], env["radar"], now=NOW
    ).created is False

    _force_column(env, "include_disappeared", 0)

    result = env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)
    assert result.created is True
    assert result.associated == 1


def test_unsupported_frequency_fails_closed_without_touching_cursor(env):
    """Una frecuencia que este código no implementa no materializa nada.

    Solo alcanzable por SQL directo sobre una base heredada —el CHECK vigente y el
    store la rechazan—, así que el estado es imposible, no inesperado: se levanta en
    vez de degradar a per_run, porque degradar sería entregar por corrida a quien
    pidió otra cosa.
    """
    _events(env, ["new"])
    cursor_before = _cursor_of(env)
    conn = connect_sqlite(env["db"])
    try:
        # El CHECK vigente rechaza 'daily', así que la fila heredada se fabrica
        # recreando la tabla sin él: es exactamente la base que el preflight de
        # schema.py detecta, vista desde el selector.
        conn.execute("PRAGMA writable_schema=ON")
        conn.execute(
            "UPDATE sqlite_master SET sql = REPLACE(sql, \"frequency = 'per_run'\", "
            "\"frequency IN ('per_run','daily')\") "
            "WHERE type='table' AND name='radar_notification_preferences'"
        )
        conn.execute("PRAGMA writable_schema=OFF")
    finally:
        conn.close()
    conn = connect_sqlite(env["db"])
    try:
        conn.execute(
            "UPDATE radar_notification_preferences SET frequency='daily' "
            "WHERE preference_id = ?",
            (env["preference"],),
        )
    finally:
        conn.close()

    with pytest.raises(SelectorError, match="frecuencia"):
        env["selector"].select_pair(env["endpoint"], env["radar"], now=NOW)

    assert _cursor_of(env) == cursor_before
    assert env["outbox"].count_batches(radar_id=env["radar"]) == 0


# ── R2B-1C-A · una sola implementación de quiet hours ─────────────────────────

def test_selector_delegates_quiet_hours_and_translates_the_error():
    """El wrapper es lo único que puede vivir aquí: una segunda copia del algoritmo
    se separaría de la del worker, y el silencio dejaría de ser una propiedad del
    sistema para ser una coincidencia entre dos módulos."""
    from app.core.notify.quiet_hours import QuietHoursError

    with pytest.raises(SelectorError):
        quiet_hours_not_before(
            NOW, start="22:00", end="07:00", timezone_name="Marte/Olympus"
        )
    # Y sigue siendo la misma respuesta que la política compartida.
    from app.core.notify.quiet_hours import quiet_hours_not_before as shared

    for start, end in (("22:00", "07:00"), ("01:00", "07:00"), (None, None)):
        assert quiet_hours_not_before(
            NOW, start=start, end=end, timezone_name="America/Bogota"
        ) == shared(NOW, start=start, end=end, timezone_name="America/Bogota")


def test_selector_does_not_reimplement_the_window_algorithm():
    """Estructural (AST): el selector no puede volver a tener su propio ZoneInfo ni su
    propia aritmética de ventana. El wrapper sí; el algoritmo no."""
    import ast
    import pathlib

    tree = ast.parse(pathlib.Path(selector_module.__file__).read_text())
    names = {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
    } | {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    for forbidden in ("ZoneInfo", "ZoneInfoNotFoundError", "timetz", "_parse_hhmm"):
        assert forbidden not in names, f"el selector reimplementa quiet hours: {forbidden}"

    imported = {
        alias.name
        for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert "quiet_hours_not_before" in imported, "debe delegar en la política compartida"
