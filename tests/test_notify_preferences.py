"""Tests de app/core/notify/preference_store.py — qué aceptó recibir la persona."""
from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
import threading
import time

import pytest

from app.core.db import connect_sqlite
from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker
from app.core.notify.outbox_store import OutboxStore
from app.core.notify.preference_store import (
    PHASE_AFTER_CURSOR_UPSERT,
    PHASE_AFTER_ENABLE,
    EndpointRevokedError,
    OwnerNotLinkedError,
    PreferenceError,
    PreferenceExistsError,
    PreferenceStore,
    PrincipalMismatchError,
    RadarNotAvailableError,
    UnsupportedPreferenceError,
)
from app.core.notify.selector import NotificationSelector
from app.core.radar.store import RadarStore

_NOW = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc)

_ANA_OWNER = "email:ana@example.com"
_BETO_OWNER = "email:beto@example.com"


def _frame_json():
    return json.dumps({"dataset": "contratos", "topic": ["adulto_mayor"]})


@pytest.fixture
def db(tmp_path):
    return tmp_path / "notify.db"


@pytest.fixture
def radars(db):
    """RadarStore primero: la FK de preferencias apunta a ``radars``."""
    return RadarStore(db)


@pytest.fixture
def identity(db, radars):
    return IdentityStore(db)


@pytest.fixture
def endpoints(db, identity):
    return EndpointStore(db, masker=Masker.from_key("clave-de-prueba"))


@pytest.fixture
def preferences(db, endpoints):
    return PreferenceStore(db)


@pytest.fixture
def ana(identity, endpoints):
    """Persona completa: principal + owner_key vinculado + endpoint verificado."""
    pid = identity.create_principal("ana@example.com")
    identity.link_owner_key(_ANA_OWNER, pid)
    eid = endpoints.create_endpoint(
        principal_id=pid, channel="whatsapp", address="+573001234567"
    )
    endpoints.mark_verified(eid, external_recipient_id="chat-ana")
    return {"principal_id": pid, "endpoint_id": eid, "owner": _ANA_OWNER}


@pytest.fixture
def beto(identity, endpoints):
    pid = identity.create_principal("beto@example.com")
    identity.link_owner_key(_BETO_OWNER, pid)
    eid = endpoints.create_endpoint(
        principal_id=pid, channel="whatsapp", address="+573009999999"
    )
    endpoints.mark_verified(eid, external_recipient_id="chat-beto")
    return {"principal_id": pid, "endpoint_id": eid, "owner": _BETO_OWNER}


def _radar(radars, owner, name="Radar PAE"):
    return radars.create_radar(
        name=name, owner=owner, query_frame_json=_frame_json()
    )


# ── opt-in ────────────────────────────────────────────────────────────────

def test_preference_is_created_disabled(preferences, radars, ana):
    """Guardar un radar no es consentir que te escriban."""
    rid = _radar(radars, ana["owner"])

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    pref = preferences.get_preference(pref_id)
    assert pref.enabled == 0
    assert not pref.is_enabled


def test_verified_endpoint_does_not_auto_enable_the_preference(
    preferences, radars, endpoints, ana
):
    """Verificar prueba que el destino es tuyo, no que quieras usarlo aquí."""
    assert endpoints.is_eligible(ana["endpoint_id"])
    rid = _radar(radars, ana["owner"])

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    assert preferences.get_preference(pref_id).enabled == 0


def test_enabling_is_an_explicit_action(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    preferences.set_enabled(pref_id, True)

    assert preferences.get_preference(pref_id).is_enabled


# ── defaults del contrato ─────────────────────────────────────────────────

def test_defaults_match_the_contract(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])

    pref = preferences.get_preference(
        preferences.create_preference(radar_id=rid, endpoint_id=ana["endpoint_id"])
    )

    assert pref.timezone == "America/Bogota"
    assert pref.frequency == "per_run"
    assert pref.max_items == 10
    assert (pref.include_new, pref.include_state_changes) == (1, 1)
    assert (pref.include_value_changes, pref.include_disappeared) == (1, 0)
    assert (pref.quiet_hours_start, pref.quiet_hours_end) == (None, None)


def test_timezone_is_persisted_explicitly(preferences, radars, ana):
    """Explícita, no implícita: una hora de silencio sin zona no significa nada."""
    rid = _radar(radars, ana["owner"])

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"], timezone="America/Lima",
    )

    assert preferences.get_preference(pref_id).timezone == "America/Lima"


# ── aislamiento entre personas ────────────────────────────────────────────

def test_endpoint_of_another_principal_is_rejected(preferences, radars, ana, beto):
    """El corazón del bloque: las FK no impiden esto, solo este control.

    Radar de Ana + endpoint de Beto es una escritura válida para SQLite: ambas
    filas existen. Sin la comprobación, Beto recibiría las búsquedas de Ana.
    """
    rid_ana = _radar(radars, ana["owner"])

    with pytest.raises(PrincipalMismatchError):
        preferences.create_preference(
            radar_id=rid_ana, endpoint_id=beto["endpoint_id"]
        )

    assert preferences.list_preferences_for_radar(rid_ana) == []


def test_each_principal_can_use_their_own_endpoint(preferences, radars, ana, beto):
    """La contracara: el control no puede bloquear el caso legítimo."""
    rid_ana = _radar(radars, ana["owner"])
    rid_beto = _radar(radars, beto["owner"])

    preferences.create_preference(radar_id=rid_ana, endpoint_id=ana["endpoint_id"])
    preferences.create_preference(radar_id=rid_beto, endpoint_id=beto["endpoint_id"])

    assert len(preferences.list_preferences_for_radar(rid_ana)) == 1
    assert len(preferences.list_preferences_for_radar(rid_beto)) == 1


def test_radar_owner_without_principal_is_rejected(preferences, radars, ana):
    """Un radar de un chat anónimo no tiene dueño conocido: no se adivina."""
    rid = _radar(radars, "anon:sin-vincular")

    with pytest.raises(OwnerNotLinkedError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"]
        )


def test_unknown_radar_is_rejected(preferences, ana):
    """Distinto de "owner sin vincular": el radar ni existe."""
    with pytest.raises(RadarNotAvailableError):
        preferences.create_preference(
            radar_id="radar-que-no-existe", endpoint_id=ana["endpoint_id"]
        )


def test_archived_radar_cannot_get_a_preference(preferences, radars, ana):
    """Un opt-in que nada puede cumplir no es un opt-in.

    El radar archivado no corre —el lease se lo niega—, así que consentir
    notificaciones sobre él crea una promesa muerta: el usuario cree que le
    avisarán y nunca pasa nada.
    """
    rid = _radar(radars, ana["owner"])
    radars.archive_radar(rid)

    with pytest.raises(RadarNotAvailableError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"]
        )

    assert preferences.list_preferences_for_radar(rid) == []


def test_restoring_the_radar_allows_the_preference_again(preferences, radars, ana):
    """El rechazo es por estado, no permanente."""
    rid = _radar(radars, ana["owner"])
    radars.archive_radar(rid)

    radars.restore_radar(rid)

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    assert preferences.get_preference(pref_id) is not None


def test_archiving_does_not_destroy_an_existing_preference(preferences, radars, ana):
    """Archivar el radar no revoca lo que la persona ya consintió.

    Solo bloquea crear preferencias nuevas. Si se restaura, el opt-in sigue tal
    como lo dejó —igual que needs_attention y enabled sobreviven al archivado—.
    """
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    preferences.set_enabled(pref_id, True)

    radars.archive_radar(rid)

    assert preferences.get_preference(pref_id).is_enabled


def test_unknown_endpoint_is_rejected(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])

    with pytest.raises(PreferenceError):
        preferences.create_preference(radar_id=rid, endpoint_id="no-existe")


def test_linking_the_owner_later_makes_it_work(preferences, radars, identity, ana):
    """El rechazo es por falta de vínculo, no permanente."""
    rid = _radar(radars, "anon:tardio")
    with pytest.raises(OwnerNotLinkedError):
        preferences.create_preference(radar_id=rid, endpoint_id=ana["endpoint_id"])

    identity.link_owner_key("anon:tardio", ana["principal_id"])

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    assert preferences.get_preference(pref_id) is not None


def test_second_owner_key_of_the_same_person_also_resolves(
    preferences, radars, identity, ana
):
    """El caso que motiva la indirección: misma persona, otro owner histórico."""
    identity.link_owner_key("anon:ana-vieja", ana["principal_id"])
    rid = _radar(radars, "anon:ana-vieja", name="Radar viejo")

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    assert preferences.get_preference(pref_id) is not None


# ── unicidad y validación ─────────────────────────────────────────────────

def test_unique_radar_endpoint(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])
    preferences.create_preference(radar_id=rid, endpoint_id=ana["endpoint_id"])

    with pytest.raises(PreferenceExistsError):
        preferences.create_preference(radar_id=rid, endpoint_id=ana["endpoint_id"])


@pytest.mark.parametrize("max_items", [0, -1, -10])
def test_non_positive_max_items_is_rejected(preferences, radars, ana, max_items):
    rid = _radar(radars, ana["owner"])

    with pytest.raises(PreferenceError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"], max_items=max_items,
        )


@pytest.mark.parametrize("bad", ["25:00", "12:60", "9:00", "0900", "mañana", ""])
def test_invalid_quiet_hours_are_rejected(preferences, radars, ana, bad):
    rid = _radar(radars, ana["owner"])

    with pytest.raises(PreferenceError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"],
            quiet_hours_start=bad, quiet_hours_end="08:00",
        )


def test_valid_quiet_hours_are_accepted(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"],
        quiet_hours_start="22:00", quiet_hours_end="07:30",
    )

    pref = preferences.get_preference(pref_id)
    assert (pref.quiet_hours_start, pref.quiet_hours_end) == ("22:00", "07:30")


def test_half_a_quiet_window_is_rejected(preferences, radars, ana):
    """Un inicio sin fin no define ninguna ventana."""
    rid = _radar(radars, ana["owner"])

    with pytest.raises(PreferenceError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"], quiet_hours_start="22:00",
        )


def test_invalid_frequency_is_rejected(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])

    with pytest.raises(PreferenceError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"], frequency="cada rato",
        )


def test_empty_timezone_is_rejected(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])

    with pytest.raises(PreferenceError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"], timezone="  ",
        )


# ── update ────────────────────────────────────────────────────────────────

def test_update_applies_and_validates(preferences, radars, ana):
    """R2B-1A · Antes este test afirmaba que ``daily`` e ``include_disappeared=True``
    persistían: bendecía exactamente los dos comportamientos que el plan P1B había
    ordenado rechazar. Ahora comprueba lo que sí se puede cambiar."""
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    preferences.update_preference(
        pref_id, frequency="per_run", max_items=3, include_state_changes=False,
    )

    pref = preferences.get_preference(pref_id)
    assert (pref.frequency, pref.max_items, pref.include_state_changes) == (
        "per_run", 3, 0,
    )


def test_update_rejects_invalid_values(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    with pytest.raises(PreferenceError):
        preferences.update_preference(pref_id, max_items=0)

    assert preferences.get_preference(pref_id).max_items == 10   # sin tocar


def test_update_cannot_move_a_preference_to_another_radar(preferences, radars, ana):
    """Reasignar el radar saltaría el control de aislamiento del alta."""
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    with pytest.raises(PreferenceError):
        preferences.update_preference(pref_id, radar_id="otro")
    with pytest.raises(PreferenceError):
        preferences.update_preference(pref_id, endpoint_id="otro")


def test_update_validates_quiet_hours_against_the_stored_pair(
    preferences, radars, ana
):
    """Cambiar solo el fin no puede dejar una ventana a medias ni inválida."""
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"],
        quiet_hours_start="22:00", quiet_hours_end="07:00",
    )

    with pytest.raises(PreferenceError):
        preferences.update_preference(pref_id, quiet_hours_end="99:99")

    preferences.update_preference(pref_id, quiet_hours_end="06:00")
    assert preferences.get_preference(pref_id).quiet_hours_end == "06:00"


def test_update_touches_updated_at(preferences, radars, ana):
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    before = preferences.get_preference(pref_id)

    preferences.update_preference(pref_id, max_items=5)

    assert preferences.get_preference(pref_id).updated_at >= before.updated_at


def test_list_preferences_for_endpoint(preferences, radars, ana):
    rid_a = _radar(radars, ana["owner"], name="Radar A")
    rid_b = _radar(radars, ana["owner"], name="Radar B")
    preferences.create_preference(radar_id=rid_a, endpoint_id=ana["endpoint_id"])
    preferences.create_preference(radar_id=rid_b, endpoint_id=ana["endpoint_id"])

    prefs = preferences.list_preferences_for_endpoint(ana["endpoint_id"])

    assert {p.radar_id for p in prefs} == {rid_a, rid_b}


# ── R2B-1A · frecuencia: solo per_run ─────────────────────────────────────

def _cursor(db_path, endpoint_id, radar_id):
    conn = connect_sqlite(db_path)
    try:
        row = conn.execute(
            "SELECT last_examined_seq FROM radar_notification_cursors "
            "WHERE endpoint_id = ? AND radar_id = ?",
            (endpoint_id, radar_id),
        ).fetchone()
    finally:
        conn.close()
    return None if row is None else row["last_examined_seq"]


def _seed_events(radars, radar_id, owner, count, *, start=1):
    radars.record_events([
        {
            "radar_id": radar_id, "owner": owner, "radar_name": "Radar PAE",
            "run_id": "run", "ts": "2026-07-15T10:00:00+00:00", "kind": "new",
            "result_key": f"K-{n}", "dataset": "procesos", "title": f"Proceso {n}",
            "entity": "Entidad", "value": str(n * 100), "status": "abierto",
            "prev_status": None, "prev_value": None,
            "urlproceso": f"https://secop/{n}", "referencia": f"REF-{n}",
        }
        for n in range(start, start + count)
    ])


def test_create_accepts_per_run(preferences, radars, ana):
    """1 · per_run es la única frecuencia que el sistema sabe cumplir."""
    rid = _radar(radars, ana["owner"])

    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"], frequency="per_run"
    )

    assert preferences.get_preference(pref_id).frequency == "per_run"


@pytest.mark.parametrize("deferred", ["daily", "weekly"])
def test_create_rejects_deferred_frequencies(preferences, radars, ana, deferred):
    """2, 3 · daily/weekly se rechazan: aceptarlas prometía una cadencia que nadie
    implementaba y entregaba una notificación por corrida."""
    rid = _radar(radars, ana["owner"])

    with pytest.raises(UnsupportedPreferenceError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"], frequency=deferred
        )

    assert preferences.list_preferences_for_radar(rid) == []


@pytest.mark.parametrize("deferred", ["daily", "weekly"])
def test_update_rejects_deferred_frequencies(preferences, radars, ana, deferred):
    """4 · Tampoco por la puerta de atrás del update."""
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    with pytest.raises(UnsupportedPreferenceError):
        preferences.update_preference(pref_id, frequency=deferred)

    assert preferences.get_preference(pref_id).frequency == "per_run"


@pytest.mark.parametrize("deferred", ["daily", "weekly"])
def test_check_constraint_rejects_direct_sql_frequency(
    preferences, radars, ana, db, deferred
):
    """5 · El CHECK es la última línea: una escritura SQL directa que esquive el
    store tampoco puede plantar una frecuencia que nadie sabe cumplir."""
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    conn = connect_sqlite(db)
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE radar_notification_preferences SET frequency = ? "
                "WHERE preference_id = ?",
                (deferred, pref_id),
            )
    finally:
        conn.close()

    assert preferences.get_preference(pref_id).frequency == "per_run"


# ── R2B-1A · opt-in sin backfill ──────────────────────────────────────────

def test_enable_plants_cursor_at_current_max_and_selects_nothing(
    preferences, radars, ana, db
):
    """6 · Radar con 500 eventos históricos: encender no entrega ninguno.

    Es el caso que define la operación. Sin plantar el cursor, el primer mensaje de
    alguien que acaba de decir "sí" sería un volcado de 500 eventos anteriores a su
    consentimiento.
    """
    rid = _radar(radars, ana["owner"])
    _seed_events(radars, rid, ana["owner"], 500)
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    preferences.enable_without_backfill(pref_id)

    conn = connect_sqlite(db)
    try:
        max_seq = conn.execute(
            "SELECT MAX(event_seq) AS m FROM radar_events WHERE radar_id = ?", (rid,)
        ).fetchone()["m"]
    finally:
        conn.close()
    assert _cursor(db, ana["endpoint_id"], rid) == max_seq

    result = NotificationSelector(db).select_pair(
        ana["endpoint_id"], rid, now=_NOW
    )
    assert (result.created, result.notification_id) == (False, None)
    assert result.skipped_reason == "empty_window"
    assert OutboxStore(db).count_batches(radar_id=rid) == 0


def test_events_after_opt_in_are_notified(preferences, radars, ana, db):
    """7 · El corte es un corte, no un silencio: lo posterior al opt-in sí llega."""
    rid = _radar(radars, ana["owner"])
    _seed_events(radars, rid, ana["owner"], 500)
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    preferences.enable_without_backfill(pref_id)

    _seed_events(radars, rid, ana["owner"], 1, start=501)

    result = NotificationSelector(db).select_pair(ana["endpoint_id"], rid, now=_NOW)

    assert result.created is True
    assert result.associated == 1
    events = OutboxStore(db).list_batch_events(result.notification_id)
    assert [e.event_id for e in events] == [_last_event_id(db, rid)]


def _last_event_id(db_path, radar_id):
    conn = connect_sqlite(db_path)
    try:
        return conn.execute(
            "SELECT id FROM radar_events WHERE radar_id = ? "
            "ORDER BY event_seq DESC LIMIT 1",
            (radar_id,),
        ).fetchone()["id"]
    finally:
        conn.close()


def test_fault_between_cursor_and_enable_rolls_back_both(
    preferences, radars, ana, db
):
    """8 · Fallo tras plantar el cursor y antes de habilitar → nada.

    Es la mitad peligrosa: un cursor avanzado sobre una preferencia apagada perdería
    en silencio toda la historia intermedia cuando alguien la encendiera después.
    """
    rid = _radar(radars, ana["owner"])
    _seed_events(radars, rid, ana["owner"], 10)
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    def boom(phase):
        if phase == PHASE_AFTER_CURSOR_UPSERT:
            raise RuntimeError("caída tras plantar el cursor")

    with pytest.raises(RuntimeError):
        preferences.enable_without_backfill(pref_id, _fault_hook=boom)

    assert preferences.get_preference(pref_id).enabled == 0
    assert _cursor(db, ana["endpoint_id"], rid) is None


def test_fault_after_enable_before_commit_rolls_back_both(
    preferences, radars, ana, db
):
    """9 · Fallo tras habilitar y antes del COMMIT → nada."""
    rid = _radar(radars, ana["owner"])
    _seed_events(radars, rid, ana["owner"], 10)
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    def boom(phase):
        if phase == PHASE_AFTER_ENABLE:
            raise RuntimeError("caída tras habilitar")

    with pytest.raises(RuntimeError):
        preferences.enable_without_backfill(pref_id, _fault_hook=boom)

    assert preferences.get_preference(pref_id).enabled == 0
    assert _cursor(db, ana["endpoint_id"], rid) is None


def test_concurrent_event_falls_unambiguously_on_one_side_of_the_cut(
    preferences, radars, ana, db
):
    """10 · Linearización: no hay ventana entre MAX(event_seq) y enabled=1.

    Una segunda conexión intenta insertar un evento justo cuando el opt-in ya tiene
    el lock de escritor y ya leyó el máximo. Reservar un ``event_seq`` es una
    escritura, así que esa inserción **no puede** colarse entre la lectura y el
    commit: espera. Al terminar, el evento queda por encima del cursor y se notifica
    —nunca se pierde por caer en un hueco que no existe.
    """
    rid = _radar(radars, ana["owner"])
    _seed_events(radars, rid, ana["owner"], 3)
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    errors = []
    started = threading.Event()
    writer_done = threading.Event()
    breaches = []

    def insert_concurrently():
        started.wait(timeout=5)
        try:
            _seed_events(radars, rid, ana["owner"], 1, start=99)
        except BaseException as exc:  # pragma: no cover - solo si el lock falla
            errors.append(exc)
        finally:
            writer_done.set()

    writer = threading.Thread(target=insert_concurrently)

    def hook(phase):
        if phase == PHASE_AFTER_CURSOR_UPSERT:
            writer.start()
            started.set()
            # Aquí el opt-in tiene el lock de escritor y ya plantó el cursor. El
            # escritor concurrente **tiene que** estar bloqueado: reservar un
            # ``event_seq`` es una escritura, y en WAL solo hay un escritor.
            #
            # Esta espera es la aserción, no una comodidad. Sin ``BEGIN IMMEDIATE``
            # el escritor entraría durante estos 300 ms y ``writer_done`` quedaría
            # marcado: es lo que convierte este test en una prueba del lock y no en
            # una descripción de lo que pasó.
            time.sleep(0.3)
            if writer_done.is_set():
                breaches.append(
                    "el escritor concurrente commiteó mientras el opt-in tenía el "
                    "lock: no hay exclusión y el corte no es un corte"
                )

    try:
        preferences.enable_without_backfill(pref_id, _fault_hook=hook)
    finally:
        started.set()
        writer.join(timeout=10)
    assert breaches == []
    assert errors == []

    cursor_value = _cursor(db, ana["endpoint_id"], rid)
    conn = connect_sqlite(db)
    try:
        rows = conn.execute(
            "SELECT event_seq FROM radar_events WHERE radar_id = ? ORDER BY event_seq",
            (rid,),
        ).fetchall()
    finally:
        conn.close()
    seqs = [r["event_seq"] for r in rows]
    assert len(seqs) == 4
    # Cada evento cae de un lado del corte, y ninguno se pierde: los 3 anteriores son
    # historia, el concurrente es futuro.
    assert sum(1 for s in seqs if s <= cursor_value) == 3
    assert sum(1 for s in seqs if s > cursor_value) == 1

    result = NotificationSelector(db).select_pair(ana["endpoint_id"], rid, now=_NOW)
    assert result.associated == 1


def test_disable_keeps_cursor_and_history(preferences, radars, ana, db):
    """Apagar no borra el corte: re-encender no puede volcar la historia."""
    rid = _radar(radars, ana["owner"])
    _seed_events(radars, rid, ana["owner"], 5)
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    preferences.enable_without_backfill(pref_id)
    planted = _cursor(db, ana["endpoint_id"], rid)

    preferences.set_enabled(pref_id, False)

    assert preferences.get_preference(pref_id).enabled == 0
    assert _cursor(db, ana["endpoint_id"], rid) == planted


def test_set_enabled_true_delegates_and_plants_the_cursor(
    preferences, radars, ana, db
):
    """Una sola ruta para encender: set_enabled(True) no puede ser la que olvida
    plantar el cursor."""
    rid = _radar(radars, ana["owner"])
    _seed_events(radars, rid, ana["owner"], 7)
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    preferences.set_enabled(pref_id, True)

    assert preferences.get_preference(pref_id).is_enabled
    assert _cursor(db, ana["endpoint_id"], rid) == 7


def test_enable_revalidates_archived_radar(preferences, radars, ana):
    """El permiso se comprueba contra el estado del instante en que se concede."""
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    radars.archive_radar(rid)

    with pytest.raises(RadarNotAvailableError):
        preferences.enable_without_backfill(pref_id)

    assert preferences.get_preference(pref_id).enabled == 0


def test_enable_revalidates_revoked_endpoint(preferences, radars, endpoints, ana):
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )
    endpoints.revoke(ana["endpoint_id"])

    with pytest.raises(EndpointRevokedError):
        preferences.enable_without_backfill(pref_id)

    assert preferences.get_preference(pref_id).enabled == 0


# ── R2B-1A · include_disappeared no está disponible ───────────────────────

def test_create_rejects_include_disappeared(preferences, radars, ana):
    """11 · No se ofrece una función que tres capas hacen imposible."""
    rid = _radar(radars, ana["owner"])

    with pytest.raises(UnsupportedPreferenceError):
        preferences.create_preference(
            radar_id=rid, endpoint_id=ana["endpoint_id"], include_disappeared=True
        )

    assert preferences.list_preferences_for_radar(rid) == []


def test_update_rejects_include_disappeared(preferences, radars, ana):
    """11 · Tampoco por update."""
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    with pytest.raises(UnsupportedPreferenceError):
        preferences.update_preference(pref_id, include_disappeared=True)

    assert preferences.get_preference(pref_id).include_disappeared == 0
