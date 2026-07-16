"""Tests de app/core/notify/preference_store.py — qué aceptó recibir la persona."""
from __future__ import annotations

import json

import pytest

from app.core.notify.endpoint_store import EndpointStore
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker
from app.core.notify.preference_store import (
    OwnerNotLinkedError,
    PreferenceError,
    PreferenceExistsError,
    PreferenceStore,
    PrincipalMismatchError,
    RadarNotAvailableError,
)
from app.core.radar.store import RadarStore

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
    rid = _radar(radars, ana["owner"])
    pref_id = preferences.create_preference(
        radar_id=rid, endpoint_id=ana["endpoint_id"]
    )

    preferences.update_preference(
        pref_id, frequency="daily", max_items=3, include_disappeared=True,
    )

    pref = preferences.get_preference(pref_id)
    assert (pref.frequency, pref.max_items, pref.include_disappeared) == ("daily", 3, 1)


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
