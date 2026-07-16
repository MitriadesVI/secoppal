"""Tests de app/core/notify/endpoint_store.py y masking.py — a dónde y si se puede."""
from __future__ import annotations

import sqlite3

import pytest

from app.core.notify.endpoint_store import (
    EndpointError,
    EndpointExistsError,
    EndpointStore,
)
from app.core.notify.identity_store import IdentityStore
from app.core.notify.masking import Masker, last4

_ADDRESS = "+573001234567"
_EMAIL = "ana@example.com"


@pytest.fixture
def masker():
    return Masker.from_key("clave-de-prueba-no-productiva")


@pytest.fixture
def db(tmp_path):
    return tmp_path / "notify.db"


@pytest.fixture
def identity(db):
    return IdentityStore(db)


@pytest.fixture
def endpoints(db, masker, identity):
    return EndpointStore(db, masker=masker)


@pytest.fixture
def ana(identity):
    return identity.create_principal(_EMAIL)


def _verified_endpoint(
    endpoints, principal_id, address=_ADDRESS, channel="whatsapp",
    recipient="chat-123",
):
    eid = endpoints.create_endpoint(
        principal_id=principal_id, channel=channel, address=address,
    )
    endpoints.mark_verified(eid, external_recipient_id=recipient)
    return eid


# ── masking ───────────────────────────────────────────────────────────────

def test_masker_requires_a_key():
    """Sin clave, el hash de un teléfono se rompe por fuerza bruta en minutos."""
    with pytest.raises(ValueError):
        Masker.from_key("")


def test_hash_is_stable_and_keyed(masker):
    other = Masker.from_key("otra-clave")

    assert masker.address_hash("whatsapp", _ADDRESS) == masker.address_hash(
        "whatsapp", _ADDRESS
    )
    assert masker.address_hash("whatsapp", _ADDRESS) != other.address_hash(
        "whatsapp", _ADDRESS
    )


def test_hash_separates_channels(masker):
    """El mismo string en dos canales son dos destinos distintos."""
    assert masker.address_hash("whatsapp", _ADDRESS) != masker.address_hash(
        "telegram", _ADDRESS
    )


def test_email_hash_normalizes_with_auth_policy(masker):
    assert masker.address_hash("email", "  Ana@Example.COM ") == masker.address_hash(
        "email", "ana@example.com"
    )


def test_safe_owner_never_returns_the_email(masker):
    safe = masker.safe_owner("email:ana@example.com")

    assert "ana@example.com" not in safe
    assert "ana" not in safe
    assert "@" not in safe
    assert safe.startswith("email:#")       # el canal sí sirve para depurar


def test_safe_owner_handles_anon_and_bare_keys(masker):
    assert masker.safe_owner("anon:abc").startswith("anon:#")
    assert "abc" not in masker.safe_owner("anon:abc")
    assert masker.safe_owner("12345").startswith("#")
    assert masker.safe_owner("") == "anon:#empty"


def test_safe_owner_is_stable_for_correlation(masker):
    """Dos líneas de log del mismo owner deben poder correlacionarse."""
    assert masker.safe_owner("email:ana@example.com") == masker.safe_owner(
        "email:ana@example.com"
    )
    assert masker.safe_owner("email:ana@example.com") != masker.safe_owner(
        "email:beto@example.com"
    )


def test_last4_is_only_for_recognition():
    assert last4(_ADDRESS) == "4567"


# ── endpoints ─────────────────────────────────────────────────────────────

def test_full_address_is_never_persisted(endpoints, ana, db):
    """El barrido es sobre toda la base, no sobre la columna que esperamos.

    Si mañana alguien añade una columna "para debug", este test lo ve.

    La dirección de contacto del endpoint se usa a propósito **distinta** del
    email del principal: ``principals.email`` sí vive en claro —es la identidad
    de la cuenta, igual que ``auth.app_accounts.email``, y sin él no hay login ni
    unicidad de persona—. Lo que este test protege es otra cosa: el destino al
    que se notifica. Si ambos fueran el mismo string, encontrarlo en el dump no
    diría cuál de los dos lo puso.
    """
    contact_email = "contacto-de-ana@otrodominio.com"
    endpoints.create_endpoint(principal_id=ana, channel="whatsapp", address=_ADDRESS)
    endpoints.create_endpoint(
        principal_id=ana, channel="email", address=contact_email
    )

    conn = sqlite3.connect(db)
    try:
        dump = "\n".join(conn.iterdump())
    finally:
        conn.close()

    assert _ADDRESS not in dump
    assert "3001234" not in dump          # ni el número sin el prefijo
    assert contact_email not in dump      # la dirección de notificación, no
    assert "otrodominio" not in dump      # ni siquiera su dominio
    assert "4567" in dump                 # last4 sí, para reconocimiento


def test_new_endpoint_is_not_eligible_until_verified(endpoints, ana):
    """Sin verificar, cualquiera registra el número de otro."""
    eid = endpoints.create_endpoint(
        principal_id=ana, channel="whatsapp", address=_ADDRESS
    )

    assert endpoints.get_endpoint(eid).verified_at is None
    assert not endpoints.is_eligible(eid)
    assert endpoints.list_eligible_endpoints(ana) == []


def test_new_endpoint_has_no_recipient_id_yet(endpoints, ana):
    """El recipient id lo emite el proveedor al verificar; antes no existe."""
    eid = endpoints.create_endpoint(
        principal_id=ana, channel="whatsapp", address=_ADDRESS
    )

    assert endpoints.get_endpoint(eid).external_recipient_id is None


def test_verified_endpoint_is_eligible(endpoints, ana):
    eid = _verified_endpoint(endpoints, ana)

    assert endpoints.is_eligible(eid)
    assert [e.endpoint_id for e in endpoints.list_eligible_endpoints(ana)] == [eid]


def test_verification_writes_the_recipient_id_with_the_proof(endpoints, ana):
    """Verificado y entregable son el mismo hecho, escrito a la vez."""
    eid = _verified_endpoint(endpoints, ana, recipient="  chat-777  ")

    endpoint = endpoints.get_endpoint(eid)
    assert endpoint.verified_at is not None
    assert endpoint.external_recipient_id == "chat-777"     # recortado


@pytest.mark.parametrize("recipient", ["", "   ", None])
def test_verification_without_recipient_id_is_rejected(endpoints, ana, recipient):
    """Un endpoint verificado sin destinatario sería elegible e inentregable.

    El fallo aparecería en el momento del envío, con el mensaje ya formado y el
    usuario esperándolo. Aquí ese estado no se puede ni representar.
    """
    eid = endpoints.create_endpoint(
        principal_id=ana, channel="whatsapp", address=_ADDRESS
    )

    with pytest.raises(EndpointError):
        endpoints.mark_verified(eid, external_recipient_id=recipient)

    assert endpoints.get_endpoint(eid).verified_at is None   # no verificó a medias
    assert not endpoints.is_eligible(eid)


def test_endpoint_with_empty_recipient_id_is_not_eligible(endpoints, ana, db):
    """Defensa del filtro: NOT NULL no distingue el string vacío.

    Se escribe por SQL directo porque la API ya no permite llegar a ese estado;
    el filtro debe aguantar igual una fila heredada o escrita a mano.
    """
    eid = endpoints.create_endpoint(
        principal_id=ana, channel="whatsapp", address=_ADDRESS
    )
    conn = sqlite3.connect(db)
    try:
        conn.execute(
            "UPDATE notification_endpoints "
            "SET verified_at = '2026-01-01T00:00:00+00:00', external_recipient_id = '' "
            "WHERE endpoint_id = ?",
            (eid,),
        )
        conn.commit()
    finally:
        conn.close()

    assert not endpoints.is_eligible(eid)
    assert endpoints.list_eligible_endpoints(ana) == []


def test_verification_cannot_redirect_an_already_verified_endpoint(endpoints, ana):
    """Redirigir un destino ya probado falla **ruidosamente**, no en silencio.

    Antes era un no-op: el recipient no cambiaba, pero el intento —que es un bug
    o alguien apropiándose de las notificaciones de otro— quedaba escondido tras
    una llamada que parecía haber funcionado.
    """
    eid = _verified_endpoint(endpoints, ana, recipient="chat-legitimo")

    with pytest.raises(EndpointError):
        endpoints.mark_verified(eid, external_recipient_id="chat-del-atacante")

    assert endpoints.get_endpoint(eid).external_recipient_id == "chat-legitimo"


def test_verifying_twice_with_the_same_recipient_is_idempotent(endpoints, ana):
    """El reintento legítimo: el proveedor reenvía el callback, la red duplica."""
    eid = _verified_endpoint(endpoints, ana, recipient="chat-123")
    first = endpoints.get_endpoint(eid).verified_at

    endpoints.mark_verified(eid, external_recipient_id="chat-123")     # no revienta

    endpoint = endpoints.get_endpoint(eid)
    assert endpoint.verified_at == first          # ni mueve la fecha original
    assert endpoint.external_recipient_id == "chat-123"
    assert endpoints.is_eligible(eid)


def test_verifying_an_unknown_endpoint_fails(endpoints):
    """Un UPDATE de 0 filas "tiene éxito": el caller creería haber verificado."""
    with pytest.raises(EndpointError):
        endpoints.mark_verified("no-existe", external_recipient_id="chat-123")


def test_a_revoked_endpoint_cannot_be_verified_afterwards(endpoints, ana):
    """La revocación es la última palabra: verificar después reescribe la historia.

    El caso real: la persona registra, se arrepiente y revoca; el callback tardío
    del proveedor llega después. Que siga inelegible no basta —lo que consta en
    la fila es lo que se auditará mañana, y diría "verificado tras darse de baja".
    """
    eid = endpoints.create_endpoint(
        principal_id=ana, channel="whatsapp", address=_ADDRESS
    )
    endpoints.revoke(eid)

    with pytest.raises(EndpointError):
        endpoints.mark_verified(eid, external_recipient_id="chat-123")

    endpoint = endpoints.get_endpoint(eid)
    assert endpoint.verified_at is None                  # sigue sin verificar
    assert endpoint.external_recipient_id is None        # y sin destinatario
    assert not endpoints.is_eligible(eid)


def test_a_verified_then_revoked_endpoint_cannot_be_reverified(endpoints, ana):
    """Revocado gana sobre "ya verificado": no se toca un endpoint dado de baja."""
    eid = _verified_endpoint(endpoints, ana, recipient="chat-123")
    endpoints.revoke(eid)

    with pytest.raises(EndpointError):
        endpoints.mark_verified(eid, external_recipient_id="chat-123")


def test_disabled_endpoint_is_not_eligible(endpoints, ana):
    eid = _verified_endpoint(endpoints, ana)

    endpoints.set_enabled(eid, False)

    assert not endpoints.is_eligible(eid)
    assert endpoints.list_eligible_endpoints(ana) == []


def test_disable_is_reversible_and_does_not_revoke(endpoints, ana):
    """Pausar y dar de baja son ejes distintos."""
    eid = _verified_endpoint(endpoints, ana)
    endpoints.set_enabled(eid, False)

    endpoints.set_enabled(eid, True)

    assert endpoints.get_endpoint(eid).revoked_at is None
    assert endpoints.is_eligible(eid)


def test_revoked_endpoint_is_not_eligible_but_the_row_remains(endpoints, ana):
    """La prueba de qué se consintió es justo lo que no puede desaparecer."""
    eid = _verified_endpoint(endpoints, ana)

    endpoints.revoke(eid)

    assert not endpoints.is_eligible(eid)
    endpoint = endpoints.get_endpoint(eid)
    assert endpoint is not None
    assert endpoint.is_revoked
    assert endpoint.verified_at is not None       # el historial no se reescribe
    assert [e.endpoint_id for e in endpoints.list_endpoints(ana)] == [eid]


def test_revoke_does_not_move_the_original_date(endpoints, ana):
    eid = _verified_endpoint(endpoints, ana)
    endpoints.revoke(eid, ts="2026-01-01T00:00:00+00:00")

    endpoints.revoke(eid, ts="2026-06-01T00:00:00+00:00")

    assert endpoints.get_endpoint(eid).revoked_at == "2026-01-01T00:00:00+00:00"


def test_revoked_endpoint_stays_out_even_if_reenabled(endpoints, ana):
    """Revocar gana sobre enabled: la baja es la decisión más fuerte."""
    eid = _verified_endpoint(endpoints, ana)
    endpoints.revoke(eid)

    endpoints.set_enabled(eid, True)

    assert not endpoints.is_eligible(eid)


def test_suspended_principal_takes_its_endpoints_out(endpoints, identity, ana):
    eid = _verified_endpoint(endpoints, ana)
    assert endpoints.is_eligible(eid)

    identity.suspend(ana)

    assert not endpoints.is_eligible(eid)
    assert endpoints.list_eligible_endpoints(ana) == []


def test_reactivating_the_principal_restores_eligibility(endpoints, identity, ana):
    eid = _verified_endpoint(endpoints, ana)
    identity.suspend(ana)

    identity.reactivate(ana)

    assert endpoints.is_eligible(eid)


def test_unique_channel_address_hash(endpoints, ana):
    endpoints.create_endpoint(principal_id=ana, channel="whatsapp", address=_ADDRESS)

    with pytest.raises(EndpointExistsError):
        endpoints.create_endpoint(
            principal_id=ana, channel="whatsapp", address=_ADDRESS
        )


def test_same_address_on_another_channel_is_a_different_endpoint(endpoints, ana):
    endpoints.create_endpoint(principal_id=ana, channel="whatsapp", address=_ADDRESS)

    eid = endpoints.create_endpoint(
        principal_id=ana, channel="telegram", address=_ADDRESS
    )

    assert endpoints.get_endpoint(eid) is not None


def test_two_principals_cannot_claim_the_same_address(endpoints, identity, ana):
    """El UNIQUE es global por canal, no por persona: si no, dos cuentas se
    registrarían el mismo teléfono y ambas recibirían."""
    beto = identity.create_principal("beto@example.com")
    endpoints.create_endpoint(principal_id=ana, channel="whatsapp", address=_ADDRESS)

    with pytest.raises(EndpointExistsError):
        endpoints.create_endpoint(
            principal_id=beto, channel="whatsapp", address=_ADDRESS
        )


def test_unknown_principal_is_rejected(endpoints):
    with pytest.raises(EndpointError):
        endpoints.create_endpoint(
            principal_id="no-existe", channel="whatsapp", address=_ADDRESS
        )


def test_invalid_channel_is_rejected(endpoints, ana):
    with pytest.raises(EndpointError):
        endpoints.create_endpoint(principal_id=ana, channel="paloma", address=_ADDRESS)


def test_empty_address_is_rejected(endpoints, ana):
    with pytest.raises(EndpointError):
        endpoints.create_endpoint(principal_id=ana, channel="whatsapp", address="  ")


# El antiguo test_mark_verified_does_not_revive_a_revoked_endpoint vivía aquí y
# solo comprobaba `not is_eligible`, que pasaba trivialmente por revoked_at aunque
# la verificación se hubiera escrito igual. Lo sustituye
# test_a_revoked_endpoint_cannot_be_verified_afterwards, que además exige que la
# fila quede sin verificar y sin destinatario.


def test_deleting_a_principal_with_endpoints_fails_by_restrict(endpoints, ana, db):
    endpoints.create_endpoint(principal_id=ana, channel="whatsapp", address=_ADDRESS)

    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM principals WHERE principal_id = ?", (ana,))
    finally:
        conn.close()


def test_eligible_listing_is_scoped_to_the_principal(endpoints, identity, ana):
    _verified_endpoint(endpoints, ana)
    beto = identity.create_principal("beto@example.com")
    beto_eid = _verified_endpoint(endpoints, beto, address="+573009999999")

    assert [e.endpoint_id for e in endpoints.list_eligible_endpoints(beto)] == [
        beto_eid
    ]
