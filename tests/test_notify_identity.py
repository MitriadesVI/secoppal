"""Tests de app/core/notify/identity_store.py — quién es la persona."""
from __future__ import annotations

import sqlite3

import pytest

from app.core.notify.identity_store import (
    IdentityError,
    IdentityStore,
    OwnerKeyConflictError,
    PrincipalExistsError,
)


@pytest.fixture
def identity(tmp_path):
    return IdentityStore(tmp_path / "notify.db")


def test_create_and_get_principal(identity):
    pid = identity.create_principal("Ana@Example.COM")

    principal = identity.get_principal(pid)
    assert principal is not None
    assert principal.email == "ana@example.com"   # normalizado por auth
    assert principal.is_active


def test_email_is_unique(identity):
    identity.create_principal("ana@example.com")

    with pytest.raises(PrincipalExistsError):
        identity.create_principal("ana@example.com")


def test_email_uniqueness_uses_the_auth_normalization(identity):
    """La unicidad no puede depender de cómo el usuario tecleó el correo.

    Si aquí se normalizara distinto que en ``auth``, la misma persona podría
    tener cuenta con un email y principal con otro, y nada los uniría.
    """
    identity.create_principal("ana@example.com")

    with pytest.raises(PrincipalExistsError):
        identity.create_principal("  ANA@Example.com  ")


def test_get_principal_by_email_normalizes_too(identity):
    pid = identity.create_principal("ana@example.com")

    assert identity.get_principal_by_email("  Ana@EXAMPLE.com ").principal_id == pid


def test_empty_email_is_rejected(identity):
    with pytest.raises(IdentityError):
        identity.create_principal("   ")


def test_owner_key_is_unique_and_resolves(identity):
    pid = identity.create_principal("ana@example.com")
    identity.link_owner_key("email:ana@example.com", pid)

    assert identity.resolve_principal_id("email:ana@example.com") == pid


def test_unknown_owner_key_resolves_to_none_without_creating_anything(identity):
    """No autocrear: un owner desconocido es una pregunta sin respuesta.

    Autocrear fabricaría un principal sin email verificado por cada chat
    anónimo, y esos principales luego recibirían notificaciones.
    """
    assert identity.resolve_principal_id("anon:desconocido") is None
    assert identity.get_principal_by_email("anon:desconocido") is None


def test_owner_key_cannot_be_silently_reassigned(identity):
    """Reasignar transferiría los radares de una persona a otra."""
    ana = identity.create_principal("ana@example.com")
    beto = identity.create_principal("beto@example.com")
    identity.link_owner_key("anon:abc", ana)

    with pytest.raises(OwnerKeyConflictError):
        identity.link_owner_key("anon:abc", beto)

    assert identity.resolve_principal_id("anon:abc") == ana


def test_relinking_the_same_pair_is_idempotent(identity):
    """El caller no debería tener que consultar antes de vincular."""
    ana = identity.create_principal("ana@example.com")
    identity.link_owner_key("anon:abc", ana)

    identity.link_owner_key("anon:abc", ana)     # no debe reventar

    assert identity.list_owner_keys(ana) == ["anon:abc"]


def test_linking_to_an_unknown_principal_fails(identity):
    with pytest.raises(IdentityError):
        identity.link_owner_key("anon:abc", "principal-que-no-existe")


def test_one_principal_holds_many_owner_keys(identity):
    """El caso real: la misma persona anónima, luego registrada, luego en chat."""
    ana = identity.create_principal("ana@example.com")
    for key in ("anon:abc", "email:ana@example.com", "12345"):
        identity.link_owner_key(key, ana)

    assert set(identity.list_owner_keys(ana)) == {
        "anon:abc", "email:ana@example.com", "12345",
    }
    assert identity.resolve_principal_id("12345") == ana


def test_suspended_principal_persists_and_is_reversible(identity):
    """Suspender no borra: la prueba de qué consintió debe sobrevivir."""
    pid = identity.create_principal("ana@example.com")

    identity.suspend(pid)
    assert identity.get_principal(pid).status == "suspended"
    assert not identity.get_principal(pid).is_active

    identity.reactivate(pid)
    assert identity.get_principal(pid).is_active


def test_invalid_status_is_rejected(identity):
    pid = identity.create_principal("ana@example.com")

    with pytest.raises(IdentityError):
        identity.set_status(pid, "borrado")


def test_deleting_a_principal_with_owner_keys_fails_by_restrict(identity):
    """RESTRICT, no CASCADE: borrar la persona no puede desatar sus radares."""
    pid = identity.create_principal("ana@example.com")
    identity.link_owner_key("anon:abc", pid)

    conn = sqlite3.connect(identity.db_path)
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("DELETE FROM principals WHERE principal_id = ?", (pid,))
    finally:
        conn.close()
