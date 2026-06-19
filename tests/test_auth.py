from __future__ import annotations

import pytest

from app.core.auth import (
    AccountError,
    AccountExistsError,
    AccountStore,
    hash_password,
    verify_password,
)


def _store(tmp_path) -> AccountStore:
    return AccountStore(tmp_path / "secoppal.db")


# ── Hashing ──────────────────────────────────────────────────────────────────
def test_hash_verify_roundtrip():
    h, salt = hash_password("s3cret!")
    assert verify_password("s3cret!", h, salt)
    assert not verify_password("wrong", h, salt)


def test_hash_uses_random_salt():
    h1, s1 = hash_password("same")
    h2, s2 = hash_password("same")
    assert s1 != s2 and h1 != h2  # mismo password, distinto salt → distinto hash


def test_verify_rejects_empty_and_corrupt():
    h, salt = hash_password("x")
    assert not verify_password("", h, salt)
    assert not verify_password("x", h, "no-es-hex")


# ── Create / authenticate ─────────────────────────────────────────────────────
def test_create_and_authenticate(tmp_path):
    store = _store(tmp_path)
    acc = store.create_account("User@Example.com", "pw123", name="Ada", role="user")
    assert acc.email == "user@example.com"  # normalizado
    assert acc.role == "user" and acc.is_active

    assert store.authenticate("user@example.com", "pw123") is not None
    assert store.authenticate("USER@example.com", "pw123") is not None  # case-insensitive
    assert store.authenticate("user@example.com", "mala") is None
    assert store.authenticate("nadie@example.com", "pw123") is None


def test_duplicate_email_rejected(tmp_path):
    store = _store(tmp_path)
    store.create_account("a@b.com", "pw")
    with pytest.raises(AccountExistsError):
        store.create_account("A@B.com", "otra")


def test_invalid_email_and_role(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(AccountError):
        store.create_account("sin-arroba", "pw")
    with pytest.raises(AccountError):
        store.create_account("a@b.com", "pw", role="superadmin")
    with pytest.raises(AccountError):
        store.create_account("a@b.com", "")


def test_inactive_account_cannot_authenticate(tmp_path):
    store = _store(tmp_path)
    store.create_account("a@b.com", "pw")
    store.create_account("admin@b.com", "pw", role="admin")  # otro admin para no bloquear
    assert store.set_active("a@b.com", False) is True
    assert store.authenticate("a@b.com", "pw") is None
    assert store.set_active("a@b.com", True) is True
    assert store.authenticate("a@b.com", "pw") is not None


def test_reset_password(tmp_path):
    store = _store(tmp_path)
    store.create_account("a@b.com", "old")
    assert store.reset_password("a@b.com", "new") is True
    assert store.authenticate("a@b.com", "old") is None
    assert store.authenticate("a@b.com", "new") is not None
    assert store.reset_password("nadie@b.com", "x") is False


# ── Roles / protección del último admin ────────────────────────────────────────
def test_set_role(tmp_path):
    store = _store(tmp_path)
    store.create_account("admin@b.com", "pw", role="admin")
    store.create_account("u@b.com", "pw")
    assert store.set_role("u@b.com", "admin") is True
    assert store.get("u@b.com").is_admin


def test_cannot_remove_last_admin(tmp_path):
    store = _store(tmp_path)
    store.create_account("admin@b.com", "pw", role="admin")
    with pytest.raises(AccountError):
        store.set_active("admin@b.com", False)
    with pytest.raises(AccountError):
        store.set_role("admin@b.com", "user")
    # Con un segundo admin sí se puede degradar al primero
    store.create_account("admin2@b.com", "pw", role="admin")
    assert store.set_role("admin@b.com", "user") is True


# ── Seed del admin ──────────────────────────────────────────────────────────
def test_ensure_admin_seed_idempotent(tmp_path):
    store = _store(tmp_path)
    assert store.ensure_admin_seed("boss@b.com", "pw", name="Boss") is True
    assert store.get("boss@b.com").is_admin
    # Segunda llamada no crea otro admin
    assert store.ensure_admin_seed("boss@b.com", "pw") is False
    assert store.ensure_admin_seed("otro@b.com", "pw") is False  # ya hay un admin


def test_ensure_admin_seed_noop_without_credentials(tmp_path):
    store = _store(tmp_path)
    assert store.ensure_admin_seed(None, None) is False
    assert store.ensure_admin_seed("boss@b.com", "") is False
    assert store.list_accounts() == []


# ── Sesiones persistentes ("recordarme") ───────────────────────────────────
def test_session_create_resolve_revoke(tmp_path):
    store = _store(tmp_path)
    store.create_account("ana@b.com", "pw", name="Ana")
    token = store.create_session("ana@b.com")
    assert token
    acc = store.resolve_session(token)
    assert acc is not None and acc.email == "ana@b.com"

    store.revoke_session(token)
    assert store.resolve_session(token) is None


def test_session_invalid_or_empty_token(tmp_path):
    store = _store(tmp_path)
    assert store.resolve_session(None) is None
    assert store.resolve_session("") is None
    assert store.resolve_session("inexistente") is None


def test_session_for_missing_account_rejected(tmp_path):
    store = _store(tmp_path)
    with pytest.raises(AccountError):
        store.create_session("nadie@b.com")


def test_session_rejected_when_account_deactivated(tmp_path):
    store = _store(tmp_path)
    store.create_account("admin@b.com", "pw", role="admin")  # evita "último admin"
    store.create_account("ana@b.com", "pw")
    token = store.create_session("ana@b.com")
    store.set_active("ana@b.com", False)
    assert store.resolve_session(token) is None  # cuenta inactiva → no auto-login


def test_session_expired_is_deleted(tmp_path):
    store = _store(tmp_path)
    store.create_account("ana@b.com", "pw")
    token = store.create_session("ana@b.com", ttl_days=-1)  # ya expirado
    assert store.resolve_session(token) is None
