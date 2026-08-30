from __future__ import annotations

import ast
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.auth import (
    AccountError,
    AccountExistsError,
    AccountStore,
    hash_password,
    verify_password,
)
from app.core.web_session import new_streamlit_chat_id, reset_user_session


STREAMLIT_APP = Path(__file__).parents[1] / "streamlit_app.py"


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


# ══════════════════════════════════════════════════════════════════════════════
# Frontera de sesión — dos personas en el mismo navegador
# ══════════════════════════════════════════════════════════════════════════════
#
# ``streamlit_app.py`` ejecuta la aplicación al importarse, así que aquí se
# cargan del AST únicamente los tres helpers de sesión y se ejecutan contra un
# ``st`` de mentira. Es la única forma de probar el *comportamiento* de la
# frontera: verificar que el source llama a cinco ``pop()`` no demuestra que no
# quede nada del usuario anterior —de hecho, esa era exactamente la forma del
# bug: la lista de claves estaba, y estaba incompleta.

class _FakeSessionState(dict):
    """``st.session_state``: acceso por atributo y por clave, como el real."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]


def _load_session_helpers():
    tree = ast.parse(STREAMLIT_APP.read_text(), filename=str(STREAMLIT_APP))
    wanted = {
        "_seed_session_defaults", "_end_user_session", "_activate_session",
        "_current_user_identity",
    }
    functions = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in wanted
    ]
    assert {f.name for f in functions} == wanted, "cambió el nombre de un helper"
    fake_st = SimpleNamespace(
        session_state=_FakeSessionState(), query_params={},
    )
    namespace = {
        "st": fake_st,
        "uuid": uuid,
        "new_streamlit_chat_id": new_streamlit_chat_id,
        "reset_user_session": reset_user_session,
    }
    exec(  # noqa: S102 - cargar los helpers reales, no una copia del test
        compile(ast.Module(body=functions, type_ignores=[]), str(STREAMLIT_APP), "exec"),
        namespace,
    )
    return namespace, fake_st.session_state


def _estado_poblado_de(ns, state, *, email: str) -> dict:
    """Una sesión de A como la deja el uso normal, y su chat_id."""
    ns["_activate_session"](SimpleNamespace(email=email, name="Ana", role="user"))
    state["history"] = [
        {"role": "user", "content": "licitaciones de vías"},
        {"role": "assistant", "trace_id": "trace-de-a", "rating": None},
    ]
    state["_open_radar"] = "radar-de-a"
    state["_open_event"] = "evento-de-a"
    state["_session_token"] = "token-de-a"
    state["_cookie_probed"] = True
    state["_workspace_user"] = {"key": f"email:{email}", "user_id": "u-a"}
    state["_radar_refresh_ts"] = 1_770_000_000.0
    state["_pending_example"] = "contratos del SENA"
    state["_workspace_flash"] = "Oportunidad guardada"
    state["comment_trace-de-a"] = "el segundo resultado no era"
    state["_feature_de_2027"] = "estado de A que todavía no existe"
    return dict(state)


def test_logout_leaves_nothing_of_the_previous_user():
    ns, state = _load_session_helpers()
    antes = _estado_poblado_de(ns, state, email="ana@b.com")

    ns["_end_user_session"]()

    # Nada del usuario anterior sigue accesible. Lo exceptuado es estado
    # sembrado de nuevo, vacío, por _seed_session_defaults (ver más abajo).
    resembradas = {
        "history", "chat_id", "user_name", "user_email", "anonymous_user_id",
    }
    for clave in antes:
        if clave in resembradas:
            continue
        assert clave not in state, f"{clave} sobrevivió al logout"
    assert state["history"] == []
    assert state["chat_id"] != antes["chat_id"]
    assert state["user_email"] == "" and state["user_name"] == ""
    # Y el estado sigue siendo utilizable: el render que continúa esta pasada
    # necesita history y chat_id, no un KeyError.
    assert set(state) >= {"history", "chat_id", "anonymous_user_id"}


def test_logout_clears_state_no_one_remembered_to_list():
    """El bug no era olvidar una clave: era que hubiera una lista."""
    ns, state = _load_session_helpers()
    _estado_poblado_de(ns, state, email="ana@b.com")

    ns["_end_user_session"]()

    assert "_feature_de_2027" not in state
    assert "comment_trace-de-a" not in state


def test_login_of_b_after_a_starts_from_a_clean_session():
    ns, state = _load_session_helpers()
    de_a = _estado_poblado_de(ns, state, email="ana@b.com")

    ns["_activate_session"](SimpleNamespace(
        email="bruno@b.com", name="Bruno", role="user",
    ))

    assert state["history"] == []
    assert "_open_radar" not in state
    assert "_open_event" not in state
    assert "_workspace_user" not in state
    assert "_session_token" not in state
    assert state["chat_id"] != de_a["chat_id"]
    assert state["account"] == {
        "email": "bruno@b.com", "name": "Bruno", "role": "user",
    }
    assert state["user_email"] == "bruno@b.com"


def test_login_does_not_depend_on_the_previous_user_pressing_logout():
    """A cerró la pestaña sin salir; B entra con sus credenciales."""
    ns, state = _load_session_helpers()
    _estado_poblado_de(ns, state, email="ana@b.com")
    # Sin _end_user_session() de por medio: la sesión de A sigue entera.
    assert state["_open_radar"] == "radar-de-a"

    ns["_activate_session"](SimpleNamespace(
        email="bruno@b.com", name="Bruno", role="user",
    ))

    assert "_open_radar" not in state
    assert state["history"] == []


def test_chat_id_is_conversation_identity_not_account_identity():
    ns, state = _load_session_helpers()
    ns["_activate_session"](SimpleNamespace(
        email="ana.perez@empresa.com", name="Ana", role="user",
    ))

    chat_id = state["chat_id"]

    assert chat_id.startswith("st_")
    assert "ana" not in chat_id and "empresa" not in chat_id
    assert chat_id != "st_default"


def test_seeding_twice_does_not_rotate_a_live_conversation():
    """Estable dentro de la sesión: los reruns no pueden cambiar el chat_id."""
    ns, state = _load_session_helpers()
    ns["_seed_session_defaults"]()
    primero = state["chat_id"]
    state["history"].append({"role": "user", "content": "vías"})

    ns["_seed_session_defaults"]()
    ns["_seed_session_defaults"]()

    assert state["chat_id"] == primero
    assert len(state["history"]) == 1


def test_the_anonymous_id_is_re_derived_from_the_url_not_carried_over():
    """No es estado de A que sobreviva: es la identidad previa a autenticarse.

    ``anonymous_user_id`` se persiste en ``query_params`` porque Streamlit crea
    una sesión nueva en cada F5 y, sin él, los radares de un usuario anónimo
    quedarían huérfanos al refrescar. El logout lo borra como todo lo demás, y
    la siembra lo reconstruye desde la URL —que el siguiente usuario ve igual—.
    Nunca fue el dueño de los datos de A: los de A cuelgan de ``email:...``.
    """
    ns, state = _load_session_helpers()
    ns["_seed_session_defaults"]()
    _estado_poblado_de(ns, state, email="ana@b.com")
    de_la_url = state["anonymous_user_id"]

    ns["_end_user_session"]()

    assert state["anonymous_user_id"] == de_la_url
    assert de_la_url != "ana@b.com"


def test_resolving_the_identity_does_not_rewrite_the_conversation_id():
    """La regresión exacta que hay que impedir.

    ``_current_user_identity`` se llama en cada rerun y antes reescribía
    ``chat_id`` a partir del email. Así exponía PII en el identificador
    persistido y hacía que la misma cuenta reutilizara su hilo entre sesiones;
    no hacía que una cuenta heredara el ``ConversationStore`` de otra. Leer
    quién es el usuario no puede escribir dónde vive su conversación.
    """
    ns, state = _load_session_helpers()
    ns["_activate_session"](SimpleNamespace(
        email="ana@b.com", name="Ana", role="user",
    ))
    de_la_sesion = state["chat_id"]

    identidad = ns["_current_user_identity"]()      # como en cada rerun
    ns["_current_user_identity"]()

    assert state["chat_id"] == de_la_sesion
    assert identidad["channel_user_key"] == "email:ana@b.com"
    assert "ana" not in state["chat_id"]


def test_the_anonymous_identity_does_not_rewrite_the_conversation_id_either():
    ns, state = _load_session_helpers()
    ns["_seed_session_defaults"]()
    de_la_sesion = state["chat_id"]

    identidad = ns["_current_user_identity"]()

    assert state["chat_id"] == de_la_sesion
    assert identidad["is_anonymous"] is True
    assert state["anonymous_user_id"] not in state["chat_id"]
