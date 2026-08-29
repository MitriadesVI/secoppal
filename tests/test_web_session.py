"""Frontera de sesión web: identidad de conversación y vaciado de estado.

El defecto que originó este módulo: el ``chat_id`` de Streamlit se derivaba del
email (``st_email_<email>``), de modo que era identidad de cuenta y no de
conversación —escribía PII en disco y hacía que la siguiente identidad heredara
el hilo de la anterior—; y el logout quitaba una lista de cinco claves conocidas,
dejando en pie ``history``, ``_open_radar`` y todo lo demás.
"""
from __future__ import annotations

from app.core.conversation_store import ConversationStore, _sanitize_chat_id
from app.core.web_session import (
    new_streamlit_chat_id,
    reset_user_session,
    scratch_chat_id,
)


EMAIL = "ana.perez@empresa.com"


# ── chat_id de sesión ────────────────────────────────────────────────────────

def test_streamlit_chat_ids_are_unique_across_sessions():
    ids = {new_streamlit_chat_id() for _ in range(500)}
    assert len(ids) == 500


def test_streamlit_chat_id_keeps_the_prefix_the_store_understands():
    chat_id = new_streamlit_chat_id()
    assert chat_id.startswith("st_")
    # Si el saneado lo transformara, dos ids distintos podrían acabar en el
    # mismo archivo —justo la colisión que este id existe para evitar.
    assert _sanitize_chat_id(chat_id) == chat_id


def test_streamlit_chat_id_carries_no_identity():
    """Ni email, ni nombre, ni el literal compartido que había antes."""
    ids = [new_streamlit_chat_id() for _ in range(20)]
    for chat_id in ids:
        cuerpo = chat_id.removeprefix("st_")
        assert cuerpo.isalnum() and cuerpo.islower()
        assert "email" not in chat_id
        assert chat_id != "st_default"


def test_conversation_memory_does_not_cross_between_chat_ids(tmp_path):
    """La unicidad del id no basta: hay que probar que el store la respeta."""
    store = ConversationStore(base_path=tmp_path)
    chat_a, chat_b = new_streamlit_chat_id(), new_streamlit_chat_id()

    store.append_turn(chat_a, "licitaciones de vías en Atlántico", "8 resultados", {})

    assert len(store.get_history(chat_a)) == 1
    assert store.get_history(chat_b) == []
    assert store.get_last_suggestions(chat_b) is None
    assert store.last_successful_turn(chat_b) is None


def test_a_chat_id_from_a_previous_session_cannot_be_reached_by_the_next(tmp_path):
    """Lo que protege al hilo anterior es que el siguiente id no lo nombra."""
    store = ConversationStore(base_path=tmp_path)
    sesion_de_a = new_streamlit_chat_id()
    store.append_turn(sesion_de_a, "contratos del SENA", "12 resultados", {})

    sesion_de_b = new_streamlit_chat_id()

    assert sesion_de_b != sesion_de_a
    assert store.get_history(sesion_de_b) == []


# ── hilos internos de trabajo ────────────────────────────────────────────────

def test_scratch_chat_id_is_stable_per_owner_and_hides_the_email():
    owner = f"email:{EMAIL}"

    primero = scratch_chat_id("radarcreate", owner)
    segundo = scratch_chat_id("radarcreate", owner)

    assert primero == segundo                      # no deja un archivo por uso
    assert EMAIL not in primero
    assert "ana" not in primero and "empresa" not in primero
    assert primero.startswith("_radarcreate_")


def test_scratch_chat_ids_of_two_owners_do_not_collide():
    a = scratch_chat_id("radarcreate", "email:ana@empresa.com")
    b = scratch_chat_id("radarcreate", "email:bruno@empresa.com")
    assert a != b


def test_scratch_chat_id_survives_the_store_sanitizer(tmp_path):
    """El saneado no puede fundir dos dueños en el mismo archivo."""
    store = ConversationStore(base_path=tmp_path)
    a = scratch_chat_id("radarcreate", "email:ana@empresa.com")
    b = scratch_chat_id("radarcreate", "email:bruno@empresa.com")

    store.append_turn(a, "mantenimiento vial", "ok", {})

    assert _sanitize_chat_id(a) != _sanitize_chat_id(b)
    assert store.get_history(b) == []


# ── vaciado del estado de sesión ─────────────────────────────────────────────

def test_reset_clears_everything_including_keys_it_never_heard_of():
    """La polaridad invertida: la clave nueva nace protegida.

    Este es el bug original. Una lista de claves conocidas solo protege de lo
    que alguien recordó escribir; ``_feature_de_2027`` representa la próxima
    feature que guarde estado del usuario y a nadie se le ocurra añadir.
    """
    state = {
        "history": [{"role": "user", "content": "secreto de A"}],
        "chat_id": "st_aaa",
        "_open_radar": "radar-de-a",
        "_open_event": "evento-de-a",
        "account": {"email": EMAIL},
        "_feature_de_2027": "estado de A que aún no existe",
    }

    borradas = reset_user_session(state)

    assert state == {}
    assert "_feature_de_2027" in borradas
    assert borradas == sorted(borradas)


def test_reset_keeps_only_what_is_asked_explicitly():
    state = {"anonymous_user_id": "abc", "history": ["x"], "_open_radar": "r-1"}

    borradas = reset_user_session(state, keep=("anonymous_user_id",))

    assert state == {"anonymous_user_id": "abc"}
    assert borradas == ["_open_radar", "history"]


def test_reset_on_an_empty_state_is_a_no_op():
    state: dict = {}
    assert reset_user_session(state) == []
    assert state == {}
