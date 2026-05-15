"""Tests para ConversationStore — 2.1"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.core.conversation_store import (
    FOLLOWUP_WINDOW_MINUTES,
    ConversationStore,
    Turn,
    is_followup,
    is_reset_command,
    merge_params,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store(tmp_path):
    return ConversationStore(base_path=tmp_path / "conversations")


def _make_turn(chat_id="tg_123", minutes_ago=0, parsed_params=None) -> Turn:
    ts = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return Turn(
        turn_id="abc123",
        timestamp=ts.isoformat(),
        chat_id=chat_id,
        user_query="procesos de pavimentacion en el Choco",
        response="Encontre 5 resultados",
        parsed_params=parsed_params or {"objeto": "pavimentacion", "departamento_resolved": "CHOCÓ"},
        result_ids=["ID-001", "ID-002"],
        trace_id="trace001",
    )


# ---------------------------------------------------------------------------
# ConversationStore — almacenamiento basico
# ---------------------------------------------------------------------------

def test_append_and_get_history(store):
    store.append_turn(
        chat_id="tg_100",
        user_query="procesos en Choco",
        response="resp1",
        parsed_params={"departamento_resolved": "CHOCÓ"},
        result_ids=["A"],
        trace_id="t1",
    )
    store.append_turn(
        chat_id="tg_100",
        user_query="y en Bolivar?",
        response="resp2",
        parsed_params={"departamento_resolved": "BOLÍVAR"},
        result_ids=["B"],
        trace_id="t2",
    )
    history = store.get_history("tg_100")
    assert len(history) == 2
    assert history[0].user_query == "procesos en Choco"
    assert history[1].user_query == "y en Bolivar?"


def test_get_history_last_n_limita(store):
    for i in range(7):
        store.append_turn(
            chat_id="tg_200",
            user_query=f"query {i}",
            response=f"resp {i}",
            parsed_params={},
        )
    history = store.get_history("tg_200", last_n=5)
    assert len(history) == 5
    assert history[-1].user_query == "query 6"


def test_get_history_chat_vacio(store):
    assert store.get_history("tg_999") == []


def test_clear_vacia_archivo(store):
    store.append_turn("tg_300", "q", "r", {})
    assert len(store.get_history("tg_300")) == 1
    store.clear("tg_300")
    assert store.get_history("tg_300") == []


def test_clear_no_falla_si_no_existe(store):
    store.clear("tg_no_existe")  # no debe lanzar excepcion


def test_corrupt_line_skipped(store, tmp_path):
    path = tmp_path / "conversations" / "tg_400.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        f.write('{"turn_id":"ok","timestamp":"2026-01-01T00:00:00+00:00","chat_id":"tg_400","user_query":"q","response":"r","parsed_params":{},"result_ids":[],"trace_id":""}\n')
        f.write("LINEA CORRUPTA NO ES JSON\n")
        f.write('{"turn_id":"ok2","timestamp":"2026-01-01T01:00:00+00:00","chat_id":"tg_400","user_query":"q2","response":"r2","parsed_params":{},"result_ids":[],"trace_id":""}\n')
    store2 = ConversationStore(base_path=tmp_path / "conversations")
    history = store2.get_history("tg_400")
    assert len(history) == 2
    assert history[0].turn_id == "ok"
    assert history[1].turn_id == "ok2"


# ---------------------------------------------------------------------------
# Sanitizacion de chat_id
# ---------------------------------------------------------------------------

def test_sanitize_telegram_negativo(store):
    store.append_turn("-987654321", "q", "r", {})
    path = store._path_for("-987654321")
    assert path.name == "tg_987654321.jsonl"


def test_sanitize_whatsapp(store):
    store.append_turn("+573001234567", "q", "r", {})
    path = store._path_for("+573001234567")
    assert path.name == "wa_573001234567.jsonl"


def test_sanitize_ya_prefijado(store):
    path = store._path_for("tg_123")
    assert path.name == "tg_123.jsonl"
    path2 = store._path_for("wa_573")
    assert path2.name == "wa_573.jsonl"


# ---------------------------------------------------------------------------
# merge_params
# ---------------------------------------------------------------------------

def test_merge_replace_sobreescribe():
    ctx = {"departamento_resolved": "CHOCÓ", "objeto": "pavimentacion"}
    new = {"departamento_resolved": "BOLÍVAR"}
    result = merge_params(ctx, new)
    assert result["departamento_resolved"] == "BOLÍVAR"
    assert result["objeto"] == "pavimentacion"  # conservado


def test_merge_campo_none_conserva_anterior():
    ctx = {"objeto": "pavimentacion", "departamento_resolved": "CHOCÓ"}
    new = {"objeto": None, "departamento_resolved": "BOLÍVAR"}
    result = merge_params(ctx, new)
    assert result["objeto"] == "pavimentacion"  # None no sobreescribe
    assert result["departamento_resolved"] == "BOLÍVAR"


def test_merge_dataset_keep_previous_unless_explicit():
    ctx = {"dataset": "procesos"}
    new = {"dataset": ""}  # vacio = no explicito
    result = merge_params(ctx, new)
    assert result["dataset"] == "procesos"  # conservado

    new_explicit = {"dataset": "contratos"}
    result2 = merge_params(ctx, new_explicit)
    assert result2["dataset"] == "contratos"  # sobreescribio


def test_merge_campo_nuevo_sin_regla():
    """Campos sin regla en MERGE_RULES usan replace por defecto."""
    ctx = {"campo_custom": "viejo"}
    new = {"campo_custom": "nuevo"}
    result = merge_params(ctx, new)
    assert result["campo_custom"] == "nuevo"


# ---------------------------------------------------------------------------
# is_followup
# ---------------------------------------------------------------------------

def test_followup_continuation_word():
    turn = _make_turn(minutes_ago=5)
    assert is_followup("y en Bolivar?", turn) is True
    assert is_followup("ahora muéstrame los de Nariño", turn) is True
    assert is_followup("también del Atlántico", turn) is True


def test_followup_query_corta_sin_verbo():
    turn = _make_turn(minutes_ago=2)
    assert is_followup("los de Bolivar", turn) is True
    assert is_followup("solo los abiertos", turn) is True


def test_is_followup_true_para_cualquier_query_con_turno_reciente():
    """H8 (2026-05): is_followup ahora solo verifica prerrequisitos.
    Clasificacion granular la hace detect_and_merge en apply_context."""
    turn = _make_turn(minutes_ago=2)
    # Con turno reciente, cualquier query activa contexto conversacional.
    assert is_followup("busca contratos en Bogota", turn) is True
    assert is_followup("necesito licitaciones de agua", turn) is True


def test_no_followup_sin_turno_previo():
    assert is_followup("y en Bolivar?", None) is False


def test_followup_expira_despues_30_min():
    turn = _make_turn(minutes_ago=FOLLOWUP_WINDOW_MINUTES + 1)
    assert is_followup("y en Bolivar?", turn) is False


def test_followup_dentro_de_ventana():
    turn = _make_turn(minutes_ago=FOLLOWUP_WINDOW_MINUTES - 1)
    assert is_followup("y en Bolivar?", turn) is True


# ---------------------------------------------------------------------------
# is_reset_command
# ---------------------------------------------------------------------------

def test_reset_commands():
    for cmd in ["/reset", "reset", "nueva", "limpiar", "nuevo"]:
        assert is_reset_command(cmd) is True
    assert is_reset_command("procesos en Choco") is False
    assert is_reset_command("") is False
