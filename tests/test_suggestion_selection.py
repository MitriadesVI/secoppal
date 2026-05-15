"""Tests para sugerencias ejecutables — selección numérica (1, 2, 3)."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.core.conversation_store import ConversationStore, Turn, is_reset_command


# ── ConversationStore: suggestions ────────────────────────────────────────────

class TestTurnSuggestions:
    def test_suggestions_in_turn(self):
        t = Turn(
            turn_id="abc", timestamp="2026-01-01T00:00:00",
            chat_id="st_test", user_query="test", response="ok",
            parsed_params={}, suggestions=[{"label": "A", "modified_params": {"estado": ["Celebrado"]}}],
        )
        assert t.suggestions == [{"label": "A", "modified_params": {"estado": ["Celebrado"]}}]

    def test_suggestions_in_to_dict(self):
        t = Turn(turn_id="x", timestamp="x", chat_id="x", user_query="x", response="x",
                 parsed_params={}, suggestions=[{"label": "X"}])
        d = t.to_dict()
        assert d["suggestions"] == [{"label": "X"}]

    def test_suggestions_in_from_dict(self):
        d = {"turn_id": "x", "timestamp": "x", "chat_id": "x", "user_query": "x",
             "response": "x", "parsed_params": {}, "suggestions": [{"label": "Y"}]}
        t = Turn.from_dict(d)
        assert t.suggestions == [{"label": "Y"}]

    def test_legacy_turn_no_suggestions(self):
        """Turn antiguo sin campo suggestions se carga con lista vacía."""
        d = {"turn_id": "x", "timestamp": "x", "chat_id": "x", "user_query": "x",
             "response": "x", "parsed_params": {}}
        t = Turn.from_dict(d)
        assert t.suggestions == []


class TestConversationStoreSuggestions:
    def test_append_and_retrieve_suggestions(self, tmp_path):
        store = ConversationStore(base_path=tmp_path / "conv")
        store.append_turn(
            chat_id="st_test", user_query="test", response="ok",
            parsed_params={}, suggestions=[{"label": "Firmados", "modified_params": {"estado": ["Celebrado"]}}],
        )
        got = store.get_last_suggestions("st_test")
        assert got is not None
        assert len(got) == 1
        assert got[0]["label"] == "Firmados"

    def test_get_last_suggestions_empty_history(self, tmp_path):
        store = ConversationStore(base_path=tmp_path / "conv")
        assert store.get_last_suggestions("st_empty") is None

    def test_get_last_suggestions_returns_only_last(self, tmp_path):
        store = ConversationStore(base_path=tmp_path / "conv")
        store.append_turn("st_test", "q1", "r1", {}, suggestions=[{"label": "A"}])
        store.append_turn("st_test", "q2", "r2", {}, suggestions=[{"label": "B"}, {"label": "C"}])
        got = store.get_last_suggestions("st_test")
        assert got == [{"label": "B"}, {"label": "C"}]


# ── Reset command ────────────────────────────────────────────────────────────

class TestResetCommand:
    def test_is_reset_command(self):
        assert is_reset_command("/reset") is True
        assert is_reset_command("reset") is True
        assert is_reset_command("nueva") is True
        assert is_reset_command("limpiar") is True
        assert is_reset_command("nuevo") is True
        assert is_reset_command("contratos de mantenimiento") is False
        assert is_reset_command("1") is False

    def test_store_clear(self, tmp_path):
        store = ConversationStore(base_path=tmp_path / "conv")
        store.append_turn("st_test", "q1", "r1", {})
        store.clear("st_test")
        assert store.get_history("st_test") == []
