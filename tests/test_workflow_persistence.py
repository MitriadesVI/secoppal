"""Unit tests for app.core.workflow.persistence.

extract_result_ids + persist_trace_and_turn are staged in Paso 3. Tests
use fake deps to verify the persistence pattern without depending on
real FeedbackStore / ConversationStore IO.
"""
from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from app.core.workflow.context import HandlerContext
from app.core.workflow.persistence import extract_result_ids, persist_trace_and_turn


# ─── extract_result_ids ──────────────────────────────────────────────────────

class TestExtractResultIds:
    def test_picks_referencia_first(self):
        rows = [
            {"referencia_del_proceso": "LP-001", "id_contrato": "C-001"},
            {"referencia_del_proceso": "LP-002"},
        ]
        assert extract_result_ids(rows) == ["LP-001", "LP-002"]

    def test_falls_back_to_id_contrato(self):
        rows = [
            {"id_contrato": "C-001"},
            {"id_contrato": "C-002"},
        ]
        assert extract_result_ids(rows) == ["C-001", "C-002"]

    def test_skips_rows_without_either(self):
        rows = [
            {"referencia_del_proceso": "LP-001"},
            {"otra_cosa": "x"},  # no identifier → dropped
            {"id_contrato": "C-003"},
        ]
        assert extract_result_ids(rows) == ["LP-001", "C-003"]

    def test_caps_at_ten_by_default(self):
        rows = [{"referencia_del_proceso": f"LP-{i}"} for i in range(20)]
        assert len(extract_result_ids(rows)) == 10
        assert extract_result_ids(rows)[0] == "LP-0"
        assert extract_result_ids(rows)[-1] == "LP-9"

    def test_custom_cap(self):
        rows = [{"referencia_del_proceso": f"LP-{i}"} for i in range(20)]
        assert len(extract_result_ids(rows, cap=3)) == 3

    def test_empty_input(self):
        assert extract_result_ids([]) == []

    def test_empty_string_ids_filtered_out(self):
        rows = [
            {"referencia_del_proceso": ""},
            {"id_contrato": ""},
            {"referencia_del_proceso": "LP-001"},
        ]
        assert extract_result_ids(rows) == ["LP-001"]


# ─── persist_trace_and_turn ──────────────────────────────────────────────────

class FakeFeedback:
    def __init__(self):
        self.calls: list[tuple[str, str, dict, dict | None]] = []

    def log_trace(self, user_query, channel, result, *, user_identity=None):
        self.calls.append((user_query, channel, dict(result), user_identity))
        return f"trace-{len(self.calls)}"


class FakeConvStore:
    def __init__(self):
        self.turns: list[dict] = []

    def append_turn(self, *, chat_id, user_query, response, parsed_params, result_ids, trace_id, **_):
        self.turns.append({
            "chat_id": chat_id,
            "user_query": user_query,
            "response": response,
            "parsed_params": dict(parsed_params),
            "result_ids": list(result_ids),
            "trace_id": trace_id,
        })
        return "turn-id"


@dataclass
class FakeDeps:
    """Minimal stand-in for WorkflowDeps — only the two attributes
    persist_trace_and_turn actually touches."""
    feedback: FakeFeedback = field(default_factory=FakeFeedback)
    conv_store: FakeConvStore = field(default_factory=FakeConvStore)


class TestPersistTraceAndTurn:
    def _mk_ctx(self, **overrides) -> HandlerContext:
        return HandlerContext(
            user_query=overrides.get("user_query", "hola"),
            channel=overrides.get("channel", "streamlit"),
            chat_id=overrides.get("chat_id", "chat-123"),
            user_identity=overrides.get("user_identity"),
            seed_resolved_params=overrides.get("seed_resolved_params"),
        )

    def test_logs_trace_and_sets_trace_id(self):
        deps = FakeDeps()
        ctx = self._mk_ctx(chat_id=None)  # no chat → no turn appended
        result = {"response": "x", "results": [], "parsed_params": {}}

        out = persist_trace_and_turn(deps, ctx, result)

        assert out is result  # mutates in place
        assert result["trace_id"] == "trace-1"
        assert len(deps.feedback.calls) == 1
        assert deps.feedback.calls[0][0] == "hola"
        assert deps.feedback.calls[0][1] == "streamlit"

    def test_anonymous_call_skips_turn(self):
        deps = FakeDeps()
        ctx = self._mk_ctx(chat_id=None)
        persist_trace_and_turn(deps, ctx, {"response": "x", "results": []})
        assert deps.conv_store.turns == []

    def test_chat_id_appends_turn(self):
        deps = FakeDeps()
        ctx = self._mk_ctx(chat_id="alice")
        result = {
            "response": "ok",
            "results": [
                {"referencia_del_proceso": "LP-001"},
                {"id_contrato": "C-002"},
            ],
            "parsed_params": {"objeto": ["x"]},
        }
        persist_trace_and_turn(deps, ctx, result)
        assert len(deps.conv_store.turns) == 1
        turn = deps.conv_store.turns[0]
        assert turn["chat_id"] == "alice"
        assert turn["response"] == "ok"
        assert turn["parsed_params"] == {"objeto": ["x"]}
        assert turn["result_ids"] == ["LP-001", "C-002"]
        assert turn["trace_id"] == "trace-1"

    def test_user_identity_propagates_to_log_trace(self):
        deps = FakeDeps()
        ctx = self._mk_ctx(user_identity={"role": "alcalde"})
        persist_trace_and_turn(deps, ctx, {"response": "x", "results": []})
        assert deps.feedback.calls[0][3] == {"role": "alcalde"}

    def test_missing_response_uses_empty_default(self):
        """Defensive: persist should not KeyError on a malformed result."""
        deps = FakeDeps()
        ctx = self._mk_ctx(chat_id="bob")
        persist_trace_and_turn(deps, ctx, {"results": []})
        assert deps.conv_store.turns[0]["response"] == ""

    def test_missing_results_uses_empty_default(self):
        deps = FakeDeps()
        ctx = self._mk_ctx(chat_id="bob")
        persist_trace_and_turn(deps, ctx, {"response": "x"})
        assert deps.conv_store.turns[0]["result_ids"] == []
