"""Workflow tripwire for clarification decided before query planning."""
from __future__ import annotations

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.outcome import Outcome, OutcomeReason


def test_context_reset_is_clarification_without_source_or_zero_recovery(
    tmp_path, monkeypatch,
) -> None:
    """A reset clears a successful frame and cannot masquerade as empty recall."""
    conversation_dir = tmp_path / "conversations"
    monkeypatch.setenv("CONVERSATION_STORE_DIR", str(conversation_dir))
    workflow = SecopalWorkflow(Settings(
        deepseek_api_key=None,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.sqlite",
        modality_followup_telemetry_path=tmp_path / "modality.jsonl",
    ))
    workflow.conv_store = ConversationStore(base_path=conversation_dir)
    workflow.conv_store.append_turn(
        chat_id="outcome-reset-e2e",
        user_query="contratos de obra en caldas",
        response="1 resultado",
        parsed_params={
            "dataset": "contratos",
            "objeto": ["obra"],
            "departamento_resolved": "Caldas",
        },
        result_ids=["seed-result"],
        outcome=Outcome.SUCCESS,
        outcome_reason=OutcomeReason.RESULTS_FOUND,
        result_count=1,
    )

    source_calls: list[str] = []

    def record(method: str, result):
        def call(*args, **kwargs):
            source_calls.append(method)
            return result

        return call

    workflow.secop_client.query = record("query", [])
    workflow.secop_client.count = record("count", 0)
    workflow.secop_client.aggregate = record("aggregate", [])
    workflow.secop_client.with_timeout = lambda *args, **kwargs: workflow.secop_client
    workflow.secop_client.derived_for_timeout = (
        lambda *args, **kwargs: workflow.secop_client
    )

    result = workflow.run_query(
        "borra los filtros", channel="telegram", chat_id="outcome-reset-e2e"
    )

    assert result["needs_clarification"] is True
    assert result["outcome"] == Outcome.CLARIFICATION
    assert result["outcome_reason"] == OutcomeReason.CLARIFICATION_REQUESTED
    assert result["followup_intent_type"] == "reset_context"
    assert result["resolved_params"] == {}
    assert source_calls == []
    assert result.get("recovery_delta") in (None, {})
    assert "zero_results_after_refinement" not in result["response"].lower()
