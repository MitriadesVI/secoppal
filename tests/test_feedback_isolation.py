from __future__ import annotations

from pathlib import Path

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


def test_workflow_uses_configured_feedback_path_not_real_data_file(tmp_path):
    """Tests must not append traces to data/feedback.jsonl."""
    real_feedback = Path("data/feedback.jsonl")
    before = real_feedback.read_bytes() if real_feedback.exists() else None
    observed_after = None

    try:
        workflow = SecopalWorkflow(Settings(secop_feedback_path=tmp_path / "feedback.jsonl"))
        workflow.secop_client.query = lambda dataset_id, soql: [  # type: ignore[method-assign]
            {
                "referencia_del_proceso": "T-1",
                "nombre_del_procedimiento": "Mantenimiento vial",
                "precio_base": "1000000",
                "fecha_de_publicacion_del": "2026-01-01",
            }
        ]
        workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
        workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]
        workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")

        result = workflow.run_query("pavimentacion", channel="streamlit", chat_id="st_hyg")
        observed_after = real_feedback.read_bytes() if real_feedback.exists() else None
    finally:
        if before is None:
            real_feedback.unlink(missing_ok=True)
        else:
            real_feedback.parent.mkdir(parents=True, exist_ok=True)
            real_feedback.write_bytes(before)

    assert result["trace_id"]
    assert (tmp_path / "feedback.jsonl").exists()
    assert observed_after == before
