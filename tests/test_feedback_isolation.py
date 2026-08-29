from __future__ import annotations

import json
from pathlib import Path

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.web_session import new_streamlit_chat_id, reset_user_session


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


# ══════════════════════════════════════════════════════════════════════════════
# Aislamiento del feedback entre dos usuarios del mismo navegador
# ══════════════════════════════════════════════════════════════════════════════

def test_no_trace_of_the_previous_user_survives_the_session_boundary():
    """B no puede ver ni calificar la respuesta de A.

    ``workflow.rate_query`` autoriza por ``trace_id``: quien lo tenga, califica.
    Lo único que impedía que B calificara la consulta de A era que no llegara al
    trace, y el logout dejaba ``history`` intacto —con el trace dentro y con la
    UI de 👍/👎 pintándose sobre él—. Después de la frontera no queda ningún
    trace de A direccionable desde la sesión: ni en el historial, ni en el
    comentario a medio escribir que la caja de texto guarda por trace.
    """
    estado = {
        "history": [
            {"role": "user", "content": "licitaciones de vías en Atlántico"},
            {
                "role": "assistant", "trace_id": "trace-de-ana",
                "rating": None, "rating_comment": None,
            },
        ],
        "comment_trace-de-ana": "el segundo resultado no era",
        "chat_id": "st_sesion_de_ana",
        "_workspace_user": {"key": "email:ana@empresa.com", "user_id": "u-ana"},
    }

    reset_user_session(estado)
    estado.setdefault("history", [])          # lo que resiembra la sesión nueva

    assert estado["history"] == []            # el bucle de render no pinta nada
    assert "comment_trace-de-ana" not in estado
    assert "trace-de-ana" not in json.dumps(estado, default=str)


def test_the_conversation_of_the_previous_user_is_not_reachable_by_the_next(
    tmp_path,
):
    """La memoria de A queda en su archivo; la sesión de B nombra otro."""
    store = ConversationStore(base_path=tmp_path)
    chat_de_ana = new_streamlit_chat_id()
    store.append_turn(chat_de_ana, "contratos del SENA", "12 resultados", {},
                      trace_id="trace-de-ana")

    chat_de_bruno = new_streamlit_chat_id()

    assert store.get_history(chat_de_bruno) == []
    assert store.last_successful_turn(chat_de_bruno) is None
    assert len(store.get_history(chat_de_ana)) == 1   # no se destruyó, se aisló
