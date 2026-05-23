"""B4.4 — E2E tests for clarification path (P1 command_incomplete is the key E2E case).

P2 (non_query_followup) and P8 (invalid_value_range) are covered by
unit tests in test_query_plan.py. P2 requires conversation context
that the E2E harness doesn't fully replicate. P8 is safety net —
the query_router auto-swaps min/max so invalid ranges never reach
the pipeline.
"""

import uuid

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def _wf():
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = lambda ds, soql: [{
        "precio_base": "1000000",
        "fecha_de_publicacion_del": "2026-01-01",
    }]
    wf.secop_client.count = lambda ds, soql: 1
    wf.narrator.narrate_with_grounding = lambda **kw: None
    return wf


def test_command_incomplete_triggers_clarification():
    """'filtra por departamento' → clarification, not SECOP execution."""
    wf = _wf()
    cid = f"test_b44_cmd_{uuid.uuid4().hex[:6]}"
    result = wf.run_query("filtra por departamento", chat_id=cid)
    assert result.get("needs_clarification") is True
    plan = result["query_plan"]
    assert plan["should_execute"] is False
    assert "command_incomplete" in plan.get("reason", "")


def test_command_incomplete_response_mentions_slot():
    wf = _wf()
    cid = f"test_b44_cmd2_{uuid.uuid4().hex[:6]}"
    result = wf.run_query("filtra por ciudad", chat_id=cid)
    assert result.get("needs_clarification") is True
    assert "ciudad" in (result.get("response") or "").lower()


def test_command_incomplete_ordena_por():
    wf = _wf()
    cid = f"test_b44_cmd3_{uuid.uuid4().hex[:6]}"
    result = wf.run_query("ordena por", chat_id=cid)
    assert result.get("needs_clarification") is True


def test_clean_query_not_blocked():
    """A normal query should execute, not be blocked."""
    wf = _wf()
    cid = f"test_b44_clean_{uuid.uuid4().hex[:6]}"
    result = wf.run_query(
        "contratos de mantenimiento en bolívar", chat_id=cid
    )
    assert result.get("needs_clarification") is not True
    plan = result["query_plan"]
    assert plan["should_execute"] is True
