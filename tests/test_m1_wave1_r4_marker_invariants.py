"""M1 wave 1 R4 (orchestrator amendment): invariants of the open-question marker.

``_temporal_open_question`` is an EPHEMERAL CONTROL SEAM, not a semantic dimension: it rides in the
``resolved_params`` of a blocked turn so that the next turn knows a period question is open. The repo
has no pending-clarification store apart from the frame (``actor_pending`` travels in the same
params), so moving it out would need a new persistence slot: not a minimal hook. It stays where it
is, declared as a seam to retire with the Conversation IR (M2). These tests pin what makes that safe:

* it never reaches the SoQL, in any turn;
* it never changes a filter (the same query with and without a question open executes the same SoQL);
* it does not survive a valid answer, nor an explicit reset, nor a complete new search (it is not in
  the frame persisted after them);
* pagination never executes a marked or blocked turn: zero source calls.
"""
from __future__ import annotations

import re
from datetime import date

import pytest

from app.config import Settings, get_settings
from app.core import temporal_period_question as tpq
from app.core.orchestrator import SecopalWorkflow

TODAY = date(2026, 10, 1)
BLOCKED = "contratos de aseo de la Alcaldía de Galapa en los últimos 3 meses de 2025"
ANSWER = "contratos de aseo de la Alcaldía de Galapa de octubre a diciembre de 2025"
NEEDLE = re.compile(r"_temporal|open_question|temporal_open", re.IGNORECASE)


def _flow(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    flow = SecopalWorkflow(Settings(
        _env_file=None, deepseek_api_key=None, secoppal_natural_alpha_mode="off",
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.db",
        secop_feedback_sqlite_enabled=False,
    ))
    calls: list[str] = []
    row = {
        "id_contrato": "X", "nombre_entidad": "ALCALDIA MUNICIPAL DE GALAPA",
        "objeto_del_contrato": "Aseo", "valor_del_contrato": "1000000", "fecha_de_firma": "2025-11-01",
    }
    monkeypatch.setattr(flow.secop_client, "query", lambda dataset, soql, **kw: (calls.append(soql), [dict(row)])[1])
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: (calls.append("count"), 1)[1])
    monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: (calls.append("aggregate"), [])[1])
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    flow.calls = calls
    return flow


@pytest.fixture
def workflow(tmp_path, monkeypatch):
    flow = _flow(tmp_path, monkeypatch)
    yield flow
    get_settings.cache_clear()


@pytest.fixture
def other(tmp_path_factory, monkeypatch):
    """A second workflow (own conversation store) to run the same query with no question open."""
    flow = _flow(tmp_path_factory.mktemp("other"), monkeypatch)
    yield flow
    get_settings.cache_clear()


def _sql(flow) -> list[str]:
    return [call for call in flow.calls if isinstance(call, str) and call not in ("count", "aggregate")]


def _last_turn_params(flow, chat) -> dict:
    return flow.conv_store.get_history(chat, last_n=1)[-1].parsed_params


def test_the_marker_never_reaches_the_soql(workflow):
    workflow.run_query(BLOCKED, chat_id="m1", today=TODAY)
    for follow in ("ver más", "la primera", ANSWER, "ver más", "contratos de aseo de la Alcaldía de Galapa en 2024"):
        workflow.run_query(follow, chat_id="m1", today=TODAY)
    assert _sql(workflow), "something was executed"
    assert not any(NEEDLE.search(sql) for sql in _sql(workflow)), _sql(workflow)


def test_a_question_open_does_not_change_the_filters_of_the_answer(workflow, other):
    workflow.run_query(BLOCKED, chat_id="m2", today=TODAY)
    answered = workflow.run_query(ANSWER, chat_id="m2", today=TODAY)
    fresh = other.run_query(ANSWER, chat_id="m2-fresh", today=TODAY)
    assert answered["needs_clarification"] is False and fresh["needs_clarification"] is False
    assert _sql(workflow) == _sql(other), "the same query executes the same SoQL with or without the question"
    keep = lambda params: {k: v for k, v in params.items() if not k.startswith("_")}
    assert keep(answered["resolved_params"]) == keep(fresh["resolved_params"])


def test_the_marker_is_in_the_blocked_frame_only(workflow):
    first = workflow.run_query(BLOCKED, chat_id="m3", today=TODAY)
    assert tpq.MARKER in first["resolved_params"] and tpq.MARKER in _last_turn_params(workflow, "m3")
    workflow.run_query("la primera", chat_id="m3", today=TODAY)
    assert tpq.MARKER in _last_turn_params(workflow, "m3"), "asking again keeps it open"


def test_a_valid_answer_clears_it_for_good(workflow):
    workflow.run_query(BLOCKED, chat_id="m4", today=TODAY)
    done = workflow.run_query(ANSWER, chat_id="m4", today=TODAY)
    assert done["needs_clarification"] is False
    assert tpq.MARKER not in done["resolved_params"]
    assert tpq.MARKER not in _last_turn_params(workflow, "m4")
    for later in ("ahora solo los de aseo", "ver más", "y en 2024?"):
        result = workflow.run_query(later, chat_id="m4", today=TODAY)
        assert tpq.MARKER not in (result.get("resolved_params") or {}), later
        assert tpq.MARKER not in _last_turn_params(workflow, "m4"), later


def test_an_explicit_reset_clears_it(workflow):
    workflow.run_query(BLOCKED, chat_id="m5", today=TODAY)
    workflow.run_query("borra todos los filtros", chat_id="m5", today=TODAY)
    assert workflow.calls == []
    assert tpq.MARKER not in _last_turn_params(workflow, "m5")
    again = workflow.run_query("la primera", chat_id="m5", today=TODAY)
    assert tpq.MARKER not in (again.get("resolved_params") or {})


@pytest.mark.parametrize("search", [
    "procesos de la Gobernación del Atlántico en 2025",
    "busca contratos de aseo de la Alcaldía de Soledad en 2024",
    "busca contratos de obra de la Alcaldía de Galapa en 2023",
])
def test_a_complete_new_search_clears_it(workflow, search):
    workflow.run_query(BLOCKED, chat_id="m6", today=TODAY)
    result = workflow.run_query(search, chat_id="m6", today=TODAY)
    assert result["needs_clarification"] is False, search
    assert tpq.MARKER not in result["resolved_params"]
    assert tpq.MARKER not in _last_turn_params(workflow, "m6")
    assert not any(NEEDLE.search(sql) for sql in _sql(workflow))


@pytest.mark.parametrize("blocked", [
    BLOCKED,
    "contratos de la Alcaldía de Galapa el 31 de febrero de 2025",
    "contratos de la Alcaldía de Galapa en los últimos 3 meses de la vigencia anterior",
])
def test_pagination_never_executes_a_marked_or_blocked_turn(workflow, blocked):
    workflow.run_query(blocked, chat_id="m7", today=TODAY)
    assert workflow.calls == []
    for phrase in ("ver más", "más resultados", "siguiente página", "ver más", "muéstrame más"):
        result = workflow.run_query(phrase, chat_id="m7", today=TODAY)
        assert workflow.calls == [], (blocked, phrase, workflow.calls)
        assert result["needs_clarification"] is True


def test_the_marker_is_removed_from_a_frame_before_it_merges():
    params = {"objeto": ["aseo"], tpq.MARKER: {"code": tpq.FOLLOWUP_CODE, "question": "q"}}
    clean, pending = tpq.take(params)
    assert tpq.MARKER not in clean and pending is not None
    assert params[tpq.MARKER], "take does not mutate its input"
