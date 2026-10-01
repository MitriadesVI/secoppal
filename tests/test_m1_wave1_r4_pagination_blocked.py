"""M1 wave 1 R4 (ZEUS3 P2-2): «ver más» after a turn that never executed has no page to continue.

A blocked turn (a period question, an invalid date) is persisted with the scope it understood and
without a date. Pagination read that turn's params as «the search whose next page is wanted» and
executed it WITHOUT the period: «contratos de la Alcaldía de Galapa en los últimos 3 meses de
2025» -> «ver más» -> every Galapa contract of any date. Pagination now refuses with a short
message, runs nothing, and does not persist the refusal (the blocked frame and its open question
stay the last turn).

Declared improvement over the base: the invalid-date case («31 de febrero de 2025» -> «ver más»)
executed the buyer without a date in the base too.

R5: the rule is scoped to what wave 1 created. The persisted signal is the marker of
``temporal_period_question``, which the temporal gate leaves in EVERY turn a temporal error blocked
(the window question AND the invalid date). A clarification of another route (P15, ambiguous actor…)
carries no marker and paginates as in the base/R3 (declared debt
PAGINATION_AFTER_NON_TEMPORAL_CLARIFICATION, owner M2/pagination).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pytest

from app.config import Settings, get_settings
from app.core import pagination_blocked as pb
from app.core import temporal_period_question as tpq
from app.core.orchestrator import SecopalWorkflow

TODAY = date(2026, 10, 1)
PHRASES = ["ver más", "más resultados", "siguiente página", "muéstrame más", "muestrame mas"]
ANCHORED = "contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025"
INVALID = "contratos de la Alcaldía de Galapa el 31 de febrero de 2025"


@pytest.fixture
def workflow(tmp_path, monkeypatch):
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
    monkeypatch.setattr(flow.secop_client, "query", lambda dataset, soql, **kw: (
        calls.append(soql),
        [{**row, "id_contrato": f"X{n}", "referencia_del_contrato": f"R{n}"} for n in range(60)],
    )[1])
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: (calls.append("count"), 60)[1])
    monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: (calls.append("aggregate"), [])[1])
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    flow.calls = calls
    yield flow
    get_settings.cache_clear()


def _history_length(workflow, chat):
    return len(workflow.conv_store.get_history(chat, last_n=50))


@pytest.mark.parametrize("blocked_query", [ANCHORED, INVALID])
@pytest.mark.parametrize("phrase", PHRASES)
def test_pagination_refuses_after_a_blocked_turn(workflow, blocked_query, phrase):
    first = workflow.run_query(blocked_query, chat_id="p1", today=TODAY)
    assert first["needs_clarification"] is True and workflow.calls == []
    before = _history_length(workflow, "p1")
    result = workflow.run_query(phrase, chat_id="p1", today=TODAY)
    assert workflow.calls == [], (phrase, workflow.calls)
    assert result["needs_clarification"] is True
    assert result["response"] == pb.MESSAGE
    assert result.get("route_reason") == "pagination_blocked_turn"
    assert _history_length(workflow, "p1") == before, "the refusal is not a turn"


def test_a_second_ver_mas_is_refused_too(workflow):
    workflow.run_query(INVALID, chat_id="p2", today=TODAY)
    for _ in range(3):
        again = workflow.run_query("ver más", chat_id="p2", today=TODAY)
        assert again["response"] == pb.MESSAGE and workflow.calls == []


def test_the_open_question_survives_a_refused_ver_mas(workflow):
    workflow.run_query(ANCHORED, chat_id="p3", today=TODAY)
    workflow.run_query("ver más", chat_id="p3", today=TODAY)
    done = workflow.run_query("octubre a diciembre de 2025", chat_id="p3", today=TODAY)
    assert done["needs_clarification"] is False
    assert (done["resolved_params"]["fecha_desde"], done["resolved_params"]["fecha_hasta"]) == (
        "2025-10-01", "2025-12-31",
    )


def test_pagination_refuses_after_the_follow_up_window_question(workflow):
    workflow.run_query("contratos de la Alcaldía de Galapa en 2025", chat_id="p4", today=TODAY)
    asked = workflow.run_query("ahora solo el último mes", chat_id="p4", today=TODAY)
    assert asked["resolved_params"][tpq.MARKER]["code"] == tpq.FOLLOWUP_CODE
    workflow.calls.clear()
    result = workflow.run_query("ver más", chat_id="p4", today=TODAY)
    assert result["response"] == pb.MESSAGE and workflow.calls == []


def test_pagination_after_an_executed_search_still_pages(workflow):
    first = workflow.run_query("contratos de la Alcaldía de Galapa en 2025", chat_id="p5", today=TODAY)
    assert first["needs_clarification"] is False
    workflow.calls.clear()
    more = workflow.run_query("ver más", chat_id="p5", today=TODAY)
    assert more.get("route_reason") != "pagination_blocked_turn"
    assert more["response"] != pb.MESSAGE
    assert more["needs_clarification"] is False
    assert more.get("intent_type") == "pagination_more" and more["rows"], "the next page is served"


def test_a_search_after_the_refusal_runs_normally(workflow):
    workflow.run_query(ANCHORED, chat_id="p6", today=TODAY)
    workflow.run_query("ver más", chat_id="p6", today=TODAY)
    fresh = workflow.run_query("contratos de la Alcaldía de Galapa en 2024", chat_id="p6", today=TODAY)
    assert fresh["needs_clarification"] is False
    assert fresh["resolved_params"]["fecha_desde"] == "2024-01-01"


# ── the decision (pure) ───────────────────────────────────────────────────────────────────

@dataclass
class FakeTurn:
    parsed_params: dict = field(default_factory=dict)
    outcome: str = ""


def test_a_turn_is_blocked_only_by_the_temporal_marker():
    assert pb.is_blocked(FakeTurn({tpq.MARKER: {"code": "x"}}))
    assert pb.is_blocked(FakeTurn({tpq.MARKER: {"code": "invalid_date", "question": ""}}))
    for outcome in ("clarification", "success", "empty", ""):
        assert not pb.is_blocked(FakeTurn({"objeto": ["aseo"]}, outcome=outcome)), outcome


def test_an_unrecognisable_turn_is_not_blocked():
    assert not pb.is_blocked(None)
    assert not pb.is_blocked(object())


# ── R5: scoped to the temporal blocks wave 1 created ──────────────────────────────────────

NON_TEMPORAL_CLARIFICATION = "procesos abiertos en Barranquilla"  # P15: broad opportunity asks


def test_the_invalid_date_turn_carries_the_marker_and_the_anchor_turn_too(workflow):
    for chat, query in (("m1", INVALID), ("m2", ANCHORED)):
        workflow.run_query(query, chat_id=chat, today=TODAY)
        last = workflow.conv_store.get_history(chat, last_n=1)[-1]
        assert pb.is_blocked(last), query


def test_an_invalid_date_answer_is_an_ordinary_follow_up(workflow):
    """The marker of a non-OPEN code only records «not executed»: it opens no question."""
    workflow.run_query(INVALID, chat_id="m3", today=TODAY)
    done = workflow.run_query("mejor el 28 de febrero de 2025", chat_id="m3", today=TODAY)
    assert done["needs_clarification"] is False
    assert tpq.MARKER not in done["resolved_params"]
    assert not pb.is_blocked(workflow.conv_store.get_history("m3", last_n=1)[-1])


def test_a_non_temporal_clarification_paginates_as_in_the_base(workflow):
    first = workflow.run_query(NON_TEMPORAL_CLARIFICATION, chat_id="n1", today=TODAY)
    assert first["needs_clarification"] is True
    assert not pb.is_blocked(workflow.conv_store.get_history("n1", last_n=1)[-1])
    more = workflow.run_query("muéstrame más", chat_id="n1", today=TODAY)
    assert more.get("route_reason") == "pagination_more"
    assert more["response"] != pb.MESSAGE


def test_a_reformulation_with_an_explicit_period_executes_and_then_pages(workflow):
    workflow.run_query(ANCHORED, chat_id="n2", today=TODAY)
    refused = workflow.run_query("ver más", chat_id="n2", today=TODAY)
    assert refused["response"] == pb.MESSAGE
    done = workflow.run_query(
        "contratos de la Alcaldía de Galapa de octubre a diciembre de 2025", chat_id="n2", today=TODAY,
    )
    assert done["needs_clarification"] is False and workflow.calls
    assert not pb.is_blocked(workflow.conv_store.get_history("n2", last_n=1)[-1])
    workflow.calls.clear()
    more = workflow.run_query("ver más", chat_id="n2", today=TODAY)
    assert more.get("route_reason") != "pagination_blocked_turn" and more["rows"]


def test_the_invalid_date_refusal_runs_nothing(workflow):
    workflow.run_query(INVALID, chat_id="n4", today=TODAY)
    workflow.calls.clear()
    assert workflow.run_query("ver más", chat_id="n4", today=TODAY)["response"] == pb.MESSAGE
    assert workflow.calls == []


def test_the_gate_marks_a_turn_blocked_by_a_temporal_invalid_signal_too():
    """The second door of the temporal gate (a typed ``temporal_invalid`` signal) marks the frame as well."""
    from types import SimpleNamespace

    from app.core.temporal_gate import temporal_plan_block

    params = {"objeto": ["aseo"]}
    signal = SimpleNamespace(kind="temporal_invalid", payload={"code": "reverse_range", "question": "¿qué rango?"})
    block = temporal_plan_block(params, [signal])
    assert block is not None and block["should_execute"] is False
    assert pb.is_blocked(FakeTurn(params)), "pagination must refuse this frame"
    assert tpq.take(params)[1] is None, "and it opens no period question"
