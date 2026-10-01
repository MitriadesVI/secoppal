"""M1 wave 1 R3 (ZEUS2 P2-3, blocking): the new question cannot open a silent path.

R2's question offered «(1) … (2) el periodo «de 2025» completo». The next turn was read as a
new follow-up: «2», «la opción 2» and «2025 completo» executed ``LIKE '%opcion%'`` /
``'%completo%'`` with no date (or with the year and an invented topic). Two changes:

* the question asks for the period WRITTEN OUT with examples the authority already resolves
  (no numbered options, no «completo»);
* a blocked turn leaves a marker (``temporal_period_question``): until a period answers it,
  any other reply asks again and nothing is executed.

End to end through ``run_query`` with a fake client: every natural reply either executes the
explicit period it names (with no word of the reply as the topic) or asks again. «1» / «2»
fall in the pre-existing suggestion selector («Solo tengo 3 sugerencias…»), which this round
does not touch.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from app.config import Settings, get_settings
from app.core import temporal_gate
from app.core import temporal_period_question as tpq
from app.core import temporal_window_anchor as twa
from app.core.entity_resolver import EntityResolver
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter

TODAY = date(2026, 10, 1)
SELECTOR = "Solo tengo 3 sugerencias"

BLOCKED = [
    "contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025",
    "contratos de la Alcaldía de Galapa en los dos últimos meses de 2025",
    "contratos de la Alcaldía de Galapa en la última semana de marzo de 2025",
]
# What a person types back. None of them is an explicit period.
NOT_AN_ANSWER = [
    "1", "2", "la primera", "la segunda", "la opción 2", "opción 1", "la 2", "ambas", "sí", "no",
    "2025 completo", "el periodo de 2025 completo", "el año completo", "dentro de 2025",
    "los últimos 3 meses", "el último mes", "la ventana", "hacia atrás", "gracias",
    "el periodo «de 2025» completo", "(2)", "2)",
]
# Replies that name an explicit period: they execute it and nothing else.
ANSWERS = [
    ("octubre a diciembre de 2025", ("2025-10-01", "2025-12-31")),
    ("desde julio de 2026", ("2026-07-01", None)),
    ("del 2025-10-01 al 2025-12-31", ("2025-10-01", "2025-12-31")),
    ("de 2025", ("2025-01-01", "2025-12-31")),
    ("todo 2025", ("2025-01-01", "2025-12-31")),
    ("octubre-diciembre 2025", ("2025-10-01", "2025-12-31")),
    ("último trimestre de 2025", ("2025-10-01", "2025-12-31")),
]


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


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
    monkeypatch.setattr(flow.secop_client, "query", lambda dataset, soql, **kw: (calls.append(soql), [dict(row)])[1])
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: (calls.append("count"), 1)[1])
    monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: (calls.append("aggregate"), [])[1])
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    flow.calls = calls
    yield flow
    get_settings.cache_clear()


def _blocked(workflow, query: str, chat: str):
    result = workflow.run_query(query, chat_id=chat, today=TODAY)
    assert result["needs_clarification"] is True and workflow.calls == [], query
    return result


# ── the question ─────────────────────────────────────────────────────────────────────────────

def test_the_question_offers_no_numbered_options_and_no_completo():
    found = twa.AnchoredWindow("los ultimos 3 meses", "de 2025", (0, 10))
    for question in (twa.clarification(found), tpq._followup_question("la ultima semana", {
        "fecha_desde": "2025-03-01", "fecha_hasta": "2025-03-31",
    })):
        assert not re.search(r"\(\d\)|\b\d\)|opci[oó]n|completo|primera|segunda", question, re.I), question
        for example in twa.EXAMPLES:
            assert f"«{example}»" in question
        assert "desde hoy" in question


@pytest.mark.parametrize("example", twa.EXAMPLES)
def test_every_example_of_the_question_is_a_period_the_parser_already_reads(router, example):
    params = router.parse(example, today=TODAY).params
    assert params.get("fecha_desde") and not params.get("_temporal_error"), example
    assert not any(params.get(key) for key in ("objeto", "objeto_or", "objeto_phrases")), params


# ── end to end: the replies ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query", BLOCKED)
@pytest.mark.parametrize("reply", NOT_AN_ANSWER)
def test_a_reply_that_is_not_a_period_never_executes(workflow, query, reply):
    _blocked(workflow, query, "r3-q")
    result = workflow.run_query(reply, chat_id="r3-q", today=TODAY)
    assert workflow.calls == [], (reply, workflow.calls)
    assert result["needs_clarification"] is True or SELECTOR in result["response"], (reply, result["response"])
    assert not (result.get("resolved_params") or {}).get("fecha_desde")


@pytest.mark.parametrize("query", BLOCKED)
@pytest.mark.parametrize("reply,period", ANSWERS)
def test_an_explicit_period_executes_it_and_nothing_else(workflow, query, reply, period):
    _blocked(workflow, query, "r3-a")
    result = workflow.run_query(reply, chat_id="r3-a", today=TODAY)
    assert result["needs_clarification"] is False, (reply, result["response"])
    params = result["resolved_params"]
    assert (params.get("fecha_desde"), params.get("fecha_hasta")) == period, reply
    assert tpq.MARKER not in params
    sql = " ".join(call for call in workflow.calls if isinstance(call, str))
    assert "MUNICIPIO DE GALAPA" in sql, "the understood scope is kept"
    assert "LIKE" not in sql.split("WHERE", 1)[-1].replace("unaccent(UPPER(nombre_entidad))", ""), sql
    assert tpq.MARKER not in sql


def test_the_marker_is_in_the_blocked_frame_and_never_in_an_executed_one(workflow):
    first = _blocked(workflow, BLOCKED[0], "r3-m")
    assert first["resolved_params"][tpq.MARKER]["code"] == twa.ERROR_CODE
    again = workflow.run_query("la primera", chat_id="r3-m", today=TODAY)
    assert again["resolved_params"][tpq.MARKER]["code"] == twa.ERROR_CODE, "asking again keeps it open"
    done = workflow.run_query("octubre a diciembre de 2025", chat_id="r3-m", today=TODAY)
    assert tpq.MARKER not in done["resolved_params"]
    later = workflow.run_query("ahora solo los de aseo", chat_id="r3-m", today=TODAY)
    assert tpq.MARKER not in (later.get("resolved_params") or {})


def test_a_reply_that_is_itself_a_new_complete_search_runs(workflow):
    _blocked(workflow, BLOCKED[0], "r3-n")
    result = workflow.run_query(
        "procesos de la Gobernación del Atlántico en 2025", chat_id="r3-n", today=TODAY,
    )
    assert result["needs_clarification"] is False
    assert result["resolved_params"]["fecha_desde"] == "2025-01-01"
    assert tpq.MARKER not in result["resolved_params"]


def test_an_invalid_period_reply_keeps_the_question_open(workflow):
    _blocked(workflow, BLOCKED[0], "r3-i")
    wrong = workflow.run_query("del 31 de febrero de 2025", chat_id="r3-i", today=TODAY)
    assert wrong["needs_clarification"] is True and workflow.calls == []
    assert wrong["resolved_params"][tpq.MARKER]["code"] == twa.ERROR_CODE
    still = workflow.run_query("la primera", chat_id="r3-i", today=TODAY)
    assert still["needs_clarification"] is True and workflow.calls == []


def test_the_follow_up_window_question_is_closed_the_same_way(workflow):
    workflow.run_query("contratos de la Alcaldía de Galapa en 2025", chat_id="r3-f", today=TODAY)
    before = len(workflow.calls)
    asked = workflow.run_query("ahora solo el último mes", chat_id="r3-f", today=TODAY)
    assert asked["needs_clarification"] is True and len(workflow.calls) == before
    assert asked["resolved_params"][tpq.MARKER]["code"] == tpq.FOLLOWUP_CODE
    for reply in ("la primera", "2", "ambas", "2025 completo"):
        again = workflow.run_query(reply, chat_id="r3-f", today=TODAY)
        assert len(workflow.calls) == before, reply
        assert again["needs_clarification"] is True or SELECTOR in again["response"], reply
    done = workflow.run_query("octubre a diciembre de 2025", chat_id="r3-f", today=TODAY)
    assert done["needs_clarification"] is False
    assert done["resolved_params"]["fecha_desde"] == "2025-10-01"


def test_an_explicit_reset_leaves_the_question_and_executes_nothing(workflow):
    _blocked(workflow, BLOCKED[0], "r3-r")
    result = workflow.run_query("borra todos los filtros", chat_id="r3-r", today=TODAY)
    assert workflow.calls == []
    assert tpq.MARKER not in (result.get("resolved_params") or {})
    assert "borre los filtros" in result["response"].lower()
    after = workflow.run_query("la primera", chat_id="r3-r", today=TODAY)
    assert tpq.MARKER not in (after.get("resolved_params") or {}), "the question is gone for good"


def test_the_re_asked_question_names_the_way_out(workflow):
    _blocked(workflow, BLOCKED[0], "r3-w")
    again = workflow.run_query("la primera", chat_id="r3-w", today=TODAY)
    assert "escríbela completa" in again["response"]
    assert twa.EXAMPLES[0] in again["response"]


# ── the decision (pure) ────────────────────────────────────────────────────────────────────

PENDING = tpq.Block(twa.ERROR_CODE, "q")
PERIOD = {"fecha_desde": "2025-10-01", "fecha_hasta": "2025-12-31"}


def _block(text, parsed, intent="change_year", pending=PENDING, previous=None):
    return tpq.block_for(text=text, intent=intent, parsed=parsed, previous=previous or {}, pending=pending)


def test_an_answer_is_an_explicit_period_without_a_window_and_without_a_stray_topic():
    assert _block("octubre a diciembre de 2025", PERIOD) is None
    assert _block("x", {}) is PENDING
    assert _block("los últimos 3 meses", {"fecha_desde": "2026-07-01", "fecha_hasta": "2026-10-01"}) is PENDING
    # R4: a topic the frame does not have, with no buyer, is a word of a reply, not a search.
    assert _block("2025 completo", {**PERIOD, "objeto": ["completo"]}, intent="contextual_requery") is PENDING
    assert _block("aseo en 2025", {**PERIOD, "objeto_or": ["aseo"]}, intent="contextual_requery") is PENDING
    assert _block("x", {**PERIOD, "_temporal_error": "invalid_date"}) is PENDING


def test_a_reset_is_never_re_asked():
    assert _block("borra todos los filtros", {}, intent="reset_context") is None


def test_a_new_search_with_a_period_answers_even_with_a_topic():
    assert _block("aseo en Medellín en 2025", {**PERIOD, "objeto": ["aseo"]}, intent="new_search") is None


def test_only_open_codes_leave_a_marker():
    params: dict = {}
    tpq.reopen(params, "invalid_date", "q")
    assert params == {}
    tpq.reopen(params, twa.ERROR_CODE, "q")
    assert params[tpq.MARKER] == {"code": twa.ERROR_CODE, "question": "q"}


def test_take_strips_the_marker_and_ignores_a_malformed_one():
    clean, pending = tpq.take({"a": 1, tpq.MARKER: {"code": tpq.FOLLOWUP_CODE, "question": "q"}})
    assert clean == {"a": 1} and pending == tpq.Block(tpq.FOLLOWUP_CODE, "q")
    for bad in ({"code": "invalid_date"}, "x", None, {}):
        clean, pending = tpq.take({"a": 1, tpq.MARKER: bad})
        assert clean == {"a": 1} and pending is None


def test_the_gate_leaves_the_marker_only_for_the_window_questions():
    params = {"_temporal_error": twa.ERROR_CODE, "_temporal_clarification": "pregunta"}
    block = temporal_gate.temporal_plan_block(params, [])
    assert block["should_execute"] is False
    assert params == {tpq.MARKER: {"code": twa.ERROR_CODE, "question": "pregunta"}}
    other = {"_temporal_error": "invalid_date", "_temporal_clarification": "otra"}
    assert temporal_gate.temporal_plan_block(other, []) is not None
    assert other == {}, "a different error leaves nothing behind"
