"""M1 wave 1 R4 (ZEUS3 P2-1): the way out the question writes is a way out.

The question promises «si en realidad quieres otra búsqueda, escríbela completa (entidad, tema y
periodo)», but the answer was refused whenever it carried a topic and the router classed it as a
follow-up: repeating the whole query with an explicit period asked again. Now a turn with an
explicit period and NO relative window is an answer when

* it repeats the topic the frame already had, or
* it names the buyer as well (entity + topic + period: a new search);

and it executes the period it wrote (the follow-up merge of a turn with a topic used to keep the
previous period). A topic that is neither («2025 completo», «los de aseo de octubre a diciembre de
2025») still asks: a word of a reply cannot be told from a topic.

Declared (asks again, by design): «exportar», «más caros primero», «y en Barranquilla?» with no
period; a turn with a relative window.
"""
from __future__ import annotations

import pytest

from app.config import Settings, get_settings
from app.core import temporal_period_question as tpq
from app.core import temporal_window_anchor as twa
from app.core.orchestrator import SecopalWorkflow
from datetime import date

TODAY = date(2026, 10, 1)
PENDING = tpq.Block(twa.ERROR_CODE, "q")
PERIOD = {"fecha_desde": "2025-10-01", "fecha_hasta": "2025-12-31"}
GALAPA = "contratos de aseo de la Alcaldía de Galapa"
BLOCKED = f"{GALAPA} en los últimos 3 meses de 2025"


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


def _sql(workflow) -> str:
    return " ".join(call for call in workflow.calls if isinstance(call, str))


def _ask(workflow, query, chat):
    result = workflow.run_query(query, chat_id=chat, today=TODAY)
    assert result["needs_clarification"] is True and workflow.calls == [], query
    return result


# ── end to end ────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("answer,period", [
    (f"{GALAPA} de octubre a diciembre de 2025", ("2025-10-01", "2025-12-31")),
    (f"{GALAPA} de abril a septiembre de 2026", ("2026-04-01", "2026-09-30")),
    (f"{GALAPA} en 2024", ("2024-01-01", "2024-12-31")),
    (f"{GALAPA} desde julio de 2026", ("2026-07-01", None)),
])
def test_repeating_the_whole_query_with_an_explicit_period_executes_it(workflow, answer, period):
    _ask(workflow, BLOCKED, "c1")
    result = workflow.run_query(answer, chat_id="c1", today=TODAY)
    assert result["needs_clarification"] is False, result["response"]
    params = result["resolved_params"]
    assert (params.get("fecha_desde"), params.get("fecha_hasta")) == period
    assert tpq.MARKER not in params
    sql = _sql(workflow)
    assert "MUNICIPIO DE GALAPA" in sql and "%aseo%" in sql, "buyer and topic of the frame are kept"


@pytest.mark.parametrize("prefix", ["", "nueva búsqueda: ", "olvida eso, "])
def test_a_new_complete_search_with_buyer_topic_and_period_executes(workflow, prefix):
    _ask(workflow, BLOCKED, "c3")
    result = workflow.run_query(
        f"{prefix}contratos de aseo de la Alcaldía de Soledad en 2024", chat_id="c3", today=TODAY,
    )
    assert result["needs_clarification"] is False, result["response"]
    params = result["resolved_params"]
    assert (params["fecha_desde"], params["fecha_hasta"]) == ("2024-01-01", "2024-12-31")
    sql = _sql(workflow)
    assert "MUNICIPIO DE SOLEDAD" in sql and "GALAPA" not in sql
    assert tpq.MARKER not in params


def test_the_answer_period_replaces_the_period_the_frame_already_had(workflow):
    """The follow-up question case: the frame has 2025, the answer writes April-September 2026."""
    workflow.run_query("contratos de vías de la Gobernación del Atlántico en 2025", chat_id="c5", today=TODAY)
    asked = workflow.run_query("y los últimos 6 meses?", chat_id="c5", today=TODAY)
    assert asked["resolved_params"][tpq.MARKER]["code"] == tpq.FOLLOWUP_CODE
    workflow.calls.clear()
    result = workflow.run_query(
        "contratos de vías de la Gobernación del Atlántico de abril a septiembre de 2026",
        chat_id="c5", today=TODAY,
    )
    assert result["needs_clarification"] is False
    params = result["resolved_params"]
    assert (params["fecha_desde"], params["fecha_hasta"]) == ("2026-04-01", "2026-09-30")
    sql = _sql(workflow)
    assert "2026-04-01" in sql and "2025-01-01" not in sql
    assert "%vias%" in sql


@pytest.mark.parametrize("reply", [
    "2025 completo", "la primera", "exportar",
    "más caros primero", "y en Barranquilla?", "los últimos 3 meses",
    f"{GALAPA} en los últimos 6 meses",
])
def test_a_reply_that_is_not_an_explicit_period_with_its_own_buyer_still_asks(workflow, reply):
    _ask(workflow, BLOCKED, "c6")
    again = workflow.run_query(reply, chat_id="c6", today=TODAY)
    assert workflow.calls == [], (reply, workflow.calls)
    assert again["needs_clarification"] is True or "Solo tengo 3 sugerencias" in again["response"], reply
    assert not (again.get("resolved_params") or {}).get("fecha_desde")


def test_a_topic_the_frame_does_not_have_and_no_buyer_asks_again(workflow):
    """The frame has no topic: «los de aseo de …» cannot be told from a word of a reply."""
    _ask(workflow, "contratos de la Alcaldía de Galapa en los últimos 3 meses de 2025", "c8")
    again = workflow.run_query("los de aseo de octubre a diciembre de 2025", chat_id="c8", today=TODAY)
    assert workflow.calls == [] and again["needs_clarification"] is True


def test_the_same_reply_is_an_answer_when_the_frame_has_that_topic(workflow):
    _ask(workflow, BLOCKED, "c9")
    result = workflow.run_query("los de aseo de octubre a diciembre de 2025", chat_id="c9", today=TODAY)
    assert result["needs_clarification"] is False
    assert result["resolved_params"]["fecha_desde"] == "2025-10-01"


def test_after_a_declared_re_ask_the_whole_query_still_answers(workflow):
    _ask(workflow, BLOCKED, "c7")
    workflow.run_query("exportar", chat_id="c7", today=TODAY)
    result = workflow.run_query(f"{GALAPA} de octubre a diciembre de 2025", chat_id="c7", today=TODAY)
    assert result["needs_clarification"] is False
    assert result["resolved_params"]["fecha_desde"] == "2025-10-01"


# ── the decision (pure) ───────────────────────────────────────────────────────────────────

def _block(parsed, previous, intent="contextual_requery", text="x"):
    return tpq.block_for(text=text, intent=intent, parsed=parsed, previous=previous, pending=PENDING)


def test_a_topic_the_frame_already_has_is_an_answer():
    previous = {"objeto": ["aseo"], "entidad": "Municipio de galapa"}
    assert _block({**PERIOD, "objeto": ["aseo"]}, previous) is None
    assert _block({**PERIOD, "objeto": ["aseo"], "entidad": "Municipio de galapa"}, previous) is None


def test_a_subset_of_the_frame_topic_is_an_answer_and_a_superset_is_not():
    previous = {"objeto": ["aseo", "oficinas"]}
    assert _block({**PERIOD, "objeto": ["aseo"]}, previous) is None
    assert _block({**PERIOD, "objeto": ["aseo", "vigilancia"]}, previous) is PENDING


def test_a_topic_the_frame_does_not_have_needs_the_buyer():
    previous = {"objeto": ["aseo"]}
    assert _block({**PERIOD, "objeto": ["completo"]}, previous) is PENDING
    assert _block({**PERIOD, "objeto": ["vigilancia"], "entidad_resolved": "MUNICIPIO DE SOLEDAD"}, previous) is None
    assert _block({**PERIOD, "objeto": ["vigilancia"], "entidad": "Municipio de soledad"}, previous) is None


def test_the_topic_is_read_from_every_topic_key_and_nested_values():
    previous = {"objeto_or": [["aseo", "limpieza"]], "objeto_phrases": ["servicio de aseo"]}
    assert _block({**PERIOD, "objeto": ["aseo"]}, previous) is None
    assert _block({**PERIOD, "objeto_or": [["aseo", "limpieza"]]}, previous) is None
    assert _block({**PERIOD, "objeto_phrases": ["servicio de aseo"]}, previous) is None
    assert _block({**PERIOD, "objeto_phrases": ["servicio de cafetería"]}, previous) is PENDING
    assert _block({**PERIOD, "objeto": ["ASEO"]}, {"objeto": ["aseo"]}) is None, "case does not matter"


def test_a_relative_window_or_an_error_is_never_an_answer_even_with_buyer_and_topic():
    with_everything = {**PERIOD, "objeto": ["aseo"], "entidad": "Municipio de galapa"}
    previous = {"objeto": ["aseo"]}
    assert _block(with_everything, previous, text="aseo de Galapa en los últimos 3 meses") is PENDING
    assert _block({**with_everything, "_temporal_error": "invalid_date"}, previous) is PENDING
    assert _block({"objeto": ["aseo"], "entidad": "Municipio de galapa"}, previous) is PENDING, "no period"


def test_answered_replaces_the_pair_atomically():
    merged = {"fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31", "objeto": ["aseo"]}
    assert tpq.answered(merged, {"fecha_desde": "2026-04-01", "fecha_hasta": "2026-09-30"}) == {
        "fecha_desde": "2026-04-01", "fecha_hasta": "2026-09-30", "objeto": ["aseo"],
    }
    open_range = tpq.answered(merged, {"fecha_desde": "2026-07-01"})
    assert open_range["fecha_desde"] == "2026-07-01" and "fecha_hasta" not in open_range, (
        "an open end is information, not an invitation to inherit the old one"
    )
    assert merged["fecha_hasta"] == "2025-12-31", "the input is not mutated"


def test_settle_changes_nothing_without_a_question_or_on_a_reset():
    merged = {"fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31", "objeto": ["aseo"]}
    parsed = {"fecha_desde": "2026-04-01", "fecha_hasta": "2026-09-30"}
    assert tpq.settle(merged, parsed, pending=None, intent="contextual_requery") is merged
    assert tpq.settle(merged, parsed, pending=PENDING, intent="reset_context") is merged
    settled = tpq.settle(merged, parsed, pending=PENDING, intent="contextual_requery")
    assert (settled["fecha_desde"], settled["fecha_hasta"]) == ("2026-04-01", "2026-09-30")
    assert merged["fecha_desde"] == "2025-01-01"
