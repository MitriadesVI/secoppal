"""M1 wave 1 R5 (ZEUS4 P2): an answer to the period question that CHANGES the frame does not run the old frame.

The question left open belongs to a blocked frame (topic, buyer, value…). A reply that carries a topic,
buyer, territory, value, status or dataset DIFFERENT from that frame is not an answer to it: R4 executed
a hybrid (the new buyer with the old topic, the new period with the old minimum value), a search nobody
asked for. R5 asks again, naming the way out («busca …» → a search the follow-up engine reads as new, or
/reset), unless the engine itself classes the turn as ``new_search`` (then the new search runs, unmerged).
Repeating the frame's own topic, buyer and value with an explicit period still answers (R4 C1/C5).
"""
from __future__ import annotations

from datetime import date

import pytest

from app.config import Settings, get_settings
from app.core import temporal_period_question as tpq
from app.core.orchestrator import SecopalWorkflow

TODAY = date(2026, 10, 1)
PENDING = tpq.Block("relative_window_explicit_anchor", "q")
PERIOD = {"fecha_desde": "2025-10-01", "fecha_hasta": "2025-12-31"}
FRAME = "contratos de aseo de la Alcaldía de Galapa por más de 100 millones"
BLOCKED = f"{FRAME} en los últimos 3 meses de 2025"


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
    row = {"id_contrato": "X", "nombre_entidad": "ALCALDIA MUNICIPAL DE GALAPA", "objeto_del_contrato": "Aseo",
           "valor_del_contrato": "150000000", "fecha_de_firma": "2025-11-01"}
    monkeypatch.setattr(flow.secop_client, "query", lambda dataset, soql, **kw: (calls.append(soql), [dict(row)])[1])
    monkeypatch.setattr(flow.secop_client, "count", lambda *a, **k: (calls.append("count"), 1)[1])
    monkeypatch.setattr(flow.secop_client, "aggregate", lambda *a, **k: (calls.append("aggregate"), [])[1])
    monkeypatch.setattr(flow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    flow.calls = calls
    yield flow
    get_settings.cache_clear()


def _sql(workflow) -> str:
    return " ".join(call for call in workflow.calls if isinstance(call, str))


def _blocked(workflow, chat):
    first = workflow.run_query(BLOCKED, chat_id=chat, today=TODAY)
    assert first["needs_clarification"] is True and workflow.calls == []
    return first


def _frame_of(workflow, chat):
    return workflow.conv_store.get_history(chat, last_n=1)[-1].parsed_params


# ── end to end: a reply that changes the frame asks again, runs nothing, keeps the question open ─────

@pytest.mark.parametrize("reply", [
    "contratos de vigilancia de la Alcaldía de Soledad de octubre a diciembre de 2025",   # topic + buyer
    "contratos de vigilancia de la Gobernación del Atlántico de octubre a diciembre de 2025",
    "contratos de aseo de menos de 50 millones de octubre a diciembre de 2025",            # value
    "contratos de aseo de la Alcaldía de Soledad de octubre a diciembre de 2025",          # buyer
    "contratos de aseo de la Alcaldía de Galapa por menos de 50 millones de octubre a diciembre de 2025",
    "los de la Alcaldía de Soledad de octubre a diciembre de 2025",                        # buyer, no topic
    "contratos de la Gobernación del Atlántico de octubre a diciembre de 2025",
])
def test_a_reply_that_changes_the_frame_asks_and_runs_nothing(workflow, reply):
    _blocked(workflow, "a1")
    result = workflow.run_query(reply, chat_id="a1", today=TODAY)
    assert result["needs_clarification"] is True and workflow.calls == [], (reply, workflow.calls)
    assert not (result.get("resolved_params") or {}).get("fecha_desde"), "the new period is not applied"
    frame = _frame_of(workflow, "a1")
    assert tpq.MARKER in frame, "the question stays open"
    assert frame.get("valor_min") == 100000000 and "SOLEDAD" not in str(frame.get("entidad_resolved")), (
        "the blocked frame is the one that was left, not a hybrid"
    )


def test_the_question_names_the_way_out_when_the_reply_is_another_search(workflow):
    _blocked(workflow, "a2")
    result = workflow.run_query(
        "contratos de vigilancia de la Alcaldía de Soledad de octubre a diciembre de 2025",
        chat_id="a2", today=TODAY,
    )
    for needle in ("«busca …»", "/reset", "sólo el periodo"):
        assert needle in result["response"], needle


def test_the_way_out_the_question_writes_is_one_that_works(workflow):
    """The promise «busca …» is true: the engine reads it as a new search and runs it, unmerged."""
    _blocked(workflow, "a3")
    result = workflow.run_query(
        "busca contratos de vigilancia de la Alcaldía de Soledad de octubre a diciembre de 2025",
        chat_id="a3", today=TODAY,
    )
    assert result["needs_clarification"] is False, result["response"]
    params = result["resolved_params"]
    assert (params["fecha_desde"], params["fecha_hasta"]) == ("2025-10-01", "2025-12-31")
    assert "valor_min" not in params and "GALAPA" not in _sql(workflow), "no fusion with the old frame"
    assert "SOLEDAD" in _sql(workflow) and "%vigilancia%" in _sql(workflow) and tpq.MARKER not in params


def test_the_reset_way_out_works(workflow):
    _blocked(workflow, "a4")
    workflow.run_query("/reset", chat_id="a4", today=TODAY)
    again = workflow.run_query("contratos de aseo de la Alcaldía de Soledad en 2025", chat_id="a4", today=TODAY)
    assert again["needs_clarification"] is False and "SOLEDAD" in _sql(workflow)


# ── repeating the frame still answers (R4 C1/C5 unchanged) ────────────────────────────────────────────

@pytest.mark.parametrize("reply", [
    f"{FRAME} de octubre a diciembre de 2025",                                  # the whole frame, same value
    "contratos de aseo de la Alcaldía de Galapa de octubre a diciembre de 2025",  # says less (no value)
    "los de aseo de octubre a diciembre de 2025",                                # topic the frame has
    "octubre a diciembre de 2025",                                               # the period alone
])
def test_the_same_frame_with_an_explicit_period_executes_it(workflow, reply):
    _blocked(workflow, "b1")
    result = workflow.run_query(reply, chat_id="b1", today=TODAY)
    assert result["needs_clarification"] is False, (reply, result["response"])
    params = result["resolved_params"]
    assert (params["fecha_desde"], params["fecha_hasta"]) == ("2025-10-01", "2025-12-31")
    assert "MUNICIPIO DE GALAPA" in _sql(workflow) and "%aseo%" in _sql(workflow)
    assert tpq.MARKER not in params


# ── the decision (pure) ──────────────────────────────────────────────────────────────────────────────

def _block(parsed, previous, intent="change_scope", text="x"):
    return tpq.block_for(text=text, intent=intent, parsed=parsed, previous=previous, pending=PENDING)


PREVIOUS = {
    "objeto": ["aseo"], "entidad": "Municipio de galapa", "entidad_resolved": "MUNICIPIO DE GALAPA.",
    "valor_min": 100000000, "dataset": "contratos",
}


@pytest.mark.parametrize("changed", [
    {"objeto": ["vigilancia"]},
    {"entidad_resolved": "MUNICIPIO DE SOLEDAD"},
    {"entidad": "Alcaldía de soledad"},
    {"departamento_resolved": "Atlántico"},
    {"valor_max": 50000000},
    {"valor_min": 200000000},
    {"estado_family": "vigente"},
    {"contract_type_include": ["obra"]},
    {"objeto_exclude": ["aseo"]},
    {"dataset": "procesos", "dataset_explicit": True},
])
def test_each_kind_of_condition_that_changes_is_not_an_answer(changed):
    assert _block({**PERIOD, **changed}, PREVIOUS) is not None


@pytest.mark.parametrize("same", [
    {},
    {"objeto": ["aseo"]},
    {"objeto": ["ASEO"]},
    {"entidad_resolved": "MUNICIPIO DE GALAPA."},
    {"entidad": "Municipio de galapa"},
    {"valor_min": 100000000},
    {"dataset": "contratos", "dataset_explicit": True},
    {"dataset": "contratos"},
])
def test_repeating_what_the_frame_has_is_an_answer(same):
    assert _block({**PERIOD, **same}, PREVIOUS) is None


def test_a_complete_new_search_runs_whatever_it_changes():
    changed = {**PERIOD, "objeto": ["vigilancia"], "entidad_resolved": "MUNICIPIO DE SOLEDAD", "valor_max": 5}
    assert _block(changed, PREVIOUS, intent="new_search") is None
    assert _block(changed, PREVIOUS) is not None


def test_only_another_search_is_told_about_the_way_out():
    stray = _block({**PERIOD, "objeto": ["completo"]}, PREVIOUS)
    other = _block({**PERIOD, "entidad_resolved": "MUNICIPIO DE SOLEDAD"}, PREVIOUS)
    assert stray == PENDING and stray.changed_frame is False and "busca" not in stray.shown
    assert other.changed_frame is True and "busca" in other.shown and "/reset" in other.shown
    assert other.code == PENDING.code and other.question == PENDING.question, "the open question is kept"


def test_a_window_or_an_error_in_the_reply_keeps_the_plain_question():
    other = {"entidad_resolved": "MUNICIPIO DE SOLEDAD"}
    assert _block({**PERIOD, **other, "_temporal_error": "invalid_date"}, PREVIOUS) == PENDING
    assert _block({**PERIOD, **other}, PREVIOUS, text="de Soledad en los últimos 3 meses") == PENDING
    assert _block({"objeto": ["aseo"], **other}, PREVIOUS) == PENDING, "no period"
