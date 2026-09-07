"""Fail-closed structural ambiguity through workflow, UI, and explicit selection."""
import socket

import pytest
from burr.core import State

from app.config import Settings
from app.core.actor_candidate_ui import actor_candidate_options
from app.core.actor_name import ActorResolutionResult
from app.core.actor_resolution import project_actor_resolution
from app.core.actor_resolution_trace import actor_resolution_trace
from app.core.actor_scope_policy import evaluate_actor_scope
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from tests.actor_prefix_fixtures import CHALLENGERS, EXACT, NAME, resolver_for


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("external I/O forbidden")
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "create_connection", forbidden)


def _workflow(tmp_path, monkeypatch):
    resolver = resolver_for(tmp_path / "actors.sqlite", [EXACT, *CHALLENGERS])
    settings = Settings(deepseek_api_key=None, secop_actor_alias_index_path=resolver.index_path,
                        secop_feedback_path=tmp_path / "feedback.jsonl",
                        secop_feedback_db_path=tmp_path / "feedback.sqlite",
                        secop_feedback_sqlite_enabled=False)
    workflow = SecopalWorkflow(settings)
    calls = []
    def empty(*args, **kwargs):
        calls.append((args, kwargs))
        return []
    monkeypatch.setattr(workflow.secop_client, "query", empty)
    monkeypatch.setattr(workflow.secop_client, "count", lambda *a, **k: 0)
    monkeypatch.setattr(workflow.secop_client, "aggregate", empty)
    monkeypatch.setattr(workflow.narrator, "narrate_with_grounding", lambda **kwargs: None)
    return workflow, calls


def test_ambiguity_ui_trace_and_rank_selection_reaches_profile(tmp_path, monkeypatch):
    workflow, calls = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query("contratos del proveedor " + NAME + " en 2025", chat_id="prefix")
    assert first["needs_clarification"]
    assert calls == []
    params = first["resolved_params"]
    assert params["actor_resolution_method"] == "exact_prefix_ambiguous"
    assert [o.observed_name for o in actor_candidate_options(params)] == [NAME.upper(), (NAME + " diaz").upper(), (NAME + " perez").upper()]
    assert actor_resolution_trace(params) == {
        "actor_resolution_status": "AMBIGUOUS", "actor_resolution_method": "exact_prefix_ambiguous",
        "candidates_count": 3, "candidates_has_more": False, "enumeration_incomplete": False,
    }
    text = first["response"]
    assert "coincidencia exacta" in text and "más largos" in text
    def forbidden(*args, **kwargs):
        raise AssertionError("selected identity reopened challenger")
    monkeypatch.setattr(type(workflow.actor_name_resolver), "resolve_exact", forbidden)
    monkeypatch.setattr(type(workflow.actor_name_resolver), "resolve", forbidden)
    selected = workflow.run_query("2", chat_id="prefix")
    assert selected["route_reason"] == "actor_profile"
    assert selected["resolved_params"]["actor_resolution_method"] == "user_selected_candidate"
    assert selected["resolved_params"]["actor_scope"]["base_number"] == "70000003"
    assert selected["resolved_params"]["fecha_desde"] == "2025-01-01"
    assert "actor_pending" not in selected["resolved_params"]


@pytest.mark.parametrize("query", ["contratos del proveedor " + NAME, "contratos de " + NAME, "qué contratos tiene " + NAME])
def test_inconclusive_reaches_policy_without_secop_fuzzy_or_other_probe(tmp_path, monkeypatch, query):
    from app.data import actor_exact_prefix as prefix
    monkeypatch.setattr(prefix, "EXISTENCE_BUDGET_MS", 0)
    workflow, calls = _workflow(tmp_path, monkeypatch)
    probes = []
    original = workflow.actor_name_resolver.resolve_exact
    def probe(name):
        probes.append(name)
        return original(name)
    monkeypatch.setattr(workflow.actor_name_resolver, "resolve_exact", probe)
    monkeypatch.setattr(workflow.actor_name_resolver, "_fuzzy_candidates", lambda *a: pytest.fail("fuzzy opened"))
    result = workflow.run_query(query, chat_id="inconclusive")
    assert result["needs_clarification"]
    params = result["resolved_params"]
    assert params["actor_resolution_status"] == "INCONCLUSIVE"
    assert params["actor_resolution_method"] == "exact_prefix_check_inconclusive"
    assert params.get("actor_pending") is None
    assert not evaluate_actor_scope(params).should_execute
    assert "no pude" in result["response"].lower()
    assert calls == []
    assert probes[-1] == NAME
    assert probes.count(NAME) == 1


def test_conversation_probe_inconclusive_does_not_return_generic_baseline(tmp_path):
    class Resolver:
        def resolve_exact(self, query):
            return ActorResolutionResult("INCONCLUSIVE", query, method="exact_prefix_check_inconclusive", reason="budget")
        def resolve(self, query):
            pytest.fail("fuzzy fallback")
    state = State({"raw_query": "qué contratos tiene " + NAME, "parsed_params": {
        "objeto": NAME, "actor_conversation_probe": {"actor_name": NAME}}})
    params = resolve_actor_name(state, Resolver())["parsed_params"]
    assert params["actor_resolution_status"] == "INCONCLUSIVE"
    assert not params.get("objeto")
    assert not evaluate_actor_scope(params).should_execute


@pytest.mark.parametrize("incomplete", [False, True])
def test_more_options_and_unknown_enumeration_have_distinct_copy(tmp_path, monkeypatch, incomplete):
    from app.data import actor_exact_prefix as prefix
    rows = [EXACT] + [("CC", str(72000000 + n), NAME + f" apellido {n}") for n in range(12)]
    resolver = resolver_for(tmp_path / "actors.sqlite", rows)
    if incomplete:
        monkeypatch.setattr(prefix, "ENUMERATION_BUDGET_MS", 0)
    result = resolver.resolve(NAME)
    params = project_actor_resolution({}, result, actor_name=NAME, original_query=NAME, origin="strong_actor_syntax")
    pending = params["actor_pending"]
    assert pending["candidates_has_more"] is (None if incomplete else True)
    assert pending["enumeration_incomplete"] is incomplete
    text = evaluate_actor_scope(params).clarification_question
    assert ("Hay más opciones compatibles" in text) is not incomplete
    assert ("No pude completar la lista" in text) is incomplete
    assert len(actor_candidate_options(params)) == (2 if incomplete else 10)


@pytest.mark.parametrize("topic", ["mantenimiento", "alimentacion", "adulto mayor", "software", "vigilancia", "aseo", "transporte"])
def test_topic_baseline_is_preserved_with_synthetic_directory(tmp_path, topic):
    resolver = resolver_for(tmp_path / "actors.sqlite", [EXACT, *CHALLENGERS, ("CC", "70000999", "transporte")])
    query = "contratos de " + topic
    baseline = dict(QueryRouter().parse(query).params)
    params = resolve_actor_name(State({"raw_query": query, "parsed_params": baseline}), resolver)["parsed_params"]
    if topic == "transporte":
        assert evaluate_actor_scope(params).reason == "actor_or_topic_ambiguous"
    else:
        assert dict(params) == baseline


@pytest.mark.parametrize("initial", ["AMBIGUOUS", "INCONCLUSIVE"])
@pytest.mark.parametrize("document", ["CC 70000003", "NIT 900111222"])
def test_document_followup_exits_clarification_without_another_challenger(tmp_path, monkeypatch, initial, document):
    from app.data import actor_exact_prefix as prefix
    if initial == "INCONCLUSIVE":
        monkeypatch.setattr(prefix, "EXISTENCE_BUDGET_MS", 0)
    workflow, _ = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query("contratos del proveedor " + NAME + " en 2025", chat_id="document")
    assert first["resolved_params"]["actor_resolution_status"] == initial
    def forbidden(*args, **kwargs):
        raise AssertionError("explicit document reopened name resolver")
    monkeypatch.setattr(workflow.actor_name_resolver, "resolve_exact", forbidden)
    monkeypatch.setattr(workflow.actor_name_resolver, "resolve", forbidden)
    selected = workflow.run_query(document, chat_id="document")
    assert selected["route_reason"] == "actor_profile", repr(selected["resolved_params"])
    params = selected["resolved_params"]
    assert params["actor_resolution_method"] == "explicit_document"
    assert params["actor_scope"]["base_number"] == document.split()[1]
    if initial == "INCONCLUSIVE":
        assert params["fecha_desde"] == "2025-01-01"
    assert params.get("actor_pending") is None
    assert "candidates_has_more" not in actor_resolution_trace(params)
    assert "enumeration_incomplete" not in actor_resolution_trace(params)


def test_full_name_followup_after_inconclusive_resolves_when_check_completes(tmp_path, monkeypatch):
    from app.data import actor_exact_prefix as prefix
    budget = prefix.EXISTENCE_BUDGET_MS
    monkeypatch.setattr(prefix, "EXISTENCE_BUDGET_MS", 0)
    workflow, _ = _workflow(tmp_path, monkeypatch)
    first = workflow.run_query("contratos del proveedor " + NAME, chat_id="fuller")
    assert first["resolved_params"]["actor_resolution_status"] == "INCONCLUSIVE"
    monkeypatch.setattr(prefix, "EXISTENCE_BUDGET_MS", budget)
    selected = workflow.run_query("contratos del proveedor " + NAME + " diaz", chat_id="fuller")
    assert selected["route_reason"] == "actor_profile"
    assert selected["resolved_params"]["actor_resolution_method"] == "exact_unique_alias"
    assert selected["resolved_params"]["actor_scope"]["base_number"] == "70000003"


def test_non_actor_trace_remains_empty():
    assert actor_resolution_trace({}) == {}


def test_bare_number_does_not_replace_inconclusive_authority(tmp_path, monkeypatch):
    from app.data import actor_exact_prefix as prefix
    monkeypatch.setattr(prefix, "EXISTENCE_BUDGET_MS", 0)
    workflow, calls = _workflow(tmp_path, monkeypatch)
    workflow.run_query("contratos del proveedor " + NAME, chat_id="number")
    result = workflow.run_query("70000003", chat_id="number")
    assert result["resolved_params"]["actor_resolution_status"] == "INCONCLUSIVE"
    assert result["needs_clarification"]
    assert calls == []


def test_prefix_topic_conflict_has_no_rank_selection_until_actor_intent_is_explicit(tmp_path, monkeypatch):
    workflow, calls = _workflow(tmp_path, monkeypatch)
    resolver = resolver_for(tmp_path / "topics.sqlite", [
        ("CC", "76000001", "transporte"), ("CC", "76000002", "transporte escolar"),
    ])
    monkeypatch.setattr(workflow, "actor_name_resolver", resolver)
    first = workflow.run_query("contratos de transporte", chat_id="topic-prefix")
    params = first["resolved_params"]
    assert evaluate_actor_scope(params).reason == "actor_or_topic_ambiguous"
    assert calls == []
    assert "actor_pending" not in params
    assert actor_candidate_options(params) == ()
    assert actor_resolution_trace(params)["candidates_count"] == 0
    selected = workflow.run_query("2", chat_id="topic-prefix")
    assert selected.get("route_reason") != "actor_profile"
    assert calls == []
    strong = workflow.run_query("contratos del proveedor transporte", chat_id="topic-prefix")
    params = strong["resolved_params"]
    assert params["actor_resolution_method"] == "exact_prefix_ambiguous"
    assert not params.get("actor_intent_conflict")
    assert len(actor_candidate_options(params)) == 2
    assert calls == []


def test_existing_exact_ambiguity_topic_pending_is_not_changed(tmp_path):
    resolver = resolver_for(tmp_path / "topics.sqlite", [
        ("CC", "76000001", "transporte"), ("CC", "76000002", "transporte"),
    ])
    query = "contratos de transporte"
    params = resolve_actor_name(State({"raw_query": query, "parsed_params": dict(QueryRouter().parse(query).params)}), resolver)["parsed_params"]
    assert evaluate_actor_scope(params).reason == "actor_or_topic_ambiguous"
    assert params["actor_resolution_method"] == "exact_ambiguous_alias"
    assert len(actor_candidate_options(params)) == 2
