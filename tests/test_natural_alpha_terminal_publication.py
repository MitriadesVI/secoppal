"""Persist terminal NONE ownership without exposing a reusable public reference."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.config import get_settings
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.natural_alpha.models import GroundedJob, Observation
from app.core.natural_alpha.provider import ProviderResult
from app.core.natural_alpha.terminal_publication import NaturalAlphaPublicationError
from app.core.workflow.admission_rules import CONTINUATION_REF_KEY
from app.core.workflow.handlers.natural_alpha import multi_job_result
from tests.test_entity_profile_workflow import WorkflowClient
from tests.test_natural_alpha_contract import IDENTITY
from tests.test_natural_alpha_runtime import MULTI, MULTI_RAW, RAW, make_flow, run  # noqa: F401

PUBLIC_REFERENCES = {"result_id", "conversation_revision", "chat_id", "continuation", "_continuation_ref"}


def test_frame_multi_returns_no_public_reference(make_flow, monkeypatch):
    flow = make_flow(response=ProviderResult(MULTI, provider_calls=1))
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
    get_settings.cache_clear()
    result = run(flow, MULTI_RAW, chat_id="alpha-terminal")
    assert result["needs_clarification"]
    assert result["route_reason"] == "natural_alpha_multi_job_stepwise"
    assert not PUBLIC_REFERENCES & result.keys()
    read = flow.conv_store.load_envelope("alpha-terminal")
    assert read.usable
    assert read.envelope.continuation["owner"] == "NONE"


def _history(result):
    tree = ast.parse(Path("streamlit_app.py").read_text())
    names = {"_clean_resolved", "_continuation_ref", "_profile_action_ref", "_assistant_history_item"}
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    namespace = {"actor_candidate_options": lambda _: []}
    exec(compile(ast.Module(body=functions, type_ignores=[]), "streamlit_app.py", "exec"), namespace)
    return json.loads(json.dumps(namespace["_assistant_history_item"](result)))


def _assert_private_result(result):
    assert not PUBLIC_REFERENCES & result.keys()
    assert "_natural_alpha_terminal_publication" not in result
    item = _history(result)
    assert item["continuation_ref"] is None
    assert item["profile_action_ref"] is None
    assert "_natural_alpha_terminal_publication" not in json.dumps(item)


def _frame_on(monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "true")
    get_settings.cache_clear()


@pytest.mark.parametrize("previous_owner", ["TABULAR", "ENTITY_PROFILE"])
def test_terminal_replaces_prior_envelope_and_rejects_old_reference_without_secop(
    make_flow, monkeypatch, previous_owner,
):
    flow = make_flow("off")
    _frame_on(monkeypatch)
    query = RAW
    if previous_owner == "ENTITY_PROFILE":
        flow.secop_client = WorkflowClient()
        query = "analiza la alcaldía de Baranoa en 2026"
    first = run(flow, query, chat_id="replaced")
    before = flow.conv_store.load_envelope("replaced")
    assert before.usable
    assert before.envelope.continuation["owner"] == previous_owner
    if previous_owner == "ENTITY_PROFILE":
        assert before.envelope.execution_record
        assert before.envelope.profile_state != ProfileConversationState().to_dict()
    old_reference = {key: first[key] for key in ("result_id", "conversation_revision", "chat_id")}
    history_before = flow.conv_store.get_history("replaced")
    flow.secop_client.calls.clear()
    flow.settings.secoppal_natural_alpha_mode = "alpha"
    flow.natural_alpha_provider.observe.return_value = ProviderResult(MULTI, provider_calls=1)
    terminal = run(flow, MULTI_RAW, chat_id="replaced")
    _assert_private_result(terminal)
    assert not flow.secop_client.calls
    after = flow.conv_store.load_envelope("replaced")
    assert after.usable
    assert after.revision == before.revision + 1
    assert after.envelope.result_id != before.envelope.result_id
    continuation = after.envelope.continuation
    assert continuation["owner"] == "NONE"
    assert all(continuation.get(key) is None for key in (
        "confirmed_profile", "pending_profile", "tabular_groups", "export_ref", "delegated",
    ))
    assert after.envelope.execution_record is None
    assert after.envelope.profile_state == ProfileConversationState().to_dict()
    assert len(flow.conv_store.get_history("replaced")) == len(history_before)
    assert len(flow.feedback.get_recent()) == 2

    rejected = run(flow, "refinar", chat_id="replaced", seed_resolved_params={
        **first.get("resolved_params", {}), CONTINUATION_REF_KEY: old_reference,
    })
    assert rejected["needs_clarification"]
    assert rejected["route_reason"].startswith("admission_")
    assert not flow.secop_client.calls
    flow.natural_alpha_provider.observe.assert_called_once()


@pytest.mark.parametrize("frame,chat", [(False, "off"), (False, None), (True, None)])
def test_without_frame_reservation_there_is_no_publication_or_reference(make_flow, monkeypatch, frame, chat):
    from app.core.natural_alpha import terminal_publication

    flow = make_flow(response=ProviderResult(MULTI, provider_calls=1))
    if frame:
        _frame_on(monkeypatch)
    publisher = Mock(side_effect=AssertionError("No reservation may be published"))
    monkeypatch.setattr(terminal_publication, "publish", publisher)
    terminal = run(flow, MULTI_RAW, chat_id=chat)
    _assert_private_result(terminal)
    publisher.assert_not_called()
    assert len(flow.feedback.get_recent()) == 1
    assert not flow.secop_client.calls
    if chat:
        assert not flow.conv_store.load_envelope(chat).usable


def test_publication_failure_raises_specific_error_and_preserves_prior_envelope(make_flow, monkeypatch):
    flow = make_flow("off")
    _frame_on(monkeypatch)
    run(flow, chat_id="failure")
    before = flow.conv_store.load_envelope("failure")
    assert before.usable
    flow.settings.secoppal_natural_alpha_mode = "alpha"
    flow.natural_alpha_provider.observe.return_value = ProviderResult(MULTI, provider_calls=1)
    flow.secop_client.calls.clear()
    publisher = Mock(side_effect=OSError("fabricated disk failure"))
    monkeypatch.setattr(flow.conv_store, "publish_turn", publisher)
    with pytest.raises(NaturalAlphaPublicationError, match="NATURAL_ALPHA_TERMINAL_PUBLICATION_FAILED"):
        run(flow, MULTI_RAW, chat_id="failure")
    publisher.assert_called_once()
    flow.natural_alpha_provider.observe.assert_called_once()
    assert not flow.secop_client.calls
    after = flow.conv_store.load_envelope("failure")
    assert after.usable and after.envelope == before.envelope
    assert len(flow.feedback.get_recent()) == 2


def test_signal_requires_object_identity_and_exact_route_and_is_always_consumed(monkeypatch):
    from app.core.natural_alpha import terminal_publication as terminal

    class EqualSignal:
        def __eq__(self, other):
            raise AssertionError("signal equality must not be invoked")

    publisher = Mock(side_effect=AssertionError("foreign signal cannot publish"))
    monkeypatch.setattr(terminal, "publish", publisher)
    for marker in (True, "sealed", object(), EqualSignal()):
        result = {"route_reason": "natural_alpha_multi_job_stepwise", terminal._SIGNAL_KEY: marker}
        assert not terminal.publish_if_terminal(None, None, None, result)
        assert terminal._SIGNAL_KEY not in result
    wrong_route = {"route_reason": "legacy"}
    terminal.mark_terminal_publication(wrong_route)
    assert not terminal.publish_if_terminal(None, None, None, wrong_route)
    assert terminal._SIGNAL_KEY not in wrong_route
    publisher.assert_not_called()


@pytest.mark.parametrize("publication_required", [False, True])
def test_terminal_signal_is_added_only_after_single_trace(publication_required):
    from app.core.natural_alpha import terminal_publication as terminal

    def trace(query, channel, result, **kwargs):
        assert terminal._SIGNAL_KEY not in result
        return "trace"

    feedback = Mock(log_trace=Mock(side_effect=trace))
    ctx = SimpleNamespace(user_query="busca suma", channel="streamlit", user_identity=IDENTITY,
                          publication_required=publication_required)
    observation = Observation("alpha", "MULTI_JOB_BLOCKED", (
        GroundedJob("SEARCH", "CONTRACTS", 0, 5), GroundedJob("ANALYZE", "TOTAL_VALUE", 6, 10),
    ))
    result = multi_job_result(SimpleNamespace(feedback=feedback), ctx, observation)
    feedback.log_trace.assert_called_once()
    assert result["trace_id"] == "trace"
    assert (terminal._SIGNAL_KEY in result) is publication_required
    if publication_required:
        assert terminal.publish_if_terminal(None, ctx, None, result)
        assert not terminal.publish_if_terminal(None, ctx, None, result)
