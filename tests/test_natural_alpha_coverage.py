"""Coverage is trace-only: real winning handlers keep their original effects."""
from copy import deepcopy
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from app.core.radar.store import RadarStore
from app.core.natural_alpha.coverage import ensure_alpha_coverage_trace
from app.core.natural_alpha.identity import issue_streamlit_identity
from app.core.natural_alpha.dogfood import report_dogfood
from app.core.workflow.context import HandlerContext
from tests.test_natural_alpha_contract import IDENTITY
from tests.test_natural_alpha_runtime import RAW, make_flow, run  # noqa: F401


COVERAGE_KEYS = {"natural_alpha_coverage_class", "natural_alpha_excluded_by_route"}


@pytest.mark.parametrize("raw,kwargs,classification", [
    ("/reset", {}, "CONTROL_BYPASS"),
    ("mis radares", {}, "FAST_PATH_BYPASS"),
    ("muestrame mas", {}, "FAST_PATH_BYPASS"),
    ("1", {}, "FAST_PATH_BYPASS"),
    ("opción 1", {"actor_candidate_selection": {"operation": "SELECT_ACTOR_CANDIDATE",
                                               "candidate_rank": 1, "pending_token": "stale"}}, "CONTROL_BYPASS"),
])
def test_missing_fast_paths_add_one_trace_without_changing_response_or_turns(make_flow, raw, kwargs, classification):
    baseline, candidate = make_flow("off"), make_flow("alpha")
    responses = []
    for flow in (baseline, candidate):
        flow.radar_store = RadarStore(flow.feedback.db_path.parent / "radar.db")
        if raw == "/reset":
            flow.conv_store.append_turn(chat_id="coverage", user_query="old", response="old",
                                        parsed_params={}, result_ids=[])
        with patch.object(flow.conv_store, "append_turn", wraps=flow.conv_store.append_turn) as append:
            responses.append(run(flow, raw, chat_id="coverage", **kwargs))
        append.assert_not_called()
        flow.natural_alpha_provider.observe.assert_not_called()
        assert not flow.secop_client.calls
        assert not flow.conv_store.get_history("coverage")
    assert not baseline.feedback.get_recent()
    traces = candidate.feedback.get_recent()
    assert len(traces) == 1
    assert traces[0]["trace_id"] == responses[1]["trace_id"]
    assert traces[0]["natural_alpha_coverage_class"] == classification
    assert traces[0]["natural_alpha_excluded_by_route"] == responses[1]["route_reason"]
    assert "natural_alpha_status" not in traces[0]
    # End-to-end wall time is measured after dispatch, not a handler response.
    assert {key: value for key, value in responses[1].items() if key not in COVERAGE_KEYS | {"trace_id", "turn_elapsed_ms"}} == {
        key: value for key, value in responses[0].items() if key not in {"trace_id", "turn_elapsed_ms"}
    }


def test_trace_only_helper_preserves_every_original_field_and_is_idempotent(make_flow):
    flow = make_flow("alpha")
    original = {"route_reason": "reset", "trace_id": "", "response": "unchanged", "nested": {"value": [1]}}
    before = deepcopy(original)
    ctx = HandlerContext(user_query="/reset", channel="streamlit", user_identity=IDENTITY)
    result = ensure_alpha_coverage_trace(flow.deps, ctx, original, excluded_route_class="CONTROL_BYPASS")
    assert original == before
    assert {key: value for key, value in result.items() if key not in COVERAGE_KEYS | {"trace_id"}} == {
        key: value for key, value in original.items() if key != "trace_id"
    }
    assert ensure_alpha_coverage_trace(flow.deps, ctx, result) is result
    assert len(flow.feedback.get_recent()) == 1


@pytest.mark.parametrize("kind", ["off", "shadow", "invalid_mode", "other_email", "anonymous", "auth_off", "other_channel", "plain_mapping"])
def test_ineligible_or_outside_dogfood_is_exact_noop(make_flow, kind):
    flow = make_flow("alpha")
    identity, channel = IDENTITY, "streamlit"
    if kind in {"off", "shadow", "invalid_mode"}:
        flow.settings.secoppal_natural_alpha_mode = kind
    elif kind == "other_email":
        identity = issue_streamlit_identity(dict(IDENTITY, email="other@example.test"),
                                           {"email": "other@example.test"}, auth_enabled=True)
    elif kind == "anonymous":
        identity = dict(IDENTITY, is_anonymous=True)
    elif kind == "auth_off":
        flow.settings.secoppal_auth_enabled = False
    elif kind == "other_channel":
        channel = "whatsapp"
    else:
        identity = dict(IDENTITY)
    result = {"route_reason": "reset", "trace_id": "", "response": "untouched"}
    ctx = HandlerContext(user_query="/reset", channel=channel, user_identity=identity)
    assert ensure_alpha_coverage_trace(flow.deps, ctx, result) is result
    assert not flow.feedback.get_recent()
    actual = flow.run_query("/reset", channel=channel, user_identity=identity, chat_id="ineligible")
    assert actual["route_reason"] == "reset"
    assert not COVERAGE_KEYS & actual.keys()
    assert not flow.feedback.get_recent()
    flow.natural_alpha_provider.observe.assert_not_called()


def test_feedback_failure_does_not_change_a_completed_handler(make_flow, caplog):
    flow = make_flow("alpha")
    deps = flow.deps
    deps.feedback = Mock(log_coverage_trace=Mock(side_effect=OSError("PRIVATE EXCEPTION TEXT")))
    result = {"route_reason": "reset", "response": "unchanged"}
    ctx = HandlerContext(user_query="private", channel="streamlit", user_identity=IDENTITY)
    assert ensure_alpha_coverage_trace(deps, ctx, result) is result
    assert "PRIVATE EXCEPTION TEXT" not in caplog.text
    assert "coverage_trace_failed" in caplog.text


@pytest.mark.parametrize("handler", ["direct_lookup", "pagination", "refinement", "suggestion"])
def test_existing_handler_trace_and_append_counts_are_not_duplicated(make_flow, handler):
    outputs, effects = [], []
    for candidate in (False, True):
        flow = make_flow("off")
        if handler == "pagination":
            run(flow, RAW, chat_id="existing")
        if handler == "suggestion":
            flow.conv_store.append_turn(chat_id="existing", user_query="seed", response="seed",
                                        parsed_params={}, result_ids=[], suggestions=[
                                            {"label": "Mantenimiento", "modified_params": {
                                                "dataset": "procesos", "objeto": ["mantenimiento"],
                                            }},
                                        ])
        before = len(flow.feedback.get_recent())
        before_jsonl = flow.feedback.path.read_text() if flow.feedback.path.exists() else ""
        flow.settings.secoppal_natural_alpha_mode = "alpha" if candidate else "off"
        raw = {"direct_lookup": "CO1.BDOS.123456", "pagination": "muestrame mas",
               "refinement": "refinar", "suggestion": "1"}[handler]
        kwargs = {"seed_resolved_params": {"dataset": "procesos", "objeto": ["mantenimiento"]}} if handler == "refinement" else {}
        with patch.object(flow.conv_store, "append_turn", wraps=flow.conv_store.append_turn) as append:
            result = run(flow, raw, chat_id="existing", **kwargs)
        assert len(flow.feedback.get_recent()) - before == 1
        if flow.feedback.jsonl_dual_write:
            jsonl = flow.feedback.path.read_text()
            assert jsonl.startswith(before_jsonl)
            assert len(jsonl.splitlines()) == len(before_jsonl.splitlines()) + 1
        assert not COVERAGE_KEYS & result.keys()
        trace = next(item for item in flow.feedback.get_recent() if item["trace_id"] == result["trace_id"])
        assert bool(COVERAGE_KEYS & trace.keys()) is candidate
        if candidate:
            assert trace["natural_alpha_coverage_class"] == "FAST_PATH_BYPASS"
            assert trace["natural_alpha_excluded_by_route"] == result["route_reason"]
        report = report_dogfood(flow.feedback.db_path, email=IDENTITY["email"], since="2000-01-01T00:00:00+00:00")
        assert report["fast_path_bypass"] == int(candidate)
        flow.natural_alpha_provider.observe.assert_not_called()
        outputs.append({key: result.get(key) for key in (
            "response", "route_reason", "parsed_params", "resolved_params", "soql_query", "results", "total_count",
        )})
        effects.append((append.call_count, flow.secop_client.calls))
    assert outputs[0] == outputs[1]
    assert effects[0] == effects[1]
    assert effects[1][0] == (0 if handler == "direct_lookup" else 1)


def test_radar_save_list_run_keep_store_effects_and_add_only_coverage_traces(make_flow):
    outputs, effects = [], []
    for mode in ("off", "alpha"):
        flow = make_flow(mode)
        flow.radar_store = RadarStore(flow.feedback.db_path.parent / "radar.db")
        flow.conv_store.append_turn(chat_id="radar", user_query=RAW, response="seed", result_ids=[],
                                    parsed_params={"dataset": "procesos", "objeto": ["mantenimiento"]})
        responses = []
        for index, raw in enumerate(("guardar esta búsqueda como Radar Mantenimiento", "mis radares",
                                     "ejecutar radar Mantenimiento"), 1):
            with patch.object(flow.conv_store, "append_turn", wraps=flow.conv_store.append_turn) as append:
                result = run(flow, raw, chat_id="radar")
            append.assert_not_called()
            assert len(flow.conv_store.get_history("radar")) == 1
            assert len(flow.feedback.get_recent()) == (index if mode == "alpha" else 0)
            if mode == "alpha":
                assert result["natural_alpha_coverage_class"] == "FAST_PATH_BYPASS"
            responses.append((result["route_reason"], result["response"]))
        assert [route for route, _ in responses] == ["radar_saved", "radar_list", "radar_run_ok"]
        radars = flow.radar_store.list_radars("radar")
        assert len(radars) == 1
        history = flow.radar_store.get_runs(radars[0].id)
        # SAVE already performs the baseline run; explicit RUN is the second.
        assert len(history) == 2 and all(item.status == "ok" for item in history)
        seen = flow.radar_store.get_seen(radars[0].id)
        assert seen
        effects.append((radars[0].frame_dict(), set(seen), [item.status for item in history], flow.secop_client.calls))
        outputs.append(responses)
        flow.natural_alpha_provider.observe.assert_not_called()
    assert outputs[0] == outputs[1]
    assert effects[0] == effects[1]


def test_successful_actor_selection_delegates_one_trace_without_coverage_duplicate(make_flow, monkeypatch):
    from app.core.workflow.handlers import actor_selection, normal_flow
    from app.core.workflow.persistence import persist_trace_and_turn

    # Fabricated accredited identity isolates trace ownership, not actor semantics.
    monkeypatch.setattr(actor_selection, "actor_profile_enabled", lambda: True)
    monkeypatch.setattr(actor_selection, "candidate_from_payload", lambda candidate: (SimpleNamespace(), "fixture"))
    monkeypatch.setattr(actor_selection, "apply_actor_identity", lambda params, *args, **kwargs: dict(params))
    delegated = []

    def traced_normal(deps, ctx):
        delegated.append(ctx.user_query)
        return persist_trace_and_turn(deps, ctx, {
            "response": "selected", "route_reason": "actor_selection_seed", "parsed_params": {},
        })

    monkeypatch.setattr(normal_flow, "handle_normal_flow", traced_normal)
    for mode in ("off", "alpha"):
        flow = make_flow(mode)
        flow.conv_store.append_turn(chat_id="actor", user_query="original", response="choose", result_ids=[],
                                    parsed_params={"actor_pending": {
                                        "version": actor_selection.ACTOR_PENDING_VERSION, "selection_token": "token",
                                        "original_query": "original", "original_params": {"dataset": "procesos"},
                                        "candidates": [{"rank": 1}],
                                    }})
        with patch.object(flow.conv_store, "append_turn", wraps=flow.conv_store.append_turn) as append:
            result = run(flow, "opción 1", chat_id="actor", actor_candidate_selection={
                "operation": "SELECT_ACTOR_CANDIDATE", "candidate_rank": 1, "pending_token": "token",
            })
        assert len(flow.feedback.get_recent()) == 1
        assert flow.feedback.get_recent()[0]["user_query"] == "opción 1"
        assert result["route_reason"] == "actor_selection_seed"
        assert not COVERAGE_KEYS & result.keys()
        append.assert_called_once()
        flow.natural_alpha_provider.observe.assert_not_called()
    assert delegated == ["original", "original"]


@pytest.mark.parametrize("failure", ["disabled", "sqlite_error"])
def test_total_sink_failure_cannot_publish_a_coverage_trace_id(make_flow, monkeypatch, failure):
    flow = make_flow("alpha")
    flow.feedback.jsonl_dual_write = False
    if failure == "disabled":
        flow.feedback.sqlite_enabled = False
    else:
        monkeypatch.setattr(flow.feedback, "_connect", Mock(side_effect=sqlite3.OperationalError("sink failed")))
    result = {"route_reason": "reset", "response": "unchanged"}
    ctx = HandlerContext(user_query="/reset", channel="streamlit", user_identity=IDENTITY)
    assert ensure_alpha_coverage_trace(flow.deps, ctx, result, excluded_route_class="CONTROL_BYPASS") is result


@pytest.mark.parametrize("kind", ["off", "shadow", "plain_identity", "wrong_email", "auth_off"])
def test_existing_trace_has_no_receipt_outside_eligible_alpha(make_flow, kind):
    flow = make_flow(kind if kind in {"off", "shadow"} else "alpha")
    identity = IDENTITY
    if kind == "plain_identity":
        identity = dict(IDENTITY)
    elif kind == "wrong_email":
        flow.settings.secoppal_natural_alpha_email = "other@example.test"
    elif kind == "auth_off":
        flow.settings.secoppal_auth_enabled = False
    result = flow.run_query("CO1.BDOS.123456", channel="streamlit", user_identity=identity)
    traces = flow.feedback.get_recent()
    assert len(traces) == 1 and traces[0]["trace_id"] == result["trace_id"]
    assert not COVERAGE_KEYS & traces[0].keys()
    assert not COVERAGE_KEYS & result.keys()
    flow.natural_alpha_provider.observe.assert_not_called()


@pytest.mark.parametrize("route", [None, "", True, 7, {}, []])
def test_invalid_route_does_not_create_an_unreceipted_trace(make_flow, route):
    flow = make_flow("alpha")
    result = {"route_reason": route, "response": "unchanged"}
    ctx = HandlerContext(user_query="same", channel="streamlit", user_identity=IDENTITY)
    assert ensure_alpha_coverage_trace(flow.deps, ctx, result) is result
    assert not flow.feedback.get_recent()
