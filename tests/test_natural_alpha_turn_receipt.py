"""One opaque receipt per turn, consumed by the real SQLite sink, never by text."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
from dataclasses import replace
import logging
import sqlite3
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from app.core.feedback import FeedbackStore
from app.core.natural_alpha.coverage_classification import classify_turn
from app.core.natural_alpha.models import Observation
from app.core.natural_alpha.turn_receipt import finalize_turn, note_seam_reached, turn_scope
from app.core.workflow.context import HandlerContext
from tests.test_natural_alpha_contract import IDENTITY
from tests.test_natural_alpha_runtime import make_flow, run  # noqa: F401

SETTINGS = SimpleNamespace(secoppal_natural_alpha_mode="alpha", secoppal_auth_enabled=True,
                           secoppal_natural_alpha_email=IDENTITY["email"])
ALPHA = Observation("alpha", "SKIPPED", (), 1, 3.0, "EMPTY_PROPOSAL").metadata()


@pytest.fixture
def store(tmp_path):
    return FeedbackStore(tmp_path / "feedback.jsonl", jsonl_dual_write=False)


def _ctx(**overrides):
    return HandlerContext(**{"user_query": "original", "channel": "streamlit", "user_identity": IDENTITY,
                             **overrides})


def _log(store, query="original", route="direct_lookup_id", **extra):
    return store.log_trace(query, "streamlit", {"route_reason": route, **extra}, user_identity=IDENTITY)


def _coverage(store):
    with sqlite3.connect(store.db_path) as conn:
        return {trace_id: (classification, turn) for trace_id, classification, turn in conn.execute(
            "SELECT trace_id, json_extract(trace_json, '$.natural_alpha_coverage_class'), "
            "json_extract(trace_json, '$.natural_alpha_coverage_turn_id') FROM traces")}


def test_rewritten_text_consumes_the_turn_once_and_later_traces_are_secondary(store):
    with turn_scope(SETTINGS, store, _ctx()) as receipt:
        # Actor selection and Profile legitimately trace a different text.
        first = _log(store, query="opción 2", route="actor_selection_seed")
        second = _log(store, query="replayed original", route="entity_profile_overview")
        assert receipt.trace_attempted and receipt.sqlite_persisted
    outside = _log(store)
    coverage = _coverage(store)
    assert coverage[first] == ("FAST_PATH_BYPASS", receipt.turn_id)
    assert coverage[second] == (None, receipt.turn_id)
    assert coverage[outside] == (None, None)
    assert not receipt.active


@pytest.mark.parametrize("inner_mode", ["alpha", "off", "shadow"])
def test_nested_turn_masks_the_outer_receipt(store, inner_mode):
    inner_settings = SimpleNamespace(**{**vars(SETTINGS), "secoppal_natural_alpha_mode": inner_mode})
    with turn_scope(SETTINGS, store, _ctx()) as outer:
        with turn_scope(inner_settings, store, _ctx(actor_candidate_selection={})) as inner:
            nested = _log(store, route="actor_selection_stale")
        assert not outer.trace_attempted
        owner = _log(store, route="reset")
    coverage = _coverage(store)
    assert coverage[nested] == (("CONTROL_BYPASS", inner.turn_id) if inner_mode == "alpha" else (None, None))
    assert coverage[owner] == ("CONTROL_BYPASS", outer.turn_id)


def test_finally_revokes_copied_context_after_turn_failure(store):
    with pytest.raises(RuntimeError, match="turn failed"):
        with turn_scope(SETTINGS, store, _ctx()):
            copied = copy_context()
            raise RuntimeError("turn failed")
    stale = copied.run(_log, store)
    assert _coverage(store)[stale] == (None, None)


def test_copied_context_cannot_authorize_another_thread(store):
    with ThreadPoolExecutor(max_workers=1) as pool, turn_scope(SETTINGS, store, _ctx()) as receipt:
        foreign = pool.submit(copy_context().run, _log, store).result(timeout=10)
        owner = _log(store)
    assert _coverage(store)[foreign] == (None, None)
    assert _coverage(store)[owner] == ("FAST_PATH_BYPASS", receipt.turn_id)


def test_concurrent_thread_turns_keep_their_own_receipts(store):
    barrier = Barrier(2)

    def worker(route):
        with turn_scope(SETTINGS, store, _ctx()) as receipt:
            barrier.wait(timeout=10)
            return receipt.turn_id, _log(store, route=route)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(worker, ("reset", "radar_list")))
    coverage = _coverage(store)
    assert {coverage[trace_id] for _, trace_id in outcomes} == {
        ("CONTROL_BYPASS", outcomes[0][0]), ("FAST_PATH_BYPASS", outcomes[1][0]),
    }


def test_async_child_inheritance_is_not_authority_and_tasks_are_isolated(store):
    async def child():
        return _log(store)

    async def worker(route):
        with turn_scope(SETTINGS, store, _ctx()) as receipt:
            await asyncio.sleep(0)
            return receipt.turn_id, _log(store, route=route)

    async def scenario():
        with turn_scope(SETTINGS, store, _ctx()) as receipt:
            inherited = await asyncio.create_task(child())
            owner = _log(store)
        return receipt, inherited, owner, await asyncio.gather(worker("reset"), worker("radar_list"))

    receipt, inherited, owner, outcomes = asyncio.run(scenario())
    coverage = _coverage(store)
    assert coverage[inherited] == (None, None)
    assert coverage[owner] == ("FAST_PATH_BYPASS", receipt.turn_id)
    assert [coverage[trace_id][1] for _, trace_id in outcomes] == [turn for turn, _ in outcomes]


def test_another_store_and_result_keys_cannot_borrow_or_forge_a_receipt(store, tmp_path):
    other = FeedbackStore(tmp_path / "other" / "feedback.jsonl", jsonl_dual_write=False)
    forged = {"natural_alpha_coverage_class": "REACHED_ALPHA", "natural_alpha_coverage_turn_id": "forged"}
    outside = _log(store, **forged)
    with turn_scope(SETTINGS, store, _ctx()) as receipt:
        foreign = _log(other)
    assert _coverage(other)[foreign] == (None, None)
    assert _coverage(store)[outside] == (None, None)
    assert not receipt.trace_attempted


@pytest.mark.parametrize("seam,metadata,expected", [
    (False, ALPHA, "UNACCREDITED"),  # Alpha fields without the seam witness.
    (True, {}, "UNACCREDITED"),  # Seam reached but no valid Alpha evidence.
    (True, ALPHA, "REACHED_ALPHA"),
])
def test_reached_alpha_requires_both_seam_witness_and_valid_metadata(store, seam, metadata, expected):
    with turn_scope(SETTINGS, store, _ctx()) as receipt:
        if seam:
            note_seam_reached(store)
        trace_id = _log(store, route="heuristic_only", **metadata)
    assert _coverage(store)[trace_id] == (expected, receipt.turn_id)


@pytest.mark.parametrize("structured,result,expected", [
    (frozenset({"profile_action_selection"}), {"route_reason": "admission_profile_action_frame_required"},
     "PROFILE_BYPASS"),
    (frozenset(), {"route_reason": "entity_profile_overview", "analytical_intent": "entity_profile",
                   "profile_planned_jobs": 4, "profile_attempted_calls": 0}, "PROFILE_BYPASS"),
    (frozenset(), {"route_reason": "entity_profile_overview", "analytical_intent": "entity_profile"}, "OTHER_BYPASS"),
    (frozenset(), {"route_reason": "entity_profile_overview", "analytical_intent": "entity_profile",
                   "profile_planned_jobs": 4.0, "profile_attempted_calls": True}, "OTHER_BYPASS"),
    (frozenset({"actor_candidate_selection"}), {"route_reason": "actor_selection_stale"}, "CONTROL_BYPASS"),
    (frozenset(), {"route_reason": "admission_invalid_tabular_cursor"}, "CONTROL_BYPASS"),
    (frozenset(), {"route_reason": "reset"}, "CONTROL_BYPASS"),
    (frozenset(), {"route_reason": "actor_selection_seed"}, "FAST_PATH_BYPASS"),
    (frozenset(), {"route_reason": "pagination_more"}, "FAST_PATH_BYPASS"),
    (frozenset(), {"route_reason": "heuristic_only"}, "OTHER_BYPASS"),
    (frozenset(), {"route_reason": ""}, "UNACCREDITED"),
    (frozenset(), {"route_reason": None}, "UNACCREDITED"),
])
def test_host_precedence_uses_structured_and_result_witnesses_only(structured, result, expected):
    assert classify_turn(seam_reached=False, structured=structured, result=result) == expected


@pytest.mark.parametrize("sinks", ["jsonl_only", "sqlite_disabled_everywhere"])
def test_jsonl_append_is_not_a_persistence_ack(tmp_path, caplog, sinks):
    store = FeedbackStore(tmp_path / "feedback.jsonl", sqlite_enabled=False,
                          jsonl_dual_write=sinks == "jsonl_only")
    ctx = _ctx()
    with turn_scope(SETTINGS, store, ctx) as receipt:
        _log(store)
        finalize_turn(receipt, store, ctx, {"route_reason": "direct_lookup_id"})
    assert receipt.trace_attempted and not receipt.sqlite_persisted
    assert "natural_alpha_coverage_persistence_failed" in caplog.text
    if sinks == "jsonl_only":
        assert len(store.path.read_text().splitlines()) == 1  # No second, disguising trace.


def test_failed_sqlite_commit_is_not_acknowledged_or_retried(store, monkeypatch, caplog):
    connect = store._connect

    @contextmanager
    def rejected_commit():
        with connect() as conn:
            conn.execute("BEGIN")  # Production connections default to autocommit.
            yield conn
            raise sqlite3.OperationalError("commit rejected")

    monkeypatch.setattr(store, "_connect", Mock(side_effect=rejected_commit))
    ctx = _ctx()
    with turn_scope(SETTINGS, store, ctx) as receipt:
        _log(store)
        finalize_turn(receipt, store, ctx, {"route_reason": "direct_lookup_id"})
    store._connect.assert_called_once()
    assert receipt.trace_attempted and not receipt.sqlite_persisted
    assert "natural_alpha_coverage_persistence_failed" in caplog.text
    monkeypatch.setattr(store, "_connect", connect)
    assert not _coverage(store)


def test_finalizer_writes_one_trace_only_record_and_never_a_second(store, caplog):
    ctx = _ctx(user_query="/reset")
    result = {"route_reason": "reset", "response": "unchanged", "trace_id": ""}
    with turn_scope(SETTINGS, store, ctx) as receipt:
        finalize_turn(receipt, store, ctx, result)
        finalize_turn(receipt, store, ctx, result)
    assert result == {"route_reason": "reset", "response": "unchanged", "trace_id": ""}
    assert list(_coverage(store).values()) == [("CONTROL_BYPASS", receipt.turn_id)]
    assert "natural_alpha_coverage" not in caplog.text
    finalize_turn(receipt, store, ctx, result)  # A closed receipt is inert.
    assert len(_coverage(store)) == 1


def test_finalizer_does_not_disguise_a_trace_it_did_not_observe(store, caplog):
    ctx = _ctx()
    with turn_scope(SETTINGS, store, ctx) as receipt:
        finalize_turn(receipt, store, ctx, {"route_reason": "direct_lookup_id", "trace_id": "foreign"})
    assert not _coverage(store)
    assert "natural_alpha_coverage_unaccredited_trace" in caplog.text


@pytest.mark.parametrize("raw", ["/reset", "CO1.BDOS.123456"])
def test_runtime_sqlite_failure_keeps_legacy_result_and_writes_nothing_extra(make_flow, monkeypatch, caplog, raw):
    caplog.set_level(logging.ERROR)
    outputs = []
    for mode in ("off", "alpha"):
        flow = make_flow(mode)
        monkeypatch.setattr(flow.feedback, "_connect", Mock(side_effect=sqlite3.OperationalError("down")))
        with patch.object(flow.feedback, "log_trace", wraps=flow.feedback.log_trace) as log_trace:
            result = run(flow, raw, chat_id="failure")
        outputs.append(({key: result.get(key) for key in ("response", "route_reason", "results", "soql_query")},
                        log_trace.call_count, flow.secop_client.calls))
        flow.natural_alpha_provider.observe.assert_not_called()
        failed = [record for record in caplog.records if "natural_alpha_coverage_persistence_failed" in record.getMessage()]
        assert len(failed) == (1 if mode == "alpha" else 0)
        caplog.clear()
    legacy_traces = 1 if raw.startswith("CO1") else 0
    assert outputs[0][0] == outputs[1][0] and outputs[0][2] == outputs[1][2]
    assert (outputs[0][1], outputs[1][1]) == (legacy_traces, 1)


@pytest.mark.parametrize("kind", ["off", "shadow", "invalid_mode", "other_email", "auth_off", "other_channel",
                                  "plain_identity"])
def test_outside_dogfood_there_is_no_receipt_and_no_extra_trace(make_flow, kind):
    flow = make_flow("alpha")
    identity, channel = IDENTITY, "streamlit"
    if kind in {"off", "shadow", "invalid_mode"}:
        flow.settings.secoppal_natural_alpha_mode = kind
    elif kind == "other_email":
        flow.settings.secoppal_natural_alpha_email = "other@example.test"
    elif kind == "auth_off":
        flow.settings.secoppal_auth_enabled = False
    elif kind == "other_channel":
        channel = "whatsapp"
    else:
        identity = dict(IDENTITY)
    reset = flow.run_query("/reset", channel=channel, user_identity=identity, chat_id="outside")
    lookup = flow.run_query("CO1.BDOS.123456", channel=channel, user_identity=identity, chat_id="outside")
    traces = flow.feedback.get_recent()
    assert reset["route_reason"] == "reset"
    assert [trace["trace_id"] for trace in traces] == [lookup["trace_id"]]
    assert not any(key.startswith("natural_alpha_coverage") for key in traces[0])
    flow.natural_alpha_provider.observe.assert_not_called()


def test_replace_of_context_inside_the_turn_keeps_the_same_receipt(store):
    ctx = _ctx()
    with turn_scope(SETTINGS, store, ctx) as receipt:
        rewritten = replace(ctx, user_query="Acción de perfil", publication_required=True)
        trace_id = _log(store, query=rewritten.user_query, route="admission_profile_action_route_unavailable")
    assert _coverage(store)[trace_id] == ("CONTROL_BYPASS", receipt.turn_id)
