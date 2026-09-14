"""Real feedback sinks: receipt isolation, one-shot consumption and acknowledgments."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import copy_context
from dataclasses import replace
import json
import sqlite3
from threading import Barrier
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.core.feedback import FeedbackStore
from app.core.natural_alpha.coverage import ensure_alpha_coverage_trace
from app.core.natural_alpha.coverage_context import coverage_scope
from app.core.workflow.context import HandlerContext
from tests.test_natural_alpha_contract import IDENTITY


@pytest.fixture
def receipt_env(tmp_path):
    store = FeedbackStore(tmp_path / "feedback.jsonl", jsonl_dual_write=False)
    settings = SimpleNamespace(secoppal_natural_alpha_mode="alpha", secoppal_auth_enabled=True,
                               secoppal_natural_alpha_email=IDENTITY["email"])
    return (SimpleNamespace(feedback=store, settings=settings),
            HandlerContext(user_query="same", channel="streamlit", user_identity=IDENTITY))


def _log(deps, ctx, **overrides):
    kwargs = dict(user_query=ctx.user_query, channel=ctx.channel, user_identity=ctx.user_identity,
                  workflow_result={"route_reason": "direct_lookup_id"})
    kwargs.update(overrides)
    return deps.feedback.log_trace(**kwargs)


def _traces(deps):
    return {row["trace_id"]: row for row in deps.feedback.get_recent(limit=100)}


def _classification(trace):
    return trace.get("natural_alpha_coverage_class")


def test_existing_trace_consumes_receipt_once_and_post_hook_cannot_write_again(receipt_env):
    deps, ctx = receipt_env
    result = {"route_reason": "direct_lookup_id", "response": "same"}
    with coverage_scope(deps, ctx) as receipt:
        first = _log(deps, ctx, workflow_result=result)
        assert result == {"route_reason": "direct_lookup_id", "response": "same"}
        assert receipt.persisted_trace_id == first
        # Even a handler that accidentally omits its ID cannot trigger a retry.
        assert ensure_alpha_coverage_trace(deps, ctx, result) is result
        assert len(_traces(deps)) == 1
        second = _log(deps, ctx)
        assert receipt.persisted_trace_id == first
    assert _classification(_traces(deps)[first]) == "FAST_PATH_BYPASS"
    assert _classification(_traces(deps)[second]) is None
    assert not receipt.active


@pytest.mark.parametrize("inner_mode", ["alpha", "off", "shadow"])
def test_reentrant_scopes_restore_outer_without_borrowing_its_receipt(receipt_env, inner_mode):
    deps, ctx = receipt_env
    inner = SimpleNamespace(feedback=deps.feedback, settings=SimpleNamespace(
        **{**vars(deps.settings), "secoppal_natural_alpha_mode": inner_mode},
    ))
    with coverage_scope(deps, ctx):
        with coverage_scope(inner, ctx, excluded_route_class="CONTROL_BYPASS"):
            nested = _log(inner, ctx)
        outer = _log(deps, ctx)
    outside = _log(deps, ctx)
    traces = _traces(deps)
    assert _classification(traces[nested]) == ("CONTROL_BYPASS" if inner_mode == "alpha" else None)
    assert _classification(traces[outer]) == "FAST_PATH_BYPASS"
    assert _classification(traces[outside]) is None


def test_finally_revokes_unconsumed_copied_context_after_handler_failure(receipt_env):
    deps, ctx = receipt_env
    with pytest.raises(RuntimeError, match="handler failed"):
        with coverage_scope(deps, ctx):
            copied = copy_context()
            raise RuntimeError("handler failed")
    stale = copied.run(_log, deps, ctx)
    outside = _log(deps, ctx)
    assert all(_classification(_traces(deps)[trace_id]) is None for trace_id in (stale, outside))


def test_copied_context_cannot_authorize_another_thread(receipt_env):
    deps, ctx = receipt_env
    with ThreadPoolExecutor(max_workers=1) as pool, coverage_scope(deps, ctx):
        copied = copy_context()
        foreign = pool.submit(copied.run, _log, deps, ctx).result(timeout=10)
        owner = _log(deps, ctx)
    assert _classification(_traces(deps)[foreign]) is None
    assert _classification(_traces(deps)[owner]) == "FAST_PATH_BYPASS"


def test_concurrent_thread_scopes_keep_their_own_classification(receipt_env):
    deps, ctx = receipt_env
    barrier = Barrier(2)

    def worker(classification):
        local = replace(ctx, user_query=classification)
        with coverage_scope(deps, local, excluded_route_class=classification):
            barrier.wait(timeout=10)
            return classification, _log(deps, local)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(worker, ("FAST_PATH_BYPASS", "CONTROL_BYPASS")))
    traces = _traces(deps)
    assert len(traces) == 2
    assert all(_classification(traces[trace_id]) == expected for expected, trace_id in outcomes)


def test_async_child_inheritance_is_not_authority_and_concurrent_tasks_are_isolated(receipt_env):
    deps, ctx = receipt_env

    async def log_in_child():
        return _log(deps, ctx)

    async def worker(classification):
        local = replace(ctx, user_query=classification)
        with coverage_scope(deps, local, excluded_route_class=classification):
            await asyncio.sleep(0)
            return classification, _log(deps, local)

    async def scenario():
        with coverage_scope(deps, ctx):
            inherited = await asyncio.create_task(log_in_child())
            owner = _log(deps, ctx)
        return inherited, owner, await asyncio.gather(worker("FAST_PATH_BYPASS"), worker("CONTROL_BYPASS"))

    inherited, owner, outcomes = asyncio.run(scenario())
    traces = _traces(deps)
    assert _classification(traces[inherited]) is None
    assert _classification(traces[owner]) == "FAST_PATH_BYPASS"
    assert all(_classification(traces[trace_id]) == expected for expected, trace_id in outcomes)


@pytest.mark.parametrize("foreign", ["query", "channel", "identity", "store"])
def test_receipt_is_bound_to_exact_log_inputs_and_store(receipt_env, tmp_path, foreign):
    deps, ctx = receipt_env
    other_deps, overrides = deps, {}
    if foreign == "store":
        other_deps = SimpleNamespace(feedback=FeedbackStore(tmp_path / "other" / "feedback.jsonl"))
    elif foreign == "identity":
        overrides["user_identity"] = dict(IDENTITY)
    else:
        overrides["user_query" if foreign == "query" else "channel"] = "foreign"
    with coverage_scope(deps, ctx):
        unrelated = _log(other_deps, ctx, **overrides)
        owner = _log(deps, ctx)
    assert _classification(_traces(other_deps)[unrelated]) is None
    assert _classification(_traces(deps)[owner]) == "FAST_PATH_BYPASS"


def test_result_keys_without_context_cannot_forge_a_coverage_receipt(receipt_env):
    deps, ctx = receipt_env
    trace_id = _log(deps, ctx, workflow_result={
        "route_reason": "direct_lookup_id", "natural_alpha_coverage_class": "FAST_PATH_BYPASS",
        "natural_alpha_excluded_by_route": "direct_lookup_id",
    })
    assert _classification(_traces(deps)[trace_id]) is None
    assert deps.feedback.log_coverage_trace(ctx.user_query, ctx.channel, {}, user_identity=IDENTITY) is None
    assert len(_traces(deps)) == 1


@pytest.mark.parametrize("sinks", ["sqlite", "jsonl", "dual", "jsonl_with_sqlite_failure"])
def test_coverage_acknowledges_only_an_existing_successful_sink(receipt_env, monkeypatch, sinks):
    deps, ctx = receipt_env
    store = deps.feedback
    store.jsonl_dual_write = sinks != "sqlite"
    store.sqlite_enabled = sinks != "jsonl"
    if sinks == "jsonl_with_sqlite_failure":
        monkeypatch.setattr(store, "_connect", Mock(side_effect=sqlite3.OperationalError("PRIVATE")))
    result = ensure_alpha_coverage_trace(deps, ctx, {"route_reason": "direct_lookup_id"})
    assert result["trace_id"]
    if store.jsonl_dual_write:
        lines = store.path.read_text().splitlines()
        assert len(lines) == 1 and json.loads(lines[0])["trace_id"] == result["trace_id"]
    else:
        assert result["trace_id"] in _traces(deps)


def test_failed_sqlite_commit_is_not_a_persistence_ack_and_is_not_retried(receipt_env, monkeypatch, caplog):
    deps, ctx = receipt_env
    connect = deps.feedback._connect

    @contextmanager
    def rejected_commit():
        with connect() as conn:
            conn.execute("BEGIN")  # Production connections default to autocommit.
            yield conn
            raise sqlite3.OperationalError("PRIVATE COMMIT ERROR")

    monkeypatch.setattr(deps.feedback, "_connect", Mock(side_effect=rejected_commit))
    result = {"route_reason": "direct_lookup_id", "response": "unchanged"}
    with coverage_scope(deps, ctx) as receipt:
        assert ensure_alpha_coverage_trace(deps, ctx, result) is result
        assert receipt.consumed and receipt.persisted_trace_id is None
        assert ensure_alpha_coverage_trace(deps, ctx, result) is result
    deps.feedback._connect.assert_called_once()
    monkeypatch.setattr(deps.feedback, "_connect", connect)
    assert not _traces(deps)
    assert "PRIVATE COMMIT ERROR" not in caplog.text


def test_legacy_trace_return_contract_is_unchanged_without_coverage(receipt_env):
    deps, ctx = receipt_env
    deps.feedback.sqlite_enabled = deps.feedback.jsonl_dual_write = False
    assert type(_log(deps, ctx)) is str
    assert not _traces(deps)
