"""Logical source receipts are per turn and survive retries and worker threads."""

from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar

import pytest

from app.core.secop_client import SecopClient
from app.core.source_call_receipts import (
    begin_source_calls,
    end_source_calls,
    last_call_id,
    logical_source_call,
    source_call_scope,
    source_calls_snapshot,
    submit_with_source_context,
)


def test_scope_and_nested_turns_restore_the_outer_collector():
    outer = begin_source_calls("outer")
    try:
        with source_call_scope(reason="search:query", scope_id="processes"):
            with logical_source_call("query", "p6dx-8zbt", "SELECT *") as receipt:
                receipt.attempts = 1
                receipt.rows_received = 2
            parent = last_call_id()
            inner = begin_source_calls("inner")
            try:
                with logical_source_call("count", "jbjy-vk9h", "SELECT count(*)"):
                    pass
                assert source_calls_snapshot()[0]["turn_hash"] == "inner"
            finally:
                end_source_calls(inner)
            assert last_call_id() == parent
            with source_call_scope(reason="search:count", parent_call_id=parent):
                with logical_source_call("count", "p6dx-8zbt", "SELECT count(*)") as count:
                    count.attempts = 1
                    count.rows_received = 1
                    count.count_value = 82
        calls = source_calls_snapshot()
        assert [call["operation"] for call in calls] == ["query", "count"]
        assert calls[1]["parent_call_id"] == calls[0]["call_id"]
        assert calls[1]["count_value"] == 82
        assert calls[0]["reason"] == "search:query"
    finally:
        assert len(end_source_calls(outer)) == 2
    assert source_calls_snapshot() is None


def test_failed_call_keeps_one_error_receipt():
    token = begin_source_calls("turn")
    try:
        with pytest.raises(TimeoutError):
            with logical_source_call("query", "p6dx-8zbt", "SELECT *") as receipt:
                receipt.attempts = 2
                raise TimeoutError("timed out")
    finally:
        calls = end_source_calls(token)
    assert len(calls) == 1
    assert (calls[0]["status"], calls[0]["attempts"], calls[0]["error_type"]) == (
        "error", 2, "TimeoutError",
    )


def test_worker_contexts_share_one_locked_collector_without_scope_loss():
    unrelated = ContextVar("unrelated_metric", default="worker-default")
    unrelated_token = unrelated.set("main-only")
    token = begin_source_calls("turn")

    def worker(number: int) -> None:
        assert unrelated.get() == "worker-default"
        with source_call_scope(reason=f"observer:{number}", scope_id="p6dx-8zbt"):
            with logical_source_call("aggregate", "p6dx-8zbt", f"SELECT {number}") as receipt:
                receipt.attempts = 1
                receipt.rows_received = 1

    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [submit_with_source_context(executor, worker, number) for number in range(5)]
            for future in futures:
                future.result()
    finally:
        calls = end_source_calls(token)
        unrelated.reset(unrelated_token)
    assert len(calls) == 5
    assert {call["reason"] for call in calls} == {f"observer:{number}" for number in range(5)}
    assert len({call["call_id"] for call in calls}) == 5


def test_secop_client_retry_is_one_logical_call(monkeypatch):
    class FlakyTransport:
        name = "fake"

        def __init__(self):
            self.attempts = 0

        def query(self, dataset_id, soql, **kwargs):
            self.attempts += 1
            if self.attempts == 1:
                raise TimeoutError("first attempt")
            return [{"id_del_proceso": "CONTROL-1"}]

    transport = FlakyTransport()
    client = SecopClient("frozen.invalid", None, max_retries=2, transport=transport)
    monkeypatch.setattr("app.core.secop_client.time.sleep", lambda _: None)
    token = begin_source_calls("turn")
    try:
        rows = client.query("p6dx-8zbt", "SELECT * WHERE id_del_proceso = 'CONTROL-1'")
    finally:
        calls = end_source_calls(token)
    assert rows[0]["id_del_proceso"] == "CONTROL-1"
    assert transport.attempts == 2
    assert len(calls) == 1
    assert calls[0]["status"] == "success"
    assert calls[0]["attempts"] == 2
    assert calls[0]["rows_received"] == 1
