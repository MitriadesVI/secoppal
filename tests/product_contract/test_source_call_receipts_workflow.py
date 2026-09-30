"""E1 receipts observe every effective source call without changing SEARCH."""

import json
import sqlite3
from pathlib import Path

from tests.product_contract.assertions import row_ids
from tests.product_contract.frozen_source import InjectedFailure, load_rows_jsonl
from tests.product_contract.harness import ProductHarness


ROWS = Path("tests/fixtures/product_contract_v1/source_rows")


def _trace(root: Path) -> dict:
    return json.loads((root / "feedback.jsonl").read_text(encoding="utf-8").splitlines()[-1])


def test_receipts_off_on_preserve_calls_rows_and_outcome(network_ledger, tmp_path, monkeypatch):
    rows = load_rows_jsonl(ROWS / "harness_canary.jsonl")
    text = "contratos de mantenimiento vial en 2026"
    monkeypatch.setenv("SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED", "false")
    off_root = tmp_path / "off"
    off = ProductHarness(off_root, monkeypatch, rows_by_dataset=rows).say(text, chat_id="off")
    monkeypatch.setenv("SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED", "true")
    on_root = tmp_path / "on"
    on = ProductHarness(on_root, monkeypatch, rows_by_dataset=rows).say(text, chat_id="on")

    assert off.params == on.params
    assert off.source_calls == on.source_calls
    assert row_ids(off.result) == row_ids(on.result)
    assert off.result["outcome"] == on.result["outcome"]
    assert off.result["response"] == on.result["response"]
    assert "source_call_receipts" not in off.result
    assert "source_call_receipts" not in _trace(off_root)
    receipts = on.result["source_call_receipts"]
    assert [(r["operation"], r["dataset"], r["soql"]) for r in receipts] == [
        (call["operation"], call["dataset"], call["soql"]) for call in on.source_calls
    ]
    assert receipts == _trace(on_root)["source_call_receipts"]
    with sqlite3.connect(on_root / "secoppal.db") as connection:
        stored = connection.execute("SELECT trace_json FROM traces").fetchone()
    assert stored is not None
    assert json.loads(stored[0])["source_call_receipts"] == receipts
    assert all(r["status"] == "success" for r in receipts)
    assert network_ledger.attempts == []


def test_zero_result_relaxation_is_observed_as_child_call(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED", "true")
    rows = load_rows_jsonl(ROWS / "f3_processes.jsonl")
    turn = ProductHarness(tmp_path / "relax", monkeypatch, rows_by_dataset=rows).say(
        "procesos de mantenimiento vial en Antioquia", chat_id="relax",
    )
    receipts = turn.result["source_call_receipts"]
    assert len(receipts) == len(turn.source_calls) == 2
    assert receipts[0]["reason"] == "search:query"
    assert receipts[1]["reason"] == "zero_result_relaxation:query"
    assert receipts[1]["parent_call_id"] == receipts[0]["call_id"]
    assert receipts[0]["rows_received"] == 0
    assert receipts[1]["rows_received"] > 0


def test_timeout_fallback_and_error_get_distinct_receipts(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED", "true")
    rows = load_rows_jsonl(ROWS / "harness_canary.jsonl")
    turn = ProductHarness(
        tmp_path / "timeouts", monkeypatch, rows_by_dataset=rows,
        failures=[InjectedFailure(lambda: TimeoutError("frozen timeout"), operation="query")],
    ).say("contratos de mantenimiento vial en 2026", chat_id="timeouts")
    receipts = turn.result["source_call_receipts"]
    assert len(receipts) == len(turn.source_calls)
    assert receipts[0]["status"] == "error"
    assert receipts[0]["error_type"] == "TimeoutError"
    assert turn.result.get("fallback_attempted")
    assert receipts[1]["reason"] == "h4_fallback:query"
    assert receipts[1]["parent_call_id"] == receipts[0]["call_id"]


def test_count_and_threaded_aggregates_have_distinct_receipts(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED", "true")
    template = load_rows_jsonl(ROWS / "f3_processes.jsonl")["p6dx-8zbt"][0]
    rows = [
        {**template, "id_del_proceso": f"IF3-PROC-{number:03d}",
         "referencia_del_proceso": f"IF3-{number:03d}"}
        for number in range(60)
    ]
    turn = ProductHarness(
        tmp_path / "aggregates", monkeypatch, rows_by_dataset={"p6dx-8zbt": rows},
    ).say("procesos de mantenimiento vial en 2025", chat_id="aggregates")
    receipts = turn.result["source_call_receipts"]
    assert turn.result["total_count"] == 60
    assert [receipt["operation"] for receipt in receipts[:2]] == ["query", "count"]
    assert receipts[1]["parent_call_id"] == receipts[0]["call_id"]
    assert receipts[1]["count_value"] == 60
    aggregates = [receipt for receipt in receipts if receipt["operation"] == "aggregate"]
    assert len(aggregates) == 5
    assert {receipt["reason"] for receipt in aggregates} == {
        "observer:top_entities", "observer:value_stats", "observer:top_modalities",
        "observer:date_range", "observer:temporal_dist",
    }
    assert all(receipt["parent_call_id"] == receipts[1]["call_id"] for receipt in aggregates)
    assert sorted((r["operation"], r["soql"]) for r in receipts) == sorted(
        (call["operation"], call["soql"]) for call in turn.source_calls
    )


def test_dual_child_calls_keep_dataset_scope(tmp_path, monkeypatch):
    monkeypatch.setenv("SECOPPAL_FOUNDATION_SOURCE_RECEIPTS_ENABLED", "true")
    rows = load_rows_jsonl(ROWS / "harness_canary.jsonl")
    rows.update(load_rows_jsonl(ROWS / "f3_processes.jsonl"))
    turn = ProductHarness(tmp_path / "dual", monkeypatch, rows_by_dataset=rows).say(
        "procesos y contratos de mantenimiento vial en 2026", chat_id="dual",
    )
    receipts = turn.result["source_call_receipts"]
    assert {receipt["dataset"] for receipt in receipts} == {"p6dx-8zbt", "jbjy-vk9h"}
    assert len(receipts) == len(turn.source_calls)
    assert any(receipt["reason"].startswith("dual_child:") for receipt in receipts)
    assert all(receipt["scope_id"] == receipt["dataset"] for receipt in receipts)
