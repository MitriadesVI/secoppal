"""FOUNDATION-TRACE: the dogfood trace projects what the turn already carries, nothing more."""

from __future__ import annotations

from copy import deepcopy

from app.core.foundation_trace_view import project, render
from tests.product_contract.harness import ProductHarness
from tests.test_foundation_messages import assert_public
from tests.test_foundation_runtime import NOW, RAW, ROWS, accepted


class _Recorder:
    """Stands for the Streamlit module: records every call and its payload."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        return lambda *args, **kwargs: self.calls.append((name, args))


def _turn(tmp_path, monkeypatch):
    harness = ProductHarness(tmp_path, monkeypatch, rows_by_dataset=deepcopy(ROWS), reference_instant=NOW,
                             today=NOW.date(), foundation_producer=lambda raw: accepted(raw),
                             settings_overrides={"secoppal_foundation_mode": "foundation_v3"})
    return harness.say(RAW).result


def test_a_legacy_result_has_no_foundation_trace():
    assert project({"route_reason": "heuristic", "rows": []}) is None
    recorder = _Recorder()
    render(recorder, None)
    assert recorder.calls == []


def test_the_trace_projects_the_turn_fields_without_changing_the_result(tmp_path, monkeypatch):
    result = _turn(tmp_path, monkeypatch)
    before = deepcopy(result)
    view = project(result)
    assert result == before
    assert view["terminal"] == result["foundation_terminal"] and view["plan_decision"] == "READY"
    assert view["producer"] == {k: v for k, v in result["foundation_telemetry"].items() if k in view["producer"]}
    assert [c["claim"] for c in view["conditions"]] and view["conditions"][0]["evidence"] == ["aseo"]
    assert [item["soql"] for item in view["plan"]] == [item["soql"] for item in result["foundation_plan"]]
    assert {r["call_id"] for r in view["receipts"]} == {r["call_id"] for r in result["source_call_receipts"]}
    assert view["groups"][0]["rows"] == len(result["groups"][0]["rows"])
    assert view["h7"] == result.get("foundation_universe")
    assert_public(result["response"])  # internal codes live in the trace, never in the answer


def test_render_shows_every_soql_and_receipt(tmp_path, monkeypatch):
    view = project(_turn(tmp_path, monkeypatch))
    recorder = _Recorder()
    render(recorder, view)
    code = [args[0] for name, args in recorder.calls if name == "code"]
    assert code == [item["soql"] for item in view["plan"]]
    tables = [args[0] for name, args in recorder.calls if name == "dataframe"]
    assert view["receipts"] in tables and view["groups"] in tables
