from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


def _load_analyze_traces_module():
    path = Path("scripts/analyze_traces.py")
    spec = importlib.util.spec_from_file_location("analyze_traces", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_trace_jsonl_includes_stage_timings_and_operational_fields(tmp_path):
    feedback_path = tmp_path / "feedback.jsonl"
    workflow = SecopalWorkflow(Settings(secop_feedback_path=feedback_path))
    workflow.secop_client.query = lambda dataset_id, soql: [  # type: ignore[method-assign]
        {
            "referencia_del_proceso": "OBS-1",
            "nombre_del_procedimiento": "Mantenimiento vial",
            "precio_base": "1000000",
            "fecha_de_publicacion_del": "2026-01-01",
        }
    ]
    workflow.secop_client.count = lambda dataset_id, soql: 1  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]
    workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")

    result = workflow.run_query("procesos de mantenimiento", channel="streamlit", chat_id="st_obs")

    assert result["trace_id"]
    assert result["timings_ms"]["parse_ms"] >= 0
    assert result["timings_ms"]["secop_count_ms"] >= 0
    assert result["timings_ms"]["secop_query_ms"] >= 0
    assert result["timings_ms"]["total_ms"] >= 0

    traces = [json.loads(line) for line in feedback_path.read_text(encoding="utf-8").splitlines()]
    assert len(traces) == 1
    trace = traces[0]
    assert trace["trace_id"] == result["trace_id"]
    assert trace["timings_ms"]["parse_ms"] >= 0
    assert trace["total_ms"] == result["timings_ms"]["total_ms"]
    assert trace["total_count"] == 1
    assert trace["query_error"] == ""
    assert trace["followup"] is False


def test_analyze_traces_rollup_reports_latency_rates_and_stage_stats(tmp_path):
    module = _load_analyze_traces_module()
    path = tmp_path / "feedback.jsonl"
    rows = [
        {
            "timestamp": "2026-05-15T21:00:00+00:00",
            "trace_id": "a",
            "total_ms": 100.0,
            "timings_ms": {"parse_ms": 10.0, "secop_query_ms": 70.0},
            "needs_llm": False,
            "degraded": False,
            "needs_clarification": False,
            "query_error": "",
            "results_count": 1,
            "followup": False,
            "route_reason": "heuristic_only",
            "intent_type": "",
        },
        {
            "timestamp": "2026-05-15T21:01:00+00:00",
            "trace_id": "b",
            "total_ms": 300.0,
            "timings_ms": {"parse_ms": 20.0, "secop_query_ms": 200.0},
            "needs_llm": True,
            "degraded": True,
            "needs_clarification": False,
            "query_error": "",
            "results_count": 0,
            "followup": True,
            "route_reason": "llm_fallback",
            "followup_intent_type": "refine_filter",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")

    summary = module.summarize(module.load_traces(path))

    assert summary["total_queries"] == 2
    assert summary["latency_total_ms"]["p50_ms"] == 100.0
    assert summary["latency_total_ms"]["p95_ms"] == 300.0
    assert summary["rates"]["llm_rate"] == 0.5
    assert summary["rates"]["degraded_rate"] == 0.5
    assert summary["rates"]["followup_rate"] == 0.5
    assert summary["by_stage"]["parse_ms"]["avg_ms"] == 15.0
    assert summary["top_routes"][0] == ("heuristic_only", 1)
