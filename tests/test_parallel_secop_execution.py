from __future__ import annotations

import threading
import time

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def test_execute_query_skips_count_when_under_limit(tmp_path):
    """Query first: when results < limit, count is skipped and total_count = len(results)."""
    events: dict[str, float] = {}

    def _query(dataset_id, soql):
        events["query_start"] = time.perf_counter()
        time.sleep(0.03)
        events["query_end"] = time.perf_counter()
        return [
            {
                "referencia_del_proceso": "PAR-1",
                "nombre_del_procedimiento": "Mantenimiento vial",
                "precio_base": "1000000",
                "fecha_de_publicacion_del": "2026-01-01",
            }
        ]

    workflow = SecopalWorkflow(Settings(secop_feedback_path=tmp_path / "feedback.jsonl"))
    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = lambda *a, **kw: 99  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]

    result = workflow.run_query("procesos de mantenimiento", channel="streamlit")

    # With 1 result (< limit 50), count should be skipped
    assert result["total_count"] == 1
    assert result["results"]
    assert "query_start" in events
    assert "query_end" in events


def test_execute_query_runs_count_when_at_limit(tmp_path):
    """When query returns exactly limit rows, count runs with short timeout."""
    events: dict[str, float] = {}

    def _query(dataset_id, soql):
        events["query_start"] = time.perf_counter()
        time.sleep(0.02)
        events["query_end"] = time.perf_counter()
        return [
            {
                "referencia_del_proceso": f"PAR-{i}",
                "nombre_del_procedimiento": f"Mantenimiento vial {i}",
                "precio_base": "1000000",
                "fecha_de_publicacion_del": "2026-01-01",
            }
            for i in range(50)
        ]

    def _count(dataset_id, soql):
        events["count_start"] = time.perf_counter()
        time.sleep(0.02)
        events["count_end"] = time.perf_counter()
        return 347

    workflow = SecopalWorkflow(Settings(secop_feedback_path=tmp_path / "feedback.jsonl"))
    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = _count  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]

    result = workflow.run_query("procesos de mantenimiento", channel="streamlit")

    assert result["total_count"] == 347
    assert result["results"]
    assert "count_start" in events
    assert "count_end" in events
