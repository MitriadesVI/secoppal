from __future__ import annotations

import threading
import time

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def test_execute_query_runs_count_and_select_in_parallel(tmp_path):
    barrier = threading.Barrier(2, timeout=1.0)
    events: dict[str, float] = {}

    def _count(dataset_id, soql):
        events["count_start"] = time.perf_counter()
        barrier.wait()
        time.sleep(0.03)
        events["count_end"] = time.perf_counter()
        return 1

    def _query(dataset_id, soql):
        events["query_start"] = time.perf_counter()
        barrier.wait()
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
    workflow.secop_client.count = _count  # type: ignore[method-assign]
    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None  # type: ignore[method-assign]

    result = workflow.run_query("procesos de mantenimiento", channel="streamlit")

    assert result["total_count"] == 1
    assert result["results"]
    assert events["query_start"] < events["count_end"]
    assert events["count_start"] < events["query_end"]
    assert result["timings_ms"]["execute_total_ms"] >= max(
        result["timings_ms"]["secop_count_ms"],
        result["timings_ms"]["secop_query_ms"],
    )
