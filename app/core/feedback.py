"""
SECOPAL — Query Feedback System
=================================

Stores complete query traces with user ratings.
Each trace captures the full pipeline: raw query → regex params → LLM params →
entity resolution → SoQL → results count → user rating.

Storage: JSONL (one JSON object per line) — append-only, easy to analyze.

Usage:
    feedback = FeedbackStore("data/feedback.jsonl")

    # After query execution, log the trace
    trace_id = feedback.log_trace(
        user_query="procesos de mantenimiento en atlantico",
        channel="streamlit",
        workflow_result=result,  # dict from SecopalWorkflow.run_query()
    )

    # When user rates the result
    feedback.rate(trace_id, rating=1, comment="encontró lo que buscaba")

    # Analysis
    stats = feedback.get_stats()
    failures = feedback.get_unrated()
    low_rated = feedback.get_low_rated()
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class FeedbackStore:
    """Append-only JSONL store for query traces and ratings."""

    def __init__(self, path: str | Path = "data/feedback.jsonl"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log_trace(
        self,
        user_query: str,
        channel: str,
        workflow_result: dict[str, Any],
    ) -> str:
        """
        Log a complete query trace. Returns trace_id for later rating.

        workflow_result is the dict returned by SecopalWorkflow.run_query()
        """
        trace_id = uuid.uuid4().hex[:12]

        trace = {
            "trace_id": trace_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_query": user_query,
            "channel": channel,
            # Pipeline trace
            "parsed_params": workflow_result.get("parsed_params", {}),
            "resolved_params": _safe_serialize(workflow_result.get("resolved_params", {})),
            "dataset_id": workflow_result.get("dataset_id", ""),
            "soql_query": workflow_result.get("soql_query", ""),
            "needs_llm": workflow_result.get("needs_llm", False),
            "needs_clarification": workflow_result.get("needs_clarification", False),
            "route_reason": workflow_result.get("route_reason", ""),
            "timings_ms": _safe_serialize(workflow_result.get("timings_ms", {})),
            "total_ms": (workflow_result.get("timings_ms", {}) or {}).get("total_ms"),
            "query_error": workflow_result.get("query_error", ""),
            "total_count": workflow_result.get("total_count", 0),
            "degraded": workflow_result.get("degraded", False),
            "followup": workflow_result.get("followup", False),
            "intent_type": workflow_result.get("intent_type", ""),
            "followup_intent_type": workflow_result.get("followup_intent_type", ""),
            # Results summary (not full results — too large)
            "results_count": len(workflow_result.get("results", [])),
            "first_result_title": _first_title(workflow_result),
            # Rating (filled later)
            "rating": None,  # 1=good, 0=bad, None=unrated
            "rating_comment": None,
            "rated_at": None,
        }

        self._append(trace)
        return trace_id

    def rate(self, trace_id: str, rating: int, comment: str | None = None) -> bool:
        """
        Rate a trace. rating: 1=relevant results, 0=wrong/irrelevant.
        Returns True if trace was found and updated.
        """
        traces = self._read_all()
        found = False

        for trace in traces:
            if trace["trace_id"] == trace_id:
                trace["rating"] = rating
                trace["rating_comment"] = comment
                trace["rated_at"] = datetime.now(timezone.utc).isoformat()
                found = True
                break

        if found:
            self._write_all(traces)
        return found

    def get_stats(self) -> dict:
        """Get summary statistics."""
        traces = self._read_all()
        total = len(traces)
        rated = [t for t in traces if t.get("rating") is not None]
        good = [t for t in rated if t["rating"] == 1]
        bad = [t for t in rated if t["rating"] == 0]
        unrated = total - len(rated)

        # Route breakdown
        heuristic_only = sum(1 for t in traces if t.get("route_reason") == "heuristic_only")
        with_llm = sum(1 for t in traces if "llm" in (t.get("route_reason") or ""))
        no_results = sum(1 for t in traces if t.get("results_count", 0) == 0)

        return {
            "total_queries": total,
            "rated": len(rated),
            "unrated": unrated,
            "good": len(good),
            "bad": len(bad),
            "precision": len(good) / len(rated) if rated else 0,
            "heuristic_only": heuristic_only,
            "with_llm": with_llm,
            "no_results": no_results,
            "no_results_pct": no_results / total if total else 0,
        }

    def get_low_rated(self) -> list[dict]:
        """Get traces rated as bad — these are your improvement targets."""
        return [t for t in self._read_all() if t.get("rating") == 0]

    def get_unrated(self, limit: int = 50) -> list[dict]:
        """Get unrated traces for review."""
        return [t for t in self._read_all() if t.get("rating") is None][:limit]

    def get_recent(self, limit: int = 20) -> list[dict]:
        """Get most recent traces."""
        traces = self._read_all()
        return traces[-limit:]

    def _append(self, trace: dict) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(trace, ensure_ascii=False, default=str) + "\n")

    def _read_all(self) -> list[dict]:
        if not self.path.exists():
            return []
        traces = []
        with self.path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        traces.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        return traces

    def _write_all(self, traces: list[dict]) -> None:
        """Rewrite entire file (for updates like rating)."""
        with self.path.open("w", encoding="utf-8") as f:
            for trace in traces:
                f.write(json.dumps(trace, ensure_ascii=False, default=str) + "\n")


def _safe_serialize(obj: Any) -> Any:
    """Make sure nested dicts are JSON-serializable."""
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_serialize(item) for item in obj]
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    return str(obj)


def _first_title(result: dict) -> str:
    """Extract title of first result for quick trace inspection."""
    results = result.get("results", [])
    if not results:
        return ""
    first = results[0]
    return str(
        first.get("nombre_del_procedimiento")
        or first.get("objeto_del_contrato")
        or ""
    )[:100]
