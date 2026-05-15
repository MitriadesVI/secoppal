#!/usr/bin/env python3
"""Rollups operacionales para traces JSONL de SECOPPAL."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any


DEFAULT_TRACE_PATH = Path("data/feedback.jsonl")


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_traces(path: Path, since_days: int | None = None) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    cutoff = None
    if since_days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)

    traces: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                trace = json.loads(line)
            except json.JSONDecodeError:
                continue
            if cutoff is not None:
                ts = _parse_timestamp(trace.get("timestamp"))
                if ts is None:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts < cutoff:
                    continue
            traces.append(trace)
    return traces


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = round((len(ordered) - 1) * pct)
    return round(float(ordered[idx]), 2)


def summarize(traces: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(traces)
    total_ms = [float(t["total_ms"]) for t in traces if isinstance(t.get("total_ms"), int | float)]
    timings_by_stage: dict[str, list[float]] = defaultdict(list)

    for trace in traces:
        timings = trace.get("timings_ms") or {}
        if isinstance(timings, dict):
            for key, value in timings.items():
                if isinstance(value, int | float):
                    timings_by_stage[key].append(float(value))

    route_counter = Counter(t.get("route_reason") or "unknown" for t in traces)
    intent_counter = Counter(t.get("followup_intent_type") or t.get("intent_type") or "none" for t in traces)
    error_counter = Counter(t.get("query_error") or "" for t in traces if t.get("query_error"))

    by_stage = {
        stage: {
            "count": len(values),
            "avg_ms": round(mean(values), 2),
            "p50_ms": percentile(values, 0.50),
            "p95_ms": percentile(values, 0.95),
        }
        for stage, values in sorted(timings_by_stage.items())
    }

    return {
        "total_queries": total,
        "latency_total_ms": {
            "count": len(total_ms),
            "avg_ms": round(mean(total_ms), 2) if total_ms else 0.0,
            "p50_ms": percentile(total_ms, 0.50),
            "p95_ms": percentile(total_ms, 0.95),
        },
        "rates": {
            "llm_rate": _rate(sum(1 for t in traces if t.get("needs_llm")), total),
            "degraded_rate": _rate(sum(1 for t in traces if t.get("degraded")), total),
            "clarification_rate": _rate(sum(1 for t in traces if t.get("needs_clarification")), total),
            "error_rate": _rate(sum(1 for t in traces if t.get("query_error")), total),
            "no_results_rate": _rate(sum(1 for t in traces if int(t.get("results_count") or 0) == 0), total),
            "followup_rate": _rate(sum(1 for t in traces if t.get("followup")), total),
        },
        "top_routes": route_counter.most_common(10),
        "top_intents": intent_counter.most_common(10),
        "top_errors": error_counter.most_common(10),
        "by_stage": by_stage,
    }


def _rate(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def print_report(summary: dict[str, Any]) -> None:
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main() -> int:
    parser = argparse.ArgumentParser(description="Analiza traces JSONL de SECOPPAL")
    parser.add_argument("--path", type=Path, default=DEFAULT_TRACE_PATH, help="Ruta al feedback/traces JSONL")
    parser.add_argument("--since-days", type=int, default=None, help="Filtrar traces de los últimos N días")
    args = parser.parse_args()

    traces = load_traces(args.path, since_days=args.since_days)
    print_report(summarize(traces))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
