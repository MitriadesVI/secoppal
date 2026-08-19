#!/usr/bin/env python3
"""Smoke local de build y latencia para el resolver AP3-B.

Usa el corpus sanitizado de AP3-A, sintetiza documentos CC no reales y crea un
SQLite temporal. Los umbrales son alarmas informativas por defecto; ``--strict``
los convierte en exit code para una máquina de benchmark controlada.
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.data.actor_alias_index import build_actor_alias_index  # noqa: E402
from app.data.actor_alias_resolver import ActorNameResolver  # noqa: E402


DEFAULT_SAMPLE = ROOT / "tests" / "fixtures" / "actor_resolution" / "benchmark_sample.jsonl"
EXACT_P95_GUIDANCE_MS = 5.0
TRIGRAM_P95_GUIDANCE_MS = 10.0


def _percentile(values: list[float], percentile: int) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    position = round((len(ordered) - 1) * percentile / 100)
    return ordered[position]


def _source_rows(sample: Path) -> tuple[list[dict], list[str]]:
    rows: list[dict] = []
    names: list[str] = []
    cc_documents: dict[str, str] = {}
    for line in sample.read_text(encoding="utf-8").splitlines():
        value = json.loads(line)
        if value.get("record_type") != "alias":
            continue
        document_type = str(value["document_type"])
        if document_type == "CC":
            identity = str(value["identity"])
            document = cc_documents.setdefault(
                identity,
                str(60_000_000_000 + len(cc_documents)),
            )
        else:
            document = str(value["document"])
        name = str(value["observed_name"])
        names.append(name)
        rows.append({
            "document_type": document_type,
            "document": document,
            "observed_name": name,
            "occurrences": value["occurrences"],
            "first_seen": value.get("first_seen"),
            "last_seen": value.get("last_seen"),
        })
    return rows, names


def _measure(resolver: ActorNameResolver, queries: list[str]) -> list[float]:
    timings: list[float] = []
    for query in queries:
        started = time.perf_counter_ns()
        resolver.resolve(query)
        timings.append((time.perf_counter_ns() - started) / 1_000_000)
    return timings


def run(sample: Path, index_path: Path, iterations: int) -> dict:
    rows, names = _source_rows(sample)
    started = time.perf_counter()
    report = build_actor_alias_index(
        rows,
        index_path,
        source_dataset="AP3-A_SANITIZED_LATENCY_FIXTURE",
        built_at="2026-08-19T00:00:00+00:00",
        build_parameters={"purpose": "AP3-B_LATENCY_SMOKE"},
    )
    build_ms = (time.perf_counter() - started) * 1000
    resolver = ActorNameResolver(index_path)
    exact_queries = names[:iterations]
    fuzzy_queries = [f"{name} variante" for name in names[:iterations]]
    for query in exact_queries[:20] + fuzzy_queries[:20]:
        resolver.resolve(query)
    exact = _measure(resolver, exact_queries)
    trigram = _measure(resolver, fuzzy_queries)
    exact_p95 = _percentile(exact, 95)
    trigram_p95 = _percentile(trigram, 95)
    return {
        "source_rows": len(rows),
        "identities": report.metadata.identity_count,
        "aliases": report.metadata.alias_count,
        "index_bytes": index_path.stat().st_size,
        "build_ms": round(build_ms, 3),
        "queries_per_path": len(exact_queries),
        "exact_latency_ms": {
            "p50": round(_percentile(exact, 50), 3),
            "p95": round(exact_p95, 3),
            "guidance": EXACT_P95_GUIDANCE_MS,
            "status": "PASS" if exact_p95 < EXACT_P95_GUIDANCE_MS else "REGRESSION_ALARM",
        },
        "trigram_latency_ms": {
            "p50": round(_percentile(trigram, 50), 3),
            "p95": round(trigram_p95, 3),
            "guidance": TRIGRAM_P95_GUIDANCE_MS,
            "status": "PASS" if trigram_p95 < TRIGRAM_P95_GUIDANCE_MS else "REGRESSION_ALARM",
        },
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=Path, default=DEFAULT_SAMPLE)
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--index", type=Path)
    parser.add_argument("--strict", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.iterations < 1:
        raise SystemExit("--iterations must be positive")
    if args.index:
        result = run(args.sample.resolve(), args.index.resolve(), args.iterations)
    else:
        with tempfile.TemporaryDirectory(prefix="secoppal-ap3b-") as directory:
            result = run(
                args.sample.resolve(),
                Path(directory) / "actor_alias_index.sqlite",
                args.iterations,
            )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    statuses = {
        result["exact_latency_ms"]["status"],
        result["trigram_latency_ms"]["status"],
    }
    return 1 if args.strict and statuses != {"PASS"} else 0


if __name__ == "__main__":
    raise SystemExit(main())
