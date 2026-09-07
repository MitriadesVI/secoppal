#!/usr/bin/env python3
"""Read-only ADR-094 impact verification and warm BEFORE/AFTER benchmarks.

Use --phase audit first (full traversal warms pages), then --phase benchmark.
Documents never leave memory. Outputs identify sample queries with SHA-256.
Budgets/acceptance must already exist in --evidence-dir before running.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
from itertools import groupby
import json
import math
from pathlib import Path
import sqlite3
import sys
from time import perf_counter

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.core.actor_identity import IdentityKey
from app.core.actor_name import ActorResolutionCandidate
from app.data.actor_alias_resolver import ActorNameResolver
from app.data.actor_exact_prefix import (
    ENUMERATION_SQL, EXISTENCE_SQL, ENUMERATION_BUDGET_MS, EXISTENCE_BUDGET_MS,
    enumerate_prefix_challengers, find_prefix_challenger, prefix_bounds,
)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def summary(values):
    ordered = sorted(values)
    return {"n": len(ordered), "p50_ms": ordered[math.ceil(len(ordered) * .5) - 1],
            "p95_ms": ordered[math.ceil(len(ordered) * .95) - 1], "max_ms": ordered[-1]}


def save(directory, name, result):
    (directory / name).write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")


def oracle(connection):
    """Independent token-prefix oracle; no runtime prefix ranges or normalizer."""
    eligible = {}
    for alias, rows in groupby(connection.execute(
        "SELECT normalized_alias,document_type,base_number FROM actor_aliases "
        "INDEXED BY sqlite_autoindex_actor_aliases_1 ORDER BY normalized_alias,document_type,base_number"
    ), lambda row: row[0]):
        group = list(rows)
        if len(group) == 1 and group[0][1] == "CC":
            eligible[alias] = group[0][2]
    found = defaultdict(set)
    for alias, number in connection.execute("SELECT normalized_alias,base_number FROM actor_aliases WHERE document_type='CC'"):
        for position, character in enumerate(alias):
            if character != " ":
                continue
            query = alias[:position]
            original = eligible.get(query)
            if original is not None and original != number and len(found[query]) < 10:
                found[query].add(number)
    return eligible, found


def audit(resolver, directory):
    started = perf_counter()
    counts, reasons = Counter(), Counter()
    by_shape = defaultdict(Counter)
    enumeration_counts = Counter()
    mismatches = []
    with resolver._connect_readonly() as connection:
        eligible, found = oracle(connection)
        for query, number in eligible.items():
            # Exact authority read from the globally unique alias traversal;
            # never infer identity by spelling or activity. Witnesses use real
            # runtime candidate construction from their representative rows.
            exact = ActorResolutionCandidate(IdentityKey("CC", number), query, query, 0, None, None, 1)
            check = find_prefix_challenger(connection, query, exact, resolver._candidate)
            counts[check.status] += 1
            shape = str(min(len(query.split()), 4))
            by_shape[shape][check.status] += 1
            by_shape[shape]["eligible"] += 1
            expected = query in found
            if check.status == "INCONCLUSIVE":
                reasons[check.reason] += 1
            elif (check.status == "FOUND") != expected:
                mismatches.append(digest(query))
            if check.status == "FOUND":
                try:
                    candidates = enumerate_prefix_challengers(connection, query, exact, resolver._candidate)
                except (OSError, ValueError, sqlite3.DatabaseError) as error:
                    enumeration_counts["incomplete"] += 1
                    reasons["enumeration_" + type(error).__name__] += 1
                    continue
                count = len(candidates)
                bucket = "one" if count == 1 else "two_to_nine" if count < 10 else "ten_plus"
                enumeration_counts[bucket] += 1
                by_shape[shape][bucket] += 1
                if count != len(found[query]) or not candidates or candidates[0] != check.witness:
                    mismatches.append(digest(query))
    report = {
        "eligible_exact_unique_cc_aliases": len(eligible), "affected_found": counts["FOUND"],
        "unaffected_absent_confirmed": counts["ABSENT_CONFIRMED"], "inconclusive_checks": counts["INCONCLUSIVE"],
        "by_token_count": dict(by_shape), "enumeration": dict(enumeration_counts), "failure_reasons": dict(reasons),
        "oracle_affected": len(found), "oracle_absent": len(eligible) - len(found),
        "runtime_vs_oracle_mismatch_query_sha256": mismatches,
        "elapsed_seconds": perf_counter() - started, "readonly": True,
        "method": "All eligible aliases checked with runtime existence and budget; FOUND enumerated with runtime dedup/representative/order and its own budget; independent full token-prefix oracle comparison",
    }
    save(directory, "impact_runtime.json", report)
    print(json.dumps(report, ensure_ascii=False))
    return int(bool(mismatches))


def benchmark(resolver, directory):
    contract = json.loads((directory / "budget_contract.json").read_text())
    assert contract["existence_budget_ms"] == EXISTENCE_BUDGET_MS
    assert contract["enumeration_budget_ms"] == ENUMERATION_BUDGET_MS
    baseline = json.loads((directory / "baseline_benchmark.json").read_text())
    assert baseline["sql"] == ENUMERATION_SQL
    old = {row["query_sha256"]: row for row in baseline["results"]}
    queries = json.loads((directory / "benchmark_queries.json").read_text())
    thresholds = contract["acceptance_thresholds"]
    results, failures = [], []
    with resolver._connect_readonly() as connection:
        plans = {name: [r[3] for r in connection.execute("EXPLAIN QUERY PLAN " + sql, (*prefix_bounds("example person"), "00000000"))]
                 for name, sql in (("existence", EXISTENCE_SQL), ("enumeration", ENUMERATION_SQL))}
        for plan in plans.values():
            assert any("normalized_alias>? AND normalized_alias<?" in line and "SEARCH" in line for line in plan)
            assert not any("SCAN" in line for line in plan)
        for sample in queries:
            query = sample["query"]
            exact = resolver._collapse_exact(connection.execute("SELECT * FROM actor_aliases WHERE normalized_alias=?", (query,)).fetchall())[0]
            timers = defaultdict(list)
            counts = Counter()
            for iteration in range(baseline["warmups"] + baseline["repetitions"]):
                start = perf_counter()
                result = resolver.resolve_exact(query)
                elapsed = (perf_counter() - start) * 1000
                counts[result.status] += 1
                if iteration >= baseline["warmups"]:
                    timers["resolver_after"].append(elapsed)
                start = perf_counter()
                check = find_prefix_challenger(connection, query, exact, resolver._candidate)
                elapsed = (perf_counter() - start) * 1000
                if iteration >= baseline["warmups"]:
                    timers["existence"].append(elapsed)
                if check.status == "FOUND":
                    start = perf_counter()
                    enumerate_prefix_challengers(connection, query, exact, resolver._candidate)
                    elapsed = (perf_counter() - start) * 1000
                    if iteration >= baseline["warmups"]:
                        timers["enumeration"].append(elapsed)
            hashed = digest(query)
            result = {"query_sha256": hashed, "shape": sample["shape"], "challengers_bucket": sample["challengers_bucket"],
                      **{key: summary(values) for key, values in timers.items()}, "statuses": dict(counts)}
            delta = result["resolver_after"]["p95_ms"] - old[hashed]["resolver_before"]["p95_ms"]
            result["resolver_p95_delta_ms"] = delta
            if delta > thresholds["each_warm_production_query_resolver_p95_delta_max_ms"]:
                failures.append({"query_sha256": hashed, "gate": "resolver_delta"})
            for phase in ("existence", "enumeration"):
                if phase in result and result[phase]["p95_ms"] > thresholds[f"each_warm_production_query_{phase}_p95_max_ms"]:
                    failures.append({"query_sha256": hashed, "gate": phase})
            if counts["INCONCLUSIVE"]:
                failures.append({"query_sha256": hashed, "gate": "unexpected_inconclusive_in_warm_sample"})
            results.append(result)
    report = {"sql": ENUMERATION_SQL, "plans": plans, "samples": len(queries), "warmups": baseline["warmups"],
              "repetitions": baseline["repetitions"], "results": results, "failures": failures,
              "verdict": "NO_GO" if failures else "GO", "baseline_artifact": "baseline_benchmark.json",
              "scope": "warm local cache; per-query thresholds frozen before implementation; includes VM, dedup, representative candidate materialization and SQL order"}
    save(directory, "after_benchmark.json", report)
    print(json.dumps({"verdict": report["verdict"], "failures": failures, "samples": len(queries)}))
    return int(bool(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-path", required=True, type=Path)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--phase", choices=["audit", "benchmark"], required=True)
    args = parser.parse_args()
    resolver = ActorNameResolver(args.index_path)
    before = args.index_path.stat()
    outcome = (audit if args.phase == "audit" else benchmark)(resolver, args.evidence_dir)
    after = args.index_path.stat()
    assert (before.st_mtime_ns, before.st_size) == (after.st_mtime_ns, after.st_size)
    return outcome


if __name__ == "__main__":
    raise SystemExit(main())
