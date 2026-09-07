#!/usr/bin/env python3
"""Paired BEFORE/AFTER re-adjudication of the ADR-094 resolver gate.

Read-only. Runtime is never touched: BEFORE is the resolver Git blob of the
frozen base, loaded the same way ``actor_prefix_dense.py`` loads it; AFTER is
the branch runtime as imported. Two phases, in order:

    --phase contract   write and hash the contract BEFORE any measurement
    --phase run        five independent paired campaigns + a sanity cohort

The contract's SHA-256 is checked again at run time, so the decision rule and
thresholds cannot be edited between writing them down and measuring.
Queries never leave memory: evidence identifies them by SHA-256 only.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.util
import json
import os
import platform
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns, process_time_ns

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.data.actor_exact_prefix import (  # noqa: E402
    ENUMERATION_BUDGET_MS,
    EXISTENCE_BUDGET_MS,
    enumerate_prefix_challengers,
    find_prefix_challenger,
)
from app.data.actor_alias_resolver import ActorNameResolver  # noqa: E402
from scripts.audits.actor_prefix_adjudicator import (  # noqa: E402
    ENUMERATION_P95_MAX_MS,
    EXISTENCE_P95_MAX_MS,
    MEASUREMENTS_PER_VERSION,
    REQUIRED_CAMPAIGNS,
    RESOLVER_DELTA_MAX_MS,
    WARMUPS_PER_VERSION,
    adjudicate,
    judge_campaign,
    percentile,
)

BASE = "345804cf55becaf728d3ec9c20dc9e2f76a3a5b9"
RUNTIME = "ddcc6b8ac4312e63ff34d3e9d187045300412268"
FAILED_QUERY_SHA256 = "00d89a30f637437a54e698a3840c5a527b95f58e9afae93ba7c84a370d8dd0e0"
SANITY_PER_BUCKET = 3
SANITY_PAIRS = 500
CONTRACT_NAME = "performance_adjudication_contract.json"
RAW_NAME = "performance_adjudication_raw.jsonl.gz"
REPORT_NAME = "performance_adjudication.json"


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def file_sha256(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def summarize(nanoseconds):
    """p50/p95/p99/max in ms from integer nanosecond samples."""
    return {
        "n": len(nanoseconds),
        "p50_ms": percentile(nanoseconds, 0.5) / 1e6,
        "p95_ms": percentile(nanoseconds, 0.95) / 1e6,
        "p99_ms": percentile(nanoseconds, 0.99) / 1e6,
        "max_ms": max(nanoseconds) / 1e6,
    }


def index_identity(path: Path) -> dict:
    stat = path.stat()
    resolver = ActorNameResolver(path)
    with resolver._connect_readonly() as connection:
        metadata = dict(connection.execute("SELECT * FROM actor_index_metadata").fetchone())
    return {
        "path": str(path),
        "sha256": file_sha256(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "schema_version": metadata["schema_version"],
        "normalization_version": metadata["normalization_version"],
        "built_at": metadata["built_at"],
        "alias_count": metadata["alias_count"],
        "identity_count": metadata["identity_count"],
    }


def index_stat_fingerprint(path: Path) -> tuple[int, int]:
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns


def load_before_resolver_module():
    """Import the frozen BEFORE resolver from its Git blob (dense mechanism)."""
    blob = subprocess.check_output(
        ["git", "show", BASE + ":app/data/actor_alias_resolver.py"], cwd=ROOT
    )
    with TemporaryDirectory() as directory:
        module_path = Path(directory) / "resolver_before.py"
        module_path.write_bytes(blob)
        spec = importlib.util.spec_from_file_location("prefix_baseline_resolver", module_path)
        assert spec and spec.loader
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module, hashlib.sha256(blob).hexdigest()


def environment_metadata(stage: str) -> dict:
    try:
        load = os.getloadavg()
    except OSError:
        load = None
    return {
        "stage": stage,
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "load_average": load,
        "pid": os.getpid(),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
    }


def select_sanity_cohort(queries, excluded_sha):
    """Deterministic: first three of each challenger bucket ordered by SHA-256."""
    buckets = defaultdict(list)
    for sample in queries:
        hashed = digest(sample["query"])
        if hashed == excluded_sha or sample["challengers_bucket"] == "case_cantillo":
            continue
        buckets[sample["challengers_bucket"]].append((hashed, sample))
    cohort = []
    for bucket in ("absent", "one", "two_to_nine", "ten_plus"):
        chosen = sorted(buckets[bucket], key=lambda item: item[0])[:SANITY_PER_BUCKET]
        if len(chosen) != SANITY_PER_BUCKET:
            raise ValueError(f"bucket {bucket} cannot supply {SANITY_PER_BUCKET} queries")
        cohort.extend({"query_sha256": h, "challengers_bucket": bucket, "shape": s["shape"]}
                      for h, s in chosen)
    return cohort


def find_query(queries, hashed):
    for sample in queries:
        if digest(sample["query"]) == hashed:
            return sample["query"]
    raise KeyError("query sha256 not present in the frozen benchmark corpus")


def measure_paired(before_resolver, after_resolver, connection, exact, query, pairs, warmups):
    """Alternating paired measurement; wall clock is the gate authority.

    Pair k puts BEFORE first on even k and AFTER first on odd k, so each version
    spends half its samples in each position and temporal drift cannot favour one.
    """
    for index in range(warmups):
        if index % 2 == 0:
            before_resolver.resolve_exact(query)
            after_resolver.resolve_exact(query)
        else:
            after_resolver.resolve_exact(query)
            before_resolver.resolve_exact(query)
    samples = {"before_ns": [], "after_ns": [], "existence_ns": [], "enumeration_ns": []}
    statuses = Counter()
    for index in range(pairs):
        order = (("before_ns", before_resolver), ("after_ns", after_resolver))
        if index % 2:
            order = tuple(reversed(order))
        for key, resolver in order:
            start = perf_counter_ns()
            result = resolver.resolve_exact(query)
            samples[key].append(perf_counter_ns() - start)
            statuses[key.removesuffix("_ns") + ":" + result.status] += 1
        start = perf_counter_ns()
        check = find_prefix_challenger(connection, query, exact, after_resolver._candidate)
        samples["existence_ns"].append(perf_counter_ns() - start)
        statuses["existence:" + check.status] += 1
        if check.status == "FOUND":
            start = perf_counter_ns()
            enumerate_prefix_challengers(connection, query, exact, after_resolver._candidate)
            samples["enumeration_ns"].append(perf_counter_ns() - start)
    return samples, dict(statuses)


def measure_cpu_diagnostic(before_resolver, after_resolver, query, pairs):
    """Diagnostic only; never an input to the gate."""
    cpu = {"before_ns": [], "after_ns": []}
    for index in range(pairs):
        order = (("before_ns", before_resolver), ("after_ns", after_resolver))
        if index % 2:
            order = tuple(reversed(order))
        for key, resolver in order:
            start = process_time_ns()
            resolver.resolve_exact(query)
            cpu[key].append(process_time_ns() - start)
    return {key: summarize(values) for key, values in cpu.items()}


def exact_candidate(resolver, connection, query):
    rows = connection.execute(
        "SELECT * FROM actor_aliases WHERE normalized_alias=?", (query,)
    ).fetchall()
    return resolver._collapse_exact(rows)[0]


def write_contract(args):
    queries = json.loads((args.evidence_dir / "benchmark_queries.json").read_text())
    find_query(queries, FAILED_QUERY_SHA256)
    after = json.loads((args.evidence_dir / "after_benchmark.json").read_text())
    baseline = json.loads((args.evidence_dir / "baseline_benchmark.json").read_text())
    primary = next(r for r in after["results"] if r["query_sha256"] == FAILED_QUERY_SHA256)
    frozen = next(r for r in baseline["results"] if r["query_sha256"] == FAILED_QUERY_SHA256)
    _, blob_sha = load_before_resolver_module()
    contract = {
        "purpose": "Predefined re-adjudication of the ADR-094 resolver p95 gate. "
                   "Written and hashed before any measurement.",
        "written_at_utc": datetime.now(UTC).isoformat(),
        "commits": {
            "base_before": BASE,
            "runtime_after": RUNTIME,
            "head": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "branch": subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=ROOT, text=True).strip(),
        },
        "before_source": {
            "mechanism": "git show <base>:app/data/actor_alias_resolver.py loaded with importlib",
            "blob_sha256": blob_sha,
            "reused_from": "scripts/audits/actor_prefix_dense.py",
            "note": "The frozen blob imports app.core.actor_name from the branch. That module "
                    "only adds an extra status, two optional fields and a validator reached "
                    "solely by method='exact_prefix_ambiguous'; the BEFORE RESOLVED path is "
                    "unchanged. The AFTER algorithm is never executed for BEFORE.",
        },
        "query": {
            "failed_query_sha256": FAILED_QUERY_SHA256,
            "resolved_from": "benchmark_queries.json",
            "shape": primary["shape"],
            "challengers_bucket": primary["challengers_bucket"],
            "never_written_in_clear": True,
        },
        "primary_gate": {
            "verdict": after["verdict"],
            "retained_artifact": "after_benchmark.json",
            "before_p95_ms": frozen["resolver_before"]["p95_ms"],
            "after_p95_ms": primary["resolver_after"]["p95_ms"],
            "limit_ms": frozen["resolver_before"]["p95_ms"] + RESOLVER_DELTA_MAX_MS,
            "must_not_be_deleted": True,
        },
        "design": {
            "campaigns": REQUIRED_CAMPAIGNS,
            "measurements_per_version_per_campaign": MEASUREMENTS_PER_VERSION,
            "warmups_per_version_per_campaign": WARMUPS_PER_VERSION,
            "paired": True,
            "alternating_order": "pair k: BEFORE first when k even, AFTER first when k odd",
            "percentiles_per_campaign_only": True,
            "clock": "time.perf_counter_ns wall clock is the gate authority; "
                     "process_time_ns is collected in a separate diagnostic pass",
        },
        "thresholds": {
            "resolver_after_p95_minus_before_p95_max_ms": RESOLVER_DELTA_MAX_MS,
            "existence_p95_max_ms": EXISTENCE_P95_MAX_MS,
            "enumeration_p95_max_ms": ENUMERATION_P95_MAX_MS,
            "existence_operational_budget_ms": EXISTENCE_BUDGET_MS,
            "enumeration_operational_budget_ms": ENUMERATION_BUDGET_MS,
            "unchanged_from": "budget_contract.json",
        },
        "decision_rule": {
            "campaign_fails_if": ["resolver delta p95 > 0.5 ms", "existence p95 > 0.5 ms",
                                  "enumeration p95 > 1.0 ms"],
            "0_of_5": "GO; original spike not reproduced; original failure retained",
            "1_of_5": "INCONCLUSIVE; no merge; no sixth campaign",
            "2_or_more_of_5": "NO_GO; performance regression confirmed",
            "forbidden": ["discarding an outlier campaign", "re-running a failed campaign",
                          "a sixth campaign", "lowering thresholds", "raising budgets",
                          "changing runtime, index or data", "picking the best result"],
        },
        "sanity_cohort": {
            "queries": select_sanity_cohort(queries, FAILED_QUERY_SHA256),
            "pairs_per_query": SANITY_PAIRS,
            "diagnostic_only": True,
            "cannot_change_the_primary_rule": True,
        },
        "index": index_identity(args.index_path),
        "environment_at_contract_time": environment_metadata("contract"),
    }
    path = args.evidence_dir / CONTRACT_NAME
    path.write_text(json.dumps(contract, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"contract": str(path), "sha256": file_sha256(path)}))
    return 0


def run_campaigns(args):
    contract_path = args.evidence_dir / CONTRACT_NAME
    contract = json.loads(contract_path.read_text())
    contract_sha = file_sha256(contract_path)
    if args.contract_sha256 and args.contract_sha256 != contract_sha:
        raise SystemExit("contract SHA-256 changed after it was written")
    assert contract["thresholds"]["resolver_after_p95_minus_before_p95_max_ms"] == RESOLVER_DELTA_MAX_MS
    assert contract["thresholds"]["existence_p95_max_ms"] == EXISTENCE_P95_MAX_MS
    assert contract["thresholds"]["enumeration_p95_max_ms"] == ENUMERATION_P95_MAX_MS

    queries = json.loads((args.evidence_dir / "benchmark_queries.json").read_text())
    primary_query = find_query(queries, FAILED_QUERY_SHA256)
    before_module, blob_sha = load_before_resolver_module()
    assert blob_sha == contract["before_source"]["blob_sha256"]

    index_before = index_stat_fingerprint(args.index_path)
    started = environment_metadata("run_start")
    after_resolver = ActorNameResolver(args.index_path)
    before_resolver = before_module.ActorNameResolver(args.index_path)

    raw = gzip.open(args.evidence_dir / RAW_NAME, "wt", encoding="utf-8")
    raw.write(json.dumps({"record": "header", "contract_sha256": contract_sha,
                          "unit": "nanoseconds", "clock": "perf_counter_ns",
                          "before_blob_sha256": blob_sha}) + "\n")
    campaigns, verdicts = [], []
    try:
        for index in range(1, REQUIRED_CAMPAIGNS + 1):
            with after_resolver._connect_readonly() as connection:
                exact = exact_candidate(after_resolver, connection, primary_query)
                environment = environment_metadata(f"campaign_{index}")
                samples, statuses = measure_paired(
                    before_resolver, after_resolver, connection, exact, primary_query,
                    MEASUREMENTS_PER_VERSION, WARMUPS_PER_VERSION,
                )
            raw.write(json.dumps({"record": "campaign", "campaign": index,
                                  "query_sha256": FAILED_QUERY_SHA256, **samples}) + "\n")
            phases = {key.removesuffix("_ns"): summarize(values)
                      for key, values in samples.items() if values}
            verdict = judge_campaign(
                index, phases["before"]["p95_ms"], phases["after"]["p95_ms"],
                phases.get("existence", {}).get("p95_ms"),
                phases.get("enumeration", {}).get("p95_ms"),
            )
            verdicts.append(verdict)
            campaigns.append({"campaign": index, "environment": environment,
                              "statuses": statuses, "resolver_before": phases["before"],
                              "resolver_after": phases["after"],
                              "existence": phases.get("existence"),
                              "enumeration": phases.get("enumeration"),
                              "resolver_p95_delta_ms": verdict.delta_ms,
                              "failed_gates": list(verdict.failed_gates),
                              "pass": verdict.passed})
            print(json.dumps({"campaign": index, "delta_ms": verdict.delta_ms,
                              "pass": verdict.passed}), flush=True)

        cpu = measure_cpu_diagnostic(before_resolver, after_resolver, primary_query, 500)
        sanity = []
        for entry in contract["sanity_cohort"]["queries"]:
            query = find_query(queries, entry["query_sha256"])
            with after_resolver._connect_readonly() as connection:
                exact = exact_candidate(after_resolver, connection, query)
                samples, statuses = measure_paired(
                    before_resolver, after_resolver, connection, exact, query,
                    SANITY_PAIRS, WARMUPS_PER_VERSION,
                )
            raw.write(json.dumps({"record": "sanity", "query_sha256": entry["query_sha256"],
                                  **samples}) + "\n")
            phases = {key.removesuffix("_ns"): summarize(values)
                      for key, values in samples.items() if values}
            delta = phases["after"]["p95_ms"] - phases["before"]["p95_ms"]
            sanity.append({**entry, "statuses": statuses, "resolver_before": phases["before"],
                           "resolver_after": phases["after"],
                           "existence": phases.get("existence"),
                           "enumeration": phases.get("enumeration"),
                           "resolver_p95_delta_ms": delta,
                           "within_resolver_delta_threshold": delta <= RESOLVER_DELTA_MAX_MS,
                           "within_existence_threshold": phases.get("existence") is None
                           or phases["existence"]["p95_ms"] <= EXISTENCE_P95_MAX_MS,
                           "within_enumeration_threshold": phases.get("enumeration") is None
                           or phases["enumeration"]["p95_ms"] <= ENUMERATION_P95_MAX_MS})
    finally:
        raw.close()

    index_after = index_stat_fingerprint(args.index_path)
    breaches = [s for s in sanity if not (s["within_resolver_delta_threshold"]
                                          and s["within_existence_threshold"]
                                          and s["within_enumeration_threshold"])]
    decision = adjudicate(verdicts)
    report = {
        "contract": CONTRACT_NAME,
        "contract_sha256": contract_sha,
        "raw_measurements": RAW_NAME,
        "commits": contract["commits"],
        "before_blob_sha256": blob_sha,
        "index_sha256": contract["index"]["sha256"],
        "index_unchanged": index_before == index_after,
        "runtime_tracked_diff_empty": not subprocess.check_output(
            ["git", "status", "--porcelain", "--", "app"], cwd=ROOT, text=True).strip(),
        "primary_gate": contract["primary_gate"],
        "failed_query_sha256": FAILED_QUERY_SHA256,
        "campaigns": campaigns,
        **decision,
        "cpu_time_diagnostic_ms": cpu,
        "sanity_cohort": {
            "queries": len(sanity),
            "pairs_per_query": SANITY_PAIRS,
            "threshold_breaches": [s["query_sha256"] for s in breaches],
            "systematic_regression_found": bool(breaches),
            "worst_resolver_p95_delta_ms": max(s["resolver_p95_delta_ms"] for s in sanity),
            "diagnostic_only": True,
            "results": sanity,
        },
        "environment": {"start": started, "end": environment_metadata("run_end")},
        "scope": "Warm local cache, single machine, read-only immutable index. "
                 "Re-adjudicates reproducibility of one frozen observation; it does not "
                 "replace after_benchmark.json and does not relax any threshold.",
    }
    (args.evidence_dir / REPORT_NAME).write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps({"performance_adjudication": decision["performance_adjudication"],
                      "fail_campaigns": decision["fail_campaigns"],
                      "index_unchanged": report["index_unchanged"],
                      "systematic_regression_found":
                      report["sanity_cohort"]["systematic_regression_found"]}))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index-path", required=True, type=Path)
    parser.add_argument("--evidence-dir", required=True, type=Path)
    parser.add_argument("--phase", choices=["contract", "run"], required=True)
    parser.add_argument("--contract-sha256")
    args = parser.parse_args()
    return write_contract(args) if args.phase == "contract" else run_campaigns(args)


if __name__ == "__main__":
    raise SystemExit(main())
