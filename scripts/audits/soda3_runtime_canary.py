#!/usr/bin/env python3
"""Capture real SECOPPAL Socrata calls once, then replay the frozen manifest A/B.

This is an experimental audit harness. It does not change the production client,
builder, feature flags, or request routing.

Capture:
    python scripts/audits/soda3_runtime_canary.py capture \
      --env-file .env --output /tmp/soda3-runtime-manifest.json

Replay:
    python scripts/audits/soda3_runtime_canary.py replay \
      --env-file .env --manifest /tmp/soda3-runtime-manifest.json \
      --output /tmp/soda3-runtime-replay.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import statistics
import subprocess
import sys
import threading
import time
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from app.core.secop_client import SecopClient  # noqa: E402
from scripts.audits.soda3_probe import (  # noqa: E402
    ProbeCase,
    _or_branch_count,
    _percentile,
    _public_record,
    _sha256_bytes,
    _write_event,
    compare_pair,
    execute_arm,
)


DEFAULT_CORPUS = ROOT / "tests" / "fixtures" / "soda3_runtime_canary_queries.json"
ALLOWED_OPERATIONS = frozenset({"query", "count", "aggregate"})
ALLOWED_DATASETS = frozenset({"p6dx-8zbt", "jbjy-vk9h"})
CRITICAL_DIVERGENCES = frozenset(
    {"ordering_only", "missing_rows", "extra_rows", "serialization", "type_conversion", "unknown"}
)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _git(*args: str) -> str | None:
    result = subprocess.run(
        ["git", *args], cwd=ROOT, check=False, capture_output=True, text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _json_hash(value: Any) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _error_class(exc: BaseException) -> str:
    current = exc
    seen: set[int] = set()
    while (current.__cause__ or current.__context__) and id(current) not in seen:
        seen.add(id(current))
        current = current.__cause__ or current.__context__  # type: ignore[assignment]
    return type(current).__name__


def load_corpus(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    corpus = json.loads(raw)
    if corpus.get("schema_version") != 1:
        raise ValueError("unsupported corpus schema_version")
    cases = corpus.get("cases")
    if not isinstance(cases, list) or not 20 <= len(cases) <= 30:
        raise ValueError("runtime corpus must contain 20 to 30 human-query cases")
    ids = [str(case.get("case_id")) for case in cases]
    if len(ids) != len(set(ids)):
        raise ValueError("runtime corpus contains duplicate case_id values")
    empty_controls = [case for case in cases if case.get("expected_empty") is True]
    if not 2 <= len(empty_controls) <= 3:
        raise ValueError("runtime corpus requires 2 to 3 deliberate expected_empty controls")
    if corpus.get("campaign", {}).get("orderingSpecifier") != "total":
        raise ValueError("runtime campaign requires orderingSpecifier=total")
    if corpus.get("campaign", {}).get("includeSynthetic") is not False:
        raise ValueError("runtime campaign requires includeSynthetic=false")
    for case in cases:
        if not all(case.get(key) not in (None, "") for key in ("case_id", "category", "human_query", "source")):
            raise ValueError(f"invalid corpus case: {case!r}")
    return corpus, hashlib.sha256(raw).hexdigest()


class CaptureLedger:
    """Thread-safe per-run ledger shared by derived recording clients."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_case: dict[str, Any] | None = None
        self._next_order = 0
        self.operations: list[dict[str, Any]] = []

    def begin_case(self, case: dict[str, Any]) -> None:
        with self._lock:
            if self._active_case is not None:
                raise RuntimeError("capture cases cannot overlap")
            self._active_case = dict(case)
            self._next_order = 0

    def reserve(
        self, operation: str, dataset_id: str, soql: str, timeout_seconds: float,
    ) -> dict[str, Any]:
        with self._lock:
            if self._active_case is None:
                raise RuntimeError("remote operation occurred outside an active capture case")
            self._next_order += 1
            case = self._active_case
            record = {
                "call_id": f"{case['case_id']}__call_{self._next_order:03d}",
                "human_case_id": case["case_id"],
                "human_query": case["human_query"],
                "category": case["category"],
                "source": case["source"],
                "expected_empty": bool(case.get("expected_empty")),
                "call_order": self._next_order,
                "operation": operation,
                "dataset_id": dataset_id,
                "exact_soql": soql,
                "soql_sha256": _sha256_bytes(soql.encode("utf-8")),
                "soql_length": len(soql),
                "or_branch_count": _or_branch_count(soql),
                "captured_timeout_seconds": timeout_seconds,
                "capture_status": "started",
                "capture_error_class": None,
                "capture_elapsed_ms": None,
                "capture_row_count": None,
                "capture_nonempty": None,
                "runtime_route": None,
            }
            self.operations.append(record)
            return record

    def finish(
        self,
        record: dict[str, Any],
        *,
        started: float,
        result: Any = None,
        error: BaseException | None = None,
    ) -> None:
        with self._lock:
            record["capture_elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
            if error is not None:
                record["capture_status"] = "error"
                record["capture_error_class"] = _error_class(error)
                return
            record["capture_status"] = "ok"
            if isinstance(result, list):
                record["capture_row_count"] = len(result)
                record["capture_nonempty"] = bool(result)
            elif isinstance(result, int):
                record["capture_row_count"] = 1
                record["capture_nonempty"] = result > 0
                record["capture_count_value"] = result
            else:
                record["capture_row_count"] = None
                record["capture_nonempty"] = None

    def end_case(self, route: str | None) -> list[dict[str, Any]]:
        with self._lock:
            if self._active_case is None:
                raise RuntimeError("no active capture case")
            case_id = self._active_case["case_id"]
            records = [item for item in self.operations if item["human_case_id"] == case_id]
            for record in records:
                record["runtime_route"] = route
            self._active_case = None
            return records


class RecordingSecopClient(SecopClient):
    """Experimental decorator that remains ``isinstance(..., SecopClient)``.

    The subclass shape matters: runtime budget helpers derive bounded clients
    only for real SecopClient instances. Every derived client is wrapped again,
    so analytics and Actor Profile cannot escape capture.
    """

    def __init__(self, delegate: SecopClient, ledger: CaptureLedger) -> None:
        self._delegate = delegate
        self._ledger = ledger
        self.domain = delegate.domain
        self.app_token = delegate.app_token
        self.timeout = delegate.timeout
        self.max_retries = delegate.max_retries
        self.client = delegate.client
        self._injected_client = delegate._injected_client

    def _record(self, operation: str, dataset_id: str, soql: str) -> Any:
        record = self._ledger.reserve(operation, dataset_id, soql, float(self.timeout))
        started = time.perf_counter()
        try:
            result = getattr(self._delegate, operation)(dataset_id, soql)
        except BaseException as exc:
            self._ledger.finish(record, started=started, error=exc)
            raise
        self._ledger.finish(record, started=started, result=result)
        return result

    def query(self, dataset_id: str, soql_query: str) -> list[dict]:
        return self._record("query", dataset_id, soql_query)

    def count(self, dataset_id: str, soql_count_query: str) -> int:
        return self._record("count", dataset_id, soql_count_query)

    def aggregate(self, dataset_id: str, soql: str) -> list[dict]:
        return self._record("aggregate", dataset_id, soql)

    def derived_for_timeout(self, timeout_s: int) -> "RecordingSecopClient":
        return RecordingSecopClient(self._delegate.derived_for_timeout(timeout_s), self._ledger)

    def with_timeout(self, timeout: int) -> "RecordingSecopClient":
        return RecordingSecopClient(self._delegate.with_timeout(timeout), self._ledger)


def _capture(args: argparse.Namespace) -> int:
    corpus, corpus_hash = load_corpus(args.corpus)
    requested_case_ids = set(args.case_id or [])
    known_case_ids = {case["case_id"] for case in corpus["cases"]}
    unknown_case_ids = requested_case_ids - known_case_ids
    if unknown_case_ids:
        raise SystemExit(f"unknown --case-id values: {sorted(unknown_case_ids)}")
    selected_cases = [
        case for case in corpus["cases"]
        if not requested_case_ids or case["case_id"] in requested_case_ids
    ]
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite existing output: {args.output}")
    if args.env_file:
        load_dotenv(args.env_file, override=False)
    os.environ["SECOPPAL_ACTOR_PROFILE_ENABLED"] = "true"
    if args.actor_index:
        os.environ["SECOP_ACTOR_ALIAS_INDEX_PATH"] = str(args.actor_index.resolve())

    from app.config import Settings, get_settings
    from app.core.conversation_store import ConversationStore
    from app.core.orchestrator import SecopalWorkflow
    from scripts.audits.audit_isolation import isolated_runtime_stores

    ledger = CaptureLedger()
    case_results: list[dict[str, Any]] = []
    reference_date = date.fromisoformat(corpus["reference_date"])
    started_at = _now()

    with isolated_runtime_stores(prefix="soda3_runtime_canary_") as stores:
        get_settings.cache_clear()
        settings = Settings(**stores.settings_overrides)
        workflow = SecopalWorkflow(settings)
        workflow.secop_client = RecordingSecopClient(workflow.secop_client, ledger)
        workflow.conv_store = ConversationStore(base_path=stores.conversation_dir)

        for case in selected_cases:
            ledger.begin_case(case)
            started = time.perf_counter()
            try:
                result = workflow.run_query(
                    case["human_query"],
                    channel="streamlit",
                    chat_id=f"soda3-runtime-{case['case_id']}",
                    today=reference_date,
                )
                runtime_status = "ok"
                runtime_error = None
                route = result.get("route_reason")
                dataset_id = result.get("dataset_id")
                needs_clarification = bool(result.get("needs_clarification"))
            except BaseException as exc:
                runtime_status = "error"
                runtime_error = _error_class(exc)
                route = None
                dataset_id = None
                needs_clarification = None
            records = ledger.end_case(route)
            case_results.append(
                {
                    **case,
                    "runtime_status": runtime_status,
                    "runtime_error_class": runtime_error,
                    "runtime_route": route,
                    "runtime_dataset_id": dataset_id,
                    "needs_clarification": needs_clarification,
                    "runtime_elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
                    "captured_operation_count": len(records),
                    "captured_success_count": sum(item["capture_status"] == "ok" for item in records),
                }
            )

    classified = sum(item["operation"] in ALLOWED_OPERATIONS for item in ledger.operations)
    manifest = {
        "schema_version": 1,
        "manifest_kind": "SODA3_RUNTIME_CANARY_V1",
        "captured_at_utc": started_at,
        "finished_at_utc": _now(),
        "git_head": _git("rev-parse", "HEAD"),
        "git_branch": _git("branch", "--show-current"),
        "corpus_path": str(args.corpus.resolve()),
        "corpus_sha256": corpus_hash,
        "reference_date": corpus["reference_date"],
        "campaign": corpus["campaign"],
        "capture_case_filter": sorted(requested_case_ids) or None,
        "app_token_present": bool(os.getenv("SECOP_APP_TOKEN")),
        "actor_profile_enabled_for_capture": True,
        "capture_coverage": {
            "remote_operations": len(ledger.operations),
            "classified_operations": classified,
            "unclassified_operations": len(ledger.operations) - classified,
            "coverage_pct": round(classified / len(ledger.operations) * 100, 3)
            if ledger.operations else 0.0,
            "operation_counts": dict(sorted(Counter(item["operation"] for item in ledger.operations).items())),
        },
        "cases": case_results,
        "operations": ledger.operations,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8",
    )
    print(json.dumps({
        "output": str(args.output.resolve()),
        "manifest_sha256": _json_hash(manifest),
        "cases": len(case_results),
        "runtime_errors": sum(case["runtime_status"] != "ok" for case in case_results),
        "cases_without_operations": sum(case["captured_operation_count"] == 0 for case in case_results),
        **manifest["capture_coverage"],
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _id_field(operation: dict[str, Any]) -> str | None:
    soql = operation["exact_soql"].lower()
    field = "id_del_proceso" if operation["dataset_id"] == "p6dx-8zbt" else "id_contrato"
    projection = soql.split(" where ", 1)[0].split(" order by ", 1)[0]
    return field if field in projection else None


def _admission(
    manifest: dict[str, Any], executions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    results = []
    for case in manifest["cases"]:
        case_exec = [item for item in executions if item["human_case_id"] == case["case_id"]]
        successful = [item for item in case_exec if item["status"] == "ok"]
        expected_empty = bool(case["expected_empty"])
        has_nonempty = any((item["row_count"] or 0) > 0 for item in successful)
        all_empty = bool(successful) and all((item["row_count"] or 0) == 0 for item in successful)
        passed = (
            case["captured_operation_count"] > 0
            and (all_empty if expected_empty else has_nonempty)
        )
        results.append({
            "case_id": case["case_id"],
            "category": case["category"],
            "expected_empty": expected_empty,
            "captured_operation_count": case["captured_operation_count"],
            "successful_replay_executions": len(successful),
            "observed_nonempty": has_nonempty,
            "observed_all_empty": all_empty,
            "admitted": passed,
        })
    return results


def _replay_summary(
    manifest: dict[str, Any],
    executions: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
    admissions: list[dict[str, Any]],
) -> dict[str, Any]:
    divergence_counts = Counter(div for pair in pairs for div in pair["divergences"])
    both_success = [pair for pair in pairs if pair["classic_status"] == pair["soda3_status"] == "ok"]
    asymmetric = [
        pair for pair in pairs
        if (pair["classic_status"] == "ok") != (pair["soda3_status"] == "ok")
    ]
    datasets: dict[str, Any] = {}
    p90_gate = True
    for dataset in sorted(ALLOWED_DATASETS):
        eligible = [
            pair for pair in pairs
            if pair["dataset"] == dataset and pair["performance_eligible"]
        ]
        classic = [float(pair["classic_elapsed_ms"]) for pair in eligible]
        soda3 = [float(pair["soda3_elapsed_ms"]) for pair in eligible]
        classic_p90 = _percentile(classic, 0.90)
        soda3_p90 = _percentile(soda3, 0.90)
        improvement = (
            round((classic_p90 - soda3_p90) / classic_p90 * 100, 3)
            if classic_p90 and soda3_p90 else None
        )
        passed = improvement is not None and improvement >= 25.0
        p90_gate = p90_gate and passed
        datasets[dataset] = {
            "performance_eligible_pairs": len(eligible),
            "classic_p50_ms": _percentile(classic, 0.50),
            "classic_p90_ms": classic_p90,
            "classic_p95_ms": _percentile(classic, 0.95),
            "soda3_p50_ms": _percentile(soda3, 0.50),
            "soda3_p90_ms": soda3_p90,
            "soda3_p95_ms": _percentile(soda3, 0.95),
            "p90_improvement_pct": improvement,
            "gate_p90_25pct": passed,
        }

    classic_timeouts = sum(item["status"] == "timeout" for item in executions if item["transport"] == "classic")
    soda3_timeouts = sum(item["status"] == "timeout" for item in executions if item["transport"] == "soda3")
    critical = sum(divergence_counts.get(name, 0) for name in CRITICAL_DIVERGENCES)
    gates = {
        "identical_soql_100pct": all(pair["soql_equal"] for pair in pairs),
        "captured_remote_operations_100pct": (
            manifest["capture_coverage"]["coverage_pct"] == 100.0
            and manifest["capture_coverage"]["unclassified_operations"] == 0
        ),
        "successful_response_parity_100pct": (
            bool(both_success) and all(not pair["divergences"] for pair in both_success)
        ),
        "critical_divergences_zero": critical == 0,
        "p90_improvement_25pct_each_dataset": p90_gate,
        "soda3_timeouts_not_greater": soda3_timeouts <= classic_timeouts,
        "asymmetric_failures_zero": not asymmetric,
        "nonvacuity_admission": all(item["admitted"] for item in admissions),
    }
    return {
        "pairs": len(pairs),
        "both_success_pairs": len(both_success),
        "strict_parity_pairs": sum(not pair["divergences"] for pair in both_success),
        "asymmetric_failure_pairs": len(asymmetric),
        "classic_timeouts": classic_timeouts,
        "soda3_timeouts": soda3_timeouts,
        "divergences": dict(sorted(divergence_counts.items())),
        "datasets": datasets,
        "gates": gates,
        "all_gates_pass": all(gates.values()),
        "verdict": "SODA3_QUERY_DISCOVERY_CLOSED" if all(gates.values()) else "SODA3_QUERY_DISCOVERY_OPEN",
    }


def _replay(args: argparse.Namespace) -> int:
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    operations = manifest.get("operations") or []
    if not operations:
        raise SystemExit("manifest contains no remote operations")
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite existing output: {args.output}")
    if args.env_file:
        load_dotenv(args.env_file, override=False)
    token = os.getenv("SECOP_APP_TOKEN")
    if not token:
        raise SystemExit("SECOP_APP_TOKEN is required for the runtime canary replay")

    rounds = int(manifest["campaign"]["rounds"])
    seed = int(manifest["campaign"]["seed"])
    domain = os.getenv("DATOS_GOV_DOMAIN", "www.datos.gov.co")
    rng = random.Random(seed)
    first_arm = {item["call_id"]: rng.choice(("classic", "soda3")) for item in operations}
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "SECOPPAL-SODA3-runtime-canary-v1",
        "X-App-Token": token,
    })
    executions: list[dict[str, Any]] = []
    pairs: list[dict[str, Any]] = []
    execution_index = 0
    args.output.parent.mkdir(parents=True, exist_ok=True)

    with args.output.open("x", encoding="utf-8") as handle:
        _write_event(handle, "campaign_metadata", {
            "started_at_utc": _now(),
            "manifest": str(args.manifest.resolve()),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "git_head": _git("rev-parse", "HEAD"),
            "git_branch": _git("branch", "--show-current"),
            "operations": len(operations),
            "rounds": rounds,
            "orderingSpecifier": "total",
            "includeSynthetic": False,
            "app_token_present": True,
        })
        for round_number in range(1, rounds + 1):
            round_operations = list(operations)
            rng.shuffle(round_operations)
            for operation in round_operations:
                start = first_arm[operation["call_id"]]
                if round_number % 2 == 0:
                    start = "soda3" if start == "classic" else "classic"
                order = (start, "soda3" if start == "classic" else "classic")
                timeout_seconds = max(1, int(math.ceil(operation["captured_timeout_seconds"])))
                probe = ProbeCase(
                    case_id=operation["call_id"],
                    family=operation["operation"],
                    dataset=operation["dataset_id"],
                    id_field=_id_field(operation),
                    soql=operation["exact_soql"],
                    or_branch_count=operation["or_branch_count"],
                )
                pair_results: dict[str, dict[str, Any]] = {}
                for position, transport in enumerate(order, start=1):
                    execution_index += 1
                    result = execute_arm(
                        session,
                        domain=domain,
                        case=probe,
                        transport=transport,
                        timeout_seconds=timeout_seconds,
                        round_number=round_number,
                        pair_position=position,
                        execution_index=execution_index,
                    )
                    result.update({
                        "human_case_id": operation["human_case_id"],
                        "human_query": operation["human_query"],
                        "category": operation["category"],
                        "operation": operation["operation"],
                        "call_order": operation["call_order"],
                        "expected_empty": operation["expected_empty"],
                        "runtime_route": operation["runtime_route"],
                        "captured_timeout_seconds": operation["captured_timeout_seconds"],
                    })
                    executions.append(result)
                    pair_results[transport] = result
                    _write_event(handle, "execution", _public_record(result))
                pair = compare_pair(pair_results["classic"], pair_results["soda3"])
                pair.update({
                    "call_id": operation["call_id"],
                    "human_case_id": operation["human_case_id"],
                    "category": operation["category"],
                    "operation": operation["operation"],
                    "call_order": operation["call_order"],
                    "expected_empty": operation["expected_empty"],
                    "runtime_route": operation["runtime_route"],
                    "captured_timeout_seconds": operation["captured_timeout_seconds"],
                })
                pairs.append(pair)

        admissions = _admission(manifest, executions)
        for pair in pairs:
            _write_event(handle, "pair_comparison", pair)
        for item in admissions:
            _write_event(handle, "case_admission", item)
        summary = _replay_summary(manifest, executions, pairs, admissions)
        summary.update({
            "finished_at_utc": _now(),
            "output": str(args.output.resolve()),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        })
        _write_event(handle, "campaign_summary", summary)
    session.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate-corpus")
    validate.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)

    capture = sub.add_parser("capture")
    capture.add_argument("--corpus", type=Path, default=DEFAULT_CORPUS)
    capture.add_argument("--env-file", type=Path)
    capture.add_argument("--actor-index", type=Path)
    capture.add_argument("--case-id", action="append")
    capture.add_argument("--output", type=Path, required=True)

    replay = sub.add_parser("replay")
    replay.add_argument("--env-file", type=Path)
    replay.add_argument("--manifest", type=Path, required=True)
    replay.add_argument("--output", type=Path, required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "validate-corpus":
        corpus, corpus_hash = load_corpus(args.corpus)
        print(json.dumps({
            "status": "valid",
            "cases": len(corpus["cases"]),
            "expected_empty": sum(bool(case["expected_empty"]) for case in corpus["cases"]),
            "corpus_sha256": corpus_hash,
        }, indent=2, sort_keys=True))
        return 0
    if args.command == "capture":
        return _capture(args)
    return _replay(args)


if __name__ == "__main__":
    raise SystemExit(main())
