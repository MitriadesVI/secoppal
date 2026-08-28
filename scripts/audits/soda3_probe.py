#!/usr/bin/env python3
"""A/B discovery harness for classic SODA and SODA3 SECOP queries.

This is deliberately a measurement spike, not a production transport. It sends
the exact same materialized SoQL to both arms, records one JSONL event per
execution, and compares canonical result sets before computing paired latency.

Example:
    .venv/bin/python scripts/audits/soda3_probe.py \
        --env-file .env --output /tmp/soda3-discovery.jsonl
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import random
import re
import statistics
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, TextIO

import requests
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURE = ROOT / "tests" / "fixtures" / "soda3_probe_cases.json"
ALLOWED_DATASETS = frozenset({"p6dx-8zbt", "jbjy-vk9h"})
REQUIRED_FAMILIES = frozenset(
    {"simple", "date", "count", "unaccent", "or_5", "or_20", "or_50", "or_100", "combined"}
)
COMPLEX_FAMILIES = frozenset({"or_20", "or_50", "or_100", "combined"})
TRANSPORTS = ("classic", "soda3")
NUMERIC_TEXT = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
OR_TOKEN = re.compile(r"\bOR\b", flags=re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class ProbeCase:
    case_id: str
    family: str
    dataset: str
    id_field: str | None
    soql: str
    or_branch_count: int


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _decimal_text(value: Decimal) -> str:
    if value == 0:
        return "0"
    normalized = value.normalize()
    rendered = format(normalized, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered


def _semantic_value(value: Any) -> Any:
    """Normalize JSON scalar type drift without changing text semantics broadly."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        try:
            return {"$number": _decimal_text(Decimal(str(value)))}
        except InvalidOperation:
            return value
    if isinstance(value, str) and NUMERIC_TEXT.fullmatch(value):
        try:
            return {"$number": _decimal_text(Decimal(value))}
        except InvalidOperation:
            return value
    if isinstance(value, list):
        return [_semantic_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _semantic_value(item) for key, item in value.items()}
    return value


def _row_set_hash(rows: list[dict[str, Any]], *, semantic: bool) -> str:
    normalized = [_semantic_value(row) if semantic else row for row in rows]
    encoded_rows = sorted(_json_bytes(row) for row in normalized)
    return _sha256_bytes(b"[" + b",".join(encoded_rows) + b"]")


def _ordered_hash(rows: list[dict[str, Any]], *, semantic: bool) -> str:
    normalized = [_semantic_value(row) if semantic else row for row in rows]
    return _sha256_bytes(_json_bytes(normalized))


def _shape_signature(rows: list[dict[str, Any]]) -> list[list[str]]:
    return sorted({tuple(sorted(str(key) for key in row)) for row in rows})


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    if isinstance(value, (int, float)):
        return "number"
    return type(value).__name__


def _type_signature(rows: list[dict[str, Any]]) -> dict[str, list[str]]:
    observed: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        for key, value in row.items():
            observed[str(key)].add(_type_name(value))
    return {key: sorted(values) for key, values in sorted(observed.items())}


def _canonical_ids(rows: list[dict[str, Any]], id_field: str | None) -> tuple[list[str], list[str]]:
    if id_field is None:
        return [], []
    ordered: list[str] = []
    for row in rows:
        if id_field not in row or row[id_field] is None:
            raise ValueError(f"response row omitted canonical id field {id_field!r}")
        ordered.append(str(row[id_field]))
    return sorted(set(ordered)), ordered


def _or_branch_count(soql: str) -> int:
    operators = len(OR_TOKEN.findall(soql))
    return operators + 1 if operators else 0


def _escape_soql_literal(value: str) -> str:
    return value.replace("'", "''")


def _materialize_or_case(raw: dict[str, Any], terms: list[str]) -> str:
    branch_count = int(raw["or_branch_count"])
    fields = raw.get("or_fields")
    if not isinstance(fields, list) or not fields or not all(isinstance(field, str) for field in fields):
        raise ValueError(f"{raw.get('case_id')}: or_fields must be a non-empty string list")
    if branch_count > len(terms):
        raise ValueError(f"{raw.get('case_id')}: requested {branch_count} OR branches, only {len(terms)} terms")

    # Intentionally preserve term order and duplicates. This spike must not hide
    # builder cost by deduplicating or rewriting OR variants.
    branches = []
    for index, term in enumerate(terms[:branch_count]):
        field = fields[index % len(fields)]
        escaped = _escape_soql_literal(str(term))
        branches.append(
            f"unaccent(UPPER({field})) LIKE unaccent(UPPER('%{escaped}%'))"
        )
    return (
        f"SELECT {raw['or_select']} WHERE ({' OR '.join(branches)}) "
        f"AND {raw['or_date_clause']} ORDER BY {raw['or_order_by']} LIMIT {int(raw['limit'])}"
    )


def load_cases(path: Path) -> tuple[dict[str, Any], list[ProbeCase], str]:
    fixture_bytes = path.read_bytes()
    corpus = json.loads(fixture_bytes)
    if corpus.get("schema_version") != 1:
        raise ValueError("unsupported fixture schema_version")
    raw_cases = corpus.get("cases")
    if not isinstance(raw_cases, list) or not 12 <= len(raw_cases) <= 16:
        raise ValueError("primary corpus must contain 12 to 16 cases")
    terms = corpus.get("or_terms")
    if not isinstance(terms, list) or not all(isinstance(term, str) for term in terms):
        raise ValueError("or_terms must be a string list")

    cases: list[ProbeCase] = []
    seen_ids: set[str] = set()
    for raw in raw_cases:
        case_id = str(raw["case_id"])
        if case_id in seen_ids:
            raise ValueError(f"duplicate case_id: {case_id}")
        seen_ids.add(case_id)
        dataset = str(raw["dataset"])
        if dataset not in ALLOWED_DATASETS:
            raise ValueError(f"{case_id}: dataset not allowed: {dataset}")
        soql = raw.get("soql")
        if soql is None:
            soql = _materialize_or_case(raw, terms)
        if not isinstance(soql, str) or not soql.strip():
            raise ValueError(f"{case_id}: empty SoQL")
        observed_or_count = _or_branch_count(soql)
        expected_or_count = int(raw.get("or_branch_count", observed_or_count))
        if observed_or_count != expected_or_count:
            raise ValueError(
                f"{case_id}: materialized {observed_or_count} OR branches, expected {expected_or_count}"
            )
        cases.append(
            ProbeCase(
                case_id=case_id,
                family=str(raw["family"]),
                dataset=dataset,
                id_field=raw.get("id_field"),
                soql=soql,
                or_branch_count=observed_or_count,
            )
        )

    families = {case.family for case in cases}
    missing_families = sorted(REQUIRED_FAMILIES - families)
    if missing_families:
        raise ValueError(f"corpus missing required families: {missing_families}")
    datasets = {case.dataset for case in cases}
    if datasets != ALLOWED_DATASETS:
        raise ValueError(f"corpus datasets must be exactly {sorted(ALLOWED_DATASETS)}")

    campaign = corpus.get("campaign") or {}
    if campaign.get("orderingSpecifier") != "total":
        raise ValueError("primary campaign requires orderingSpecifier=total")
    if campaign.get("includeSynthetic") is not False:
        raise ValueError("primary campaign requires includeSynthetic=false")
    return corpus, cases, _sha256_bytes(fixture_bytes)


def _git_value(*args: str) -> str | None:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _public_record(record: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in record.items() if not key.startswith("_")}


def _write_event(handle: TextIO, event_type: str, payload: dict[str, Any]) -> None:
    handle.write(
        json.dumps(
            {"event_type": event_type, **payload},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    )
    handle.flush()


def execute_arm(
    session: requests.Session,
    *,
    domain: str,
    case: ProbeCase,
    transport: str,
    timeout_seconds: int,
    round_number: int,
    pair_position: int,
    execution_index: int,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "timestamp_utc": _utc_now(),
        "execution_index": execution_index,
        "round": round_number,
        "pair_position": pair_position,
        "dataset": case.dataset,
        "case_id": case.case_id,
        "family": case.family,
        "transport": transport,
        "exact_soql": case.soql,
        "soql_sha256": _sha256_bytes(case.soql.encode("utf-8")),
        "soql_length": len(case.soql),
        "or_branch_count": case.or_branch_count,
        "elapsed_ms": None,
        "status": None,
        "http_status": None,
        "error_class": None,
        "response_bytes": 0,
        "row_count": None,
        "canonical_ids": [],
        "ordered_ids": [],
        "canonical_hash": None,
        "raw_hash": None,
        "content_type": None,
    }
    started = time.perf_counter()
    try:
        if transport == "classic":
            response = session.get(
                f"https://{domain}/resource/{case.dataset}.json",
                params={"$query": case.soql},
                timeout=timeout_seconds,
            )
        elif transport == "soda3":
            response = session.post(
                f"https://{domain}/api/v3/views/{case.dataset}/query.json",
                json={
                    "query": case.soql,
                    "timeout": timeout_seconds,
                    "includeSynthetic": False,
                    "orderingSpecifier": "total",
                },
                timeout=timeout_seconds,
            )
        else:  # pragma: no cover - guarded by scheduler
            raise ValueError(f"unknown transport: {transport}")
    except requests.Timeout as exc:
        base["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        base["status"] = "timeout"
        base["error_class"] = type(exc).__name__
        return base
    except requests.RequestException as exc:
        base["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
        base["status"] = "network_error"
        base["error_class"] = type(exc).__name__
        return base

    base["elapsed_ms"] = round((time.perf_counter() - started) * 1000, 3)
    base["http_status"] = response.status_code
    base["response_bytes"] = len(response.content)
    base["content_type"] = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if response.status_code not in (200, 202):
        base["status"] = "http_error"
        base["error_class"] = "HTTPError"
        return base

    try:
        decoded = response.json()
    except (ValueError, requests.JSONDecodeError) as exc:
        base["status"] = "decode_error"
        base["error_class"] = type(exc).__name__
        return base
    if not isinstance(decoded, list) or not all(isinstance(row, dict) for row in decoded):
        base["status"] = "contract_error"
        base["error_class"] = "UnexpectedResponseShape"
        return base

    rows: list[dict[str, Any]] = decoded
    try:
        canonical_ids, ordered_ids = _canonical_ids(rows, case.id_field)
    except ValueError:
        base["status"] = "contract_error"
        base["error_class"] = "MissingCanonicalId"
        base["row_count"] = len(rows)
        return base

    base.update(
        {
            "status": "ok",
            "row_count": len(rows),
            "canonical_ids": canonical_ids,
            "ordered_ids": ordered_ids,
            "canonical_hash": _row_set_hash(rows, semantic=True),
            "raw_hash": _row_set_hash(rows, semantic=False),
            "_ordered_canonical_hash": _ordered_hash(rows, semantic=True),
            "_shape_signature": _shape_signature(rows),
            "_type_signature": _type_signature(rows),
        }
    )
    return base


def compare_pair(classic: dict[str, Any], soda3: dict[str, Any]) -> dict[str, Any]:
    divergences: list[str] = []
    soql_equal = classic["exact_soql"] == soda3["exact_soql"]
    both_ok = classic["status"] == soda3["status"] == "ok"
    id_set_parity: bool | None = None

    if not soql_equal:
        divergences.append("unknown")
    elif both_ok:
        classic_ids = set(classic["canonical_ids"])
        soda3_ids = set(soda3["canonical_ids"])
        has_ids = bool(classic_ids or soda3_ids or classic["ordered_ids"] or soda3["ordered_ids"])
        if has_ids:
            missing = classic_ids - soda3_ids
            extra = soda3_ids - classic_ids
            id_set_parity = not missing and not extra
            if missing:
                divergences.append("missing_rows")
            if extra:
                divergences.append("extra_rows")

        if not divergences:
            if classic["canonical_hash"] == soda3["canonical_hash"]:
                if classic["raw_hash"] != soda3["raw_hash"]:
                    divergences.append("type_conversion")
                elif classic.get("_ordered_canonical_hash") != soda3.get("_ordered_canonical_hash"):
                    divergences.append("ordering_only")
            elif classic.get("_shape_signature") != soda3.get("_shape_signature"):
                divergences.append("serialization")
            elif classic.get("_type_signature") != soda3.get("_type_signature"):
                divergences.append("type_conversion")
            else:
                divergences.append("unknown")
    else:
        statuses = {classic["status"], soda3["status"]}
        if statuses & {"decode_error", "contract_error"}:
            divergences.append("serialization")
        elif statuses & {"timeout", "network_error"}:
            divergences.append("remote_variance")
        elif any(
            result["status"] == "http_error" and (result["http_status"] or 0) >= 500
            for result in (classic, soda3)
        ):
            divergences.append("remote_variance")
        else:
            divergences.append("unknown")

    performance_eligible = soql_equal and both_ok and all(
        divergence in {"ordering_only"} for divergence in divergences
    )
    classic_ms = classic["elapsed_ms"]
    soda3_ms = soda3["elapsed_ms"]
    improvement = None
    if performance_eligible and classic_ms and soda3_ms:
        improvement = round((classic_ms - soda3_ms) / classic_ms * 100, 3)

    return {
        "case_id": classic["case_id"],
        "family": classic["family"],
        "dataset": classic["dataset"],
        "round": classic["round"],
        "soql_equal": soql_equal,
        "id_set_parity": id_set_parity,
        "classic_status": classic["status"],
        "soda3_status": soda3["status"],
        "classic_http_status": classic["http_status"],
        "soda3_http_status": soda3["http_status"],
        "classic_elapsed_ms": classic_ms,
        "soda3_elapsed_ms": soda3_ms,
        "soda3_improvement_pct": improvement,
        "performance_eligible": performance_eligible,
        "divergences": divergences,
    }


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def _latency_summary(executions: list[dict[str, Any]]) -> dict[str, float | int | None]:
    values = [float(item["elapsed_ms"]) for item in executions if item["status"] == "ok"]
    return {
        "successful": len(values),
        "p50_ms": _percentile(values, 0.50),
        "p90_ms": _percentile(values, 0.90),
        "p95_ms": _percentile(values, 0.95),
    }


def _status_summary(executions: list[dict[str, Any]]) -> dict[str, int]:
    return dict(sorted(Counter(str(item["status"]) for item in executions).items()))


def _pair_improvements(pairs: list[dict[str, Any]]) -> list[float]:
    return [
        float(pair["soda3_improvement_pct"])
        for pair in pairs
        if pair["performance_eligible"] and pair["soda3_improvement_pct"] is not None
    ]


def summarize_case(
    case: ProbeCase,
    executions: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    case_exec = [item for item in executions if item["case_id"] == case.case_id]
    case_pairs = [item for item in pairs if item["case_id"] == case.case_id]
    divergence_counts = Counter(
        divergence for pair in case_pairs for divergence in pair["divergences"]
    )
    improvements = _pair_improvements(case_pairs)
    return {
        "case_id": case.case_id,
        "family": case.family,
        "dataset": case.dataset,
        "soql_length": len(case.soql),
        "or_branch_count": case.or_branch_count,
        "classic_latency": _latency_summary(
            [item for item in case_exec if item["transport"] == "classic"]
        ),
        "soda3_latency": _latency_summary(
            [item for item in case_exec if item["transport"] == "soda3"]
        ),
        "classic_statuses": _status_summary(
            [item for item in case_exec if item["transport"] == "classic"]
        ),
        "soda3_statuses": _status_summary(
            [item for item in case_exec if item["transport"] == "soda3"]
        ),
        "id_set_parity_rounds": sum(pair["id_set_parity"] is True for pair in case_pairs),
        "strict_parity_rounds": sum(not pair["divergences"] for pair in case_pairs),
        "performance_eligible_rounds": sum(pair["performance_eligible"] for pair in case_pairs),
        "median_paired_soda3_improvement_pct": (
            round(statistics.median(improvements), 3) if improvements else None
        ),
        "divergences": dict(sorted(divergence_counts.items())),
    }


def _slice_summary(
    executions: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    classic = [item for item in executions if item["transport"] == "classic"]
    soda3 = [item for item in executions if item["transport"] == "soda3"]
    divergence_counts = Counter(
        divergence for pair in pairs for divergence in pair["divergences"]
    )
    improvements = _pair_improvements(pairs)
    return {
        "executions": len(executions),
        "pairs": len(pairs),
        "classic_latency": _latency_summary(classic),
        "soda3_latency": _latency_summary(soda3),
        "classic_statuses": _status_summary(classic),
        "soda3_statuses": _status_summary(soda3),
        "id_set_parity_pairs": sum(pair["id_set_parity"] is True for pair in pairs),
        "strict_parity_pairs": sum(not pair["divergences"] for pair in pairs),
        "performance_eligible_pairs": sum(pair["performance_eligible"] for pair in pairs),
        "median_paired_soda3_improvement_pct": (
            round(statistics.median(improvements), 3) if improvements else None
        ),
        "divergences": dict(sorted(divergence_counts.items())),
    }


def _derive_verdict(summary: dict[str, Any]) -> str:
    total_pairs = int(summary["pairs"])
    comparable = int(summary["performance_eligible_pairs"])
    divergence_counts = summary["divergences"]
    blocking = sum(
        int(divergence_counts.get(kind, 0))
        for kind in ("missing_rows", "extra_rows", "serialization", "type_conversion", "unknown")
    )
    classic_statuses = summary["classic_statuses"]
    soda3_statuses = summary["soda3_statuses"]
    classic_network_failures = sum(classic_statuses.get(kind, 0) for kind in ("timeout", "network_error"))
    soda3_network_failures = sum(soda3_statuses.get(kind, 0) for kind in ("timeout", "network_error"))
    if total_pairs and (
        classic_network_failures > total_pairs * 0.25
        or soda3_network_failures > total_pairs * 0.25
    ):
        return "INCONCLUSIVE_NETWORK_VARIANCE"
    if blocking:
        return "NO_GO_DIVERGENCE"
    if total_pairs == 0 or comparable < total_pairs * 0.80:
        return "INCONCLUSIVE_NETWORK_VARIANCE"

    overall_gain = summary.get("median_paired_soda3_improvement_pct")
    complex_gain = summary.get("complex_queries", {}).get("median_paired_soda3_improvement_pct")
    if (
        overall_gain is not None
        and complex_gain is not None
        and overall_gain >= 15.0
        and complex_gain >= 15.0
        and soda3_network_failures <= classic_network_failures
    ):
        return "GO_SODA3_QUERY"
    if (
        complex_gain is not None
        and complex_gain >= 20.0
        and soda3_network_failures <= classic_network_failures
    ) or soda3_network_failures + 1 < classic_network_failures:
        return "GO_SODA3_PARTIAL"
    return "NO_MATERIAL_GAIN"


def campaign_summary(
    cases: list[ProbeCase],
    executions: list[dict[str, Any]],
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    summary = _slice_summary(executions, pairs)
    summary["cases"] = len(cases)
    summary["rounds"] = len({item["round"] for item in executions})
    summary["soql_mismatch_pairs"] = sum(not pair["soql_equal"] for pair in pairs)
    summary["complex_queries"] = _slice_summary(
        [item for item in executions if item["family"] in COMPLEX_FAMILIES],
        [item for item in pairs if item["family"] in COMPLEX_FAMILIES],
    )
    summary["datasets"] = {
        dataset: _slice_summary(
            [item for item in executions if item["dataset"] == dataset],
            [item for item in pairs if item["dataset"] == dataset],
        )
        for dataset in sorted(ALLOWED_DATASETS)
    }
    summary["families"] = {
        family: _slice_summary(
            [item for item in executions if item["family"] == family],
            [item for item in pairs if item["family"] == family],
        )
        for family in sorted({case.family for case in cases})
    }
    summary["verdict"] = _derive_verdict(summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURE)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--rounds", type=int)
    parser.add_argument("--timeout-seconds", type=int)
    parser.add_argument("--seed", type=int)
    parser.add_argument("--case-id", action="append", default=[])
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--allow-unauthenticated", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    corpus, all_cases, corpus_hash = load_cases(args.fixtures)
    campaign = corpus["campaign"]
    rounds = args.rounds if args.rounds is not None else int(campaign["rounds"])
    timeout_seconds = (
        args.timeout_seconds
        if args.timeout_seconds is not None
        else int(campaign["timeout_seconds"])
    )
    seed = args.seed if args.seed is not None else int(campaign["seed"])
    if rounds < 1:
        raise SystemExit("--rounds must be positive")
    if timeout_seconds < 1 or timeout_seconds > 120:
        raise SystemExit("--timeout-seconds must be between 1 and 120")

    selected_ids = set(args.case_id)
    cases = [case for case in all_cases if not selected_ids or case.case_id in selected_ids]
    missing_ids = selected_ids - {case.case_id for case in cases}
    if missing_ids:
        raise SystemExit(f"unknown --case-id values: {sorted(missing_ids)}")

    validation = {
        "status": "valid",
        "fixture": str(args.fixtures.resolve()),
        "fixture_sha256": corpus_hash,
        "corpus_case_count": len(all_cases),
        "selected_case_count": len(cases),
        "datasets": sorted({case.dataset for case in cases}),
        "families": sorted({case.family for case in cases}),
        "max_soql_length": max(len(case.soql) for case in cases),
        "max_or_branch_count": max(case.or_branch_count for case in cases),
        "rounds": rounds,
        "timeout_seconds": timeout_seconds,
        "seed": seed,
        "orderingSpecifier": "total",
        "includeSynthetic": False,
    }
    if args.validate_only:
        print(json.dumps(validation, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.output is None:
        raise SystemExit("--output is required unless --validate-only is used")
    if args.output.exists():
        raise SystemExit(f"refusing to overwrite existing output: {args.output}")

    if args.env_file is not None:
        if not args.env_file.is_file():
            raise SystemExit(f"env file not found: {args.env_file}")
        load_dotenv(args.env_file, override=False)
    app_token = os.getenv("SECOP_APP_TOKEN")
    if not app_token and not args.allow_unauthenticated:
        raise SystemExit(
            "SECOP_APP_TOKEN is required for the primary campaign; use --env-file without logging the token"
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    domain = str(corpus.get("domain") or "www.datos.gov.co")
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "SECOPPAL-SODA3-discovery-v1",
        }
    )
    if app_token:
        session.headers.update({"X-App-Token": app_token})

    executions: list[dict[str, Any]] = []
    pairs: list[dict[str, Any]] = []
    started_at = _utc_now()
    rng = random.Random(seed)
    first_arm = {case.case_id: rng.choice(TRANSPORTS) for case in cases}
    execution_index = 0

    with args.output.open("x", encoding="utf-8") as handle:
        _write_event(
            handle,
            "campaign_metadata",
            {
                **validation,
                "started_at_utc": started_at,
                "domain": domain,
                "git_head": _git_value("rev-parse", "HEAD"),
                "git_branch": _git_value("branch", "--show-current"),
                "python": platform.python_version(),
                "requests": requests.__version__,
                "app_token_present": bool(app_token),
                "classic_contract": {
                    "method": "GET",
                    "path": "/resource/{dataset_id}.json",
                    "query_parameter": "$query",
                    "client_timeout_seconds": timeout_seconds,
                },
                "soda3_contract": {
                    "method": "POST",
                    "path": "/api/v3/views/{dataset_id}/query.json",
                    "timeout": timeout_seconds,
                    "client_timeout_seconds": timeout_seconds,
                    "orderingSpecifier": "total",
                    "includeSynthetic": False,
                },
            },
        )

        for round_number in range(1, rounds + 1):
            round_cases = list(cases)
            rng.shuffle(round_cases)
            for case in round_cases:
                start = first_arm[case.case_id]
                if round_number % 2 == 0:
                    start = "soda3" if start == "classic" else "classic"
                order = (start, "soda3" if start == "classic" else "classic")
                pair_results: dict[str, dict[str, Any]] = {}
                for pair_position, transport in enumerate(order, start=1):
                    execution_index += 1
                    result = execute_arm(
                        session,
                        domain=domain,
                        case=case,
                        transport=transport,
                        timeout_seconds=timeout_seconds,
                        round_number=round_number,
                        pair_position=pair_position,
                        execution_index=execution_index,
                    )
                    executions.append(result)
                    pair_results[transport] = result
                    _write_event(handle, "execution", _public_record(result))
                pair = compare_pair(pair_results["classic"], pair_results["soda3"])
                pairs.append(pair)

        for pair in pairs:
            _write_event(handle, "pair_comparison", pair)
        for case in cases:
            _write_event(handle, "case_summary", summarize_case(case, executions, pairs))
        summary = campaign_summary(cases, executions, pairs)
        summary["started_at_utc"] = started_at
        summary["finished_at_utc"] = _utc_now()
        summary["output"] = str(args.output.resolve())
        _write_event(handle, "campaign_summary", summary)

    session.close()
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
