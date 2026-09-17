"""Measure linguistic observation only, never execute a product capability."""
import hashlib
import json
import time
from collections import Counter
from pathlib import Path

from app.core.natural_alpha.grounding import validate_and_ground
from app.core.natural_alpha.prompt import provider_request

DIRECTORY = Path(__file__).resolve().parent
ROOT = DIRECTORY.parents[1]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_frozen():
    manifest = json.loads((DIRECTORY / "manifest.json").read_text())
    superseded = manifest.get("superseded", {})
    for name, expected in manifest["sha256"].items():
        if name in superseded:
            # Historical sha under an explicit successor freeze: the artifact
            # moved on (see `superseded`), so it stops pinning the live file.
            continue
        if sha256(ROOT / name) != expected:
            raise ValueError(f"FROZEN_ARTIFACT_CHANGED:{name}")
    cases = json.loads((DIRECTORY / "corpus.json").read_text())
    gold = json.loads((DIRECTORY / "gold.json").read_text())
    identifiers = [case["case_id"] for case in cases]
    if len(identifiers) != len(set(identifiers)) or set(identifiers) != set(gold["expected_jobs"]):
        raise ValueError("CORPUS_GOLD_KEY_MISMATCH")
    return cases, gold, manifest


def evaluate(cases, gold, provider, *, progress=None):
    rows, records = [], []
    stopped = ""
    for case in cases:
        key = case["case_id"] + "::NATURAL_ALPHA_V1"
        request = provider_request(case["raw_text"])
        started = time.perf_counter()
        response = provider.observe(key, request)
        elapsed = round((time.perf_counter() - started) * 1000, 3)
        records.append({"call_key": key, "request": request, "payload": response.payload,
                        "provider_error": response.error, "provider_calls": response.provider_calls,
                        "latency_ms": elapsed})
        status, jobs, invalid_reason = "MATCHED", (), ""
        if response.error:
            status = "PROVIDER_ERROR"
        else:
            try:
                jobs = validate_and_ground(case["raw_text"], response.payload)
            except (TypeError, ValueError) as exc:
                status = "INVALID"
                invalid_reason = "GROUNDING" if str(exc) == "UNGROUNDED_EVIDENCE" else "SCHEMA"
        expected = gold["expected_jobs"][case["case_id"]]
        pairs = [[job.action, job.aspect] for job in jobs]
        rows.append({"case_id": case["case_id"], "family": case["family"], "status": status,
                     "invalid_reason": invalid_reason, "expected": expected, "actual": pairs,
                     "exact": status == "MATCHED" and sorted(pairs) == sorted(expected),
                     "jobs": [job.metadata() for job in jobs], "latency_ms": elapsed})
        if progress:
            progress(case["case_id"], status)
        if response.error == "PROVIDER_CONTRACT_INCOMPATIBLE":
            stopped = response.error
            break
    return {"oracle_status": gold["oracle_status"], "expected_total": len(cases),
            "completed": len(rows), "stopped_reason": stopped,
            "metrics": metrics(rows, complete=len(rows) == len(cases)),
            "results": rows, "provider_records": records,
            "provider_calls": sum(record["provider_calls"] for record in records)}


def metrics(rows, *, complete):
    total = len(rows)
    singles = [row for row in rows if len(row["expected"]) == 1]
    multis = [row for row in rows if len(row["expected"]) >= 2]
    errors = sum(row["status"] == "PROVIDER_ERROR" for row in rows)
    invalid = sum(row["status"] == "INVALID" for row in rows)
    schema = sum(row["invalid_reason"] == "SCHEMA" for row in rows)
    accuracy = sum(row["exact"] for row in singles) / len(singles) if singles else None
    recall = sum(len(row["actual"]) >= 2 for row in multis) / len(multis) if multis else None
    false_multi = sum(len(row["actual"]) >= 2 for row in singles) / len(singles) if singles else None
    invalid_rate = invalid / total if total else None
    gates = {
        "complete": complete,
        "provider_errors_zero": errors == 0,
        "schema_invalid_at_most_2_5pct": bool(total) and schema / total <= .025,
        "single_classification_at_least_85pct": accuracy is not None and accuracy >= .85,
        "multi_recall_at_least_90pct": recall is not None and recall >= .90,
        "single_false_multi_at_most_2_5pct": false_multi is not None and false_multi <= .025,
    }
    families = Counter(row["family"] for row in rows)
    return {"provider_errors": errors, "invalid": invalid, "schema_invalid": schema,
            "invalid_rate": invalid_rate, "single_count": len(singles), "multi_count": len(multis),
            "single_accuracy": accuracy, "multi_detection_recall": recall,
            "single_false_multi_rate": false_multi, "gates": gates,
            "gates_pass": all(gates.values()),
            "by_family": {family: {"total": count,
                                    "exact": sum(row["exact"] for row in rows if row["family"] == family)}
                          for family, count in families.items()}}
