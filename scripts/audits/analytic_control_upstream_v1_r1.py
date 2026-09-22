"""Reproduce parser and CA admission deltas for the frozen R1 input manifest.

Example:
  python scripts/audits/analytic_control_upstream_v1_r1.py \
    --base-root /path/to/a83cc4f-worktree --candidate-root /path/to/r1-worktree \
    --output /tmp/analytic-control-r1-blast.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "tests/fixtures/analytic_control_upstream_v1_r1/blast_manifest.json"

_WORKER = r'''
import inspect
import json
import sys
from datetime import date
from pathlib import Path
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter

router = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
accepts_material_evidence = "material_evidence" in inspect.signature(admit_contract_activity).parameters
out = []
for item in json.load(sys.stdin):
    raw = item["raw"]
    try:
        parsed = router.parse(raw, today=date(2026, 9, 22))
        admission_kwargs = {
            "analytic_controls": parsed.analytic_controls,
            "material_residual": parsed.material_residual,
        }
        if accepts_material_evidence:
            admission_kwargs["material_evidence"] = parsed.material_evidence
        admission = admit_contract_activity(raw, parsed.params, **admission_kwargs)
        value = {
            "params": parsed.params,
            "negation": [request.to_state() for request in parsed.negation_requests],
            "analytic_controls": [control.to_state() for control in parsed.analytic_controls],
            "material_residual": list(parsed.material_residual),
            "ca_admission": {
                "status": admission.status.value,
                "reason": admission.reason,
                "operation": admission.request.operation.value if admission.request else None,
            },
        }
    except Exception as exc:
        value = {"error": type(exc).__name__ + ":" + str(exc)}
    out.append({"id": item["id"], "value": value})
json.dump(out, sys.stdout, ensure_ascii=False, sort_keys=True, default=str)
'''


def _inputs(manifest: dict) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    labels = ("actor", "contract_activity", "adversarial")
    for label, source in zip(labels, manifest["sources"], strict=True):
        path = ROOT / source["path"]
        data = path.read_bytes()
        if hashlib.sha256(data).hexdigest() != source["sha256"]:
            raise ValueError(f"SOURCE_HASH_CHANGED:{source['path']}")
        rows = (
            [json.loads(line) for line in data.decode().splitlines() if line.strip()]
            if source["format"] == "jsonl" else json.loads(data)
        )
        selected = [
            row for row in rows
            if isinstance(row, dict) and isinstance(row.get(source["raw_key"]), str)
        ]
        if len(selected) != source["selected_count"]:
            raise ValueError(f"SOURCE_COUNT_CHANGED:{source['path']}")
        result.extend({
            "id": f"{label}/{row[source['id_key']]}",
            "raw": row[source["raw_key"]],
        } for row in selected)
    ids = [item["id"] for item in result]
    if len(ids) != len(set(ids)) or len(ids) != manifest["total_inputs"]:
        raise ValueError("INPUT_ID_OR_TOTAL_MISMATCH")
    return result


def _snapshot(root: Path, inputs: list[dict[str, str]]) -> tuple[str, dict[str, dict]]:
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    env = {**os.environ, "PYTHONPATH": str(root), "PYTHONDONTWRITEBYTECODE": "1"}
    proc = subprocess.run(
        [sys.executable, "-c", _WORKER], cwd=root, env=env,
        input=json.dumps(inputs, ensure_ascii=False), text=True,
        capture_output=True, check=True, timeout=900,
    )
    rows = json.loads(proc.stdout)
    if [row["id"] for row in rows] != [item["id"] for item in inputs]:
        raise ValueError("SNAPSHOT_ID_ORDER_MISMATCH")
    return sha, {row["id"]: row["value"] for row in rows}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-root", required=True, type=Path)
    parser.add_argument("--candidate-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    manifest = json.loads(MANIFEST.read_text())
    inputs = _inputs(manifest)
    base_sha, base = _snapshot(args.base_root, inputs)
    candidate_sha, candidate = _snapshot(args.candidate_root, inputs)
    if base_sha != manifest["base_sha"]:
        raise ValueError(f"BASE_SHA_MISMATCH:{base_sha}")
    fields = manifest["comparison_fields"]
    changed: list[dict] = []
    field_counts = {field: 0 for field in fields}
    for item in inputs:
        identifier = item["id"]
        before, after = base[identifier], candidate[identifier]
        different = [field for field in fields if before.get(field) != after.get(field)]
        if before.get("error") != after.get("error"):
            different.append("error")
        if not different:
            continue
        for field in different:
            field_counts[field] = field_counts.get(field, 0) + 1
        changed.append({
            "id": identifier,
            "raw": item["raw"],
            "fields": different,
            "expected_reason": manifest["expected_delta_reasons"].get(identifier),
            "before": {field: before.get(field) for field in different},
            "after": {field: after.get(field) for field in different},
        })
    unexplained = [row["id"] for row in changed if row["expected_reason"] is None]
    report = {
        "manifest_sha256": hashlib.sha256(MANIFEST.read_bytes()).hexdigest(),
        "base_sha": base_sha,
        "candidate_sha": candidate_sha,
        "candidate_dirty": bool(subprocess.check_output(
            ["git", "status", "--porcelain"], cwd=args.candidate_root, text=True,
        ).strip()),
        "total_inputs": len(inputs),
        "source_fixtures": manifest["sources"],
        "changed_count": len(changed),
        "field_counts": field_counts,
        "expected_count": len(changed) - len(unexplained),
        "unexplained_count": len(unexplained),
        "unexplained_ids": unexplained,
        "changed": changed,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n")
    print(json.dumps({key: report[key] for key in (
        "base_sha", "candidate_sha", "candidate_dirty", "total_inputs",
        "changed_count", "field_counts", "expected_count", "unexplained_count",
        "unexplained_ids",
    )}, ensure_ascii=False))
    return 0 if not unexplained else 1


if __name__ == "__main__":
    raise SystemExit(main())
