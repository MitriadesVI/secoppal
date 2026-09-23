#!/usr/bin/env python3
"""Compute the Schema 2 to Schema 3 exact projection gate from frozen inputs."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path

from app.core.buyer_source_names import load_registry
from app.utils.money import normalize_text

ROOT = Path(__file__).resolve().parents[2]
BASELINE = ROOT / "tests/fixtures/adr073_schema3/schema2_projection_baseline.json"
SIDECAR = ROOT / "app/data/buyer_source_names.json"
DATASETS = ("p6dx-8zbt", "jbjy-vk9h")


def report() -> dict:
    frozen = json.loads(BASELINE.read_text(encoding="utf-8"))
    payload = json.loads(SIDECAR.read_text(encoding="utf-8"))
    registry = load_registry(SIDECAR)
    before = {row["canonical_name"]: row for row in frozen["bindings"]}
    after = {unit["canonical_name"]: unit for unit in payload["buyer_units"]}
    before_index = {normalize_text(name): row for name, row in before.items()}
    census = frozen["census_canonicals"]
    bound = [name for name in census if normalize_text(name) in before_index]
    complement = [name for name in census if normalize_text(name) not in before_index]

    def old_project(name: str, dataset: str) -> tuple[str, ...] | None:
        row = before_index.get(normalize_text(name))
        return tuple(row["sources"][dataset]) if row and dataset in row["sources"] else None

    def compare(names: list[str]) -> dict:
        pairs = [(name, dataset) for name in names for dataset in DATASETS]
        diffs = [{"canonical": name, "dataset": dataset,
                  "before": old_project(name, dataset), "after": registry.project(name, dataset)}
                 for name, dataset in pairs
                 if old_project(name, dataset) != registry.project(name, dataset)]
        return {"comparisons": len(pairs), "semantic_diffs": diffs}

    binding = compare(list(before))
    missing = compare(complement)
    sweep = compare(census)
    canonical_diff = sorted(set(before) ^ set(after))
    source_diff = sorted(name for name in set(before) & set(after)
                         if before[name]["sources"] != after[name]["source_names"])
    dataset_diff = sorted(name for name in set(before) & set(after)
                          if set(before[name]["sources"]) != set(after[name]["source_names"]))
    provenance_diff = sorted(name for name in set(before) & set(after)
                             if before[name]["projection_evidence"] != after[name]["projection_evidence"])
    identities_consistent = len(bound) + len(complement) == len(census)
    comparisons_consistent = binding["comparisons"] + missing["comparisons"] == sweep["comparisons"]
    fiscal_kinds = Counter(item.kind for unit in registry.bindings()
                           for item in unit.fiscal_evidence)
    outcomes = Counter(item.outcome for unit in registry.bindings()
                       for item in unit.source_observations)
    shared_observations = Counter(item.base_number for unit in registry.bindings()
                                  for item in unit.source_observations)
    d0 = sorted(unit.canonical_name for unit in registry.bindings()
                if unit.fiscal_attestations
                and any(ob.source == "SECOP_DIRECTORY" and ob.outcome == "NOT_FOUND"
                        for ob in unit.source_observations))
    pass_gate = (frozen["schema_version_before"] == 2 and payload["schema_version"] == 3
                 and len(before) == len(after) == len(bound) == len(registry)
                 and identities_consistent and comparisons_consistent
                 and not canonical_diff and not source_diff and not dataset_diff
                 and not provenance_diff and not binding["semantic_diffs"]
                 and not missing["semantic_diffs"] and not sweep["semantic_diffs"])
    return {
        "base_sha": frozen["base_sha"],
        "schema_version_before": frozen["schema_version_before"],
        "schema_version_after": payload["schema_version"],
        "sidecar_sha256_before": frozen["sidecar_sha256_before"],
        "sidecar_sha256_after": hashlib.sha256(SIDECAR.read_bytes()).hexdigest(),
        "bindings_before": len(before), "bindings_after": len(after),
        "canonical_set_diff": canonical_diff,
        "source_projection_diff": source_diff,
        "dataset_diff": dataset_diff,
        "projection_provenance_diff": provenance_diff,
        "binding": binding,
        "complement_identities": len(complement), "complement": missing,
        "census_identities": len(census), "census_sweep": sweep,
        "consistency": {
            "binding_plus_complement_equals_sweep": comparisons_consistent,
            "with_binding_plus_without_binding_equals_census": identities_consistent,
        },
        "semantic_projection_diff_count": (
            len(binding["semantic_diffs"]) + len(missing["semantic_diffs"]) + len(sweep["semantic_diffs"])
        ),
        "fiscal_evidence_kinds": dict(fiscal_kinds),
        "directory_observation_outcomes": dict(outcomes),
        "shared_nit_canaries": {base: shared_observations[base]
                                for base in ("899999061", "899999239")},
        "d0_canary_canonicals": d0,
        "verdict": "PASS" if pass_gate else "FAIL",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()
    result = report()
    content = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(content, encoding="utf-8")
    else:
        print(content, end="")
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
