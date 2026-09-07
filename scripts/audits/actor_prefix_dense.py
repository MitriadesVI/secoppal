#!/usr/bin/env python3
"""Dense synthetic BEFORE/AFTER diagnostics; never supersedes the primary gate."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import perf_counter

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.audits.actor_prefix_validation import summary
from tests.actor_prefix_fixtures import CHALLENGERS, EXACT, NAME, resolver_for
from app.data.actor_exact_prefix import enumerate_prefix_challengers, find_prefix_challenger

BASE = "345804cf55becaf728d3ec9c20dc9e2f76a3a5b9"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    blob = subprocess.check_output(["git", "show", BASE + ":app/data/actor_alias_resolver.py"], cwd=ROOT)
    with TemporaryDirectory() as baseline_directory:
        module_path = Path(baseline_directory) / "resolver_before.py"
        module_path.write_bytes(blob)
        spec = importlib.util.spec_from_file_location("prefix_baseline_resolver", module_path)
        assert spec and spec.loader
        baseline = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(baseline)
    cases = {
        "exact_without_challenger": (NAME, [EXACT]),
        "exact_plus_one": (NAME, [EXACT, CHALLENGERS[0]]),
        "three_way_synthetic": (NAME, [EXACT, *CHALLENGERS]),
        "same_exact_identity_10000": (NAME, [EXACT] + [("CC", EXACT[1], NAME + f" extra {n:05d}") for n in range(10000)]),
        "same_challenger_identity_10000": (NAME, [EXACT] + [("CC", "70000002", NAME + f" extra {n:05d}") for n in range(10000)] + [("CC", "70000003", NAME + " zultimo")]),
        "same_alias_10000_identities": (NAME, [EXACT] + [("CC", str(75000000 + n), NAME + " extra") for n in range(10000)]),
    }
    for query in ("ana", "ana maria", NAME, NAME + " diaz"):
        cases[f"tokens_{len(query.split())}"] = (query, [("CC", EXACT[1], query), ("CC", "70000002", query + " extra")])
    reports = []
    with TemporaryDirectory() as temporary:
        for case, (query, rows) in cases.items():
            resolver = resolver_for(Path(temporary) / (case + ".sqlite"), rows)
            before = baseline.ActorNameResolver(resolver.index_path)
            timers = defaultdict(list)
            states = Counter()
            with resolver._connect_readonly() as connection:
                exact = resolver._collapse_exact(connection.execute("SELECT * FROM actor_aliases WHERE normalized_alias=?", (query,)).fetchall())[0]
                for iteration in range(55):
                    for name, callable_ in (("resolver_before", lambda: before.resolve_exact(query)),
                                            ("resolver_after", lambda: resolver.resolve_exact(query)),
                                            ("existence", lambda: find_prefix_challenger(connection, query, exact, resolver._candidate))):
                        start = perf_counter()
                        result = callable_()
                        elapsed = (perf_counter() - start) * 1000
                        if iteration >= 5:
                            timers[name].append(elapsed)
                            states[name + ":" + result.status] += 1
                    if result.status == "FOUND":
                        start = perf_counter()
                        try:
                            enumerate_prefix_challengers(connection, query, exact, resolver._candidate)
                            state = "complete"
                        except (OSError, ValueError) as error:
                            state = type(error).__name__
                        elapsed = (perf_counter() - start) * 1000
                        if iteration >= 5:
                            timers["enumeration"].append(elapsed)
                            states["enumeration:" + state] += 1
            reports.append({"case": case, "rows": len(rows), "statuses": dict(states), **{key: summary(values) for key, values in timers.items()}})
    report = {"base": BASE, "baseline_resolver_blob_sha256": hashlib.sha256(blob).hexdigest(),
              "repetitions": 50, "warmups": 5, "synthetic": True, "results": reports,
              "limitation": "Supplemental dense full-runtime comparison against frozen resolver blob. Original production BEFORE/AFTER gate remains NO_GO; this cannot replace it or change thresholds."}
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({"cases": len(reports), "scope": report["limitation"]}))


if __name__ == "__main__":
    main()
