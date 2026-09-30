"""Paired global delta by nodeid: BASE ledger vs CANDIDATE ledger (netblock_ledger.py output)."""
import json
import sys

FAILING = {"failed", "error"}


def load(path):
    with open(path, encoding="utf-8") as stream:
        return {row["nodeid"]: row["outcome"] for row in map(json.loads, stream)}


base, cand = load(sys.argv[1]), load(sys.argv[2])
delta = {
    "CANDIDATE_ONLY_FAILURES": sorted(n for n in base.keys() & cand.keys()
                                      if cand[n] == "failed" and base[n] not in FAILING),
    "NEW_ERRORS": sorted(n for n in cand if cand[n] == "error" and base.get(n) != "error"),
    "BASE_PASSED_NOW_MISSING": sorted(n for n in base if base[n] == "passed" and n not in cand),
    "NEW_TESTS_FAILED": sorted(n for n in cand.keys() - base.keys() if cand[n] in FAILING),
}
summary = {
    "base_total": len(base), "candidate_total": len(cand),
    "new_tests": sorted(cand.keys() - base.keys()),
    "fixed_in_candidate": sorted(n for n in base.keys() & cand.keys() if base[n] in FAILING and cand[n] not in FAILING),
    "base_outcomes": {o: sum(v == o for v in base.values()) for o in sorted(set(base.values()))},
    "candidate_outcomes": {o: sum(v == o for v in cand.values()) for o in sorted(set(cand.values()))},
    **{key: {"count": len(value), "nodeids": value} for key, value in delta.items()},
}
print(json.dumps(summary, indent=1))
sys.exit(1 if any(delta.values()) else 0)
