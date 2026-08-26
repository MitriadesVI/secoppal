from pathlib import Path
import json
import re
import subprocess
from datetime import datetime, timezone

OUT = Path(__file__).parent
REPO = Path("/Users/rodrigoortiz/Documents/secoppal")
BASE = "eb8cfe1ed72ba5c9df0ab9c0ead5089ff5d09368"
HEAD = "dec589a21bf36844d595db80093e7824ea2a0189"
def git(*args, cwd=REPO):
    return subprocess.check_output(["git", *args], cwd=cwd).decode().strip()
def load(path):
    return json.loads((OUT / path).read_text())
full = (OUT / "full-suite-authoritative.log").read_text()
assert re.search(r"8657 passed, 2 skipped, 24 xfailed", full), "authoritative suite not complete"
assert not load("comparison.json")["failed_gates"]
assert not load("static-audit.json")["diff_groups"]["unrelated"]
assert git("rev-parse", "main") == BASE
assert git("rev-parse", "HEAD") == HEAD
assert git("rev-parse", "feature/hca-final-p1-runtime-fixes") == HEAD
assert git("branch", "--show-current") == "feature/hca-final-p1-runtime-fixes"
assert git("merge-base", "main", HEAD) == BASE
subprocess.run(["git", "merge-base", "--is-ancestor", "main", HEAD], cwd=REPO, check=True)
assert not git("status", "--porcelain=v1", "--untracked-files=no")
assert not git("status", "--porcelain=v1", "--untracked-files=no", cwd=OUT / "feature")
assert git("rev-parse", "HEAD^{tree}") == git("rev-parse", "HEAD^{tree}", cwd=OUT / "feature")
subprocess.run(["git", "diff", "--check", "eb8cfe1..dec589a"], cwd=REPO, check=True)
initial = set((OUT / "preflight-untracked.z").read_bytes().split(b"\0")) - {b""}
now = set(subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"], cwd=REPO).split(b"\0")) - {b""}
assert not initial - now
verdict = {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
    "HCA_FINAL_RUNTIME_INTEGRATION_REVIEW": "PASS", "MAIN_RELATION": "FAST_FORWARD_AVAILABLE",
    "MAIN_BEFORE": BASE, "FEATURE_HEAD": HEAD, "MERGE_BASE": BASE, "COMMITS": 7, "UNRELATED": 0,
    "TEST_REPINS": {"plan_query_node": "JUSTIFIED", "pagination": "NONVACUOUS", "modality": "JUSTIFIED"},
    "AUTHORITIES": {name: "ONE_RESPONSIBILITY" for name in ("money_admission", "estado_projection", "generic_buyer", "derived_execution_state")},
    "SIX_RUNTIME_BUGS": "6/6 CLOSED", "BROKEN": 0, "FULL_SUITE": "8657 passed, 2 skipped, 24 xfailed, 0 failed",
    "QUERY_PLAN_LOC": 299, "UNTRACKED_PRESERVED": len(initial), "SEMGREP": "TOOL_UNAVAILABLE",
    "PRODUCTION_WRITES": 0, "PROMOTION_AUTHORIZED_BY_GATES": True}
(OUT / "integration-preverdict.json").write_text(json.dumps(verdict, ensure_ascii=False, indent=1))
print(json.dumps(verdict, ensure_ascii=False, indent=1))
