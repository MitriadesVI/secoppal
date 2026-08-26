import ast
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path("/Users/rodrigoortiz/Documents/secoppal")
OUT = Path(__file__).parent
def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT)
paths = git("diff", "--name-only", "eb8cfe1..dec589a").decode().splitlines()
groups = {key: [] for key in ("runtime", "tests", "fixtures", "docs", "makefile", "evidence", "unrelated")}
for path in paths:
    key = ("runtime" if path.startswith("app/") else "fixtures" if path.startswith("tests/fixtures/")
           else "tests" if path.startswith("tests/") else "docs" if path.startswith("docs/")
           else "makefile" if path == "Makefile" else "evidence" if path == "scripts/audits/probe_query.py" else "unrelated")
    groups[key].append(path)
commits = []
for line in git("log", "--reverse", "--format=%H %s", "eb8cfe1..dec589a").decode().splitlines():
    sha, subject = line.split(" ", 1)
    commits.append({"sha": sha, "subject": subject, "paths": git("diff-tree", "--no-commit-id", "--name-only", "-r", sha).decode().splitlines()})
fixtures = {}
for path in ("tests/fixtures/audits/conversation_holistic_v1.yaml", "tests/fixtures/audits/negation_focal_v1.yaml",
             "tests/fixtures/audits/time_v2_focal.yaml", "tests/fixtures/audits/control_language_v1.yaml",
             "tests/fixtures/audits/actor_hygiene_v1.yaml", "tests/fixtures/query_corpus_v1.yaml", "tests/fixtures/dialogue_policy_corpus.yaml"):
    values = [hashlib.sha256(git("show", sha + ":" + path)).hexdigest() for sha in ("eb8cfe1", "dec589a")]
    fixtures[path] = {"base_sha256": values[0], "feature_sha256": values[1], "identical": values[0] == values[1]}
assert all(f["identical"] for f in fixtures.values())
assert not groups["unrelated"]
repins = {}
for file, function in (("tests/test_plan_query_node.py", "test_state_only_builds_where_but_execution_requests_scope_or_topic"),
                       ("tests/test_pagination_buffer.py", "test_pagination_drops_post_query_ecosystem_to_refetch_same_source"),
                       ("tests/test_modality_pipeline_integration.py", "test_no_modality_pre_post_golden_parity")):
    rows = []
    for sha in ("eb8cfe1", "dec589a"):
        tree = ast.parse(git("show", sha + ":" + file).decode())
        fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == function)
        rows.append({"commit": sha, "asserts": sum(isinstance(n, ast.Assert) for n in ast.walk(fn)), "ast": ast.dump(fn, include_attributes=False)})
    repins[file] = {"base_asserts": rows[0]["asserts"], "feature_asserts": rows[1]["asserts"], "function_identical": rows[0]["ast"] == rows[1]["ast"]}
source = ast.parse(git("show", "dec589a:scripts/audits/probe_query.py").decode())
settings_calls = [ast.unparse(n) for n in ast.walk(source) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id in ("Settings", "SecopalWorkflow")]
report = {"diff_groups": groups, "commits": commits, "fixture_hashes": fixtures, "repins": repins,
          "new_script_settings_calls": settings_calls, "query_plan_identical": not git("diff", "eb8cfe1..dec589a", "--", "app/core/query_plan.py")}
(OUT / "static-audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=1))
print(json.dumps({"counts": {k: len(v) for k, v in groups.items()}, "repins": repins, "settings": settings_calls}, ensure_ascii=False, indent=1))
