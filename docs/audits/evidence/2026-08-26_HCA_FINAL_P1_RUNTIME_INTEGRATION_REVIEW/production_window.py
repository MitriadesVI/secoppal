"""Narrow production observation: attribute only appends from this HCA window."""
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

REPO = Path("/Users/rodrigoortiz/Documents/secoppal")
OUT = Path(__file__).parent
def snapshot():
    return {str(p): {"size": p.stat().st_size, "mtime_ns": p.stat().st_mtime_ns}
            for p in (REPO / "data").rglob("*") if p.is_file() and p.suffix in (".jsonl", ".json", ".db", ".sqlite", ".wal")}

before = snapshot()
start = time.time_ns()
env = dict(os.environ)
env["PYTHONHASHSEED"] = "0"
with (OUT / "production-window-runner.log").open("w") as log:
    process = subprocess.run([str(REPO / ".venv/bin/python3"),
        str(REPO / "scripts/audits/run_conversation_holistic_audit.py"),
        "--only", "HCA-FOLLOWUP-016", "--out", str(OUT / "production-window-hca")],
        cwd=REPO, env=env, stdout=log, stderr=log)
end = time.time_ns()
after = snapshot()
changes = []
attributed = []
for path, state in after.items():
    old = before.get(path, {"size": 0, "mtime_ns": 0})
    if old == state:
        continue
    p = Path(path)
    item = {"path": path, "before": old, "after": state, "appended_rows": 0, "audit_rows": 0}
    if p.suffix == ".jsonl" and state["size"] > old["size"]:
        with p.open("rb") as stream:
            stream.seek(old["size"])
            appended = stream.read()
        for row in appended.splitlines():
            item["appended_rows"] += 1
            if b"hca1-HCA-FOLLOWUP-016" in row:
                item["audit_rows"] += 1
    if "hca1-HCA-FOLLOWUP-016" in p.name or item["audit_rows"]:
        attributed.append(path)
    changes.append(item)
isolation = json.loads((OUT / "production-window-hca/isolation.json").read_text())
evidence = {"start_utc": datetime.fromtimestamp(start / 1e9, timezone.utc).isoformat(),
    "elapsed_seconds": (end-start)/1e9, "runner_exit": process.returncode,
    "channel": "telegram", "chat_id": "hca1-HCA-FOLLOWUP-016", "changes": changes,
    "attributed_production_writes": attributed, "isolation": isolation}
(OUT / "production-window.json").write_text(json.dumps(evidence, indent=1))
print(json.dumps(evidence, indent=1))
assert process.returncode == 0 and not attributed and not isolation["outside_sandbox"]
