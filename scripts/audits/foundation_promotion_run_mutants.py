"""Apply each mutant, run its killer tests, restore the bytes; results in $MUTANT_SCRATCH."""
import json
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
sys.path.insert(0, os.path.dirname(__file__))
from foundation_promotion_mutants import MUTANTS, SP
WT = Path(sys.argv[1])
out = []
PY = "/Users/rodrigoortiz/Documents/secoppal/.venv/bin/python"
for name, edits, killers in MUTANTS:
    saved = {}
    try:
        for path, old, new in edits:
            f = WT / path
            saved.setdefault(path, f.read_text() if f.exists() else None)
            if old is None:
                f.write_text(new)
            else:
                text = f.read_text()
                assert text.count(old) == 1, (name, path, old[:60])
                f.write_text(text.replace(old, new))
        files = sorted({k.split("::")[0] for k in killers})
        junit = Path(SP) / f"mut_{name}.xml"
        env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "TZ": "America/Bogota", "PYTHONHASHSEED": "0"}
        subprocess.run([PY, "-m", "pytest", "-p", "netblock", "-p", "no:cacheprovider", "-q", "-x" if False else "-q", *files, f"--junitxml={junit}"],
                       cwd=WT, env=env, capture_output=True, text=True, timeout=900)
        root = ET.parse(junit).getroot()
        failed = []
        for case in root.iter("testcase"):
            node = case.get("classname", "").replace(".", "/") + ".py::" + case.get("name")
            for child in case:
                if child.tag in ("failure", "error"):
                    failed.append((node, child.tag, (child.get("message") or "")[:160]))
        def hit(node, k):
            return node == k or node.startswith(k + "[") or ("::" not in k and node.startswith(k + "::"))
        killer_hits = [f for f in failed if any(hit(f[0], k) for k in killers)]
        by_assertion = [f for f in killer_hits if f[1] == "failure" and ("AssertionError" in f[2] or "assert" in f[2] or "Failed" in f[2] or "Did not raise" in f[2] or "DID NOT RAISE" in f[2])]
        out.append({"mutant": name, "killed": bool(killer_hits), "killed_by_assertion": bool(by_assertion),
                    "killer_failures": killer_hits[:6], "other_failures": len(failed) - len(killer_hits)})
    finally:
        for path, text in saved.items():
            f = WT / path
            if text is None:
                f.unlink(missing_ok=True)
            else:
                f.write_text(text)
    print(name, out[-1]["killed"], out[-1]["killed_by_assertion"], len(out[-1]["killer_failures"]), flush=True)
json.dump(out, open(Path(SP) / "mutation_results.json", "w"), indent=1, ensure_ascii=False)
