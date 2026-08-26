"""Gate estructural del ORACLE_PATCH_V1: sólo seis casos pueden cambiar.

Compara el corpus HCA de un ref git contra el del árbol de trabajo caso por caso
(por hash del fragmento parseado, no por líneas) y falla si cambia un séptimo
caso, si cambia el conjunto de IDs o si cambia su orden.

Uso:
    .venv/bin/python docs/audits/evidence/2026-08-26_HCA_ORACLE_PATCH_V1/corpus_gate.py \
        --base <git-ref> [--json <salida>]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[4]
CORPUS_REL = "tests/fixtures/audits/conversation_holistic_v1.yaml"
AUTHORIZED = [
    "HCA-ACTOR-004",
    "HCA-ENTITY-001",
    "HCA-FOLLOWUP-007",
    "HCA-FOLLOWUP-016",
    "HCA-META-001B",
    "HCA-TOPIC-007",
]


def frag_sha(case: dict) -> str:
    blob = json.dumps(case, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="git ref con el corpus pre-patch")
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    path = ROOT / CORPUS_REL
    after_bytes = path.read_bytes()
    before_bytes = subprocess.run(
        ["git", "-C", str(ROOT), "show", f"{args.base}:{CORPUS_REL}"],
        check=True, capture_output=True,
    ).stdout

    before = yaml.safe_load(before_bytes.decode("utf-8"))["cases"]
    after = yaml.safe_load(after_bytes.decode("utf-8"))["cases"]

    problems: list[str] = []
    if [c["id"] for c in before] != [c["id"] for c in after]:
        problems.append("el conjunto o el orden de los case_id cambió")

    b = {str(c["id"]): frag_sha(c) for c in before}
    a = {str(c["id"]): frag_sha(c) for c in after}
    changed = sorted(i for i in b if i in a and b[i] != a[i])
    unexpected = sorted(set(changed) - set(AUTHORIZED))
    missing = sorted(set(AUTHORIZED) - set(changed))
    if unexpected:
        problems.append(f"casos cambiados fuera del alcance: {unexpected}")

    out = {
        "base_ref": args.base,
        "sha_before": hashlib.sha256(before_bytes).hexdigest(),
        "sha_after": hashlib.sha256(after_bytes).hexdigest(),
        "cases": len(after),
        "authorized_case_ids": AUTHORIZED,
        "changed_case_ids": changed,
        "unexpected_changed_cases": unexpected,
        "authorized_but_unchanged": missing,
        "fragment_hashes": [
            {"case_id": i, "before": b[i], "after": a[i]} for i in changed
        ],
        "gate": "BLOCK" if problems else "PASS",
        "problems": problems,
    }
    text = json.dumps(out, ensure_ascii=False, indent=1)
    if args.json:
        Path(args.json).write_text(text + "\n", encoding="utf-8")
    print(text)
    if problems:
        sys.exit(1)


if __name__ == "__main__":
    main()
