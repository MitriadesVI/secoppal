"""Build the sealed M2A_GOLD (release r2) from the imported advisory candidate plus the user's 2026-10-02 adjudications.

The advisory tree under ``docs/audits/m2a_advisory_2026-10-01/`` stays byte-identical (it is evidence, pinned by
``IMPORT_MANIFEST.sha256``). The sealed copy lives in ``docs/audits/m2a_conversation/gold/`` and differs from the
candidate only by the patches below (``ADJUDICATION_2026-10-02.md``):

  U1  SF166 — year 2023 is outside the D10 catalog (2024–2026): CLARIFY SMMLV_YEAR_AMBIGUOUS; control of
      non-extrapolation. Adding 2023 would be a separate future product decision.
  U2  SF063 — the pending pre-state is normalised to a reachable one (asks 3 -> 2, one PROTOCOL:reask): the contract
      caps asks at 2. Disposition and labels unchanged.
  D1  SF168 — declared expressly as a test of an invalid persisted state (an executed SMMLV anchor over several
      years cannot be produced under §15.5); labels unchanged.

REAL records are not touched (the REAL Gold never enters git). Usage:
  python scripts/audits/m2a_conversation/build_sealed_gold.py          # write
  python scripts/audits/m2a_conversation/build_sealed_gold.py --check  # exit 1 if the committed copy differs
"""

from __future__ import annotations

import argparse
import collections
import copy
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "docs/audits/m2a_advisory_2026-10-01/02_gold_followups/gold_candidato"
DST = ROOT / "docs/audits/m2a_conversation/gold"
ADJ_DOC = "docs/audits/m2a_conversation/gold/ADJUDICATION_2026-10-02.md"


def load(path: Path) -> list[dict]:
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def dump(rows: list[dict]) -> str:
    return "\n".join(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows)


def sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def u1_sf166(rec: dict) -> None:
    rec["expected_disposition"] = "CLARIFY"
    rec["adjudicated_by"] = sorted(set(rec["adjudicated_by"]) | {"U1"})
    for k in ("label_A", "label_B"):
        L = rec[k]
        L["expected_disposition"] = "CLARIFY"
        L["disposition_reason"] = "SMMLV_YEAR_AMBIGUOUS"
        L["resulting_ir"] = None
        L["effects"] = []
        L["expected_echo"] = []
        for o in L["ops"]:
            if o["slot"] == "TIME":
                o["unresolved"] = True
        L["notes"] = ("[SUSTITUIDA por U1] " + L["notes"] + " | U1 (usuario, 2026-10-02): 2023 no está en el catálogo de D10 (2024–2026) ⇒ "
                      "CLARIFY SMMLV_YEAR_AMBIGUOUS; control de no extrapolación (adjudicated_by: [\"U1\"]).")


def u2_sf063(rec: dict) -> None:
    rec["adjudicated_by"] = sorted(set(rec["adjudicated_by"]) | {"U2"})
    for k in ("label_A", "label_B"):
        p = rec[k]["prior"]
        p["pending"]["asks"] = 2
        p["chain"] = ["PROTOCOL:reask"]
        rec[k]["notes"] = (rec[k]["notes"] + " | U2 (usuario, 2026-10-02): preestado normalizado a uno alcanzable "
                           "(asks 3 -> 2, una sola PROTOCOL:reask); etiqueta sin cambios (adjudicated_by: [\"U2\"]).")


def d1_sf168(rec: dict) -> None:
    rec["adjudicated_by"] = sorted(set(rec["adjudicated_by"]) | {"D1"})
    rec["invalid_persisted_state"] = {
        "declared_by": "D1 (HERA, 2026-10-02)",
        "why": "un ancla ejecutada con SMMLV sobre varios años no es alcanzable bajo §15.5; el caso prueba que la "
               "re-decisión sobre la efectiva compuesta falla cerrado (§6.6) ante un estado persistido inválido",
    }


PATCHES = {"SF166": u1_sf166, "SF063": u2_sf063, "SF168": d1_sf168}


def build() -> dict[str, str]:
    gold = load(SRC / "M2A_GOLD_synthetic.jsonl")
    front = load(SRC / "FOLLOWUP_FRONTIER_synthetic.jsonl")
    before = {r["gold_id"]: sha(json.dumps(r, ensure_ascii=False, sort_keys=True)) for r in gold if r["gold_id"] in PATCHES}
    out = []
    for r in gold:
        r = copy.deepcopy(r)
        if r["gold_id"] in PATCHES:
            PATCHES[r["gold_id"]](r)
        out.append(r)
    if {r["gold_id"] for r in gold} & set(PATCHES) != set(PATCHES):
        raise SystemExit("a patched id is missing from the candidate")
    after = {r["gold_id"]: sha(json.dumps(r, ensure_ascii=False, sort_keys=True)) for r in out if r["gold_id"] in PATCHES}
    gold_body, front_body = dump(out), dump(front)
    idx = json.loads((SRC / "M2A_GOLD_AND_FRONTIER_INDEX.json").read_text(encoding="utf-8"))
    idx["sealed"] = True
    idx["release"] = "M2A_GOLD r2 (2026-10-02)"
    idx["rules"] = idx["rules"] + " + U1/U2 (usuario, 2026-10-02) + D1"
    idx["note"] = ("Copia sellada. Difiere del candidato asesor sólo por U1 (SF166), U2 (SF063) y D1 (SF168); ver "
                   + ADJ_DOC + ". REAL sin cambios.")
    syn = idx["slices"]["SYNTHETIC"]["M2A_GOLD"]
    syn["sha256"] = sha(gold_body)
    syn["by_disposition"] = dict(collections.Counter(str(r["expected_disposition"]) for r in out))
    idx["slices"]["SYNTHETIC"]["FOLLOWUP_FRONTIER"]["sha256"] = sha(front_body)
    idx["patches"] = {g: {"before_sha256": before[g], "after_sha256": after[g]} for g in sorted(PATCHES)}
    return {
        "M2A_GOLD_synthetic.jsonl": gold_body,
        "FOLLOWUP_FRONTIER_synthetic.jsonl": front_body,
        "M2A_GOLD_AND_FRONTIER_INDEX.json": json.dumps(idx, ensure_ascii=False, indent=1) + "\n",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--check", action="store_true", help="compare with the committed copy instead of writing")
    a = ap.parse_args()
    files = build()
    if a.check:
        bad = [n for n, body in files.items() if (DST / n).read_text(encoding="utf-8") != body]
        print("sealed gold", "DIFFERS: " + ", ".join(bad) if bad else "reproduces byte for byte")
        return 1 if bad else 0
    DST.mkdir(parents=True, exist_ok=True)
    for n, body in files.items():
        (DST / n).write_text(body, encoding="utf-8")
    print("written", sorted(files))
    return 0


if __name__ == "__main__":
    sys.exit(main())
