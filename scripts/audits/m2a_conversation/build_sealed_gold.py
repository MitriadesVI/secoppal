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
  U3  release r3 (contract amendment R3) — ADDS SF192–SF195. The records are built from the two blind labels kept
      verbatim in ``docs/audits/m2a_conversation/labeling_r3/labels_{A,B}.jsonl``, plus only the user's adjudication
      of their differences (``gold/ADJUDICATION_R3.md``). A disagreement on a primary field stops the build.

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
ADJ_DOC_R3 = "docs/audits/m2a_conversation/gold/ADJUDICATION_R3.md"
LABELS_R3 = ROOT / "docs/audits/m2a_conversation/labeling_r3"


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


# ---- U3 (release r3): additions from the blind labels A/B + the user's adjudication of their differences ----------
U3_IDS = ("SF192", "SF193", "SF194", "SF195")
U3_PRIMARY = ("turn_kind", "m2a_scope", "expected_disposition", "disposition_reason", "resulting_ir", "effects",
              "expected_echo")
U3_INPUT = ("gold_id", "schema", "slice", "source_kind", "input_kind", "followup_text", "followup_sha256", "prior")
_U3 = " | U3 (usuario, 2026-10-02): "


def _u3_note(L: dict, text: str) -> None:
    L["notes"] = L["notes"] + _U3 + text


def _u3_op_form(L: dict) -> list[str]:
    """Forma de las ops (forma de B; convención del Gold): CLEAR sin polaridad ni value_hint; KEEP de una ranura vacía
    sin value_hint (A y B ya coinciden en su polaridad POSITIVE). Lo demás de cada op queda tal cual."""
    changed = []
    for o in L["ops"]:
        if o["op"] == "CLEAR" and (o["polarity"], o["value_hint"]) != (None, None):
            o["polarity"], o["value_hint"] = None, None
            changed.append(f"CLEAR {o['slot']}: polarity y value_hint null")
        elif o["op"] == "KEEP" and o["value_hint"] is not None:
            o["value_hint"] = None
            changed.append(f"KEEP {o['slot']}: value_hint null")
    return changed


def u3_sf192(L: dict, _other: dict) -> None:
    c1 = copy.deepcopy(L["prior"]["ir"])
    c1["order"], c1["cardinality"] = None, None
    if L["alternatives"] != [c1]:
        L["alternatives"] = [c1]
        _u3_note(L, "alternatives = sólo C1 «quitar orden y límite» (IR sin ORDER ni CARDINALITY); los candidatos "
                    "SET_ORDER por criterio alternativo los determina E-ORDER (§19, DEP-E-ORDER) y no se sellan.")


def u3_sf193(L: dict, _other: dict) -> None:
    for o in L["ops"]:
        if o["op"] == "SET" and o["slot"] == "TIME" and not o["refers_to_result_set"]:
            o["refers_to_result_set"] = True
            _u3_note(L, "refers_to_result_set = true en «los de 2024» (convención del Gold sellado, matriz T12).")
    if L["gap_codes"]:
        L["gap_codes"] = []
        _u3_note(L, "gap_codes = [].")


def u3_sf194(L: dict, _other: dict) -> None:
    """Sin diferencias que adjudicar (las confianzas se registran tal cual)."""


def u3_sf195(L: dict, _other: dict) -> None:
    canonical = sorted(L["gap_codes"])  # el orden mayoritario del Gold sellado (alfabético, 20 de 33 listas)
    if L["gap_codes"] != canonical:
        L["gap_codes"] = canonical
        _u3_note(L, "gap_codes en el orden canónico del Gold (alfabético); mismo conjunto en A y B.")


U3_RULES = {"SF192": u3_sf192, "SF193": u3_sf193, "SF194": u3_sf194, "SF195": u3_sf195}


def u3_additions() -> list[dict]:
    """The four r3 records: the blind labels verbatim, plus only the adjudicated differences."""
    labels = {side: load(LABELS_R3 / f"labels_{side}.jsonl") for side in ("A", "B")}
    if [r["gold_id"] for r in labels["A"]] != list(U3_IDS) or [r["gold_id"] for r in labels["B"]] != list(U3_IDS):
        raise SystemExit("U3: the blind labels do not cover exactly SF192–SF195 in order")
    out = []
    for a, b in zip(labels["A"], labels["B"]):
        gid = a["gold_id"]
        if any(a[k] != b[k] for k in U3_INPUT):
            raise SystemExit(f"U3: {gid} labels A and B were not given the same input")
        if sha(a["followup_text"]) != a["followup_sha256"]:
            raise SystemExit(f"U3: {gid} followup_sha256 does not match its text")
        disagreement = [k for k in U3_PRIMARY if a[k] != b[k]]
        if disagreement:
            raise SystemExit(f"U3: {gid} primary disagreement {disagreement}: needs a new adjudication")
        A, B = copy.deepcopy(a), copy.deepcopy(b)
        for L, other in ((A, B), (B, A)):
            changed = _u3_op_form(L)
            if changed:
                _u3_note(L, "forma de las ops, como B (" + "; ".join(changed) + ").")
            U3_RULES[gid](L, other)
        out.append({"adjudicated_by": ["U3"],
                    "ambiguous_by": sorted(s for s, L in (("A", A), ("B", B)) if L["ambiguity"] != "CLEAR"),
                    "expected_disposition": A["expected_disposition"], "gold_id": gid, "label_A": A, "label_B": B,
                    "m2a_scope": A["m2a_scope"], "primary_agreement": True, "slice": "SYNTHETIC_FOLLOWUP"})
    return out


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
    additions = u3_additions()
    taken = {r["gold_id"] for r in gold + front}
    if taken & set(U3_IDS):
        raise SystemExit(f"U3: added ids already exist in the candidate: {sorted(taken & set(U3_IDS))}")
    out = sorted(out + additions, key=lambda r: r["gold_id"])
    gold_body, front_body = dump(out), dump(front)
    idx = json.loads((SRC / "M2A_GOLD_AND_FRONTIER_INDEX.json").read_text(encoding="utf-8"))
    idx["sealed"] = True
    idx["release"] = "M2A_GOLD r3 (2026-10-02)"
    idx["rules"] = idx["rules"] + " + U1/U2 (usuario, 2026-10-02) + D1 + U3 (enmienda R3)"
    idx["note"] = ("Copia sellada. Difiere del candidato asesor por U1 (SF166), U2 (SF063) y D1 (SF168), ver "
                   + ADJ_DOC + ", y por las adiciones U3 (SF192–SF195), ver " + ADJ_DOC_R3 + ". REAL sin cambios.")
    syn = idx["slices"]["SYNTHETIC"]["M2A_GOLD"]
    syn["ids"] = sorted(r["gold_id"] for r in out)
    syn["n"] = len(out)
    syn["sha256"] = sha(gold_body)
    syn["by_disposition"] = dict(collections.Counter(str(r["expected_disposition"]) for r in out))
    idx["slices"]["SYNTHETIC"]["FOLLOWUP_FRONTIER"]["sha256"] = sha(front_body)
    idx["patches"] = {g: {"before_sha256": before[g], "after_sha256": after[g]} for g in sorted(PATCHES)}
    idx["additions"] = {r["gold_id"]: {"sha256": sha(json.dumps(r, ensure_ascii=False, sort_keys=True))}
                        for r in additions}
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
