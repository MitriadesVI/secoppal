"""Build or verify the M2A_GOLD seal (M2A-0, HERA).

The seal binds, by sha256: every synthetic record, every REAL id and REAL record digest (the REAL text never leaves the
private directory), the Gold schema, the adjudications A1–A9.1 and the orchestrator classifications, the GOLD/FRONTIER
partition and its index, the gaps ledger, the DEV/TEST partition, the contract that interprets the Gold and the gate
that proves it representable. Any change to any of them breaks ``verify``.

  python scripts/audits/m2a_conversation/m2a_gold_seal.py build  --real-dir <private GOLD_CANDIDATE dir>
  python scripts/audits/m2a_conversation/m2a_gold_seal.py verify [--real-dir <private GOLD_CANDIDATE dir>]

Without ``--real-dir`` (CI), the REAL file and per-record digests are not recomputed; everything public is, and the REAL
ids are checked against the public index. Exit 1 on any mismatch.
"""

from __future__ import annotations

import argparse
import collections
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ADV = "docs/audits/m2a_advisory_2026-10-01"
CANDIDATE = f"{ADV}/02_gold_followups/gold_candidato"
GOLD = "docs/audits/m2a_conversation/gold"  # sealed copy: candidate + U1/U2/D1 (build_sealed_gold.py)
SEAL = ROOT / "docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json"
REPORT = "docs/audits/m2a_conversation/gold/REPRESENTABILITY_REPORT.json"
CHECKER = "scripts/audits/m2a_conversation/check_m2a_gold_contract.py"
S19_EXTRACTOR = "scripts/audits/m2a_conversation/extract_contract_dependencies.py"  # U19: §19 -> one sealed extraction
LABELING_R3 = "docs/audits/m2a_conversation/labeling_r3"
PARTITION_SALT = "m2a-gold-partition/1"
# DEV/TEST frozen at seal 3aac2a20 (also pinned in tests/test_m2a_gold_seal.py). The seal is sticky, so without this pin
# ``verify`` would only compare the partition with itself. Change it only after checking that no existing id moved.
# r3 (2026-10-02): 0 moves, 4 additions (SF192/SF193/SF194 TEST, SF195 DEV); r2 pin was 04197bbd….
PARTITION_SHA256 = "a73d77c94d46d425ec271414d15f8d6fdf5a42f27e8f94d1d4bdaf1353e6831f"
REAL_LOCATION = "~/Documents/secoppal-asesoria-2026-10-01/privado_no_git/gold_followups_real/GOLD_CANDIDATE/"

PUBLIC_FILES = (
    f"{GOLD}/M2A_GOLD_synthetic.jsonl",
    f"{GOLD}/FOLLOWUP_FRONTIER_synthetic.jsonl",
    f"{GOLD}/M2A_GOLD_AND_FRONTIER_INDEX.json",
    f"{CANDIDATE}/M2A_GOLD_synthetic.jsonl",
    f"{CANDIDATE}/FOLLOWUP_FRONTIER_synthetic.jsonl",
    f"{CANDIDATE}/M2A_GOLD_AND_FRONTIER_INDEX.json",
    f"{CANDIDATE}/FOLLOWUP_GAPS_LEDGER.jsonl",
    f"{CANDIDATE}/FOLLOWUP_GAPS_LEDGER.md",
    f"{GOLD}/CONTRACT_S19_DEPENDENCIES.json",
)
RULE_FILES = (
    "docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md",
    f"{ADV}/02_gold_followups/kit_v2_2/ADJUDICATED_OVERRIDES.md",
    f"{ADV}/01_diseno_m2a/ADJUDICATION_T3_2026-10-01.md",
    f"{CANDIDATE}/CLASSIFICATION_UNCLASSIFIED_4.md",
    f"{GOLD}/ADJUDICATION_2026-10-02.md",
    "scripts/audits/m2a_conversation/build_sealed_gold.py",
    f"{ADV}/01_diseno_m2a/PRODUCT_DECISIONS_2026-10-01.md",
    f"{ADV}/02_gold_followups/kit_v2_2/SCHEMA_v2_1.md",
    f"{ADV}/02_gold_followups/kit_v2_2/CONTRACT_ADDENDUM_FOLLOWUPS_v2.md",
    "docs/audits/m2a_conversation/M2A0_H7B_REVIEW.md",
    "docs/audits/m2a_conversation/IMPORT_MANIFEST.sha256",
    CHECKER,
    f"{GOLD}/ADJUDICATION_R3.md",
    f"{LABELING_R3}/labels_A.jsonl",
    f"{LABELING_R3}/labels_B.jsonl",
    f"{LABELING_R3}/R3_LABELER_INPUT.md",
    "scripts/audits/m2a_conversation/m2a_contract_text.py",
    S19_EXTRACTOR,
)
ADJUDICATIONS = {
    "A1": "F26 retirada; VALUE por lado; unidad heredada única",
    "A2": "SMMLV ejecutable con año único; Gold M1 no se re-sella",
    "A3": "top-N > 100 = P8 (ejecutar 100 y anunciar)",
    "A4": "búsqueda nueva = consulta autosuficiente",
    "A5": "relativo temporal con referente único",
    "A6": "fallo de fuente: semantic_ref sí, result_set_ref no",
    "A7": "aclaración pendiente: respuesta, cancelación, búsqueda nueva",
    "A8": "rango vacío: la cota nueva sustituye la restricción VALUE",
    "A8.1": "rango degenerado a un punto: igual que A8; igualdad pedida explícita vale",
    "A9": "prueba estructural de autosuficiencia",
    "A9.1": "con SEARCH activa, sólo condiciones/modificadores = DELTA",
    "G-FU-03": "PendingClarification.candidate_patches[]",
    "U1": "SF166: año 2023 fuera del catálogo D10 ⇒ CLARIFY SMMLV_YEAR_AMBIGUOUS (control de no extrapolación)",
    "U2": "SF063: preestado normalizado a alcanzable (asks 3 -> 2)",
    "D1": "SF168: declarado prueba de estado persistido inválido",
    "U3": "R3: SF192–SF195 añadidos; etiquetado ciego A/B y adjudicación del usuario (gold/ADJUDICATION_R3.md)",
}


def sha_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def digest(value) -> str:
    return sha_bytes(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode("utf-8"))


def load(path: Path) -> list[dict]:
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def partition(gold: dict[str, list[dict]]) -> dict:
    out = {"DEV": {}, "TEST": {}}
    for slice_name, recs in gold.items():
        strata = collections.defaultdict(list)
        for r in recs:
            strata[str(r["expected_disposition"])].append(r["gold_id"])
        dev, test = [], []
        for _, ids in sorted(strata.items()):
            ordered = sorted(ids, key=lambda i: sha_bytes(f"{PARTITION_SALT}:{i}".encode()))
            for k, i in enumerate(ordered):
                (test if k % 2 == 0 else dev).append(i)
        out["DEV"][slice_name] = sorted(dev)
        out["TEST"][slice_name] = sorted(test)
    return out


def body(real_dir: Path | None, previous: dict | None) -> dict:
    files = {p: {"sha256": sha_bytes((ROOT / p).read_bytes())} for p in PUBLIC_FILES}
    for p in PUBLIC_FILES:
        if p.endswith(".jsonl"):
            files[p]["records"] = len(load(ROOT / p))
    idx = json.loads((ROOT / f"{GOLD}/M2A_GOLD_AND_FRONTIER_INDEX.json").read_text(encoding="utf-8"))
    syn_gold = load(ROOT / f"{GOLD}/M2A_GOLD_synthetic.jsonl")
    syn_front = load(ROOT / f"{GOLD}/FOLLOWUP_FRONTIER_synthetic.jsonl")
    slices = {"M2A_GOLD": {}, "FOLLOWUP_FRONTIER": {}}
    for name in ("REAL", "SYNTHETIC"):
        g, f = idx["slices"][name]["M2A_GOLD"], idx["slices"][name]["FOLLOWUP_FRONTIER"]
        slices["M2A_GOLD"][name] = {"n": g["n"], "ids": sorted(g["ids"]), "by_disposition": g["by_disposition"],
                                    "index_sha256": g["sha256"]}
        slices["FOLLOWUP_FRONTIER"][name] = {"n": f["n"], "index_sha256": f["sha256"], "by_reason": f["by_reason"],
                                             "records": sorted(({"gold_id": r["gold_id"], "frontier_reason": r["frontier_reason"],
                                                                 "gap_ids": r["gap_ids"]} for r in f["records"]),
                                                               key=lambda r: r["gold_id"])}
    totals = {"M2A_GOLD": {"REAL": slices["M2A_GOLD"]["REAL"]["n"], "SYNTHETIC": slices["M2A_GOLD"]["SYNTHETIC"]["n"]},
              "FOLLOWUP_FRONTIER": {"REAL": slices["FOLLOWUP_FRONTIER"]["REAL"]["n"],
                                    "SYNTHETIC": slices["FOLLOWUP_FRONTIER"]["SYNTHETIC"]["n"]}}
    for k in totals:
        totals[k]["TOTAL"] = totals[k]["REAL"] + totals[k]["SYNTHETIC"]
    record_sha = {"SYNTHETIC": {r["gold_id"]: digest(r) for r in syn_gold + syn_front}}
    real_private = {"location": REAL_LOCATION, "policy": "texto REAL fuera de git; sólo ids, conteos y digests"}
    adjudicated = collections.defaultdict(list)
    classified = {}
    gold_real = None
    if real_dir is not None:
        gold_real = load(real_dir / "M2A_GOLD_real.jsonl")
        front_real = load(real_dir / "FOLLOWUP_FRONTIER_real.jsonl")
        real_private["files"] = {
            "M2A_GOLD_real.jsonl": {"sha256": sha_bytes((real_dir / "M2A_GOLD_real.jsonl").read_bytes()), "records": len(gold_real)},
            "FOLLOWUP_FRONTIER_real.jsonl": {"sha256": sha_bytes((real_dir / "FOLLOWUP_FRONTIER_real.jsonl").read_bytes()),
                                             "records": len(front_real)},
        }
        record_sha["REAL"] = {r["gold_id"]: digest(r) for r in gold_real + front_real}
        for r in gold_real + front_real + syn_gold + syn_front:
            for a in r.get("adjudicated_by") or []:
                adjudicated[a].append(r["gold_id"])
            if "orchestrator_classification" in r:
                classified[r["gold_id"]] = r["orchestrator_classification"]
    elif previous is not None:  # CI: carry the private-side facts, they cannot be recomputed without the files
        real_private["files"] = previous["real_private"]["files"]
        record_sha["REAL"] = previous["record_sha256"]["REAL"]
        adjudicated = previous["adjudications"]["records"]
        classified = previous["adjudications"]["orchestrator_classification"]
        for name, real_n in (("M2A_GOLD_real.jsonl", totals["M2A_GOLD"]["REAL"]),
                             ("FOLLOWUP_FRONTIER_real.jsonl", totals["FOLLOWUP_FRONTIER"]["REAL"])):
            if real_private["files"][name]["records"] != real_n:
                raise SystemExit(f"REAL count mismatch for {name}")
        if real_private["files"]["M2A_GOLD_real.jsonl"]["sha256"] != slices["M2A_GOLD"]["REAL"]["index_sha256"]:
            raise SystemExit("REAL GOLD sha differs from the public index")
    real_ids = set(slices["M2A_GOLD"]["REAL"]["ids"]) | {r["gold_id"] for r in slices["FOLLOWUP_FRONTIER"]["REAL"]["records"]}
    if set(record_sha.get("REAL", {})) and set(record_sha["REAL"]) != real_ids:
        raise SystemExit("REAL record ids differ from the index")
    gold_for_partition = {"SYNTHETIC": syn_gold}
    if gold_real is not None:
        gold_for_partition["REAL"] = gold_real
    part = partition(gold_for_partition)
    if previous is not None and "partition" in previous:  # sticky: an id keeps its DEV/TEST side across re-seals
        for slice_name, recs in gold_for_partition.items():
            ids = {r["gold_id"] for r in recs}
            prev = {side: set(previous["partition"][side].get(slice_name, [])) for side in ("DEV", "TEST")}
            new_ids = ids - prev["DEV"] - prev["TEST"]
            for side in ("DEV", "TEST"):
                part[side][slice_name] = sorted((prev[side] & ids) | (set(part[side][slice_name]) & new_ids))
    if gold_real is None and previous is not None:
        for side in ("DEV", "TEST"):
            part[side]["REAL"] = previous["partition"][side]["REAL"]
    return {
        "seal_schema": "m2a-gold-seal/1",
        "gold_schema": idx["schema"],
        "sealed_on": "2026-10-02",  # release r3 (r1/r2: 2026-10-01 on main 8c1f8c54)
        "base_commit": "fa0b6de8",
        "totals": totals,
        "slices": slices,
        "public_files": files,
        "record_sha256": record_sha,
        "real_private": real_private,
        "interpreting_rules": {p: sha_bytes((ROOT / p).read_bytes()) for p in RULE_FILES},
        "adjudications": {"rules": ADJUDICATIONS, "records": {k: sorted(v) for k, v in sorted(adjudicated.items())},
                          "orchestrator_classification": dict(sorted(classified.items())),
                          "note": "R0205 entra en M2A_GOLD por A8.1 + money_comparators («en adelante» = GTE) sin re-etiquetado. "
                "Única re-etiqueta: SF166 (U1, usuario 2026-10-02); SF063 sólo normaliza el preestado (U2); "
                "SF168 se declara (D1). r3: U3 añade SF192–SF195 desde las etiquetas ciegas A/B + adjudicación."},
        "partition": {"rule": "por slice y disposición; orden sha256(salt:id); posiciones pares → TEST, impares → DEV; "
                              "fija desde el sello r2: un id conserva su lado en re-sellados posteriores",
                      "salt": PARTITION_SALT, **part},
        "gates": {"sealed_gold_reproduces_from_candidate": _sealed_reproduces(),
                  "contract_dependencies_reproduce": _dependencies_reproduce(),
                  "import_manifest_ok": _import_manifest_ok(),
                  "representability": {"checker": CHECKER, "report": REPORT,
                                       "report_sha256": sha_bytes((ROOT / REPORT).read_bytes()),
                                       "violations": len(json.loads((ROOT / REPORT).read_text(encoding="utf-8"))["violations"])}},
    }


def _import_manifest_ok() -> bool:
    for line in (ROOT / "docs/audits/m2a_conversation/IMPORT_MANIFEST.sha256").read_text(encoding="utf-8").splitlines():
        digest, path = line.split(maxsplit=1)
        if sha_bytes((ROOT / path).read_bytes()) != digest:
            return False
    return True


def _dependencies_reproduce() -> bool:
    """U19: the §19 extraction is regenerated from the contract and must match the committed file byte for byte."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("s19", ROOT / S19_EXTRACTOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    try:
        return (ROOT / GOLD / "CONTRACT_S19_DEPENDENCIES.json").read_text(encoding="utf-8") == mod.render()
    except SystemExit:
        return False


def _sealed_reproduces() -> bool:
    import importlib.util
    spec = importlib.util.spec_from_file_location("bsg", ROOT / "scripts/audits/m2a_conversation/build_sealed_gold.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return all((ROOT / GOLD / n).read_text(encoding="utf-8") == body for n, body in mod.build().items())


def seal_of(b: dict) -> dict:
    return {**b, "seal_sha256": digest(b)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=("build", "verify"))
    ap.add_argument("--real-dir", type=Path)
    a = ap.parse_args()
    if a.cmd == "build":
        if a.real_dir is None:
            raise SystemExit("build needs --real-dir")
        previous = json.loads(SEAL.read_text(encoding="utf-8")) if SEAL.exists() else None
        s = seal_of(body(a.real_dir, previous))
        SEAL.parent.mkdir(parents=True, exist_ok=True)
        SEAL.write_text(json.dumps(s, ensure_ascii=False, indent=1, sort_keys=True) + "\n", encoding="utf-8")
        print("sealed", s["seal_sha256"], json.dumps(s["totals"]))
        return 0
    stored = json.loads(SEAL.read_text(encoding="utf-8"))
    fresh = seal_of(body(a.real_dir, stored))
    if fresh != stored:
        diff = sorted(k for k in set(fresh) | set(stored) if fresh.get(k) != stored.get(k))
        print("SEAL BROKEN:", diff)
        return 1
    part = {side: stored["partition"][side] for side in ("DEV", "TEST")}
    if digest(part) != PARTITION_SHA256:
        print("SEAL BROKEN: DEV/TEST partition differs from the frozen pin")
        return 1
    if not stored["gates"]["import_manifest_ok"]:
        print("SEAL BROKEN: imported advisory tree differs from IMPORT_MANIFEST.sha256")
        return 1
    if not stored["gates"]["sealed_gold_reproduces_from_candidate"]:
        print("SEAL BROKEN: sealed gold does not reproduce from the candidate + adjudications")
        return 1
    if not stored["gates"]["contract_dependencies_reproduce"]:
        print("SEAL BROKEN: the §19 extraction does not reproduce from the contract")
        return 1
    if stored["gates"]["representability"]["violations"] != 0:
        print("SEAL BROKEN: representability violations")
        return 1
    print("seal OK", stored["seal_sha256"], "REAL", "recomputed" if a.real_dir else "carried (private dir absent)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
