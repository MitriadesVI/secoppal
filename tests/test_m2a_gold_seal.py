"""M2A-0: the sealed follow-up Gold stays sealed and representable by M2A_CONVERSATION_CONTRACT_V1.

Public side only (the REAL Gold never enters git): synthetic records, ids, index, ledger, partition, rules and the
contract are recomputed against ``docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json``. With ``M2A_GOLD_REAL_DIR`` set to the
private GOLD_CANDIDATE directory the REAL digests are recomputed too.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SEAL = ROOT / "docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json"


def _module(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / f"scripts/audits/m2a_conversation/{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _real_dir() -> Path | None:
    value = os.environ.get("M2A_GOLD_REAL_DIR")
    return Path(value) if value else None


def test_seal_totals_are_the_adjudicated_ones():
    seal = json.loads(SEAL.read_text(encoding="utf-8"))
    assert seal["totals"]["M2A_GOLD"] == {"REAL": 49, "SYNTHETIC": 154, "TOTAL": 203}  # r3: + SF192–SF195 (U3)
    assert seal["totals"]["FOLLOWUP_FRONTIER"] == {"REAL": 16, "SYNTHETIC": 41, "TOTAL": 57}
    gold = set(seal["slices"]["M2A_GOLD"]["REAL"]["ids"]) | set(seal["slices"]["M2A_GOLD"]["SYNTHETIC"]["ids"])
    frontier = {r["gold_id"] for s in ("REAL", "SYNTHETIC") for r in seal["slices"]["FOLLOWUP_FRONTIER"][s]["records"]}
    assert not gold & frontier
    assert "R0205" in gold
    dev = {i for s in seal["partition"]["DEV"].values() for i in s}
    test = {i for s in seal["partition"]["TEST"].values() for i in s}
    assert not dev & test and dev | test == gold


# DEV/TEST partition frozen at seal 3aac2a20 (r2 kept it: 0 moves). Independent of the seal file: the seal is sticky and
# would otherwise only compare the partition with itself. New ids may be added later; then update this pin ONLY after
# checking that no existing id changed side (``m2a_gold_seal.py`` keeps sides), and declare it in the commit.
# r3 (2026-10-02): 0 moves, 4 additions (SF192/SF193/SF194 TEST, SF195 DEV); the r2 pin was 04197bbd….
PARTITION_SHA256 = "a73d77c94d46d425ec271414d15f8d6fdf5a42f27e8f94d1d4bdaf1353e6831f"


def test_partition_is_frozen():
    seal = json.loads(SEAL.read_text(encoding="utf-8"))
    part = {side: seal["partition"][side] for side in ("DEV", "TEST")}
    digest = hashlib.sha256(json.dumps(part, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
    assert digest == PARTITION_SHA256


def test_seal_recomputes_byte_for_byte():
    seal_mod = _module("m2a_gold_seal")
    stored = json.loads(SEAL.read_text(encoding="utf-8"))
    fresh = seal_mod.seal_of(seal_mod.body(_real_dir(), stored))
    assert fresh == stored


def test_gold_is_representable_by_the_contract():
    checker = _module("check_m2a_gold_contract")
    out = checker.run(_real_dir())
    assert out["violations"] == []
    assert out["slices"]["SYNTHETIC"] == {"gold": 154, "frontier": 41}


def test_sealed_gold_reproduces_from_candidate_and_adjudications():
    builder = _module("build_sealed_gold")
    gold_dir = ROOT / "docs/audits/m2a_conversation/gold"
    for name, body in builder.build().items():
        assert (gold_dir / name).read_text(encoding="utf-8") == body, name
    rows = {json.loads(x)["gold_id"]: json.loads(x)
            for x in (gold_dir / "M2A_GOLD_synthetic.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()}
    assert rows["SF166"]["expected_disposition"] == "CLARIFY"  # U1: 2023 outside the D10 catalog
    assert {rows["SF063"][k]["prior"]["pending"]["asks"] for k in ("label_A", "label_B")} == {2}  # U2
    assert rows["SF168"]["invalid_persisted_state"]  # D1
    r3 = {gid: rows[gid] for gid in ("SF192", "SF193", "SF194", "SF195")}  # U3: added from the blind labels A/B
    assert all(r["adjudicated_by"] == ["U3"] and r["primary_agreement"] for r in r3.values())
    assert {r["label_A"]["labeler"] for r in r3.values()} == {"A"} and {r["label_B"]["labeler"] for r in r3.values()} == {"B"}
    sf192 = r3["SF192"]
    c1 = {**sf192["label_A"]["prior"]["ir"], "order": None, "cardinality": None}
    assert sf192["label_A"]["alternatives"] == sf192["label_B"]["alternatives"] == [c1]  # only C1 is sealed (§19)


def test_the_contract_s19_extraction_reproduces_and_is_the_one_authority():
    """U19: §19 -> one generated, sealed extraction; SF075 depends on FOUNDATION_O1B_EXPLICIT_TIME_PARITY from it."""
    extractor = _module("extract_contract_dependencies")
    path = ROOT / "docs/audits/m2a_conversation/gold/CONTRACT_S19_DEPENDENCIES.json"
    assert path.read_text(encoding="utf-8") == extractor.render()
    extraction = json.loads(path.read_text(encoding="utf-8"))
    assert "FOUNDATION_O1B_EXPLICIT_TIME_PARITY" in extraction["case_owners"]["SF075"]
    assert extraction["source"]["contract_sha256"] == hashlib.sha256(
        (ROOT / "docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md").read_bytes()).hexdigest()
