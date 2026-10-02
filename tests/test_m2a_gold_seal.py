"""M2A-0: the sealed follow-up Gold stays sealed and representable by M2A_CONVERSATION_CONTRACT_V1.

Public side only (the REAL Gold never enters git): synthetic records, ids, index, ledger, partition, rules and the
contract are recomputed against ``docs/audits/m2a_conversation/gold/M2A_GOLD_SEAL.json``. With ``M2A_GOLD_REAL_DIR`` set to the
private GOLD_CANDIDATE directory the REAL digests are recomputed too.
"""

from __future__ import annotations

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
    assert seal["totals"]["M2A_GOLD"] == {"REAL": 49, "SYNTHETIC": 150, "TOTAL": 199}
    assert seal["totals"]["FOLLOWUP_FRONTIER"] == {"REAL": 16, "SYNTHETIC": 41, "TOTAL": 57}
    gold = set(seal["slices"]["M2A_GOLD"]["REAL"]["ids"]) | set(seal["slices"]["M2A_GOLD"]["SYNTHETIC"]["ids"])
    frontier = {r["gold_id"] for s in ("REAL", "SYNTHETIC") for r in seal["slices"]["FOLLOWUP_FRONTIER"][s]["records"]}
    assert not gold & frontier
    assert "R0205" in gold
    dev = {i for s in seal["partition"]["DEV"].values() for i in s}
    test = {i for s in seal["partition"]["TEST"].values() for i in s}
    assert not dev & test and dev | test == gold


def test_seal_recomputes_byte_for_byte():
    seal_mod = _module("m2a_gold_seal")
    stored = json.loads(SEAL.read_text(encoding="utf-8"))
    fresh = seal_mod.seal_of(seal_mod.body(_real_dir(), stored))
    assert fresh == stored


def test_gold_is_representable_by_the_contract():
    checker = _module("check_m2a_gold_contract")
    out = checker.run(_real_dir())
    assert out["violations"] == []
    assert out["slices"]["SYNTHETIC"] == {"gold": 150, "frontier": 41}
