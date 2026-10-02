"""Mutation check of ``check_m2a_gold_contract.py``.

Each mutant breaks one contract rule in a copy of the synthetic Gold; the checker must report a violation for that
record (or for the partition). Usage: ``python <this> [<scratch_dir>]``. Exit 1 if any mutant survives.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SPEC = importlib.util.spec_from_file_location("chk", ROOT / "scripts/audits/m2a_conversation/check_m2a_gold_contract.py")
chk = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(chk)
SRC = chk.GOLD_DIR


def both(rec: dict, fn) -> None:
    for k in ("label_A", "label_B"):
        fn(rec[k])


def prior_conditions(L: dict) -> list[dict]:
    return (L["prior"]["ir"] or {"conditions": []})["conditions"]


def pick(rows: list[dict], pred) -> str:
    return next(r["gold_id"] for r in rows if pred(r))


def mutants(rows: list[dict]) -> dict:
    def carry_lost(L):
        L["resulting_ir"]["conditions"] = [c for c in L["resulting_ir"]["conditions"] if c["dimension"] != "BUYER"]

    def unknown_op(L):
        for o in L["ops"]:
            if o["op"] == "SET_UNIVERSE":
                o["op"] = "SET_SCOPE"

    def new_search_with_patch(L):
        L["ops"] = [{"op": "SET", "slot": "TIME", "operator": "during_year", "polarity": "POSITIVE", "level": None,
                     "quote": "x", "value_hint": "2020", "unresolved": False, "refers_to_result_set": False,
                     "depends_on_prior_value": False, "inherited": {"year_source": "STATED", "unit_source": None}}]

    def a8_nondegenerate(L):
        for c in L["prior"]["ir"]["conditions"]:
            if c["dimension"] == "VALUE":
                c["value_hint"] = "999999 millones" if c["operator"].startswith("less") else "1 millones"

    def retired_reason(L):
        L["disposition_reason"] = "SMMLV_UNSUPPORTED"

    def a91_new_search(L):
        L["turn_kind"] = "NEW_SEARCH"
        L["ops"] = []

    def answer_wrong_slot(L):
        for o in L["ops"]:
            if o["op"] == "ANSWER":
                o["slot"] = "MODALITY"

    def operator_not_in_main(L):
        for c in L["resulting_ir"]["conditions"]:
            if c["dimension"] == "TIME":
                c["operator"] = "during_week"

    def executed_empty_interval(L):
        for c in L["resulting_ir"]["conditions"]:
            if c["dimension"] == "VALUE":
                c["value_hint"] = "900 millones" if c["operator"].startswith("greater") else "100 millones"

    def universe_without_op(L):
        L["resulting_ir"]["universe"] = "PROCESSES" if L["resulting_ir"]["universe"] != "PROCESSES" else "CONTRACTS"

    def a91_target(r):
        L = r["label_A"]
        if L["turn_kind"] != "DELTA" or L["prior"]["state"] != "EXECUTED" or not L["resulting_ir"]:
            return False
        if any(o["slot"] in ("TOPIC", "ACTOR") for o in L["ops"]):
            return False
        anchor = {(p["dimension"], p["value_hint"]) for p in prior_conditions(L)}
        return not any(c["dimension"] in ("TOPIC", "ACTOR") and c["polarity"] == "POSITIVE"
                       and (c["dimension"], c["value_hint"]) not in anchor for c in L["resulting_ir"]["conditions"])

    def to_new_search_empty(L):
        L["turn_kind"] = "NEW_SEARCH"
        L["expected_disposition"] = "NEW_SEARCH"
        L["disposition_reason"] = None
        L["ops"] = []
        L["resulting_ir"] = {**(L["prior"]["ir"] or {}), "conditions": []}

    def to_pending_unanswered(L):
        L["expected_disposition"] = "REASK"
        L["disposition_reason"] = "PENDING_UNANSWERED"
        L["resulting_ir"] = None

    def to_no_anchor(L):
        L["expected_disposition"] = "CLARIFY"
        L["disposition_reason"] = "NO_ANCHOR"
        L["resulting_ir"] = None

    def to_expired(L):
        L["expected_disposition"] = "CLARIFY"
        L["disposition_reason"] = "CONTEXT_EXPIRED_CONFIRM"

    def answer_plus_edit_executed(L):
        L["expected_disposition"] = "EXECUTE"
        L["disposition_reason"] = None

    def mixed_units_replaced(L):
        for c in L["prior"]["ir"]["conditions"]:
            if c["dimension"] == "VALUE":
                c["unit"] = "SMMLV"

    def hybrid_over_pending(L):
        for o in L["ops"]:
            if o["op"] == "ANSWER":
                o["op"] = "SET"
        L["turn_kind"] = "DELTA"
        L["expected_disposition"] = "EXECUTE"
        L["disposition_reason"] = None
        L["resulting_ir"] = copy.deepcopy(L["prior"]["ir"])

    def inherit_from_two_units(L):
        for o in L["ops"]:
            if o["slot"] and o["slot"].startswith("VALUE"):
                o["inherited"]["unit_source"] = "PREVIOUS_ACCEPTED_VALUE"
        L["expected_disposition"] = "EXECUTE"
        L["disposition_reason"] = None

    def smmlv_multiyear_executed(L):
        L["expected_disposition"] = "EXECUTE"
        L["disposition_reason"] = None
        L["resulting_ir"] = copy.deepcopy(L["prior"]["ir"])

    return {
        "V3b_SF051_hybrid_over_pending": ("SF051", hybrid_over_pending),
        "V8_SF102_unit_from_two_units": ("SF102", inherit_from_two_units),
        "V9_SF168_smmlv_several_years": ("SF168", smmlv_multiyear_executed),
        "Z6_A8.1_on_mixed_units": ("SF090", mixed_units_replaced),
        "Z1_SF051_to_new_search_empty": ("SF051", to_new_search_empty),
        "Z2_SF056_to_pending_unanswered": ("SF056", to_pending_unanswered),
        "Z3_SF124_to_no_anchor": ("SF124", to_no_anchor),
        "Z4_SF141_to_expired": ("SF141", to_expired),
        "Z5_SF053_F25_executed": ("SF053", answer_plus_edit_executed),
        "M1_carry_lost": (pick(rows, lambda r: r["label_A"]["turn_kind"] == "DELTA" and r["expected_disposition"] == "EXECUTE"
                               and not any(o["slot"] == "BUYER" for o in r["label_A"]["ops"])
                               and any(c["dimension"] == "BUYER" for c in prior_conditions(r["label_A"]))), carry_lost),
        "M2_unknown_op": (pick(rows, lambda r: any(o["op"] == "SET_UNIVERSE" for o in r["label_A"]["ops"])), unknown_op),
        "M3_new_search_with_patch": (pick(rows, lambda r: r["label_A"]["turn_kind"] == "NEW_SEARCH"), new_search_with_patch),
        "M4_A8_nondegenerate": ("SF090", a8_nondegenerate),
        "M5_retired_reason": (pick(rows, lambda r: r["expected_disposition"] == "CLARIFY"), retired_reason),
        "M6_A9.1_new_search_without_topic": (pick(rows, a91_target), a91_new_search),
        "M8_answer_wrong_slot": (pick(rows, lambda r: any(o["op"] == "ANSWER" for o in r["label_A"]["ops"])), answer_wrong_slot),
        "M9_executed_empty_interval": ("SF087", executed_empty_interval),
        "M10_operator_not_in_main": (pick(rows, lambda r: r["label_A"]["resulting_ir"] is not None and any(
            c["dimension"] == "TIME" for c in r["label_A"]["resulting_ir"]["conditions"])), operator_not_in_main),
        "M11_universe_change_without_op": (pick(rows, lambda r: r["label_A"]["turn_kind"] == "DELTA"
                                                and r["expected_disposition"] == "EXECUTE"
                                                and not any(o["slot"] == "UNIVERSE" for o in r["label_A"]["ops"])
                                                and r["label_A"]["resulting_ir"]["universe"] != "UNSPECIFIED"),
                                           universe_without_op),
    }


def run_on(scratch: Path, name: str, rows: list[dict]) -> dict:
    d = scratch / f"mut_{name}"
    shutil.rmtree(d, ignore_errors=True)
    shutil.copytree(SRC, d)
    (d / "M2A_GOLD_synthetic.jsonl").write_text("\n".join(json.dumps(x, ensure_ascii=False, sort_keys=True) for x in rows))
    chk.GOLD_DIR = d
    try:
        return chk.run(None)
    finally:
        chk.GOLD_DIR = SRC


def main() -> int:
    scratch = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(tempfile.mkdtemp(prefix="m2a_mut_"))
    rows = chk.load(SRC / "M2A_GOLD_synthetic.jsonl")
    ok = True
    for name, (gid, fn) in mutants(rows).items():
        mutated = copy.deepcopy(rows)
        both(next(x for x in mutated if x["gold_id"] == gid), fn)
        out = run_on(scratch, name, mutated)
        killed = any(v.startswith(gid) for v in out["violations"])
        ok &= killed
        print("KILLED" if killed else "SURVIVED", name, gid)
    frontier = chk.load(SRC / "FOLLOWUP_FRONTIER_synthetic.jsonl")
    out = run_on(scratch, "M7_frontier_absorbed", rows + [next(x for x in frontier if x["gold_id"] == "SF038")])
    killed = any("SF038" in v for v in out["violations"])
    ok &= killed
    print("KILLED" if killed else "SURVIVED", "M7_frontier_absorbed", "SF038")
    print("ALL_KILLED", ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
