"""Mutation check of ``check_m2a_gold_contract.py``.

Each mutant breaks one contract rule in a copy of the synthetic Gold; the checker must report a violation for that
record (or for the partition). Part of the reproducible M2A-0 gate.

  python scripts/audits/m2a_conversation/mutate_check_m2a_gold_contract.py [--scratch-dir DIR]

``--scratch-dir`` defaults to a fresh temporary directory and must lie outside the repository (the copies of the Gold
must never land in the worktree). Exit 1 if any mutant survives.
"""

from __future__ import annotations

import argparse
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
CANDIDATE = ROOT / "docs/audits/m2a_advisory_2026-10-01/02_gold_followups/gold_candidato/M2A_GOLD_synthetic.jsonl"


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

    def asks_three(L):
        L["prior"]["pending"]["asks"] = 3

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

    # ---- R3 (contract amendment, release r3): each mutant must die by ITS rule -------------------------------------
    def executed(L, resulting):
        L["expected_disposition"], L["disposition_reason"], L["alternatives"] = "EXECUTE", None, []
        L["resulting_ir"] = resulting

    def sf192_keeps_cardinality(L):  # «sin orden» executed keeping top-10: check_ir «cardinality without order»
        executed(L, {**L["prior"]["ir"], "order": None})

    def sf192_drops_cardinality(L):  # CARDINALITY removed without a cited op: G3
        executed(L, {**L["prior"]["ir"], "order": None, "cardinality": None})

    def sf192_reason_outside_a3(L):
        L["disposition_reason"] = "CARDINALITY_WITHOUT_ORDER"

    def sf192_seals_set_order_candidate(L):  # SET_ORDER candidates are E-ORDER's (§19), never sealed: G9
        L["alternatives"] = L["alternatives"] + [{**L["prior"]["ir"], "order": {"direction_source": "DEFAULTED",
                                                                                "operator": "date_desc"}}]

    def sf193_keep_on_present_slot(L):  # the anchor HAS a buyer: KEEP_ON_EMPTY_SLOT is incoherent (G9)
        L["prior"]["ir"]["conditions"].append({"dimension": "BUYER", "id": "c9", "level": None, "operator": "is",
                                               "polarity": "POSITIVE", "unit": None, "value_hint": "INVIAS"})

    def sf194_executed(L):  # negation over several positives executed: G10 F16 fail-closed branch
        executed(L, {**L["prior"]["ir"], "conditions": L["prior"]["ir"]["conditions"] + [
            {"dimension": "TOPIC", "id": "c4", "level": None, "operator": "contains", "polarity": "NEGATIVE",
             "unit": None, "value_hint": "cafetería"}]})

    def sf194_wrong_dimension(L):  # R3-4: the code names the dimension of the slot (G10)
        L["disposition_reason"] = "UNRESOLVED_VALUE:BUYER"

    def sf194_non_slot_dimension(L):  # R3-3: <DIM> never a dimension that is not a slot (G1 tail)
        L["disposition_reason"] = "UNRESOLVED_VALUE:RETRIEVAL_INTENT"

    def p3_same_patch(L):  # P-3: +X and −X in one patch never resolved by F16 (G10)
        neg = next(o for o in L["ops"] if o["op"] == "SET" and o["polarity"] == "NEGATIVE")
        L["ops"] = L["ops"] + [{**neg, "polarity": "POSITIVE", "quote": "de " + str(neg["value_hint"]),
                                 "refers_to_result_set": True}]  # continuation: only P-3 may kill it

    def sf195_drops_cardinality(L):  # CLEAR(ORDER)+SET_ORDER keeps CARDINALITY (F10): G3
        L["resulting_ir"] = {**L["resulting_ir"], "cardinality": None}

    # ---- R3b (ZEUS NO_GO repair): disposition <-> code pairs (A.3), conflict, INFORM and expired anchors ------------
    def reason_is(code):
        return lambda L: L.update(disposition_reason=code)

    def sf113_inform_with_alternative(L):  # INFORM never leaves a pending (§8.4)
        L["alternatives"] = [L["prior"]["ir"]]

    def sf138_other_pending_over_expired(L):  # over an expired anchor only CONTEXT_EXPIRED_CONFIRM (§6 H1)
        L["disposition_reason"] = "KEEP_ON_EMPTY_SLOT"
        L["ops"] = L["ops"] + [{"op": "KEEP", "slot": "BUYER", "operator": None, "polarity": "POSITIVE", "level": None,
                                "quote": "con la misma entidad", "value_hint": None, "unresolved": True,
                                "refers_to_result_set": False, "depends_on_prior_value": True,
                                "inherited": {"year_source": None, "unit_source": None}}]

    # ---- U25 (R3b-2): two causes, 4b scope, KEEP + SET executed, schema O5, answer attribution -------------------
    def sf196_two_causes_clarify(L):  # §6 3b: two causes never ask
        L["expected_disposition"] = "CLARIFY"

    def sf196_two_causes_lower_code(L):  # §6 3b: the top code of the closed precedence, not another one
        L["disposition_reason"] = "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"

    def sf199_out_scope_reask(L):  # §7.1 4b is INFORM UNSUPPORTED_PROPOSAL:<motivo>, never REASK
        L.update(expected_disposition="REASK", disposition_reason="ANSWER_UNRESOLVED")

    def sf199_unsupported_in_scope_in(L):  # UNSUPPORTED_PROPOSAL:<motivo> lives in its own scope
        L["m2a_scope"] = "IN"

    def sf197_conflict_executed(L):  # KEEP + SET of one slot never executes (U22)
        res = copy.deepcopy(L["prior"]["ir"])
        res["conditions"] = [{**c, "value_hint": "Alcaldía de Villavicencio"} if c["dimension"] == "BUYER" else c
                             for c in res["conditions"]]
        L.update(expected_disposition="EXECUTE", disposition_reason=None, resulting_ir=res)

    def sf198_null_inherited_on_set(L):  # schema 2.1 O5: only a KEEP inherits nothing (null)
        next(o for o in L["ops"] if o["op"] == "SET")["inherited"] = None

    def sf200_answer_loses_other_slot(L):  # the selected candidate's slots only (§8.3 «Selección»), never TIME
        L["resulting_ir"] = {**L["resulting_ir"], "conditions": [
            c for c in L["resulting_ir"]["conditions"] if c["dimension"] != "TIME"]}

    # ---- R3c (ZEUS NO_GO on 7b30b221, zmut.py): G3 regression, R3-1 answers, expired loop, 4b motives --------------
    def z1_sf200_keeps_base_order(L):  # only CLEAR(CARDINALITY) over the frame: not C1
        L["resulting_ir"] = {**L["resulting_ir"], "order": copy.deepcopy(L["prior"]["ir"]["order"])}

    def z2_sf059_drops_carried_geo(L):  # a carried condition lost without a cited op (the G3 regression of R3b-2)
        L["prior"]["ir"]["conditions"].append({"dimension": "GEOGRAPHY", "id": "cg", "level": "MUNICIPALITY",
                                               "operator": "is", "polarity": "POSITIVE", "unit": None,
                                               "value_hint": "Cali"})

    def z3_sf200_invents_order(L):  # an un-cited new ORDER although C1 removes it
        L["resulting_ir"] = {**L["resulting_ir"], "order": {"direction_source": "DEFAULTED", "operator": "date_desc"}}

    def z4_sf199_reask(L):  # the pre-U25 A reading, against the sealed §19 dependency on M2A-5
        L.update(expected_disposition="REASK", disposition_reason="ANSWER_UNRESOLVED", m2a_scope="IN")

    def z5_sf198_expired_confirm_loop(L):  # «continuar» with a refusal cause asks forever (H1)
        L.update(expected_disposition="CLARIFY", disposition_reason="CONTEXT_EXPIRED_CONFIRM", effects=[],
                 expected_echo=["CONTEXT_EXPIRED"])

    def z11_sf199_other_motive(L):  # 4b only for motives with a sealed case (G8)
        L["disposition_reason"] = "UNSUPPORTED_PROPOSAL:NO_ACTION"

    return {
        "ZEUS_R3b_Z1_SF200_keeps_base_order": ("SF200", z1_sf200_keeps_base_order),
        "ZEUS_R3b_Z2_SF059_drops_carried_geo": ("SF059", z2_sf059_drops_carried_geo),
        "ZEUS_R3b_Z3_SF200_invents_order": ("SF200", z3_sf200_invents_order),
        "ZEUS_R3b_Z4_SF199_reask": ("SF199", z4_sf199_reask),
        "ZEUS_R3b_Z5_SF198_expired_confirm_loop": ("SF198", z5_sf198_expired_confirm_loop),
        "ZEUS_R3b_Z11_SF199_other_motive": ("SF199", z11_sf199_other_motive),
        "U25_SF196_two_causes_clarify": ("SF196", sf196_two_causes_clarify),
        "U25_SF196_two_causes_lower_code": ("SF196", sf196_two_causes_lower_code),
        "U25_SF199_out_scope_reask": ("SF199", sf199_out_scope_reask),
        "U25_SF199_unsupported_in_scope_IN": ("SF199", sf199_unsupported_in_scope_in),
        "U25_SF197_conflict_executed": ("SF197", sf197_conflict_executed),
        "U25_SF198_null_inherited_on_set": ("SF198", sf198_null_inherited_on_set),
        "U25_SF200_answer_loses_other_slot": ("SF200", sf200_answer_loses_other_slot),
        "R3b_ZEUS_SF193_CLARIFY_with_REASK_code": ("SF193", reason_is("PENDING_UNANSWERED")),
        "R3b_ZEUS_SF193_CLARIFY_with_INFORM_only_code": ("SF193", reason_is("REFERENT_EXTENSIONAL")),
        "R3b_ZEUS_SF193_CLARIFY_with_UNSUPPORTED_PROPOSAL": ("SF193", reason_is("UNSUPPORTED_PROPOSAL:NO_ACTION")),
        "R3b_ZEUS_SF193_CLARIFY_with_OPERATION_CHANGE": ("SF193", reason_is("OPERATION_CHANGE:SEARCH")),
        "R3b_SF113_INFORM_with_REASK_code": ("SF113", reason_is("ANSWER_UNRESOLVED")),
        "R3b_SF113_INFORM_with_CONTEXT_EXPIRED_CONFIRM": ("SF113", reason_is("CONTEXT_EXPIRED_CONFIRM")),
        "R3b_SF001_EXECUTE_with_reason": ("SF001", reason_is("KEEP_ON_EMPTY_SLOT")),
        "R3b_SF193_conflict_without_conflicting_ops": ("SF193", reason_is("DELTA_SLOT_CONFLICT")),
        "R3b_SF113_INFORM_with_alternative": ("SF113", sf113_inform_with_alternative),
        "R3b_SF138_other_pending_over_expired_anchor": ("SF138", sf138_other_pending_over_expired),
        "R3_SF192_executed_keeps_cardinality": ("SF192", sf192_keeps_cardinality),
        "R3_SF192_executed_drops_cardinality": ("SF192", sf192_drops_cardinality),
        "R3_SF192_reason_outside_A3": ("SF192", sf192_reason_outside_a3),
        "R3_SF192_seals_set_order_candidate": ("SF192", sf192_seals_set_order_candidate),
        "R3_SF193_keep_on_present_slot": ("SF193", sf193_keep_on_present_slot),
        "R3_SF194_executed": ("SF194", sf194_executed),
        "R3_SF194_wrong_dimension": ("SF194", sf194_wrong_dimension),
        "R3_SF194_non_slot_dimension": ("SF194", sf194_non_slot_dimension),
        "R3_P3_SF001_same_patch_both_polarities": ("SF001", p3_same_patch),
        "R3_SF195_drops_cardinality": ("SF195", sf195_drops_cardinality),
        "U2_SF063_asks_three": ("SF063", asks_three),
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


def record_mutants(rows: list[dict]) -> dict:
    """Mutants that replace or alter a whole record (isolated G11 rules)."""
    original = {r["gold_id"]: r for r in chk.load(CANDIDATE)}

    def unpatched_sf166(rows_):
        return [copy.deepcopy(original["SF166"]) if r["gold_id"] == "SF166" else r for r in rows_]

    def d1_declaration_removed(rows_):
        out = copy.deepcopy(rows_)
        for r in out:
            if r["gold_id"] == "SF168":
                r.pop("invalid_persisted_state", None)
        return out

    def sf192_partial_declaration_removed(rows_):  # R3b: the sealed C1-only alternatives must be declared partial
        out = copy.deepcopy(rows_)
        for r in out:
            if r["gold_id"] == "SF192":
                r.pop("partial_alternatives", None)
        return out

    return {"U1_SF166_unpatched_2023_executed": ("SF166", unpatched_sf166),
            "D1_SF168_declaration_removed": ("SF168", d1_declaration_removed),
            "R3b_SF192_partial_declaration_removed": ("SF192", sf192_partial_declaration_removed)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Mutation check of check_m2a_gold_contract.py")
    ap.add_argument("--scratch-dir", type=Path, help="where mutated copies are written (outside the repository)")
    a = ap.parse_args()
    scratch = (a.scratch_dir or Path(tempfile.mkdtemp(prefix="m2a_mut_"))).resolve()
    if scratch == ROOT or ROOT in scratch.parents:
        ap.error(f"--scratch-dir must be outside the repository: {scratch}")
    scratch.mkdir(parents=True, exist_ok=True)
    rows = chk.load(SRC / "M2A_GOLD_synthetic.jsonl")
    ok = True
    for name, (gid, fn) in mutants(rows).items():
        mutated = copy.deepcopy(rows)
        both(next(x for x in mutated if x["gold_id"] == gid), fn)
        out = run_on(scratch, name, mutated)
        killed = any(v.startswith(gid) for v in out["violations"])
        ok &= killed
        print("KILLED" if killed else "SURVIVED", name, gid)
    for name, (gid, fn) in record_mutants(rows).items():
        out = run_on(scratch, name, fn(rows))
        killed = [v for v in out["violations"] if v.startswith(gid)]
        expected = "declaring its alternatives partial" if name.startswith("R3b_") else "D10 catalog"
        isolated = bool(killed) and all(expected in v for v in killed)  # its own rule's message only
        ok &= isolated
        print("KILLED" if isolated else "SURVIVED", name, gid, "(its rule only)" if isolated else killed[:2])
    frontier = chk.load(SRC / "FOLLOWUP_FRONTIER_synthetic.jsonl")
    out = run_on(scratch, "M7_frontier_absorbed", rows + [next(x for x in frontier if x["gold_id"] == "SF038")])
    killed = any("SF038" in v for v in out["violations"])
    ok &= killed
    print("KILLED" if killed else "SURVIVED", "M7_frontier_absorbed", "SF038")
    print("ALL_KILLED", ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
