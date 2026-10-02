"""M2A-0 gate: every M2A_GOLD record is representable by M2A_CONVERSATION_CONTRACT_V1.

Read-only audit (HERA). It never imports runtime code that executes anything: it reads the closed operator
table and the top-N cap of the integrated IR v2 so the Gold is checked against ``main``, not against a copy.

What it proves, per label (A and B) of every Gold record:
  G1  closed vocabulary: turn kind, scope, disposition, reason, effects, echo, op, slot, dimension, operator;
  G2  no record needs legacy params: the effective state is ``prior.ir`` + patch (+ declared host rules);
  G3  carry identity and attributed change (INV remoción sólo citada);
  G4  NEW_SEARCH is not a patch; ANSWER is a pending answer; CANCEL_PENDING is explicit control;
  G5  A8.1 is numeric over typed bounds (no lexicon) and A9.1 is structural over the turn's dimensions;
  G6  every CLARIFY with alternatives is a list of generic candidate patches;
  G7  rows/page need a result set: no protocol turn executes over a prior that never reached the source;
  G8  partition: GOLD ∩ FRONTIER = ∅, GOLD scopes ⊆ IN*, deferred ledger gaps never reach GOLD;
  G9  structural coherence of each disposition (host floor, window, pending answers, F25, two candidates).
It proves the Gold is REPRESENTABLE by the contract; it does not DERIVE each disposition from §7.2/§8.3, which needs
the producer's structural evidence (continuation, frame) and is a gate of M2A-8.

Output: counts and ids only (never text). Usage:
  python scripts/audits/m2a_conversation/check_m2a_gold_contract.py [--real-dir <private GOLD_CANDIDATE dir>] [--json out.json]
Exit 1 if any violation.
"""

from __future__ import annotations

import argparse
import collections
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from app.core.foundation_authority_values import MAX_TOP_N  # noqa: E402
from app.core.foundation_operator_table import OPERATORS  # noqa: E402
from app.core.semantic_dimensions import SemanticDimension  # noqa: E402

GOLD_DIR = ROOT / "docs/audits/m2a_advisory_2026-10-01/02_gold_followups/gold_candidato"

# --- Contract vocabulary (M2A_CONVERSATION_CONTRACT_V1 §4, §6, §7, §11) -------------------------------------
GOLD_OP_TO_CONTRACT = {
    "SET": "SET", "CLEAR": "CLEAR", "KEEP": "KEEP", "SET_ORDER": "SET_ORDER",
    "SET_UNIVERSE": "REPLACE_UNIVERSE",          # Gold 2.1 name -> contract name (§4.2)
    "ANSWER": "PENDING_ANSWER",                  # not a mutation: selects/fills a candidate patch (§8)
    "CANCEL_PENDING": "CANCEL_PENDING",          # control of the pending, not a mutation (§7.4)
}
SLOTS = {"TIME", "VALUE.lower", "VALUE.upper", "VALUE", "GEOGRAPHY", "BUYER", "ACTOR", "TOPIC", "STATE",
         "MODALITY", "CONTRACT_TYPE_OR_NATURE", "ORDER", "CARDINALITY", "UNIVERSE"}
TURN_KINDS = {"DELTA", "NEW_SEARCH", "ANSWER_TO_CLARIFICATION", "PROTOCOL"}
GOLD_SCOPES = {"IN", "IN_NEW_SEARCH", "IN_PROTOCOL"}
DISPOSITIONS = {"EXECUTE", "NEW_SEARCH", "CLARIFY", "REASK", "INFORM"}
REASONS = {
    "CONTEXT_EXPIRED_CONFIRM", "BUYER_OR_PLACE_AFTER_ACTOR", "REGION_NOT_IN_LIST", "RELATIVE_TO_PREVIOUS_VALUE",
    "UNIT_AMBIGUOUS", "MONTH_YEAR_AMBIGUOUS", "EMPTY_SEARCH_AFTER_DELTA", "STATE_NOT_IN_UNIVERSE",
    "TERRITORY_VS_CARRIED_BUYER", "NO_ANCHOR", "ANCHOR_UNREADABLE", "TARGET_NOT_ADDRESSABLE",
    "VALUE_WITHOUT_COMPARATOR", "SMMLV_YEAR_AMBIGUOUS", "OPTION_NOT_CHOSEN", "ANSWER_EDITS_OTHER_SLOT",
    "PENDING_UNANSWERED", "ANSWER_UNRESOLVED", "CLEAR_ON_EMPTY_SLOT", "REFERENT_EXTENSIONAL",
}
REASON_PREFIXES = ("UNRESOLVED_VALUE:", "OPERATION_CHANGE:")
RETIRED_REASONS = {"SMMLV_UNSUPPORTED", "INHERITED_UNIT_IMPLAUSIBLE", "TOPN_CAP_EXCEEDED"}  # A1, A2, P8
EFFECTS = {"VALUE_RANGE_REPLACED", "TOPIC_REPLACED", "POLARITY_INVERTED", "VIEW_REDERIVED", "UNIVERSE_CHANGED",
           "TOPN_CARRIED", "TOPN_CAPPED", "ORDER_DIRECTION_DEFAULTED", "PENDING_CANCELLED", "CONTEXT_RESET"}
ECHO = {"CARRIED", "UNIT_INHERITED", "UNIT_SHARED_IN_RANGE", "YEAR_INHERITED", "YEAR_CLOCK_INFERRED",
        "RELATIVE_WINDOW_RESOLVED", "VALUE_RANGE_REPLACED", "TOPIC_REPLACED", "POLARITY_INVERTED",
        "UNIVERSE_CHANGED", "TOPN_CARRIED", "TOPN_CAPPED", "ORDER_DIRECTION_DEFAULTED", "CONTEXT_RESET",
        "VIEW_DERIVED", "VIEW_REDERIVED", "CONTEXT_EXPIRED"}
# Gap codes the contract does not close: a GOLD record may not depend on them (contract §19).
LIVE_GAPS = {"OPPORTUNITY_STATE", "PAGE_VS_TOPN", "OPSWITCH", "TOTALES", "FIRMADOS", "ACTORTIME", "PENDING_OPSWITCH"}
# Surface forms of the first-turn VALUE reading (contract M1 / E-VALUE): M2a composes typed bounds, it does not
# read them. Allowed in GOLD only when both labels agree on the typed operation (the gap is a touch marker).
SURFACE_FORM_GAPS = {"B5RANGE", "COMPARATOR_FORM", "NEGATED_COMPARATOR"}
# A5 / ADR-103 D7 (OQ-3 = b): a temporal relative with a unique active referent resolves; the rest is G-FU-08.
RELATIVE_GAPS = {"RELATIVE_ANCHOR"}
M2A_OWNED_GAPS = {"G-FU-01", "G-FU-01b", "G-FU-02", "G-FU-02b", "G-FU-03"}
UNIT_SCALE = {"PESOS": 1, "MILES": 10**3, "MILLONES": 10**6, "MIL_MILLONES": 10**9, "BILLONES": 10**12}
GOLD_ONLY_DIMENSIONS = {"SOURCE"}  # Gold 2.1 IR keeps SOURCE as a condition; IR v2 carries it in source_evidence


def load(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def side(c: dict) -> str | None:
    if c["dimension"] != "VALUE":
        return None
    lower = c["operator"].startswith("greater")
    if c["polarity"] == "NEGATIVE":  # O4: a negated bound restricts the opposite side
        lower = not lower
    return "VALUE.lower" if lower else "VALUE.upper"


def slot_of(c: dict) -> str:
    return side(c) or c["dimension"]


def key(c: dict) -> tuple:
    return (c["dimension"], c["operator"], c["polarity"], c["value_hint"], c["level"], c["unit"])


def amount(c: dict) -> int | None:
    """Typed magnitude of a VALUE bound in COP; None if not comparable (SMMLV or unreadable)."""
    scale = UNIT_SCALE.get(c.get("unit") or "")
    m = re.search(r"\d[\d.]*(?:,\d+)?", c["value_hint"] or "")
    if scale is None or not m:
        return None
    return int(float(m.group(0).replace(".", "").replace(",", ".")) * scale)


def touched(ops: list[dict]) -> set[str]:
    out = set()
    for o in ops:
        s = o.get("slot")
        if s == "VALUE":
            out |= {"VALUE.lower", "VALUE.upper"}
        elif s:
            out.add(s)
    return out


def derive_effects(L: dict) -> set[str]:
    """Structural effects of Gold 2.1 §6 (REAL labels do not store the derived fields)."""
    prior, res, ops = L["prior"], L["resulting_ir"], L["ops"]
    pir, out = prior.get("ir"), set()
    if any(o["op"] == "CANCEL_PENDING" for o in ops) or (L["turn_kind"] == "NEW_SEARCH" and prior["state"] == "PENDING"):
        out.add("PENDING_CANCELLED")
    if (L["m2a_scope"] == "IN_NEW_SEARCH" and prior.get("owner") == "FOUNDATION_SEARCH"
            and prior["state"] in ("EXECUTED", "EXECUTED_EMPTY", "NOT_EXECUTED")):
        out.add("CONTEXT_RESET")
    if res is None or pir is None or L["turn_kind"] == "NEW_SEARCH":
        return out
    t = touched(ops)
    rk = {key(c) for c in res["conditions"]}
    explicit_clear = any(o["op"] == "CLEAR" and o["slot"] == "VALUE" for o in ops)
    for c in pir["conditions"]:
        s = side(c)
        if s and key(c) not in rk:
            own_side = any(o["slot"] == s and o["op"] in ("SET", "ANSWER") for o in ops)
            if not own_side and (explicit_clear or s not in t):
                out.add("VALUE_RANGE_REPLACED")
    for o in ops:
        if o["op"] == "SET" and o["polarity"] == "NEGATIVE" and o["slot"] and not o["slot"].startswith("VALUE"):
            pos = [c for c in pir["conditions"] if c["dimension"] == o["slot"] and c["polarity"] == "POSITIVE"]
            if len(pos) == 1 and pos[0]["value_hint"] == o["value_hint"] and key(pos[0]) not in rk:
                out.add("POLARITY_INVERTED")
    if pir.get("view_source") == "DERIVED" and res.get("product_view") != pir.get("product_view"):
        out.add("VIEW_REDERIVED")
    return out


class Report:
    def __init__(self) -> None:
        self.violations: list[str] = []
        self.counts: collections.Counter = collections.Counter()
        self.ids: dict[str, list[str]] = collections.defaultdict(list)

    def bad(self, gid: str, lab: str, msg: str) -> None:
        self.violations.append(f"{gid}/{lab}: {msg}")

    def note(self, name: str, gid: str) -> None:
        self.counts[name] += 1
        if gid not in self.ids[name]:
            self.ids[name].append(gid)


def check_ir(r: Report, gid: str, lab: str, ir: dict | None, where: str) -> None:
    if ir is None:
        return
    for c in ir["conditions"]:
        dim = c["dimension"]
        if dim in GOLD_ONLY_DIMENSIONS:
            r.note("gold_only_dimension_" + dim, gid)
            continue
        try:
            d = SemanticDimension(dim)
        except ValueError:
            r.bad(gid, lab, f"{where}: dimension {dim} not in SemanticDimension")
            continue
        if d is not SemanticDimension.UNSUPPORTED and c["operator"] not in OPERATORS.get(d, frozenset()):
            r.bad(gid, lab, f"{where}: operator {c['operator']} not allowed for {dim} in main")
        if (dim == "GEOGRAPHY") != (c["level"] is not None):
            r.bad(gid, lab, f"{where}: level iff GEOGRAPHY")
    o = ir.get("order")
    if o and o["operator"] not in OPERATORS[SemanticDimension.ORDER]:
        r.bad(gid, lab, f"{where}: order operator")
    ca = ir.get("cardinality")
    if ca:
        if not o:
            r.bad(gid, lab, f"{where}: cardinality without order")
        if ca["n"] != min(ca["n_requested"], MAX_TOP_N) or not 1 <= ca["n"] <= MAX_TOP_N:
            r.bad(gid, lab, f"{where}: cardinality n != min(n_requested, MAX_TOP_N)")
    lowers = [c for c in ir["conditions"] if side(c) == "VALUE.lower"]
    uppers = [c for c in ir["conditions"] if side(c) == "VALUE.upper"]
    if len(lowers) > 1 or len(uppers) > 1:
        r.bad(gid, lab, f"{where}: more than one bound per VALUE side")


def check_label(r: Report, rec: dict, lab: str, L: dict) -> None:
    gid = rec["gold_id"]
    prior, res, ops = L["prior"], L["resulting_ir"], L["ops"]
    tk, disp, reason = L["turn_kind"], L["expected_disposition"], L["disposition_reason"]
    # G1 closed vocabulary
    if tk not in TURN_KINDS:
        r.bad(gid, lab, f"turn_kind {tk}")
    if L["m2a_scope"] not in GOLD_SCOPES:
        r.bad(gid, lab, f"scope {L['m2a_scope']} outside IN*")
    if disp not in DISPOSITIONS:
        r.bad(gid, lab, f"disposition {disp}")
    if reason is not None and reason not in REASONS and not reason.startswith(REASON_PREFIXES):
        r.bad(gid, lab, f"reason {reason}")
    if reason in RETIRED_REASONS:
        r.bad(gid, lab, f"retired reason {reason}")
    eff = derive_effects(L)
    if "effects" in L:
        for e in L["effects"]:
            if e not in EFFECTS:
                r.bad(gid, lab, f"effect {e}")
        structural = {"VALUE_RANGE_REPLACED", "POLARITY_INVERTED", "PENDING_CANCELLED", "CONTEXT_RESET", "VIEW_REDERIVED"}
        if L["effects"] and disp in ("EXECUTE", "NEW_SEARCH") and (set(L["effects"]) & structural) != eff:
            r.bad(gid, lab, f"stored effects {sorted(set(L['effects']) & structural)} != derived {sorted(eff)}")
        if not L["effects"] and eff:
            r.note("effects_derived_labeler_left_empty", gid)
        eff |= set(L["effects"])
    else:
        r.note("effects_derived_not_stored", gid)
    L = {**L, "effects": sorted(eff)}
    for e in L.get("expected_echo", []):
        if e not in ECHO:
            r.bad(gid, lab, f"echo {e}")
    for o in ops:
        if o["op"] not in GOLD_OP_TO_CONTRACT:
            r.bad(gid, lab, f"op {o['op']}")
        if o["slot"] is not None and o["slot"] not in SLOTS:
            r.bad(gid, lab, f"slot {o['slot']}")
        if (o["slot"] is None) != (o["op"] == "CANCEL_PENDING"):
            r.bad(gid, lab, "slot null iff CANCEL_PENDING")
        r.counts["op:" + GOLD_OP_TO_CONTRACT.get(o["op"], "?")] += 1
    gaps = set(L.get("gap_codes") or [])
    for g in gaps & LIVE_GAPS:
        r.bad(gid, lab, f"depends on live gap {g}")
    if gaps & SURFACE_FORM_GAPS:
        other = rec["label_B" if lab == "A" else "label_A"]
        sig = lambda X: sorted((o["op"], o["slot"], o["operator"], o["polarity"]) for o in X["ops"])  # noqa: E731
        if sig(L) != sig(other):
            r.bad(gid, lab, "surface-form gap with labels disagreeing on the typed op")
        else:
            r.note("surface_form_read_by_VALUE_authority", gid)
    if gaps & RELATIVE_GAPS:
        pir0 = prior.get("ir") or {"conditions": []}
        times = [c for c in pir0["conditions"] if c["dimension"] == "TIME" and c["polarity"] == "POSITIVE"]
        rel = [o for o in ops if o["slot"] == "TIME" and o["depends_on_prior_value"]]
        if disp == "EXECUTE" and not (len(times) == 1 and rel and
                                      all(o["inherited"]["year_source"] == "PREVIOUS_ACCEPTED_TIME" for o in rel)):
            r.bad(gid, lab, "relative temporal executed without a unique active referent (A5)")
        else:
            r.note("A5_unique_temporal_referent", gid)
    check_ir(r, gid, lab, prior.get("ir"), "prior")
    check_ir(r, gid, lab, res, "result")
    for alt in L.get("alternatives") or []:
        check_ir(r, gid, lab, alt, "alternative")
    # G2 no legacy params anywhere
    blob = json.dumps(L)
    if "parsed_params" in blob:
        r.bad(gid, lab, "mentions parsed_params")

    pir = prior.get("ir")
    ctrl = [o for o in ops if o["op"] == "CANCEL_PENDING"]
    answers = [o for o in ops if o["op"] == "ANSWER"]
    patch_ops = [o for o in ops if o["op"] not in ("ANSWER", "CANCEL_PENDING")]

    # G4 turn shapes
    if tk == "NEW_SEARCH":
        r.note("shape:NEW_SEARCH_fresh_acceptance", gid)
        if patch_ops or answers:
            r.bad(gid, lab, "NEW_SEARCH carries patch ops (must be a fresh acceptance)")
        if prior["state"] == "PENDING":
            r.note("control:CANCEL_PENDING_by_new_search", gid)
            if "PENDING_CANCELLED" not in L["effects"]:
                r.bad(gid, lab, "NEW_SEARCH over a pending without PENDING_CANCELLED")
        if prior["state"] in ("EXECUTED", "EXECUTED_EMPTY", "NOT_EXECUTED") and "CONTEXT_RESET" not in L["effects"]:
            r.bad(gid, lab, "NEW_SEARCH over an anchor without CONTEXT_RESET")
    if ctrl:
        r.note("control:CANCEL_PENDING_explicit", gid)
        if prior["state"] != "PENDING":
            r.bad(gid, lab, "CANCEL_PENDING without a pending")
    if answers:
        pend = prior.get("pending")
        if prior["state"] != "PENDING" or not pend:
            r.bad(gid, lab, "ANSWER without a pending")
        else:
            for a in answers:
                if a["slot"] not in pend["candidate_slots"]:
                    r.bad(gid, lab, f"ANSWER slot {a['slot']} not a candidate slot")
            kind = "select_candidate" if L["input_kind"] == "EVENT" or pend.get("options") else "open_answer_patch"
            r.note("pending_answer:" + kind, gid)
            if patch_ops and disp == "EXECUTE":
                r.bad(gid, lab, "answer + other edit executed (F25 must REASK)")
    if prior["state"] == "PENDING":
        pend = prior.get("pending") or {}
        n = len(pend.get("candidate_slots") or [])
        r.note(f"pending:candidate_slots={n}", gid)
        r.note(f"pending:options={len(pend.get('options') or [])}", gid)

    # G6 alternatives = generic candidate patches (diff over slots only)
    alts = (L.get("alternatives") or []) if disp == "CLARIFY" else []
    if L.get("alternatives") and disp != "CLARIFY":
        r.note("alternative_reading_of_ambiguous_label", gid)
    if alts:
        r.note("clarify_with_candidate_patches", gid)
        base = pir or {"conditions": []}
        bk = {key(c) for c in base["conditions"]}
        for alt in alts:
            new = [c for c in alt["conditions"] if key(c) not in bk]
            slots = {slot_of(c) for c in new} | ({"UNIVERSE"} if alt["universe"] != (pir or {}).get("universe") else set())
            if not slots <= SLOTS:
                r.bad(gid, lab, f"alternative needs non-slot change {slots - SLOTS}")
            r.counts["candidate_patch_slots:" + "+".join(sorted(slots))] += 1

    # G3 carry identity / attribution, only where an executed effective exists over an anchor
    if res is not None and pir is not None and tk in ("DELTA", "ANSWER_TO_CLARIFICATION") and disp == "EXECUTE":
        t = touched(ops)
        rk = collections.Counter(key(c) for c in res["conditions"])
        for c in pir["conditions"]:
            s = slot_of(c)
            if s in t:
                continue
            if rk[key(c)] == 0:
                ok = (c["dimension"] == "VALUE" and "VALUE_RANGE_REPLACED" in L["effects"]) or (
                    "POLARITY_INVERTED" in L["effects"] and c["polarity"] == "POSITIVE")
                if ok:
                    r.note("removal_by_declared_host_rule", gid)
                else:
                    r.bad(gid, lab, f"carried condition {c['dimension']} lost without an op on its slot")
        pk = collections.Counter(key(c) for c in pir["conditions"])
        for c in res["conditions"]:
            if pk[key(c)] == 0 and slot_of(c) not in t and c["dimension"] not in t:
                r.bad(gid, lab, f"new condition {c['dimension']} without an op on its slot")
        if not ({"UNIVERSE"} & t) and res["universe"] != pir["universe"]:
            r.bad(gid, lab, "universe changed without REPLACE_UNIVERSE")
        if res["universe"] == pir["universe"] == "UNSPECIFIED" and res["product_view"] != pir["product_view"]:
            if "VIEW_REDERIVED" not in L["effects"]:
                r.bad(gid, lab, "derived view changed without VIEW_REDERIVED")
            r.note("view_rederived", gid)
        if "ORDER" not in t and res.get("order") != pir.get("order"):
            r.bad(gid, lab, "order changed without SET_ORDER")
        if "CARDINALITY" not in t and res.get("cardinality") != pir.get("cardinality"):
            r.bad(gid, lab, "cardinality changed without an op")
        r.note("delta_executed_carry_checked", gid)

    # G5a A8.1 numeric (no lexicon): the only way a VALUE bound vanishes without CLEAR is an empty/degenerate interval
    if "VALUE_RANGE_REPLACED" in L["effects"] and pir is not None and res is not None:
        explicit = any(o["op"] == "CLEAR" and o["slot"] == "VALUE" for o in ops)
        if explicit:
            r.note("value_replaced_by_explicit_clear", gid)
        else:
            new = [o for o in ops if o["op"] in ("SET", "ANSWER") and o["slot"] in ("VALUE.lower", "VALUE.upper")]
            kept_sides = {s for s in ("VALUE.lower", "VALUE.upper")} - {o["slot"] for o in new}
            olds = [c for c in pir["conditions"] if side(c) in kept_sides]
            news = [c for c in res["conditions"] if side(c) in {o["slot"] for o in new}]
            if len(olds) != 1 or len(news) != 1:
                r.bad(gid, lab, "A8.1 replacement not a single opposite-bound conflict")
            else:
                lo, hi = (news[0], olds[0]) if side(news[0]) == "VALUE.lower" else (olds[0], news[0])
                a, b = amount(lo), amount(hi)
                if a is None or b is None:
                    r.bad(gid, lab, "A8.1 applied to non-comparable amounts (must stay AMBIGUOUS)")
                elif a > b:
                    r.note("A8_empty_interval", gid)
                elif a == b:
                    r.note("A8.1_singleton_interval", gid)
                else:
                    r.bad(gid, lab, "VALUE_RANGE_REPLACED with a non-degenerate interval")
    # converse: a kept opposite bound never yields an empty/degenerate interval in an executed result
    if res is not None and disp in ("EXECUTE", "NEW_SEARCH"):
        lows = [c for c in res["conditions"] if side(c) == "VALUE.lower" and c["polarity"] == "POSITIVE"]
        ups = [c for c in res["conditions"] if side(c) == "VALUE.upper" and c["polarity"] == "POSITIVE"]
        if lows and ups:
            a, b = amount(lows[0]), amount(ups[0])
            if a is not None and b is not None and a >= b:
                quoted_singleton = tk == "NEW_SEARCH" or any(
                    o["slot"] in ("VALUE.lower", "VALUE.upper") and o.get("quote") and
                    sum(1 for p in ops if p.get("quote") == o.get("quote")) == 2 for o in ops)
                if a > b or not quoted_singleton:
                    r.bad(gid, lab, "executed interval empty or degenerate without explicit request")
                else:
                    r.note("explicit_singleton_kept", gid)

    # G5b A9.1 structural: with an executed SEARCH anchor, NEW_SEARCH only if the turn brings a new TOPIC/ACTOR
    if pir is not None and prior["state"] in ("EXECUTED", "EXECUTED_EMPTY", "NOT_EXECUTED") and prior.get("owner") == "FOUNDATION_SEARCH":
        anchor_pos = {(c["dimension"], c["value_hint"]) for c in pir["conditions"] if c["polarity"] == "POSITIVE"}
        if tk == "NEW_SEARCH":
            brings = {(c["dimension"], c["value_hint"]) for c in (res or {"conditions": []})["conditions"]
                      if c["dimension"] in ("TOPIC", "ACTOR") and c["polarity"] == "POSITIVE"}
            if not (brings - anchor_pos):
                r.bad(gid, lab, "NEW_SEARCH over an anchor without a new TOPIC/ACTOR (A9.1 says DELTA)")
            else:
                r.note("A9_new_search_with_new_topic_or_actor", gid)
        elif tk == "DELTA":
            new_content = [o for o in ops if o["op"] == "SET" and o["slot"] in ("TOPIC", "ACTOR") and o["polarity"] == "POSITIVE"
                           and (o["slot"], o["value_hint"]) not in anchor_pos]
            if not new_content:
                r.note("A9.1_delta_conditions_or_modifiers_only", gid)
            elif any(o["op"] == "KEEP" or o["refers_to_result_set"] for o in ops):
                r.note("A9_structural_continuation_KEEP_or_set_referent", gid)
            elif len(ops) == 1:
                r.note("F22_lone_content_condition_delta", gid)
            else:
                r.bad(gid, lab, "DELTA with new TOPIC/ACTOR, several conditions and no structural continuation")

    # G9 structural coherence of the disposition (does not derive it: §7.2/§8.3 need the producer's evidence)
    gap = prior.get("gap_seconds")
    if tk == "NEW_SEARCH" and (res is None or not res["conditions"]):
        r.bad(gid, lab, "NEW_SEARCH without own conditions (host floor)")
    if reason == "CONTEXT_EXPIRED_CONFIRM" and not (gap is not None and gap > 1800):
        r.bad(gid, lab, "CONTEXT_EXPIRED_CONFIRM within the 30 min window")
    if tk in ("DELTA", "ANSWER_TO_CLARIFICATION") and disp == "EXECUTE" and gap is not None and gap > 1800:
        r.bad(gid, lab, "fragment executed over an expired anchor (F11)")
    if reason in ("NO_ANCHOR", "ANCHOR_UNREADABLE") and prior["state"] not in ("NONE", "UNREADABLE"):
        r.bad(gid, lab, f"{reason} with a readable anchor")
    if disp == "REASK":
        if prior["state"] != "PENDING":
            r.bad(gid, lab, "REASK without a pending")
        if reason == "PENDING_UNANSWERED" and (answers or ctrl):
            r.bad(gid, lab, "PENDING_UNANSWERED although the turn answers or cancels")
        if reason in ("OPTION_NOT_CHOSEN", "ANSWER_UNRESOLVED", "ANSWER_EDITS_OTHER_SLOT") and not answers:
            r.bad(gid, lab, f"{reason} without an answer")
        if reason == "ANSWER_EDITS_OTHER_SLOT" and not patch_ops:
            r.bad(gid, lab, "ANSWER_EDITS_OTHER_SLOT without another edit")
    if answers and patch_ops and disp != "REASK":
        r.bad(gid, lab, "answer plus another edit not re-asked (F25)")
    if tk == "ANSWER_TO_CLARIFICATION" and disp == "EXECUTE" and (patch_ops or ctrl):
        r.bad(gid, lab, "executed answer carries more than the answer")
    if reason == "CLEAR_ON_EMPTY_SLOT":
        prior_slots = {slot_of(c) for c in (pir or {"conditions": []})["conditions"]} | {
            c["dimension"] for c in (pir or {"conditions": []})["conditions"]}
        if len(ops) != 1 or ops[0]["op"] != "CLEAR" or ops[0]["slot"] in prior_slots:
            r.bad(gid, lab, "CLEAR_ON_EMPTY_SLOT is not a lone CLEAR of an absent slot")
    if reason in ("BUYER_OR_PLACE_AFTER_ACTOR", "TERRITORY_VS_CARRIED_BUYER") and len(L.get("alternatives") or []) != 2:
        r.bad(gid, lab, f"{reason} without two candidate patches")

    # G7 result-set referent
    if L["m2a_scope"] == "IN_PROTOCOL" and prior["state"] in ("NOT_EXECUTED",) and disp == "EXECUTE" and L["input_kind"] != "EVENT":
        r.bad(gid, lab, "protocol over rows executed without a result set")
    if prior["state"] == "NOT_EXECUTED":
        r.note("semantic_ref_after_source_failure", gid)
    r.counts["scope:" + L["m2a_scope"]] += 1
    r.counts["disposition:" + str(disp)] += 1


def run(real_dir: Path | None) -> dict:
    idx = json.loads((GOLD_DIR / "M2A_GOLD_AND_FRONTIER_INDEX.json").read_text(encoding="utf-8"))
    ledger = load(GOLD_DIR / "FOLLOWUP_GAPS_LEDGER.jsonl")
    out: dict = {"slices": {}}
    r = Report()
    files = {"SYNTHETIC": (GOLD_DIR / "M2A_GOLD_synthetic.jsonl", GOLD_DIR / "FOLLOWUP_FRONTIER_synthetic.jsonl")}
    if real_dir is not None:
        files["REAL"] = (real_dir / "M2A_GOLD_real.jsonl", real_dir / "FOLLOWUP_FRONTIER_real.jsonl")
    for name, (gpath, fpath) in files.items():
        gold, front = load(gpath), load(fpath)
        gids, fids = {g["gold_id"] for g in gold}, {f["gold_id"] for f in front}
        exp = idx["slices"][name]
        if sorted(gids) != sorted(exp["M2A_GOLD"]["ids"]):
            r.bad(name, "-", "GOLD ids differ from the index")
        if fids != {x["gold_id"] for x in exp["FOLLOWUP_FRONTIER"]["records"]}:
            r.bad(name, "-", "FRONTIER ids differ from the index")
        if gids & fids:
            r.bad(name, "-", f"GOLD ∩ FRONTIER = {sorted(gids & fids)}")
        for g in ledger:
            if g["gap_id"] in M2A_OWNED_GAPS:
                continue
            leak = sorted(set(g["example_ids"]) & gids)
            if leak:
                r.bad(name, g["gap_id"], f"deferred gap examples inside GOLD: {leak}")
        for rec in gold:
            if rec["m2a_scope"] not in GOLD_SCOPES:
                r.bad(rec["gold_id"], "-", "GOLD record out of IN* scope")
            adjudicated = bool(rec["adjudicated_by"]) or "orchestrator_classification" in rec
            if (rec["ambiguous_by"] or not rec["primary_agreement"]) and not adjudicated:
                r.bad(rec["gold_id"], "-", "ambiguous/disputed without adjudication")
            if rec["expected_disposition"] is None:
                r.bad(rec["gold_id"], "-", "no agreed disposition")
            for lab in ("label_A", "label_B"):
                check_label(r, rec, lab[-1], rec[lab])
        out["slices"][name] = {"gold": len(gold), "frontier": len(front)}
    out["violations"] = r.violations
    out["counts"] = dict(sorted(r.counts.items()))
    out["ids"] = {k: sorted(v) for k, v in sorted(r.ids.items())}
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--real-dir", type=Path)
    ap.add_argument("--json", type=Path)
    a = ap.parse_args()
    out = run(a.real_dir)
    text = json.dumps(out, ensure_ascii=False, indent=1, sort_keys=True)
    if a.json:
        a.json.write_text(text + "\n", encoding="utf-8")
    print(json.dumps({"slices": out["slices"], "violations": len(out["violations"])}, ensure_ascii=False))
    for v in out["violations"][:60]:
        print("VIOLATION", v)
    return 1 if out["violations"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
