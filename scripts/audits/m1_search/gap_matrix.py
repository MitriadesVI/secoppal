"""M1 PHASE A / A3: GOLD / LEGACY / FOUNDATION gap matrix with the guilty stage.

Stdlib only. Inputs are the captures written by capture_foundation_legacy.py (captures.jsonl) and,
optionally, a directory of Gold label files (M1_0_SEARCH_CONTRACT.md section 8, validated with
gold_schema.py). Never reads query text into its outputs: reports carry gold_id and aggregates only.

    # no Gold yet: distribution by stage, F-vs-L disagreements
    python scripts/audits/m1_search/gap_matrix.py --captures CAPTURES_DIR --out-json J --out-md M

    # with Gold labels (every *.jsonl in the directory, or explicit files)
    python scripts/audits/m1_search/gap_matrix.py --captures CAPTURES_DIR --gold LABELS_DIR \
        --out-json J --out-md M

    python scripts/audits/m1_search/gap_matrix.py --selftest      # synthetic fixtures, every category

Foundation categories per Gold condition (first stage that diverges, product order):
  IR_CANNOT_EXPRESS | F_NOT_PROPOSED | F_PROPOSED_HOST_UNRESOLVED |
  HOST_RESOLVED_PROJECTION_UNSUPPORTED | GATE_EXCLUDED (intrinsically OK but the gate kept it out) | OK
Legacy: L_OK | L_MISSING | L_WRONG (plus L_SPURIOUS for conditions the Gold does not have).
Parity = exclusive regressions: legacy L_OK and Foundation (as the product runs it) not OK.

Contract v1.7 (P-ACT-2): a Gold request with `host_universe_redirect` (PROCESSES + ACTOR) expects the host to
EXECUTE ON CONTRACTS and ANNOUNCE it. Both arms are scored on the UNIVERSE_VIEW row with the same rule:
executing on processes is a universe failure, blocking/clarifying where the Gold says EXECUTE is a failure,
and a silent switch to contracts (no notice) is a failure. Foundation must still *propose* the cited
universe (PROCESSES): the redirect is the host's job, never a rewrite of the IR.

Contract v1.8 (ACTOR x STATE, universe NOT cited). Both arms, same rule, evidence-driven:
  * AWARDED (`host_universe_redirect` with `effect: APPROXIMATION`, UNIVERSE_VIEW row): executing on processes is
    a failure; executing on contracts WITHOUT the notice or without the declared approximation effect is a
    failure; blocking where the Gold executes is a failure. Foundation scores IR_CANNOT_EXPRESS (UNSPECIFIED is
    not in the IR) unless the capture carries host evidence for the request.
  * PRE_AWARD (`expected_disposition CLARIFY` + `ACTOR_STATE_INCOMPATIBLE`, ACTOR_STATE_POLICY row): executing the
    request in ANY universe is a failure; clarifying/blocking is OK. A process that is not awarded has no awardee.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

F_CATEGORIES = ("OK", "F_NOT_PROPOSED", "F_PROPOSED_HOST_UNRESOLVED", "HOST_RESOLVED_PROJECTION_UNSUPPORTED",
                "IR_CANNOT_EXPRESS", "GATE_EXCLUDED")
L_CATEGORIES = ("L_OK", "L_MISSING", "L_WRONG")
# Vocabulary of the frozen producer prompt v4.2 (foundation_prompts.py): what the IR can say at all.
IR_VOCAB = {("TOPIC", "contains"), ("TOPIC", "whole_token_folded"), ("BUYER", "is"), ("GEOGRAPHY", "is"),
            ("TIME", "during_year"), ("VALUE", "greater_than"), ("VALUE", "less_than"),
            ("STATE", "is"), ("STATE", "family"), ("MODALITY", "is"), ("MODALITY", "family"),
            ("CONTRACT_TYPE_OR_NATURE", "is"), ("CONTRACT_TYPE_OR_NATURE", "family"), ("SOURCE", "is"),
            ("UNSUPPORTED", None)}
IR_OPERATIONS = {"SEARCH", "COUNT", "SUM_VALUE"}      # the prompt's operation vocabulary
HOST_RUNS_OPERATIONS = {"SEARCH"}                      # admit_proposal: UNSUPPORTED_OPERATION otherwise
STOP = frozenset("de del la las los el en y o u a al un una que con por para sobre sin se es son me mi".split())
GATE_NOUNS = frozenset({"contratos", "procesos"})
REDIRECT_FIELD = "host_universe_redirect"  # contract v1.7 (gold_schema.REDIRECT_FIELD)
PREAWARD_REASON = "ACTOR_STATE_INCOMPATIBLE"  # contract v1.8 (gold_schema.PREAWARD_REASON)
PV_TO_LEGACY = {"CURRENT_OPPORTUNITIES": ("PROCESSES", "OPPORTUNITIES"), "HISTORICAL_PROCESSES": ("PROCESSES", "RECORDS"),
                "CONTRACTS": ("CONTRACTS", "RECORDS")}
LEGACY_HANDLES = {"SEARCH": {"SEARCH"}, "PROFILE_ACTOR": {"ACTOR_PROFILE"}, "PROFILE_BUYER": {"ENTITY_PROFILE"},
                  "COUNT": {"ANALYTICS"}, "SUM_VALUE": {"ANALYTICS"}, "GROUP_BY": {"ANALYTICS"},
                  "RANK_SUPPLIERS": {"ANALYTICS", "ENTITY_PROFILE"}}


# --------------------------------------------------------------------------- text helpers
def fold(text) -> str:
    out = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode().lower()
    return " ".join(out.replace("_", " ").split())


def stems(*texts) -> set[str]:
    words = (w for t in texts for w in re.findall(r"[a-z0-9]+", fold(t)))
    return {w[:5] for w in words if w not in STOP and len(w) > 1}


def overlap(a: set, b: set) -> float:
    return len(a & b) / max(1, len(a))


def reason_class(reason) -> str:
    """Strip model-chosen ids/values from a host reason so aggregates carry no query text."""
    text = str(reason or "")
    if not text:
        return "NONE"
    head = text.split(":")
    if head[0] in {"UNRESOLVED_HOST_DIMENSION", "UNSUPPORTED_CONDITION"} and len(head) > 1 and head[1].isupper():
        return f"{head[0]}:{head[1]}"
    if head[0] in {"MISSING_HOST_RESOLUTION", "MISSING_HOST_POLICY", "UNSUPPORTED_CONDITION", "EMPTY_HOST_RESOLUTION",
                   "MULTIPLE_HOST_RESOLUTIONS", "HOST_RESIDUAL"}:
        return head[0]
    if text.startswith("SOQL_PROJECTION_BLOCKED"):
        return "SOQL_PROJECTION_BLOCKED"
    return re.sub(r"[^A-Z0-9_:.-]", "", head[0]) or "NONE" if re.fullmatch(r"[A-Z0-9_ ]+(:.*)?", text) else "OTHER"


# --------------------------------------------------------------------------- gold access
def gold_requests(gold: dict) -> list[tuple[str, dict]]:
    """(operation, request) pairs that M1 SEARCH measures: SEARCH records and SEARCH jobs of a MULTI."""
    op = gold.get("operation")
    if op == "SEARCH":
        return [("SEARCH", r) for r in gold.get("requests", [])]
    if op == "MULTI":
        return [("SEARCH", r) for job in gold.get("jobs", []) if job.get("operation") == "SEARCH"
                for r in job.get("requests", [])]
    return []


def op_class(gold: dict) -> str:
    reqs = [r for _, r in gold_requests(gold)]
    if gold.get("operation") not in {"SEARCH", "MULTI"} or not reqs:
        return gold.get("operation", "?")
    if len(reqs) > 1:
        return "SEARCH:DUAL"
    r = reqs[0]
    if r["universe"] == "UNSPECIFIED":
        return "SEARCH:UNSPECIFIED"
    return "SEARCH:OPPORTUNITIES" if r.get("view") == "OPPORTUNITIES" else f"SEARCH:{r['universe']}"


def row_key(cond: dict) -> str:
    dim = cond["dimension"]
    if dim == "GEOGRAPHY":
        return f"GEOGRAPHY/{cond.get('level') or 'UNKNOWN'}"
    if dim in {"TIME", "VALUE", "ORDER"}:
        return f"{dim}/{cond['operator']}"
    return dim


def gold_logic_groups(logic: str | None) -> set[str]:
    return set(re.findall(r"\b(OR|NOT)\(", logic or ""))


# --------------------------------------------------------------------------- foundation per condition
def in_vocab(cond: dict) -> bool:
    dim = cond["dimension"]
    return (dim, None if dim == "UNSUPPORTED" else cond.get("operator")) in IR_VOCAB


def f_status(gold_cond: dict, match: dict | None, detail: str, fnd: dict) -> tuple[str, str]:
    """Category and detail of one Gold condition on the intrinsic Foundation chain (gate not applied)."""
    if not in_vocab(gold_cond):
        return "IR_CANNOT_EXPRESS", f"F_PROXY:{match['dimension']}" if match else "F_SILENT"
    if fnd["producer_status"] != "PROPOSAL":
        return "F_NOT_PROPOSED", f"PRODUCER:{fnd['producer_status']}"
    if match is None or detail:
        return "F_NOT_PROPOSED", detail or "ABSENT"
    if match.get("host_status") != "READY":
        return "F_PROPOSED_HOST_UNRESOLVED", f"{match.get('host_status')}:{reason_class(match.get('host_reason'))}"
    if match.get("projection_reason"):
        return "HOST_RESOLVED_PROJECTION_UNSUPPORTED", reason_class(match["projection_reason"])
    return "OK", ""


def match_request(gold_conds: list[dict], f_conds: list[dict]) -> tuple[dict, list[dict]]:
    """Gold cond id -> (foundation cond | None, detail); plus unmatched foundation conditions."""
    left = list(f_conds)
    result: dict[str, tuple[dict | None, str]] = {}
    gold_stems = {g["id"]: stems(g.get("quote"), g.get("value_hint")) for g in gold_conds}
    pending = []
    for g in gold_conds:  # pass 1: same dimension, ranked by quote overlap
        cands = [(overlap(gold_stems[g["id"]], stems(*f["quotes"])), f) for f in left
                 if f["dimension"] == g["dimension"]]
        cands.sort(key=lambda sf: -sf[0])
        if cands and cands[0][0] > 0:
            f = cands[0][1]
            left.remove(f)
            result[g["id"]] = (f, _pair_detail(g, f))
        else:
            pending.append(g)
    for g in pending:  # pass 2: a lone same-dimension proposal with another value, then other dimensions
        same = [f for f in left if f["dimension"] == g["dimension"]]
        gold_same = [x for x in pending if x["dimension"] == g["dimension"]]
        if len(same) == 1 and len(gold_same) == 1:
            left.remove(same[0])
            result[g["id"]] = (same[0], "VALUE_DIFFERS")
            continue
        cross = sorted(((overlap(gold_stems[g["id"]], stems(*f["quotes"])), f) for f in left), key=lambda sf: -sf[0])
        if cross and cross[0][0] > 0:
            left.remove(cross[0][1])
            result[g["id"]] = (cross[0][1], f"PROPOSED_AS:{cross[0][1]['dimension']}")
        else:
            result[g["id"]] = (None, "ABSENT")
    return result, left


def _pair_detail(g: dict, f: dict) -> str:
    if g.get("polarity") != f.get("polarity"):
        return "POLARITY"
    if g["dimension"] == "VALUE":
        gold_up, f_up = g["operator"].startswith("greater"), f["operator"].startswith("greater")
        if gold_up != f_up:
            return "DIRECTION"
    return ""


def foundation_request_for(gold_req: dict, fnd: dict, used: set) -> dict | None:
    wanted = {"PROCESSES": "processes", "CONTRACTS": "contracts"}.get(gold_req["universe"])
    for strict in (True, False):
        for req in fnd.get("requests", []):
            if req["request_id"] in used:
                continue
            if not strict or req["subject"] == wanted:
                return req
    return None


# --------------------------------------------------------------------------- legacy per condition
def legacy_status(g: dict, ldims: dict) -> tuple[str, str]:
    dim = g["dimension"]
    cands = [c for c in ldims["conditions"] if c["dimension"] == dim]
    if dim == "GEOGRAPHY":
        want = g.get("level")
        exact = [c for c in cands if want in (None, "UNKNOWN") or c.get("level") == want]
        cands = exact or cands
    if not cands:
        return "L_MISSING", ""
    gold_words = stems(g.get("quote"), g.get("value_hint"))
    for c in cands:
        verdict = _legacy_compare(g, c, gold_words)
        if verdict[0] == "L_OK":
            return verdict
    return _legacy_compare(g, cands[0], gold_words)


def _legacy_compare(g: dict, c: dict, gold_words: set) -> tuple[str, str]:
    dim, polarity = g["dimension"], g.get("polarity", "POSITIVE")
    if polarity != c["polarity"]:
        return "L_WRONG", "POLARITY"
    value = c["value"]
    if dim in {"TOPIC", "BUYER", "MODALITY", "CONTRACT_TYPE_OR_NATURE"}:
        words = stems(value)
        share = overlap(gold_words, words)
        if dim == "TOPIC":
            return ("L_OK", "") if share >= 1 else (("L_WRONG", "PARTIAL_TOPIC") if share > 0 else ("L_WRONG", "VALUE"))
        return ("L_OK", "") if share > 0 else ("L_WRONG", "VALUE")
    if dim == "GEOGRAPHY":
        level_ok = g.get("level") in (None, "UNKNOWN") or c.get("level") == g.get("level")
        if not level_ok:
            return "L_WRONG", f"LEVEL:{c.get('level')}"
        return ("L_OK", "") if overlap(gold_words, stems(value)) > 0 else ("L_WRONG", "VALUE")
    if dim == "TIME":
        years = set(re.findall(r"(?:19|20)\d{2}", f"{g.get('value_hint')} {g.get('quote')}"))
        if g["operator"] == "during_year":
            return ("L_OK", "") if c["operator"] == "during_year" and c["value"] in years else ("L_WRONG", "VALUE")
        if g["operator"] == "during_month":
            hint = str(g.get("value_hint") or "")
            return ("L_OK", "") if c["operator"] == "during_month" and c["value"] == hint else ("L_WRONG", "VALUE")
        found = set(re.findall(r"(?:19|20)\d{2}", c["value"]))
        return ("L_OK", "") if not years or years & found else ("L_WRONG", "VALUE")
    if dim == "VALUE":
        if g["operator"].startswith("greater") != c["operator"].startswith("greater"):
            return "L_WRONG", "DIRECTION"
        digits = re.sub(r"\D", "", str(g.get("value_hint") or ""))
        if digits and digits != re.sub(r"\D", "", c["value"]):
            return "L_WRONG", "AMOUNT"
        exclusive = g["operator"] in {"greater_than", "less_than"}
        return "L_OK", "BOUNDARY_INCLUSIVE" if exclusive else ""
    if dim == "STATE":
        return ("L_OK", "") if str(g.get("value_hint")) == c["value"] else ("L_WRONG", "VALUE")
    if dim == "ORDER":
        return ("L_OK", "") if g["operator"] == c["operator"] else ("L_WRONG", "VALUE")
    if dim == "ACTOR":
        # Symmetric with the other dimensions (METIS D-ACTOR §5): detecting *an* actor is not
        # enough. A document must match digit for digit (DV ignored); a name must share every
        # content word of the Gold quote after folding. Queued/unresolved actors are not OK.
        if c.get("status") not in (None, "RESOLVED") or c.get("resolved") is False:
            return "L_WRONG", "ACTOR_UNRESOLVED"
        gold_digits = re.sub(r"\D", "", g.get("quote") or "")
        got_digits = re.sub(r"\D", "", str(value or ""))
        if len(gold_digits) >= 5:
            same = got_digits[:len(gold_digits)] == gold_digits or gold_digits[:len(got_digits)] == got_digits
            return ("L_OK", "") if got_digits and same else ("L_WRONG", "VALUE")
        # A name the directory resolved exactly (to a document) is a correct execution; the
        # directory, not this harness, is the authority on name -> identity.
        if got_digits:
            return "L_OK", "NAME_RESOLVED_BY_DIRECTORY"
        share = overlap(gold_words, stems(value))
        return ("L_OK", "") if share >= 1 else ("L_WRONG", "VALUE")
    return "L_OK", ""


# --------------------------------------------------------------------------- one gold x capture
def evaluate(gold: dict, cap: dict) -> dict:
    """All per-condition verdicts of one Gold record against its capture."""
    fnd, leg = cap["foundation"], cap["legacy"].get("dims")
    admitted = bool(cap["gate"].get("admitted"))
    rows: list[dict] = []
    spurious_f: list[dict] = []
    used: set = set()
    op = gold.get("operation")
    out = {"gold_id": gold["gold_id"], "slice": gold.get("slice"), "op_class": op_class(gold), "operation": op,
           "admitted": admitted, "rows": rows, "spurious_f": spurious_f, "spurious_l": [],
           "disposition": gold.get("expected_disposition"), "ambiguity": gold.get("ambiguity")}
    for _, req in gold_requests(gold):
        f_req = foundation_request_for(req, fnd, used)
        if f_req is not None:
            used.add(f_req["request_id"])
        f_conds = [c for c in fnd.get("conditions", []) if f_req and f_req["request_id"] in c["request_ids"]]
        mapping, left = match_request(req.get("conditions", []), f_conds)
        spurious_f.extend({"dimension": c["dimension"], "label": c.get("unsupported_label")} for c in left)
        for g in req.get("conditions", []):
            f_match, detail = mapping[g["id"]]
            intrinsic, why = f_status(g, f_match, detail, fnd)
            product = "GATE_EXCLUDED" if intrinsic == "OK" and not admitted else intrinsic
            lcat, ldetail = legacy_status(g, leg) if leg else ("L_MISSING", "LEGACY_ERROR")
            rows.append({"key": row_key(g), "dimension": g["dimension"], "foundation_intrinsic": intrinsic,
                         "foundation": product, "f_detail": why, "legacy": lcat, "l_detail": ldetail,
                         "polarity": g.get("polarity")})
        rows.extend(_universe_rows(req, f_req, fnd, admitted, leg, gold.get("product_view")))
        rows.extend(_preaward_rows(gold, req, f_req, fnd, admitted, leg))
        rows.extend(_logic_rows(req, f_req, fnd, admitted, leg))
        if leg:
            out["spurious_l"].extend(_legacy_extras(req, leg, gold))
    return out


def _universe_rows(req: dict, f_req: dict | None, fnd: dict, admitted: bool, leg: dict | None,
                   product_view: str | None = None) -> list[dict]:
    rows = []
    gold_view = req.get("view")
    unspecified = req["universe"] == "UNSPECIFIED"
    redirect = req.get(REDIRECT_FIELD)
    if unspecified and redirect and f_req is not None and REDIRECT_FIELD in f_req:
        intrinsic, why = _foundation_redirect(redirect, f_req, fnd)   # v1.8: the host policy left evidence; score it
    elif unspecified:
        intrinsic, why = "IR_CANNOT_EXPRESS", "UNSPECIFIED_UNIVERSE"
    elif fnd["producer_status"] != "PROPOSAL" or f_req is None:
        intrinsic, why = "F_NOT_PROPOSED", f"PRODUCER:{fnd['producer_status']}" if f_req is None else "ABSENT"
    elif f_req["subject"] != {"PROCESSES": "processes", "CONTRACTS": "contracts"}[req["universe"]]:
        intrinsic, why = "F_NOT_PROPOSED", "SUBJECT"
    elif (gold_view == "OPPORTUNITIES") != (f_req.get("effective_kind") == "opportunities"):
        intrinsic, why = "F_NOT_PROPOSED", "VIEW"
    elif fnd.get("admission", {}).get("reason", "") in {"UNSUPPORTED_UNIVERSE", "UNSUPPORTED_OPERATION", "MULTI_REQUEST",
                                                          "UNSUPPORTED_SOURCE", "UNGROUNDED_REQUEST_UNIVERSE"}:
        intrinsic, why = "F_PROPOSED_HOST_UNRESOLVED", fnd["admission"]["reason"]
    elif redirect:
        intrinsic, why = _foundation_redirect(redirect, f_req, fnd)
    else:
        intrinsic, why = "OK", ""
    product = "GATE_EXCLUDED" if intrinsic == "OK" and not admitted else intrinsic
    lcat, ldetail = "L_MISSING", "LEGACY_ERROR"
    if leg and redirect:
        lcat, ldetail = _legacy_redirect(redirect, leg)
    elif leg:
        if unspecified:
            want = PV_TO_LEGACY.get(product_view or "", None)
        else:
            want = ({"PROCESSES": "PROCESSES", "CONTRACTS": "CONTRACTS"}[req["universe"]], gold_view or "RECORDS")
        universe_ok = want is not None and want[0] in leg["universe"]
        view_ok = want is not None and (leg["view"] == want[1])
        lcat, ldetail = ("L_OK", "") if universe_ok and view_ok else ("L_WRONG", "UNIVERSE" if not universe_ok else "VIEW")
    rows.append({"key": "UNIVERSE_VIEW", "dimension": "UNIVERSE_VIEW", "foundation_intrinsic": intrinsic,
                 "foundation": product, "f_detail": why, "legacy": lcat, "l_detail": ldetail, "polarity": None})
    return rows


def _foundation_redirect(redirect: dict, f_req: dict, fnd: dict) -> tuple[str, str]:
    """v1.7: Foundation proposed the cited universe faithfully; did the HOST execute on the redirect universe
    and announce it? Evidence: `host_universe_redirect` on the captured request (written by the capture of the
    host policy). Absent evidence is never read as success."""
    got = f_req.get(REDIRECT_FIELD)
    if got == redirect:
        return "OK", ""
    status = fnd.get("final_status")
    if got:
        return "F_PROPOSED_HOST_UNRESOLVED", "REDIRECT_MISMATCH"
    if status == "READY":
        return "F_PROPOSED_HOST_UNRESOLVED", "UNIVERSE_NOT_REDIRECTED"   # ran on the proposed PROCESSES
    return "F_PROPOSED_HOST_UNRESOLVED", f"BLOCKED_WHERE_GOLD_EXECUTES:{status}"


def _legacy_redirect(redirect: dict, leg: dict) -> tuple[str, str]:
    """v1.7 for legacy: same rule as `_foundation_redirect`. Only the redirect universe may be executed
    (a PROCESSES+CONTRACTS scope still executes on processes), and the switch must be announced
    (`dims.universe_notices`, which legacy never emits: a silent switch is a failure for both arms). v1.8: when
    the Gold redirect carries an `effect` (APPROXIMATION) the arm must also declare it (`dims.universe_effects`)."""
    if leg.get("needs_clarification") or leg.get("handled_as") == "CLARIFY":
        return "L_WRONG", "BLOCKED_WHERE_GOLD_EXECUTES"
    if list(leg["universe"]) != [redirect["executed_universe"]]:
        return "L_WRONG", "UNIVERSE"
    if leg["view"] != "RECORDS":
        return "L_WRONG", "VIEW"
    if redirect["notice"] not in (leg.get("universe_notices") or []):
        return "L_WRONG", "NOTICE_MISSING"
    if redirect.get("effect") and redirect["effect"] not in (leg.get("universe_effects") or []):
        return "L_WRONG", "EFFECT_MISSING"   # v1.8: an approximation that is not declared as one
    return "L_OK", ""


def _preaward_rows(gold: dict, req: dict, f_req: dict | None, fnd: dict, admitted: bool, leg: dict | None) -> list[dict]:
    """v1.8 PRE_AWARD (ACTOR + a state before the award): the Gold says CLARIFY / ACTOR_STATE_INCOMPATIBLE. Executing
    the request in ANY universe is the failure; the same rule for both arms. Evidence: Foundation `final_status`
    (READY = the host executes) and legacy `needs_clarification` / `handled_as == CLARIFY`."""
    if gold.get("expected_disposition") != "CLARIFY" or gold.get("disposition_reason") != PREAWARD_REASON:
        return []
    if fnd["producer_status"] != "PROPOSAL" or f_req is None:
        intrinsic, why = "F_NOT_PROPOSED", f"PRODUCER:{fnd['producer_status']}" if f_req is None else "ABSENT"
    elif fnd.get("final_status") == "READY":
        intrinsic, why = "F_PROPOSED_HOST_UNRESOLVED", "EXECUTED_ACTOR_PREAWARD"
    else:
        intrinsic, why = "OK", ""
    product = "GATE_EXCLUDED" if intrinsic == "OK" and not admitted else intrinsic
    lcat, ldetail = "L_MISSING", "LEGACY_ERROR"
    if leg:
        clarified = bool(leg.get("needs_clarification")) or leg.get("handled_as") == "CLARIFY"
        lcat, ldetail = ("L_OK", "") if clarified else ("L_WRONG", "EXECUTED_ACTOR_PREAWARD")
    return [{"key": "ACTOR_STATE_POLICY", "dimension": "POLICY", "foundation_intrinsic": intrinsic,
             "foundation": product, "f_detail": why, "legacy": lcat, "l_detail": ldetail, "polarity": None}]


def _logic_rows(req: dict, f_req: dict | None, fnd: dict, admitted: bool, leg: dict | None) -> list[dict]:
    groups = gold_logic_groups(req.get("logic"))
    if not groups:
        return []
    expr = (f_req or {}).get("expr") or ""
    f_groups = set(re.findall(r"\b(OR|NOT)\(", expr))
    if fnd["producer_status"] != "PROPOSAL" or not groups <= f_groups:
        intrinsic, why = "F_NOT_PROPOSED", "LOGIC_FLATTENED"
    elif "NONCONJUNCTIVE" in str(fnd.get("projection_request_level") or fnd.get("projection_reason") or ""):
        intrinsic, why = "HOST_RESOLVED_PROJECTION_UNSUPPORTED", "NONCONJUNCTIVE_EXPRESSION"
    else:
        intrinsic, why = "OK", ""
    product = "GATE_EXCLUDED" if intrinsic == "OK" and not admitted else intrinsic
    # legacy executes only conjunctions: any gold OR/NOT group is never faithfully executed
    return [{"key": "LOGIC_OR_NOT", "dimension": "LOGIC", "foundation_intrinsic": intrinsic, "foundation": product,
             "f_detail": why, "legacy": "L_WRONG" if leg else "L_MISSING", "l_detail": "CONJUNCTION_ONLY",
             "polarity": None}]


def _legacy_extras(req: dict, leg: dict, gold: dict) -> list[dict]:
    """Legacy conditions the Gold does not carry (spurious), by dimension."""
    out = []
    gold_by_dim = defaultdict(list)
    for g in req.get("conditions", []):
        gold_by_dim[g["dimension"]].append(g)
    known = stems(req.get("universe_quote"), *[g.get("quote") for g in req.get("conditions", [])],
                  *[g.get("value_hint") for g in req.get("conditions", [])])
    for c in leg["conditions"]:
        if c["dimension"] == "TOPIC":
            if c["polarity"] == "POSITIVE" and stems(c["value"]) - known:
                out.append({"dimension": "TOPIC", "detail": "EXTRA_TOKENS"})
        elif not gold_by_dim.get(c["dimension"]):
            out.append({"dimension": c["dimension"], "detail": c.get("key")})
    return out


def operation_row(gold: dict, cap: dict) -> dict:
    """Operation-level verdict for any Gold operation (the SEARCH conditions are in `evaluate`)."""
    op, fnd, leg = gold.get("operation"), cap["foundation"], cap["legacy"].get("dims")
    f_ops = {r["operation"] for r in fnd.get("requests", [])}
    if op not in IR_OPERATIONS and op != "MULTI":
        intrinsic, why = "IR_CANNOT_EXPRESS", "OPERATION_OUTSIDE_IR"
    elif fnd["producer_status"] != "PROPOSAL":
        intrinsic, why = "F_NOT_PROPOSED", f"PRODUCER:{fnd['producer_status']}"
    elif op == "MULTI" or op not in f_ops:
        intrinsic, why = "F_NOT_PROPOSED", "OPERATION_DIFFERS" if op != "MULTI" else "MULTI"
    elif op not in HOST_RUNS_OPERATIONS:
        intrinsic, why = "F_PROPOSED_HOST_UNRESOLVED", "UNSUPPORTED_OPERATION"
    else:
        intrinsic, why = "OK", ""
    admitted = bool(cap["gate"].get("admitted"))
    product = "GATE_EXCLUDED" if intrinsic == "OK" and not admitted else intrinsic
    handled = (leg or {}).get("handled_as")
    ok = handled in LEGACY_HANDLES.get(op, set()) or (op == "SEARCH" and handled == "ACTOR_PROFILE"
                                                       and _actor_only(gold))
    return {"key": f"OPERATION/{op}", "dimension": "OPERATION", "foundation_intrinsic": intrinsic,
            "foundation": product, "f_detail": why, "legacy": "L_OK" if ok else "L_WRONG",
            "l_detail": f"HANDLED_AS:{handled}", "polarity": None}


def _actor_only(gold: dict) -> bool:
    conds = [c for _, r in gold_requests(gold) for c in r.get("conditions", [])]
    return bool(conds) and all(c["dimension"] == "ACTOR" for c in conds)


def entry_verdicts(ev: dict) -> dict:
    """Entry level: worst category in product order for Foundation; worst for legacy."""
    order = ("IR_CANNOT_EXPRESS", "F_NOT_PROPOSED", "F_PROPOSED_HOST_UNRESOLVED",
             "HOST_RESOLVED_PROJECTION_UNSUPPORTED", "GATE_EXCLUDED", "OK")
    intrinsic = [r["foundation_intrinsic"] for r in ev["rows"]]
    product = [r["foundation"] for r in ev["rows"]]
    f_in = next((c for c in order if c in intrinsic), "OK")
    f_pr = next((c for c in order if c in product), "OK")
    if ev["spurious_f"] and f_pr == "OK":
        f_pr = "F_NOT_PROPOSED"  # an unmatched Foundation condition narrows the search wrongly
    legacy = [r["legacy"] for r in ev["rows"]]
    l_cat = ("L_WRONG" if "L_WRONG" in legacy or ev["spurious_l"] else "L_MISSING" if "L_MISSING" in legacy else "L_OK")
    return {"foundation_intrinsic": f_in, "foundation": f_pr, "legacy": l_cat}


# --------------------------------------------------------------------------- aggregation
def aggregate_gold(golds: list[dict], caps: dict) -> dict:
    matrix = defaultdict(lambda: {"GOLD": 0, "LEGACY": Counter(), "FOUNDATION": Counter(),
                                  "FOUNDATION_INTRINSIC": Counter(), "PARITY": Counter()})
    by_class = defaultdict(lambda: defaultdict(lambda: {"GOLD": 0, "LEGACY": Counter(), "FOUNDATION": Counter()}))
    entries = Counter()
    entry_by_class = defaultdict(Counter)
    regressions, gains, missing = [], [], []
    spurious = {"foundation": Counter(), "legacy": Counter()}
    labels_f = Counter()
    dispositions = defaultdict(Counter)
    for gold in golds:
        cap = caps.get(gold["gold_id"])
        if cap is None:
            missing.append(gold["gold_id"])
            continue
        if gold.get("ambiguity") == "AMBIGUOUS":
            entries["AMBIGUOUS_SKIPPED"] += 1
            continue
        ev = evaluate(gold, cap)
        if gold.get("operation") != "SEARCH" or gold.get("expected_disposition") != "EXECUTE":
            dispositions[f"{gold.get('operation')}/{gold.get('expected_disposition')}"][
                f"F:{cap['foundation'].get('final_status')}|L:{(cap['legacy'].get('dims') or {}).get('handled_as')}"] += 1
        if gold.get("operation") != "SEARCH" and gold.get("operation") != "MULTI":
            ev["rows"] = [operation_row(gold, cap)]
        elif gold.get("expected_disposition") != "EXECUTE":
            # v1.8: a CLARIFY record has no condition rows, except the host-policy row that scores execution itself
            ev["rows"] = [r for r in ev["rows"] if r["key"] == "ACTOR_STATE_POLICY"]
        elif gold.get("operation") == "MULTI":
            ev["rows"].append(operation_row(gold, cap))
        for row in ev["rows"]:
            for table in (matrix[row["key"]], by_class[ev["op_class"]][row["key"]]):
                table["GOLD"] += 1
                table["LEGACY"][row["legacy"]] += 1
                table["FOUNDATION"][row["foundation"]] += 1
            matrix[row["key"]]["FOUNDATION_INTRINSIC"][row["foundation_intrinsic"]] += 1
            l_ok, f_ok = row["legacy"] == "L_OK", row["foundation"] == "OK"
            tag = ("BOTH_OK" if l_ok and f_ok else "LEGACY_ONLY_OK" if l_ok else "FOUNDATION_ONLY_OK" if f_ok
                   else "NEITHER_OK")
            matrix[row["key"]]["PARITY"][tag] += 1
            if tag == "LEGACY_ONLY_OK":
                regressions.append({"gold_id": gold["gold_id"], "key": row["key"], "culprit": row["foundation"],
                                    "detail": row["f_detail"]})
            if tag == "FOUNDATION_ONLY_OK":
                gains.append({"gold_id": gold["gold_id"], "key": row["key"], "legacy": row["legacy"],
                              "detail": row["l_detail"]})
        for extra in ev["spurious_f"]:
            spurious["foundation"][extra["dimension"]] += 1
            if extra["dimension"] == "UNSUPPORTED" and extra.get("label"):
                labels_f[extra["label"]] += 1
        for extra in ev["spurious_l"]:
            spurious["legacy"][extra["dimension"]] += 1
        verdict = entry_verdicts(ev) if ev["rows"] else None
        if verdict:
            entries[f"F:{verdict['foundation']}"] += 1
            entries[f"L:{verdict['legacy']}"] += 1
            entry_by_class[ev["op_class"]][f"F:{verdict['foundation']}"] += 1
            entry_by_class[ev["op_class"]][f"L:{verdict['legacy']}"] += 1
            f_ok_entry, l_ok_entry = verdict["foundation"] == "OK", verdict["legacy"] == "L_OK"
            entries["ENTRY_PARITY:LEGACY_ONLY_OK"] += l_ok_entry and not f_ok_entry
            entries["ENTRY_PARITY:FOUNDATION_ONLY_OK"] += f_ok_entry and not l_ok_entry
            entries["ENTRY_PARITY:BOTH_OK"] += f_ok_entry and l_ok_entry
            entries["ENTRY_PARITY:NEITHER_OK"] += not f_ok_entry and not l_ok_entry
            entries["EVALUATED"] += 1
    freeze = lambda d: {k: {kk: (dict(vv) if isinstance(vv, Counter) else vv) for kk, vv in v.items()} for k, v in d.items()}
    return {"matrix": freeze(matrix), "by_op_class": {k: freeze(v) for k, v in by_class.items()},
            "entry_level": dict(entries), "entry_level_by_op_class": {k: dict(v) for k, v in entry_by_class.items()},
            "regressions_legacy_ok_foundation_not": regressions, "gains_foundation_ok_legacy_not": gains,
            "spurious": {k: dict(v) for k, v in spurious.items()},
            "foundation_unsupported_labels_ge5": {k: v for k, v in labels_f.items() if v >= 5},
            "non_search_or_non_execute": {k: dict(v) for k, v in dispositions.items()},
            "gold_without_capture": missing}


# --------------------------------------------------------------------------- no-gold distribution
def pct(values, q):
    values = sorted(v for v in values if v is not None)
    if not values:
        return None
    return round(values[min(len(values) - 1, int(q * (len(values) - 1) + 0.5))], 1)


def stage_label(cap: dict) -> str:
    """Entry-level stage without Gold: where the first divergence from 'Foundation would execute it' sits."""
    f = cap["foundation"]
    if f["producer_status"] != "PROPOSAL":
        return f"PRODUCER_{f['producer_status']}"
    status = f.get("final_status")
    admitted = cap["gate"].get("admitted")
    if status == "READY":
        return "F_READY_ADMITTED" if admitted else "GATE_EXCLUDED_F_READY"
    if (f.get("admission") or {}).get("status") != "READY":
        return "HOST_" + str((f.get("admission") or {}).get("status"))
    return "PROJECTION_" + str(status)


def distribution(caps: list[dict]) -> dict:
    gate, prod, host, props = Counter(), Counter(), Counter(), Counter()
    by_slice = defaultdict(Counter)
    lat_final, lat_total, retries = [], [], 0
    stage, stage_by_gate = Counter(), Counter()
    f_dims, l_dims, disagree = Counter(), Counter(), defaultdict(Counter)
    f_ops, handled, routes = Counter(), Counter(), Counter()
    labels, reasons_admit, reasons_proj, ops_logic = Counter(), Counter(), Counter(), Counter()
    h7 = Counter()
    funnel = defaultdict(Counter)
    value_classes, unsupported_classes, silent = Counter(), Counter(), Counter()
    legacy_ops, time_cross = Counter(), Counter()
    no_request = Counter()
    for cap in caps:
        g, f, leg = cap["gate"], cap["foundation"], cap["legacy"].get("dims")
        gate["ADMITTED" if g.get("admitted") else ("INELIGIBLE_LEXICAL" if g.get("ineligibility_code") else "VETOED_BY_OWNER")] += 1
        if g.get("specialized_owner"):
            gate[f"owner_any:{g['specialized_owner']}"] += 1
            if g.get("eligible"):
                gate[f"eligible_but_vetoed:{g['specialized_owner']}"] += 1
        if g.get("ineligibility_code") and g["ineligibility_code"] != "INELIGIBLE_LEXICAL":
            gate[g["ineligibility_code"]] += 1
        prod[f["producer_status"]] += 1
        lat_final.append(f.get("latency_ms"))
        lat_total.append(f.get("total_latency_ms"))
        retries += (f.get("provider_calls") or 1) > 1
        for e in f.get("attempt_errors", []):
            prod[f"attempt_error:{e}"] += 1
        if f["producer_status"] == "PROPOSAL":
            host[f"final:{f.get('final_status')}|{reason_class(f.get('final_reason'))}"] += 1
            reasons_admit[f"{f.get('admission', {}).get('status')}|{reason_class(f.get('admission', {}).get('reason'))}"] += 1
            reasons_proj[f"{f.get('projection_status')}|{reason_class(f.get('projection_reason'))}"] += 1
            for r in f["requests"]:
                f_ops[r["operation"]] += 1
                h7["producer_kind:" + str(r.get("producer_kind")) + "->" + str(r.get("effective_kind"))] += 1
                h7["authority:" + str(r.get("h7_authority"))] += 1
                for rule in r.get("h7_rules") or []:
                    h7["rule:" + rule] += 1
                if r.get("expr") and re.search(r"\b(OR|NOT)\(", r["expr"]):
                    ops_logic["request_with_OR_NOT"] += 1
            if len(f["requests"]) > 1:
                ops_logic["multi_request"] += 1
            for c in f["conditions"]:
                key = f"{c['dimension']}/{c['operator']}/{c['polarity']}" if c["dimension"] != "UNSUPPORTED" else "UNSUPPORTED"
                f_dims[key] += 1
                row = funnel[c["dimension"]]
                row["proposed"] += 1
                row["gate_admitted"] += bool(g.get("admitted"))
                if c.get("host_status") == "READY":
                    row["host_ready"] += 1
                else:
                    row[f"host_unresolved:{reason_class(c.get('host_reason'))}"] += 1
                if c.get("projection_reason"):
                    row[f"projection_unsupported:{reason_class(c['projection_reason'])}"] += 1
                if c["dimension"] == "VALUE" and str(c.get("host_reason") or "").startswith("VALUE_OPERATOR"):
                    value_classes[_classify(" ".join(c["quotes"]), _VALUE_CLASSES)] += 1
                if c["dimension"] == "UNSUPPORTED":
                    unsupported_classes[_classify(" ".join(c["quotes"]) + " " + str(c.get("unsupported_label")),
                                                  _UNSUPPORTED_CLASSES)] += 1
                if c["dimension"] == "UNSUPPORTED" and c.get("unsupported_label"):
                    labels[fold(c["unsupported_label"])] += 1
        s = stage_label(cap)
        stage[s] += 1
        stage_by_gate[f"{'ADMITTED' if g.get('admitted') else 'NOT_ADMITTED'}|{s}"] += 1
        by_slice[cap["slice"]][s] += 1
        if leg:
            for c in leg["conditions"]:
                l_dims[f"{c['dimension']}/{c['polarity']}"] += 1
            handled[leg["handled_as"]] += 1
            routes[leg["route_reason"]] += 1
            for c in leg["conditions"]:
                legacy_ops[f"{c['dimension']}/{c.get('level') or c['operator']}"] += 1
            if f["producer_status"] == "PROPOSAL" and f["requests"]:
                _disagree(f, leg, disagree)
                l_time = next((c["operator"] for c in leg["conditions"] if c["dimension"] == "TIME"), "none")
                f_time = sorted({c["operator"] for c in f["conditions"] if c["dimension"] == "TIME"}) or ["none"]
                f_time = f_time + (["+UNSUPPORTED"] if any(c["dimension"] == "UNSUPPORTED" for c in f["conditions"]) else [])
                time_cross[f"legacy:{l_time} | foundation:{'/'.join(f_time)}"] += 1
                if f.get("final_status") == "READY":  # executed: does the plan carry what legacy saw?
                    f_dim_set = {c["dimension"] for c in f["conditions"]}
                    for dim in sorted({c["dimension"] for c in leg["conditions"] if c["polarity"] == "POSITIVE"}):
                        if dim not in f_dim_set:
                            silent[f"{dim}|{'admitted' if g.get('admitted') else 'not_admitted'}"] += 1
                    if (l_time in {"during_month", "between_dates"} and f_time == ["during_year"]):
                        silent[f"TIME_NARROWED_TO_YEAR|{'admitted' if g.get('admitted') else 'not_admitted'}"] += 1
            elif f["producer_status"] == "PROPOSAL":
                no_request[f"legacy_handled_as:{leg['handled_as']}"] += 1
    return {"n": len(caps), "gate": dict(gate), "producer": dict(prod),
            "producer_latency_ms": {"final_attempt_p50": pct(lat_final, .5), "final_attempt_p90": pct(lat_final, .9),
                                    "final_attempt_max": max([v for v in lat_final if v is not None], default=None),
                                    "with_retry_p50": pct(lat_total, .5), "with_retry_p90": pct(lat_total, .9)},
            "retried_calls": retries, "stage_without_gold": dict(stage), "stage_by_gate": dict(stage_by_gate),
            "stage_by_slice": {k: dict(v) for k, v in by_slice.items()},
            "host_final": dict(host), "admission": dict(reasons_admit), "projection": dict(reasons_proj),
            "h7": dict(h7), "foundation_dims": dict(f_dims), "legacy_dims": dict(l_dims),
            "foundation_ops": dict(f_ops), "foundation_logic": dict(ops_logic),
            "unsupported_labels_ge5": {k: v for k, v in labels.items() if v >= 5},
            "legacy_handled_as": dict(handled), "legacy_routes": dict(routes),
            "condition_funnel_by_dimension": {k: dict(v) for k, v in funnel.items()},
            "ready_but_legacy_saw_dimension_foundation_lacks": dict(silent),
            "value_operator_rejections_by_class": dict(value_classes),
            "unsupported_conditions_by_class": dict(unsupported_classes),
            "legacy_condition_ops": dict(legacy_ops), "time_cross_legacy_vs_foundation": dict(time_cross),
            "foundation_no_request_vs_legacy": dict(no_request),
            "foundation_vs_legacy": {k: dict(v) for k, v in disagree.items()}}


_VALUE_CLASSES = (("ORDER_LANGUAGE", r"\b(caro|caros|cara|altos?|costos[oa]s?|barat[oa]s?|bajos?|mayor valor|menor valor|"
                                     r"mayores contratos|grandes?)\b"),
                  ("BETWEEN_RANGE", r"\bentre\b|\d\s+y\s+\d"),
                  ("INCLUSIVE_BOUND", r"\b(o mas|o menos|al menos|minimo|maximo|desde|hasta|no mas|no menos|no mayores?|"
                                     r"no menores?|a lo sumo|en adelante|igual)\b"),
                  ("COMPARATIVE_LEXICON_GAP", r"\b(mas|menos|encima|debajo|mayor|menor|cuantia)\b"))
_UNSUPPORTED_CLASSES = (("ACTOR_LIKE", r"proveedor|contratista|consorcio|nit\b|persona|empresa|razon social|nombre|cedula|union temporal|"
                                      r"\d{7,}|identific|document"),
                        ("TIME_LIKE", r"mes|trimestre|semestre|fecha|periodo|ayer|hoy|semana|reciente|ultim|dia|mensual|reciente"),
                        ("ORDER_LIKE", r"orden|caro|alto|mayor|menor|ranking|top|mejor"),
                        ("GEOGRAPHY_LIKE", r"ciudad|municipio|region|zona|departamento|ubicacion|territorio"))


def _classify(text: str, classes) -> str:
    folded = fold(text)
    return next((name for name, pattern in classes if re.search(pattern, folded)), "OTHER")


def _disagree(f: dict, leg: dict, table: dict) -> None:
    fdims = defaultdict(int)
    for c in f["conditions"]:
        key = c["dimension"] if c["dimension"] != "GEOGRAPHY" else "GEOGRAPHY"
        fdims[key] += 1
    ldims = defaultdict(int)
    for c in leg["conditions"]:
        ldims[c["dimension"]] += 1
    for dim in sorted(set(fdims) | set(ldims) | {"ORDER", "ACTOR"}):
        a, b = fdims.get(dim, 0) > 0, ldims.get(dim, 0) > 0
        table[dim]["both" if a and b else "foundation_only" if a else "legacy_only" if b else "neither"] += 1
    f_subjects = {r["subject"] for r in f["requests"]}
    l_universe = {{"PROCESSES": "processes", "CONTRACTS": "contracts"}[u] for u in leg["universe"]}
    table["UNIVERSE"]["agree" if f_subjects == l_universe else "disagree"] += 1
    f_opp = any(r.get("effective_kind") == "opportunities" for r in f["requests"])
    table["VIEW_OPPORTUNITY"]["agree" if f_opp == (leg["view"] == "OPPORTUNITIES") else
                              ("foundation_only" if f_opp else "legacy_only")] += 1


# --------------------------------------------------------------------------- io / render
def read_jsonl(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def load_gold(spec: list[str]) -> list[dict]:
    from gold_schema import validate

    files: list[Path] = []
    for item in spec:
        p = Path(item)
        files.extend(sorted(p.glob("*.jsonl")) if p.is_dir() else [p])
    golds, errors = [], []
    for path in files:
        for record in read_jsonl(path):
            problems = validate(record)
            if problems:
                errors.extend(problems)
            golds.append(record)
    if errors:
        print("GOLD VALIDATION ERRORS:", *errors[:30], sep="\n  ", file=sys.stderr)
        raise SystemExit(f"{len(errors)} gold validation errors; fix labels before joining")
    return golds


def render_md(result: dict) -> str:
    lines = ["# Gap matrix (generado por gap_matrix.py)", ""]
    dist = result["distribution"]
    lines += [f"Entradas capturadas: {dist['n']}. Sin texto de consultas; sólo gold_id y agregados.", "",
              "## Etapa por entrada (sin Gold)", "", "| etapa | n |", "|---|---:|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(dist["stage_without_gold"].items(), key=lambda kv: -kv[1])]
    gold = result.get("gold")
    if gold:
        lines += ["", "## Matriz por dimensión (GOLD / LEGACY / FOUNDATION)", "",
                  "| fila | gold | L_OK | L_MISSING | L_WRONG | F:OK | F_NOT_PROPOSED | F_HOST_UNRESOLVED | "
                  "HOST_PROJ_UNSUP | IR_CANNOT | GATE_EXCL | L-only-OK | F-only-OK |", "|---|" + "---:|" * 12]
        for key, row in sorted(gold["matrix"].items()):
            f, l, p = row["FOUNDATION"], row["LEGACY"], row["PARITY"]
            lines.append(f"| {key} | {row['GOLD']} | {l.get('L_OK', 0)} | {l.get('L_MISSING', 0)} | {l.get('L_WRONG', 0)} | "
                         f"{f.get('OK', 0)} | {f.get('F_NOT_PROPOSED', 0)} | {f.get('F_PROPOSED_HOST_UNRESOLVED', 0)} | "
                         f"{f.get('HOST_RESOLVED_PROJECTION_UNSUPPORTED', 0)} | {f.get('IR_CANNOT_EXPRESS', 0)} | "
                         f"{f.get('GATE_EXCLUDED', 0)} | {p.get('LEGACY_ONLY_OK', 0)} | {p.get('FOUNDATION_ONLY_OK', 0)} |")
        lines += ["", f"Nivel entrada: {json.dumps(gold['entry_level'], sort_keys=True)}"]
    return "\n".join(lines) + "\n"


def run(captures: Path, gold_spec: list[str] | None) -> dict:
    caps = read_jsonl(captures / "captures.jsonl") if captures.is_dir() else read_jsonl(captures)
    by_id = {c["gold_id"]: c for c in caps}
    result = {"meta": {"captures": len(caps), "with_gold": bool(gold_spec)}, "distribution": distribution(caps)}
    stability = (captures / "stability.json") if captures.is_dir() else captures.with_name("stability.json")
    if stability.exists():  # written by capture_foundation_legacy.py --stage stability (ids only)
        report = json.loads(stability.read_text())
        result["stability"] = {k: report[k] for k in ("sample", "compared", "raw_identical", "n_canonical_changed",
                                                      "n_status_changed", "n_outcome_changed", "canonical_changed")}
        result["stability"]["outcome_changed_ids"] = [o["gold_id"] for o in report["outcome_changed"]]
        result["stability"]["latency_ms_p50"] = {k: pct(v, .5) for k, v in report["latency_ms"].items()}
    if gold_spec:
        golds = load_gold(gold_spec)
        result["meta"]["gold"] = len(golds)
        result["gold"] = aggregate_gold(golds, by_id)
    return result


# --------------------------------------------------------------------------- selftest
def _fixture() -> tuple[list[dict], dict]:
    """Synthetic gold + captures (no real text) that exercise every Foundation category."""
    def cond(cid, dim, op, quote, pol="POSITIVE", hint=None, level=None):
        return {"id": cid, "dimension": dim, "operator": op, "polarity": pol, "quote": quote,
                "value_hint": hint or quote, "level": level}

    def gold(gid, conds, universe="CONTRACTS", view="RECORDS", logic=None, operation="SEARCH", extra=None,
             redirect=None):
        request = {"request_id": "q1", "universe": universe, "view": view, "universe_quote": "contratos",
                   "conditions": conds, "logic": logic or f"AND({','.join(c['id'] for c in conds)})"}
        if redirect:
            request[REDIRECT_FIELD] = redirect
        return {"gold_id": gid, "slice": "SYNTHETIC", "operation": operation, "expected_disposition": "EXECUTE",
                "ambiguity": "CLEAR", "requests": [request], **(extra or {})}

    def fcond(cid, dim, op, quote, host="READY", reason="", proj=None, pol="POSITIVE", label=None):
        return {"id": cid, "request_ids": ["r1"], "dimension": dim, "operator": op, "polarity": pol, "role": "FILTER",
                "quotes": [quote], "unsupported_label": label, "host_status": host, "host_reason": reason,
                "projection_reason": proj}

    def cap(gid, conds, admitted=True, status="PROPOSAL", subject="contracts", expr="AND(c1)", kind="records",
            final="READY", legacy_conds=None, handled="SEARCH", universe=("CONTRACTS",), view="RECORDS", op="SEARCH",
            level=None, f_redirect=None, notices=None, effects=None, needs_clar=False):
        return {"gold_id": gid, "slice": "SYNTHETIC", "gate": {"admitted": admitted},
                "legacy": {"dims": {"conditions": legacy_conds or [], "universe": list(universe), "view": view,
                                    "handled_as": handled, "route_reason": "heuristic_only", "owner": "TABULAR",
                                    "universe_notices": notices or [], "universe_effects": effects or [],
                                    "needs_clarification": needs_clar}},
                "foundation": {"producer_status": status, "final_status": final, "conditions": conds,
                               "requests": [{"request_id": "r1", "operation": op, "subject": subject, "expr": expr,
                                             "effective_kind": kind,
                                             **({REDIRECT_FIELD: f_redirect} if f_redirect else {})}],
                               "admission": {"status": "READY", "reason": ""}, "projection_status": "READY",
                               "projection_request_level": level, "projection_reason": ""}}

    def lcond(dim, op, value, pol="POSITIVE", **extra):
        return {"dimension": dim, "operator": op, "polarity": pol, "value": value, "key": "k", **extra}

    REDIRECT_OK = {"executed_universe": "CONTRACTS", "notice": "ACTOR_PROCESSES_TO_CONTRACTS"}
    AWARD = {"executed_universe": "CONTRACTS", "notice": "ACTOR_AWARDED_TO_CONTRACTS", "effect": "APPROXIMATION"}
    PRE = {"expected_disposition": "CLARIFY", "disposition_reason": PREAWARD_REASON}
    actor_state = lambda hint: [cond("c1", "ACTOR", "is", "CONSORCIO X"), cond("c2", "STATE", "is", "x", hint=hint)]
    golds = [
        gold("T01", [cond("c1", "TOPIC", "contains", "aseo")]),
        gold("T02", [cond("c1", "GEOGRAPHY", "is", "Cali", level="MUNICIPALITY")]),
        gold("T03", [cond("c1", "TIME", "during_month", "marzo de 2025", hint="2025-03")]),
        gold("T04", [cond("c1", "VALUE", "greater_or_equal", "al menos 500 millones", hint="500000000")]),
        gold("T05", [cond("c1", "ORDER", "value_desc", "los más caros")]),
        gold("T06", [cond("c1", "ACTOR", "is", "CONSORCIO X")]),
        gold("T07", [cond("c1", "MODALITY", "family", "mínima cuantía", pol="NEGATIVE")]),
        gold("T08", [cond("c1", "TOPIC", "contains", "aseo")]),                       # topic proposed as other value
        gold("T09", [cond("c1", "BUYER", "is", "alcaldía de Soledad")]),              # gate excluded
        gold("T10", [cond("c1", "STATE", "family", "cerradas", hint="closed")], universe="PROCESSES"),
        gold("T11", [cond("c1", "TOPIC", "contains", "aseo"), cond("c2", "TOPIC", "contains", "vigilancia")],
             logic="OR(c1,c2)"),
        gold("T12", [cond("c1", "TOPIC", "contains", "aseo")], universe="UNSPECIFIED", view="RECORDS",
             extra={"product_view": "CURRENT_OPPORTUNITIES"}),
        gold("T13", [cond("c1", "TOPIC", "contains", "aseo")], universe="PROCESSES", view="OPPORTUNITIES"),
        gold("T14", [], operation="COUNT", extra={"aspect": "TOTAL"}),
        # contract v1.7 (P-ACT-2): PROCESSES + ACTOR must be executed on CONTRACTS and announced
        gold("T15", [cond("c1", "ACTOR", "is", "CONSORCIO X")], universe="PROCESSES", redirect=REDIRECT_OK),
        gold("T16", [cond("c1", "ACTOR", "is", "CONSORCIO X")], universe="PROCESSES", redirect=REDIRECT_OK),
        gold("T17", [cond("c1", "ACTOR", "is", "CONSORCIO X")], universe="PROCESSES", redirect=REDIRECT_OK),
        gold("T18", [cond("c1", "ACTOR", "is", "CONSORCIO X", pol="NEGATIVE")], universe="PROCESSES",
             view="OPPORTUNITIES", redirect=REDIRECT_OK),
        gold("T19", [cond("c1", "BUYER", "is", "Gobernación del Huila")], universe="PROCESSES"),  # BUYER: untouched
        # contract v1.8: ACTOR x STATE with the universe not cited
        *[gold(gid, actor_state("awarded"), universe="UNSPECIFIED", redirect=AWARD, extra={"product_view": "CONTRACTS"})
          for gid in ("T20", "T21", "T22", "T23", "T24")],
        *[gold(gid, actor_state("published"), universe="UNSPECIFIED", extra={"product_view": "CONTRACTS", **PRE})
          for gid in ("T25", "T26", "T27")],
        gold("T28", actor_state("awarded"), universe="CONTRACTS"),                 # explicit universe: no policy
    ]
    caps = {c["gold_id"]: c for c in [
        cap("T01", [fcond("c1", "TOPIC", "contains", "aseo")], legacy_conds=[lcond("TOPIC", "contains", "aseo")]),
        cap("T02", [fcond("c1", "GEOGRAPHY", "is", "Cali", "NEEDS_CLARIFICATION", "UNRESOLVED_HOST_DIMENSION:GEOGRAPHY")],
            final="NEEDS_CLARIFICATION", legacy_conds=[lcond("GEOGRAPHY", "is", "Cali", level="MUNICIPALITY")]),
        cap("T03", [fcond("c1", "UNSUPPORTED", "is", "marzo de 2025", "UNSUPPORTED", "UNSUPPORTED_CONDITION", label="mes")],
            legacy_conds=[lcond("TIME", "during_month", "2025-03")]),
        cap("T04", [fcond("c1", "VALUE", "greater_than", "al menos 500 millones")],
            legacy_conds=[lcond("VALUE", "greater_or_equal", "500000000", inclusive_bound=True)]),
        cap("T05", [], legacy_conds=[lcond("ORDER", "value_desc", "valor_desc")]),
        cap("T06", [fcond("c1", "UNSUPPORTED", "is", "CONSORCIO X", "UNSUPPORTED", "UNSUPPORTED_CONDITION", label="proveedor")],
            legacy_conds=[lcond("ACTOR", "is", "consorcio x")]),
        cap("T07", [fcond("c1", "MODALITY", "family", "mínima cuantía", proj="UNSUPPORTED_MODALITY_POLARITY", pol="NEGATIVE")],
            legacy_conds=[lcond("MODALITY", "family", "minima_cuantia", "NEGATIVE")]),
        cap("T08", [fcond("c1", "TOPIC", "contains", "limpieza")], legacy_conds=[lcond("TOPIC", "contains", "limpieza")]),
        cap("T09", [fcond("c1", "BUYER", "is", "alcaldía de Soledad")], admitted=False,
            legacy_conds=[lcond("BUYER", "is", "ALCALDIA DE SOLEDAD")]),
        cap("T10", [fcond("c1", "STATE", "is", "cerradas", "NEEDS_CLARIFICATION", "UNRESOLVED_HOST_DIMENSION:STATE")],
            subject="processes", final="NEEDS_CLARIFICATION", universe=("PROCESSES",),
            legacy_conds=[lcond("STATE", "is", "closed")]),
        cap("T11", [fcond("c1", "TOPIC", "contains", "aseo"), fcond("c2", "TOPIC", "contains", "vigilancia")],
            expr="OR(c1,c2)", level="NONCONJUNCTIVE_EXPRESSION",
            legacy_conds=[lcond("TOPIC", "contains", "aseo vigilancia")]),
        cap("T12", [fcond("c1", "TOPIC", "contains", "aseo")], legacy_conds=[lcond("TOPIC", "contains", "aseo")],
            universe=("PROCESSES",), view="OPPORTUNITIES"),
        cap("T13", [fcond("c1", "TOPIC", "contains", "aseo")], subject="processes", kind="records",
            universe=("PROCESSES",), view="OPPORTUNITIES", legacy_conds=[lcond("TOPIC", "contains", "aseo")]),
        cap("T14", [], op="SEARCH", legacy_conds=[], handled="SEARCH"),
        # T15: faithful PROCESSES proposal + host redirect announced (both arms OK)
        cap("T15", [fcond("c1", "UNSUPPORTED", "is", "CONSORCIO X", "UNSUPPORTED", "UNSUPPORTED_CONDITION", label="proveedor")],
            subject="processes", f_redirect=REDIRECT_OK, universe=("CONTRACTS",), notices=[REDIRECT_OK["notice"]],
            legacy_conds=[lcond("ACTOR", "is", "consorcio x")]),
        # T16: both arms execute on processes: universe failure
        cap("T16", [fcond("c1", "UNSUPPORTED", "is", "CONSORCIO X", "UNSUPPORTED", "UNSUPPORTED_CONDITION", label="proveedor")],
            subject="processes", universe=("PROCESSES",), legacy_conds=[lcond("ACTOR", "is", "consorcio x")]),
        # T17: Foundation blocks where the Gold executes; legacy switches to contracts in silence
        cap("T17", [fcond("c1", "UNSUPPORTED", "is", "CONSORCIO X", "UNSUPPORTED", "UNSUPPORTED_CONDITION", label="proveedor")],
            subject="processes", final="NEEDS_CLARIFICATION", universe=("CONTRACTS",),
            legacy_conds=[lcond("ACTOR", "is", "consorcio x")]),
        # T18: Foundation redirect evidence disagrees with the Gold; legacy keeps the processes scope
        cap("T18", [fcond("c1", "UNSUPPORTED", "is", "CONSORCIO X", "UNSUPPORTED", "UNSUPPORTED_CONDITION", label="proveedor",
                          pol="NEGATIVE")],
            subject="processes", kind="opportunities", f_redirect={"executed_universe": "PROCESSES", "notice": "NONE"},
            universe=("PROCESSES", "CONTRACTS"), view="OPPORTUNITIES",
            legacy_conds=[lcond("ACTOR", "is", "consorcio x", "NEGATIVE")]),
        cap("T19", [fcond("c1", "BUYER", "is", "Gobernación del Huila")], subject="processes", universe=("PROCESSES",),
            legacy_conds=[lcond("BUYER", "is", "GOBERNACION DEL HUILA")]),
        # T20: awarded -> contracts, notice AND approximation declared (both arms OK)
        cap("T20", [], f_redirect=AWARD, universe=("CONTRACTS",), notices=[AWARD["notice"]], effects=["APPROXIMATION"]),
        # T21: STATE_ALWAYS_WINS shape: both arms executed on processes (universe failure)
        cap("T21", [], subject="processes", f_redirect={"executed_universe": "PROCESSES", "notice": "NONE"},
            universe=("PROCESSES",)),
        # T22: contracts + notice but the approximation is not declared as such (failure for both arms)
        cap("T22", [], f_redirect={k: v for k, v in AWARD.items() if k != "effect"}, universe=("CONTRACTS",),
            notices=[AWARD["notice"]]),
        # T23: silent switch to contracts: no notice at all (legacy); Foundation has no host evidence
        cap("T23", [], universe=("CONTRACTS",)),
        # T24: blocked where the Gold executes
        cap("T24", [], final="NEEDS_CLARIFICATION", universe=("CONTRACTS",), needs_clar=True),
        # T25: ACTOR_ALWAYS_WINS shape: pre-award executed on contracts (failure in both arms)
        cap("T25", [], universe=("CONTRACTS",)),
        # T26: pre-award executed on processes: failure in ANY universe
        cap("T26", [], subject="processes", universe=("PROCESSES",)),
        # T27: pre-award clarified by both arms
        cap("T27", [], final="NEEDS_CLARIFICATION", handled="CLARIFY", universe=("CONTRACTS",), needs_clar=True),
        cap("T28", [], universe=("CONTRACTS",)),
    ]}
    return golds, caps


def selftest() -> int:
    golds, caps = _fixture()
    expect = {  # gold_id -> (row key, foundation category, legacy category)
        "T01": ("TOPIC", "OK", "L_OK"), "T02": ("GEOGRAPHY/MUNICIPALITY", "F_PROPOSED_HOST_UNRESOLVED", "L_OK"),
        "T03": ("TIME/during_month", "IR_CANNOT_EXPRESS", "L_OK"), "T04": ("VALUE/greater_or_equal", "IR_CANNOT_EXPRESS", "L_OK"),
        "T05": ("ORDER/value_desc", "IR_CANNOT_EXPRESS", "L_OK"), "T06": ("ACTOR", "IR_CANNOT_EXPRESS", "L_OK"),
        "T07": ("MODALITY", "HOST_RESOLVED_PROJECTION_UNSUPPORTED", "L_OK"),
        "T08": ("TOPIC", "F_NOT_PROPOSED", "L_WRONG"), "T09": ("BUYER", "GATE_EXCLUDED", "L_OK"),
        "T10": ("STATE", "F_PROPOSED_HOST_UNRESOLVED", "L_OK"),
        "T11": ("LOGIC_OR_NOT", "HOST_RESOLVED_PROJECTION_UNSUPPORTED", "L_WRONG"),
        "T12": ("UNIVERSE_VIEW", "IR_CANNOT_EXPRESS", "L_OK"), "T13": ("UNIVERSE_VIEW", "F_NOT_PROPOSED", "L_OK"),
        "T14": ("OPERATION/COUNT", "F_NOT_PROPOSED", "L_WRONG"),
        "T15": ("UNIVERSE_VIEW", "OK", "L_OK"),
        "T16": ("UNIVERSE_VIEW", "F_PROPOSED_HOST_UNRESOLVED", "L_WRONG"),
        "T17": ("UNIVERSE_VIEW", "F_PROPOSED_HOST_UNRESOLVED", "L_WRONG"),
        "T18": ("UNIVERSE_VIEW", "F_PROPOSED_HOST_UNRESOLVED", "L_WRONG"),
        "T19": ("UNIVERSE_VIEW", "OK", "L_OK"),   # BUYER over processes is not redirected
        "T20": ("UNIVERSE_VIEW", "OK", "L_OK"),
        "T21": ("UNIVERSE_VIEW", "F_PROPOSED_HOST_UNRESOLVED", "L_WRONG"),
        "T22": ("UNIVERSE_VIEW", "F_PROPOSED_HOST_UNRESOLVED", "L_WRONG"),
        "T23": ("UNIVERSE_VIEW", "IR_CANNOT_EXPRESS", "L_WRONG"),
        "T24": ("UNIVERSE_VIEW", "IR_CANNOT_EXPRESS", "L_WRONG"),
        "T25": ("ACTOR_STATE_POLICY", "F_PROPOSED_HOST_UNRESOLVED", "L_WRONG"),
        "T26": ("ACTOR_STATE_POLICY", "F_PROPOSED_HOST_UNRESOLVED", "L_WRONG"),
        "T27": ("ACTOR_STATE_POLICY", "OK", "L_OK"),
        "T28": ("UNIVERSE_VIEW", "OK", "L_OK"),   # explicit CONTRACTS: no policy, no notice, no approximation
    }
    details = {  # gold_id -> (Foundation detail, legacy detail) of the UNIVERSE_VIEW row
        "T15": ("", ""), "T16": ("UNIVERSE_NOT_REDIRECTED", "UNIVERSE"),
        "T17": ("BLOCKED_WHERE_GOLD_EXECUTES:NEEDS_CLARIFICATION", "NOTICE_MISSING"),
        "T18": ("REDIRECT_MISMATCH", "UNIVERSE"), "T19": ("", ""),
        "T20": ("", ""), "T21": ("REDIRECT_MISMATCH", "UNIVERSE"), "T22": ("REDIRECT_MISMATCH", "EFFECT_MISSING"),
        "T23": ("UNSPECIFIED_UNIVERSE", "NOTICE_MISSING"), "T24": ("UNSPECIFIED_UNIVERSE", "BLOCKED_WHERE_GOLD_EXECUTES"),
        "T25": ("EXECUTED_ACTOR_PREAWARD", "EXECUTED_ACTOR_PREAWARD"),
        "T26": ("EXECUTED_ACTOR_PREAWARD", "EXECUTED_ACTOR_PREAWARD"), "T27": ("", ""), "T28": ("", ""),
    }
    failures = []
    for gold in golds:
        cap = caps[gold["gold_id"]]
        ev = evaluate(gold, cap)
        rows = [operation_row(gold, cap)] if gold["operation"] != "SEARCH" else ev["rows"]
        key, f_cat, l_cat = expect[gold["gold_id"]]
        hit = [r for r in rows if r["key"] == key]
        want_detail = details.get(gold["gold_id"])
        if hit and want_detail and (hit[0]["f_detail"], hit[0]["l_detail"]) != want_detail:
            failures.append((gold["gold_id"], "DETAIL", want_detail, (hit[0]["f_detail"], hit[0]["l_detail"])))
        if not hit or hit[0]["foundation"] != f_cat or hit[0]["legacy"] != l_cat:
            failures.append((gold["gold_id"], key, f_cat, l_cat, [(r["key"], r["foundation"], r["legacy"]) for r in rows]))
    result = aggregate_gold(golds, caps)
    assert result["entry_level"].get("EVALUATED", 0) >= 10
    assert any(r["gold_id"] == "T09" and r["culprit"] == "GATE_EXCLUDED" for r in result["regressions_legacy_ok_foundation_not"])
    assert distribution(list(caps.values()))["n"] == len(caps)
    policy = result["matrix"].get("ACTOR_STATE_POLICY")   # v1.8: CLARIFY records keep ONLY the policy row
    assert policy and policy["GOLD"] == 3 and policy["FOUNDATION"].get("OK") == 1 and policy["LEGACY"].get("L_OK") == 1, policy
    if failures:
        for failure in failures:
            print("SELFTEST FAIL", failure)
        return 1
    print(f"selftest OK: {len(golds)} fixtures, categories covered:",
          sorted({r["foundation"] for g in golds for r in evaluate(g, caps[g['gold_id']])['rows']}))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--captures", type=Path, help="captures dir (captures.jsonl) or the jsonl file itself")
    ap.add_argument("--gold", nargs="*", help="Gold label dir(s) / file(s); omit for the no-Gold distribution")
    ap.add_argument("--out-json", type=Path)
    ap.add_argument("--out-md", type=Path)
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args(argv)
    if args.selftest:
        return selftest()
    if not args.captures:
        ap.error("--captures is required")
    result = run(args.captures, args.gold)
    if args.out_json:
        args.out_json.write_text(json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True) + "\n")
    if args.out_md:
        args.out_md.write_text(render_md(result))
    if not args.out_json and not args.out_md:
        print(json.dumps(result, ensure_ascii=False, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
