"""M2A-8a0 oracle A: the GOLD audit, and the harness that runs both oracles over the cases.

GOLD decides MEANING, the CONTRACT decides mechanical invariants, and the oracle verifies both with two independent
pure functions over dicts (neither imports the reducer or any M2a module):

* ``audit(actuals)`` (this module): is the adjudicated meaning right? Disposition, code, the effective as a SEMANTIC
  projection and the effects. The projection (``project_effective``) keeps what the Gold's resulting IR says: the requests'
  (subject, universe_kind), every condition's dimension, slot, polarity, operator, level and encoded value, the unit of a
  VALUE, the direction source of an ORDER, and the absence of logic, all as multisets (the order is the reducer's);
* ``m2a_oracle_transition.check_transition(base, patch, effective)``: did the reducer get there without falsifying
  identity or provenance? ``audit_transitions`` runs it over the cases of the Gold; M2A-3 calls it from its own
  properties over GENERATED bases too.

A reducer passes the oracle when ``audit(...)["ok"]`` and ``audit_transitions(...)["ok"]``.

``audit`` declares and reports its COVERAGE (what was audited, against what the seal holds) and what it could not verify.
"""

from __future__ import annotations

import collections
import copy
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audits.m2a_conversation import m2a_oracle_gold as gold  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_expect import (  # noqa: E402
    APPLICABLE, NO_PRODUCER, load_cases,
)
from scripts.audits.m2a_conversation.m2a_oracle_gold import canon, digest, sha  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_transition import check_transition  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_typing import condition_dict, dataset_of, origin  # noqa: E402

ACTUAL_KEYS = frozenset({"disposition", "code", "effective", "effects"})

# What the GOLD projection leaves out: all of it mechanics, which the contract decides and ``check_transition`` checks.
PROJECTION_EXCLUSIONS = (
    {"field": "effective.schema, exact keys of the effective, of requests and of conditions", "to": "T-STRUCT"},
    {"field": "requests[].request_id, scope_id, universe_origin, view, dataset", "to": "T-REQ-UNTOUCHED / T-REQ-REDIRECT / "
                                                                                        "T-REQ-ORIGIN / T-REQ-DATASET"},
    {"field": "conditions[].condition_id", "to": "T-COND-CARRIED (carried) / T-STRUCT (unique); placed ids are the reducer's"},
    {"field": "conditions[].request_ids (which requests a condition applies to)", "to": "T-COND-REQUESTS"},
    {"field": "conditions[].authority, receipt_rules, policies, origin, provenance (but VALUE unit and ORDER direction "
              "source)", "to": "T-COND-CARRIED (carried) / T-COND-PLACED (placed)"},
    {"field": "which conditions are carried and which placed", "to": "T-COND-CARRIED / T-COND-PLACED / T-COND-FABRICATED / "
                                                                       "T-COND-WRITTEN-SLOT / T-COND-REMOVAL / T-COND-ATOMIC"},
    {"field": "effective.policies, base_trace_id, root_trace_id", "to": "T-STRUCT (policies well formed); the rest is "
                                                                        "informative (§3.2, §6.6): LG-REQUEST-POLICIES"},
)

# What neither oracle can see or the Gold does not discriminate, by name. The manifest declares them.
LIMITED_GUARANTEES = (
    {"id": "LG-BASES-WITHOUT-HOST-FIELDS", "kind": "ORACLE_BLIND_SPOT",
     "what": "on the Gold's own bases policies, receipt_rules and provenance.{source_amount, year, year_source, buyer_scope, "
             "inherited_from} are empty (the Gold carries none), so rebuilding a carried condition with those defaults is "
             "invisible there",
     "closed_by": "M2A-3 properties that call check_transition over GENERATED bases with every field filled, a dual anchor "
                  "and a view (the oracle invents no content in the Gold's bases)"},
    {"id": "LG-DATASET-VALUE", "kind": "ORACLE_BLIND_SPOT",
     "what": "the value of a redirected request's dataset is the host table's: without ``dataset_of`` the check is "
             "relational (never the old one, never another subject's)",
     "closed_by": "M2A-3 passes its own subject-to-dataset table as dataset_of"},
    {"id": "LG-REQUEST-POLICIES", "kind": "ORACLE_BLIND_SPOT",
     "what": "which policies the host re-decides on the composed effective (§6.6) is not determined by the Gold or the "
             "contract text: any well-formed policy list over the effective's conditions passes",
     "closed_by": "the authorities' own gates (E-*/F)"},
    {"id": "LG-FRESH-ACCEPTANCE", "kind": "ORACLE_BLIND_SPOT",
     "what": "the effective of a NEW_SEARCH is built by from_acceptance (M2A-1b), not the reducer: only its meaning is "
             "compared and ``audit`` reports it as unverified when it is not supplied; no transition is checked",
     "closed_by": "M2A-1b / M2A-6"},
    {"id": "LG-CLASSIFICATION-CERTIFIED-BY-8B", "kind": "SCOPE",
     "what": "the §20.2 gate «búsqueda nueva y delta confundidos» is certified by M2A-8b with the real producer, not by this "
             "oracle: it measures the §7.2 scope only where the facts of P determine it UNIQUELY, reads the absence of every "
             "continuation marker as «no evidence» (closed world over the labels) and cannot see the «verb of request» that "
             "makes a frame, so every lone content condition is NO_MEDIBLE for classification",
     "closed_by": "M2A-8b"},
    {"id": "GOLD-GAP-F16-ANY-VALUE", "kind": "GOLD_GAP",
     "what": "a reducer that inverts the polarity of the single positive of a slot whatever the value of the negative SET "
             "is not discriminated by the Gold: no applicable case sets a negative of another value over a single positive",
     "closed_by": "check_transition T-COND-REMOVAL on any input that exercises it (killed by f16_any_value)"},
    {"id": "GOLD-GAP-SET-CLEARS-WHOLE-DIMENSION", "kind": "GOLD_GAP",
     "what": "a reducer whose SET of one polarity removes the other polarity of the dimension is not discriminated by the "
             "Gold: no applicable case carries both polarities of a dimension through a SET",
     "closed_by": "check_transition T-COND-REMOVAL (killed by set_clears_whole_dimension)"},
    {"id": "GOLD-GAP-CLEAR-DIMENSION-ONLY-POSITIVE", "kind": "GOLD_GAP",
     "what": "a reducer whose CLEAR of a dimension leaves its negative conditions is not discriminated by the Gold: no "
             "applicable case clears a dimension that holds a negative condition",
     "closed_by": "check_transition T-COND-WRITTEN-SLOT (killed by clear_dimension_only_positive)"},
    {"id": "GOLD-GAP-H6-OVERFIRE", "kind": "GOLD_GAP",
     "what": "a reducer that over-fires H6 (a clarification for any new territory next to a buyer) is not discriminated: no "
             "applicable EXECUTE case has a territory next to a carried buyer (the H6 cases, SF123 and SF155, are the "
             "D-BUYER and E-GEO lanes')",
     "closed_by": "D-BUYER (buyer.territory) and a Gold case that executes a compatible territory over a carried buyer"},
    {"id": "GOLD-GAP-REPLACE-UNIVERSE-KEEPS-OLD-KIND", "kind": "GOLD_GAP",
     "what": "a reducer that keeps the old universe kind after REPLACE_UNIVERSE is not discriminated by the Gold: no "
             "applicable case changes the kind (SF075, records to opportunities, is declared dependent downstream)",
     "closed_by": "check_transition T-REQ-TARGET (killed by redirect_keeps_old_kind)"},
)


def project_effective(effective: dict | None):
    """The comparable MEANING of an effective ``m2a-accepted/1`` dict (see the module docstring). Raises ``ValueError``
    on an effective that is not of that shape: an actual that is not a dict of it is a mismatch, not a crash."""
    if effective is None:
        return None
    try:
        entries = []
        for c in effective["conditions"]:
            prov = c["provenance"]
            entries.append({"dimension": c["dimension"], "slot": c["slot"], "operator": c["operator"],
                            "polarity": c["polarity"], "level": c["level"], "value": list(c["value"]),
                            "unit": prov["unit"] if c["dimension"] == "VALUE" else None,
                            "order_direction_source": prov["order_direction_source"] if c["dimension"] == "ORDER" else None})
        return {"requests": sorted([r["subject"], r["universe_kind"]] for r in effective["requests"]),
                "logic": None if effective["logic"] is None else "PRESENT", "conditions": sorted(entries, key=canon)}
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError(f"malformed effective: {error!r}") from error


def effective_equal(have, want) -> bool:
    """The one comparison of two projections (the audit never compares them any other way)."""
    return have == want


def _mismatch(out: list, gid: str, field: str, expected, actual, source: str) -> None:
    item = {"gold_id": gid, "field": field}
    if source == "SYNTHETIC":
        item.update(expected=expected, actual=actual)
    else:  # REAL content never enters a report: digests only
        item.update(expected_sha256=digest(expected), actual_sha256=digest(actual))
    out.append(item)


def _decision_problems(cases: list[dict], decisions: dict[str, dict]) -> list[str]:
    """The cases must be the ones the verified decisions produce (no stale cases, no case hidden by an unrecorded one)."""
    out = []
    for c in cases:
        decided = "USER_DECISION" in c["rules"]
        if decided != (c["gold_id"] in decisions):
            out.append(f"{c['gold_id']}: the case and the verified decisions disagree")
        elif decided and decisions[c["gold_id"]]["owner"] not in c["owners"]:
            out.append(f"{c['gold_id']}: the decided owner is not the case's")
    return out


def audit(actuals: dict, cases: list[dict] | None = None, *, require_sources=("SYNTHETIC",),
          require_fresh: bool = False) -> dict:
    """The GOLD oracle: completeness (exactly the ``APLICABLE_M2A3`` ids), then per-case exact comparison of disposition,
    code, effective (semantic projection) and reducer-scope effects. Pure but for verifying the user's decisions against
    their records. The report declares its COVERAGE: ``ok`` is «no error in what was audited»; ``complete`` is «and that
    was every case the seal holds, with nothing left unverified». ``require_sources`` names the sources that must have
    cases loaded (asking for REAL with no REAL case fails); ``require_fresh`` turns an absent effective of a fresh
    acceptance into a mismatch instead of an ``unverified`` entry."""
    cases = load_cases() if cases is None else cases
    decisions = gold.load_decisions()
    decision_problems = _decision_problems(cases, decisions)
    sealed = gold.seal()["slices"]["M2A_GOLD"]
    loaded = collections.Counter(c["source"] for c in cases)
    by_id = {c["gold_id"]: c for c in cases}
    expected_ids = sorted(g for g, c in by_id.items() if c["applicability"] == APPLICABLE)
    missing, extra = sorted(set(expected_ids) - set(actuals)), sorted(set(actuals) - set(expected_ids))
    mismatches: list[dict] = []
    unverified: dict[str, list[str]] = {}
    for gid in sorted(set(expected_ids) & set(actuals)):
        case, actual = by_id[gid], actuals[gid]
        exp, src = case["expected"], case["source"]
        if type(actual) is not dict or set(actual) != ACTUAL_KEYS:
            _mismatch(mismatches, gid, "shape", sorted(ACTUAL_KEYS), sorted(actual) if type(actual) is dict else None, src)
            continue
        if actual["disposition"] != exp["disposition"]:
            _mismatch(mismatches, gid, "disposition", exp["disposition"], actual["disposition"], src)
        if actual["code"] != exp["code"]:
            _mismatch(mismatches, gid, "code", exp["code"], actual["code"], src)
        if exp["effective_scope"] == "FRESH_ACCEPTANCE" and actual["effective"] is None and not require_fresh:
            unverified.setdefault("fresh_acceptance_effective", []).append(gid)  # built by from_acceptance (M2A-1b)
        else:
            try:
                have = project_effective(actual["effective"])
            except ValueError as error:
                have = {"malformed": str(error)}
            if not effective_equal(have, exp["effective"]):
                _mismatch(mismatches, gid, "effective", exp["effective"], have, src)
        if exp["effects_status"] != "MEDIBLE":
            unverified.setdefault("effects_not_measurable", []).append(gid)
            continue
        effects = actual["effects"]
        listed = sorted(effects) if isinstance(effects, (list, tuple, set, frozenset)) else None
        if listed is None or len(listed) != len(set(listed)):
            _mismatch(mismatches, gid, "effects", exp["effects"], effects if listed is None else listed, src)
        elif not set(exp["effects"]) <= set(listed) or set(listed) - set(exp["effects"]) - set(exp["effects_gold"]):
            # what the reducer owes must be there; beyond it only what the Gold lists (echo-level, or the echo-level
            # reading of A8.1 after an explicit CLEAR) is allowed: an effect the Gold does not list is invented
            _mismatch(mismatches, gid, "effects", exp["effects"], listed, src)
    source_problems = [f"{s}: required but no case is loaded" for s in require_sources if not loaded.get(s)]
    coverage = {
        "sealed_m2a_gold": {s: len(v["ids"]) for s, v in sorted(sealed.items())},
        "loaded": dict(sorted(loaded.items())),
        "applicable_expected": dict(sorted(collections.Counter(by_id[g]["source"] for g in expected_ids).items())),
        "partitions": dict(sorted(collections.Counter(c["partition"] for c in cases).items())),
        "full_sources": sorted(s for s, v in sealed.items() if loaded.get(s) == len(v["ids"])),
        "required_sources": list(require_sources)}
    ok = not (missing or extra or mismatches or decision_problems or source_problems)
    return {"ok": ok, "complete": ok and not unverified and coverage["full_sources"] == sorted(sealed),
            "coverage": coverage, "expected": len(expected_ids), "actual": len(actuals), "missing": missing,
            "extra": extra, "mismatches": mismatches, "unverified": unverified, "source_problems": source_problems,
            "decision_problems": decision_problems,
            "declared_red": [{"gold_id": g, "owner": c["owners"][0], "owners": c["owners"]}
                             for g, c in by_id.items() if c["applicability"].startswith("DEPENDIENTE(")],
            "no_medible": [{"gold_id": g, "motive": c["applicability"] if c["applicability"] == NO_PRODUCER
                            else c["applicability"][len("NO_MEDIBLE("):-1]}
                           for g, c in by_id.items() if c["applicability"].startswith("NO_MEDIBLE")]}


def audit_transitions(actuals: dict, cases: list[dict] | None = None, *, dataset_of_fn=None) -> dict:
    """The CONTRACT oracle over the cases of the Gold: ``check_transition(base, patch, effective)`` for every applicable
    case whose effective was composed (a fresh acceptance has no transition) and which the reducer executed. ``ok`` is no
    violation; the violations name their rule (``TRANSITION_RULES``). The Gold's bases carry no host fields (see
    ``LG-BASES-WITHOUT-HOST-FIELDS``): M2A-3 also calls ``check_transition`` over generated bases."""
    cases = load_cases() if cases is None else cases
    violations, checked, skipped = [], 0, []
    for c in cases:
        if c["applicability"] != APPLICABLE or c["gold_id"] not in actuals:
            continue
        actual = actuals[c["gold_id"]]
        effective = actual.get("effective") if isinstance(actual, dict) else None
        if c["expected"]["effective_scope"] != "COMPOSED" or effective is None:
            skipped.append(c["gold_id"])
            continue
        checked += 1
        violations += [{"gold_id": c["gold_id"], **v} for v in check_transition(
            c["inputs"]["base"], c["inputs"]["patch"], effective, dataset_of=dataset_of_fn)]
    return {"ok": not violations, "checked": checked, "violations": violations, "not_checked": sorted(skipped)}


def audit_classification(scopes: dict, cases: list[dict] | None = None) -> dict:
    """The §7.2 scope (``IN`` / ``IN_NEW_SEARCH``) of every applicable case whose P facts determine the classification
    UNIQUELY (see ``LG-CLASSIFICATION-CERTIFIED-BY-8B``: the gate on confusing a new search with a delta is M2A-8b's)."""
    cases = load_cases() if cases is None else cases
    wanted = {c["gold_id"]: c for c in cases
              if c["applicability"] == APPLICABLE and c["expected"]["classification"]["measurable"]}
    bad = [g for g in sorted(set(wanted) & set(scopes)) if scopes[g] != wanted[g]["expected"]["scope"]]
    mismatches = [{"gold_id": g, "expected": wanted[g]["expected"]["scope"], "actual": scopes[g]}
                  if wanted[g]["source"] == "SYNTHETIC" else {"gold_id": g, "field": "scope"} for g in bad]
    missing, extra = sorted(set(wanted) - set(scopes)), sorted(set(scopes) - set(wanted))
    undetermined = sorted(c["gold_id"] for c in cases if c["applicability"] == APPLICABLE
                          and not c["expected"]["classification"]["measurable"])
    return {"ok": not (missing or extra or mismatches), "expected": len(wanted), "missing": missing, "extra": extra,
            "mismatches": mismatches, "no_medible_for_classification": undetermined}


# ---- a faithful reducer output, to prove both oracles accept it and to build mutants of it ------------------------------
def realize_effective(case: dict) -> dict | None:
    """The ``m2a-accepted/1`` dict a faithful reducer returns for the case: the base's requests it keeps, byte-identical;
    the redirected request with the op's origin, the named subject's ids and the dataset of its subject; the base's
    conditions it carries, byte-identical but for ``request_ids`` rewritten when a dual anchor is reduced; the patch's
    conditions it places, unchanged but for ids (the reducer's own). A fresh acceptance has its own requests."""
    e = case["expected"]["effective"]
    if e is None:
        return None
    base, patch = case["inputs"]["base"], case["inputs"]["patch"]
    redirect = next((op for op in patch["ops"] if op["op"] == "REPLACE_UNIVERSE"), None)
    if case["expected"]["effective_scope"] == "FRESH_ACCEPTANCE":
        requests = [{"request_id": f"q{n}", "scope_id": "s1", "subject": s, "universe_kind": k,
                     "universe_origin": origin("t1", sha(case["gold_id"] + "|effective"), 0), "view": None,
                     "dataset": dataset_of(s)} for n, (s, k) in enumerate(e["requests"], 1)]
    elif redirect is None:
        requests = copy.deepcopy(base["requests"])
    else:
        target = redirect["universe"]
        named = [r for r in base["requests"] if r["subject"] == target["subject"]]
        touched = copy.deepcopy((named or base["requests"])[0])
        touched.update(subject=target["subject"], universe_kind=target["universe_kind"],
                       universe_origin=copy.deepcopy(redirect["origin"]),
                       dataset=named[0]["dataset"] if named else dataset_of(target["subject"]))
        requests = [touched]
    base_rids = sorted(r["request_id"] for r in base["requests"])
    rids = sorted(r["request_id"] for r in requests)
    carried_pool = list(base["conditions"])
    placed_pool = [op["condition"] for op in patch["ops"] if op["condition"]]
    written = [op["slot"] for op in patch["ops"] if op["op"] in ("SET", "SET_ORDER", "CLEAR")]

    def covered(w, key):
        return key == w or key.startswith(w + "/")

    conditions = []
    for i, entry in enumerate(e["conditions"]):
        slot = entry["slot"]
        if any(covered(w, slot) for w in written):
            source = next(c for c in placed_pool if c["slot"] == slot and c["value"] == entry["value"])
            placed_pool.remove(source)
            condition = copy.deepcopy(source)
            condition["condition_id"] = f"o{i + 1}"
            condition["request_ids"] = rids
        else:
            source = next(c for c in carried_pool if c["slot"] == slot and c["value"] == entry["value"])
            carried_pool.remove(source)
            condition = copy.deepcopy(source)
            if rids != base_rids:
                condition["request_ids"] = rids
        conditions.append(condition)
    return {"schema": "m2a-accepted/1", "requests": requests, "conditions": conditions, "logic": None, "policies": [],
            "base_trace_id": "t0", "root_trace_id": "t0"}


def reference_actuals(cases: list[dict]) -> dict:
    return {c["gold_id"]: {"disposition": c["expected"]["disposition"], "code": c["expected"]["code"],
                           "effective": realize_effective(c), "effects": list(c["expected"]["effects"])}
            for c in cases if c["applicability"] == APPLICABLE}


__all__ = ["audit", "audit_transitions", "audit_classification", "check_transition", "project_effective",
           "realize_effective", "reference_actuals", "condition_dict"]
