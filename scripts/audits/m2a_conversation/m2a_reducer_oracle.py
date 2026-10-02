"""M2A-8a0: the ORACLE of the M2a reducer, independent of M2A-3.

Question it answers: *according to the sealed Gold, what must the reducer return for each case, and, given what it
returned, was it right?* Everything expected comes from the sealed ``M2A_GOLD`` (first-level adjudicated disposition,
``disposition_reason``, ``resulting_ir``, effects derived with the sealed checker's rules and annex B of the contract).
The replay can only COINCIDE with an expectation, never define it. Nothing here imports or calls the reducer or any
registered M2a module (the consumption pin treats ``scripts/`` as external): inputs and expectations are plain dicts in
the ``m2a-accepted/1`` / ``m2a-patch/1`` shapes of contract §3 and §5.

Parts: ``m2a_oracle_gold`` (reading the Gold), ``m2a_oracle_typing`` (``value_hint`` -> closed values, the dicts),
``m2a_oracle_applicability`` (dependency rules) and this module (expectation, projection, audit, artifact, CLI).

Public API (pure, deterministic):

* ``load_cases(real_dir=None)``  one record per ``M2A_GOLD`` case (FRONTIER never enters). Keys: ``gold_id``, ``source``,
  ``partition`` (DEV/TEST of the seal), ``applicability``, ``owners``, ``rules`` (what decided the applicability),
  ``flags``, ``inputs`` (``base``, ``patch``, ``p_facts``, ``context``) and ``expected`` (``disposition``, ``code``,
  ``effective`` as a semantic projection, ``effects``, ``scope``, ``classification``). ``inputs``/``expected`` exist
  only for ``APLICABLE_M2A3``: a dependent case is declared red with its owner, never given a guessed expectation.
* ``audit(actuals, cases=None)``  ``actuals = {gold_id: {disposition, code, effective, effects}}`` (``effective`` is an
  ``m2a-accepted/1`` dict or ``None``). Completeness is exactly the ``APLICABLE_M2A3`` ids.
* ``audit_classification(scopes, cases=None)``  the §7.2 scope of every case whose P facts determine it.
* ``reference_actuals(cases)``  what a Gold-faithful reducer returns; used to prove the audit accepts it and to build
  mutants of it.

Applicability: ``APLICABLE_M2A3`` (typed input and a Gold-determined expectation: M2A-3 must reach 100 %),
``DEPENDIENTE(<owner>)`` (needs a rule or authority that does not exist yet, contract §19: red declared),
``NO_MEDIBLE_SIN_PRODUCTOR`` (the decision needs structural evidence of the producer that the Gold does not carry) and
``NO_MEDIBLE(<motive>)`` (the Gold does not allow typing or determining something without guessing).

The effective is compared by SEMANTIC PROJECTION (``project_effective``): insensitive to the order of collections and to
the ids the reducer assigns; exact in dimension, slot, polarity, operator, level, encoded value, VALUE side and
comparator, unit, ORDER direction source, which requests each condition applies to, the requests, and in WHICH
conditions are carried from the base unchanged (identity of every field but ``condition_id`` and ``request_ids``) and
which new ones are the patch's own conditions placed unchanged (``from_patch``: a SET puts ``c``, it does not rebuild
it; contract §5.2). Not in the projection (the Gold does not determine them): the effective's policies, the ids the
reducer assigns, datasets and trace ids.

Use (M2A-3 iterates on DEV and measures TEST once, to freeze)::

    cases = select_partition(load_cases(), "DEV")             # synthetic; load_cases(real_dir) adds the private REAL slice
    inputs = {c["gold_id"]: c["inputs"] for c in cases if c["applicability"] == "APLICABLE_M2A3"}
    report = audit({gid: run(inputs[gid]) for gid in inputs}, cases)   # report["ok"], ["mismatches"], ["declared_red"]
    audit_classification({gid: classify(inputs[gid]["p_facts"]) for gid in ...}, cases)   # where measurable

CLI: ``build`` / ``check`` / ``report`` [--real-dir D]; the REAL section of the manifest is ids, counts and digests.
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
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audits.m2a_conversation import m2a_oracle_gold as gold  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_applicability import dependencies  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_gold import canon, digest, sha  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_typing import (  # noqa: E402
    POLARIZED, Untypeable, accepted_dict, condition_dict, ir_conditions, origin, patch_dict, request_dicts, requests_of,
    sem_key, typed_ops,
)

ORACLE_DIR = "docs/audits/m2a_conversation/oracle"
ARTIFACT = f"{ORACLE_DIR}/M2A_REDUCER_ORACLE_synthetic.jsonl"
MANIFEST = f"{ORACLE_DIR}/M2A_REDUCER_ORACLE_MANIFEST.json"
SCHEMA = "m2a-reducer-oracle/1"

APPLICABLE = "APLICABLE_M2A3"
NO_PRODUCER = "NO_MEDIBLE_SIN_PRODUCTOR"

# Effects the reducer itself must report: the declared substitutions of contract §6.5 plus the two effects of the
# classification (§7, §17). The other A.4 effects (UNIVERSE_CHANGED, TOPN_*, ORDER_DIRECTION_DEFAULTED) are echo-level
# (M2A-7): the Gold lists them, so an actual may carry them, but it may not invent any the Gold does not list.
REDUCER_EFFECTS = frozenset({"VALUE_RANGE_REPLACED", "TOPIC_REPLACED", "POLARITY_INVERTED", "VIEW_REDERIVED",
                             "CONTEXT_RESET", "PENDING_CANCELLED"})
ACTUAL_KEYS = frozenset({"disposition", "code", "effective", "effects"})
OracleError = gold.OracleError


class NoProducer(Exception):
    """The case needs structural evidence of the producer that the Gold does not carry."""

    def __init__(self, motive: str):
        super().__init__(motive)
        self.motive = motive


# ---- expectation of a case ----------------------------------------------------------------------------------------------
def _covered(written: str, key: str) -> bool:
    """Whether a written slot (or a dimension-wide CLEAR) covers the slot ``key``."""
    return key == written or (written in POLARIZED and key.startswith(written + "/")) or (
        written == "VALUE" and key.startswith("VALUE/"))


def _carry_split(base: list[dict], ops: list[dict], result: list[dict]) -> tuple[list[dict], list[dict]]:
    """Which conditions of the Gold result are carried from the base (identity) and which are new, and which base
    conditions vanished with no op on their slot (to be explained by a declared substitution). Never invented: a new
    condition outside every written slot makes the case untypeable."""
    written = [e["slot"] for e in ops if e["op"] in ("SET", "SET_ORDER", "CLEAR")]
    pool = collections.defaultdict(list)
    for b in base:
        pool[sem_key(b)].append(b)
    entries = []
    for c in result:
        touched = any(_covered(w, c["slot"]) for w in written)
        hit = pool.get(sem_key(c))
        if hit and not touched:
            entries.append({"carried": hit.pop(0)["id"]})
        elif touched:
            entries.append({"new": c})
        else:
            raise Untypeable("NEW_CONDITION_WITHOUT_OP")
    lost = [b for items in pool.values() for b in items if not any(_covered(w, b["slot"]) for w in written)]
    return entries, lost


def _topic_replaced(base: list[dict], ops: list[dict], result: list[dict]) -> bool:
    """F3/F22 (§6.5): a positive SET of TOPIC substituted a different positive TOPIC of the base."""
    sets = [e["cond"] for e in ops if e["op"] == "SET" and e["slot"] == "TOPIC/POSITIVE"]
    kept = {sem_key(c) for c in result}
    return bool(sets) and any(b["slot"] == "TOPIC/POSITIVE" and sem_key(b) not in kept
                              and all(sem_key(b) != sem_key(s) for s in sets) for b in base)


def _effects(rec: dict, base: list[dict], ops: list[dict], results: dict[str, list[dict] | None]):
    """(reducer-scope effects, all Gold effects, status). The union of what the labels stored and what the sealed
    checker derives (annex B of the contract: where a label did not store them they are derived), plus the §6.5 rule
    for TOPIC_REPLACED the checker does not derive. A stored reducer-scope effect the derivation cannot corroborate, or
    labels that disagree on the reducer-scope effects, make the effects of the case NO_MEDIBLE (never compared)."""
    chk = gold.checker()
    scoped, everything, uncorroborated = [], set(), False
    for name in ("label_A", "label_B"):
        label = rec[name]
        derived = set(chk.derive_effects(label))
        if results.get(name) is not None and _topic_replaced(base, ops, results[name]):
            derived.add("TOPIC_REPLACED")
        stored = set(label.get("effects") or [])
        uncorroborated |= bool((stored & REDUCER_EFFECTS) - derived)
        everything |= derived | stored
        scoped.append((derived | stored) & REDUCER_EFFECTS)
    if uncorroborated:
        return [], sorted(everything), "NO_MEDIBLE(EFFECTS_NOT_CORROBORATED)"
    if scoped[0] != scoped[1]:
        return [], sorted(everything), "NO_MEDIBLE(EFFECTS_AB_DISAGREE)"
    return sorted(scoped[0]), sorted(everything), "MEDIBLE"


def _canonical_value(c: dict) -> tuple:
    return (c["dimension"], c["polarity"], tuple(v for v in c["value"] if not v.startswith("excluded=")))


def p_facts(rec: dict, base: list[dict], ops: list[dict], fresh: list[dict] | None) -> dict:
    """The neutral facts of P for §7.2, only where the Gold labels determine them (None = not determined).

    ``continuation``: KEEP/CLEAR directive, ``depends_on_prior_value`` or ``refers_to_result_set`` in an op (both labels
    must agree). ``new``: P brings a TOPIC/ACTOR (polarity, value) not in the anchor (A9.1). ``frame``: universe
    evidence or two own conditions; the «verb of request» evidence is not in the Gold, so otherwise it is undetermined.
    ``admissible_alone``: P forms a SEARCH by itself (own condition and universe evidence); with no universe evidence
    it depends on the Product View (F wave), so it is undetermined unless P has no own condition at all. For a fresh
    acceptance (NEW_SEARCH) P is the resulting IR and a universe other than UNSPECIFIED is universe evidence."""
    A, B = rec["label_A"], rec["label_B"]

    def continuation(label):
        return any(o["op"] in ("KEEP", "CLEAR") or o["depends_on_prior_value"] or o["refers_to_result_set"]
                   for o in label["ops"])

    if fresh is not None:
        own, content = fresh, fresh
        universe_evidence = A["resulting_ir"]["universe"] != "UNSPECIFIED"
    else:
        own = [e["cond"] for e in ops if e["cond"] is not None]
        content = [e["cond"] for e in ops if e["op"] == "SET" and e["cond"] is not None]
        universe_evidence = any(e["op"] == "REPLACE_UNIVERSE" for e in ops)
    anchor = {_canonical_value(b) for b in base}
    new = any(c["dimension"] in ("TOPIC", "ACTOR") and _canonical_value(c) not in anchor for c in content)
    cont = continuation(A) if continuation(A) == continuation(B) else None
    return {"anchor": "SEARCH", "pending": False, "continuation": cont, "new": new,
            "frame": True if (universe_evidence or len(own) >= 2) else None,
            "admissible_alone": False if not own else (True if universe_evidence else None),
            "own_conditions": len(own), "universe_evidence": universe_evidence}


def outcomes(f: dict) -> set[str]:
    """§7.2 over an anchor without a pending, three-valued: every resolution the undetermined facts still allow."""
    def options(v):
        return (True, False) if v is None else (v,)

    out = set()
    for cont in options(f["continuation"]):
        if cont or not f["new"]:  # (a) a continuation wins; (b) no new TOPIC/ACTOR is a delta (A9.1)
            out.add("DELTA")
            continue
        for frame in options(f["frame"]):
            for alone in options(f["admissible_alone"]):
                out.add("DELTA" if not frame else ("NEW_SEARCH" if alone else "UNDEFINED"))  # (d) / (c) / not in §7.2
    return out


def _projection(entries: list[dict], pairs, placed: bool) -> dict:
    """The expected projection. ``placed``: the new conditions are the patch's own (a composed delta) and must come out
    unchanged; a fresh acceptance has no patch, so nothing is said about it (``None``)."""
    subjects = sorted(s for s, _ in pairs)
    conds = []
    for e in entries:
        if "carried" in e:
            conds.append({"carried": e["carried"], "applies_to": subjects})
        else:
            t = e["new"]
            conds.append({"new": {"dimension": t["dimension"], "slot": t["slot"], "operator": t["operator"],
                                  "polarity": t["polarity"], "level": t["level"], "value": list(t["value"]),
                                  "unit": t["unit"], "order_direction_source": t["ods"], "applies_to": subjects,
                                  "from_patch": True if placed else None}})
    return {"requests": sorted([s, k] for s, k in pairs), "logic": None, "conditions": sorted(conds, key=canon)}


def _same_conditions(a: list[dict], b: list[dict]) -> bool:
    return sorted(map(sem_key, a)) == sorted(map(sem_key, b))


def _expectation(rec: dict) -> tuple[dict, dict]:
    """(inputs, expected) of an applicable candidate; raises Untypeable / NoProducer."""
    A, B = rec["label_A"], rec["label_B"]
    gid, disposition = rec["gold_id"], rec["expected_disposition"]
    if A["followup_sha256"] != B["followup_sha256"] or A["disposition_reason"] != B["disposition_reason"]:
        raise Untypeable("LABELS_DISAGREE_ON_REASON_OR_TURN")
    if A["expected_disposition"] != disposition or B["expected_disposition"] != disposition:
        raise Untypeable("LABEL_DISPOSITION_DISAGREES_WITH_RECORD")
    if A["turn_kind"] != B["turn_kind"] or not A["m2a_scope"] == B["m2a_scope"] == rec["m2a_scope"]:
        raise Untypeable("TURN_KIND_OR_SCOPE_AB_DISAGREE")
    priors = [ir_conditions(L["prior"]["ir"]) for L in (A, B)]
    if [(c["id"], sem_key(c)) for c in priors[0]] != [(c["id"], sem_key(c)) for c in priors[1]] or requests_of(
            A["prior"]["ir"]) != requests_of(B["prior"]["ir"]):
        raise Untypeable("PRIOR_AB_DISAGREE")
    base_typed, base_dict = priors[0], accepted_dict(gid, A["prior"]["ir"])
    both_ops = [typed_ops(L) for L in (A, B)]
    if both_ops[0] != both_ops[1]:
        raise Untypeable("OPS_AB_DISAGREE")
    ops = both_ops[0]
    executed = disposition in ("EXECUTE", "NEW_SEARCH")
    results: dict[str, list[dict] | None] = {"label_A": None, "label_B": None}
    effective = effective_scope = None
    lost: list[dict] = []
    if executed:
        results = {n: ir_conditions(rec[n]["resulting_ir"]) for n in ("label_A", "label_B")}
        pairs = requests_of(A["resulting_ir"])
        if not _same_conditions(results["label_A"], results["label_B"]) or pairs != requests_of(B["resulting_ir"]):
            raise Untypeable("RESULT_AB_DISAGREE")
        if disposition == "NEW_SEARCH":
            entries, effective_scope = [{"new": c} for c in results["label_A"]], "FRESH_ACCEPTANCE"
        else:
            entries, lost = _carry_split(base_typed, ops, results["label_A"])
            effective_scope = "COMPOSED"
        effective = _projection(entries, pairs, placed=disposition != "NEW_SEARCH")
    reducer_effects, gold_effects, status = _effects(rec, base_typed, ops, results)
    if not executed and reducer_effects:  # a turn that does not execute changes nothing: no declared substitution applies
        reducer_effects, status = [], "NO_MEDIBLE(EFFECTS_ON_NON_EXECUTION)"
    for b in lost:  # a base condition vanished with no op on its slot: only a declared substitution may do it
        declared = ("VALUE_RANGE_REPLACED" if b["dimension"] == "VALUE" else
                    "POLARITY_INVERTED" if b["polarity"] == "POSITIVE" else None)
        if declared is None or declared not in (set(reducer_effects) | set(gold_effects)):
            raise Untypeable("CARRIED_CONDITION_LOST_WITHOUT_OP")
    # turn_kind says only whether P is read as a patch (its ops) or as a fresh acceptance (the resulting IR); what §7.2
    # concludes from the facts is compared with the Gold afterwards, never taken from it
    facts = p_facts(rec, base_typed, ops, results["label_A"] if A["turn_kind"] == "NEW_SEARCH" else None)
    possible = outcomes(facts)
    measurable = possible == {A["turn_kind"]}
    if not measurable and len(possible) == 1:
        raise Untypeable("GOLD_CONTRADICTS_SECTION_7_2")  # the facts determine a scope and it is not the Gold's
    if not measurable and disposition == "NEW_SEARCH":
        raise NoProducer("CLASSIFICATION_NEEDS_PRODUCER_EVIDENCE")
    rids = [r["request_id"] for r in base_dict["requests"]]
    inputs = {"base": base_dict, "patch": patch_dict(A["followup_sha256"], ops, rids), "p_facts": facts,
              "context": {"gap_seconds": A["prior"].get("gap_seconds"), "anchor_state": A["prior"]["state"]}}
    expected = {"disposition": disposition, "code": A["disposition_reason"], "scope": rec["m2a_scope"],
                "effective": effective, "effective_scope": effective_scope, "effects": reducer_effects,
                "effects_gold": gold_effects, "effects_status": status,
                "classification": {"measurable": measurable, "expected": A["turn_kind"] if measurable else None,
                                   "possible": sorted(possible)}}
    return inputs, expected


def flags(rec: dict) -> list[str]:
    """Facts about the Gold itself that a later owner must not overlook (never used to define an expectation)."""
    chk, out = gold.checker(), []
    A, B = rec["label_A"], rec["label_B"]
    if A["resulting_ir"] and B["resulting_ir"] and sorted(map(chk.key, A["resulting_ir"]["conditions"])) != sorted(
            map(chk.key, B["resulting_ir"]["conditions"])):
        out.append("RESULT_IR_AB_DISAGREE")  # the labels typed the result differently and nothing adjudicates it
    if rec["ambiguous_by"]:
        out.append("AMBIGUOUS_ADJUDICATED")
    if rec.get("invalid_persisted_state"):
        out.append("INVALID_PERSISTED_STATE_DECLARED")
    return out


def build_case(source: str, rec: dict, partition: str, decisions: dict[str, dict] | None = None) -> dict:
    gold.check_reason(rec["label_A"]["disposition_reason"])
    case = {"gold_id": rec["gold_id"], "source": source, "partition": partition,
            "disposition": rec["expected_disposition"], "reason": rec["label_A"]["disposition_reason"],
            "m2a_scope": rec["m2a_scope"], "flags": flags(rec)}
    rules, owners = dependencies(rec, decisions)
    if owners:
        case.update(applicability=f"DEPENDIENTE({owners[0]})", owners=owners, rules=rules, inputs=None, expected=None)
        return case
    try:
        inputs, expected = _expectation(rec)
    except NoProducer as error:
        case.update(applicability=NO_PRODUCER, owners=[], rules=[error.motive], inputs=None, expected=None)
        return case
    except Untypeable as error:
        case.update(applicability=f"NO_MEDIBLE({error.motive})", owners=[], rules=[error.motive], inputs=None,
                    expected=None)
        return case
    case.update(applicability=APPLICABLE, owners=[], rules=rules, inputs=inputs, expected=expected)
    return case


def load_cases(real_dir: Path | None = None) -> list[dict]:
    """One case per ``M2A_GOLD`` record, in seal order, after the seal verifies (``m2a_gold_seal verify``). The REAL
    slice needs the private ``real_dir`` (never in git). Nothing about the Gold is fixed in the oracle: the ids, the
    partition, the vocabulary and the rules that read it come from the verified seal and the sealed checker."""
    gold.verify_seal(real_dir)
    unknown = REDUCER_EFFECTS - set(gold.checker().EFFECTS)
    if unknown:
        raise OracleError(f"the oracle scopes effects the sealed checker no longer lists: {sorted(unknown)}")
    partition, decisions = gold.partition_of(gold.seal()), gold.load_decisions()
    return [build_case(source, rec, partition[rec["gold_id"]], decisions) for source, rec in gold.gold_records(real_dir)]


# ---- the semantic projection of an actual, and the audit -----------------------------------------------------------------
def _content(c: dict) -> str:
    """A condition without the two fields the reducer is free to rewrite: its id and the requests it applies to."""
    return canon({k: v for k, v in c.items() if k not in ("condition_id", "request_ids")})


def project_effective(effective: dict | None, base: dict, patch: dict | None = None) -> dict | None:
    """The comparable semantics of an effective ``m2a-accepted/1`` dict (see the module docstring). Raises ``ValueError``
    on a malformed effective: an actual that is not of that shape is a mismatch, not a crash. ``patch``: the patch the
    reducer composed (``None`` for a fresh acceptance); a new condition that is one of its conditions, unchanged, is
    ``from_patch``."""
    if effective is None:
        return None
    try:
        requests = effective["requests"]
        subject_of = {r["request_id"]: r["subject"] for r in requests}
        carried_as: dict[str, str] = {}
        for b in base["conditions"]:
            carried_as.setdefault(_content(b), b["condition_id"])
        placed = {_content(op["condition"]) for op in (patch or {"ops": []})["ops"] if op["condition"]}
        entries = []
        for c in effective["conditions"]:
            applies = sorted(subject_of[i] for i in c["request_ids"])
            if _content(c) in carried_as:  # every field but id and requests is the base's: carried, not re-resolved
                entries.append({"carried": carried_as[_content(c)], "applies_to": applies})
                continue
            prov = c["provenance"]
            entries.append({"new": {"dimension": c["dimension"], "slot": c["slot"], "operator": c["operator"],
                                    "polarity": c["polarity"], "level": c["level"], "value": list(c["value"]),
                                    "unit": prov["unit"] if c["dimension"] == "VALUE" else None,
                                    "order_direction_source": (prov["order_direction_source"]
                                                               if c["dimension"] == "ORDER" else None),
                                    "applies_to": applies,
                                    "from_patch": (_content(c) in placed) if patch is not None else None}})
        return {"requests": sorted([r["subject"], r["universe_kind"]] for r in requests),
                "logic": None if effective["logic"] is None else "PRESENT",
                "conditions": sorted(entries, key=canon)}
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError(f"malformed effective: {error!r}") from error


def effective_equal(have: dict | None, want: dict | None) -> bool:
    """The one comparison of two projections (the audit never compares them any other way)."""
    return have == want


def select_partition(cases: list[dict], partition: str) -> list[dict]:
    """The cases of one side of the seal's DEV/TEST partition: M2A-3 iterates on DEV and measures TEST once, to freeze."""
    if partition not in ("DEV", "TEST"):
        raise OracleError(f"unknown partition {partition!r}")
    return [c for c in cases if c["partition"] == partition]


def _mismatch(out: list, gid: str, field: str, expected, actual, source: str) -> None:
    item = {"gold_id": gid, "field": field}
    if source == "SYNTHETIC":
        item.update(expected=expected, actual=actual)
    else:  # REAL content never enters a report: digests only
        item.update(expected_sha256=digest(expected), actual_sha256=digest(actual))
    out.append(item)


def audit(actuals: dict, cases: list[dict] | None = None) -> dict:
    """Completeness (exactly the ``APLICABLE_M2A3`` ids), then per-case exact comparison. Pure."""
    cases = load_cases() if cases is None else cases
    by_id = {c["gold_id"]: c for c in cases}
    expected_ids = sorted(g for g, c in by_id.items() if c["applicability"] == APPLICABLE)
    missing, extra = sorted(set(expected_ids) - set(actuals)), sorted(set(actuals) - set(expected_ids))
    mismatches: list[dict] = []
    fresh_not_supplied: list[str] = []
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
        if exp["effective_scope"] == "FRESH_ACCEPTANCE" and actual["effective"] is None:
            fresh_not_supplied.append(gid)  # built by from_acceptance (M2A-1b), not by the reducer: compared if supplied
        else:
            try:
                have = project_effective(actual["effective"], case["inputs"]["base"], case["inputs"]["patch"])
            except ValueError as error:
                have = {"malformed": str(error)}
            if not effective_equal(have, exp["effective"]):
                _mismatch(mismatches, gid, "effective", exp["effective"], have, src)
        if exp["effects_status"] != "MEDIBLE":
            continue
        effects = actual["effects"]
        listed = sorted(effects) if isinstance(effects, (list, tuple, set, frozenset)) else None
        if listed is None or len(listed) != len(set(listed)):
            _mismatch(mismatches, gid, "effects", exp["effects"], effects if listed is None else listed, src)
        elif sorted(set(listed) & REDUCER_EFFECTS) != exp["effects"] or (
                set(listed) - REDUCER_EFFECTS - set(exp["effects_gold"])):
            _mismatch(mismatches, gid, "effects", exp["effects"], listed, src)
    return {"ok": not (missing or extra or mismatches), "expected": len(expected_ids), "actual": len(actuals),
            "missing": missing, "extra": extra, "mismatches": mismatches,
            "fresh_acceptance_not_supplied": fresh_not_supplied,
            "declared_red": [{"gold_id": g, "owner": c["owners"][0], "owners": c["owners"]}
                             for g, c in by_id.items() if c["applicability"].startswith("DEPENDIENTE(")],
            "no_medible": [{"gold_id": g, "motive": c["applicability"] if c["applicability"] == NO_PRODUCER
                            else c["applicability"][len("NO_MEDIBLE("):-1]}
                           for g, c in by_id.items() if c["applicability"].startswith("NO_MEDIBLE")]}


def audit_classification(scopes: dict, cases: list[dict] | None = None) -> dict:
    """The §7.2 scope (``IN`` / ``IN_NEW_SEARCH``) of every applicable case whose P facts determine the classification."""
    cases = load_cases() if cases is None else cases
    wanted = {c["gold_id"]: c for c in cases
              if c["applicability"] == APPLICABLE and c["expected"]["classification"]["measurable"]}
    bad = [g for g in sorted(set(wanted) & set(scopes)) if scopes[g] != wanted[g]["expected"]["scope"]]
    mismatches = [{"gold_id": g, "expected": wanted[g]["expected"]["scope"], "actual": scopes[g]}
                  if wanted[g]["source"] == "SYNTHETIC" else {"gold_id": g, "field": "scope"} for g in bad]
    missing, extra = sorted(set(wanted) - set(scopes)), sorted(set(scopes) - set(wanted))
    return {"ok": not (missing or extra or mismatches), "expected": len(wanted), "missing": missing, "extra": extra,
            "mismatches": mismatches}


# ---- a Gold-faithful reducer output, to prove the audit accepts it and to build mutants of it ---------------------------
def realize_effective(case: dict) -> dict | None:
    """The ``m2a-accepted/1`` dict a Gold-faithful reducer returns: the base's conditions it carries and the patch's
    conditions it places, unchanged (ids and requests are the reducer's choice); a fresh acceptance has no patch."""
    e = case["expected"]["effective"]
    if e is None:
        return None
    base, patch = case["inputs"]["base"], case["inputs"]["patch"]
    requests = request_dicts([(s, k) for s, k in e["requests"]], "t1", sha(case["gold_id"] + "|effective"))
    rid_of = {r["subject"]: r["request_id"] for r in requests}
    by_id = {c["condition_id"]: c for c in base["conditions"]}
    placed = [op["condition"] for op in (patch["ops"] if patch else []) if op["condition"]]
    conditions = []
    for i, entry in enumerate(e["conditions"]):
        if "carried" in entry:
            condition = copy.deepcopy(by_id[entry["carried"]])
            condition["request_ids"] = sorted(rid_of[s] for s in entry["applies_to"])
            conditions.append(condition)
            continue
        n = entry["new"]
        if n["from_patch"]:
            source = next(c for c in placed if c["slot"] == n["slot"] and c["value"] == n["value"])
            condition = copy.deepcopy(source)
            condition.update(condition_id=f"o{i + 1}", request_ids=sorted(rid_of[s] for s in n["applies_to"]))
            conditions.append(condition)
            continue
        t = {"dimension": n["dimension"], "operator": n["operator"], "polarity": n["polarity"], "level": n["level"],
             "value": n["value"], "unit": n["unit"], "ods": n["order_direction_source"], "slot": n["slot"],
             "unit_source": "STATED" if n["unit"] else None}
        conditions.append(condition_dict(t, f"o{i + 1}", [rid_of[s] for s in n["applies_to"]],
                                         origin("t1", sha(f"{case['gold_id']}|{i}"), i)))
    return {"schema": "m2a-accepted/1", "requests": requests, "conditions": conditions, "logic": None, "policies": [],
            "base_trace_id": "t0", "root_trace_id": "t0"}


def reference_actuals(cases: list[dict]) -> dict:
    return {c["gold_id"]: {"disposition": c["expected"]["disposition"], "code": c["expected"]["code"],
                           "effective": realize_effective(c), "effects": list(c["expected"]["effects"])}
            for c in cases if c["applicability"] == APPLICABLE}


# ---- artifact, manifest, CLI -----------------------------------------------------------------------------------------------
def _summary(cases: list[dict]) -> dict:
    applicable = [c for c in cases if c["applicability"] == APPLICABLE]
    count = collections.Counter
    return {"cases": len(cases), "by_applicability": dict(sorted(count(c["applicability"] for c in cases).items())),
            "dependent_by_any_owner": dict(sorted(count(o for c in cases for o in c["owners"]).items())),
            "applicable_by_partition": dict(sorted(count(c["partition"] for c in applicable).items())),
            "applicable_by_disposition": dict(sorted(count(c["expected"]["disposition"] for c in applicable).items())),
            "all_by_partition": dict(sorted(count(c["partition"] for c in cases).items())),
            "classification_measurable": sum(1 for c in applicable if c["expected"]["classification"]["measurable"]),
            "effects_not_measurable": sum(1 for c in applicable if c["expected"]["effects_status"] != "MEDIBLE")}


def _real_section(cases: list[dict]) -> dict:
    ids = collections.defaultdict(list)
    for c in cases:
        ids[c["applicability"]].append(c["gold_id"])
    return {"summary": _summary(cases), "ids_by_applicability": {k: sorted(v) for k, v in sorted(ids.items())},
            "case_sha256": {c["gold_id"]: digest(c) for c in cases}}


def build_artifacts(real_dir: Path | None, previous: dict | None) -> tuple[str, dict]:
    synthetic = load_cases()
    text = "".join(json.dumps(c, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n" for c in synthetic)
    if real_dir is not None:
        real = _real_section([c for c in load_cases(real_dir) if c["source"] == "REAL"])
    elif previous is not None:
        real = previous["real"]
    else:
        raise SystemExit("build needs --real-dir the first time (the REAL section is private)")
    decisions = gold.load_decisions()
    manifest = {"schema": SCHEMA, "gold_seal_sha256": gold.seal()["seal_sha256"], "seal_verified": True,
                "applicability_decisions": {"path": f"{ORACLE_DIR}/APPLICABILITY_DECISIONS.json",
                                            "sha256": hashlib.sha256(gold.DECISIONS_PATH.read_bytes()).hexdigest(),
                                            "ids": sorted(decisions)},
                "artifact": {"path": ARTIFACT, "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                             "records": len(synthetic)},
                "synthetic": _summary(synthetic), "real": real,
                "policy": "texto REAL fuera de git; aquí sólo ids, conteos y digests"}
    return text, manifest


def real_manifest_problems(manifest: dict) -> list[str]:
    """What can be checked about the REAL section without the private directory: ids and coverage."""
    ids = sorted(i for group in manifest["real"]["ids_by_applicability"].values() for i in group)
    problems = []
    if ids != sorted(gold.seal()["slices"]["M2A_GOLD"]["REAL"]["ids"]):
        problems.append("the REAL ids of the manifest differ from the seal")
    if sorted(manifest["real"]["case_sha256"]) != ids:
        problems.append("the REAL digests do not cover the REAL ids")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("cmd", choices=("build", "check", "report"))
    ap.add_argument("--real-dir", type=Path)
    a = ap.parse_args(argv)
    artifact, manifest_path = ROOT / ARTIFACT, ROOT / MANIFEST
    previous = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else None
    text, manifest = build_artifacts(a.real_dir, previous)
    if a.cmd == "report":
        print(json.dumps({"synthetic": manifest["synthetic"], "real": manifest["real"]["summary"]},
                         ensure_ascii=False, indent=1, sort_keys=True))
        return 0
    if a.cmd == "build":
        artifact.parent.mkdir(parents=True, exist_ok=True)
        artifact.write_text(text, encoding="utf-8")
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=1, sort_keys=True) + "\n",
                                 encoding="utf-8")
        print("oracle built", manifest["artifact"]["sha256"])
        return 0
    problems = []
    if not artifact.exists() or artifact.read_text(encoding="utf-8") != text:
        problems.append("the synthetic artifact differs from the recomputation")
    if previous != manifest:
        problems.append("the manifest differs from the recomputation")
    problems += real_manifest_problems(previous or manifest)
    for p in problems:
        print("ORACLE DRIFT:", p)
    if not problems:
        print("oracle OK", manifest["artifact"]["sha256"], "REAL",
              "recomputed" if a.real_dir else "carried (private dir absent)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
