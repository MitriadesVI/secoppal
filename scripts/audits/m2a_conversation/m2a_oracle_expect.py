"""M2A-8a0 oracle, part 4: the expectation of every case, derived from the sealed Gold.

``load_cases`` verifies the seal (``m2a_gold_seal verify``), the user's recorded decisions and the effects vocabulary,
then builds one case per ``M2A_GOLD`` record: its applicability, its typed inputs (``base``, ``patch``, ``p_facts``,
``context``: the SAME shape for every case, so the shape never says what §7.2 concludes) and its expectation
(``disposition``, ``code``, ``scope``, the effective as a semantic projection, the reducer-scope effects and the
classification). ``inputs`` and ``expected`` exist only for ``APLICABLE_M2A3``; a red case is declared with its owner,
never given a guessed expectation.

Everything expected here comes from the Gold: GOLD decides MEANING. The mechanical invariants of the transition
(identity of what is carried, provenance of what is placed, how a redirected universe is rebuilt) are the CONTRACT's and
are checked by the independent ``check_transition``. Where the Gold does not determine something it is ``NO_MEDIBLE`` or
a named limited guarantee (``LIMITED_GUARANTEES``), never a convenient choice.
"""

from __future__ import annotations

import collections
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audits.m2a_conversation import m2a_oracle_gold as gold  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_applicability import dependencies, surface_form_problem  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_gold import OracleError, canon  # noqa: E402
from scripts.audits.m2a_conversation.m2a_oracle_typing import (  # noqa: E402
    POLARIZED, Untypeable, accepted_dict, fresh_ops, ir_conditions, money_amount, patch_dict, requests_of, sem_key,
    typed_ops,
)

APPLICABLE = "APLICABLE_M2A3"
NO_PRODUCER = "NO_MEDIBLE_SIN_PRODUCTOR"

# Effects the reducer itself must report: the declared substitutions of contract §6.5 and the two effects of the
# classification (§7, §17). The echo-level effects (§11, M2A-7) are listed by the Gold, so an actual may carry them, but
# it may not invent one the Gold does not list. EVERY effect of the sealed vocabulary must be one or the other: a new
# one after a re-seal is refused until someone decides which (``check_effects_vocabulary``), never ignored in silence.
REDUCER_EFFECTS = frozenset({"VALUE_RANGE_REPLACED", "TOPIC_REPLACED", "POLARITY_INVERTED", "VIEW_REDERIVED",
                             "CONTEXT_RESET", "PENDING_CANCELLED"})
ECHO_EFFECTS = frozenset({"UNIVERSE_CHANGED", "TOPN_CARRIED", "TOPN_CAPPED", "ORDER_DIRECTION_DEFAULTED"})
CLASSIFICATION_EFFECTS = frozenset({"CONTEXT_RESET", "PENDING_CANCELLED", "VIEW_REDERIVED"})


class NoProducer(Exception):
    """The case needs structural evidence of the producer that the Gold does not carry."""

    def __init__(self, motive: str):
        super().__init__(motive)
        self.motive = motive


def check_effects_vocabulary() -> None:
    sealed = set(gold.checker().EFFECTS)
    unknown = sealed - REDUCER_EFFECTS - ECHO_EFFECTS
    gone = (REDUCER_EFFECTS | ECHO_EFFECTS) - sealed
    if unknown or gone:
        raise OracleError(f"the effects of the sealed vocabulary and the oracle's scope differ: new {sorted(unknown)}, "
                          f"gone {sorted(gone)}: decide whether each is the reducer's or the echo's (REDUCER_EFFECTS / "
                          f"ECHO_EFFECTS) and give it a derivation")


# ---- what the patch writes, carries and loses ---------------------------------------------------------------------------
def _covered(written: str, key: str) -> bool:
    """Whether a written slot (or a dimension-wide CLEAR) covers the slot ``key``."""
    return key == written or (written in POLARIZED and key.startswith(written + "/")) or (
        written == "VALUE" and key.startswith("VALUE/"))


def _carry_split(base: list[dict], ops: list[dict], result: list[dict]) -> tuple[list[dict], list[dict]]:
    """Which conditions of the Gold result are carried from the base (identity) and which are new (the patch's own), and
    which base conditions vanished with no op on their slot (to be explained by a declared substitution). Never
    invented: a new condition outside every written slot makes the case untypeable."""
    written = [e["slot"] for e in ops if e["op"] in ("SET", "SET_ORDER", "CLEAR")]
    pool = collections.defaultdict(list)
    for b in base:
        pool[sem_key(b)].append(b)
    entries = []
    for c in result:
        touched = any(_covered(w, c["slot"]) for w in written)
        hit = pool.get(sem_key(c))
        if hit and not touched:
            entries.append({"carried": hit.pop(0)["id"], "carried_typed": c})
        elif touched:
            entries.append({"new": c})
        else:
            raise Untypeable("NEW_CONDITION_WITHOUT_OP")
    lost = [b for items in pool.values() for b in items if not any(_covered(w, b["slot"]) for w in written)]
    return entries, lost


def _canonical_value(c: dict) -> tuple:
    return (c["dimension"], c["polarity"], tuple(v for v in c["value"] if not v.startswith("excluded=")))


def _topic_replaced(base: list[dict], ops: list[dict], result: list[dict]) -> bool:
    """F3/F22 (§6.5): a positive SET of TOPIC substituted a different positive TOPIC of the base."""
    sets = [e["cond"] for e in ops if e["op"] == "SET" and e["slot"] == "TOPIC/POSITIVE"]
    kept = {sem_key(c) for c in result}
    return bool(sets) and any(b["slot"] == "TOPIC/POSITIVE" and sem_key(b) not in kept
                              and all(sem_key(b) != sem_key(s) for s in sets) for b in base)


def _declared_substitutions(base: list[dict], ops: list[dict], lost: list[dict]) -> set[str]:
    """The §6.5 substitutions that explain a base condition vanishing with no op on its slot, each checked against the
    typed values (never attributed to the nearest effect): A8.1 for a VALUE bound, F16 for a polarity inversion."""
    out = set()
    for b in lost:
        if b["dimension"] == "VALUE":  # A8.1: the patch's new bound on the opposite side leaves the interval empty
            side = "VALUE/upper" if b["slot"] == "VALUE/lower" else "VALUE/lower"
            new = [e["cond"] for e in ops if e["op"] == "SET" and e["slot"] == side]
            if not new:
                raise Untypeable("CARRIED_CONDITION_LOST_WITHOUT_OP")
            lo, hi = (money_amount(new[0]["value"]), money_amount(b["value"])) if side == "VALUE/lower" else (
                money_amount(b["value"]), money_amount(new[0]["value"]))
            if lo < hi:
                raise Untypeable("A8_1_BOUND_LOST_BUT_INTERVAL_NOT_EMPTY")
            out.add("VALUE_RANGE_REPLACED")
        elif b["dimension"] in POLARIZED and b["polarity"] == "POSITIVE":  # F16: the single positive, same value
            negative = b["slot"].replace("/POSITIVE", "/NEGATIVE")
            same = [e for e in ops if e["op"] == "SET" and e["slot"] == negative
                    and _canonical_value(e["cond"])[2] == _canonical_value(b)[2]]
            if not same or sum(1 for x in base if x["slot"] == b["slot"]) != 1:
                raise Untypeable("CARRIED_CONDITION_LOST_WITHOUT_OP")
            out.add("POLARITY_INVERTED")
        else:
            raise Untypeable("CARRIED_CONDITION_LOST_WITHOUT_OP")
    return out


def _effects(rec: dict, base: list[dict], ops: list[dict], results: dict[str, list[dict] | None], lost: list[dict],
             executed: bool):
    """(reducer-scope effects, all Gold effects, status).

    The reducer-scope effects are derived HERE from the typed values: A8.1 (VALUE_RANGE_REPLACED: the new bound of the
    patch against a carried opposite bound that leaves the interval empty or degenerate), F16 (POLARITY_INVERTED), F3/F22
    (TOPIC_REPLACED) and, for the classification, what the sealed checker derives. They are CROSS-CHECKED against what
    the labels stored and the checker derives (annex B of the contract): a stored reducer-scope effect nobody can
    corroborate makes the effects of the case NO_MEDIBLE. An explicit CLEAR(VALUE) followed by a SET that does not
    replace a side is an obligation of ECHO (§11: VALUE_RANGE_REPLACED, M2A-7), not an effect of the reducer: the Gold may
    list it and the reducer is not required to."""
    chk = gold.checker()
    substitutions = set()
    if executed:
        substitutions |= _declared_substitutions(base, ops, lost)
        if results.get("label_A") is not None and _topic_replaced(base, ops, results["label_A"]):
            substitutions.add("TOPIC_REPLACED")
    explicit_clear_value = any(e["op"] == "CLEAR" and (e["slot"] == "VALUE" or e["slot"].startswith("VALUE/")) for e in ops)
    everything, classification, uncorroborated = set(), None, False
    for name in ("label_A", "label_B"):
        label = rec[name]
        derived, stored = set(chk.derive_effects(label)), set(label.get("effects") or [])
        everything |= derived | stored
        listed = (derived | stored) & REDUCER_EFFECTS
        here = derived & CLASSIFICATION_EFFECTS
        if classification is not None and classification != here:
            return [], sorted(everything), "NO_MEDIBLE(EFFECTS_AB_DISAGREE)"
        classification = here
        if explicit_clear_value and "VALUE_RANGE_REPLACED" not in substitutions:
            listed.discard("VALUE_RANGE_REPLACED")  # echo-level (§11)
        if not executed and listed - here:
            return [], sorted(everything), "NO_MEDIBLE(EFFECTS_ON_NON_EXECUTION)"
        uncorroborated |= bool(listed - substitutions - here)
    wanted = substitutions | (classification or set())
    unknown = (everything | wanted) - REDUCER_EFFECTS - ECHO_EFFECTS
    if unknown:
        raise OracleError(f"effects {sorted(unknown)} have no scope decision (REDUCER_EFFECTS / ECHO_EFFECTS)")
    everything |= wanted
    if uncorroborated:
        return [], sorted(everything), "NO_MEDIBLE(EFFECTS_NOT_CORROBORATED)"
    return sorted(wanted), sorted(everything), "MEDIBLE"


# ---- the facts of P and §7.2 --------------------------------------------------------------------------------------------
def p_facts(rec: dict, base: list[dict], ops: list[dict]) -> dict:
    """The neutral facts of P for §7.2, only where the Gold labels determine them (None = not determined), read from the
    typed ops of P (a fresh acceptance is read as the patch it would be, so every case is read the same way).

    ``continuation``: KEEP/CLEAR directive, ``depends_on_prior_value`` or ``refers_to_result_set`` in an op of either
    label, and both labels must agree; the absence of every marker in both labels is read as «no evidence» (the labels
    mark it, the host never detects it with a lexicon: a closed-world reading, declared in ``LIMITED_GUARANTEES``).
    ``new``: P brings a TOPIC/ACTOR (polarity, value) not in the anchor (A9.1). ``frame``: universe evidence or two own
    conditions; the «verb of request» evidence is not in the Gold, so otherwise it is undetermined.
    ``admissible_alone``: P forms a SEARCH by itself (own condition and universe evidence); with no universe evidence it
    depends on the Product View (F wave), so it is undetermined unless P has no own condition at all."""
    A, B = rec["label_A"], rec["label_B"]

    def continuation(label):
        return any(o["op"] in ("KEEP", "CLEAR") or o["depends_on_prior_value"] or o["refers_to_result_set"]
                   for o in label["ops"])

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


# ---- the expected effective ---------------------------------------------------------------------------------------------
def _check_universe(base_dict: dict, ops: list[dict], pairs: list[tuple[str, str]], fresh: bool) -> None:
    """The Gold's result universe must be what the ops say (REPLACE_UNIVERSE names the new one, §5.2; nothing else changes
    it): a Gold whose universe moves without an op, or an op that names another universe, is not measurable."""
    if fresh:
        return
    redirects = [e for e in ops if e["op"] == "REPLACE_UNIVERSE"]
    if redirects:
        if len(redirects) != 1 or pairs != [tuple(redirects[0]["universe"])]:
            raise Untypeable("UNIVERSE_TARGET_DISAGREES_WITH_RESULT")
    elif pairs != [(r["subject"], r["universe_kind"]) for r in base_dict["requests"]]:
        raise Untypeable("UNIVERSE_CHANGED_WITHOUT_OP")


def _projection(entries: list[dict], pairs) -> dict:
    """The expected semantic projection: the Gold decides MEANING, so only what the adjudicated result says: the requests'
    (subject, universe_kind), the conditions' dimension, slot, polarity, operator, level and encoded value, the unit of a
    VALUE and the direction source of an ORDER, and no logic. Identity and provenance (what is carried, what is placed,
    ids, origins, datasets) are the contract's, checked by ``check_transition``."""
    conds = []
    for e in entries:
        t = e["new"] if "new" in e else e["carried_typed"]
        conds.append({"dimension": t["dimension"], "slot": t["slot"], "operator": t["operator"],
                      "polarity": t["polarity"], "level": t["level"], "value": list(t["value"]),
                      "unit": t["unit"], "order_direction_source": t["ods"]})
    return {"requests": sorted([s, k] for s, k in pairs), "logic": None, "conditions": sorted(conds, key=canon)}


def _same_conditions(a: list[dict], b: list[dict]) -> bool:
    return sorted(map(sem_key, a)) == sorted(map(sem_key, b))


def _agree(A: dict, B: dict, *paths: tuple[str, ...], motive: str) -> None:
    for path in paths:
        x, y = A, B
        for key in path:
            x, y = x.get(key), y.get(key)
        if x != y:
            raise Untypeable(motive)


def _expectation(rec: dict) -> tuple[dict, dict]:
    """(inputs, expected) of an applicable candidate; raises Untypeable / NoProducer."""
    A, B = rec["label_A"], rec["label_B"]
    gid, disposition = rec["gold_id"], rec["expected_disposition"]
    _agree(A, B, ("followup_sha256",), ("disposition_reason",), ("turn_kind",), ("m2a_scope",),
           motive="LABELS_DISAGREE_ON_REASON_OR_TURN")
    if A["m2a_scope"] != rec["m2a_scope"] or A["expected_disposition"] != disposition or (
            B["expected_disposition"] != disposition):
        raise Untypeable("LABEL_DISPOSITION_DISAGREES_WITH_RECORD")
    _agree(A["prior"], B["prior"], ("gap_seconds",), ("state",), ("owner",), motive="PRIOR_AB_DISAGREE")
    if surface_form_problem(rec, gold.checker().SURFACE_FORM_GAPS):
        raise Untypeable("SURFACE_FORM_AB_DISAGREE")
    priors = [ir_conditions(L["prior"]["ir"]) for L in (A, B)]
    if [(c["id"], sem_key(c)) for c in priors[0]] != [(c["id"], sem_key(c)) for c in priors[1]] or requests_of(
            A["prior"]["ir"]) != requests_of(B["prior"]["ir"]):
        raise Untypeable("PRIOR_AB_DISAGREE")
    base_typed, base_dict = priors[0], accepted_dict(gid, A["prior"]["ir"])
    executed = disposition in ("EXECUTE", "NEW_SEARCH")
    fresh = A["turn_kind"] == "NEW_SEARCH"
    both_ops = [typed_ops(L) for L in (A, B)]
    if both_ops[0] != both_ops[1]:
        raise Untypeable("OPS_AB_DISAGREE")
    results: dict[str, list[dict] | None] = {"label_A": None, "label_B": None}
    if executed:
        results = {n: ir_conditions(rec[n]["resulting_ir"]) for n in ("label_A", "label_B")}
        pairs = requests_of(A["resulting_ir"])
        if not _same_conditions(results["label_A"], results["label_B"]) or pairs != requests_of(B["resulting_ir"]):
            raise Untypeable("RESULT_AB_DISAGREE")
    if fresh:
        if both_ops[0] or not executed:
            raise Untypeable("NEW_SEARCH_NOT_A_FRESH_ACCEPTANCE")
        ops = fresh_ops(A["resulting_ir"])
    else:
        ops = both_ops[0]
    effective, effective_scope, lost = None, None, []
    if executed:
        if fresh:
            entries, effective_scope = [{"new": c} for c in results["label_A"]], "FRESH_ACCEPTANCE"
        else:
            entries, lost = _carry_split(base_typed, ops, results["label_A"])
            effective_scope = "COMPOSED"
        _check_universe(base_dict, ops, pairs, fresh)
        effective = _projection(entries, pairs)
    reducer_effects, gold_effects, status = _effects(rec, base_typed, ops, results, lost, executed)
    facts = p_facts(rec, base_typed, ops)
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
                                   "possible": sorted(possible),
                                   "motive": None if measurable else "NO_MEDIBLE(FACTS_OF_P_UNDETERMINED)"}}
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
    if (set(A.get("gap_codes") or []) | set(B.get("gap_codes") or [])) & set(chk.SURFACE_FORM_GAPS):
        out.append("SURFACE_FORM_READ_BY_E_VALUE")  # measurable by compose: the operators come from the Gold (§19)
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
    """One case per ``M2A_GOLD`` record, in seal order, after the seal verifies (``m2a_gold_seal verify``) and the
    user's decisions are checked against their records. The REAL slice needs the private ``real_dir`` (never in git).
    Nothing about the Gold is fixed in the oracle: the ids, the partition, the vocabulary and the rules that read it come
    from the verified seal and the sealed checker."""
    gold.verify_seal(real_dir)
    check_effects_vocabulary()
    partition, decisions = gold.partition_of(gold.seal()), gold.load_decisions()
    return [build_case(source, rec, partition[rec["gold_id"]], decisions) for source, rec in gold.gold_records(real_dir)]


def select_partition(cases: list[dict], partition: str) -> list[dict]:
    """The cases of one side of the seal's DEV/TEST partition: M2A-3 iterates on DEV and measures TEST once, to freeze."""
    if partition not in ("DEV", "TEST"):
        raise OracleError(f"unknown partition {partition!r}")
    return [c for c in cases if c["partition"] == partition]


def dumps(value) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
