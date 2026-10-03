"""M2A-8a0 oracle B: ``check_transition(base, patch, effective)``, the invariants of a transition, from the CONTRACT.

GOLD decides meaning; the CONTRACT decides mechanical invariants; the oracle verifies both with two independent pure
functions over dicts. This file proves the second one:

* it depends on the standard library only, and judges a transition without any case of the Gold: it is fed with GENERATED
  bases (every host field filled, dual anchors, views), which is how M2A-3 will call it from its own properties;
* a faithful dict-level application written from the contract (``probe_apply``, NOT the reducer: this file never imports
  or calls M2a) is lawful over the Gold's transitions, over generated ones and over hand-built ones;
* every rule of ``TRANSITION_RULES`` is killed by each of its named mutants (the table the manifest declares: rule ->
  contract clause -> mutant), and each mutant manifests on at least one input;
* the attacks that the independent review found against the first design (a stale dataset or universe origin after
  REPLACE_UNIVERSE, re-originating or re-dataset-ing an untouched request, carried conditions rebuilt with defaults, a
  dangling policy) die, and so do the four reducer bugs the Gold alone cannot discriminate;
* the Gold oracle and this one are independent: what one lets through the other catches.
"""

from __future__ import annotations

import ast
import copy
import random
import re
import urllib.parse
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from scripts.audits.m2a_conversation import m2a_contract_text as contract_text
from scripts.audits.m2a_conversation import m2a_oracle_gold as gold
from scripts.audits.m2a_conversation import m2a_oracle_transition as transition
from scripts.audits.m2a_conversation import m2a_oracle_typing as typing_
from scripts.audits.m2a_conversation import m2a_reducer_oracle as oracle

ROOT = Path(__file__).resolve().parents[1]
FAST = settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
CASES = oracle.load_cases()
APPLICABLE = [c for c in CASES if c["applicability"] == oracle.APPLICABLE]
COMPOSED = [c for c in APPLICABLE if c["expected"]["effective_scope"] == "COMPOSED"]
REFERENCE = oracle.reference_actuals(CASES)
check = transition.check_transition


def host_dataset(subject: str) -> str:
    """A subject-to-dataset table that is NOT the oracle's placeholder: the host's own, as M2A-3 would pass it."""
    return f"host.table.{subject}"


# ====================================================================================================================
# INDEPENDENCE OF THE FUNCTION
# ====================================================================================================================
def test_the_transition_oracle_depends_on_the_standard_library_only():
    tree = ast.parse((ROOT / "scripts/audits/m2a_conversation/m2a_oracle_transition.py").read_text(encoding="utf-8"))
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            imported.add((node.module or "").split(".")[0])
    assert imported <= {"__future__", "collections"}


def test_the_function_is_pure_it_never_mutates_its_arguments():
    for case in COMPOSED[:20]:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        effective = REFERENCE[case["gold_id"]]["effective"]
        snapshot = copy.deepcopy((base, patch, effective))
        check(base, patch, effective)
        assert (base, patch, effective) == snapshot


def test_none_is_not_a_transition_the_gold_oracle_decides_whether_something_had_to_be_produced():
    for case in COMPOSED[:20]:
        assert check(case["inputs"]["base"], case["inputs"]["patch"], None) == []


# ====================================================================================================================
# A FAITHFUL APPLICATION WRITTEN FROM THE CONTRACT, AND GENERATED TRANSITIONS
# ====================================================================================================================
def covered(written: str, key: str) -> bool:
    return key == written or (written in typing_.POLARIZED and key.startswith(written + "/")) or (
        written == "VALUE" and key.startswith("VALUE/"))


def value_of(condition: dict, name: str) -> str:
    return next(i.partition("=")[2] for i in condition["value"] if i.startswith(name + "="))


def probe_apply(base: dict, patch: dict, dataset_of=typing_.dataset_of) -> dict:
    """A dict-level application of the patch as the contract says (§5.2, §6.1-§6.3, §6.5, §14); NOT the reducer."""
    ops = patch["ops"]
    requests = copy.deepcopy(base["requests"])
    base_rids = sorted(r["request_id"] for r in requests)
    redirect = next((op for op in ops if op["op"] == "REPLACE_UNIVERSE"), None)
    if redirect is not None:
        target = redirect["universe"]
        named = [r for r in requests if r["subject"] == target["subject"]]
        touched = copy.deepcopy((named or requests)[0])
        touched.update(subject=target["subject"], universe_kind=target["universe_kind"],
                       universe_origin=copy.deepcopy(redirect["origin"]),
                       dataset=named[0]["dataset"] if named else dataset_of(target["subject"]))
        requests = [touched]
    rids = sorted(r["request_id"] for r in requests)
    conditions = copy.deepcopy(base["conditions"])
    carried = {c["condition_id"] for c in conditions}
    for op in ops:
        if op["op"] == "CLEAR":
            conditions = [c for c in conditions if not covered(op["slot"], c["slot"])]
    for n, op in enumerate(op for op in ops if op["op"] in ("SET", "SET_ORDER")):
        new = copy.deepcopy(op["condition"])
        new.update(condition_id=f"new-{n}", request_ids=rids)
        slot = new["slot"]
        conditions = [c for c in conditions if c["slot"] != slot]
        if slot.endswith("/NEGATIVE") and slot.split("/")[0] in typing_.POLARIZED:  # F16: the STANDING (carried) positive
            positive = slot.replace("/NEGATIVE", "/POSITIVE")
            single = [c for c in conditions if c["slot"] == positive and c["condition_id"] in carried]
            if len(single) == 1 and single[0]["value"][1:-1] == new["value"][1:-1]:
                conditions = [c for c in conditions if c["slot"] != positive]
        if slot.startswith("VALUE/"):  # A8.1
            other = "VALUE/upper" if slot == "VALUE/lower" else "VALUE/lower"
            opposite = [c for c in conditions if c["slot"] == other and c["condition_id"] in carried]
            if opposite:
                lo, hi = (int(value_of(new, "amount_cop")), int(value_of(opposite[0], "amount_cop"))) if slot == "VALUE/lower" \
                    else (int(value_of(opposite[0], "amount_cop")), int(value_of(new, "amount_cop")))
                if lo >= hi:
                    conditions = [c for c in conditions if c is not opposite[0]]
        conditions.append(new)
    for c in conditions:
        if c["condition_id"] in carried and set(rids) != set(base_rids):
            c["request_ids"] = rids
    return {"schema": "m2a-accepted/1", "requests": requests, "conditions": conditions, "logic": base["logic"],
            "policies": [], "base_trace_id": "b0", "root_trace_id": base["root_trace_id"]}


def enrich(base: dict, patch: dict, seed: int, smmlv: bool = False) -> tuple[dict, dict]:
    """Fill EVERY host field with non-default content: what the Gold's bases lack (policies, receipt_rules, authority,
    provenance.{source_amount, year, year_source, buyer_scope, inherited_from}, origins, a view): the generated bases."""
    rng = random.Random(seed)
    base, patch = copy.deepcopy(base), copy.deepcopy(patch)

    def fill(condition):
        condition.update(authority=f"auth-{rng.randint(0, 99)}@{rng.randint(1, 3)}",
                         receipt_rules=sorted(rng.sample(["unit.sibling", "year.inherited", "topn.capped", "reanchored"],
                                                         rng.randint(1, 3))),
                         policies=sorted(rng.sample(["pol-a", "pol-b", "pol-c"], rng.randint(1, 2))))
        condition["provenance"].update(source_amount=f"{rng.randint(1, 999)}.5", year=rng.randint(2024, 2026),
                                       year_source=rng.choice(["STATED", "PREVIOUS_ACCEPTED_TIME"]),
                                       buyer_scope=rng.choice(["05001", "NATIONAL", "08"]),
                                       inherited_from=f"t-{rng.randint(0, 9)}")
        if smmlv and condition["dimension"] == "VALUE" and rng.random() < 0.3:
            condition["provenance"]["unit"] = "SMMLV"  # the unit class decides whether A8.1 can compare two bounds (§15.2)
    for c in base["conditions"]:
        fill(c)
        c["origin"] = {"kind": "SPAN", "trace_id": f"t{rng.randint(0, 4)}", "raw_sha256": f"{rng.getrandbits(256):064x}",
                       "span": [rng.randint(0, 5), rng.randint(6, 20)], "event_id": None}
    for r in base["requests"]:
        r.update(scope_id=base["requests"][0]["scope_id"], view=rng.choice([None, {"decision": "PV1", "n": rng.randint(0, 9)}]))
        r["universe_origin"] = {"kind": "SPAN", "trace_id": f"u{rng.randint(0, 4)}", "raw_sha256": f"{rng.getrandbits(256):064x}",
                                "span": [1, 9], "event_id": None}
    for op in patch["ops"]:
        if op["condition"]:
            fill(op["condition"])
            op["condition"]["origin"] = copy.deepcopy(op["origin"])
    return base, patch


def make_dual(base: dict, patch: dict) -> tuple[dict, dict]:
    """The same transition over a dual anchor (processes + contracts sharing the scope)."""
    base, patch = copy.deepcopy(base), copy.deepcopy(patch)
    first = base["requests"][0]
    other = copy.deepcopy(first)
    other.update(request_id="r2", subject="processes" if first["subject"] == "contracts" else "contracts")
    other["dataset"] = typing_.dataset_of(other["subject"])
    if len(base["requests"]) == 2:
        return base, patch
    base["requests"].append(other)
    for c in base["conditions"]:
        c["request_ids"] = ["r1", "r2"]
    for op in patch["ops"]:
        if op["condition"]:
            op["condition"]["request_ids"] = ["r1", "r2"]
    return base, patch


GOLD_TRANSITIONS = [(c["gold_id"], c["inputs"]["base"], c["inputs"]["patch"]) for c in COMPOSED]


def test_the_faithful_application_is_lawful_over_every_transition_of_the_gold():
    assert GOLD_TRANSITIONS
    for gid, base, patch in GOLD_TRANSITIONS:
        assert check(base, patch, probe_apply(base, patch)) == [], gid


def test_the_realised_gold_reference_is_lawful_and_agrees_with_the_faithful_application():
    for case in COMPOSED:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        reference = REFERENCE[case["gold_id"]]["effective"]
        assert check(base, patch, reference) == [], case["gold_id"]
        meaning = oracle.project_effective
        assert meaning(reference) == meaning(probe_apply(base, patch)), case["gold_id"]  # two derivations, one meaning


@FAST
@given(index=st.integers(min_value=0, max_value=len(GOLD_TRANSITIONS) - 1), seed=st.integers(min_value=0, max_value=10**6),
       dual=st.booleans())
def test_the_faithful_application_is_lawful_over_rich_and_dual_bases_built_from_the_gold(index, seed, dual):
    _, base, patch = GOLD_TRANSITIONS[index]
    if dual:
        base, patch = make_dual(base, patch)
    base, patch = enrich(base, patch, seed)
    assert check(base, patch, probe_apply(base, patch)) == []


VOCABULARY = [
    ("TOPIC", "contains", "POSITIVE", None, None, w) for w in ("aseo", "obras", "vigilancia", "dotación")
] + [
    ("TOPIC", "contains", "NEGATIVE", None, None, w) for w in ("aseo", "obras", "papelería")
] + [
    ("BUYER", "is", "POSITIVE", None, None, w) for w in ("Alcaldía de Pasto", "SENA", "Gobernación del Tolima")
] + [
    ("GEOGRAPHY", "is", "POSITIVE", "DEPARTMENT", None, w) for w in ("Boyacá", "Cesar")
] + [
    ("TIME", "during_year", "POSITIVE", None, None, str(y)) for y in (2023, 2024, 2025)
] + [
    ("VALUE", op, "POSITIVE", None, "MILLONES", f"{n} millones")
    for op in ("greater_or_equal", "greater_than", "less_than", "less_or_equal") for n in (50, 100, 200, 400)
]


def random_transition(seed: int) -> tuple[dict, dict]:
    """A base and a patch the contract does not refuse, generated from a seed: random slots, values, universes."""
    rng = random.Random(seed)
    subject = rng.choice(["contracts", "processes"])
    pairs = [(subject, "records")]
    rids = ["r1"]
    if rng.random() < 0.3:
        pairs = [("processes", "records"), ("contracts", "records")]
        rids = ["r1", "r2"]
    raw = f"{seed:064x}"
    requests = typing_.request_dicts(pairs, "t0", raw)
    taken, base_conditions = {}, []
    for n, entry in enumerate(rng.sample(VOCABULARY, rng.randint(2, 6)), 1):
        dimension, operator, polarity, level, unit, hint = entry
        t = typing_.tcond(dimension, operator, polarity, level, unit, hint)
        if t["slot"] in taken and not (t["slot"] == "TOPIC/POSITIVE" and taken[t["slot"]] == 1):  # at most two TOPIC positives
            continue
        taken[t["slot"]] = taken.get(t["slot"], 0) + 1
        base_conditions.append(typing_.condition_dict(t, f"c{n}", rids, typing_.origin("t0", raw, n)))
    if rng.random() < 0.4:
        order = typing_.tcond("ORDER", "value_desc", "POSITIVE", None, None, None, ods="STATED")
        base_conditions.append(typing_.condition_dict(order, "c-order", rids, typing_.origin("t0", raw, 40)))
        if rng.random() < 0.6:
            cardinality = typing_.tcond("CARDINALITY", "top_n", "POSITIVE", None, None, None, n_requested=10)
            base_conditions.append(typing_.condition_dict(cardinality, "c-card", rids, typing_.origin("t0", raw, 41)))
    base = {"schema": "m2a-accepted/1", "requests": requests, "conditions": base_conditions, "logic": None, "policies": [],
            "base_trace_id": None, "root_trace_id": "t0"}
    ops, used = [], set()
    for k in range(rng.randint(1, 3)):
        kind = rng.choice(["SET", "SET", "CLEAR", "KEEP", "SET_ORDER", "REPLACE_UNIVERSE"])
        origin = typing_.origin("t1", raw, k)
        if kind == "SET":
            dimension, operator, polarity, level, unit, hint = rng.choice(VOCABULARY)
            t = typing_.tcond(dimension, operator, polarity, level, unit, hint)
            if t["slot"] in used:
                continue
            used.add(t["slot"])
            ops.append({"op": "SET", "slot": t["slot"], "condition": typing_.condition_dict(t, f"p{k}", rids, origin),
                        "universe": None, "origin": origin})
        elif kind == "SET_ORDER" and "ORDER" not in used:
            used.add("ORDER")
            t = typing_.tcond("ORDER", rng.choice(["value_desc", "date_asc"]), "POSITIVE", None, None, None, ods="STATED")
            ops.append({"op": "SET_ORDER", "slot": "ORDER", "condition": typing_.condition_dict(t, f"p{k}", rids, origin),
                        "universe": None, "origin": origin})
        elif kind == "CLEAR" and base_conditions:
            c = rng.choice(base_conditions)
            slot = c["slot"].split("/")[0] if rng.random() < 0.5 and c["slot"].split("/")[0] in typing_.POLARIZED else c["slot"]
            if any(covered(slot, u) or covered(u, slot) for u in used):
                continue
            used.add(slot)
            ops.append({"op": "CLEAR", "slot": slot, "condition": None, "universe": None, "origin": origin})
        elif kind == "KEEP" and base_conditions:
            c = rng.choice(base_conditions)
            if any(covered(u, c["slot"]) or covered(c["slot"], u) for u in used):
                continue
            used.add(c["slot"])
            ops.append({"op": "KEEP", "slot": c["slot"], "condition": None, "universe": None, "origin": origin})
        elif kind == "REPLACE_UNIVERSE" and "UNIVERSE" not in used:
            used.add("UNIVERSE")
            ops.append({"op": "REPLACE_UNIVERSE", "slot": "UNIVERSE", "condition": None, "origin": origin,
                        "universe": {"subject": rng.choice(["contracts", "processes"]),
                                     "universe_kind": rng.choice(["records", "opportunities"])}})
    if not ops:
        ops = [{"op": "REPLACE_UNIVERSE", "slot": "UNIVERSE", "condition": None, "origin": typing_.origin("t1", raw, 0),
                "universe": {"subject": "contracts", "universe_kind": "records"}}]
    patch = {"schema": "m2a-patch/1", "ops": ops, "origin_kind": "TEXT"}
    return base, patch


def refused(base, patch) -> bool:
    return transition._refusal(base, patch["ops"]) is not None


@FAST
@given(seed=st.integers(min_value=0, max_value=10**7))
def test_the_faithful_application_is_lawful_over_generated_transitions_with_the_host_table(seed):
    base, patch = enrich(*random_transition(seed), seed, smmlv=True)
    if refused(base, patch):
        return
    for request in base["requests"]:
        request["dataset"] = host_dataset(request["subject"])
    effective = probe_apply(base, patch, dataset_of=host_dataset)
    assert check(base, patch, effective, dataset_of=host_dataset) == []
    assert check(base, patch, effective) == []  # the relational check agrees


GENERATED = []
for _seed in range(4000):
    _base, _patch = enrich(*random_transition(_seed), _seed, smmlv=True)
    if not refused(_base, _patch):
        GENERATED.append((f"generated-{_seed}", _base, _patch))
        if len(GENERATED) == 400:
            break


# ====================================================================================================================
# HAND-BUILT TRANSITIONS: what neither the Gold nor a random draw is guaranteed to exercise
# ====================================================================================================================
def cond(dimension, operator, polarity, hint, cid, rids, *, level=None, unit=None, n_requested=None, ods=None, n=0):
    t = typing_.tcond(dimension, operator, polarity, level, unit, hint, ods=ods, n_requested=n_requested)
    return typing_.condition_dict(t, cid, rids, typing_.origin("t0", "a" * 64, n))


def base_of(conditions, pairs=(("contracts", "records"),), dataset=host_dataset):
    requests = typing_.request_dicts(list(pairs), "t0", "b" * 64)
    for r in requests:
        r["dataset"] = dataset(r["subject"])
    rids = [r["request_id"] for r in requests]
    return {"schema": "m2a-accepted/1", "requests": requests, "conditions": [dict(c, request_ids=rids) for c in conditions],
            "logic": None, "policies": [], "base_trace_id": None, "root_trace_id": "t0"}


def patch_of(*ops):
    return {"schema": "m2a-patch/1", "ops": list(ops), "origin_kind": "TEXT"}


def set_op(c, n=0):
    org = typing_.origin("t1", "c" * 64, n)
    return {"op": "SET_ORDER" if c["dimension"] == "ORDER" else "SET", "slot": c["slot"],
            "condition": dict(c, origin=org), "universe": None, "origin": org}


def clear_op(slot, n=0):
    return {"op": "CLEAR", "slot": slot, "condition": None, "universe": None, "origin": typing_.origin("t1", "c" * 64, n)}


def keep_op(slot, n=0):
    return {"op": "KEEP", "slot": slot, "condition": None, "universe": None, "origin": typing_.origin("t1", "c" * 64, n)}


def redirect_op(subject, kind, n=0):
    return {"op": "REPLACE_UNIVERSE", "slot": "UNIVERSE", "condition": None,
            "universe": {"subject": subject, "universe_kind": kind}, "origin": typing_.origin("t1", "c" * 64, n)}


R = ["r1"]
TOPIC_A = cond("TOPIC", "contains", "POSITIVE", "aseo", "c1", R, n=1)
TOPIC_B_NEG = cond("TOPIC", "contains", "NEGATIVE", "obras", "p1", R, n=2)
TOPIC_A_NEG = cond("TOPIC", "contains", "NEGATIVE", "aseo", "p1", R, n=2)
TOPIC_C = cond("TOPIC", "contains", "POSITIVE", "dotación", "c2", R, n=3)
TOPIC_NEG = cond("TOPIC", "contains", "NEGATIVE", "papelería", "c3", R, n=4)
BUYER = cond("BUYER", "is", "POSITIVE", "SENA", "c4", R, n=5)
LOWER = cond("VALUE", "greater_or_equal", "POSITIVE", "100 millones", "c5", R, unit="MILLONES", n=6)
UPPER = cond("VALUE", "less_or_equal", "POSITIVE", "400 millones", "c6", R, unit="MILLONES", n=7)
NEW_UPPER_COLLIDING = cond("VALUE", "less_or_equal", "POSITIVE", "100 millones", "p2", R, unit="MILLONES", n=8)
NEW_UPPER_OK = cond("VALUE", "less_or_equal", "POSITIVE", "800 millones", "p2", R, unit="MILLONES", n=8)
ORDER = cond("ORDER", "value_desc", "POSITIVE", None, "c7", R, ods="STATED", n=9)
CARD = cond("CARDINALITY", "top_n", "POSITIVE", None, "c8", R, n_requested=10, n=10)
NEW_ORDER = cond("ORDER", "date_desc", "POSITIVE", None, "p3", R, ods="STATED", n=11)
NEW_TOPIC = cond("TOPIC", "contains", "POSITIVE", "vigilancia", "p4", R, n=12)


def hand(name, conditions, ops, pairs=(("contracts", "records"),)):
    base = base_of(conditions, pairs)
    rids = [r["request_id"] for r in base["requests"]]
    patch = patch_of(*ops)
    for op in patch["ops"]:
        if op["condition"]:
            op["condition"]["request_ids"] = rids
    return name, (base, patch)


HAND = [
    hand("f16_other_value", [TOPIC_A], [set_op(TOPIC_B_NEG)]),
    hand("f16_same_value", [TOPIC_A], [set_op(TOPIC_A_NEG)]),
    hand("two_positives_and_a_negative_set", [TOPIC_A, TOPIC_C], [set_op(TOPIC_B_NEG)]),
    # R3 (P-3): +X and -X in one patch is NOT a lawful transition any more (OP_NOT_ACCREDITED); it is a refusal case below
    hand("clear_topic_with_both_polarities", [TOPIC_A, TOPIC_NEG, BUYER], [clear_op("TOPIC")]),
    hand("a81_collision", [LOWER, BUYER], [set_op(NEW_UPPER_COLLIDING)]),
    hand("a81_no_collision", [LOWER, BUYER], [set_op(NEW_UPPER_OK)]),
    hand("set_order_keeps_cardinality", [ORDER, CARD, BUYER], [set_op(NEW_ORDER)]),
    hand("set_replaces_a_slot", [TOPIC_A, BUYER], [set_op(cond("TOPIC", "contains", "POSITIVE", "vigilancia", "p1", R, n=2))]),
    hand("redirect_new_subject_and_kind", [TOPIC_A, BUYER], [redirect_op("processes", "opportunities")]),
    hand("redirect_same_subject_other_kind", [TOPIC_A], [redirect_op("contracts", "opportunities")]),
    hand("dual_reduced_to_the_named", [TOPIC_A, BUYER], [redirect_op("contracts", "records"), set_op(NEW_TOPIC, 1)],
         pairs=(("processes", "records"), ("contracts", "records"))),
    hand("dual_untouched", [TOPIC_A, BUYER], [set_op(NEW_TOPIC)], pairs=(("processes", "records"), ("contracts", "records"))),
    hand("single_untouched", [TOPIC_A, BUYER, LOWER], [set_op(NEW_TOPIC), keep_op("BUYER/POSITIVE", 1)]),
]
HAND = [(name, *enrich(base, patch, 7)) for name, (base, patch) in HAND]
for _name, _base, _patch in HAND:
    for _r in _base["requests"]:
        _r["dataset"] = host_dataset(_r["subject"])
POOL = [*GOLD_TRANSITIONS, *GENERATED, *HAND]
HAND_BY_NAME = {n: (b, p) for n, b, p in HAND}


@pytest.mark.parametrize("name", [n for n, *_ in HAND])
def test_every_hand_built_transition_is_lawful_when_faithful_with_the_host_table(name):
    base, patch = HAND_BY_NAME[name]
    assert check(base, patch, probe_apply(base, patch, dataset_of=host_dataset), dataset_of=host_dataset) == []


# ====================================================================================================================
# THE REFUSALS (§6 steps 2, 3b, 5; §6.7): what the contract refuses, and for which cause first
# ====================================================================================================================
def with_unit(condition, unit):
    return {**condition, "provenance": {**condition["provenance"], "unit": unit}}


SMMLV_UPPER = with_unit(cond("VALUE", "less_or_equal", "POSITIVE", "400 millones", "p2", R, unit="MILLONES", n=8), "SMMLV")
TOPIC_A_P0 = cond("TOPIC", "contains", "POSITIVE", "aseo", "p0", R, n=1)
BUYER_A_NEG = cond("BUYER", "is", "NEGATIVE", "SENA", "p5", R, n=5)
BUYER_A_P = cond("BUYER", "is", "POSITIVE", "SENA", "p6", R, n=6)
TOPIC_C_NEG = cond("TOPIC", "contains", "NEGATIVE", "dotación", "p7", R, n=7)
CARD_NEW = cond("CARDINALITY", "top_n", "POSITIVE", None, "p8", R, n_requested=20, n=11)
TOPIC_OTHER = cond("TOPIC", "contains", "POSITIVE", "otro", "p9", R)

REFUSAL_CASES = {  # name -> (base, patch): each patch has a cause the contract refuses; the faithful application is NOT lawful
    "effective_for_two_set_one_slot": hand("c", [TOPIC_A], [set_op(NEW_TOPIC), set_op(TOPIC_OTHER)])[1],
    "effective_for_keep_and_clear_one_slot": hand("kc", [TOPIC_A], [keep_op("TOPIC/POSITIVE"), clear_op("TOPIC", 1)])[1],
    "effective_for_keep_and_set_same_value": hand("ks", [TOPIC_A, BUYER], [keep_op("TOPIC/POSITIVE"), set_op(TOPIC_A_P0, 1)])[1],
    "effective_for_keep_and_set_order": hand("ko", [ORDER, CARD, BUYER], [keep_op("ORDER"), set_op(NEW_ORDER, 1)])[1],
    "effective_for_keep_and_replace_universe": hand("ku", [TOPIC_A], [keep_op("UNIVERSE"), redirect_op("contracts", "records", 1)])[1],
    "effective_for_two_replace_universe": hand("rr", [TOPIC_A], [redirect_op("processes", "records"), redirect_op("contracts", "records", 1)])[1],
    "effective_for_keep_on_empty_slot": hand("k", [TOPIC_A], [keep_op("BUYER/POSITIVE")])[1],
    "effective_for_f16_several_positives": hand("f", [TOPIC_A, TOPIC_C, BUYER], [set_op(TOPIC_A_NEG)])[1],
    "effective_for_f16_same_patch_positive": hand("p3", [BUYER], [set_op(TOPIC_A_P0), set_op(TOPIC_A_NEG, 1)])[1],
    "effective_for_a81_incomparable_units": hand("a", [LOWER, BUYER], [set_op(SMMLV_UPPER)])[1],
    "effective_for_clear_order_leaving_cardinality": hand("o", [ORDER, CARD, BUYER], [clear_op("ORDER")])[1],
    "effective_for_set_cardinality_without_order": hand("sc", [TOPIC_A, BUYER], [set_op(CARD_NEW)])[1],
    "effective_for_clear_on_empty_slot": hand("e", [TOPIC_A], [clear_op("MODALITY")])[1],
}
EXPECTED_REFUSAL = {
    "effective_for_two_set_one_slot": "DELTA_SLOT_CONFLICT", "effective_for_keep_and_clear_one_slot": "DELTA_SLOT_CONFLICT",
    "effective_for_keep_and_set_same_value": "DELTA_SLOT_CONFLICT", "effective_for_keep_and_set_order": "DELTA_SLOT_CONFLICT",
    "effective_for_keep_and_replace_universe": "DELTA_SLOT_CONFLICT", "effective_for_two_replace_universe": "DELTA_SLOT_CONFLICT",
    "effective_for_keep_on_empty_slot": "KEEP_ON_EMPTY_SLOT", "effective_for_f16_several_positives": "OP_NOT_ACCREDITED:TOPIC",
    "effective_for_f16_same_patch_positive": "OP_NOT_ACCREDITED:TOPIC",
    "effective_for_a81_incomparable_units": "OP_NOT_ACCREDITED:VALUE",
    "effective_for_clear_order_leaving_cardinality": "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER",
    "effective_for_set_cardinality_without_order": "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER",
    "effective_for_clear_on_empty_slot": "CLEAR_ON_EMPTY_SLOT",
}


@pytest.mark.parametrize("name", sorted(REFUSAL_CASES))
def test_a_patch_the_contract_refuses_must_not_produce_an_effective(name):
    base, patch = enrich(*REFUSAL_CASES[name], 3)
    assert check(base, patch, None) == []
    violations = check(base, patch, probe_apply(base, patch))
    assert [v["rule"] for v in violations] == ["T-REFUSAL"]
    assert transition.expected_refusal(base, patch) == EXPECTED_REFUSAL[name]
    assert check(base, patch, None, refusal=EXPECTED_REFUSAL[name]) == []  # and refusing with its code is lawful


@pytest.mark.parametrize("a3,deltarefusal", [("UNRESOLVED_VALUE:TOPIC", "OP_NOT_ACCREDITED:TOPIC"),
                                               ("UNRESOLVED_VALUE:VALUE", "OP_NOT_ACCREDITED:VALUE"),
                                               ("VALUE_WITHOUT_COMPARATOR", "OP_NOT_ACCREDITED:VALUE")])
def test_the_a3_code_published_for_op_not_accredited_is_the_same_refusal(a3, deltarefusal):
    assert transition._published(a3) == deltarefusal
    for name in ("effective_for_f16_several_positives", "effective_for_a81_incomparable_units"):
        base, patch = enrich(*REFUSAL_CASES[name], 3)
        top = transition.expected_refusal(base, patch)
        assert (check(base, patch, None, refusal=a3) == []) == (top == deltarefusal)


def test_the_contract_does_not_refuse_what_it_does_not_name():
    """KEEP of a slot the patch does not write; F16 over the single carried positive (a declared substitution); a negation of
    ANOTHER value next to several positives (F17); a bound against a comparable carried one; CLEAR(ORDER)+CLEAR(CARDINALITY);
    CLEAR(ORDER)+SET_ORDER (F10); an ANSWER-free delta over a base with logic that no op addresses."""
    lawful = {
        "keep_other_slot": hand("a", [TOPIC_A, BUYER], [set_op(NEW_TOPIC), keep_op("BUYER/POSITIVE", 1)])[1],
        "f16_single_carried": hand("b", [TOPIC_A, BUYER], [set_op(TOPIC_A_NEG)])[1],
        "negate_another_value_next_to_several": hand("c", [TOPIC_A, TOPIC_C, BUYER], [set_op(TOPIC_B_NEG)])[1],
        "comparable_bound": hand("d", [LOWER, BUYER], [set_op(NEW_UPPER_OK)])[1],
        "clear_order_and_cardinality": hand("e", [ORDER, CARD, BUYER], [clear_op("ORDER"), clear_op("CARDINALITY", 1)])[1],
        "clear_order_and_set_order": hand("f", [ORDER, CARD, BUYER], [clear_op("ORDER"), set_op(NEW_ORDER, 1)])[1],
        "negative_next_to_a_different_positive": hand("g", [BUYER], [set_op(TOPIC_A_P0), set_op(TOPIC_B_NEG, 1)])[1],
        "keep_the_universe": hand("h", [TOPIC_A], [keep_op("UNIVERSE"), set_op(NEW_TOPIC, 1)])[1],
        "clear_of_an_empty_slot_is_h12_only_when_it_is_the_only_op": hand("i", [TOPIC_A, BUYER],
                                                                          [clear_op("MODALITY"), set_op(NEW_TOPIC, 1)])[1],
    }
    for name, (base, patch) in lawful.items():
        base, patch = enrich(base, patch, 5)
        assert transition.refusal_causes(base, patch) == [] and transition.expected_refusal(base, patch) is None, name
        assert check(base, patch, probe_apply(base, patch)) == [], name


def test_a_cause_in_a_slot_the_patch_replaces_is_not_carried_so_it_is_not_a_cause():
    """F16 fail-closed needs the carried positives: a SET of the positive slot in the same patch replaces them first."""
    base, patch = hand("r", [TOPIC_A, TOPIC_C, BUYER], [set_op(TOPIC_OTHER), set_op(TOPIC_A_NEG, 1)])[1]
    base, patch = enrich(base, patch, 5)
    assert transition.refusal_causes(base, patch) == []
    carried_positive_cleared = hand("s", [TOPIC_A, TOPIC_C, BUYER], [clear_op("TOPIC/POSITIVE"), set_op(TOPIC_A_NEG, 1)])[1]
    assert transition.refusal_causes(*enrich(*carried_positive_cleared, 5)) == []


# ====================================================================================================================
# THE MUTANTS: every rule of the table is killed by each mutant it names
# ====================================================================================================================
def req_ids(effective):
    return [r["request_id"] for r in effective["requests"]]


def with_request_rename(effective, old, new):
    effective = copy.deepcopy(effective)
    for r in effective["requests"]:
        if r["request_id"] == old:
            r["request_id"] = new
    for c in effective["conditions"]:
        c["request_ids"] = sorted(new if i == old else i for i in c["request_ids"])
    return effective


def carried_of(base, patch, effective):
    ids = {c["condition_id"] for c in base["conditions"]}
    return [c for c in effective["conditions"] if c["condition_id"] in ids]


def placed_of(base, patch, effective):
    ids = {c["condition_id"] for c in base["conditions"]}
    return [c for c in effective["conditions"] if c["condition_id"] not in ids]


def redirect_of(patch):
    return next((op for op in patch["ops"] if op["op"] == "REPLACE_UNIVERSE"), None)


def m_untouched_requests(fn):
    def mutate(base, patch, effective):
        return fn(base, patch, copy.deepcopy(effective)) if redirect_of(patch) is None else None
    return mutate


def m_redirected(fn):
    def mutate(base, patch, effective):
        return fn(base, patch, copy.deepcopy(effective)) if redirect_of(patch) is not None and len(effective["requests"]) == 1 else None
    return mutate


def tamper(kind, field, change):
    def mutate(base, patch, effective):
        effective = copy.deepcopy(effective)
        pool = (carried_of if kind == "carried" else placed_of)(base, patch, effective)
        if not pool:
            return None
        target = pool[0]
        target[field] = {**target[field], **change} if isinstance(change, dict) else change
        return effective
    return mutate


def m_rename_carried(base, patch, effective):
    effective = copy.deepcopy(effective)
    pool = carried_of(base, patch, effective)
    if not pool:
        return None
    pool[0]["condition_id"] = "renamed-carried"
    return effective


def m_placed_changed(base, patch, effective):
    effective = copy.deepcopy(effective)
    pool = placed_of(base, patch, effective)
    if not pool:
        return None
    pool[0]["level"] = "DEPARTMENT" if pool[0]["level"] is None else None
    return effective


def reset_host_fields(base, patch, effective):
    effective = copy.deepcopy(effective)
    pool = carried_of(base, patch, effective)
    if not pool:
        return None
    for c in pool:
        c.update(policies=[], receipt_rules=[], authority="oracle.synthetic@1")
        c["provenance"].update(source_amount=None, year=None, year_source=None, buyer_scope=None, inherited_from=None)
    return effective


def readd(base, patch, effective, predicate):
    """The effective with the base conditions that satisfy ``predicate`` carried back in."""
    gone = [c for c in base["conditions"] if predicate(c) and not any(
        e["condition_id"] == c["condition_id"] for e in effective["conditions"])]
    if not gone:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"] += [dict(c, request_ids=effective["requests"] and sorted(req_ids(effective))) for c in copy.deepcopy(gone)]
    return effective


def written_slots(patch):
    return [op["slot"] for op in patch["ops"] if op["op"] in ("SET", "SET_ORDER", "CLEAR")]


def m_carried_in_written_slot(base, patch, effective):
    sets = [op["slot"] for op in patch["ops"] if op["op"] in ("SET", "SET_ORDER")]
    return readd(base, patch, effective, lambda c: c["slot"] in sets)


def m_cleared_kept(base, patch, effective):
    clears = [op["slot"] for op in patch["ops"] if op["op"] == "CLEAR"]
    return readd(base, patch, effective, lambda c: any(covered(w, c["slot"]) for w in clears))


def m_clear_only_positive(base, patch, effective):
    clears = [op["slot"] for op in patch["ops"] if op["op"] == "CLEAR" and op["slot"] in typing_.POLARIZED]
    return readd(base, patch, effective, lambda c: c["slot"].endswith("/NEGATIVE") and any(covered(w, c["slot"]) for w in clears))


def m_removal_without_op(base, patch, effective):
    pool = [c for c in carried_of(base, patch, effective) if not any(covered(w, c["slot"]) for w in written_slots(patch))]
    if not pool:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"] = [c for c in effective["conditions"] if c["condition_id"] != pool[0]["condition_id"]]
    return effective


def m_f16_any_value(base, patch, effective):
    for op in patch["ops"]:
        if op["op"] == "SET" and op["slot"].endswith("/NEGATIVE") and op["slot"].split("/")[0] in typing_.POLARIZED:
            positive = op["slot"].replace("/NEGATIVE", "/POSITIVE")
            single = [c for c in base["conditions"] if c["slot"] == positive]
            if len(single) == 1 and single[0]["value"][1:-1] != op["condition"]["value"][1:-1]:
                effective = copy.deepcopy(effective)
                effective["conditions"] = [c for c in effective["conditions"] if c["condition_id"] != single[0]["condition_id"]]
                return effective
    return None


def m_f16_positive_kept(base, patch, effective):
    """The reducer that never inverts: the single carried +X stays next to the -X it places (ZEUS tx1)."""
    for op in patch["ops"]:
        if op["op"] == "SET" and op["slot"].endswith("/NEGATIVE") and op["slot"].split("/")[0] in typing_.POLARIZED:
            standing = [c for c in base["conditions"] if c["slot"] == op["slot"].replace("/NEGATIVE", "/POSITIVE")]
            canonical = [i for i in op["condition"]["value"] if not i.startswith("excluded=")]
            if len(standing) == 1 and [i for i in standing[0]["value"] if not i.startswith("excluded=")] == canonical:
                return readd(base, patch, effective, lambda c: c["condition_id"] == standing[0]["condition_id"])
    return None


def m_set_clears_whole_dimension(base, patch, effective):
    for op in patch["ops"]:
        if op["op"] == "SET" and op["slot"].split("/")[0] in typing_.POLARIZED:
            dimension = op["slot"].split("/")[0]
            others = [c for c in carried_of(base, patch, effective) if c["slot"].startswith(dimension + "/") and c["slot"] != op["slot"]
                      and not any(covered(w, c["slot"]) for w in written_slots(patch))]
            if others:
                effective = copy.deepcopy(effective)
                gone = {c["condition_id"] for c in others}
                effective["conditions"] = [c for c in effective["conditions"] if c["condition_id"] not in gone]
                return effective
    return None


def m_set_order_drops_cardinality(base, patch, effective):
    if not any(op["op"] == "SET_ORDER" for op in patch["ops"]) or any(op["slot"] == "CARDINALITY" for op in patch["ops"]):
        return None
    cards = [c for c in carried_of(base, patch, effective) if c["slot"] == "CARDINALITY"]
    if not cards:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"] = [c for c in effective["conditions"] if c["slot"] != "CARDINALITY"]
    return effective


def m_a81_without_collision(base, patch, effective):
    for op in patch["ops"]:
        if op["op"] == "SET" and op["slot"].startswith("VALUE/"):
            other = "VALUE/upper" if op["slot"] == "VALUE/lower" else "VALUE/lower"
            kept = [c for c in carried_of(base, patch, effective) if c["slot"] == other]
            if kept:
                effective = copy.deepcopy(effective)
                effective["conditions"] = [c for c in effective["conditions"] if c is not kept[0] and c["condition_id"] != kept[0]["condition_id"]]
                return effective
    return None


def m_no_a81(base, patch, effective):
    """The reducer that never applies A8.1: the collided opposite bound the faithful application dropped stays."""
    for op in patch["ops"]:
        if op["op"] == "SET" and op["slot"].startswith("VALUE/"):
            other = "VALUE/upper" if op["slot"] == "VALUE/lower" else "VALUE/lower"
            if any(covered(w, other) for w in written_slots(patch)):
                continue  # an explicit CLEAR(VALUE) is not A8.1
            lost = [c for c in base["conditions"] if c["slot"] == other and not any(
                e["condition_id"] == c["condition_id"] for e in effective["conditions"])]
            if lost:
                effective = copy.deepcopy(effective)
                effective["conditions"].append(dict(copy.deepcopy(lost[0]), request_ids=sorted(req_ids(effective))))
                return effective
    return None


def m_op_not_applied(base, patch, effective):
    pool = placed_of(base, patch, effective)
    if not pool:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"] = [c for c in effective["conditions"] if c["condition_id"] != pool[0]["condition_id"]]
    return effective


def m_op_applied_twice(base, patch, effective):
    pool = [c for c in placed_of(base, patch, effective) if c["slot"].split("/")[0] in typing_.POLARIZED]
    if not pool:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"].append(dict(copy.deepcopy(pool[0]), condition_id="new-twice"))
    return effective


def m_fabricated(base, patch, effective):
    unwritten = "MODALITY/POSITIVE"
    if any(c["slot"] == unwritten for c in effective["conditions"]) or any(covered(w, unwritten) for w in written_slots(patch)):
        return None
    effective = copy.deepcopy(effective)
    t = typing_.tcond("MODALITY", "is", "POSITIVE", None, None, "mínima cuantía")
    effective["conditions"].append(typing_.condition_dict(t, "fabricated", sorted(req_ids(effective)), typing_.origin("t9", "9" * 64, 1)))
    return effective


def m_logic_invented(base, patch, effective):
    effective = copy.deepcopy(effective)
    effective["logic"] = {"operator": "OR", "condition_id": None, "children": []}
    return effective


def m_effective_extra_key(base, patch, effective):
    return {**copy.deepcopy(effective), "extra": 1}


def m_duplicate_request_id(base, patch, effective):
    if len(effective["requests"]) < 2:
        return None
    effective = copy.deepcopy(effective)
    effective["requests"][1]["request_id"] = effective["requests"][0]["request_id"]
    return effective


def m_duplicate_condition_id(base, patch, effective):
    if len(effective["conditions"]) < 2:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"][1]["condition_id"] = effective["conditions"][0]["condition_id"]
    return effective


def m_condition_unknown_request(base, patch, effective):
    if not effective["conditions"]:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"][0]["request_ids"] = ["r-unknown"]
    return effective


def m_dual_scope_differs(base, patch, effective):
    if len(effective["requests"]) < 2:
        return None
    effective = copy.deepcopy(effective)
    effective["requests"][1]["scope_id"] += "-x"
    return effective


def m_dangling_policy(base, patch, effective):
    effective = copy.deepcopy(effective)
    effective["policies"] = [{"policy_id": "topn-default", "version": "1", "condition_ids": ["no-such-condition"], "effect": "EQUIVALENT"}]
    return effective


def m_malformed_policy(base, patch, effective):
    effective = copy.deepcopy(effective)
    effective["policies"] = [{"policy": "topn-default@1"}]
    return effective


def m_two_bounds_same_side(base, patch, effective):
    pool = [c for c in effective["conditions"] if c["slot"] == "VALUE/lower"]
    if not pool:
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"].append(dict(copy.deepcopy(pool[0]), condition_id="second-bound"))
    return effective


def m_cardinality_without_order(base, patch, effective):
    if not any(c["slot"] == "CARDINALITY" for c in effective["conditions"]):
        return None
    effective = copy.deepcopy(effective)
    effective["conditions"] = [c for c in effective["conditions"] if c["slot"] != "ORDER"]
    return effective


def m_request_ids_not_rewritten(base, patch, effective):
    if len(base["requests"]) < 2 or len(effective["requests"]) != 1:
        return None
    effective = copy.deepcopy(effective)
    carried = carried_of(base, patch, effective)
    if not carried:
        return None
    carried[0]["request_ids"] = sorted(r["request_id"] for r in base["requests"])
    return effective


def m_carried_request_ids_changed(base, patch, effective):
    if len(base["requests"]) < 2 or len(effective["requests"]) != 2:
        return None
    carried = carried_of(base, patch, effective)
    if not carried:
        return None
    effective = copy.deepcopy(effective)
    carried_of(base, patch, effective)[0]["request_ids"] = [req_ids(effective)[0]]
    return effective


def m_placed_applies_to_one_request(base, patch, effective):
    if len(effective["requests"]) < 2:
        return None
    pool = placed_of(base, patch, effective)
    if not pool:
        return None
    effective = copy.deepcopy(effective)
    placed_of(base, patch, effective)[0]["request_ids"] = [req_ids(effective)[0]]
    return effective


# ---- requests ---------------------------------------------------------------------------------------------------------
def m_M3(base, patch, effective):
    effective["requests"][0]["universe_origin"] = copy.deepcopy(patch["ops"][0]["origin"])
    return effective


def m_M4(base, patch, effective):
    for r in effective["requests"]:
        r["scope_id"] = r["scope_id"] + "-changed"
    return effective


def m_M5(base, patch, effective):
    return with_request_rename(effective, effective["requests"][0]["request_id"], "q-renamed")


def m_untouched_dataset(base, patch, effective):
    effective["requests"][0]["dataset"] = "some.other.dataset"
    return effective


def m_redirect_request_id(base, patch, effective):
    return with_request_rename(effective, effective["requests"][0]["request_id"], "q-renamed")


def m_redirect_scope_id(base, patch, effective):
    effective["requests"][0]["scope_id"] += "-changed"
    return effective


def m_redirect_view(base, patch, effective):
    effective["requests"][0]["view"] = {"decision": "PV9"}
    return effective


def m_redirect_old_kind(base, patch, effective):
    old = next(r for r in base["requests"] if r["request_id"] == effective["requests"][0]["request_id"])
    if old["universe_kind"] == effective["requests"][0]["universe_kind"]:
        return None
    effective["requests"][0]["universe_kind"] = old["universe_kind"]
    return effective


def m_redirect_old_subject(base, patch, effective):
    old = next(r for r in base["requests"] if r["request_id"] == effective["requests"][0]["request_id"])
    if old["subject"] == effective["requests"][0]["subject"]:
        return None
    effective["requests"][0]["subject"] = old["subject"]
    return effective


def m_M2(base, patch, effective):
    old = next(r for r in base["requests"] if r["request_id"] == effective["requests"][0]["request_id"])
    effective["requests"][0]["universe_origin"] = copy.deepcopy(old["universe_origin"])
    return effective


def m_M1(base, patch, effective):
    old = next(r for r in base["requests"] if r["request_id"] == effective["requests"][0]["request_id"])
    if old["subject"] == effective["requests"][0]["subject"]:
        return None
    effective["requests"][0]["dataset"] = old["dataset"]
    return effective


def m_dual_keeps_both(base, patch, effective):
    if len(base["requests"]) < 2:
        return None
    effective["requests"] = copy.deepcopy(base["requests"])
    for c in effective["conditions"]:
        c["request_ids"] = sorted(r["request_id"] for r in base["requests"])
    return effective


def m_dual_keeps_the_other(base, patch, effective):
    if len(base["requests"]) < 2:
        return None
    other = copy.deepcopy(next(r for r in base["requests"] if r["subject"] != redirect_of(patch)["universe"]["subject"]))
    effective["requests"] = [other]
    for c in effective["conditions"]:
        c["request_ids"] = [other["request_id"]]
    return effective


def m_request_added(base, patch, effective):
    extra = copy.deepcopy(effective["requests"][0])
    extra["request_id"] = "r-extra"
    extra["subject"] = "processes" if extra["subject"] == "contracts" else "contracts"
    effective["requests"].append(extra)
    return effective


MUTANTS = {
    # name: (rule, function(base, patch, faithful_effective) -> mutated effective, or None when it does not manifest)
    "effective_extra_key": ("T-STRUCT", m_effective_extra_key),
    "duplicate_request_id": ("T-STRUCT", m_duplicate_request_id),
    "duplicate_condition_id": ("T-STRUCT", m_duplicate_condition_id),
    "condition_unknown_request": ("T-STRUCT", m_condition_unknown_request),
    "dual_scope_differs": ("T-STRUCT", m_dual_scope_differs),
    "dangling_policy": ("T-STRUCT", m_dangling_policy),
    "malformed_policy": ("T-STRUCT", m_malformed_policy),
    "two_bounds_same_side": ("T-STRUCT", m_two_bounds_same_side),
    "cardinality_without_order": ("T-STRUCT", m_cardinality_without_order),
    "request_ids_not_rewritten_after_reduction": ("T-STRUCT", m_request_ids_not_rewritten),
    "request_added_without_op": ("T-REQ-SET", m_untouched_requests(m_request_added)),
    "dual_keeps_both": ("T-REQ-SET", m_redirected(m_dual_keeps_both)),
    "dual_keeps_the_other": ("T-REQ-SET", m_redirected(m_dual_keeps_the_other)),
    "M3_reoriginate_untouched_request": ("T-REQ-UNTOUCHED", m_untouched_requests(m_M3)),
    "M4_change_scope_id_untouched": ("T-REQ-UNTOUCHED", m_untouched_requests(m_M4)),
    "M5_change_request_id_untouched": ("T-REQ-UNTOUCHED", m_untouched_requests(m_M5)),
    "untouched_request_dataset_changed": ("T-REQ-UNTOUCHED", m_untouched_requests(m_untouched_dataset)),
    "redirect_changes_request_id": ("T-REQ-REDIRECT", m_redirected(m_redirect_request_id)),
    "redirect_changes_scope_id": ("T-REQ-REDIRECT", m_redirected(m_redirect_scope_id)),
    "redirect_changes_view": ("T-REQ-REDIRECT", m_redirected(m_redirect_view)),
    "redirect_keeps_old_kind": ("T-REQ-TARGET", m_redirected(m_redirect_old_kind)),
    "redirect_keeps_old_subject": ("T-REQ-TARGET", m_redirected(m_redirect_old_subject)),
    "M2_stale_origin_after_redirect": ("T-REQ-ORIGIN", m_redirected(m_M2)),
    "M1_stale_dataset_after_redirect": ("T-REQ-DATASET", m_redirected(m_M1)),
    **{f"tamper_carried_{f}": ("T-COND-CARRIED", tamper("carried", f, ch)) for f, ch in
       {"origin": {"trace_id": "t-tampered"}, "provenance": {"inherited_from": "t-tampered"}, "authority": "tampered@9",
        "policies": ["tampered"], "receipt_rules": ["tampered.rule"]}.items()},
    "rename_carried_condition_id": ("T-COND-CARRIED", m_rename_carried),
    "reset_carried_host_fields": ("T-COND-CARRIED", reset_host_fields),
    **{f"tamper_placed_{f}": ("T-COND-PLACED", tamper("placed", f, ch)) for f, ch in
       {"origin": {"trace_id": "t-tampered"}, "provenance": {"inherited_from": "t-tampered"}, "authority": "tampered@9",
        "policies": ["tampered"], "receipt_rules": ["tampered.rule"]}.items()},
    "placed_value_changed": ("T-COND-PLACED", m_placed_changed),
    "fabricated_condition": ("T-COND-FABRICATED", m_fabricated),
    "carried_in_written_slot": ("T-COND-WRITTEN-SLOT", m_carried_in_written_slot),
    "cleared_condition_kept": ("T-COND-WRITTEN-SLOT", m_cleared_kept),
    "clear_dimension_only_positive": ("T-COND-WRITTEN-SLOT", m_clear_only_positive),
    "removal_without_op": ("T-COND-REMOVAL", m_removal_without_op),
    "f16_any_value": ("T-COND-REMOVAL", m_f16_any_value),
    "f16_positive_kept_next_to_its_negation": ("T-COND-F16", m_f16_positive_kept),
    "set_order_drops_cardinality": ("T-COND-REMOVAL", m_set_order_drops_cardinality),
    "set_clears_whole_dimension": ("T-COND-REMOVAL", m_set_clears_whole_dimension),
    "a81_without_collision": ("T-COND-REMOVAL", m_a81_without_collision),
    "op_not_applied": ("T-COND-ATOMIC", m_op_not_applied),
    "op_applied_twice": ("T-COND-ATOMIC", m_op_applied_twice),
    "carried_request_ids_changed_without_reduction": ("T-COND-REQUESTS", m_carried_request_ids_changed),
    "placed_applies_to_one_request": ("T-COND-REQUESTS", m_placed_applies_to_one_request),
    "no_a81": ("T-COND-INTERVAL", m_no_a81),
    "logic_invented": ("T-EFF-LOGIC", m_logic_invented),
}


def table() -> dict[str, set[str]]:
    return {rule["id"]: set(rule["killed_by"]) for rule in transition.TRANSITION_RULES}


def test_the_rule_table_names_exactly_the_mutants_of_the_tests():
    rules = table()
    by_rule: dict[str, set[str]] = {}
    for name, (rule, _fn) in MUTANTS.items():
        by_rule.setdefault(rule, set()).add(name)
    for name, (rule, _fn) in {**REFUSAL_CODE_MUTANTS, **UNJUSTIFIED_MUTANTS}.items():
        by_rule.setdefault(rule, set()).add(name)
    by_rule.setdefault("T-REFUSAL", set()).update(REFUSAL_CASES)
    for rule, names in rules.items():
        assert names == by_rule.get(rule, set()), rule
    assert set(by_rule) <= set(rules)


def test_every_rule_cites_a_clause_and_says_what_it_demands():
    ids = [r["id"] for r in transition.TRANSITION_RULES]
    assert len(set(ids)) == len(ids)
    for rule in transition.TRANSITION_RULES:
        assert rule["clause"].strip() and rule["demands"].strip() and rule["killed_by"], rule["id"]
        assert "§" in rule["clause"] or "INV-" in rule["clause"] or "M2A-1" in rule["clause"], rule["id"]


def faithful(base, patch):
    dataset_of = host_dataset if base["requests"][0]["dataset"].startswith("host.") else typing_.dataset_of
    return probe_apply(base, patch, dataset_of=dataset_of)


def manifestations(name):
    rule, fn = MUTANTS[name]
    out = []
    for label, base, patch in POOL:
        effective = faithful(base, patch)
        mutated = fn(base, patch, copy.deepcopy(effective))
        if mutated is not None and mutated != effective:
            out.append((label, base, patch, mutated))
    return rule, out


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_each_mutant_manifests_somewhere_and_is_killed_by_its_rule_everywhere(name):
    rule, found = manifestations(name)
    assert found, f"{name} never manifests on the pool: it kills nothing"
    for label, base, patch, mutated in found:
        rules = {v["rule"] for v in check(base, patch, mutated)}
        assert rule in rules, (name, label, sorted(rules))


# ---- the code of a refusal: closed precedence (§6 step 3b) and the slot order of §4.2 ----------------------------------------
def random_messy(seed: int) -> tuple[dict, dict]:
    """A generated transition with 1-3 troublemaker ops added: a second SET of a slot, a KEEP next to a SET, a KEEP of an
    absent slot, +X and -X in one patch (P-3), a negation of a carried positive, an incomparable bound, a CLEAR of ORDER.
    Some of them are refused (possibly for several causes), some are not."""
    base, patch = random_transition(seed)
    rng = random.Random(seed ^ 0x5EED)
    raw = f"{seed:064x}"
    ops = patch["ops"]
    rids = [r["request_id"] for r in base["requests"]]
    present = {c["slot"] for c in base["conditions"]}
    for k in range(rng.randint(1, 3)):
        trouble = rng.choice(["twice", "keep_and_set", "keep_absent", "p3", "negate_carried", "incomparable", "clear_order"])
        origin = typing_.origin("t2", raw, 50 + k)
        sets = [o for o in ops if o["op"] == "SET"]
        if trouble == "twice" and sets:
            dup = copy.deepcopy(rng.choice(sets))
            dup["condition"]["condition_id"] = f"dup{k}"
            ops.append(dup)
        elif trouble == "keep_and_set" and sets:
            ops.append({"op": "KEEP", "slot": rng.choice(sets)["slot"], "condition": None, "universe": None, "origin": origin})
        elif trouble == "keep_absent":
            absent = [s for s in ("MODALITY/POSITIVE", "STATE/POSITIVE", "ACTOR/NEGATIVE", "UNIVERSE") if s not in present]
            ops.append({"op": "KEEP", "slot": rng.choice(absent), "condition": None, "universe": None, "origin": origin})
        elif trouble == "p3":
            hint = rng.choice(["aseo", "obras"])
            for polarity in ("POSITIVE", "NEGATIVE"):
                tc = typing_.tcond("TOPIC", "contains", polarity, None, None, hint)
                ops.append({"op": "SET", "slot": tc["slot"], "universe": None, "origin": origin,
                            "condition": typing_.condition_dict(tc, f"p3{polarity[0]}{k}", rids, origin)})
        elif trouble == "negate_carried":
            positives = [c for c in base["conditions"] if c["slot"] == "TOPIC/POSITIVE"]
            if positives:
                hint = urllib.parse.unquote(positives[0]["value"][1].partition("=")[2])
                tc = typing_.tcond("TOPIC", "contains", "NEGATIVE", None, None, hint)
                ops.append({"op": "SET", "slot": tc["slot"], "universe": None, "origin": origin,
                            "condition": typing_.condition_dict(tc, f"nc{k}", rids, origin)})
        elif trouble == "incomparable":
            tc = typing_.tcond("VALUE", rng.choice(["greater_or_equal", "less_or_equal"]), "POSITIVE", None, "MILLONES",
                               "100 millones")
            condition = with_unit(typing_.condition_dict(tc, f"ic{k}", rids, origin), "SMMLV")
            ops.append({"op": "SET", "slot": tc["slot"], "condition": condition, "universe": None, "origin": origin})
        elif trouble == "clear_order":
            ops.append({"op": "CLEAR", "slot": "ORDER", "condition": None, "universe": None, "origin": origin})
    return base, {**patch, "ops": ops}


MULTI_CAUSE = {  # patches with SEVERAL causes, to pin the closed precedence and the slot order
    "conflict_and_keep_on_empty": hand("m1", [TOPIC_A, BUYER], [set_op(NEW_TOPIC), set_op(TOPIC_OTHER), keep_op("MODALITY/POSITIVE", 1)])[1],
    "keep_on_empty_and_opna": hand("m2", [BUYER], [keep_op("TIME/POSITIVE"), set_op(TOPIC_A_P0, 1), set_op(TOPIC_A_NEG, 2)])[1],
    "opna_and_cardinality": hand("m3", [ORDER, CARD, TOPIC_A], [clear_op("ORDER"), set_op(NEW_TOPIC, 1),
                                                                set_op(cond("TOPIC", "contains", "NEGATIVE", "vigilancia", "p3", R, n=3), 2)])[1],
    "conflict_and_cardinality": hand("m4", [ORDER, CARD, TOPIC_A], [clear_op("ORDER"), set_op(NEW_TOPIC, 1), set_op(TOPIC_OTHER, 2)])[1],
    "keep_on_empty_and_cardinality": hand("m5", [ORDER, CARD, TOPIC_A], [clear_op("ORDER"), keep_op("BUYER/POSITIVE", 1)])[1],
    "two_opna_in_two_dimensions": hand("m6", [LOWER], [set_op(BUYER_A_P), set_op(BUYER_A_NEG, 1), set_op(TOPIC_A_P0, 2), set_op(TOPIC_A_NEG, 3)])[1],
    "every_cause_at_once": hand("m7", [ORDER, CARD, TOPIC_A, LOWER], [clear_op("ORDER"), set_op(NEW_TOPIC, 1), set_op(TOPIC_OTHER, 2),
                                                                      keep_op("MODALITY/POSITIVE", 3), set_op(SMMLV_UPPER, 4)])[1],
    "opna_value_and_opna_topic": hand("m8", [LOWER, TOPIC_A, TOPIC_C], [set_op(SMMLV_UPPER), set_op(TOPIC_A_NEG, 1)])[1],
}
EXPECTED_TOP = {"conflict_and_keep_on_empty": "DELTA_SLOT_CONFLICT", "keep_on_empty_and_opna": "KEEP_ON_EMPTY_SLOT",
                "opna_and_cardinality": "OP_NOT_ACCREDITED:TOPIC", "conflict_and_cardinality": "DELTA_SLOT_CONFLICT",
                "keep_on_empty_and_cardinality": "KEEP_ON_EMPTY_SLOT", "two_opna_in_two_dimensions": "OP_NOT_ACCREDITED:TOPIC",
                "every_cause_at_once": "DELTA_SLOT_CONFLICT", "opna_value_and_opna_topic": "OP_NOT_ACCREDITED:TOPIC"}

REFUSED_POOL = [*((f"case:{n}", *enrich(*REFUSAL_CASES[n], 5)) for n in sorted(REFUSAL_CASES)),
                *((f"multi:{n}", *enrich(*MULTI_CAUSE[n], 5)) for n in sorted(MULTI_CAUSE))]
LAWFUL_POOL = [entry for entry in POOL if transition.expected_refusal(entry[1], entry[2]) is None]
MESSY = []
for _seed in range(6000):
    _base, _patch = enrich(*random_messy(_seed), _seed, smmlv=True)
    MESSY.append((f"messy-{_seed}", _base, _patch))
REFUSED_POOL += [m for m in MESSY if transition.expected_refusal(m[1], m[2]) is not None][:600]
LAWFUL_MESSY = [m for m in MESSY if transition.expected_refusal(m[1], m[2]) is None][:200]


def _codes(causes):
    return [c["code"] for c in causes]


def m_conflict_as_keep_on_empty(base, patch, causes):
    codes = _codes(causes)
    return "KEEP_ON_EMPTY_SLOT" if codes and codes[0] == "DELTA_SLOT_CONFLICT" and "KEEP_ON_EMPTY_SLOT" in codes else None


def m_keep_on_empty_as_opna(base, patch, causes):
    codes = _codes(causes)
    lower = [c for c in codes if c.startswith("OP_NOT_ACCREDITED")]
    return lower[0] if codes and codes[0] == "KEEP_ON_EMPTY_SLOT" and lower else None


def m_opna_as_cardinality(base, patch, causes):
    codes = _codes(causes)
    return "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER" if (codes and codes[0].startswith("OP_NOT_ACCREDITED")
                                                        and "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER" in codes) else None


def m_conflict_as_cardinality(base, patch, causes):
    codes = _codes(causes)
    return "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER" if (codes and codes[0] == "DELTA_SLOT_CONFLICT"
                                                        and "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER" in codes) else None


def m_precedence_reversed(base, patch, causes):
    return causes[-1]["code"] if len({c["refusal"] for c in causes}) >= 2 else None  # the LOWEST precedence first


def m_slot_order_ignored(base, patch, causes):
    same = [c for c in causes if c["refusal"] == causes[0]["refusal"]] if causes else []
    return same[-1]["code"] if len({c["code"] for c in same}) >= 2 else None  # the LAST slot of the same precedence


def m_wrong_dimension(base, patch, causes):
    if not causes or not causes[0]["code"].startswith("OP_NOT_ACCREDITED:"):
        return None
    return "OP_NOT_ACCREDITED:" + ("GEOGRAPHY" if causes[0]["code"].endswith(":TOPIC") else "TOPIC")


def m_replaced_by_a_host_rule(base, patch, causes):
    return "CONTEXT_EXPIRED_CONFIRM" if causes else None  # H1 would fire AFTER the causes (§6 step 3b precedes step 4)


def m_code_outside_the_vocabulary(base, patch, causes):
    return "NOT_A_CODE_OF_ANNEX_A3" if causes else None


REFUSAL_CODE_MUTANTS = {
    "refusal_conflict_reported_as_keep_on_empty": ("T-REFUSAL-CODE", m_conflict_as_keep_on_empty),
    "refusal_keep_on_empty_reported_as_opna": ("T-REFUSAL-CODE", m_keep_on_empty_as_opna),
    "refusal_opna_reported_as_cardinality": ("T-REFUSAL-CODE", m_opna_as_cardinality),
    "refusal_conflict_reported_as_cardinality": ("T-REFUSAL-CODE", m_conflict_as_cardinality),
    "refusal_precedence_reversed": ("T-REFUSAL-CODE", m_precedence_reversed),
    "refusal_slot_order_ignored": ("T-REFUSAL-CODE", m_slot_order_ignored),
    "refusal_wrong_dimension": ("T-REFUSAL-CODE", m_wrong_dimension),
    "refusal_replaced_by_a_host_rule": ("T-REFUSAL-CODE", m_replaced_by_a_host_rule),
    "refusal_code_outside_the_vocabulary": ("T-REFUSAL-CODE", m_code_outside_the_vocabulary),
}
def untouched_dimension_code(base, patch):
    touched = {op["slot"].partition("/")[0] for op in patch["ops"]}
    return "OP_NOT_ACCREDITED:" + next(d for d in ("STATE", "MODALITY", "ACTOR", "CONTRACT_TYPE_OR_NATURE") if d not in touched)


UNJUSTIFIED_MUTANTS = {  # a lawful patch refused for a structural cause that is not there, or blamed on an untouched dimension
    "refuse_lawful_as_slot_conflict": ("T-REFUSAL-UNJUSTIFIED", "DELTA_SLOT_CONFLICT"),
    "refuse_lawful_as_keep_on_empty": ("T-REFUSAL-UNJUSTIFIED", "KEEP_ON_EMPTY_SLOT"),
    "refuse_lawful_as_cardinality_without_order": ("T-REFUSAL-UNJUSTIFIED", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"),
    "refuse_lawful_as_target_not_addressable": ("T-REFUSAL-UNJUSTIFIED", "TARGET_NOT_ADDRESSABLE"),
    "refuse_op_not_accredited_on_an_untouched_dimension": ("T-REFUSAL-UNJUSTIFIED", untouched_dimension_code),
}


@pytest.mark.parametrize("name", sorted(MULTI_CAUSE))
def test_the_precedence_of_several_causes_is_the_contracts_and_the_correct_code_is_lawful(name):
    base, patch = enrich(*MULTI_CAUSE[name], 5)
    causes = transition.refusal_causes(base, patch)
    assert len(causes) >= 2 and causes[0]["code"] == EXPECTED_TOP[name] == transition.expected_refusal(base, patch)
    assert [PRECEDENCE_INDEX[c["refusal"]] for c in causes] == sorted(PRECEDENCE_INDEX[c["refusal"]] for c in causes)
    assert check(base, patch, None, refusal=EXPECTED_TOP[name]) == []
    assert [v["rule"] for v in check(base, patch, probe_apply(base, patch))] == ["T-REFUSAL"]  # an effective is unlawful


PRECEDENCE_INDEX = {code: n for n, code in enumerate(transition.PRECEDENCE)}


def test_every_refusal_of_the_pool_is_lawful_with_its_own_code_and_unlawful_with_any_other_cause_first():
    for label, base, patch in REFUSED_POOL:
        causes = transition.refusal_causes(base, patch)
        top = transition.expected_refusal(base, patch)
        assert check(base, patch, None, refusal=top) == [], label
        for cause in causes[1:]:
            if cause["code"] != top:
                assert {v["rule"] for v in check(base, patch, None, refusal=cause["code"])} == {"T-REFUSAL-CODE"}, label


@pytest.mark.parametrize("name", sorted(REFUSAL_CODE_MUTANTS))
def test_each_refusal_code_mutant_manifests_and_dies_on_its_rule(name):
    rule, mutate = REFUSAL_CODE_MUTANTS[name]
    hits = 0
    for label, base, patch in REFUSED_POOL:
        wrong = mutate(base, patch, transition.refusal_causes(base, patch))
        if wrong is None or wrong == transition.expected_refusal(base, patch):
            continue
        hits += 1
        assert rule in {v["rule"] for v in check(base, patch, None, refusal=wrong)}, (name, label, wrong)
    assert hits >= 3, f"{name} manifests {hits} times"


@pytest.mark.parametrize("name", sorted(UNJUSTIFIED_MUTANTS))
def test_each_unjustified_refusal_mutant_dies_on_every_lawful_transition(name):
    rule, code = UNJUSTIFIED_MUTANTS[name]
    assert len(LAWFUL_POOL) > 100 and len(LAWFUL_MESSY) > 20
    for label, base, patch in [*LAWFUL_POOL, *LAWFUL_MESSY]:
        assert base["logic"] is None
        refusal = code(base, patch) if callable(code) else code
        assert rule in {v["rule"] for v in check(base, patch, None, refusal=refusal)}, (name, label)


def test_what_the_dicts_cannot_show_is_not_judged_and_says_so():
    """An authority refusal (OP_NOT_ACCREDITED over an op the authority did not resolve) and TARGET_NOT_ADDRESSABLE over a base
    with a logic expression cannot be decided from the dicts: they are accepted when an op touches the dimension named; the Gold
    oracle decides them by case (LG-AUTHORITY-REFUSALS)."""
    for label, base, patch in LAWFUL_POOL[:50]:
        dimension = patch["ops"][0]["slot"].partition("/")[0]
        assert check(base, patch, None, refusal=f"OP_NOT_ACCREDITED:{dimension}") == [], label
        assert check(base, patch, None, refusal=f"UNRESOLVED_VALUE:{dimension}") == [], label
        assert check(base, patch, None, refusal="CONTEXT_EXPIRED_CONFIRM") == [], label  # a host rule: the Gold's to decide
    label, base, patch = LAWFUL_POOL[0]
    with_logic = {**copy.deepcopy(base), "logic": {"operator": "OR", "condition_id": None, "children": []}}
    assert check(with_logic, patch, None, refusal="TARGET_NOT_ADDRESSABLE") == []
    assert check(base, patch, None, refusal="TARGET_NOT_ADDRESSABLE") != []


def test_a_refusal_code_must_be_in_the_vocabulary_of_annex_a3_and_blame_a_dimension_an_op_touches():
    label, base, patch = LAWFUL_POOL[0]
    touched = patch["ops"][0]["slot"].partition("/")[0]
    for garbage in ("NOT_A_CODE", "", "delta_slot_conflict", "UNRESOLVED_VALUE:", "OP_NOT_ACCREDITED:NO_SUCH_DIM",
                    "UNRESOLVED_VALUE:topic", "BASE_INVALID:X", "SMMLV_UNSUPPORTED", "OPERATION_CHANGE:",
                    "UNSUPPORTED_PROPOSAL:", ":TOPIC"):
        assert {v["rule"] for v in check(base, patch, None, refusal=garbage)} == {"T-REFUSAL-CODE"}, garbage
    untouched = untouched_dimension_code(base, patch)
    for code in (untouched, untouched.replace("OP_NOT_ACCREDITED", "UNRESOLVED_VALUE")):
        assert {v["rule"] for v in check(base, patch, None, refusal=code)} == {"T-REFUSAL-UNJUSTIFIED"}, code
    assert check(base, patch, None, refusal=f"OP_NOT_ACCREDITED:{touched}") == []
    for code in ("OPERATION_CHANGE:COUNT", "UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL", "STALE_OPTION", "BASE_INVALID"):
        assert check(base, patch, None, refusal=code) == [], code  # in the vocabulary: a host or protocol outcome, not judged
    value_op = next((e for e in LAWFUL_POOL if any(op["slot"].startswith("VALUE") for op in e[2]["ops"])), None)
    assert value_op and check(value_op[1], value_op[2], None, refusal="VALUE_WITHOUT_COMPARATOR") == []
    other = next(e for e in LAWFUL_POOL if not any(op["slot"].startswith("VALUE") for op in e[2]["ops"]))
    assert {v["rule"] for v in check(other[1], other[2], None, refusal="VALUE_WITHOUT_COMPARATOR")} == {"T-REFUSAL-UNJUSTIFIED"}


def test_the_f16_inversion_must_happen_the_positive_cannot_survive_next_to_its_negation():
    """ZEUS tx1: the single carried +X kept next to the placed -X is a violation of T-COND-F16; the faithful inversion is not."""
    for name, (base, patch) in (("f16_same_value", HAND_BY_NAME["f16_same_value"]),):
        effective = probe_apply(base, patch, dataset_of=host_dataset)
        assert check(base, patch, effective, dataset_of=host_dataset) == []
        kept = m_f16_positive_kept(base, patch, effective)
        assert kept is not None and {v["rule"] for v in check(base, patch, kept, dataset_of=host_dataset)} == {"T-COND-F16"}
    base, patch = enrich(*hand("b", [BUYER], [set_op(BUYER_A_NEG)])[1], 5)  # the same for another polarized dimension
    effective = probe_apply(base, patch)
    assert check(base, patch, effective) == []
    assert {v["rule"] for v in check(base, patch, m_f16_positive_kept(base, patch, effective))} == {"T-COND-F16"}
    base, patch = enrich(*hand("c", [TOPIC_A, TOPIC_C], [set_op(TOPIC_B_NEG)])[1], 5)  # another value: nothing to invert
    assert m_f16_positive_kept(base, patch, probe_apply(base, patch)) is None


def test_an_effective_and_a_refusal_together_are_a_misuse():
    label, base, patch = LAWFUL_POOL[0]
    with pytest.raises(ValueError, match="not both"):
        check(base, patch, probe_apply(base, patch), refusal="DELTA_SLOT_CONFLICT")


@FAST
@given(seed=st.integers(min_value=0, max_value=10**6), shuffle=st.integers(min_value=0, max_value=10**6))
def test_the_outcome_does_not_depend_on_the_order_of_the_ops_of_a_patch(seed, shuffle):
    """§6 step 2: «el desenlace no depende del orden de las operaciones dentro del parche»."""
    base, patch = enrich(*random_messy(seed), seed, smmlv=True)
    ops = list(patch["ops"])
    random.Random(shuffle).shuffle(ops)
    shuffled = {**patch, "ops": ops}
    assert transition.refusal_causes(base, patch) == transition.refusal_causes(base, shuffled)
    assert transition.expected_refusal(base, patch) == transition.expected_refusal(base, shuffled)
    if transition.expected_refusal(base, patch) is None:
        effective = probe_apply(base, patch)
        assert check(base, patch, effective) == check(base, shuffled, effective)


@FAST
@given(seed=st.integers(min_value=0, max_value=10**7))
def test_a_refusal_with_the_expected_code_is_lawful_and_an_effective_for_it_is_not_over_generated_messy_patches(seed):
    base, patch = enrich(*random_messy(seed), seed, smmlv=True)
    expected = transition.expected_refusal(base, patch)
    if expected is None:
        assert check(base, patch, probe_apply(base, patch, dataset_of=host_dataset)) is not None
        return
    assert check(base, patch, None, refusal=expected) == []
    assert [v["rule"] for v in check(base, patch, probe_apply(base, patch))] == ["T-REFUSAL"]


# ---- the oracle against the Gold and the contract ----------------------------------------------------------------------------
def gold_refusal_of(case):
    expected = case["expected"]
    code = expected["code"]
    if expected["disposition"] in ("CLARIFY", "INFORM") and code:
        if code in ("DELTA_SLOT_CONFLICT", "KEEP_ON_EMPTY_SLOT", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", "CLEAR_ON_EMPTY_SLOT"):
            return code
        if code.startswith("UNRESOLVED_VALUE:") or code == "VALUE_WITHOUT_COMPARATOR":
            return transition._published(code)
    return None


def test_the_contract_oracle_and_the_gold_agree_on_every_refusal_of_every_applicable_case():
    """Two independent derivations (the contract's mechanics; the adjudicated Gold) say the same for each case: a refusal
    cause the contract finds is exactly what the Gold expects, and where the Gold executes there is none."""
    refused_by_the_gold = 0
    for case in APPLICABLE:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        assert transition.expected_refusal(base, patch) == gold_refusal_of(case), case["gold_id"]
        refused_by_the_gold += gold_refusal_of(case) is not None
    assert refused_by_the_gold >= 6  # SF113 (H12), SF193/SF198/SF196 (KEEP), SF194 (F16), SF197 (conflict)


def test_the_gold_refusals_are_lawful_with_the_gold_code_and_the_new_cases_exercise_the_new_rules():
    by_id = {c["gold_id"]: c for c in APPLICABLE}
    for gid, want in {"SF193": "KEEP_ON_EMPTY_SLOT", "SF194": "OP_NOT_ACCREDITED:TOPIC", "SF196": "KEEP_ON_EMPTY_SLOT",
                      "SF197": "DELTA_SLOT_CONFLICT", "SF198": "KEEP_ON_EMPTY_SLOT", "SF113": "CLEAR_ON_EMPTY_SLOT"}.items():
        base, patch = by_id[gid]["inputs"]["base"], by_id[gid]["inputs"]["patch"]
        assert transition.expected_refusal(base, patch) == want, gid
        assert check(base, patch, None, refusal=by_id[gid]["expected"]["code"]) == [], gid
    sf196 = by_id["SF196"]["inputs"]
    assert _codes(transition.refusal_causes(sf196["base"], sf196["patch"])) == ["KEEP_ON_EMPTY_SLOT",
                                                                                "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"]


def test_the_harness_judges_the_refusals_of_the_gold_with_the_code_the_reducer_reports():
    report = oracle.audit_transitions(REFERENCE, CASES)
    assert report["ok"] and report["refusals_checked"] >= 6
    wrong = copy.deepcopy(REFERENCE)
    wrong["SF196"]["code"] = "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"  # the lower precedence first
    report = oracle.audit_transitions(wrong, CASES)
    assert not report["ok"] and {v["gold_id"] for v in report["violations"]} == {"SF196"}
    assert {v["rule"] for v in report["violations"]} == {"T-REFUSAL-CODE"}
    invented = copy.deepcopy(REFERENCE)
    victim = next(c["gold_id"] for c in APPLICABLE if c["expected"]["disposition"] == "EXECUTE")
    invented[victim].update(disposition="CLARIFY", code="DELTA_SLOT_CONFLICT", effective=None)
    report = oracle.audit_transitions(invented, CASES)
    assert {v["rule"] for v in report["violations"]} == {"T-REFUSAL-UNJUSTIFIED"} and report["violations"][0]["gold_id"] == victim


def test_the_constants_of_the_transition_oracle_are_the_sealed_contracts():
    text = contract_text.read()
    assert list(transition.PRECEDENCE) == [c.partition(":")[0] for c in contract_text.refusal_precedence(text)]
    assert transition.H12 == gold.host_rules()["H12"][1]
    assert set(transition.PRECEDENCE) <= {c.partition(":")[0] for c in contract_text.refusal_codes(text)}
    published = contract_text.refusal_table(text)["OP_NOT_ACCREDITED:<DIM>"]
    assert set(published) == {"UNRESOLVED_VALUE:<DIM>", "VALUE_WITHOUT_COMPARATOR"}
    rows = contract_text.table(contract_text.section(text, "### 4.2 Ranuras", "### 4.3 Identidad"), "| Ranura | Contenido |")
    names = {"TOPIC", "BUYER", "ACTOR", "GEOGRAPHY", "TIME", "STATE", "MODALITY", "CONTRACT_TYPE_OR_NATURE", "VALUE", "ORDER",
             "CARDINALITY", "UNIVERSE"}
    dims = []
    for row in rows:
        for d in re.findall(r"[A-Z][A-Z_]+", row[0]):
            if d in names and d not in dims:
                dims.append(d)
    expected = []
    for d in dims:
        expected += {"VALUE": ["VALUE/lower", "VALUE/upper"], "ORDER": ["ORDER"], "CARDINALITY": ["CARDINALITY"],
                     "UNIVERSE": ["UNIVERSE"]}.get(d, [f"{d}/POSITIVE", f"{d}/NEGATIVE"])
    assert list(transition.SLOT_ORDER) == expected


def test_the_polarized_dimensions_and_the_comparable_units_are_the_contracts():
    polarized = {s.partition("/")[0] for s in transition.SLOT_ORDER if s.endswith("/POSITIVE")}
    assert set(transition.POLARIZED) == polarized == set(typing_.POLARIZED)
    scale, single = gold.contract_comparable_units()
    assert set(transition.PESOS_SCALE) == scale == set(typing_.UNIT_SCALE) and single == "SMMLV"
    assert scale == {"PESOS", "MILES", "MILLONES", "MIL_MILLONES", "BILLONES"}


def test_a_polarized_dimension_or_a_unit_that_no_longer_matches_the_contract_fails_the_build(monkeypatch):
    monkeypatch.setattr(transition, "POLARIZED", frozenset(transition.POLARIZED - {"STATE"}))
    with pytest.raises(oracle.OracleError, match="polarized dimensions"):
        oracle.load_cases()
    monkeypatch.undo()
    monkeypatch.setattr(transition, "PESOS_SCALE", frozenset(transition.PESOS_SCALE | {"SMMLV"}))
    with pytest.raises(oracle.OracleError, match="comparable units"):
        oracle.load_cases()


def test_a_constant_that_no_longer_matches_the_contract_fails_the_build(monkeypatch):
    monkeypatch.setattr(transition, "PRECEDENCE", tuple(reversed(transition.PRECEDENCE)))
    with pytest.raises(oracle.OracleError, match="precedence"):
        oracle.load_cases()
    monkeypatch.undo()
    monkeypatch.setattr(transition, "SLOT_ORDER", tuple(reversed(transition.SLOT_ORDER)))
    with pytest.raises(oracle.OracleError, match="slot order"):
        oracle.load_cases()
    monkeypatch.undo()
    monkeypatch.setattr(transition, "H12", "ANOTHER_CODE")
    with pytest.raises(oracle.OracleError, match="H12"):
        oracle.load_cases()


@pytest.mark.parametrize("name", sorted(REFUSAL_CASES))
def test_each_refusal_mutant_is_an_effective_for_a_refused_patch(name):
    base, patch = enrich(*REFUSAL_CASES[name], 5)
    assert {v["rule"] for v in check(base, patch, probe_apply(base, patch))} == {"T-REFUSAL"}


def test_the_four_reducer_bugs_the_gold_cannot_discriminate_die_on_hand_built_inputs():
    """f16_any_value, set_clears_whole_dimension, clear_dimension_only_positive, replace_universe_keeps_old_kind."""
    for name, hand_name in (("f16_any_value", "f16_other_value"), ("set_clears_whole_dimension", "two_positives_and_a_negative_set"),
                            ("clear_dimension_only_positive", "clear_topic_with_both_polarities"),
                            ("redirect_keeps_old_kind", "redirect_same_subject_other_kind")):
        base, patch = HAND_BY_NAME[hand_name]
        effective = probe_apply(base, patch, dataset_of=host_dataset)
        mutated = MUTANTS[name][1](base, patch, copy.deepcopy(effective))
        assert mutated is not None and mutated != effective, name
        assert MUTANTS[name][0] in {v["rule"] for v in check(base, patch, mutated)}, name


def test_a_declared_substitution_is_checked_on_the_values_never_taken_on_trust():
    base, patch = HAND_BY_NAME["a81_collision"]
    ok = probe_apply(base, patch)
    assert check(base, patch, ok) == []
    assert {c["slot"] for c in ok["conditions"]} == {"BUYER/POSITIVE", "VALUE/upper"}  # A8.1: the lower bound was replaced
    base, patch = HAND_BY_NAME["a81_no_collision"]
    ok = probe_apply(base, patch)
    assert {c["slot"] for c in ok["conditions"]} == {"BUYER/POSITIVE", "VALUE/lower", "VALUE/upper"}
    assert check(base, patch, ok) == []


# ---- harmless variations -----------------------------------------------------------------------------------------------
def harmless_variants(base, patch, effective):
    renamed = copy.deepcopy(effective)
    for c in renamed["conditions"]:
        if c["condition_id"].startswith("new-"):
            c["condition_id"] = "reducer-" + c["condition_id"]  # placed ids are the reducer's
    reordered = copy.deepcopy(effective)
    reordered["conditions"].reverse()
    traces = copy.deepcopy(effective)
    traces.update(base_trace_id="elsewhere", root_trace_id="root-is-informative")
    policies = copy.deepcopy(effective)
    policies["policies"] = [{"policy_id": "p", "version": "1", "condition_ids": [effective["conditions"][0]["condition_id"]],
                             "effect": "EQUIVALENT"}] if effective["conditions"] else []
    return [renamed, reordered, traces, policies]


def test_what_the_contract_leaves_to_the_reducer_does_not_change_the_verdict():
    for label, base, patch in POOL:
        effective = faithful(base, patch)
        for variant in harmless_variants(base, patch, effective):
            assert check(base, patch, variant) == [], label


# ---- the attacks of the independent review, as regressions ------------------------------------------------------------------------
def reference_of(case):
    return REFERENCE[case["gold_id"]]["effective"]


def test_attack_w1_stale_dataset_after_replace_universe_dies_on_every_gold_case_that_redirects():
    hit = 0
    for case in COMPOSED:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        if redirect_of(patch) is None:
            continue
        effective = reference_of(case)
        old = next(r for r in base["requests"] if r["request_id"] == effective["requests"][0]["request_id"])
        if old["subject"] == effective["requests"][0]["subject"]:
            continue
        mutated = copy.deepcopy(effective)
        mutated["requests"][0]["dataset"] = old["dataset"]
        assert "T-REQ-DATASET" in {v["rule"] for v in check(base, patch, mutated)}, case["gold_id"]
        hit += 1
    assert hit >= 1


def test_attack_w2_universe_origin_of_the_anchor_after_replace_universe_dies():
    hit = 0
    for case in COMPOSED:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        if redirect_of(patch) is None:
            continue
        mutated = copy.deepcopy(reference_of(case))
        mutated["requests"][0]["universe_origin"] = copy.deepcopy(base["requests"][0]["universe_origin"])
        assert "T-REQ-ORIGIN" in {v["rule"] for v in check(base, patch, mutated)}, case["gold_id"]
        hit += 1
    assert hit >= 5


def test_attack_w3_w4_an_untouched_request_reoriginated_or_given_another_dataset_dies_on_every_case():
    hit = 0
    for case in COMPOSED:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        if redirect_of(patch) is not None:
            continue
        for field, value in (("universe_origin", copy.deepcopy(patch["ops"][0]["origin"])), ("dataset", "some.other.dataset")):
            mutated = copy.deepcopy(reference_of(case))
            mutated["requests"][0][field] = value
            assert {v["rule"] for v in check(base, patch, mutated)} == {"T-REQ-UNTOUCHED"}, (case["gold_id"], field)
        hit += 1
    assert hit >= 50


def test_attack_w5_a_dangling_effective_policy_dies_on_every_case():
    for case in COMPOSED:
        mutated = copy.deepcopy(reference_of(case))
        mutated["policies"] = [{"policy": "topn-default@1"}]
        assert "T-STRUCT" in {v["rule"] for v in check(case["inputs"]["base"], case["inputs"]["patch"], mutated)}


def test_attack_w6_carried_conditions_rebuilt_with_defaults_are_invisible_on_the_golds_bases_and_die_on_rich_ones():
    seen_blind = seen_caught = 0
    for case in COMPOSED:
        base, patch = case["inputs"]["base"], case["inputs"]["patch"]
        effective = reference_of(case)
        rebuilt_effective = reset_host_fields(base, patch, effective)
        if rebuilt_effective is not None and rebuilt_effective == effective:
            seen_blind += 1  # the Gold's base already holds the defaults: nothing to see (LG-BASES-WITHOUT-HOST-FIELDS)
        rich_base, rich_patch = enrich(base, patch, 11)
        rich_effective = probe_apply(rich_base, rich_patch)
        mutated = reset_host_fields(rich_base, rich_patch, rich_effective)
        if mutated is not None and mutated != rich_effective:
            assert "T-COND-CARRIED" in {v["rule"] for v in check(rich_base, rich_patch, mutated)}, case["gold_id"]
            seen_caught += 1
    assert seen_blind >= 50 and seen_caught >= 50


def test_the_dataset_value_is_exact_with_the_host_table_and_relational_without_it():
    base, patch = HAND_BY_NAME["redirect_new_subject_and_kind"]
    effective = probe_apply(base, patch, dataset_of=host_dataset)
    garbage = copy.deepcopy(effective)
    garbage["requests"][0]["dataset"] = "garbage.dataset"
    assert check(base, patch, garbage) == []  # relational: neither the old one nor another subject's (LG-DATASET-VALUE)
    assert {v["rule"] for v in check(base, patch, garbage, dataset_of=host_dataset)} == {"T-REQ-DATASET"}


# ====================================================================================================================
# THE TWO ORACLES ARE INDEPENDENT
# ====================================================================================================================
def test_what_the_gold_oracle_cannot_see_the_transition_oracle_catches_and_the_other_way_round():
    case = next(c for c in COMPOSED if redirect_of(c["inputs"]["patch"]) is None and carried_of(
        c["inputs"]["base"], c["inputs"]["patch"], reference_of(c)))
    base, patch = case["inputs"]["base"], case["inputs"]["patch"]
    gid = case["gold_id"]
    # identity falsified, meaning intact: the Gold oracle passes, the transition oracle fails
    tampered = tamper("carried", "origin", {"trace_id": "t-tampered"})(base, patch, reference_of(case))
    actuals = {**REFERENCE, gid: {**REFERENCE[gid], "effective": tampered}}
    assert oracle.audit({gid: actuals[gid]}, [case])["ok"]
    assert not oracle.audit_transitions({gid: actuals[gid]}, [case])["ok"]
    # meaning falsified, identity of everything else intact: the Gold oracle fails
    wrong = copy.deepcopy(reference_of(case))
    wrong["conditions"].pop()
    assert not oracle.audit({gid: {**REFERENCE[gid], "effective": wrong}}, [case])["ok"]


def test_the_harness_runs_the_contract_oracle_over_every_composed_case_of_the_gold():
    report = oracle.audit_transitions(REFERENCE, CASES)
    assert report["ok"] and report["checked"] == len(COMPOSED) and report["violations"] == []
    refused = {g for g, a in REFERENCE.items() if a["disposition"] in ("CLARIFY", "INFORM", "REASK")}
    assert report["refusals_checked"] == len(refused)
    assert set(report["not_checked"]) == {c["gold_id"] for c in APPLICABLE} - {c["gold_id"] for c in COMPOSED} - refused
    broken = copy.deepcopy(REFERENCE)
    victim = next(c["gold_id"] for c in COMPOSED if len(broken[c["gold_id"]]["effective"]["conditions"]) > 1)
    broken[victim]["effective"]["conditions"][0]["authority"] = "tampered@9"
    report = oracle.audit_transitions(broken, CASES)
    assert not report["ok"] and {v["gold_id"] for v in report["violations"]} == {victim}


def test_the_manifest_declares_the_transition_rules_with_their_clause_and_killing_mutants():
    declared = oracle.declarations()["transition_rules"]
    assert [r["id"] for r in declared] == [r["id"] for r in transition.TRANSITION_RULES]
    assert all(r["clause"] and r["killed_by"] for r in declared)
