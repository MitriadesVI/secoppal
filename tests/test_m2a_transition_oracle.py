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
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

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


def enrich(base: dict, patch: dict, seed: int) -> tuple[dict, dict]:
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
        if t["slot"] in taken:
            continue
        taken[t["slot"]] = True
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
    base, patch = enrich(*random_transition(seed), seed)
    if refused(base, patch):
        return
    for request in base["requests"]:
        request["dataset"] = host_dataset(request["subject"])
    effective = probe_apply(base, patch, dataset_of=host_dataset)
    assert check(base, patch, effective, dataset_of=host_dataset) == []
    assert check(base, patch, effective) == []  # the relational check agrees


GENERATED = []
for _seed in range(4000):
    _base, _patch = enrich(*random_transition(_seed), _seed)
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
    # F16 inverts the STANDING single positive only: a positive the same patch places is never "vigente" (found by
    # Hypothesis, seed 88199: the faithful application used to displace the patch's own positive)
    hand("one_patch_sets_both_polarities_of_one_value", [BUYER],
         [set_op(cond("TOPIC", "contains", "POSITIVE", "aseo", "p0", R, n=1)), set_op(TOPIC_A_NEG, 1)]),
    hand("one_patch_sets_both_polarities_over_the_standing_positive", [TOPIC_A],
         [set_op(cond("TOPIC", "contains", "POSITIVE", "aseo", "p0", R, n=1)), set_op(TOPIC_A_NEG, 1)]),
    hand("negative_first_then_positive_over_the_standing_positive", [TOPIC_A],
         [set_op(TOPIC_A_NEG, 1), set_op(cond("TOPIC", "contains", "POSITIVE", "aseo", "p0", R, n=1))]),
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
# THE REFUSALS
# ====================================================================================================================
CONFLICT_CASES = {
    "effective_for_slot_conflict": hand("c", [TOPIC_A], [set_op(NEW_TOPIC), set_op(cond("TOPIC", "contains", "POSITIVE", "otro", "p9", R))])[1],
    "effective_for_keep_on_empty_slot": hand("k", [TOPIC_A], [keep_op("BUYER/POSITIVE")])[1],
    "effective_for_clear_on_empty_slot": hand("e", [TOPIC_A], [clear_op("MODALITY")])[1],
    "effective_for_clear_order_leaving_cardinality": hand("o", [ORDER, CARD, BUYER], [clear_op("ORDER")])[1],
    "keep_and_clear_one_slot": hand("kc", [TOPIC_A], [keep_op("TOPIC/POSITIVE"), clear_op("TOPIC", 1)])[1],
    "two_replace_universe": hand("rr", [TOPIC_A], [redirect_op("processes", "records"), redirect_op("contracts", "records", 1)])[1],
}


@pytest.mark.parametrize("name", sorted(CONFLICT_CASES))
def test_a_patch_the_contract_refuses_must_not_produce_an_effective(name):
    base, patch = CONFLICT_CASES[name]
    base, patch = enrich(base, patch, 3)
    assert check(base, patch, None) == []
    violations = check(base, patch, probe_apply(base, patch))
    assert [v["rule"] for v in violations] == ["T-REFUSAL"]


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
    refusal = {n for n in CONFLICT_CASES if n.startswith("effective_for")}
    for rule, names in rules.items():
        expected = by_rule.get(rule, set()) | (refusal if rule == "T-REFUSAL" else set())
        assert names == expected, rule
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


@pytest.mark.parametrize("name", sorted(CONFLICT_CASES))
def test_each_refusal_mutant_is_an_effective_for_a_refused_patch(name):
    base, patch = enrich(*CONFLICT_CASES[name], 5)
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
    assert set(report["not_checked"]) == {c["gold_id"] for c in APPLICABLE} - {c["gold_id"] for c in COMPOSED}
    broken = copy.deepcopy(REFERENCE)
    victim = next(c["gold_id"] for c in COMPOSED if len(broken[c["gold_id"]]["effective"]["conditions"]) > 1)
    broken[victim]["effective"]["conditions"][0]["authority"] = "tampered@9"
    report = oracle.audit_transitions(broken, CASES)
    assert not report["ok"] and {v["gold_id"] for v in report["violations"]} == {victim}


def test_the_manifest_declares_the_transition_rules_with_their_clause_and_killing_mutants():
    declared = oracle.declarations()["transition_rules"]
    assert [r["id"] for r in declared] == [r["id"] for r in transition.TRANSITION_RULES]
    assert all(r["clause"] and r["killed_by"] for r in declared)
