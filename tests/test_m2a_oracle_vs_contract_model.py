"""M2A-8a0: the transition oracle against the contract lane's own executable model (a differential, not a dependency).

The sealed contract ships a SYMBOLIC model of §6 (``tests/test_m2a_contract_r3_multiop.py``: ``causes`` for the refusal causes of
step 3b with their closed precedence and §4.2 slot order, ``apply`` for the composition of a patch without causes, with the
declared substitutions F16 and A8.1). It is written by the contract lane from the contract text and approved with it. The
oracle's ``refusal_causes`` and the faithful application used to test it are written from the SAME text, separately, over
dicts. Here both are fed the same generated (base, patch) and must agree on:

* the refusal causes, in order (so the closed precedence, the slot order, F16's «several positives», P-3, §15.2's
  incomparable units, KEEP with SET/CLEAR of one slot, and the CARDINALITY-without-ORDER state rule agree);
* the composed slot contents when there is no cause (so what the oracle accepts as a faithful effective is what the model
  composes).

The model's own abstractions are respected: ops flagged ``unresolved`` (an AUTHORITY's refusal: not writable in a
``m2a-patch/1``), the host questions, the reserved labels and a universe of ``opportunities`` (a §19 COMPOSICION row) are
outside the dict-level oracle and are not generated; ``TARGET_NOT_ADDRESSABLE`` needs a base with a ``logic`` expression and
is not generated either (declared in the oracle's docstring and its limited guarantees).

This file imports the model as a module of the repo; it imports no M2a module and not the reducer.
"""

from __future__ import annotations

import random

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import tests.test_m2a_contract_r3_multiop as model
from scripts.audits.m2a_conversation import m2a_oracle_transition as transition

FAST = settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
TOKENS = ("x", "y", "w")
AMOUNT = 1_000_000
SUPPORTED_STATUS = {"ok", "crosses", "noncomparable", "state_missing"}
DIMENSION_OF = {k: k.partition("/")[0] for k in model.CONDITION_SLOTS}


def supported(op) -> bool:
    return (op.kind in ("SET", "SET_ORDER", "CLEAR", "KEEP", "REPLACE_UNIVERSE") and op.status in SUPPORTED_STATUS
            and not (op.kind == "REPLACE_UNIVERSE" and op.value == "opportunities"))


OPS = [op for op in model.OPS if supported(op)]


def condition(key: str, token: str, cid: str, *, unit: str = "MILLONES", amount: int = AMOUNT) -> dict:
    dimension, _, side = key.partition("/")
    negative = side == "NEGATIVE"
    if dimension == "VALUE":
        value = ["money@1", f"amount_cop={amount}", f"comparator={'>=' if side == 'lower' else '<='}", f"token={token}"]
    elif dimension == "ORDER":
        value = ["order@1", f"key={token}", "direction=asc", f"token={token}"]
    elif dimension == "CARDINALITY":
        value = ["cardinality@2", "n=10", f"token={token}"]
    else:
        value = ["v@1", f"phrase={token}", f"excluded={int(negative)}", f"token={token}"]
    return {"condition_id": cid, "request_ids": ["r1"], "slot": key, "dimension": dimension, "operator": "is",
            "polarity": "NEGATIVE" if negative else "POSITIVE", "level": None, "value": value, "authority": "model@1",
            "receipt_rules": [], "policies": [], "origin": {"kind": "SPAN", "trace_id": "t0", "raw_sha256": "0" * 64,
                                                             "span": [0, 1], "event_id": None},
            "provenance": {"unit": unit if dimension == "VALUE" else None, "source_amount": None, "unit_source": None,
                           "year": None, "year_source": None, "buyer_scope": None, "order_direction_source": None,
                           "inherited_from": None}}


def dicts_of(state, ops) -> tuple[dict, dict]:
    request = {"request_id": "r1", "scope_id": "s1", "subject": "contracts", "universe_kind": "records",
               "universe_origin": {"kind": "SPAN", "trace_id": "t0", "raw_sha256": "0" * 64, "span": [0, 1], "event_id": None},
               "view": None, "dataset": "d.contracts"}
    conditions = [condition(key, token, f"c-{key}-{n}") for key in model.CONDITION_SLOTS
                  for n, token in enumerate(state.slots[model.IDX[key]])]
    base = {"schema": "m2a-accepted/1", "requests": [request], "conditions": conditions, "logic": None, "policies": [],
            "base_trace_id": None, "root_trace_id": "t0"}
    origin = {"kind": "SPAN", "trace_id": "t1", "raw_sha256": "1" * 64, "span": [0, 1], "event_id": None}
    patch_ops = []
    for n, op in enumerate(ops):
        entry = {"op": op.kind, "slot": op.slot, "condition": None, "universe": None, "origin": origin}
        if op.kind in ("SET", "SET_ORDER"):
            side = op.slot.partition("/")[2]
            amount = {("lower", "ok"): 100_000, ("lower", "crosses"): 2 * AMOUNT, ("upper", "ok"): 5 * AMOUNT,
                      ("upper", "crosses"): AMOUNT // 2}.get((side, op.status), AMOUNT)
            entry["condition"] = condition(op.slot, op.value, f"p{n}", amount=amount,
                                           unit="SMMLV" if op.status == "noncomparable" else "MILLONES")
        elif op.kind == "REPLACE_UNIVERSE":
            entry["universe"] = {"subject": op.value, "universe_kind": "records"}
        patch_ops.append(entry)
    return base, {"schema": "m2a-patch/1", "ops": patch_ops, "origin_kind": "TEXT"}


def random_case(seed: int):
    rng = random.Random(seed)
    slots = []
    for key in model.CONDITION_SLOTS:
        dimension = key.partition("/")[0]
        if dimension in ("ORDER", "CARDINALITY") or dimension == "VALUE":
            count = rng.choice([0, 0, 1])
        else:
            count = rng.choice([0, 0, 1, 1, 2])
        slots.append(tuple(sorted(rng.sample(TOKENS, count))) if count else ())
    state = model.State(tuple(slots), frozenset(), ("contracts",))
    if rng.random() < 0.5:  # keep ORDER and CARDINALITY a coherent pair most of the time
        slots = list(state.slots)
        if slots[model.IDX["CARDINALITY"]] and not slots[model.IDX["ORDER"]]:
            slots[model.IDX["ORDER"]] = ("date",)
        state = state._replace(slots=tuple(slots))
    ops = rng.sample(OPS, rng.randint(1, 4))
    return state, tuple(ops)


def model_codes(state, ops) -> list[str]:
    return [c.code.replace("UNRESOLVED_VALUE:", "OP_NOT_ACCREDITED:") if c.code.startswith("UNRESOLVED_VALUE:") else c.code
            for c in model.causes(state, ops)]


def test_the_operations_the_model_generates_are_the_ones_the_oracle_can_read():
    assert len(OPS) > 60
    assert {op.kind for op in OPS} == {"SET", "SET_ORDER", "CLEAR", "KEEP", "REPLACE_UNIVERSE"}
    assert {op.status for op in OPS} == SUPPORTED_STATUS


@pytest.mark.parametrize("block", range(8))
def test_the_oracle_and_the_contract_model_agree_on_every_refusal_cause_and_its_order(block):
    causes_seen: dict[str, int] = {}
    for seed in range(block * 1500, (block + 1) * 1500):
        state, ops = random_case(seed)
        base, patch = dicts_of(state, ops)
        mine = [c["code"] for c in transition.refusal_causes(base, patch)]
        theirs = model_codes(state, ops)
        assert mine == theirs, (seed, ops, mine, theirs)
        for code in mine:
            causes_seen[code] = causes_seen.get(code, 0) + 1
    assert {"DELTA_SLOT_CONFLICT", "KEEP_ON_EMPTY_SLOT", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"} <= set(causes_seen)
    assert any(code.startswith("OP_NOT_ACCREDITED:") for code in causes_seen)


@FAST
@given(seed=st.integers(min_value=0, max_value=10**7))
def test_the_closed_precedence_agrees_with_the_model_over_generated_patches(seed):
    state, ops = random_case(seed)
    base, patch = dicts_of(state, ops)
    mine = transition.expected_refusal(base, patch)
    theirs = model_codes(state, ops)
    assert (mine if mine != transition.H12 else None) == (theirs[0] if theirs else None) or (
        mine == transition.H12 and not theirs)


def slots_of(effective) -> dict[str, tuple[str, ...]]:
    out: dict[str, list[str]] = {}
    for c in effective["conditions"]:
        token = next(i.partition("=")[2] for i in c["value"] if i.startswith("token="))
        out.setdefault(c["slot"], []).append(token)
    return {k: tuple(sorted(v)) for k, v in out.items()}


def faithful_application(base: dict, patch: dict) -> dict:
    """The oracle's tests' faithful application (§5.2, §6.1-§6.3, §6.5) at the level of this file's dicts."""
    import copy
    ops = patch["ops"]

    def covers(written, key):
        return key == written or (written in ("TOPIC", "BUYER", "ACTOR", "GEOGRAPHY", "TIME", "STATE", "MODALITY",
                                              "CONTRACT_TYPE_OR_NATURE") and key.startswith(written + "/")) or (
            written == "VALUE" and key.startswith("VALUE/"))

    conditions = copy.deepcopy(base["conditions"])
    carried = {c["condition_id"] for c in conditions}
    for op in ops:
        if op["op"] == "CLEAR":
            conditions = [c for c in conditions if not covers(op["slot"], c["slot"])]
    for op in (o for o in ops if o["op"] in ("SET", "SET_ORDER")):
        new = copy.deepcopy(op["condition"])
        slot = new["slot"]
        conditions = [c for c in conditions if c["slot"] != slot]
        if slot.endswith("/NEGATIVE"):
            positive = slot.replace("/NEGATIVE", "/POSITIVE")
            single = [c for c in conditions if c["slot"] == positive and c["condition_id"] in carried]
            if len(single) == 1 and single[0]["value"][1:-2] == new["value"][1:-2]:
                conditions = [c for c in conditions if c is not single[0]]
        if slot.startswith("VALUE/"):
            other = "VALUE/upper" if slot == "VALUE/lower" else "VALUE/lower"
            opposite = [c for c in conditions if c["slot"] == other and c["condition_id"] in carried]
            if opposite:
                amount = lambda c: int(next(i.partition("=")[2] for i in c["value"] if i.startswith("amount_cop=")))  # noqa: E731
                lo, hi = (amount(new), amount(opposite[0])) if slot == "VALUE/lower" else (amount(opposite[0]), amount(new))
                if lo >= hi:
                    conditions = [c for c in conditions if c is not opposite[0]]
        conditions.append(new)
    return {"conditions": conditions}


@pytest.mark.parametrize("block", range(8))
def test_the_faithful_application_composes_the_same_slots_as_the_model_when_there_is_no_cause(block):
    compared = 0
    for seed in range(block * 1500, (block + 1) * 1500):
        state, ops = random_case(seed)
        if model.causes(state, ops):
            continue
        base, patch = dicts_of(state, ops)
        assert not transition.refusal_causes(base, patch)
        composed = model.apply(state, ops)
        mine = slots_of(faithful_application(base, patch))
        theirs = {key: tuple(sorted(composed.slots[model.IDX[key]])) for key in model.CONDITION_SLOTS
                  if composed.slots[model.IDX[key]]}
        assert mine == theirs, (seed, ops, mine, theirs)
        compared += 1
    assert compared > 300
