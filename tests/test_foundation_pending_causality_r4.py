"""R4 causal witnesses: approved C1/C3/C5/C6, strict v2; no contextual C2 admission or echo certification."""
from __future__ import annotations

import copy
import itertools
import json
from collections import Counter
from dataclasses import replace
from datetime import timedelta

import pytest
from hypothesis import given, settings, strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
from app.core.foundation_authority_values import Comparator, MoneyValue
from app.core.interpretation_contract import ContractError, Polarity
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_foundation_accepted_contract import T0, VALUES, condition, interpretation, origin, record, st_patch


def set_op(dim=D.BUYER, *, cid="asked", start=0):
    c = condition(fa, dim, cid=cid, org=origin(start=start))
    return fa.PatchOp(fa.PatchOpKind.SET, c.slot, c, None, c.origin)


def clear_op(slot, *, start=0):
    return fa.PatchOp(fa.PatchOpKind.CLEAR, slot, None, None, origin(start=start))


def pending(*, frame_ops, asked_ops, frame=None, base_trace_id=None):
    return fa.PendingClarification("p1", "t1", T0, T0 + timedelta(minutes=30), 1, frame, base_trace_id,
                                   None, None, "BUYER_OR_PLACE_AFTER_ACTOR", (), frozenset(),
                                   frame_ops=frame_ops, asked_ops=asked_ops)


def identity(op):
    # Independent oracle from the approved C3 formula, not the production helper.
    payload = (tuple(op.condition.value) if op.condition is not None else
               (op.universe.subject.value, op.universe.universe_kind.value) if op.universe is not None else ())
    return op.op.value, op.slot.key, payload


def key(op):
    return identity(op), json.dumps(op.to_dict(), sort_keys=True, ensure_ascii=False, allow_nan=False)


def read_again(item):
    return fa.PendingClarification.from_dict(json.loads(json.dumps(item.to_dict())))


@settings(max_examples=100, deadline=None)
@given(st_patch())
def test_fresh_preserves_nonempty_original_asking_input_without_frame_ops(patch):
    item = pending(frame_ops=(), asked_ops=patch.ops)
    again = read_again(item)
    assert again.base_trace_id is None and again.frame_ops == ()
    assert Counter(json.dumps(o.to_dict(), sort_keys=True) for o in again.asked_ops) == Counter(
        json.dumps(o.to_dict(), sort_keys=True) for o in patch.ops)


def test_no_base_refuses_frame_ops_even_when_they_belong_to_asked_ops():
    op = set_op()
    with pytest.raises(ContractError, match="PENDING_FRAME_OPS_WITHOUT_BASE"):
        pending(frame_ops=(op,), asked_ops=(op,))


def test_constructor_requires_both_witnesses_explicitly():
    data = dict(pending_id="p", asked_trace_id="t", created_at_utc=T0,
                expires_at_utc=T0 + timedelta(minutes=1), asks=1, frame=None, base_trace_id=None,
                ambiguous_span=None, ambiguous_raw_sha256=None, reason="NO_ANCHOR",
                candidate_patches=(), answer_slots=frozenset())
    for supplied in ({}, {"frame_ops": ()}, {"asked_ops": ()}):
        with pytest.raises(TypeError):
            fa.PendingClarification(**data, **supplied)


def test_explicit_empty_asking_input_is_a_fact_not_an_omission():
    item = pending(frame_ops=(), asked_ops=())
    assert read_again(item) == item
    assert item.to_dict()["frame_ops"] == item.to_dict()["asked_ops"] == []


def test_submultiset_uses_semantics_not_dataclass_or_python_identity():
    op = set_op()
    clone = replace(op, condition=replace(op.condition, condition_id="different-id"))
    assert clone is not op and clone != op and identity(clone) == identity(op)
    item = pending(frame_ops=(clone,), asked_ops=(op,), frame=interpretation(), base_trace_id="t0")
    assert read_again(item).frame_ops == (clone,)


@pytest.mark.parametrize("frame_ops,asked_ops", [
    ((set_op(),), (set_op(D.GEOGRAPHY),)),
    ((set_op(), set_op()), (set_op(),)),
    ((clear_op(fa.Slot(D.BUYER)),), (clear_op(fa.Slot(D.BUYER, Polarity.POSITIVE)),)),
])
def test_foreign_operation_missing_multiplicity_and_wide_narrow_mismatch_fail_closed(frame_ops, asked_ops):
    with pytest.raises(ContractError, match="PENDING_FRAME_OPS_OUTSIDE_ASKED_OPS"):
        pending(frame_ops=frame_ops, asked_ops=asked_ops, frame=interpretation(), base_trace_id="t0")


@settings(max_examples=100, deadline=None)
@given(st_patch())
def test_every_permutation_has_one_persisted_multibag_and_digest(patch):
    expected = tuple(sorted(patch.ops, key=key))
    payloads, hashes = set(), set()
    for ops in itertools.permutations(patch.ops):
        item = pending(frame_ops=ops, asked_ops=ops, frame=interpretation(), base_trace_id="t0")
        assert item.frame_ops == item.asked_ops == expected
        assert read_again(item) == item
        payloads.add(json.dumps(item.to_dict(), sort_keys=True))
        hashes.add(fa.pending_sha256(item))
    assert len(payloads) == len(hashes) == 1


def test_full_provenance_tiebreak_and_duplicate_witnesses_are_preserved():
    a = set_op(cid="z", start=4)
    b = set_op(cid="a", start=8)
    assert identity(a) == identity(b)
    ops = (a, b, a)
    item = pending(frame_ops=(), asked_ops=ops)
    assert item.asked_ops == tuple(sorted(ops, key=key))
    assert len(item.asked_ops) == 3
    assert read_again(item).asked_ops == item.asked_ops
    assert fa.pending_sha256(item) != fa.pending_sha256(pending(frame_ops=(), asked_ops=(a, b)))


@pytest.mark.parametrize("field", ["frame_ops", "asked_ops"])
def test_reader_rejects_noncanonical_order_before_constructor_normalization(field):
    ops = (set_op(), set_op(D.TIME))
    item = pending(frame_ops=ops, asked_ops=ops, frame=interpretation(), base_trace_id="t0")
    data = item.to_dict()
    data[field].reverse()
    before = copy.deepcopy(data)
    with pytest.raises(ContractError, match="NONCANONICAL_PENDING_" + field.upper()):
        fa.PendingClarification.from_dict(data)
    assert data == before


@pytest.mark.parametrize("change", [
    lambda d: d.update(schema="m2a-pending/1"),
    lambda d: d.update(schema="m2a-pending/unknown"),
    lambda d: d.pop("frame_ops"),
    lambda d: d.pop("asked_ops"),
    lambda d: d.update(effects=["POLARITY_INVERTED"]),
    lambda d: d.update(raw_query="previous turn"),
    lambda d: d.update(asked_ops=["SET BUYER"]),
    lambda d: d.update(asked_ops=[{"op": "legacy"}]),
    lambda d: d.update(frame_ops=None),
    lambda d: d.update(asked_ops={}),
    lambda d: d["asked_ops"][0].update(effects=True),
])
def test_strict_reader_rejects_v1_omissions_effects_raw_legacy_and_bad_members(change):
    data = pending(frame_ops=(), asked_ops=(set_op(),)).to_dict()
    change(data)
    with pytest.raises(ContractError):
        fa.PendingClarification.from_dict(data)


@pytest.mark.parametrize("bad", [("raw",), [set_op()], (set_op().to_dict(),)])
def test_constructor_rejects_untyped_witness_members(bad):
    with pytest.raises(ContractError):
        pending(frame_ops=(), asked_ops=bad)


def test_reader_enforces_local_containment_and_no_base_law():
    op = set_op()
    data = pending(frame_ops=(op,), asked_ops=(op,), frame=interpretation(), base_trace_id="t0").to_dict()
    data["asked_ops"] = []
    with pytest.raises(ContractError, match="PENDING_FRAME_OPS_OUTSIDE_ASKED_OPS"):
        fa.PendingClarification.from_dict(data)
    data["asked_ops"] = [op.to_dict()]
    data["base_trace_id"] = None
    with pytest.raises(ContractError, match="PENDING_FRAME_OPS_WITHOUT_BASE"):
        fa.PendingClarification.from_dict(data)


def test_clear_set_commutes_and_duplicate_clear_keeps_existing_reducer_law():
    base = interpretation(conditions=(condition(fa, D.BUYER),))
    clear = clear_op(fa.Slot(D.BUYER, Polarity.POSITIVE))
    setting = set_op(cid="new")
    ops = (setting, clear, clear)
    results = [fd.compose_ops(base, permutation, base_trace_id="t0") for permutation in itertools.permutations(ops)]
    assert all(isinstance(result, fd.Composed) for result in results)
    assert len({json.dumps(result.effective.to_dict(), sort_keys=True) for result in results}) == 1
    item = pending(frame_ops=ops, asked_ops=ops, frame=results[0].effective, base_trace_id="t0")
    assert len(read_again(item).frame_ops) == 3
    conflict = fd.compose_ops(base, (setting, setting), base_trace_id="t0")
    assert conflict.code == "DELTA_SLOT_CONFLICT"
    asked = pending(frame_ops=(), asked_ops=(setting, setting), frame=base, base_trace_id="t0")
    assert len(read_again(asked).asked_ops) == 2


def test_record1_contains_pending2_and_digest_covers_each_witness():
    original = record(resolution=fa.TurnResolution.CLARIFY, with_patch=False, with_pending=True)
    data = original.to_dict()
    assert data["schema"] == "m2a-record/1" and data["pending"]["schema"] == "m2a-pending/2"
    assert fa.AcceptedTurnRecord.from_dict(data) == original
    data["pending"]["schema"] = "m2a-pending/1"
    with pytest.raises(ContractError):
        fa.AcceptedTurnRecord.from_dict(data)
    op = set_op()
    first = pending(frame_ops=(), asked_ops=(op,), frame=original.pending.frame, base_trace_id="t0")
    second = replace(first, frame_ops=(op,))
    third = replace(first, asked_ops=(op, op))
    assert len({fa.pending_sha256(p) for p in (first, second, third)}) == 3
    assert len({replace(original, pending=p).record_sha256 for p in (first, second, third)}) == 3


def test_f16_histories_same_frame_and_same_final_effective_remain_distinct_after_roundtrip():
    positive = condition(fa, D.BUYER, cid="old")
    # New original negative operation: same accredited buyer, opposite polarity.
    encoded = replace(VALUES[D.BUYER][0], excluded=True).encode()
    negative = replace(positive, condition_id="new", value=encoded, polarity=Polarity.NEGATIVE,
                       slot=fa.slot_of(D.BUYER, encoded), origin=origin(start=4))
    setting = fa.PatchOp(fa.PatchOpKind.SET, negative.slot, negative, None, negative.origin)
    clearing = clear_op(positive.slot)
    base = interpretation(conditions=(positive,))
    a_ops, b_ops = (clearing, setting), (setting,)
    a = fd.compose_ops(base, a_ops, base_trace_id="t0")
    b = fd.compose_ops(base, b_ops, base_trace_id="t0")
    assert isinstance(a, fd.Composed) and isinstance(b, fd.Composed)
    assert a.effective.to_dict() == b.effective.to_dict()
    answer = set_op(D.TIME, cid="answer")
    final_a = fd.compose_ops(a.effective, (answer,), base_trace_id="t0")
    final_b = fd.compose_ops(b.effective, (answer,), base_trace_id="t0")
    assert final_a.effective.to_dict() == final_b.effective.to_dict()
    # Same asking multibag isolates the frame witness; no echo implementation is claimed.
    pa = pending(frame_ops=a_ops, asked_ops=a_ops, frame=a.effective, base_trace_id="t0")
    pb = pending(frame_ops=b_ops, asked_ops=a_ops, frame=b.effective, base_trace_id="t0")
    assert read_again(pa).frame_ops != read_again(pb).frame_ops
    assert fa.pending_sha256(pa) != fa.pending_sha256(pb)


def test_distinct_asking_inputs_same_frame_do_not_collapse():
    base = interpretation(conditions=(condition(fa, D.TOPIC),))
    buyer, geo = set_op(), set_op(D.GEOGRAPHY)
    a = pending(frame_ops=(), asked_ops=(buyer,), frame=base, base_trace_id="t0")
    b = pending(frame_ops=(), asked_ops=(geo,), frame=base, base_trace_id="t0")
    assert a.frame == b.frame and a.candidate_patches == b.candidate_patches
    assert read_again(a).asked_ops == (buyer,)
    assert read_again(b).asked_ops == (geo,)
    assert fa.pending_sha256(a) != fa.pending_sha256(b)


def test_a81_histories_same_frame_and_final_effective_keep_distinct_operations():
    lower = condition(fa, D.VALUE, 0, "old-lower")
    upper = condition(fa, D.VALUE, 2, "old-upper")
    base = interpretation(conditions=(lower, upper))
    value = MoneyValue(1_000_000_000, Comparator.GTE)
    new_lower = replace(lower, condition_id="new-lower", value=value.encode(), origin=origin(start=4))
    setting = fa.PatchOp(fa.PatchOpKind.SET, new_lower.slot, new_lower, None, new_lower.origin)
    clearing = clear_op(upper.slot)
    explicit_ops, a81_ops = (clearing, setting), (setting,)
    explicit = fd.compose_ops(base, explicit_ops, base_trace_id="t0")
    automatic = fd.compose_ops(base, a81_ops, base_trace_id="t0")
    assert isinstance(explicit, fd.Composed) and isinstance(automatic, fd.Composed)
    assert explicit.effective.to_dict() == automatic.effective.to_dict()
    answer = set_op(D.TIME, cid="answer")
    final_explicit = fd.compose_ops(explicit.effective, (answer,), base_trace_id="t0")
    final_automatic = fd.compose_ops(automatic.effective, (answer,), base_trace_id="t0")
    assert final_explicit.effective.to_dict() == final_automatic.effective.to_dict()
    pa = pending(frame_ops=explicit_ops, asked_ops=explicit_ops, frame=explicit.effective, base_trace_id="t0")
    pb = pending(frame_ops=a81_ops, asked_ops=explicit_ops, frame=automatic.effective, base_trace_id="t0")
    assert read_again(pa).frame_ops == tuple(sorted(explicit_ops, key=key))
    assert read_again(pb).frame_ops == a81_ops
    assert fa.pending_sha256(pa) != fa.pending_sha256(pb)
