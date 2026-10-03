"""R4 M2A4: original creation inputs → actual strict store → context-bound response. Independent causal controls."""
from __future__ import annotations

import inspect
import itertools
import json
import os
from collections import Counter
from dataclasses import fields, replace

import pytest
from hypothesis import given, settings, strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_accepted_store as store
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
import app.core.foundation_pending as pending_protocol
import app.core.foundation_pending_create as create
import tests.test_foundation_pending as T
import tests.test_m2a3_oracle_link as LINK
from app.core.foundation_authority_values import Comparator, MoneyValue
from app.core.interpretation_contract import ContractError
from app.core.semantic_dimensions import SemanticDimension as D


P = pending_protocol
PROPS = settings(max_examples=80, derandomize=True, deadline=None)


def month_question():
    return create.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", T._org(20), frozenset({T.TIME_POS}))


def asking(base, frame_ops, *, open_ops=()):
    # Explicit NEW synthetic inputs; every call declares both original typed subsets.
    return create.ask_authority(month_question(), base=base, base_trace_id="t0", asked_trace_id="t1", now=T.T0,
                                frame_ops=frame_ops, open_ops=open_ops)


def answer_op(dim=D.TIME, token="y"):
    op = T.set_op(dim, token, i=30)
    org = T.origin("t2", 0, "c" * 64)
    return replace(op, condition=replace(op.condition, origin=org), origin=org)


def answer_turn(*ops, **facts):
    return P.PendingTurn(T.facts(**facts), tuple(ops))


def reread(p):
    return fa.PendingClarification.from_dict(json.loads(json.dumps(p.to_dict())))


def persist_and_load(tmp_path, base, p):
    """Real append/read APIs with synthetic published records, entirely in pytest's tmp_path."""
    tmp_path.mkdir(exist_ok=True)
    location = store.AcceptedStoreLocation("chat-1", tmp_path / ("chat-1" + store.ENVELOPE_SUFFIX))
    if base is not None:
        anchor = fa.AcceptedTurnRecord("t0", "chat-1", "u0", T.T0, T.T0, "foundation_v5_1", T.RAW_BASE,
                                       fa.TurnScope.IN_NEW_SEARCH, fa.TurnResolution.NEW_SEARCH, None, None, None,
                                       base, None, None, None, (), "t0")
        store.append(location, anchor, epoch=0)
    record = fa.AcceptedTurnRecord("t1", "chat-1", "u1", T.T0, T.T0, "foundation_v5_1", T.RAW_TURN,
                                   fa.TurnScope.IN, fa.TurnResolution.CLARIFY, None, p.base_trace_id, None,
                                   None, None, None, p, (), "t0" if base is not None else None)
    store.append(location, record, epoch=0)
    loaded = store.read(location)
    assert loaded.status is store.StoreStatus.OK
    assert loaded.records[-1] == record
    if base is not None:
        accredited = store.anchor_of(loaded, {"t0", "t1"})
        assert accredited.status is store.AnchorStatus.ANCHOR
        assert accredited.record.trace_id == p.base_trace_id
        return loaded.records[-1].pending, accredited.record.effective, accredited.record.trace_id, location
    return loaded.records[-1].pending, None, None, location


def test_authority_creation_requires_both_original_typed_input_fields():
    base = T.base_of((D.TOPIC, "x"))
    kwargs = dict(base=base, base_trace_id="t0", asked_trace_id="t1", now=T.T0)
    for supplied in ({}, {"frame_ops": ()}, {"open_ops": ()}):
        with pytest.raises(TypeError):
            create.ask_authority(month_question(), **kwargs, **supplied)
    for name in ("frame_ops", "open_ops"):
        assert inspect.signature(create.ask_authority).parameters[name].default is inspect.Parameter.empty


def test_no_response_api_or_creation_result_offers_a_second_asking_source():
    assert "asked_ops" not in {f.name for f in fields(create.Creation)}
    import app.core.foundation_pending_answer as answers
    for function in (P.respond, answers.answer_in_language, answers.answer_by_event):
        assert not set(inspect.signature(function).parameters) & {"asked_ops", "no_asked_patch"}


def test_creation_store_reload_response_and_reask_keep_both_witnesses(tmp_path):
    base = T.base_of((D.TOPIC, "x"), (D.BUYER, "x"))
    original = (T.set_op(D.BUYER, "y", i=1),)
    made = asking(base, original)
    assert made.disposition.value == "CLARIFY"
    loaded, actual_base, reference, location = persist_and_load(tmp_path, base, made.pending)
    assert loaded.frame_ops == loaded.asked_ops == original
    first = P.respond(loaded, answer_turn(), trace_id="t2", now=T.T0 + T.MINUTE,
                      base=actual_base, base_trace_id=reference)
    assert (first.kind.value, first.code) == ("REASK", "PENDING_UNANSWERED")
    assert replace(first.pending, asks=1) == loaded
    again = reread(first.pending)
    final = P.respond(again, answer_turn(answer_op()), trace_id="t2", now=T.T0 + 2 * T.MINUTE,
                      base=actual_base, base_trace_id=reference)
    assert final.kind.value == "PENDING_ANSWER"
    assert final.answer.composed.effective.base_trace_id == "t1"
    assert store.read(location).records[-1].pending == loaded  # protocol never writes or mutates the store


def test_h5_same_frame_different_persisted_original_asking_patch_changes_f24(tmp_path):
    histories = [T.h5(label) for label in ("BUYER", "GEOGRAPHY")]
    a, b = (h[1].pending for h in histories)
    assert a.frame == b.frame and a.candidate_patches == b.candidate_patches
    assert a.frame_ops == b.frame_ops == () and a.asked_ops != b.asked_ops
    outputs = []
    reply = answer_turn(answer_op(D.BUYER, "y"))
    for i, (base, made, *_original_inputs) in enumerate(histories):
        loaded, bound_base, reference, _ = persist_and_load(tmp_path / str(i), base, made.pending)
        assert loaded.asked_ops == made.pending.asked_ops
        outputs.append(P.respond(loaded, reply, trace_id="t2", now=T.T0 + T.MINUTE,
                                 base=bound_base, base_trace_id=reference))
    # Independent oracle: repeating original BUYER is F24; answering BUYER after original GEO selects BUYER.
    assert (outputs[0].kind.value, outputs[0].code) == ("REASK", "ANSWER_UNRESOLVED")
    assert (outputs[1].kind.value, outputs[1].answer.candidate_id) == ("PENDING_ANSWER", "c1")
    assert outputs[0].pending.frame_ops == a.frame_ops and outputs[0].pending.asked_ops == a.asked_ops


@pytest.mark.parametrize("mechanism", ["F16", "A8.1"])
def test_different_original_frame_histories_survive_real_store_even_with_same_frame_and_response(tmp_path, mechanism):
    if mechanism == "F16":
        base = T.base_of((D.TOPIC, "x"), (D.BUYER, "x"))
        clearing, setting = T.clear_op(T.BUYER_POS), T.set_op(D.BUYER, "x", excluded=True, i=1)
    else:
        base = T.base_of((D.TOPIC, "x"), (D.VALUE, MoneyValue(100, Comparator.GTE)),
                         (D.VALUE, MoneyValue(900, Comparator.LT)))
        clearing = T.clear_op(fa.Slot.from_dict("VALUE/upper"))
        setting = T.set_op(D.VALUE, MoneyValue(1000, Comparator.GTE), i=1)
    histories = [asking(base, ops) for ops in ((clearing, setting), (setting,))]
    assert all(h.disposition.value == "CLARIFY" for h in histories)
    assert histories[0].pending.frame == histories[1].pending.frame
    assert histories[0].pending.frame_ops != histories[1].pending.frame_ops
    outcomes = []
    for i, made in enumerate(histories):
        loaded, bound_base, reference, _ = persist_and_load(tmp_path / str(i), base, made.pending)
        assert loaded.frame_ops == made.pending.frame_ops
        outcomes.append(P.respond(loaded, answer_turn(answer_op()), trace_id="t2", now=T.T0 + T.MINUTE,
                                  base=bound_base, base_trace_id=reference))
    assert all(d.kind.value == "PENDING_ANSWER" for d in outcomes)
    assert outcomes[0].answer.composed.effective == outcomes[1].answer.composed.effective
    assert fa.pending_sha256(histories[0].pending) != fa.pending_sha256(histories[1].pending)
    # No echo implementation or certification is implied by this capacity test.


@pytest.mark.parametrize("action", ["answer", "event", "stale_event", "cancel", "expired"])
def test_missing_or_foreign_context_is_rejected_before_any_decision_and_leaves_state_intact(action, monkeypatch):
    base, made = T.r31()
    p = reread(made.pending)
    event = P.OptionEvent(p.pending_id if action != "stale_event" else "foreign-pending", "c1")
    t = event if "event" in action else answer_turn(
        cancel_directive=action == "cancel", own_conditions=0 if action == "cancel" else 1)
    now = T.T0 + (31 if action == "expired" else 1) * T.MINUTE
    before = p.to_dict(), base.to_dict()
    def forbidden(*args, **kwargs):
        raise AssertionError("decision/answer reached before contextual validation")
    monkeypatch.setattr(P, "_event", forbidden)
    monkeypatch.setattr(P, "_text", forbidden)
    for kwargs, code in (({}, "INVALID_BASE"), ({"base": base}, "INVALID_BASE_TRACE_ID"),
                         ({"base": base, "base_trace_id": "foreign"}, "PENDING_BASE_TRACE_MISMATCH"),
                         ({"base": replace(base, root_trace_id="foreign-root"), "base_trace_id": "t0"},
                          "PENDING_FRAME_MISMATCH")):
        with pytest.raises(ContractError, match=code):
            P.respond(p, t, trace_id="t2", now=now, **kwargs)
    assert before == (p.to_dict(), base.to_dict())


def test_full_frame_mismatch_and_invalid_replay_fail_closed_after_strict_decode():
    base = T.base_of((D.TOPIC, "x"), (D.BUYER, "x"))
    op = T.set_op(D.BUYER, "y")
    p = asking(base, (op,)).pending
    for corrupted in (replace(p, frame=replace(p.frame, root_trace_id="foreign-root")),
                      replace(p, frame_ops=(op, op), asked_ops=(op, op))):
        read = reread(corrupted)  # negative persisted data is shape-admissible; C2 must not be confused with codec validity
        with pytest.raises(ContractError, match="PENDING_FRAME_MISMATCH"):
            P.respond(read, answer_turn(answer_op()), trace_id="t2", now=T.T0 + T.MINUTE,
                      base=base, base_trace_id="t0")


def test_empty_frame_witness_requires_the_entire_exact_base_identity():
    base, made = T.h5()[:2]
    p = reread(made.pending)
    assert p.frame_ops == () and p.frame.to_dict() == base.to_dict()
    changed = replace(base.conditions[0], authority="foreign_authority@1")
    foreign = replace(base, conditions=(changed, *base.conditions[1:]))
    assert [c.value for c in foreign.conditions] == [c.value for c in base.conditions]
    with pytest.raises(ContractError, match="PENDING_FRAME_MISMATCH"):
        P.respond(p, answer_turn(answer_op(D.GEOGRAPHY, "y")), trace_id="t2", now=T.T0 + T.MINUTE,
                  base=foreign, base_trace_id="t0")


def test_fresh_pending_can_persist_original_nonempty_asking_input_and_respond_without_base(tmp_path):
    frame = T.base_of((D.TOPIC, "x"))
    original = (T.set_op(D.TOPIC, "x"),)
    made = create.ask_fresh(month_question(), frame=frame, asked_trace_id="t1", now=T.T0, asked_ops=original)
    loaded, base, ref, _ = persist_and_load(tmp_path, None, made.pending)
    assert loaded.frame_ops == () and loaded.asked_ops == original and ref is None
    response = P.respond(loaded, answer_turn(answer_op()), trace_id="t2", now=T.T0 + T.MINUTE)
    assert response.kind.value == "PENDING_ANSWER"


@PROPS
@given(st.permutations((T.set_op(D.BUYER, "y", i=1), T.set_op(D.GEOGRAPHY, "y", i=2))))
def test_permuted_original_resolved_ops_persist_and_replay_one_canonical_history(ops):
    base = T.base_of((D.TOPIC, "x"))
    made = asking(base, tuple(ops))
    assert made.disposition.value == "CLARIFY"
    p = reread(made.pending)
    assert Counter(json.dumps(o.to_dict(), sort_keys=True) for o in p.frame_ops) == Counter(
        json.dumps(o.to_dict(), sort_keys=True) for o in ops)
    outputs = [asking(base, tuple(order)).pending.to_dict() for order in itertools.permutations(ops)]
    assert all(o == outputs[0] for o in outputs)
    create.validate_pending_base(p, base=base, base_trace_id="t0")


@pytest.mark.parametrize("source", ["SYNTHETIC", "REAL"])
def test_dev_gold_creation_with_original_typed_inputs_preserves_complete_asking_multibag(source):
    real_dir = os.environ.get("M2A_GOLD_REAL_DIR")
    assert source != "REAL" or real_dir, "REAL creation gate requires the private directory, never a skipped PASS"
    cases = LINK.cases(real_dir if source == "REAL" else None)
    chosen = [c for c in cases if c["source"] == source and c["partition"] == "DEV"
              and c["applicability"] == "APLICABLE_M2A3" and c["expected"]["disposition"] == "CLARIFY"]
    assert chosen, f"No {source} DEV creation inputs accredited"
    for case in chosen:
        inputs = case["inputs"]
        base = LINK.base_of(inputs["base"])
        patch = fa.SemanticPatch.from_dict(inputs["patch"])
        facts = LINK.facts_of(inputs)
        outcome = fo.delta_outcome(base, patch, base_trace_id="t0", anchor_expired=facts.anchor_expired)
        assert outcome.disposition.value == "CLARIFY" and outcome.code == case["expected"]["code"], case["gold_id"]
        made = create.pending_from_question(outcome.pending, base=base, base_trace_id="t0", asked_trace_id="t1", now=T.T0)
        assert made.disposition.value == "CLARIFY", case["gold_id"]
        p = reread(made.pending)
        original_bag = Counter(json.dumps(o.to_dict(), sort_keys=True) for o in patch.ops)
        assert Counter(json.dumps(o.to_dict(), sort_keys=True) for o in p.asked_ops) == original_bag, case["gold_id"]
        create.validate_pending_base(p, base=base, base_trace_id="t0")


@PROPS
@given(st.sampled_from(("BUYER", "GEOGRAPHY")), st.integers(3, 40))
def test_f24_after_reload_uses_persisted_semantics_not_candidate_ids_or_original_object_identity(label, index):
    base = T.base_of((D.ACTOR, "x"))
    buyer, place = T.set_op(D.BUYER, "y"), T.set_op(D.GEOGRAPHY, "y", i=1)
    original = buyer if label == "BUYER" else place
    org = T._org(index)
    original = replace(original, condition=replace(original.condition, condition_id=f"original-{index}", origin=org),
                       origin=org)
    question = create.AuthorityQuestion("BUYER_OR_PLACE_AFTER_ACTOR", org, frozenset({T.BUYER_POS, T.GEO_POS}),
                                        ((buyer,), (place,)))
    made = create.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T.T0,
                                frame_ops=(), open_ops=(original,))
    loaded = reread(made.pending)
    response = P.respond(loaded, answer_turn(answer_op(D.BUYER, "y")), trace_id="t2", now=T.T0 + T.MINUTE,
                         base=base, base_trace_id="t0")
    assert response.kind.value == ("REASK" if label == "BUYER" else "PENDING_ANSWER")
    if label == "BUYER":
        assert response.code == "ANSWER_UNRESOLVED" and response.pending.asked_ops == (original,)
