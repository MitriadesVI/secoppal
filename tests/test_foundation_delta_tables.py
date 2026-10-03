"""M2A-3 phase 2: exhaustive tables generated from the closed domains (never cases written by hand: ADR-100's hand tests
covered 4 of 266 pairs), each expectation written here from the contract's text, independently of the reducer.

* INV-M2A-18 (§15.1–§15.2, A8/A8.1): previous side × new side × relation of the amounts × inclusivity of both
  comparators × origin of the bounds (carried / same turn) × units (pesos, SMMLV, mixed, unknown: §15.2 compares an
  unknown unit with nothing).
* INV-M2A-19 (§6.5 F16, P-3, R3-4; F17; F3/F22): the positives of the base × the negated value × a positive of the same
  patch.
* §7.2 and §8 (A7) over the boolean domain of the facts, plus INV-M2A-05 and INV-M2A-20; §7.1 rows 1–6.
* Pins of what the reducer copies from the host and the contract: the subject-to-dataset table, the SMMLV catalogue, the
  refusal precedence and the host codes.
Every executed or refused row is also judged by the frozen oracle's ``check_transition``.
"""
from __future__ import annotations

import itertools

import pytest

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
from app.core.foundation_authority_values import Comparator, MoneyValue, TopicValue
from app.core.foundation_operations import Operation, Subject
from app.core.interpretation_contract import ContractError
from app.core.semantic_dimensions import SemanticDimension as D
from scripts.audits.m2a_conversation import m2a_oracle_transition as transition
from app.core.foundation_turn_scope import AnchorState, InputKind, ReferentSignal, TurnFacts, classify_turn
from tests.test_foundation_delta_properties import RAW_BASE, RAW_TURN, VALUES, condition, dataset_of, origin
from tests.test_m2a_contract_r3_sweep import CT

TEXT = CT.read()

REQUEST = fa.AcceptedRequest("r1", "s1", Subject.CONTRACTS, fa.UniverseKind.RECORDS, origin("t0", 0, RAW_BASE), None,
                             fd.SUBJECT_DATASET[Subject.CONTRACTS])
TOPIC = TopicValue("aseo", False, False)


def base_of(*conditions) -> fa.AcceptedInterpretation:
    return fa.canonical_interpretation((REQUEST,), tuple(conditions), None, (), None, "t-root")


def judged(base, patch, outcome) -> list[dict]:
    if outcome.disposition is fo.DeltaDisposition.EXECUTE:
        return transition.check_transition(base.to_dict(), patch.to_dict(), outcome.effective.to_dict(),
                                           dataset_of=dataset_of)
    return transition.check_transition(base.to_dict(), patch.to_dict(), None, dataset_of=dataset_of,
                                       refusal=outcome.code)


# ---- INV-M2A-18: VALUE by side and A8.1 ------------------------------------------------------------------------------
UNITS = {"PESOS": fa.Provenance(unit=fa.Unit.MILLONES, source_amount="5", unit_source=fa.UnitSource.STATED),
         "SMMLV": fa.Provenance(unit=fa.Unit.SMMLV, source_amount="500", unit_source=fa.UnitSource.STATED, year=2025,
                                year_source=fa.YearSource.STATED),
         "UNKNOWN": fa.Provenance()}  # a bound whose unit nobody stated (§15.2: comparable with nothing)
SIDES = {"lower": (Comparator.GTE, Comparator.GT), "upper": (Comparator.LTE, Comparator.LT)}  # (inclusive, exclusive)


def bound(side: str, inclusive: bool, amount: int, unit: str, cid: str, org) -> fa.AcceptedCondition:
    comparator = SIDES[side][0 if inclusive else 1]
    return condition(D.VALUE, MoneyValue(amount, comparator), cid, ("r1",), org, UNITS[unit], rich=False)


def a81_rows():
    for prev_side, new_side in itertools.product(("lower", "upper"), repeat=2):
        for amount, prev_incl, new_incl, origin_kind, units in itertools.product(
                (100, 500, 900), (True, False), (True, False), ("carried", "same_turn"),
                (("PESOS", "PESOS"), ("SMMLV", "SMMLV"), ("PESOS", "SMMLV"), ("SMMLV", "PESOS"),
                 ("UNKNOWN", "UNKNOWN"), ("UNKNOWN", "PESOS"), ("UNKNOWN", "SMMLV"), ("PESOS", "UNKNOWN"),
                 ("SMMLV", "UNKNOWN"))):
            yield prev_side, new_side, amount, prev_incl, new_incl, origin_kind, units
    for new_side, amount, new_incl, unit in itertools.product(("lower", "upper"), (100, 500, 900), (True, False),
                                                              ("PESOS", "SMMLV", "UNKNOWN")):
        yield None, new_side, amount, None, new_incl, "carried", (None, unit)


def a81_expected(prev_side, new_side, amount, origin_kind, units) -> tuple[str, str | None, set[int], set[str]]:
    """(disposition, code, the amounts left on the effective, effects) from §15.1–§15.2."""
    prev_amount = 500
    if prev_side is None:
        return "EXECUTE", None, {amount}, set()
    if origin_kind == "same_turn":
        if prev_side == new_side:  # two SET of one slot (§6.2)
            return "CLARIFY", "DELTA_SLOT_CONFLICT", set(), set()
        return "EXECUTE", None, {prev_amount, amount}, set()  # the user's own range, read by E-VALUE (§15.2)
    if prev_side == new_side:  # §15.1: the new bound replaces the bound of its side
        return "EXECUTE", None, {amount}, set()
    if units[0] == "UNKNOWN":  # §15.2 + §8.4 (3): no unit an answer can choose compares with an unknown carried one
        return "INFORM", "UNRESOLVED_VALUE:VALUE", set(), set()
    if units[0] != units[1] or units[1] == "UNKNOWN":  # §15.2 (A8): not compared, not replaced, not kept in silence
        return "CLARIFY", "UNRESOLVED_VALUE:VALUE", set(), set()
    low, high = (amount, prev_amount) if new_side == "lower" else (prev_amount, amount)
    if low >= high:  # empty or degenerate, generated by a follow-up: A8.1
        return "EXECUTE", None, {amount}, {"VALUE_RANGE_REPLACED"}
    return "EXECUTE", None, {prev_amount, amount}, set()


@pytest.mark.parametrize("row", list(a81_rows()), ids=lambda r: "-".join(map(str, r)))
def test_inv_m2a_18_value_by_side_and_a81(row):
    prev_side, new_side, amount, prev_incl, new_incl, origin_kind, units = row
    topic = condition(D.TOPIC, TOPIC, "c0", ("r1",), origin("t0", 0, RAW_BASE), rich=False)
    new = bound(new_side, new_incl, amount, units[1], "n1", origin("t1", 3, RAW_TURN))
    ops = [fa.PatchOp(fa.PatchOpKind.SET, new.slot, new, None, new.origin)]
    if prev_side is None:
        base = base_of(topic)
    elif origin_kind == "carried":
        base = base_of(topic, bound(prev_side, prev_incl, 500, units[0], "c1", origin("t0", 3, RAW_BASE)))
    else:
        base = base_of(topic)
        prev = bound(prev_side, prev_incl, 500, units[0], "n0", origin("t1", 6, RAW_TURN))
        ops.append(fa.PatchOp(fa.PatchOpKind.SET, prev.slot, prev, None, prev.origin))
    patch = fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT)
    outcome = fo.delta_outcome(base, patch, base_trace_id="t0")
    disposition, code, amounts, effects = a81_expected(prev_side, new_side, amount, origin_kind, units)
    assert (outcome.disposition.value, outcome.code) == (disposition, code)
    if disposition == "EXECUTE":
        left = {fa.money_bound(c)[0] for c in outcome.effective.conditions if c.dimension is D.VALUE}
        assert left == amounts and {e.value for e in outcome.effects} == effects
    assert judged(base, patch, outcome) == []


# ---- INV-M2A-19: polarity (F16, P-3, R3-4), F17 and F3/F22 ---------------------------------------------------------
X, Y, Z, W = (TopicValue(name, False, False) for name in ("aseo", "vias", "obras", "papel"))
POSITIVES = ((), (X,), (Y,), (X, Y), (Y, Z))


def neg(value: TopicValue) -> TopicValue:
    return TopicValue(value.phrase_folded, value.token_mode, True)


def f16_expected(positives, same_patch) -> tuple[str, str | None, set, set, set]:
    """(disposition, code, positives left, negatives left, effects) from §6.5 (and §6.2 for the slots)."""
    if same_patch == X:  # P-3: +X and -X created by one patch
        return "CLARIFY", "UNRESOLVED_VALUE:TOPIC", set(), set(), set()
    if same_patch is not None:  # the positive slot is written: nothing carried to invert; F3 if it replaced another
        replaced = {"TOPIC_REPLACED"} if any(p != same_patch for p in positives) else set()
        return "EXECUTE", None, {same_patch}, {neg(X)}, replaced
    if positives == (X,):  # F16: the single carried positive of the negated value is inverted
        return "EXECUTE", None, set(), {neg(X)}, {"POLARITY_INVERTED"}
    if X in positives:  # R3-4: one of several positives: not accredited in M2a (G-FU-09)
        return "CLARIFY", "UNRESOLVED_VALUE:TOPIC", set(), set(), set()
    return "EXECUTE", None, set(positives), {neg(X)}, set()  # F17: another value adds the exclusion


@pytest.mark.parametrize("positives, same_patch", list(itertools.product(POSITIVES, (None, X, W))))
def test_inv_m2a_19_polarity(positives, same_patch):
    base = base_of(*(condition(D.TOPIC, v, f"c{i}", ("r1",), origin("t0", 3 * i, RAW_BASE), rich=False)
                     for i, v in enumerate(positives)),
                   condition(D.BUYER, VALUES[D.BUYER](False)[0], "cb", ("r1",), origin("t0", 30, RAW_BASE),
                             rich=False))
    negated = condition(D.TOPIC, neg(X), "n1", ("r1",), origin("t1", 0, RAW_TURN), rich=False)
    ops = [fa.PatchOp(fa.PatchOpKind.SET, negated.slot, negated, None, negated.origin)]
    if same_patch is not None:
        positive = condition(D.TOPIC, same_patch, "n2", ("r1",), origin("t1", 3, RAW_TURN), rich=False)
        ops.append(fa.PatchOp(fa.PatchOpKind.SET, positive.slot, positive, None, positive.origin))
    patch = fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT)
    outcome = fo.delta_outcome(base, patch, base_trace_id="t0")
    disposition, code, left_positive, left_negative, effects = f16_expected(positives, same_patch)
    assert (outcome.disposition.value, outcome.code) == (disposition, code)
    if disposition == "EXECUTE":
        topics = [fa.decode_value(c.value) for c in outcome.effective.conditions if c.dimension is D.TOPIC]
        assert {t for t in topics if not t.excluded} == left_positive
        assert {t for t in topics if t.excluded} == left_negative
        assert {e.value for e in outcome.effects} == effects
    assert judged(base, patch, outcome) == []


# ---- §7.2 and §8 (A7): the boolean domain of the facts ------------------------------------------------------------
BOOL = (False, True)


def spec_scope(f) -> tuple:
    """(scope, control, context_reset) or the refusal, from §7.2, §8.3 rows 2–3 and D7's floor."""
    allowed = (not f["continuation"]) and f["frame"] and f["admissible_alone"] and f["own_conditions"] > 0
    reset = not f["anchor_expired"]
    if f["pending"]:
        if f["cancel_directive"]:
            return ("IN_NEW_SEARCH", "CANCEL_PENDING", reset) if allowed else ("IN", "CANCEL_PENDING", False)
        if allowed and not f["within_answer_slots"]:
            return "IN_NEW_SEARCH", "CANCEL_PENDING", reset
        return "IN", None, False
    if f["continuation"] or not f["new_content"] or not f["frame"] or f["own_conditions"] == 0:
        return "IN", None, False
    # (c), or no row of §7.2: then §7.2 does not say NEW_SEARCH and §7.1 row 8 («lo demás: delta») decides
    return ("IN_NEW_SEARCH", None, reset) if f["admissible_alone"] else ("IN", None, False)


FACT_NAMES = ("pending", "continuation", "new_content", "frame", "admissible_alone", "cancel_directive",
              "within_answer_slots", "anchor_expired")


def test_section_7_2_over_the_whole_boolean_domain_with_inv_m2a_05_and_20():
    rows, refused = 0, 0
    for values in itertools.product(BOOL, repeat=len(FACT_NAMES)):
        for own in (0, 1, 2):
            f = dict(zip(FACT_NAMES, values), own_conditions=own)
            facts = TurnFacts(InputKind.TEXT, AnchorState.FOUNDATION_SEARCH, **f)
            want = spec_scope(f)
            try:
                got = classify_turn(facts)
            except ContractError as error:
                assert str(error) == want, (f, str(error))
                refused += 1
                continue
            rows += 1
            assert (got.scope.value, None if got.control is None else got.control.value, got.context_reset) == want, f
            assert type(got.scope) is fa.TurnScope  # INV-M2A-20: exactly one scope of A.1
            if got.scope is fa.TurnScope.IN_NEW_SEARCH:  # INV-M2A-05: a fragment never runs alone
                assert not f["continuation"] and f["frame"] and f["admissible_alone"] and own > 0
    assert rows == 2 ** len(FACT_NAMES) * 3 and refused == 0  # INV-M2A-20: one scope for every turn (Q-R8-3 closed)


def test_an_undetermined_fact_is_read_three_valued_never_defaulted():
    base = dict(input_kind=InputKind.TEXT, anchor=AnchorState.FOUNDATION_SEARCH, anchor_expired=False, pending=False)
    assert classify_turn(TurnFacts(**base, continuation=None, new_content=False, own_conditions=1)).scope is (
        fa.TurnScope.IN)  # both continuations end in a delta: determined
    with pytest.raises(ContractError, match="UNDETERMINED_FACT"):
        classify_turn(TurnFacts(**base, continuation=False, new_content=True, frame=None, admissible_alone=True,
                                own_conditions=2))


# ---- §7.1 rows 1–6 ---------------------------------------------------------------------------------------------------
def test_section_7_1_rows_in_their_order():
    def facts(**kw):
        defaults = dict(input_kind=InputKind.TEXT, anchor=AnchorState.FOUNDATION_SEARCH, anchor_expired=False,
                        pending=False, continuation=True, new_content=False, frame=False, admissible_alone=False,
                        own_conditions=1)
        return TurnFacts(**{**defaults, **kw})

    cases = [
        (facts(anchor=AnchorState.NONE), ("OUT_NO_ANCHOR", "NO_ANCHOR")),
        (facts(anchor=AnchorState.UNREADABLE), ("OUT_NO_ANCHOR", "ANCHOR_UNREADABLE")),
        (facts(anchor=AnchorState.NONE, admissible_alone=True), ("IN_NEW_SEARCH", None)),
        (facts(anchor=AnchorState.NONE, input_kind=InputKind.EVENT), ("OUT_NO_ANCHOR", "NO_ANCHOR")),
        (facts(anchor=AnchorState.LEGACY, legacy_operation=Operation.COUNT),
         ("OUT_ANCHOR_NOT_SEARCH", "ANCHOR_NOT_SEARCH:COUNT")),
        (facts(input_kind=InputKind.EVENT), ("IN_PROTOCOL", None)),
        (facts(foreign_operation=Operation.COUNT), ("OUT_OPERATION_CHANGE", "OPERATION_CHANGE:COUNT")),
        (facts(unsupported_motive="MULTI_REQUEST"), ("OUT_UNSUPPORTED_PROPOSAL", "UNSUPPORTED_PROPOSAL:MULTI_REQUEST")),
        (facts(unsupported_motive="NO_ACTION", pending=True), ("IN", None)),  # with a pending: §8.3 row 8
        (facts(referent=ReferentSignal.EXTENSIONAL), ("OUT_REFERENT_EXTENSIONAL", "REFERENT_EXTENSIONAL")),
        (facts(referent=ReferentSignal.INTENSIONAL), ("IN", None)),
        (facts(relative_non_temporal=True), ("OUT_RELATIVE_TO_PREVIOUS", "RELATIVE_TO_PREVIOUS_VALUE")),
        (facts(foreign_operation=Operation.COUNT, unsupported_motive="MULTI_REQUEST"),
         ("OUT_OPERATION_CHANGE", "OPERATION_CHANGE:COUNT")),  # row 4 precedes 4b
    ]
    for given, (scope, code) in cases:
        got = classify_turn(given)
        assert (got.scope.value, got.code) == (scope, code), given
        assert got.code is None or fa.reason_code_allowed(got.code, got.resolution)
    with pytest.raises(ContractError, match="REFERENT_SIGNAL_ABSENT"):  # ABSENT is never intensional (§7.2)
        classify_turn(facts(referent=ReferentSignal.ABSENT))


# ---- pins of what the reducer copies -------------------------------------------------------------------------------
def test_the_subject_dataset_table_is_the_builders():
    from app.core.soql_builder import SoQLBuilder

    assert fd.SUBJECT_DATASET == {Subject.PROCESSES: SoQLBuilder.PROCESOS_DATASET,
                                  Subject.CONTRACTS: SoQLBuilder.CONTRATOS_DATASET}


def test_the_smmlv_catalogue_is_d10s():
    from app.data.smmlv import SMMLV_COP_BY_YEAR

    assert fo.SMMLV_CATALOGUE_YEARS == frozenset(SMMLV_COP_BY_YEAR) == frozenset({2024, 2025, 2026})


def test_the_refusal_precedence_and_the_host_codes_are_the_contracts():
    assert [r.value for r in fd.PRECEDENCE] == [c.partition(":")[0] for c in CT.refusal_precedence(TEXT)]
    host = {rule: code for rule, _disposition, code in CT.host_rules(TEXT)}
    assert all(host[rule] == code for rule, code in fo.HOST_CODES.items())
    assert tuple(host) == fo.HOST_RULES
