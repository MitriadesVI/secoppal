"""M2A-3: one DIRECT test per rule of the reducer, written from the contract's words (ZEUS NO_GO on 15e72829, P1 and P2).

The properties (``test_foundation_delta_properties``) judge generated cases; these pin each rule on the smallest case
that shows it, so a one-line mutant of the rule dies under the gate's configuration (seed 0) and not only when the
generator happens to reach it. Every refusal or effective is also judged by the frozen oracle where it can judge.

* §6 H6 (F30) and §8.4: CLARIFY TERRITORY_VS_CARRIED_BUYER with its two candidates; «buscar con ambos» executes
  (the candidates are judged by the causes of §6 step 3b, §8.4 (2), and the rule they answer is not raised again).
* §6 H1, §6.7 (R3-1), §8.4 (1)–(3), H3, H8, H11, H12.
* §6 step 3b: one cause per OPERATION; §6.6 policies without owner (placed, carried across a change of universe, the
  O1B lens), §19 DEP-O1B; §15.2 an unknown unit is comparable with nothing.
* §7.1–§7.2: rows and the facts the table cannot reach on its own; ``search_effects``.
* ZEUS NO_GO on the integration 8066e03d: F1, a §19 COMPOSICION dependency never passes through the frame of a pending
  (it decides the turn, read from the base); F3, the policy a placed condition needs is judged on the universe the
  same patch leaves; F4, a dual anchor reduced is a change of universe for H8 (§14, the contract's model).
"""
from __future__ import annotations

from dataclasses import replace
from datetime import date

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
from app.core.foundation_authority_values import (
    BuyerKind, BuyerValue, CardinalityBasis, CardinalityValue, Comparator, GeoValue, ModalityValue, MoneyValue,
    OrderDirection, OrderKey, OrderValue, StateValue, TimeValue, TopicValue,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Subject
from app.core.foundation_turn_scope import (
    AnchorState, InputKind, TurnFacts, classify_turn, search_effects,
)
from app.core.interpretation_contract import (
    ConditionExpression, Effect, HostPolicy, LogicOperator, Polarity, ValueSide,
)
from app.core.semantic_dimensions import SemanticDimension as D
from scripts.audits.m2a_conversation import m2a_oracle_transition as transition
from tests.test_foundation_delta_properties import RAW_BASE, RAW_TURN, condition, dataset_of, origin

POS, NEG = Polarity.POSITIVE, Polarity.NEGATIVE
PROCESSES, CONTRACTS = Subject.PROCESSES, Subject.CONTRACTS
RECORDS, OPPORTUNITIES = fa.UniverseKind.RECORDS, fa.UniverseKind.OPPORTUNITIES

ASEO, VIAS = TopicValue("aseo", False, False), TopicValue("vias", False, False)
VALLE = BuyerValue("VALLE", BuyerKind.UNIT, ("VALLE",), None, False)
ICBF = BuyerValue("ICBF", BuyerKind.UNIT, ("ICBF",), None, False)
ANTIOQUIA = GeoValue(GeoLevel.DEPARTMENT, "ANTIOQUIA", None, "05", (), False)
VALLE_GEO = GeoValue(GeoLevel.DEPARTMENT, "VALLE", None, "76", (), False)
CARIBE = GeoValue(GeoLevel.REGION, "CARIBE", None, None, ("08", "13"), False)  # SF123: a region has no code
OPEN = StateValue("open", (("p6dx-8zbt", "estado_del_procedimiento", ("Abierto",)),), False)


def terminated(excluded: bool) -> StateValue:
    return StateValue("terminated", (("jbjy-vk9h", "estado_contrato", ("Terminado",)),), excluded)


def year(first: int, last: int | None = None) -> TimeValue:
    last = first if last is None else last
    return TimeValue(date(first, 1, 1), date(last + 1, 1, 1), "year" if first == last else "range", False)


SCOPE_76 = fa.Provenance(buyer_scope="76")
NATIONAL = fa.Provenance(buyer_scope="NATIONAL")
PESOS = fa.Provenance(unit=fa.Unit.PESOS, unit_source=fa.UnitSource.STATED)
UNKNOWN_UNIT = fa.Provenance()


def smmlv(year_: int) -> fa.Provenance:
    return fa.Provenance(unit=fa.Unit.SMMLV, source_amount="500", unit_source=fa.UnitSource.STATED, year=year_,
                         year_source=fa.YearSource.STATED)


ORDER_VALUE = OrderValue(list(OrderKey)[0], list(OrderDirection)[0])
TOP_10 = CardinalityValue(10, CardinalityBasis.FIGURE, 10)


def request(rid: str, subject: Subject, kind=RECORDS) -> fa.AcceptedRequest:
    return fa.AcceptedRequest(rid, "s1", subject, kind, origin("t0", 0, RAW_BASE), None, fd.SUBJECT_DATASET[subject])


def base(*specs, subjects=(PROCESSES,), kind=RECORDS, group=(), policies=()) -> fa.AcceptedInterpretation:
    """``specs``: (dimension, value[, provenance]); ``group``: indexes joined by one OR (the rest ANDed)."""
    requests = tuple(request(f"r{i}", s, kind) for i, s in enumerate(subjects, 1))
    rids = tuple(r.request_id for r in requests)
    conditions = tuple(condition(d, v, f"c{i}", rids, origin("t0", 3 * i, RAW_BASE), rest[0] if rest else None,
                                 False) for i, (d, v, *rest) in enumerate(specs))
    logic = None
    if group:
        atoms = lambda ids: tuple(ConditionExpression(LogicOperator.ATOM, f"c{i}") for i in ids)  # noqa: E731
        rest = atoms(i for i in range(len(specs)) if i not in group)
        logic = ConditionExpression(LogicOperator.OR, None, atoms(group))
        logic = ConditionExpression(LogicOperator.AND, None, (logic, *rest)) if rest else logic
    return fa.canonical_interpretation(requests, conditions, logic, tuple(policies), None, "t-root")


def set_op(dimension, value, i=0, provenance=None, cid="p1", kind=fa.PatchOpKind.SET) -> fa.PatchOp:
    org = origin("t1", 3 * i, RAW_TURN)
    c = condition(dimension, value, cid, ("r1",), org, provenance, False)
    return fa.PatchOp(kind, c.slot, c, None, org)


def op(kind, slot, i=0, universe=None) -> fa.PatchOp:
    return fa.PatchOp(kind, slot, None, universe, origin("t1", 3 * i, RAW_TURN))


def to(subject, kind=RECORDS, i=0) -> fa.PatchOp:
    return op(fa.PatchOpKind.REPLACE_UNIVERSE, fa.UNIVERSE_SLOT, i, fa.UniverseTarget(subject, kind))


def clear(slot, i=0) -> fa.PatchOp:
    return op(fa.PatchOpKind.CLEAR, slot, i)


def patch(*ops) -> fa.SemanticPatch:
    return fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT)


def answer(b, *ops, expired=False) -> fo.DeltaOutcome:
    outcome = fo.delta_outcome(b, patch(*ops), base_trace_id="t0", anchor_expired=expired)
    p = patch(*ops)
    if outcome.disposition is fo.DeltaDisposition.EXECUTE:
        bad = transition.check_transition(b.to_dict(), p.to_dict(), outcome.effective.to_dict(), dataset_of=dataset_of)
    elif outcome.disposition is fo.DeltaDisposition.DEPENDENCY:
        bad = []
    else:
        bad = transition.check_transition(b.to_dict(), p.to_dict(), None, dataset_of=dataset_of, refusal=outcome.code)
    assert bad == [], bad  # the frozen oracle agrees wherever it can judge
    return outcome


def said(outcome) -> tuple:
    return outcome.disposition.value, outcome.code


def ops_of(candidate) -> list:
    return sorted((o.op.value, o.slot.key) for o in candidate)


S = fa.Slot
GEO_POS, BUYER_POS, TOPIC_POS = S(D.GEOGRAPHY, POS), S(D.BUYER, POS), S(D.TOPIC, POS)
TIME_POS, STATE_POS, STATE = S(D.TIME, POS), S(D.STATE, POS), S(D.STATE)
ORDER, CARDINALITY = S(D.ORDER), S(D.CARDINALITY)
LOWER, UPPER = S(D.VALUE, side=ValueSide.LOWER), S(D.VALUE, side=ValueSide.UPPER)


# ---- H6 (F30) and §8.4 (2)–(3): the ZEUS P1 -------------------------------------------------------------------------
H6_BASE = base((D.TOPIC, ASEO), (D.BUYER, VALLE, SCOPE_76), (D.TIME, year(2025)))


def test_h6_asks_with_its_two_candidates_and_buscar_con_ambos_executes():
    geo = set_op(D.GEOGRAPHY, ANTIOQUIA)
    outcome = answer(H6_BASE, geo)
    assert said(outcome) == ("CLARIFY", "TERRITORY_VS_CARRIED_BUYER")
    p = outcome.pending
    assert p.frame == H6_BASE and p.frame_ops == () and p.open_ops == (geo,)
    change_buyer, both = p.candidates
    assert ops_of(change_buyer) == [("CLEAR", "BUYER/POSITIVE"), ("SET", "GEOGRAPHY/POSITIVE")] and both == (geo,)
    assert {GEO_POS, BUYER_POS} <= p.answer_slots
    for candidate in p.candidates:  # §8.4 (2): no cause of §6 step 3b; (3): each answer executes on the frame
        assert isinstance(fd.compose_ops(p.frame, candidate, base_trace_id="t0"), fd.Composed)
        assert fo.executes(p.frame, candidate, "t0", answered="H6")
    kept = fd.compose_ops(p.frame, both, base_trace_id="t0").effective
    assert {c.slot.key for c in kept.conditions} >= {"BUYER/POSITIVE", "GEOGRAPHY/POSITIVE"}  # with both
    changed = fd.compose_ops(p.frame, change_buyer, base_trace_id="t0").effective
    assert "BUYER/POSITIVE" not in {c.slot.key for c in changed.conditions}
    # the exemption is load-bearing: re-judged as a fresh turn, «ambos» meets H6 again (M2A-4 composes the choice)
    assert not fo.executes(p.frame, both, "t0")


def test_h6_asks_for_a_region_without_a_territorial_code():  # SF123's shape: a region is another territory
    assert said(answer(H6_BASE, set_op(D.GEOGRAPHY, CARIBE))) == ("CLARIFY", "TERRITORY_VS_CARRIED_BUYER")


def test_h6_reads_only_a_carried_territorial_buyer_of_another_territory():
    plain = base((D.TOPIC, ASEO))
    placed = answer(plain, set_op(D.BUYER, VALLE, 0, SCOPE_76), set_op(D.GEOGRAPHY, ANTIOQUIA, 1, cid="p2"))
    assert said(placed) == ("EXECUTE", None)  # the buyer the same patch places is not carried
    assert said(answer(base((D.TOPIC, ASEO), (D.BUYER, ICBF, NATIONAL)), set_op(D.GEOGRAPHY, ANTIOQUIA))) == (
        "EXECUTE", None)  # a national buyer has no territory
    assert said(answer(H6_BASE, set_op(D.GEOGRAPHY, VALLE_GEO))) == ("EXECUTE", None)  # its own territory


def test_h6_with_a_candidate_that_does_not_compose_informs():  # §8.4 (2): the buyer sits inside an OR group
    grouped = base((D.TOPIC, ASEO), (D.BUYER, VALLE, SCOPE_76), (D.TIME, year(2025)), group=(0, 1))
    assert said(answer(grouped, set_op(D.GEOGRAPHY, ANTIOQUIA))) == ("INFORM", "TERRITORY_VS_CARRIED_BUYER")


def test_a_frame_never_holds_a_second_question():  # §8.4 (1): H6 first, H8 would stay in its frame
    b = base((D.TOPIC, ASEO), (D.BUYER, VALLE, SCOPE_76), (D.STATE, OPEN))
    outcome = answer(b, set_op(D.GEOGRAPHY, ANTIOQUIA), to(CONTRACTS, i=1))
    assert said(outcome) == ("INFORM", "TERRITORY_VS_CARRIED_BUYER")


def test_answer_slots_must_cover_the_candidates():
    geo = set_op(D.GEOGRAPHY, ANTIOQUIA)
    change_buyer = (clear(BUYER_POS), geo)
    assert "answer_slots do not cover the candidates" in fo.law_violations(
        H6_BASE, (), (change_buyer,), frozenset({GEO_POS}), "t0", geo.origin, "H6")
    assert fo.law_violations(H6_BASE, (), (change_buyer, (geo,)), frozenset({GEO_POS, BUYER_POS}), "t0", geo.origin,
                             "H6") == []


# ---- H1 (§10.1 injected) ---------------------------------------------------------------------------------------------
def test_h1_asks_continuar_with_the_turns_patch_on_the_expired_anchor():
    b = base((D.TOPIC, ASEO), (D.TIME, year(2025)))
    turn = set_op(D.TOPIC, VIAS)
    outcome = answer(b, turn, expired=True)
    assert said(outcome) == ("CLARIFY", "CONTEXT_EXPIRED_CONFIRM")
    p = outcome.pending
    assert p.frame == b and p.open_ops == (turn,) and p.candidates == ((turn,),) and TOPIC_POS in p.answer_slots
    assert fo.executes(p.frame, p.candidates[0], "t0")


def test_h1_informs_with_the_rule_that_would_fire_and_yields_to_a_dependency():
    assert said(answer(H6_BASE, set_op(D.GEOGRAPHY, ANTIOQUIA), expired=True)) == (
        "INFORM", "TERRITORY_VS_CARRIED_BUYER")
    recalc = base((D.TOPIC, ASEO), (D.VALUE, MoneyValue(100, Comparator.GTE), smmlv(2025)), (D.TIME, year(2025)))
    for expired in (False, True):  # §19 DEP-SMMLV-RECALC decides before H1 too
        outcome = answer(recalc, clear(TIME_POS), expired=expired)
        assert outcome.disposition is fo.DeltaDisposition.DEPENDENCY
        assert outcome.dependency.dep_id == "DEP-SMMLV-RECALC"


def test_one_cause_on_an_expired_anchor_informs():
    b = base((D.TOPIC, ASEO))
    keep = op(fa.PatchOpKind.KEEP, TIME_POS)
    assert said(answer(b, keep)) == ("CLARIFY", "KEEP_ON_EMPTY_SLOT")
    assert said(answer(b, keep, expired=True)) == ("INFORM", "KEEP_ON_EMPTY_SLOT")


# ---- §6.7 (R3-1) -----------------------------------------------------------------------------------------------------
def test_r3_1_a_clear_of_the_order_asks_with_c1_only():
    b = base((D.TOPIC, ASEO), (D.ORDER, ORDER_VALUE), (D.CARDINALITY, TOP_10))
    outcome = answer(b, clear(ORDER))
    assert said(outcome) == ("CLARIFY", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER")
    p = outcome.pending
    assert [ops_of(c) for c in p.candidates] == [[("CLEAR", "CARDINALITY"), ("CLEAR", "ORDER")]]  # C1, no E-ORDER
    assert {ORDER, CARDINALITY} <= p.answer_slots
    assert fo.executes(p.frame, p.candidates[0], "t0")
    assert said(answer(b, clear(ORDER), clear(CARDINALITY, 1))) == ("EXECUTE", None)


def test_r3_1_informs_when_c1_would_leave_an_empty_search():  # §8.4 (3): C1 empties it (H11), no answer executes
    b = base((D.ORDER, ORDER_VALUE), (D.CARDINALITY, TOP_10))
    assert said(answer(b, clear(ORDER))) == ("INFORM", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER")


# ---- §8.4 (3) without candidates -------------------------------------------------------------------------------------
def test_no_answer_on_a_grouped_slot_can_execute():  # the model's witness (test_r3c_zeus_witnesses)
    b = base((D.TOPIC, ASEO), (D.BUYER, ICBF, NATIONAL), group=(0, 1))
    assert said(answer(b, set_op(D.TOPIC, VIAS))) == ("INFORM", "TARGET_NOT_ADDRESSABLE")


def test_a81_never_compares_an_unknown_unit():  # §15.2, oracle 9e54d2f5
    carried_unknown = base((D.TOPIC, ASEO), (D.VALUE, MoneyValue(500_000_000, Comparator.LT), UNKNOWN_UNIT))
    collides = set_op(D.VALUE, MoneyValue(1_423_500_000, Comparator.GTE), provenance=smmlv(2025))
    outcome = answer(carried_unknown, collides)  # never VALUE_RANGE_REPLACED in silence
    assert said(outcome) == ("INFORM", "UNRESOLVED_VALUE:VALUE")  # no unit an answer chooses compares with it
    assert [c.code for c in outcome.causes] == ["OP_NOT_ACCREDITED:VALUE"]
    carried_pesos = base((D.TOPIC, ASEO), (D.VALUE, MoneyValue(500_000_000, Comparator.LT), PESOS))
    unknown_new = set_op(D.VALUE, MoneyValue(100, Comparator.GTE), provenance=UNKNOWN_UNIT)
    outcome = answer(carried_pesos, unknown_new)  # a stated unit answers it
    assert said(outcome) == ("CLARIFY", "UNRESOLVED_VALUE:VALUE") and outcome.pending.answer_slots == {LOWER}
    assert said(answer(carried_pesos, set_op(D.VALUE, MoneyValue(100, Comparator.GTE), provenance=PESOS))) == (
        "EXECUTE", None)


# ---- §6 step 3b: one cause per operation; §6.6 policies without owner; §19 DEP-O1B --------------------------------
def test_a_condition_that_needs_a_host_policy_is_not_accredited():
    family = ModalityValue(("licitacion_publica", "seleccion_abreviada"), "modality-lp-family@1", False)
    outcome = answer(base((D.TOPIC, ASEO)), set_op(D.MODALITY, family))
    assert said(outcome) == ("CLARIFY", "UNRESOLVED_VALUE:MODALITY") and len(outcome.causes) == 1
    literal_ids = set_op(D.MODALITY, ModalityValue(("licitacion_publica",), None, False))
    by_operator = replace(literal_ids, condition=replace(literal_ids.condition, operator="family"))
    assert said(answer(base((D.TOPIC, ASEO)), by_operator)) == ("CLARIFY", "UNRESOLVED_VALUE:MODALITY")
    literal = set_op(D.MODALITY, ModalityValue(("minima_cuantia",), None, False))
    assert said(answer(base((D.TOPIC, ASEO)), literal)) == ("EXECUTE", None)


def test_a_negated_terminado_over_contracts_needs_the_complement_policy():
    contracts = base((D.TOPIC, ASEO), subjects=(CONTRACTS,))
    assert said(answer(contracts, set_op(D.STATE, terminated(True)))) == ("CLARIFY", "UNRESOLVED_VALUE:STATE")
    dual = base((D.TOPIC, ASEO), subjects=(PROCESSES, CONTRACTS))
    assert said(answer(dual, set_op(D.STATE, terminated(True)))) == ("CLARIFY", "UNRESOLVED_VALUE:STATE")
    processes = base((D.TOPIC, ASEO))
    assert said(answer(processes, set_op(D.STATE, terminated(True)))) == ("EXECUTE", None)
    inverted = base((D.STATE, terminated(False)), subjects=(CONTRACTS,))  # F16 would place −Terminado
    outcome = answer(inverted, set_op(D.STATE, terminated(True)))
    assert said(outcome) == ("CLARIFY", "UNRESOLVED_VALUE:STATE") and not outcome.effects


def test_a_change_of_universe_with_carried_policies_is_one_cause():
    policies = (HostPolicy("ADR-100", "1", ("c1",), Effect.EQUIVALENT),
                HostPolicy("topic-aseo-morph", "1", ("c0",), Effect.EQUIVALENT))
    b = base((D.TOPIC, ASEO), (D.STATE, OPEN), policies=policies)
    outcome = answer(b, to(CONTRACTS))
    assert said(outcome) == ("CLARIFY", "UNRESOLVED_VALUE:UNIVERSE")
    assert [c.code for c in outcome.causes] == ["OP_NOT_ACCREDITED:UNIVERSE"]
    assert outcome.pending.answer_slots == {fa.UNIVERSE_SLOT}
    one = base((D.TOPIC, ASEO), policies=policies[1:])
    assert said(answer(one, to(CONTRACTS))) == ("CLARIFY", "UNRESOLVED_VALUE:UNIVERSE")
    dual = base((D.TOPIC, ASEO), subjects=(PROCESSES, CONTRACTS), policies=policies[1:])
    kept = answer(dual, to(CONTRACTS))  # a dual anchor reduced keeps its own request: nothing is re-decided
    assert said(kept) == ("EXECUTE", None) and kept.effective.policies == policies[1:]


def test_a_change_of_universe_carrying_a_condition_that_needs_a_policy_is_refused():
    family = ModalityValue(("licitacion_publica",), "modality-lp-family@1", False)
    assert said(answer(base((D.TOPIC, ASEO), (D.MODALITY, family)), to(CONTRACTS))) == (
        "CLARIFY", "UNRESOLVED_VALUE:UNIVERSE")
    negated = base((D.TOPIC, ASEO), (D.STATE, terminated(True)))
    assert said(answer(negated, to(CONTRACTS))) == ("CLARIFY", "UNRESOLVED_VALUE:UNIVERSE")


def test_the_sf075_shape_is_dep_o1b_on_the_universe_with_or_without_time():
    policy = (HostPolicy("topic-aseo-morph", "1", ("c0",), Effect.EQUIVALENT),)
    with_time = base((D.TOPIC, ASEO), (D.TIME, year(2025)), policies=policy)
    outcome = answer(with_time, to(PROCESSES, OPPORTUNITIES))
    assert said(outcome) == ("CLARIFY", "UNRESOLVED_VALUE:UNIVERSE") and len(outcome.causes) == 1
    assert said(answer(base((D.TOPIC, ASEO)), to(PROCESSES, OPPORTUNITIES))) == (
        "CLARIFY", "UNRESOLVED_VALUE:UNIVERSE")


def test_the_o1b_lens_counts_only_a_change_of_the_state():
    lens = base((D.TOPIC, ASEO), (D.STATE, OPEN), kind=OPPORTUNITIES)
    assert said(answer(lens, set_op(D.STATE, OPEN))) == ("CLARIFY", "UNRESOLVED_VALUE:STATE")
    assert said(answer(lens, set_op(D.STATE, terminated(True)))) == ("CLARIFY", "UNRESOLVED_VALUE:STATE")
    cleared = answer(lens, clear(STATE_POS))
    assert said(cleared) == ("CLARIFY", "UNRESOLVED_VALUE:STATE")
    wide = answer(lens, clear(STATE))  # one operation, one cause, even if it covers two slots
    assert [c.code for c in wide.causes] == ["OP_NOT_ACCREDITED:STATE"]
    assert said(answer(lens, op(fa.PatchOpKind.KEEP, STATE_POS), set_op(D.TOPIC, VIAS, 1))) == ("EXECUTE", None)
    stateless = base((D.TOPIC, ASEO), kind=OPPORTUNITIES)  # a CLEAR of a STATE the anchor lacks changes nothing
    assert said(answer(stateless, clear(STATE), set_op(D.TOPIC, VIAS, 1))) == ("EXECUTE", None)
    assert said(answer(lens, set_op(D.TOPIC, VIAS))) == ("EXECUTE", None)


def test_an_operation_on_a_group_is_one_target_cause_and_substitution_victims_count():
    f16 = base((D.TOPIC, ASEO), (D.BUYER, ICBF, NATIONAL), group=(0, 1))
    negated = set_op(D.TOPIC, TopicValue("aseo", False, True))
    outcome = answer(f16, negated)  # the positive it would invert sits inside the OR group
    assert outcome.code == "TARGET_NOT_ADDRESSABLE" and len(outcome.causes) == 1
    a81 = base((D.TOPIC, ASEO), (D.VALUE, MoneyValue(500, Comparator.LT), PESOS), group=(0, 1))
    outcome = answer(a81, set_op(D.VALUE, MoneyValue(900, Comparator.GTE), provenance=PESOS))
    assert outcome.code == "TARGET_NOT_ADDRESSABLE" and len(outcome.causes) == 1
    both = base((D.TOPIC, ASEO), (D.TOPIC, TopicValue("aseo", False, True)), group=(0, 1))
    again = answer(both, set_op(D.TOPIC, TopicValue("aseo", False, True)))
    assert [c.code for c in again.causes] == ["TARGET_NOT_ADDRESSABLE"]  # its slot and its victim: one operation


# ---- H3, H8, H11, H12 ------------------------------------------------------------------------------------------------
SMMLV_BASE = base((D.TOPIC, ASEO), (D.VALUE, MoneyValue(100, Comparator.GTE), smmlv(2025)))


def test_h3_a_smmlv_bound_needs_one_catalogued_year():
    assert said(answer(SMMLV_BASE, set_op(D.TIME, year(2023)))) == ("CLARIFY", "SMMLV_YEAR_AMBIGUOUS")
    assert said(answer(SMMLV_BASE, set_op(D.TIME, year(2024, 2025)))) == ("CLARIFY", "SMMLV_YEAR_AMBIGUOUS")
    assert said(answer(SMMLV_BASE, set_op(D.TIME, year(2025)))) == ("EXECUTE", None)
    recalc = answer(SMMLV_BASE, set_op(D.TIME, year(2026)))  # E-VALUE recalculates from source_amount
    assert recalc.disposition is fo.DeltaDisposition.DEPENDENCY and recalc.dependency.dep_id == "DEP-SMMLV-RECALC"


def test_h8_a_state_or_a_modality_over_a_changed_universe():
    outcome = answer(base((D.TOPIC, ASEO), (D.STATE, OPEN)), to(CONTRACTS))
    assert said(outcome) == ("CLARIFY", "STATE_NOT_IN_UNIVERSE") and outcome.pending.answer_slots == {fa.UNIVERSE_SLOT}
    modality = answer(base((D.TOPIC, ASEO), (D.MODALITY, ModalityValue(("minima_cuantia",), None, False))),
                      to(CONTRACTS))
    assert modality.disposition is fo.DeltaDisposition.DEPENDENCY
    assert modality.dependency.dep_id == "DEP-MODALITY-UNIVERSE"


def test_h11_and_h12():
    assert said(answer(base((D.TOPIC, ASEO)), clear(TOPIC_POS))) == ("CLARIFY", "EMPTY_SEARCH_AFTER_DELTA")
    assert said(answer(base((D.TOPIC, ASEO)), clear(TIME_POS))) == ("INFORM", "CLEAR_ON_EMPTY_SLOT")


# ---- F1: a §19 COMPOSICION dependency is never laundered through the frame of a pending --------------------------
LITERAL = ModalityValue(("minima_cuantia",), None, False)
SMMLV_TIME_ORDER = base((D.TOPIC, ASEO), (D.VALUE, MoneyValue(100, Comparator.GTE), smmlv(2025)), (D.TIME, year(2025)),
                        (D.ORDER, ORDER_VALUE), (D.CARDINALITY, TOP_10))


def depends_on(outcome, dep_id: str) -> bool:
    return outcome.disposition is fo.DeltaDisposition.DEPENDENCY and outcome.dependency.dep_id == dep_id


def test_h6_with_a_change_of_universe_that_carries_a_modality_is_its_dependency():  # zeus_int p1_h6_dep
    b = base((D.TOPIC, ASEO), (D.BUYER, VALLE, SCOPE_76), (D.MODALITY, LITERAL))
    assert depends_on(answer(b, to(CONTRACTS)), "DEP-MODALITY-UNIVERSE")  # the same turn without a pending
    assert depends_on(answer(b, set_op(D.GEOGRAPHY, ANTIOQUIA), to(CONTRACTS, i=1)), "DEP-MODALITY-UNIVERSE")


def test_r3_1_with_a_dependency_in_its_frame_is_that_dependency():  # zeus_int p2_dep_launder A, B, B2
    with_modality = base((D.TOPIC, ASEO), (D.MODALITY, LITERAL), (D.ORDER, ORDER_VALUE), (D.CARDINALITY, TOP_10))
    assert depends_on(answer(with_modality, clear(ORDER), to(CONTRACTS, i=1)), "DEP-MODALITY-UNIVERSE")
    assert depends_on(answer(SMMLV_TIME_ORDER, clear(TIME_POS), clear(ORDER, 1)), "DEP-SMMLV-RECALC")
    assert depends_on(answer(SMMLV_TIME_ORDER, set_op(D.TIME, year(2026)), clear(ORDER, 1)), "DEP-SMMLV-RECALC")
    assert said(answer(SMMLV_TIME_ORDER, clear(ORDER))) == ("CLARIFY", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER")


def test_one_cause_whose_frame_drops_the_time_of_a_smmlv_bound_is_the_recalc():  # zeus_int p9_more
    b = base((D.TOPIC, ASEO), (D.VALUE, MoneyValue(100, Comparator.GTE), smmlv(2025)), (D.TIME, year(2025)))
    outcome = answer(b, clear(TIME_POS), op(fa.PatchOpKind.KEEP, GEO_POS, 1))
    assert depends_on(outcome, "DEP-SMMLV-RECALC")  # never a pending whose answer runs another year's wage (§15.5)
    assert said(answer(b, op(fa.PatchOpKind.KEEP, GEO_POS))) == ("CLARIFY", "KEEP_ON_EMPTY_SLOT")


def test_h6_with_a_dependency_first_is_that_dependency():  # zeus_int p2_dep_launder C
    b = base((D.TOPIC, ASEO), (D.BUYER, VALLE, SCOPE_76), (D.VALUE, MoneyValue(100, Comparator.GTE), smmlv(2025)),
             (D.TIME, year(2025)))
    assert depends_on(answer(b, set_op(D.GEOGRAPHY, ANTIOQUIA), clear(TIME_POS, 1)), "DEP-SMMLV-RECALC")


def test_a_relative_turn_whose_rest_raises_a_dependency_is_that_dependency():  # zeus_int p9_more, H4
    redirect = to(CONTRACTS)
    outcome = fo.relative_outcome(base((D.TOPIC, ASEO), (D.MODALITY, LITERAL)), (redirect,), base_trace_id="t0",
                                  origin=redirect.origin, answer_slots=frozenset({LOWER}), anchor_expired=False,
                                  pending_present=False)
    assert depends_on(outcome, "DEP-MODALITY-UNIVERSE")
    lawful = fo.relative_outcome(base((D.TOPIC, ASEO)), (redirect,), base_trace_id="t0", origin=redirect.origin,
                                 answer_slots=frozenset({LOWER}), anchor_expired=False, pending_present=False)
    assert said(lawful) == ("CLARIFY", "RELATIVE_TO_PREVIOUS_VALUE")
    assert [r.subject for r in lawful.pending.frame.requests] == [CONTRACTS]
    assert said(fo.relative_outcome(base((D.TOPIC, ASEO)), (redirect,), base_trace_id="t0", origin=redirect.origin,
                                    answer_slots=frozenset({LOWER}), anchor_expired=False, pending_present=True)) == (
        "INFORM", "RELATIVE_TO_PREVIOUS_VALUE")


def test_the_law_names_a_frame_that_raises_a_dependency():
    b = base((D.TOPIC, ASEO), (D.MODALITY, LITERAL))
    redirect = to(CONTRACTS)
    assert fo.law_violations(b, (redirect,), (), frozenset({LOWER}), "t0", redirect.origin) == [
        "frame raises a §19 dependency"]


# ---- F3 and F4 (ZEUS on the integration) ------------------------------------------------------------------------------
def test_the_policy_a_placed_condition_needs_is_judged_on_the_universe_the_patch_leaves():  # zeus_int p6_misc
    processes = base((D.TOPIC, ASEO))
    outcome = answer(processes, set_op(D.STATE, terminated(True)), to(CONTRACTS, i=1))
    assert said(outcome) == ("CLARIFY", "UNRESOLVED_VALUE:STATE")
    contracts = base((D.TOPIC, ASEO), subjects=(CONTRACTS,))  # leaving contracts: no complement to accredit, but
    left = answer(contracts, set_op(D.STATE, terminated(True)), to(PROCESSES, i=1))  # Terminado is no process state
    assert said(left) == ("INFORM", "STATE_NOT_IN_UNIVERSE") and not left.causes


def test_a_dual_anchor_reduced_is_a_change_of_universe_for_h8():  # §14 and the model (zeus_int p8_model_dual)
    dual_modality = base((D.TOPIC, ASEO), (D.MODALITY, LITERAL), subjects=(PROCESSES, CONTRACTS))
    for subject in (PROCESSES, CONTRACTS):
        assert depends_on(answer(dual_modality, to(subject)), "DEP-MODALITY-UNIVERSE")
    dual_terminated = base((D.TOPIC, ASEO), (D.STATE, terminated(False)), subjects=(PROCESSES, CONTRACTS))
    assert said(answer(dual_terminated, to(PROCESSES))) == ("CLARIFY", "STATE_NOT_IN_UNIVERSE")  # no process literal
    assert said(answer(dual_terminated, to(CONTRACTS))) == ("EXECUTE", None)


# ---- §7.1–§7.2 and search_effects ------------------------------------------------------------------------------------
def facts(**kw) -> TurnFacts:
    defaults = dict(input_kind=InputKind.TEXT, anchor=AnchorState.FOUNDATION_SEARCH, anchor_expired=False,
                    pending=False, continuation=False, new_content=True, frame=True, admissible_alone=True,
                    own_conditions=1, within_answer_slots=False)
    return TurnFacts(**{**defaults, **kw})


def test_classify_turn_rows_the_table_reads_only_in_aggregate():
    assert classify_turn(facts()).context_reset is True
    assert classify_turn(facts(anchor_expired=True)).context_reset is False  # «con ancla vigente» (§7.2)
    unreadable = classify_turn(facts(anchor=AnchorState.UNREADABLE, admissible_alone=False))
    assert (unreadable.scope, unreadable.code) == (fa.TurnScope.OUT_NO_ANCHOR, "ANCHOR_UNREADABLE")
    assert classify_turn(facts(unsupported_motive="NO_ACTION")).scope is fa.TurnScope.OUT_UNSUPPORTED_PROPOSAL
    nothing = dict(unsupported_motive="NO_ACTION", own_conditions=0, frame=False, new_content=False)
    assert classify_turn(facts(**nothing, pending=True)).scope is fa.TurnScope.IN  # §8.3 row 8, not row 4b
    assert classify_turn(facts(pending=True, own_conditions=0)).scope is fa.TurnScope.IN  # D7's floor
    assert classify_turn(facts(pending=True, within_answer_slots=True)).scope is fa.TurnScope.IN  # §8.3 row 3
    assert classify_turn(facts(pending=True)).scope is fa.TurnScope.IN_NEW_SEARCH


def test_a_new_framed_but_not_admissible_turn_is_a_delta():  # P3: §7.1 row 8, INV-M2A-20
    got = classify_turn(facts(admissible_alone=False))
    assert (got.scope, got.resolution) == (fa.TurnScope.IN, fa.TurnResolution.DELTA)


def test_search_effects():
    anchor = base((D.TOPIC, ASEO))
    vias = (condition(D.TOPIC, VIAS, "n1", ("r1",), origin("t1", 0, RAW_TURN), rich=False),)
    aseo = (condition(D.TOPIC, ASEO, "n1", ("r1",), origin("t1", 0, RAW_TURN), rich=False),)
    assert search_effects(anchor, vias, anchor_expired=False, pending_cancelled=False) == {
        "CONTEXT_RESET", "TOPIC_REPLACED"}
    assert search_effects(anchor, aseo, anchor_expired=True, pending_cancelled=True) == {"PENDING_CANCELLED"}
    assert search_effects(None, vias, anchor_expired=False, pending_cancelled=False) == frozenset()
