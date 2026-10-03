"""M2A-4: one DIRECT test per rule of the pending protocol, written from the contract's words, plus its mutants.

Scope (contract §8.1–§8.4, §10.1, §17; ``M2A4_CLOSE.md`` M2A4-D1…D18). The tests reach the five modules through ONE
namespace, ``P`` (``load()``), so every ``@direct`` test is also the kill-suite of the mutants at the bottom: a mutant
is a one-line change of a module's source, loaded under private names, swapped in for ``P`` and judged by the SAME
tests. A mutant that no test notices is a test that does not count (``test_every_mutant_is_killed``).

* §8.2, §8.4: creation (span, candidates resolved at ask time, plazo of 30 minutes, ``asks = 1``), the law of the
  possible pending (an impossible frame or candidate is NEVER a pending: ``INFORM``, 0 calls, nothing built), the frame
  that raises a §19 dependency (U29), ``ask_authority`` (M2A4-D6), ``ask_fresh``, ``pending_without_anchor``.
* §8.3 rows 1 and 4–8 and «Selección» (U22, U26, U27), F24/F25/F29, ``KEEP`` identity, exact identity of candidates,
  the answer composed on the FRAME and naming the asking turn as its base, the declared costs (M2A4-D4, D5).
* §17 and §10.1: the five causes, ``control`` ⇔ ``cancellation`` (INV-M2A-23), «reset», the rest of P after a cancel
  (M2A4-D1), an empty list is never a cancellation, the expired anchor (M2A4-D3), expiry over an injected instant, the
  cap of ``asks``.
"""
from __future__ import annotations

import importlib
import inspect
import sys
import types
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
from app.core.foundation_authority_values import (
    ActorValue, BuyerKind, BuyerValue, CardinalityBasis, CardinalityValue, Comparator, ContractTypeValue, GeoValue,
    ModalityValue, MoneyValue, OrderDirection, OrderKey, OrderValue, StateValue, TimeValue, TopicValue,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Operation, Subject
from app.core.foundation_turn_scope import AnchorState, InputKind, ReferentSignal, TurnFacts
from app.core.interpretation_contract import ContractError, Polarity, ValueSide
from app.core.semantic_dimensions import SemanticDimension as D

ROOT = Path(__file__).resolve().parents[1]
RAW_BASE, RAW_TURN = "a" * 64, "b" * 64
T0 = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
POS, NEG = Polarity.POSITIVE, Polarity.NEGATIVE
MINUTE = timedelta(minutes=1)
STEMS = ("foundation_pending_clock", "foundation_pending_cancel", "foundation_pending_answer",
         "foundation_pending_create", "foundation_pending")  # in import order


# ---- the modules under test, through one namespace (so a mutant can be swapped in) ----------------------------------
def load(mutations: dict[str, list[tuple[str, str]]] | None = None) -> SimpleNamespace:
    """The five modules of M2A-4 as ONE namespace of public names. ``mutations`` maps a module stem to ``(old, new)``
    source replacements: the mutated modules are loaded under private names and import each other, never the real ones."""
    modules = []
    for stem in STEMS:
        if not mutations:
            modules.append(importlib.import_module(f"app.core.{stem}"))
            continue
        source = (ROOT / "app/core" / f"{stem}.py").read_text("utf-8")
        for other in STEMS:
            source = source.replace(f"from app.core.{other} import", f"from m2a4_mut_{other} import")
        for old, new in mutations.get(stem, ()):
            assert source.count(old) == 1, f"mutation anchor not unique in {stem}: {old!r}"
            source = source.replace(old, new)
        module = types.ModuleType(f"m2a4_mut_{stem}")
        module.__file__ = f"<m2a4_mut_{stem}>"
        sys.modules[module.__name__] = module
        exec(compile(source, module.__file__, "exec"), module.__dict__)
        modules.append(module)
    names = {}
    for module in modules:
        names.update({k: v for k, v in vars(module).items() if not k.startswith("_")})
    return SimpleNamespace(**names)


P = load()
DIRECT = []


def direct(function):
    """A test that is also part of the kill-suite of the mutants (no fixtures, no parameters)."""
    DIRECT.append(function)
    return function


# ---- typed fixtures --------------------------------------------------------------------------------------------------
def origin(trace: str, start: int, raw: str) -> fa.ConditionOrigin:
    return fa.ConditionOrigin(fa.OriginKind.SPAN, trace, raw, (start, start + 2))


def _time(year: int, excluded: bool) -> TimeValue:
    return TimeValue(date(year, 1, 1), date(year + 1, 1, 1), "year", excluded)


_LITERALS = (("jbjy-vk9h", "estado_contrato", ("Activo",)), ("p6dx-8zbt", "estado_del_procedimiento", ("Abierto",)))
TOKENS = ("x", "y", "z", "w")
MONEY_PROVENANCE = fa.Provenance(unit=fa.Unit.PESOS, unit_source=fa.UnitSource.STATED)


def typed_value(dimension: D, token: str, excluded: bool = False):
    """The typed value a symbolic token stands for in ``dimension`` (the same token, the same canonical value)."""
    i = TOKENS.index(token)
    if dimension is D.TOPIC:
        return TopicValue(("aseo", "vias", "obras", "agua")[i], False, excluded)
    if dimension is D.BUYER:
        name = ("ICBF", "VALLE", "MEDELLIN", "CALI")[i]
        return BuyerValue(name, BuyerKind.UNIT, (name,), None, excluded)
    if dimension is D.ACTOR:
        return ActorValue(("900123456", "800222333", "700333444", "600444555")[i], "NIT", f"Actor {token}", excluded)
    if dimension is D.GEOGRAPHY:
        name, code = (("VALLE", "76"), ("ANTIOQUIA", "05"), ("CUNDINAMARCA", "25"), ("SANTANDER", "68"))[i]
        return GeoValue(GeoLevel.DEPARTMENT, name, None, code, (), excluded)
    if dimension is D.TIME:
        return _time((2025, 2024, 2026, 2023)[i], excluded)
    if dimension is D.STATE:
        return StateValue(("open", "published", "closed", "awarded")[i], _LITERALS, excluded)
    if dimension is D.MODALITY:
        return ModalityValue((("minima_cuantia",), ("seleccion_abreviada",), ("contratacion_directa",),
                              ("concurso_meritos",))[i], None, excluded)
    if dimension is D.CONTRACT_TYPE_OR_NATURE:
        return ContractTypeValue((("obra",), ("servicios",), ("suministro",), ("consultoria",))[i], excluded)
    raise AssertionError(dimension)


def condition(dimension: D, value, cid: str, org: fa.ConditionOrigin, rids=("r1",), provenance=None):
    encoded = value.encode()
    slot = fa.slot_of(dimension, encoded)
    if dimension is D.VALUE:
        operator, provenance = fa.VALUE_OPERATOR_OF[value.comparator], provenance or MONEY_PROVENANCE
    elif dimension is D.ORDER:
        operator = f"{value.key.value}_{value.direction.value}"
    elif dimension is D.CARDINALITY:
        operator = "top_n"
    elif dimension is D.TOPIC:
        operator = "whole_token_folded" if value.token_mode else "contains"
    elif dimension is D.TIME:
        operator = "during_year" if value.precision == "year" else "between_dates"
    else:
        operator = "is"
    return fa.AcceptedCondition(cid, tuple(sorted(rids)), slot, dimension, operator, slot.polarity or POS,
                                value.level if dimension is D.GEOGRAPHY else None, encoded,
                                f"{dimension.value.lower()}_authority@1", (), (), org, provenance or fa.Provenance())


def request(subject: Subject = Subject.CONTRACTS, rid: str = "r1"):
    return fa.AcceptedRequest(rid, "s1", subject, fa.UniverseKind.RECORDS, origin("t0", 0, RAW_BASE), None,
                              fd.SUBJECT_DATASET[subject])


def base_of(*specs, subjects=(Subject.CONTRACTS,)) -> fa.AcceptedInterpretation:
    """A base (the anchor): ``specs`` are ``(dimension, token[, excluded])``, or ``(D.ORDER|D.CARDINALITY, value)``,
    or ``(D.VALUE, value, provenance)``."""
    requests = tuple(request(s, f"r{i}") for i, s in enumerate(subjects, 1))
    rids = tuple(r.request_id for r in requests)
    conditions = []
    for i, spec in enumerate(specs):
        dimension, token, *rest = spec
        value = token if dimension in (D.ORDER, D.CARDINALITY, D.VALUE) else typed_value(dimension, token, *rest)
        provenance = rest[0] if dimension is D.VALUE and rest else None
        conditions.append(condition(dimension, value, f"c{i + 1}", origin("t0", 3 * i + 3, RAW_BASE), rids, provenance))
    return fa.canonical_interpretation(requests, tuple(conditions), None, (), None, "t-root")


ORDER_VALUE = OrderValue(OrderKey.VALUE, OrderDirection.DESC)
ORDER_DATE = OrderValue(OrderKey.DATE, OrderDirection.DESC)
TOP_10 = CardinalityValue(10, CardinalityBasis.FIGURE, 10)
TOP_20 = CardinalityValue(20, CardinalityBasis.FIGURE, 20)


def slot(dimension: D, polarity: Polarity | None = None, side: ValueSide | None = None) -> fa.Slot:
    return fa.Slot(dimension, polarity, side)


def _org(i: int) -> fa.ConditionOrigin:
    return origin("t1", 3 * i, RAW_TURN)


def set_op(dimension: D, token, excluded: bool = False, i: int = 0) -> fa.PatchOp:
    value = token if dimension in (D.ORDER, D.CARDINALITY, D.VALUE) else typed_value(dimension, token, excluded)
    cond = condition(dimension, value, f"p{i + 1}", _org(i))
    kind = fa.PatchOpKind.SET_ORDER if dimension is D.ORDER else fa.PatchOpKind.SET
    return fa.PatchOp(kind, cond.slot, cond, None, cond.origin)


def clear_op(target: fa.Slot, i: int = 0) -> fa.PatchOp:
    return fa.PatchOp(fa.PatchOpKind.CLEAR, target, None, None, _org(i))


def keep_op(target: fa.Slot, i: int = 0) -> fa.PatchOp:
    return fa.PatchOp(fa.PatchOpKind.KEEP, target, None, None, _org(i))


def universe_op(subject: Subject, i: int = 0) -> fa.PatchOp:
    target = fa.UniverseTarget(subject, fa.UniverseKind.RECORDS)
    return fa.PatchOp(fa.PatchOpKind.REPLACE_UNIVERSE, fa.UNIVERSE_SLOT, None, target, _org(i))


TOPIC_POS, BUYER_POS, GEO_POS = slot(D.TOPIC, POS), slot(D.BUYER, POS), slot(D.GEOGRAPHY, POS)
TIME_POS, ACTOR_POS = slot(D.TIME, POS), slot(D.ACTOR, POS)
ORDER_SLOT, CARD_SLOT = slot(D.ORDER), slot(D.CARDINALITY)


def facts(**kw) -> TurnFacts:
    """The closed facts of an ANSWER (not a new search, not a continuation of the anchor) that meets a pending."""
    values = dict(input_kind=InputKind.TEXT, anchor=AnchorState.FOUNDATION_SEARCH, anchor_expired=False, pending=True,
                  continuation=False, new_content=False, frame=False, admissible_alone=False, own_conditions=1)
    values.update(kw)
    return TurnFacts(**values)


def new_search_facts(**kw) -> TurnFacts:
    """P is ¬Continuación ∧ Marco ∧ Admisible sola (§8.3 row 3), and not an answer unless ``within_answer_slots``."""
    return facts(**{**dict(continuation=False, new_content=True, frame=True, admissible_alone=True, own_conditions=2,
                           within_answer_slots=False), **kw})


def turn(*ops, **kw) -> object:
    flags = {k: kw.pop(k) for k in ("no_selection", "unresolved") if k in kw}
    return P.PendingTurn(kw.pop("facts", None) or facts(), tuple(ops), **flags)


def ask(outcome, base, *, trace="t1", now=T0, origin_=None):
    """The ``Creation`` of a delta outcome that asked (the pending M2A-3 describes, built under the law)."""
    assert outcome.disposition is fo.DeltaDisposition.CLARIFY, outcome
    return P.pending_from_question(outcome.pending, base=base, base_trace_id="t0", asked_trace_id=trace, now=now,
                                   origin=origin_)


def delta_pending(base, *ops, expired=False, trace="t1", now=T0):
    patch = fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT)
    return ask(fo.delta_outcome(base, patch, base_trace_id="t0", anchor_expired=expired), base, trace=trace, now=now)


def kinds(composed) -> list[str]:
    return [c.slot.key for c in composed.effective.conditions]


def values_of(composed, key: str) -> list:
    return [c.value for c in composed.effective.conditions if c.slot.key == key]


def respond(pending, t, *, now=T0 + MINUTE, trace="t2", asked_ops=None):
    """``asked_ops`` are the operations of the turn that asked (F24). ``None`` or ``()``: the test says the asking turn had
    no typed operation to compare with (``no_asked_patch``), as a real caller must say it (M2A4-D22)."""
    asked = tuple(asked_ops or ())
    return P.respond(pending, t, trace_id=trace, now=now, asked_ops=asked, no_asked_patch=not asked)


# ---- the pendings the tests keep asking about --------------------------------------------------------------------
def h6():
    """§6 H6 (F30): a new territory next to a carried territorial buyer of another territory: two candidates."""
    base = base_of((D.TOPIC, "x"), (D.BUYER, "x"), (D.TIME, "x"))
    base = replace(base, conditions=tuple(
        replace(c, provenance=fa.Provenance(buyer_scope="76")) if c.slot == BUYER_POS else c
        for c in base.conditions))
    return base, delta_pending(base, set_op(D.GEOGRAPHY, "y"))  # Antioquia (05) vs the buyer of Valle (76)


def r31():
    """§6.7 (R3-1): ``CLEAR(ORDER)`` with the CARDINALITY carried; without E-ORDER only C1 «quitar orden y límite»."""
    base = base_of((D.TOPIC, "x"), (D.ORDER, ORDER_VALUE), (D.CARDINALITY, TOP_10))
    return base, delta_pending(base, clear_op(ORDER_SLOT))


def h1():
    """§6 H1 (§10.1): a delta over an expired anchor asks to confirm: the one candidate is «continuar»."""
    base = base_of((D.TOPIC, "x"), (D.BUYER, "x"), (D.TIME, "x"))
    return base, delta_pending(base, set_op(D.TIME, "y"), expired=True)


def h11():
    """§6 H11 (F19): the delta leaves nothing: no candidate, the whole turn is the open span, frame = the base."""
    base = base_of((D.TOPIC, "x"))
    return base, delta_pending(base, clear_op(slot(D.TOPIC)))


def authority_month():
    """H10 (F7), an authority's reading: a month with no year over a plurianual TIME. The resolved part of P (a new
    buyer) is the frame; the open slot is TIME. The frame DIFFERS from the base (the answer must compose on it)."""
    base = base_of((D.TOPIC, "x"), (D.BUYER, "x"), (D.TIME, "x"))
    resolved = (set_op(D.BUYER, "y", i=1),)
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0, frame_ops=resolved)
    return base, made


def h5(label: str = "BUYER"):
    """H5 (F12), an authority's reading: «con X» after an ACTOR, X a buyer or a place. The producer's provisional label
    is the open operation (the asking patch): which candidate it equals depends on it (M2A4-D5)."""
    base = base_of((D.ACTOR, "x"))
    buyer, place = set_op(D.BUYER, "y"), set_op(D.GEOGRAPHY, "y")
    question = P.AuthorityQuestion("BUYER_OR_PLACE_AFTER_ACTOR", origin("t1", 0, RAW_TURN),
                                   frozenset({BUYER_POS, GEO_POS}), ((buyer,), (place,)))
    made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0,
                           open_ops=(buyer if label == "BUYER" else place,))
    return base, made, buyer, place


def no_anchor():
    return P.pending_without_anchor("NO_ANCHOR", asked_trace_id="t1", now=T0)


# ---- creation (§8.1, §8.2, §8.4) -----------------------------------------------------------------------------------
@direct
def test_creation_builds_the_pending_of_h6_with_its_span_candidates_and_plazo():
    base, made = h6()
    p = made.pending
    assert made.disposition.value == "CLARIFY" and made.code == "TERRITORY_VS_CARRIED_BUYER"
    assert (p.pending_id, p.asked_trace_id, p.base_trace_id, p.asks) == ("pending-t1", "t1", "t0", 1)
    assert (p.created_at_utc, p.expires_at_utc) == (T0, T0 + timedelta(minutes=30))  # §8.2: 30 minutes from creation
    assert p.frame == base  # frame_ops = (): the open operation is the whole turn
    assert (p.ambiguous_span, p.ambiguous_raw_sha256) == ((0, 2), RAW_TURN)  # the citation of the open operation
    assert [c.candidate_id for c in p.candidate_patches] == ["c1", "c2"]
    change_buyer, both = (tuple((o.op.value, o.slot.key) for o in c.patch.ops) for c in p.candidate_patches)
    assert change_buyer == (("CLEAR", "BUYER/POSITIVE"), ("SET", "GEOGRAPHY/POSITIVE")) and both == (
        ("SET", "GEOGRAPHY/POSITIVE"),)
    assert {o.slot for c in p.candidate_patches for o in c.patch.ops} <= p.answer_slots
    assert [(o.op.value, o.slot.key) for o in made.asked_ops] == [("SET", "GEOGRAPHY/POSITIVE")]


@direct
def test_creation_expired_confirm_has_the_anchor_as_frame_one_candidate_and_no_span():
    base, made = h1()
    p = made.pending
    assert (made.code, p.reason, p.frame, p.base_trace_id) == ("CONTEXT_EXPIRED_CONFIRM",) * 2 + (base, "t0")
    assert (p.ambiguous_span, p.ambiguous_raw_sha256) == (None, None)  # §8.1: not born of a span
    assert len(p.candidate_patches) == 1  # «continuar» = the turn's own patch on the expired anchor
    assert [(o.op.value, o.slot.key) for o in p.candidate_patches[0].patch.ops] == [("SET", "TIME/POSITIVE")]
    assert p.answer_slots == {TIME_POS}


@direct
def test_creation_r31_keeps_the_resolved_part_in_the_frame_and_asks_with_c1_only():
    base, made = r31()
    p = made.pending
    assert p.reason == "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER" and p.frame == base
    assert [tuple(sorted((o.op.value, o.slot.key) for o in c.patch.ops)) for c in p.candidate_patches] == [
        (("CLEAR", "CARDINALITY"), ("CLEAR", "ORDER"))]
    assert {s.key for s in p.answer_slots} == {"ORDER", "CARDINALITY"}
    assert p.ambiguous_span == (0, 2)  # the citation that takes the ORDER away


@direct
def test_creation_with_resolved_operations_composes_them_into_the_frame():
    base, made = authority_month()
    p = made.pending
    assert p.frame != base and p.frame.base_trace_id == "t0"
    assert {c.slot.key: c.value for c in p.frame.conditions}["BUYER/POSITIVE"] == typed_value(D.BUYER, "y").encode()
    assert p.answer_slots == {TIME_POS} and p.candidate_patches == ()
    assert [(o.op.value, o.slot.key) for o in made.asked_ops] == [("SET", "BUYER/POSITIVE")]  # F24 reads these


@direct
def test_the_plazo_is_thirty_minutes_from_the_injected_instant_and_the_instant_is_validated():
    assert P.PENDING_TTL == timedelta(minutes=30)
    for now in (T0, T0 + timedelta(hours=5, microseconds=7)):
        assert P.expires_at(now) == now + timedelta(minutes=30)
        assert no_anchor().pending.expires_at_utc == T0 + timedelta(minutes=30)
    for bad in (datetime(2026, 10, 3, 12, 0), T0.astimezone(timezone(timedelta(hours=2))), "now", None):
        for call in (lambda: P.expires_at(bad), lambda: P.pending_without_anchor("NO_ANCHOR", asked_trace_id="t1", now=bad)):
            with pytest.raises(ContractError):
                call()


def _question_with(base, candidate_ops=(), frame_ops=(), reason="UNIT_AMBIGUOUS", open_ops=None):
    frame = base if not frame_ops else fd.compose_ops(base, frame_ops, base_trace_id="t0").effective
    slots = set(P.AuthorityQuestion(reason, origin("t1", 0, RAW_TURN), frozenset({TIME_POS})).answer_slots)
    candidates = tuple(candidate_ops)
    for c in candidates:
        slots |= {o.slot for o in c} | {fd.SLOT_BY_KEY[key] for key in fd.covered_keys(c)}
    return fo.PendingQuestion(reason, frame, tuple(frame_ops), tuple(open_ops or ()), candidates, frozenset(slots))


@direct
def test_the_law_never_builds_a_pending_with_an_impossible_candidate():
    """§8.4 (2): a candidate that does not compose on the frame (a KEEP of a slot the frame does not hold) is no option."""
    base = base_of((D.TOPIC, "x"))
    question = _question_with(base, candidate_ops=[(keep_op(BUYER_POS),)], open_ops=(set_op(D.TIME, "x"),))
    made = P.pending_from_question(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert (made.disposition.value, made.code, made.pending) == ("INFORM", "UNIT_AMBIGUOUS", None)
    assert any("candidate" in v for v in made.violations)


@direct
def test_the_law_never_builds_a_pending_without_an_executing_answer():
    """§8.4 (3), R3c: every candidate composes but none executes (here: each leaves the search empty)."""
    base = base_of((D.TOPIC, "x"))
    only = (clear_op(slot(D.TOPIC)),)
    question = _question_with(base, candidate_ops=[only], open_ops=(set_op(D.TIME, "x"),))
    made = P.pending_from_question(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert made.disposition.value == "INFORM" and made.pending is None
    assert "no answer executes" in made.violations


@direct
def test_the_law_refuses_a_frame_with_a_cause_and_a_frame_that_holds_a_question():
    base = base_of((D.TOPIC, "x"))
    conflict = (set_op(D.TIME, "x"), set_op(D.TIME, "y", i=1))  # two writers: DELTA_SLOT_CONFLICT
    frame = fd.compose_ops(base, (set_op(D.TIME, "x"),), base_trace_id="t0").effective
    question = fo.PendingQuestion("UNIT_AMBIGUOUS", frame, conflict, (), (), frozenset({TIME_POS}))
    with pytest.raises(ContractError):  # the question's frame is not base + its frame_ops: never trusted
        P.pending_from_question(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    asks = _question_with(base, frame_ops=(clear_op(slot(D.TOPIC)),), open_ops=(set_op(D.TIME, "x"),))
    made = P.pending_from_question(asks, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert made.disposition.value == "CLARIFY"  # H11 (the whole turn) is the one question a frame may hold


@direct
def test_a_frame_that_raises_a_section_19_dependency_is_that_dependency_not_a_pending():
    """U29, F1: the frame is read FROM THE BASE; a change of universe with a MODALITY carried is DEP-MODALITY-UNIVERSE."""
    base = base_of((D.TOPIC, "x"), (D.MODALITY, "x"))
    redirect = universe_op(Subject.PROCESSES)
    frame = fd.compose_ops(base, (redirect,), base_trace_id="t0").effective
    question = fo.PendingQuestion("UNIT_AMBIGUOUS", frame, (redirect,), (), (), frozenset({TIME_POS}))
    made = P.pending_from_question(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert made.disposition.value == "DEPENDENCY" and made.pending is None
    assert made.dependency.dep_id == "DEP-MODALITY-UNIVERSE"


@direct
def test_a_pending_of_a_rule_that_is_always_possible_never_degrades_to_an_inform():
    """§8.4 table and A.3: ``CONTEXT_EXPIRED_CONFIRM`` / ``NO_ANCHOR`` are never INFORM codes: a law violation there is a
    question M2A-3 would never have asked (ContractError), not a quiet INFORM."""
    base = base_of((D.TOPIC, "x"))
    wipe = (clear_op(slot(D.TOPIC)),)  # «continuar» composes but leaves nothing to search: it does not execute (H11)
    question = fo.PendingQuestion("CONTEXT_EXPIRED_CONFIRM", base, (), wipe, (wipe,), frozenset({slot(D.TOPIC)}))
    with pytest.raises(ContractError):
        P.pending_from_question(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)


@direct
def test_creation_refuses_inputs_that_are_not_what_they_say():
    base = base_of((D.TOPIC, "x"))
    question = _question_with(base, open_ops=(set_op(D.TIME, "x"),))
    with pytest.raises(ContractError):
        P.pending_from_question("question", base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    for kwargs in ({"base": "base"}, {"base_trace_id": " "}, {"asked_trace_id": ""}, {"now": datetime(2026, 1, 1)}):
        args = dict(base=base, base_trace_id="t0", asked_trace_id="t1", now=T0) | kwargs
        with pytest.raises(ContractError):
            P.pending_from_question(question, **args)
    no_origin = fo.PendingQuestion("UNIT_AMBIGUOUS", base, (), (), (), frozenset({TIME_POS}))
    with pytest.raises(ContractError):  # a question with no operation and no citation has no span to be born of
        P.pending_from_question(no_origin, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert P.pending_from_question(no_origin, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0,
                                   origin=origin("t1", 4, RAW_TURN)).pending.ambiguous_span == (4, 6)


# ---- ask_authority (M2A4-D6): H5, H7, H9, H10, UNRESOLVED_VALUE, VALUE_WITHOUT_COMPARATOR ---------------------------
@direct
def test_h5_has_two_candidates_one_buyer_one_place_and_the_provisional_label_in_the_asking_patch():
    base, made, buyer, place = h5()
    p = made.pending
    assert (made.code, p.reason, p.frame, p.base_trace_id) == ("BUYER_OR_PLACE_AFTER_ACTOR",) * 2 + (base, "t0")
    assert [tuple((o.op.value, o.slot.key) for o in c.patch.ops) for c in p.candidate_patches] == [
        (("SET", "BUYER/POSITIVE"),), (("SET", "GEOGRAPHY/POSITIVE"),)]
    assert p.answer_slots == {BUYER_POS, GEO_POS} and made.asked_ops == (buyer,)


@direct
def test_each_authority_rule_builds_its_pending_over_its_open_slots():
    base = base_of((D.TOPIC, "x"))
    for reason, slots in (("REGION_NOT_IN_LIST", {GEO_POS}), ("UNIT_AMBIGUOUS", {slot(D.VALUE, side=ValueSide.LOWER)}),
                          ("MONTH_YEAR_AMBIGUOUS", {TIME_POS}), ("UNRESOLVED_VALUE:TIME", {TIME_POS})):
        question = P.AuthorityQuestion(reason, origin("t1", 5, RAW_TURN), frozenset(slots))
        made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
        assert made.disposition.value == "CLARIFY" and made.pending.reason == reason, reason
        assert made.pending.answer_slots == slots and made.pending.candidate_patches == ()
        assert made.pending.ambiguous_span == (5, 7)  # the authority's ambiguous window


@direct
def test_unresolved_value_carries_the_options_the_authority_resolved_at_ask_time():
    """SF058-like: ``UNRESOLVED_VALUE:BUYER`` with three buyers as candidates, all in the dimension they ask about."""
    base = base_of((D.TOPIC, "x"))
    options = tuple((set_op(D.BUYER, t),) for t in ("x", "y", "z"))
    question = P.AuthorityQuestion("UNRESOLVED_VALUE:BUYER", origin("t1", 0, RAW_TURN), frozenset({BUYER_POS}), options)
    made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert [c.candidate_id for c in made.pending.candidate_patches] == ["c1", "c2", "c3"]
    assert len({c.display_key for c in made.pending.candidate_patches}) == 3
    # a candidate outside the dimension asked about is refused
    with pytest.raises(ContractError):
        P.AuthorityQuestion("UNRESOLVED_VALUE:BUYER", origin("t1", 0, RAW_TURN), frozenset({BUYER_POS}),
                            ((set_op(D.TIME, "x"),),))


@direct
def test_value_without_comparator_answers_on_both_sides_whatever_the_caller_says():
    base = base_of((D.TOPIC, "x"))
    question = P.AuthorityQuestion("VALUE_WITHOUT_COMPARATOR", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert made.pending.answer_slots == {slot(D.VALUE, side=ValueSide.LOWER), slot(D.VALUE, side=ValueSide.UPPER)}


@direct
def test_an_authority_question_must_be_well_formed():
    o, s = origin("t1", 0, RAW_TURN), frozenset({TIME_POS})
    buyer, place, other = set_op(D.BUYER, "y"), set_op(D.GEOGRAPHY, "y"), set_op(D.TIME, "y")
    bad = [("H6", (), s), ("TERRITORY_VS_CARRIED_BUYER", (), s), ("NO_ANCHOR", (), s), ("UNRESOLVED_VALUE:NOT_A_SLOT", (), s),
           ("MONTH_YEAR_AMBIGUOUS", ((other,),), s),  # §8.4 table: «las demás, ninguno»
           ("BUYER_OR_PLACE_AFTER_ACTOR", ((buyer,),), s),  # F12: exactly two
           ("BUYER_OR_PLACE_AFTER_ACTOR", ((buyer,), (buyer,)), s),  # one buyer, one place
           ("BUYER_OR_PLACE_AFTER_ACTOR", ((buyer, place), (place,)), s)]
    for reason, candidates, slots in bad:
        with pytest.raises(ContractError):
            P.AuthorityQuestion(reason, o, slots, candidates)
    with pytest.raises(ContractError):
        P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", fa.ConditionOrigin(fa.OriginKind.HOST_POLICY, "t1"), s)


@direct
def test_an_authority_question_on_an_expired_anchor_or_with_a_pending_vigente_is_an_inform():
    """§8.4: on an expired anchor only H1's pending may exist; with a pending vigente no other is born."""
    base = base_of((D.TOPIC, "x"))
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    for flag in ("anchor_expired", "pending_present"):
        made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0, **{flag: True})
        assert (made.disposition.value, made.code, made.pending) == ("INFORM", "MONTH_YEAR_AMBIGUOUS", None)


@direct
def test_an_authority_question_the_law_forbids_is_an_inform_with_its_code():
    """§8.4 (3): no answer on the open slots executes (the only slot a frame of ORDER + CARDINALITY..., here an
    ORDER-less CARDINALITY answer cannot compose): ``INFORM`` with the rule's code and nothing built."""
    base = base_of((D.TOPIC, "x"))
    question = P.AuthorityQuestion("UNRESOLVED_VALUE:CARDINALITY", origin("t1", 0, RAW_TURN), frozenset({CARD_SLOT}))
    made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    # CLEAR / KEEP of an empty CARDINALITY: KEEP_ON_EMPTY_SLOT, CLEAR_ON_EMPTY_SLOT (H12), SET without ORDER: refused
    assert (made.disposition.value, made.code, made.pending) == ("INFORM", "UNRESOLVED_VALUE:CARDINALITY", None)


@direct
def test_ask_fresh_builds_a_pending_without_base_and_without_a_frame_it_is_the_declared_red():
    frame = base_of((D.TOPIC, "x"))
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    made = P.ask_fresh(question, frame=frame, asked_trace_id="t1", now=T0, asked_ops=())
    assert made.disposition.value == "CLARIFY" and made.pending.base_trace_id is None and made.pending.frame == frame
    none = P.ask_fresh(question, frame=None, asked_trace_id="t1", now=T0, asked_ops=())
    assert (none.disposition.value, none.dependency.dep_id, none.dependency.owners) == (
        "DEPENDENCY", "DEP-M1-PENDING-FRAME", ("M2A-1b",))
    with pytest.raises(ContractError):  # a new search has no base
        P.ask_fresh(question, frame=replace(frame, base_trace_id="t0"), asked_trace_id="t1", now=T0, asked_ops=())


@direct
def test_ask_fresh_judges_the_law_on_the_frame_too():
    """§8.4 for a new search that asks: the frame, the candidates and an executing answer are judged as for any pending."""
    frame = base_of((D.TOPIC, "x"))
    impossible = P.AuthorityQuestion("UNRESOLVED_VALUE:CARDINALITY", origin("t1", 0, RAW_TURN), frozenset({CARD_SLOT}))
    made = P.ask_fresh(impossible, frame=frame, asked_trace_id="t1", now=T0, asked_ops=(set_op(D.TIME, "x"),))
    assert (made.disposition.value, made.code, made.pending) == ("INFORM", "UNRESOLVED_VALUE:CARDINALITY", None)
    assert "no answer executes" in made.violations and len(made.asked_ops) == 1
    refused = P.AuthorityQuestion("UNRESOLVED_VALUE:BUYER", origin("t1", 0, RAW_TURN), frozenset({BUYER_POS}),
                                  ((keep_op(BUYER_POS),),))  # KEEP of a slot the frame does not hold
    again = P.ask_fresh(refused, frame=frame, asked_trace_id="t1", now=T0, asked_ops=())
    assert again.disposition.value == "INFORM" and any("candidate" in v for v in again.violations)


@direct
def test_ask_fresh_has_no_default_for_the_asking_patch():
    """M2A4-D28 (ZEUS P2-B): ``respond`` refuses ``asked_ops=()`` by omission (D22); ``ask_fresh`` must not reopen it with a
    default of ``()``. The caller of a new search that asks hands over the typed operations of the turn, or an empty tuple
    it WROTE: ``Creation.asked_ops`` is what F24 reads back."""
    assert inspect.signature(P.ask_fresh).parameters["asked_ops"].default is inspect.Parameter.empty
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    with pytest.raises(TypeError):
        P.ask_fresh(question, frame=base_of((D.TOPIC, "x")), asked_trace_id="t1", now=T0)
    buyer = set_op(D.BUYER, "y")
    made = P.ask_fresh(question, frame=base_of((D.TOPIC, "x")), asked_trace_id="t1", now=T0, asked_ops=(buyer,))
    assert made.asked_ops == (buyer,)


@direct
def test_pending_without_anchor_has_no_frame_no_candidate_and_no_slot():
    made = no_anchor()
    p = made.pending
    assert (made.code, p.reason, p.frame, p.base_trace_id, p.candidate_patches, p.answer_slots) == (
        "NO_ANCHOR", "NO_ANCHOR", None, None, (), frozenset())
    assert (p.ambiguous_span, p.ambiguous_raw_sha256) == (None, None)
    assert P.pending_without_anchor("ANCHOR_UNREADABLE", asked_trace_id="t1", now=T0).pending.reason == (
        "ANCHOR_UNREADABLE")
    with pytest.raises(ContractError):
        P.pending_without_anchor("CONTEXT_EXPIRED_CONFIRM", asked_trace_id="t1", now=T0)


# ---- §8.3 row 1: an option event -------------------------------------------------------------------------------------
@direct
def test_row_1_an_event_selects_its_candidate_and_composes_on_the_frame_with_no_llm():
    base, made = h6()
    p = made.pending
    d = respond(p, P.OptionEvent(p.pending_id, "c1"))  # «cambiar de comprador»
    assert (d.kind.value, d.scope.value, d.resolution.value, d.answer.row, d.answer.candidate_id) == (
        "PENDING_ANSWER", "IN_PROTOCOL", "PENDING_ANSWER", 1, "c1")
    assert kinds(d.answer.composed) == ["TOPIC/POSITIVE", "GEOGRAPHY/POSITIVE", "TIME/POSITIVE"]
    assert d.pending is None and d.control is None and d.base_trace_id == "t1"  # answered, not cancelled
    d = respond(p, P.OptionEvent(p.pending_id, "c2"))  # «buscar con ambos»: H6 is not raised again (it is what is answered)
    assert kinds(d.answer.composed) == ["TOPIC/POSITIVE", "BUYER/POSITIVE", "GEOGRAPHY/POSITIVE", "TIME/POSITIVE"]


@direct
def test_row_1_an_option_of_a_pending_that_no_longer_exists_is_stale_and_changes_nothing():
    _, made = h6()
    p = made.pending
    for pending, event in ((None, P.OptionEvent("pending-t1", "c1")), (p, P.OptionEvent("pending-other", "c1")),
                           (p, P.OptionEvent(p.pending_id, "c9")), (p, P.OptionEvent(p.pending_id, "")),
                           (None, P.ResetEvent("pending-t1")), (p, P.ResetEvent("pending-other"))):
        d = respond(pending, event)
        assert (d.kind.value, d.code, d.scope.value, d.control, d.answer) == ("INFORM", "STALE_OPTION", "IN_PROTOCOL", None, None)
        assert d.pending is pending  # a stale id leaves the pending that does exist INTACT (and counts no ask)


@direct
def test_row_1_an_event_composes_the_candidate_as_resolved_at_ask_time_and_names_the_asking_turn_as_base():
    base, made = h6()
    d = respond(made.pending, P.OptionEvent(made.pending.pending_id, "c2"))
    effective = d.answer.composed.effective
    assert effective.base_trace_id == "t1" == made.pending.asked_trace_id != made.pending.base_trace_id
    placed = next(c for c in effective.conditions if c.slot == GEO_POS)
    assert placed.origin == made.pending.candidate_patches[1].patch.ops[0].origin  # as the authority resolved it


# ---- §8.3 «Selección», rows 4–8 -------------------------------------------------------------------------------------
@direct
def test_row_4_compose_on_the_frame_never_the_base():
    """§8.4: the frame is the base plus the resolved part of the asking turn; an answer composes on IT."""
    base, made = authority_month()
    p = made.pending
    d = respond(p, turn(set_op(D.TIME, "x")), asked_ops=made.asked_ops)
    assert (d.kind.value, d.answer.row, d.resolution.value) == ("PENDING_ANSWER", 4, "PENDING_ANSWER")
    assert values_of(d.answer.composed, "BUYER/POSITIVE") == [typed_value(D.BUYER, "y").encode()]  # the frame's buyer
    assert values_of(d.answer.composed, "BUYER/POSITIVE") != [typed_value(D.BUYER, "x").encode()]  # not the base's
    assert d.answer.composed.effective.base_trace_id == "t1" and d.base_trace_id == "t1"  # the asking turn


def test_the_answer_composes_through_a_seam_that_takes_the_frame_and_nothing_else():
    """The answer is never handed the base: ``compose_answer(frame, …)`` is the only composition, and no function of the
    answer module has a parameter named like a base or an anchor."""
    assert list(inspect.signature(P.compose_answer).parameters)[0] == "frame"
    for name in ("answer_in_language", "answer_by_event", "compose_answer", "respond"):
        parameters = set(inspect.signature(getattr(P, name)).parameters)
        assert not parameters & {"base", "anchor", "base_effective"}, (name, parameters)
    # F24 compares with the asking patch: ``respond`` has no default for it (silently off = the product chooses for the user)
    assert inspect.signature(P.respond).parameters["asked_ops"].default is inspect.Parameter.empty
    with pytest.raises(TypeError):
        P.respond(r31()[1].pending, turn(), trace_id="t2", now=T0)


@direct
def test_row_5_an_answer_plus_an_edit_of_another_slot_is_reasked_never_a_hybrid():
    """F25 (SF051, SF053, SF201): the answer is on ``answer_slots`` AND edits another slot."""
    _, made = r31()
    d = respond(made.pending, turn(clear_op(CARD_SLOT), set_op(D.TIME, "x", i=1)), asked_ops=made.asked_ops)
    assert (d.kind.value, d.code, d.answer.row, d.answer.composed) == ("REASK", "ANSWER_EDITS_OTHER_SLOT", 5, None)
    assert d.pending == replace(made.pending, asks=2) and d.control is None


@direct
def test_row_6_no_selection_is_reasked_and_the_product_does_not_choose():
    """F24 (SF061): ``no_selection`` ⇒ ``REASK OPTION_NOT_CHOSEN``; row 5 comes first (the contract's order)."""
    _, made = h6()
    p = made.pending
    d = respond(p, turn(no_selection=True))
    assert (d.kind.value, d.code, d.answer.row, d.pending.asks) == ("REASK", "OPTION_NOT_CHOSEN", 6, 2)
    edit = respond(p, turn(set_op(D.GEOGRAPHY, "y"), set_op(D.TIME, "y", i=1), no_selection=True))
    assert (edit.code, edit.answer.row) == ("ANSWER_EDITS_OTHER_SLOT", 5)
    outside_only = respond(p, turn(set_op(D.TIME, "y"), no_selection=True))  # nothing inside: not row 5
    assert (outside_only.code, outside_only.answer.row) == ("OPTION_NOT_CHOSEN", 6)


@direct
def test_row_7_an_answer_that_selects_no_candidate_or_two_is_reasked():
    _, made = r31()
    p = made.pending
    for ops in ((clear_op(CARD_SLOT),), (set_op(D.CARDINALITY, TOP_20),), (set_op(D.ORDER, ORDER_DATE),)):
        d = respond(p, turn(*ops), asked_ops=made.asked_ops)  # «dame 20» never composes on the base's ORDER
        assert (d.kind.value, d.code, d.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7), ops
    twin = fa.CandidatePatch("c2", "twin", p.candidate_patches[0].patch)
    two = replace(p, candidate_patches=(p.candidate_patches[0], twin))
    d = respond(two, turn(clear_op(CARD_SLOT), clear_op(ORDER_SLOT, 1)), asked_ops=made.asked_ops)
    assert (d.code, d.answer.row) == ("ANSWER_UNRESOLVED", 7)  # two candidates equal to P: ≥2 is row 7 too


@direct
def test_row_7_a_conflict_in_p_is_decided_on_p_as_it_stands():
    """U27: the §6 step 2 causes are decided BEFORE normalizing and before F24: a pending's answer with them is row 7."""
    _, made = h6()
    p = made.pending
    for ops in ((keep_op(GEO_POS), set_op(D.GEOGRAPHY, "y", i=1)),  # KEEP + writer of the same slot
                (set_op(D.GEOGRAPHY, "y"), set_op(D.GEOGRAPHY, "z", i=1)),  # two writers
                (keep_op(BUYER_POS), clear_op(BUYER_POS, 1), set_op(D.GEOGRAPHY, "y", i=2)),  # KEEP + CLEAR
                (keep_op(BUYER_POS), clear_op(slot(D.BUYER), 1), set_op(D.GEOGRAPHY, "y", i=2))):  # dimension-wide
        d = respond(p, turn(*ops), asked_ops=made.asked_ops)
        assert (d.kind.value, d.code, d.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7), ops


@direct
def test_row_8_an_answer_that_touches_nothing_of_the_question_is_reasked_f29():
    _, made = r31()
    p = made.pending
    d = respond(p, turn(set_op(D.TIME, "x")))  # outside answer_slots and nothing inside
    assert (d.kind.value, d.code, d.answer.row, d.pending) == ("REASK", "PENDING_UNANSWERED", 8, replace(p, asks=2))
    assert respond(p, turn(keep_op(TOPIC_POS))).code == "PENDING_UNANSWERED"  # a KEEP of identity: empty after normalizing


@direct
def test_a_value_no_authority_resolved_selects_nothing_but_still_touches_its_slot():
    """§8.3 row 7 («autoridad no RESOLVED»), SF189: there is no typed operation, but P said something in that slot: it
    touches it (rows 5 and 8 count it), writes it (a second writer is a conflict) and selects no candidate."""
    base = base_of((D.TOPIC, "x"))
    options = tuple((set_op(D.BUYER, t),) for t in ("x", "y", "z"))
    question = P.AuthorityQuestion("UNRESOLVED_VALUE:BUYER", origin("t1", 0, RAW_TURN), frozenset({BUYER_POS}), options)
    p = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0).pending
    said = frozenset({BUYER_POS})
    row_7 = respond(p, turn(unresolved=said))  # «la del Caribe»: inside, nothing resolved
    assert (row_7.kind.value, row_7.code, row_7.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7)
    row_8 = respond(p, turn(unresolved=frozenset({TIME_POS})))  # an unresolved value OUTSIDE and nothing inside
    assert (row_8.code, row_8.answer.row) == ("PENDING_UNANSWERED", 8)
    row_5 = respond(p, turn(set_op(D.BUYER, "y"), unresolved=frozenset({TIME_POS})))  # an answer plus an unresolved edit
    assert (row_5.code, row_5.answer.row) == ("ANSWER_EDITS_OTHER_SLOT", 5)
    clash = respond(p, turn(set_op(D.BUYER, "y"), unresolved=said))  # two writers of BUYER: decided on P, row 7
    assert (clash.code, clash.answer.row) == ("ANSWER_UNRESOLVED", 7)
    chosen = respond(p, turn(set_op(D.BUYER, "y")))
    assert chosen.kind.value == "PENDING_ANSWER" and chosen.answer.candidate_id == "c2"
    with pytest.raises(ContractError):  # a dimension-wide slot is no slot a value can be said in
        turn(unresolved=frozenset({slot(D.BUYER)}))


@direct
def test_an_empty_list_of_operations_is_never_a_cancellation():
    """§17.1: no operation and no directive is row 8 (F29): the pending stays, ``asks + 1``, no ``CANCEL_PENDING``."""
    for pending in (h6()[1].pending, r31()[1].pending, h1()[1].pending, no_anchor().pending):
        d = respond(pending, turn())
        assert (d.kind.value, d.code, d.control, d.cancellation) == ("REASK", "PENDING_UNANSWERED", None, None)
        assert d.pending == replace(pending, asks=2) and d.answer.row == 8


@direct
def test_normalization_drops_only_the_identity_keeps_and_only_with_candidates():
    """§8.3: with candidates the KEEPs of slots the frame holds that no other operation touches are the identity. (Option A
    of H6 is not the asking patch nor a part of it, so it is the answer the KEEPs are dropped FROM.)"""
    _, made = h6()
    p = made.pending
    a = (clear_op(BUYER_POS, 1), set_op(D.GEOGRAPHY, "y", i=2))
    kept = respond(p, turn(keep_op(TOPIC_POS), *a), asked_ops=made.asked_ops)  # «cambiar de comprador» + a KEEP of filler
    assert (kept.kind.value, kept.answer.candidate_id) == ("PENDING_ANSWER", "c1")
    keep_of_nothing = respond(p, turn(keep_op(ACTOR_POS), *a), asked_ops=made.asked_ops)  # a KEEP of an empty slot: no identity
    assert keep_of_nothing.code == "ANSWER_EDITS_OTHER_SLOT"  # touches ACTOR (outside) with BUYER/GEO (inside): row 5
    twice = respond(p, turn(keep_op(TOPIC_POS), keep_op(TOPIC_POS, 3), *a), asked_ops=made.asked_ops)
    assert twice.code == "ANSWER_EDITS_OTHER_SLOT"  # each KEEP is touched by the other one: none is the identity


@direct
def test_declared_cost_keep_without_candidates_is_reasked_not_normalized():
    """M2A4-D4 (P2-4): the normalization is only with candidates (U27). Without them a KEEP of another slot is row 5, a
    REASK that fails safe; this test fixes the cost so relaxing it is a decision, not a drift."""
    _, made = authority_month()
    p = made.pending
    assert p.candidate_patches == ()
    d = respond(p, turn(keep_op(TOPIC_POS), set_op(D.TIME, "x", i=1)), asked_ops=made.asked_ops)
    assert (d.kind.value, d.code, d.answer.row) == ("REASK", "ANSWER_EDITS_OTHER_SLOT", 5)
    assert respond(p, turn(set_op(D.TIME, "x")), asked_ops=made.asked_ops).kind.value == "PENDING_ANSWER"  # no filler


def _fillers(frame):
    """Every KEEP of identity an answer can carry as filler: each slot the frame holds, the universe, and pairs of them."""
    keeps = [keep_op(c.slot, 10 + i) for i, c in enumerate(frame.conditions)] + [keep_op(fa.UNIVERSE_SLOT, 20)]
    return [(k,) for k in keeps] + [pair for pair in zip(keeps, keeps[1:])]


@direct
def test_f24_an_answer_identical_to_the_asking_patch_does_not_answer_with_or_without_filler():
    """F24, U27 and M2A4-D20 (HERA on ZEUS P1-1; §8.4: «un candidato idéntico al parche que preguntó sólo se alcanza por
    evento»): the same operations as the turn that asked are row 7. A KEEP of filler does not change that: it is judged on
    the answer as it stands AND on the answer once the identity KEEPs are gone."""
    base, made = h6()
    p = made.pending
    same = respond(p, turn(set_op(D.GEOGRAPHY, "y")), asked_ops=made.asked_ops)
    assert (same.kind.value, same.code, same.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7)
    for filler in _fillers(base):  # H6: «buscar con ambos» is the asking patch (SET GEO), whatever KEEP goes with it
        d = respond(p, turn(*filler, set_op(D.GEOGRAPHY, "y", i=5)), asked_ops=made.asked_ops)
        assert (d.kind.value, d.code, d.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7), filler
    for label in ("BUYER", "GEOGRAPHY"):  # H5: the option the producer's label names is the asking patch
        base5, made5, buyer, place = h5(label)
        repeated = set_op(D.BUYER, "y", i=5) if label == "BUYER" else set_op(D.GEOGRAPHY, "y", i=5)
        other = set_op(D.GEOGRAPHY, "y", i=5) if label == "BUYER" else set_op(D.BUYER, "y", i=5)
        for filler in ((), *_fillers(base5)):
            d = respond(made5.pending, turn(*filler, repeated), asked_ops=made5.asked_ops)
            assert (d.kind.value, d.code) == ("REASK", "ANSWER_UNRESOLVED"), (label, filler)
            chosen = respond(made5.pending, turn(*filler, other), asked_ops=made5.asked_ops)  # the OTHER option is reachable
            assert chosen.kind.value == "PENDING_ANSWER", (label, filler)
    # nothing declared to compare with: F24 has nothing to refuse (the caller said so, M2A4-D22)
    assert respond(p, turn(set_op(D.GEOGRAPHY, "y")), asked_ops=()).kind.value == "PENDING_ANSWER"
    # a pending with NO candidate whose asking patch holds the (typed) open operation: repeating it answers nothing either
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    month = P.ask_authority(question, base=base_of((D.TOPIC, "x")), base_trace_id="t0", asked_trace_id="t1", now=T0,
                            open_ops=(set_op(D.TIME, "x"),))
    assert month.pending.candidate_patches == ()
    d = respond(month.pending, turn(set_op(D.TIME, "x", i=5)), asked_ops=month.asked_ops)
    assert (d.kind.value, d.code, d.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7)
    assert respond(month.pending, turn(set_op(D.TIME, "y", i=5)), asked_ops=month.asked_ops).kind.value == "PENDING_ANSWER"
    # the asking patch may itself hold an identity KEEP (the user said «con el mismo comprador, y en …»): repeating it whole
    # is F24 on the answer AS IT STANDS, the sealed order, which holds for every pending (with or without candidates)
    kept = P.ask_authority(question, base=base_of((D.TOPIC, "x"), (D.BUYER, "x")), base_trace_id="t0", asked_trace_id="t1",
                           now=T0, frame_ops=(keep_op(BUYER_POS),), open_ops=(set_op(D.TIME, "x", i=1),))
    assert [o.op.value for o in kept.asked_ops] == ["KEEP", "SET"]
    whole = respond(kept.pending, turn(keep_op(BUYER_POS, 5), set_op(D.TIME, "x", i=6)), asked_ops=kept.asked_ops)
    assert (whole.kind.value, whole.code, whole.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7)
    # an answer that is the asking patch AND says something more (a value nobody resolved) is not «the same patch»
    more = respond(p, turn(set_op(D.GEOGRAPHY, "y", i=5), unresolved=frozenset({TIME_POS})), asked_ops=made.asked_ops)
    assert (more.code, more.answer.row) == ("ANSWER_EDITS_OTHER_SLOT", 5)


@direct
def test_the_normalized_f24_is_only_for_pendings_with_candidates_the_sealed_codes_stand_elsewhere():
    """M2A4-D26 (HERA, ZEUS P3-2, correcting D20): where F24 protects the SELECTION of a candidate it is judged on the answer
    without its identity KEEPs too. With no candidate it blocks no execution and only changed sealed codes (row 5, row 8 →
    row 7): they stand. The sealed F24, on the answer as it stands, holds for every pending."""
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    made = P.ask_authority(question, base=base_of((D.TOPIC, "x"), (D.BUYER, "x")), base_trace_id="t0", asked_trace_id="t1",
                           now=T0, frame_ops=(set_op(D.BUYER, "y", i=1),), open_ops=(set_op(D.TIME, "x"),))
    p, asked = made.pending, made.asked_ops
    assert p.candidate_patches == () and [o.slot.key for o in asked] == ["BUYER/POSITIVE", "TIME/POSITIVE"]
    row_8 = respond(p, turn(set_op(D.BUYER, "y", i=5), keep_op(TOPIC_POS, 6)), asked_ops=asked)  # nothing of the question
    assert (row_8.kind.value, row_8.code, row_8.answer.row) == ("REASK", "PENDING_UNANSWERED", 8)
    row_5 = respond(p, turn(set_op(D.BUYER, "y", i=5), set_op(D.TIME, "x", i=6), keep_op(TOPIC_POS, 7)), asked_ops=asked)
    assert (row_5.kind.value, row_5.code, row_5.answer.row) == ("REASK", "ANSWER_EDITS_OTHER_SLOT", 5)  # D4: no candidate
    whole = respond(p, turn(set_op(D.BUYER, "y", i=5), set_op(D.TIME, "x", i=6)), asked_ops=asked)  # the sealed F24
    assert (whole.kind.value, whole.code, whole.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7)


@direct
def test_f24_a_part_of_the_asking_patch_answers_nothing_either():
    """M2A4-D20 (HERA, fail closed): the turn that asked may carry other resolved operations (SET TIME + SET GEO): repeating
    only «en Antioquia» is a part of what the product proposed and tells no candidate from another: row 7."""
    base = h6()[0]
    made = delta_pending(base, set_op(D.TIME, "y"), set_op(D.GEOGRAPHY, "y", i=1))
    p = made.pending
    assert [(o.op.value, o.slot.key) for o in made.asked_ops] == [("SET", "TIME/POSITIVE"), ("SET", "GEOGRAPHY/POSITIVE")]
    part = set_op(D.GEOGRAPHY, "y", i=5)
    for ops in ((part,), (keep_op(TOPIC_POS, 6), part), (part, keep_op(fa.UNIVERSE_SLOT, 6))):  # the open value, with filler
        d = respond(p, turn(*ops), asked_ops=made.asked_ops)
        assert (d.kind.value, d.code, d.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7), ops
    whole = respond(p, turn(set_op(D.TIME, "y", i=4), part), asked_ops=made.asked_ops)  # the whole asking patch: F24 itself
    assert (whole.kind.value, whole.code) == ("REASK", "ANSWER_UNRESOLVED")
    a = respond(p, turn(keep_op(TOPIC_POS, 6), clear_op(BUYER_POS, 7), part), asked_ops=made.asked_ops)  # A is not a part of it
    assert (a.kind.value, a.answer.candidate_id) == ("PENDING_ANSWER", "c1")
    assert values_of(a.answer.composed, "TIME/POSITIVE") == [typed_value(D.TIME, "y").encode()]  # the frame's TIME survives
    assert respond(p, P.OptionEvent(p.pending_id, "c2")).kind.value == "PENDING_ANSWER"  # B by event
    unsaid = respond(p, turn(part, unresolved=frozenset({TIME_POS})), asked_ops=made.asked_ops)  # it also says something more
    assert (unsaid.code, unsaid.answer.row) == ("ANSWER_EDITS_OTHER_SLOT", 5)  # not «a part»: it edits another slot too
    assert respond(p, turn(keep_op(TOPIC_POS, 6)), asked_ops=made.asked_ops).code == "PENDING_UNANSWERED"  # empty: row 8


@direct
def test_declared_cost_h6_b_only_by_event_a_by_repeating_both_operations():
    """M2A4-D5 (rewritten by D20; ZEUS refuted the earlier text): «buscar con ambos» (B) IS the asking patch and is reached
    only by event, with or without a KEEP; «cambiar de comprador» (A) only by repeating its two operations (with or without
    a KEEP of filler). Everything else falls safe (REASK)."""
    base, made = h6()
    p = made.pending
    a = respond(p, turn(clear_op(BUYER_POS), set_op(D.GEOGRAPHY, "y", i=1)), asked_ops=made.asked_ops)
    assert (a.kind.value, a.answer.candidate_id) == ("PENDING_ANSWER", "c1")
    assert kinds(a.answer.composed) == kinds(respond(p, P.OptionEvent(p.pending_id, "c1")).answer.composed)
    for filler in _fillers(base):
        if not any(k.slot in (BUYER_POS, GEO_POS) for k in filler):  # a KEEP of BUYER or GEO next to A is a §6 step-2 conflict
            assert respond(p, turn(*filler, clear_op(BUYER_POS), set_op(D.GEOGRAPHY, "y", i=1)),
                           asked_ops=made.asked_ops).answer.candidate_id == "c1"
        assert respond(p, turn(*filler, set_op(D.GEOGRAPHY, "y", i=1)), asked_ops=made.asked_ops).code == "ANSWER_UNRESOLVED"
    assert respond(p, P.OptionEvent(p.pending_id, "c2")).kind.value == "PENDING_ANSWER"


@direct
def test_declared_cost_h5_the_option_the_producers_label_names_is_reached_only_by_event():
    """M2A4-D5 (rewritten by D20): in H5 the option equal to the asking patch (the provisional label of the producer) is
    reached only by event, filler or not; the other one in language. Which one is which depends on the label."""
    expectation = {"BUYER": {"como comprador": "ANSWER_UNRESOLVED", "como lugar": "PENDING_ANSWER"},
                   "GEOGRAPHY": {"como comprador": "PENDING_ANSWER", "como lugar": "ANSWER_UNRESOLVED"}}
    for label, expected in expectation.items():
        base, made, buyer, place = h5(label)
        for said, op in (("como comprador", set_op(D.BUYER, "y", i=3)), ("como lugar", set_op(D.GEOGRAPHY, "y", i=3))):
            for filler in ((), *_fillers(base)):
                d = respond(made.pending, turn(*filler, op), asked_ops=made.asked_ops)
                got = "PENDING_ANSWER" if d.kind.value == "PENDING_ANSWER" else d.code
                assert got == expected[said], (label, said, filler, got)
        for candidate in ("c1", "c2"):  # by event both are reachable, whatever the label
            assert respond(made.pending, P.OptionEvent(made.pending.pending_id, candidate)).kind.value == "PENDING_ANSWER"


@direct
def test_selection_is_by_the_exact_value_never_by_the_slot():
    """§8.3 «Selección» (U22): (op, slot, canonical encoded value). Another value in a candidate's slot selects nothing."""
    _, made, buyer, place = h5()
    other = respond(made.pending, turn(set_op(D.BUYER, "z", i=2)), asked_ops=made.asked_ops)  # a buyer no option offered
    assert (other.kind.value, other.code, other.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7)
    wrong_dimension = respond(made.pending, turn(set_op(D.GEOGRAPHY, "z", i=2)), asked_ops=made.asked_ops)
    assert wrong_dimension.code == "ANSWER_UNRESOLVED"
    chosen = respond(made.pending, turn(set_op(D.GEOGRAPHY, "y", i=2)), asked_ops=(buyer,))
    assert chosen.kind.value == "PENDING_ANSWER" and chosen.answer.candidate_id == "c2"


@direct
def test_h1_a_selection_in_language_is_never_an_execution_over_the_expired_anchor():
    """M2A4-D2 (P1-2, U27): «continuar» is confirmed only by event; every answer in language, with or without a KEEP of
    filler, is row 7 (or rows 5/8, which are REASKs too)."""
    base, made = h1()
    p = made.pending
    fillers = ((), (keep_op(TOPIC_POS, 1),), (keep_op(BUYER_POS, 1),), (keep_op(TOPIC_POS, 1), keep_op(BUYER_POS, 2)))
    for filler in fillers:
        for answer in ((set_op(D.TIME, "y"),), (set_op(D.TIME, "z"),), (clear_op(TIME_POS),), ()):
            d = respond(p, turn(*answer, *filler), asked_ops=made.asked_ops)
            assert d.kind.value == "REASK" and d.pending.asks == 2, (answer, filler, d.kind)
    assert respond(p, P.OptionEvent(p.pending_id, "c1")).kind.value == "PENDING_ANSWER"  # by event it does
    # D2 needs no ``asked_ops``: the one candidate IS the asking patch. A caller that passes none, or the wrong ones, still
    # gets no execution (the F24 comparison is not what stops it here)
    for wrong in ((), (clear_op(TIME_POS, 3),), (set_op(D.ACTOR, "z", i=3),)):
        for filler in fillers:
            d = P.respond(p, turn(set_op(D.TIME, "y"), *filler), trace_id="t2", now=T0 + MINUTE, asked_ops=wrong,
                          no_asked_patch=not wrong)
            assert d.kind.value == "REASK", (wrong, filler)


@direct
def test_an_answer_that_composes_but_leaves_a_question_open_is_row_7_never_a_hybrid():
    """§8.3: «si el resultado sigue ambiguo ⇒ REASK»: the answer composes with no cause, but H11 (an empty search) or H12
    (a lone CLEAR of an empty slot) is still there: nothing executes."""
    _, made = h11()  # frame = the base (TOPIC x); the open span was CLEAR(TOPIC), dimension-wide
    wipe = respond(made.pending, turn(clear_op(TOPIC_POS, 1)), asked_ops=made.asked_ops)  # TOPIC/POSITIVE only: not F24
    assert (wipe.kind.value, wipe.code, wipe.answer.row, wipe.answer.composed) == ("REASK", "ANSWER_UNRESOLVED", 7, None)
    keep = respond(made.pending, turn(keep_op(TOPIC_POS, 1)), asked_ops=made.asked_ops)  # KEEP: nothing left open
    assert keep.kind.value == "PENDING_ANSWER" and kinds(keep.answer.composed) == ["TOPIC/POSITIVE"]
    base = base_of((D.TOPIC, "x"))
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    asked = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    lone = respond(asked.pending, turn(clear_op(TIME_POS, 1)), asked_ops=asked.asked_ops)  # H12: nothing to clear
    assert (lone.kind.value, lone.code, lone.answer.row) == ("REASK", "ANSWER_UNRESOLVED", 7)


@direct
def test_a_candidate_is_composed_whole_not_the_answer_that_selected_it():
    """§8.3 «Selección» 5: «se compone su parche completo»: the chosen candidate's operations, never P (which may differ by
    the identity KEEPs it was normalized from, and by origin)."""
    _, made = h6()
    p = made.pending
    d = respond(p, turn(keep_op(TOPIC_POS), clear_op(BUYER_POS, 1), set_op(D.GEOGRAPHY, "y", i=2)), asked_ops=made.asked_ops)
    assert d.answer.patch == p.candidate_patches[0].patch.ops
    assert all(o.op.value != "KEEP" for o in d.answer.patch)


# ---- the cap of asks, the plazo and expiry -------------------------------------------------------------------------
@direct
def test_a_reask_keeps_the_pending_whole_but_asks_and_never_extends_the_plazo():
    _, made = r31()
    p = made.pending
    for ops, code in (((set_op(D.TIME, "x"),), "PENDING_UNANSWERED"), ((clear_op(CARD_SLOT),), "ANSWER_UNRESOLVED"),
                      ((clear_op(CARD_SLOT), set_op(D.TIME, "x", i=1)), "ANSWER_EDITS_OTHER_SLOT")):
        d = respond(p, turn(*ops), now=T0 + 20 * MINUTE, asked_ops=made.asked_ops)
        kept = d.pending
        assert d.code == code and kept.asks == 2 and d.cancellation is None
        assert replace(kept, asks=1) == p  # frame, question, candidates, ids and plazo untouched
        assert (kept.created_at_utc, kept.expires_at_utc) == (T0, T0 + 30 * MINUTE)


@direct
def test_the_cap_of_asks_cancels_the_pending_on_the_failure_after_the_second_ask():
    """§8.3: ``asks`` reaches 2 and the answer fails again ⇒ the pending is cancelled (``CANCEL_PENDING``) and it is said."""
    _, made = r31()
    first = respond(made.pending, turn(set_op(D.TIME, "x")), asked_ops=made.asked_ops)
    assert first.pending.asks == 2 and first.control is None
    second = respond(first.pending, turn(clear_op(CARD_SLOT)), asked_ops=made.asked_ops)
    assert (second.kind.value, second.code, second.pending) == ("REASK", "ANSWER_UNRESOLVED", None)
    assert second.control.value == "CANCEL_PENDING" and second.cancellation.cause.value == "EXHAUSTED"
    assert second.effects == {"PENDING_CANCELLED"} and not second.anchor_eliminated
    for pending in (h6()[1].pending, h5()[1].pending, authority_month()[1].pending, h11()[1].pending):
        asked = replace(pending, asks=2)
        for t in (turn(no_selection=True), turn(), turn(keep_op(ACTOR_POS)),
                  turn(set_op(D.ORDER, ORDER_DATE), set_op(D.TIME, "w", i=1))):
            d = respond(asked, t)
            assert d.kind.value == "REASK" and d.pending is None and d.control.value == "CANCEL_PENDING"


@direct
def test_an_answer_that_succeeds_at_the_cap_still_answers():
    _, made = r31()
    last = replace(made.pending, asks=2)
    d = respond(last, turn(clear_op(CARD_SLOT), clear_op(ORDER_SLOT, 1)), asked_ops=made.asked_ops)
    assert d.kind.value == "PENDING_ANSWER" and d.control is None


@direct
def test_expiry_is_at_the_deadline_over_the_injected_instant():
    """§17.2 e, U26: a pending lives 30 minutes from its creation; from the deadline ON it is expired."""
    _, made = r31()
    p = made.pending
    edge = T0 + timedelta(minutes=30)
    assert not P.pending_expired(p, edge - timedelta(microseconds=1))
    assert P.pending_expired(p, edge) and P.pending_expired(p, edge + timedelta(microseconds=1))
    live = respond(p, turn(set_op(D.TIME, "x")), now=edge - timedelta(microseconds=1))
    assert live.kind.value == "REASK" and live.cancellation is None
    dead = respond(p, turn(set_op(D.TIME, "x")), now=edge)
    assert (dead.kind.value, dead.scope, dead.resolution, dead.pending) == ("EXPIRED", None, None, None)
    assert dead.control.value == "CANCEL_PENDING" and dead.cancellation.cause.value == "EXPIRED"


@direct
def test_the_deadline_is_never_later_than_thirty_minutes_after_creation():
    """A record that says more than the contract's 30 minutes never outlives them; one that says less is honoured."""
    _, made = r31()
    p = made.pending
    long_lived = replace(p, expires_at_utc=T0 + timedelta(hours=2))
    assert P.pending_expired(long_lived, T0 + timedelta(minutes=30)) and not P.pending_expired(
        long_lived, T0 + timedelta(minutes=29))
    short = replace(p, expires_at_utc=T0 + 10 * MINUTE)
    assert P.pending_expired(short, T0 + 10 * MINUTE) and not P.pending_expired(short, T0 + 9 * MINUTE)


@direct
def test_an_event_on_an_expired_pending_is_stale_and_writes_the_cancellation():
    """§8.3 (STALE_OPTION) and §17.2 e: the next turn, an event this time, carries ``CANCEL_PENDING``."""
    _, made = h6()
    p = made.pending
    for event in (P.OptionEvent(p.pending_id, "c1"), P.OptionEvent("pending-other", "c1")):
        d = respond(p, event, now=T0 + 31 * MINUTE)
        assert (d.kind.value, d.code, d.pending, d.control.value) == ("INFORM", "STALE_OPTION", None, "CANCEL_PENDING")
        assert d.cancellation.cause.value == "EXPIRED" and not d.anchor_eliminated
    live = respond(p, P.OptionEvent(p.pending_id, "c1"), now=T0 + 29 * MINUTE)
    assert live.kind.value == "PENDING_ANSWER"


# ---- §17: cancellation -----------------------------------------------------------------------------------------------
PENDINGS = {"H6": lambda: h6()[1].pending, "R3-1": lambda: r31()[1].pending, "H1": lambda: h1()[1].pending,
            "H11": lambda: h11()[1].pending, "H5": lambda: h5()[1].pending, "H10": lambda: authority_month()[1].pending,
            "NO_ANCHOR": lambda: no_anchor().pending}


@direct
def test_every_cause_writes_cancel_pending_and_only_a_cancelled_expired_confirm_removes_the_anchor():
    """§17.2 a–e and M2A4-D3: ``control = CANCEL_PENDING`` for each cause; the expired anchor goes with EVERY cause that
    cancels a ``CONTEXT_EXPIRED_CONFIRM`` (a, b, c, d, e), and with no cancellation of another pending."""
    for name, make in PENDINGS.items():
        pending = make()
        for cause in P.CancelCause:
            if cause is P.CancelCause.RESET and name != "H1":
                with pytest.raises(ContractError):  # there is no «reset» for another pending in the contract
                    P.cancel_pending(pending, cause)
                continue
            c = P.cancel_pending(pending, cause)
            assert c.control.value == "CANCEL_PENDING" and c.effects == {"PENDING_CANCELLED"}
            assert (c.pending_id, c.asked_trace_id, c.cause) == (pending.pending_id, pending.asked_trace_id, cause)
            assert c.eliminates_expired_anchor is (name == "H1"), (name, cause)
    assert {c.value for c in P.CancelCause} == {"DIRECTIVE", "NEW_SEARCH", "RESET", "EXHAUSTED", "EXPIRED"}
    with pytest.raises(ContractError):
        P.cancel_pending("pending", P.CancelCause.DIRECTIVE)


@direct
def test_the_base_after_a_cancel_is_the_frame_and_an_expired_confirm_or_a_missing_frame_has_none():
    """§17.3: the pending's frame, never an older turn; none for ``CONTEXT_EXPIRED_CONFIRM`` and for no frame."""
    for name, make in PENDINGS.items():
        pending = make()
        base = P.fragment_base(pending)
        assert (base is None) == (name in ("H1", "NO_ANCHOR")), name
        assert base is None or base is pending.frame


@direct
def test_row_2_the_rest_of_p_after_a_cancel_is_a_delta_on_the_frame_a7():
    """§8.3 row 2, §17.3: «déjalo como estaba y además en 2024»: the directive cancels; the fragment composes on the FRAME
    (the asking turn is the base), keeping the ORDER and the CARDINALITY the pending was about."""
    _, made = r31()
    p = made.pending
    d = respond(p, turn(set_op(D.TIME, "x"), facts=facts(cancel_directive=True, continuation=True)))
    assert (d.kind.value, d.resolution.value, d.scope.value, d.control.value) == ("DELTA", "DELTA", "IN", "CANCEL_PENDING")
    assert d.cancellation.cause.value == "DIRECTIVE" and d.pending is None
    assert d.base_trace_id == "t1" and d.outcome.effective.base_trace_id == "t1"
    assert kinds(d.outcome) == ["TOPIC/POSITIVE", "TIME/POSITIVE", "ORDER", "CARDINALITY"]


@direct
def test_row_2_the_rest_of_p_may_ask_and_its_pending_replaces_the_cancelled_one():
    """§8.3 row 3 (a new search that asks) and A7 for a delta: the new pending substitutes the cancelled one."""
    base, made = h6()
    p = made.pending
    other = base_of((D.TOPIC, "x"), (D.BUYER, "y"))
    other = replace(other, conditions=tuple(
        replace(c, provenance=fa.Provenance(buyer_scope="76")) if c.slot == BUYER_POS else c for c in other.conditions))
    holder = replace(p, frame=other)
    d = respond(holder, turn(set_op(D.GEOGRAPHY, "y"), facts=facts(cancel_directive=True, continuation=True)))
    assert (d.kind.value, d.code, d.control.value) == ("CLARIFY", "TERRITORY_VS_CARRIED_BUYER", "CANCEL_PENDING")
    assert d.created.pending is d.pending and d.pending.asked_trace_id == "t2" != p.asked_trace_id
    assert d.pending.base_trace_id == "t1"  # composed over the cancelled pending's frame
    assert d.pending.pending_id == "pending-t2" != p.pending_id


@direct
def test_row_2_the_rest_of_p_may_say_a_value_nobody_resolved_it_is_an_inform_never_dropped():
    """The cancel stands (the host writes it), and what is left of P, a value no authority resolved, is told with its code:
    ``INFORM UNRESOLVED_VALUE:<DIM>`` and no pending (there is no span to ask on): the user reformulates."""
    _, made = r31()
    d = respond(made.pending, turn(unresolved=frozenset({TIME_POS, BUYER_POS}),
                                   facts=facts(cancel_directive=True, continuation=True)))
    assert (d.kind.value, d.code, d.pending, d.outcome) == ("INFORM", "UNRESOLVED_VALUE:BUYER", None, None)
    assert d.control.value == "CANCEL_PENDING" and d.cancellation.cause.value == "DIRECTIVE"  # §4.2 order: BUYER first


@direct
def test_row_2_a_cancel_with_nothing_to_apply_cancels_and_executes_nothing():
    """«Déjalo como estaba»: ``CANCEL_PENDING``, no operation, no search: INFORM with no code (M2A4-D12); the base stays."""
    _, made = r31()
    d = respond(made.pending, turn(facts=facts(cancel_directive=True, own_conditions=0, continuation=False)))
    assert (d.kind.value, d.code, d.scope.value, d.pending, d.answer, d.outcome) == ("INFORM", None, "IN", None, None, None)
    assert d.control.value == "CANCEL_PENDING" and d.cancellation.cause.value == "DIRECTIVE" and not d.anchor_eliminated


@direct
def test_row_2_a_directive_with_a_new_search_is_a_new_search_under_cancel_pending():
    _, made = r31()
    d = respond(made.pending, turn(facts=new_search_facts(cancel_directive=True)))
    assert (d.kind.value, d.scope.value, d.control.value, d.pending, d.answer) == (
        "NEW_SEARCH", "IN_NEW_SEARCH", "CANCEL_PENDING", None, None)
    assert d.context_reset  # a new search over a vigente anchor: CONTEXT_RESET (§7.2, §11)


@direct
def test_row_3_a_new_search_with_a_pending_vigente_cancels_it_the_host_writes_it_a7():
    """§8.3 row 3, §17.2 b: ¬Continuación ∧ Marco ∧ Admisible sola, and not a compatible answer ⇒ NEW_SEARCH with control."""
    for name, make in PENDINGS.items():
        pending = make()
        d = respond(pending, turn(facts=new_search_facts()))
        assert (d.kind.value, d.scope.value, d.control.value, d.pending) == (
            "NEW_SEARCH", "IN_NEW_SEARCH", "CANCEL_PENDING", None), name
        assert d.cancellation.cause.value == "NEW_SEARCH" and d.anchor_eliminated is (name == "H1")


@direct
def test_row_3_a_compatible_answer_is_not_a_new_search_and_a_continuation_never_is():
    """A7/A9: every condition of P inside ``answer_slots`` is an answer (row 4); a continuation wins over a new search."""
    _, made = r31()
    answer = respond(made.pending, turn(clear_op(CARD_SLOT), clear_op(ORDER_SLOT, 1),
                                        facts=new_search_facts(within_answer_slots=True)), asked_ops=made.asked_ops)
    assert answer.kind.value == "PENDING_ANSWER" and answer.control is None
    kept = respond(made.pending, turn(set_op(D.TIME, "x"), facts=new_search_facts(continuation=True)))
    assert kept.kind.value == "REASK" and kept.code == "PENDING_UNANSWERED"  # never a new search with a mark of continuation
    with pytest.raises(ContractError):  # a fact the typed operations contradict
        respond(made.pending, turn(set_op(D.TIME, "x"), facts=new_search_facts(within_answer_slots=True)))


@direct
def test_m2a4_d1_after_a_reset_the_rest_of_p_is_a_search_only_if_not_a_continuation_and_admissible_alone():
    """P1-1: with a ``CONTEXT_EXPIRED_CONFIRM`` (no base) the rest of P is a new search ONLY IF ¬Continuación(P) ∧
    Admisible sola(P); else ``CLARIFY NO_ANCHOR``. Never composed on the expired frame (A9: the mark wins)."""
    _, made = h1()
    p = made.pending
    cases = {  # (continuation, admissible_alone) -> kind
        (False, True): "NEW_SEARCH", (True, True): "CLARIFY", (False, False): "CLARIFY", (True, False): "CLARIFY"}
    for (continuation, alone), kind in cases.items():
        d = respond(p, turn(set_op(D.TIME, "z"), facts=facts(
            cancel_directive=True, continuation=continuation, admissible_alone=alone, anchor_expired=True,
            frame=alone, new_content=True, own_conditions=1)))
        assert d.kind.value == kind, (continuation, alone, d.kind)
        assert d.control.value == "CANCEL_PENDING" and d.anchor_eliminated and d.outcome is None and d.answer is None
        if kind == "CLARIFY":
            assert (d.code, d.scope.value, d.pending.reason, d.pending.frame) == (
                "NO_ANCHOR", "OUT_NO_ANCHOR", "NO_ANCHOR", None)
            assert d.created.pending is d.pending and d.pending.asked_trace_id == "t2"
        else:
            assert (d.scope.value, d.context_reset, d.pending) == ("IN_NEW_SEARCH", False, None)


@direct
def test_m2a4_d1_a_pending_without_a_frame_has_no_base_either():
    d = respond(no_anchor().pending, turn(set_op(D.TIME, "z"), facts=facts(
        cancel_directive=True, continuation=False, admissible_alone=False, anchor=AnchorState.NONE)))
    assert (d.kind.value, d.code, d.scope.value) == ("CLARIFY", "NO_ANCHOR", "OUT_NO_ANCHOR")
    assert d.control.value == "CANCEL_PENDING" and not d.anchor_eliminated  # nothing was an expired anchor


@direct
def test_the_facts_a_cancel_over_no_base_needs_are_three_valued_and_never_defaulted():
    _, made = h1()
    p = made.pending
    undetermined = facts(cancel_directive=True, continuation=None, admissible_alone=True, anchor_expired=True)
    with pytest.raises(ContractError, match="UNDETERMINED_FACT:continuation"):
        respond(p, turn(set_op(D.TIME, "z"), facts=undetermined))
    decided = facts(cancel_directive=True, continuation=None, admissible_alone=False, anchor_expired=True)
    assert respond(p, turn(set_op(D.TIME, "z"), facts=decided)).code == "NO_ANCHOR"  # not admissible: the rest decides nothing
    # M2A4-D28 (ZEUS P3-1): the third fact the rule reads, ``own_conditions``, is three-valued too (the scope is decided by
    # Marco(P) = False whatever it is, so ``classify_turn`` does not refuse and the rule itself must)
    own = facts(cancel_directive=True, continuation=False, admissible_alone=True, own_conditions=None, frame=False,
                anchor_expired=True)
    with pytest.raises(ContractError, match="^UNDETERMINED_FACT:own_conditions$"):
        respond(p, turn(set_op(D.TIME, "z"), facts=own))
    for count, kind in ((0, "CLARIFY"), (1, "NEW_SEARCH")):
        assert respond(p, turn(set_op(D.TIME, "z"), facts=replace(own, own_conditions=count))).kind.value == kind


@direct
def test_reset_before_an_expired_confirm_cancels_and_removes_the_anchor_c():
    """§10.1, §17.2 c: «reset» is ``CANCEL_PENDING`` and the expired anchor is gone; nothing executes."""
    _, made = h1()
    d = respond(made.pending, P.ResetEvent(made.pending.pending_id))
    assert (d.kind.value, d.scope.value, d.code, d.pending, d.outcome, d.answer) == (
        "INFORM", "IN_PROTOCOL", None, None, None, None)
    assert d.control.value == "CANCEL_PENDING" and d.cancellation.cause.value == "RESET" and d.anchor_eliminated
    with pytest.raises(ContractError):  # no reset button for another pending
        respond(h6()[1].pending, P.ResetEvent("pending-t1"))


@direct
def test_the_expiry_of_an_expired_confirm_also_removes_the_anchor_e():
    _, made = h1()
    text = respond(made.pending, turn(set_op(D.TIME, "z")), now=T0 + 30 * MINUTE)
    assert (text.kind.value, text.control.value, text.anchor_eliminated) == ("EXPIRED", "CANCEL_PENDING", True)
    event = respond(made.pending, P.OptionEvent(made.pending.pending_id, "c1"), now=T0 + 45 * MINUTE)
    assert (event.code, event.control.value, event.anchor_eliminated) == ("STALE_OPTION", "CANCEL_PENDING", True)


@direct
def test_out_rows_under_a_pending_are_informs_with_the_pending_intact_and_no_ask_counted():
    """§7.1 rows 4, 4b, 5, 6 with a pending vigente: INFORM, 0 calls, the pending untouched (§8.4: no second pending)."""
    _, made = r31()
    p = made.pending
    rows = {"OUT_OPERATION_CHANGE": dict(foreign_operation=Operation.COUNT),
            "OUT_UNSUPPORTED_PROPOSAL": dict(unsupported_motive="MULTI_REQUEST"),
            "OUT_REFERENT_EXTENSIONAL": dict(referent=ReferentSignal.EXTENSIONAL),
            "OUT_RELATIVE_TO_PREVIOUS": dict(relative_non_temporal=True)}
    for scope, extra in rows.items():
        d = respond(p, turn(facts=facts(**extra)))
        assert (d.kind.value, d.scope.value, d.pending, d.control, d.answer) == ("INFORM", scope, p, None, None), scope
        assert d.code is not None
    assert respond(p, turn(facts=facts(relative_non_temporal=True))).code == "RELATIVE_TO_PREVIOUS_VALUE"
    # NO_ACTION with a pending is not row 4b but §8.3 row 8 (F29)
    assert respond(p, turn(facts=facts(unsupported_motive="NO_ACTION"))).code == "PENDING_UNANSWERED"


@direct
def test_a_declared_dependency_in_an_answer_stops_the_turn_and_keeps_the_pending_intact():
    """§19: an answer that composes into a COMPOSICION dependency executes nothing and does not consume the pending."""
    base = base_of((D.TOPIC, "x"), (D.MODALITY, "x"))
    question = P.AuthorityQuestion("UNRESOLVED_VALUE:UNIVERSE", origin("t1", 0, RAW_TURN), frozenset({fa.UNIVERSE_SLOT}))
    made = P.ask_authority(question, base=base, base_trace_id="t0", asked_trace_id="t1", now=T0)
    assert made.disposition.value == "CLARIFY"
    d = respond(made.pending, turn(universe_op(Subject.PROCESSES)))
    assert (d.kind.value, d.dependency.dep_id, d.pending, d.control, d.resolution) == (
        "DEPENDENCY", "DEP-MODALITY-UNIVERSE", made.pending, None, None)
    assert d.answer.kind.value == "DEPENDENCY"


@direct
def test_a_pending_without_frame_cannot_be_composed_on_it_is_the_declared_red():
    """M2A4-D14: a first-turn pending of M1 has no frame M2a can compose on (``DEP-M1-PENDING-FRAME``, M2A-1b)."""
    frameless = replace(no_anchor().pending, reason="UNRESOLVED_VALUE:TIME", answer_slots=frozenset({TIME_POS}))
    d = respond(frameless, turn(set_op(D.TIME, "x")))
    assert (d.kind.value, d.dependency.dep_id, d.pending) == ("DEPENDENCY", "DEP-M1-PENDING-FRAME", frameless)


@direct
def test_no_anchor_pending_a_fragment_is_row_8_and_the_complete_search_is_a_new_search():
    p = no_anchor().pending
    f = facts(anchor=AnchorState.NONE)
    d = respond(p, turn(set_op(D.TIME, "x"), facts=f))
    assert (d.kind.value, d.code, d.pending) == ("REASK", "PENDING_UNANSWERED", replace(p, asks=2))
    ns = respond(p, turn(facts=new_search_facts(anchor=AnchorState.NONE)))
    assert (ns.kind.value, ns.control.value, ns.context_reset) == ("NEW_SEARCH", "CANCEL_PENDING", False)


@direct
def test_a_turn_with_no_pending_is_not_this_modules_and_events_are_validated():
    with pytest.raises(ContractError):
        respond(None, turn(set_op(D.TIME, "x")))
    with pytest.raises(ContractError):
        respond(r31()[1].pending, "turn")
    with pytest.raises(ContractError):
        respond(r31()[1].pending, P.OptionEvent(" ", "c1"))
    with pytest.raises(ContractError):
        P.respond(r31()[1].pending, turn(), trace_id=" ", now=T0, asked_ops=())
    with pytest.raises(ContractError):
        P.respond(r31()[1].pending, turn(), trace_id="t2", now=T0, asked_ops=("op",))
    with pytest.raises(ContractError):
        respond(r31()[1].pending, turn(facts=facts(input_kind=InputKind.EVENT)))
    with pytest.raises(ContractError):
        respond(r31()[1].pending, turn(facts=facts(pending=False)))


@direct
def test_inv_m2a_23_a_pending_that_stops_existing_is_answered_cancelled_or_replaced():
    """INV-M2A-23 as a property of the decision: ``control`` ⇔ ``cancellation``; a cancelled pending does not remain, and
    the pending a decision leaves is the one it kept (``asks``…), or the new one it created."""
    answers = (turn(set_op(D.TIME, "x")), turn(), turn(no_selection=True), turn(facts=new_search_facts()),
               turn(set_op(D.TIME, "x"), facts=facts(cancel_directive=True, continuation=True)),
               turn(facts=facts(cancel_directive=True, own_conditions=0)),
               *(turn(facts=facts(cancel_directive=True, continuation=True, **extra)) for extra in (  # rows 4, 4b, 5, 6 + directive
                   dict(foreign_operation=Operation.COUNT), dict(unsupported_motive="MULTI_REQUEST"),
                   dict(referent=ReferentSignal.EXTENSIONAL), dict(relative_non_temporal=True))))
    for name, make in PENDINGS.items():
        pending = make()
        for t in answers:
            d = respond(pending, t) if not (name == "NO_ANCHOR" and t.facts.cancel_directive) else respond(
                pending, turn(*t.ops, facts=replace(t.facts, anchor=AnchorState.NONE)))
            assert (d.control is not None) == (d.cancellation is not None)
            if d.cancellation is not None:
                assert d.pending is None or d.pending is (d.created and d.created.pending)
            elif d.pending is not None:
                assert replace(d.pending, asks=pending.asks) == pending  # kept, at most with one more ask
            else:
                assert d.kind.value == "PENDING_ANSWER"  # the only way a pending leaves with no cancellation: answered


@direct
def test_a_turn_of_rows_4_5_or_6_that_carries_the_cancel_directive_cancels_the_pending():
    """§17.2 (a): the directive cancels the pending. §7.1 exempts EXPLICITLY only row 4b («ancla y pendiente intactas (no se
    consumen ni se cancelan)»), and §8.4 says row 6 gets there «salvo que §8.3 la cancele antes» (ZEUS P2-1, P3-3; M2A4-D21,
    D27). So rows 4 (operation change), 5 (extensional referent) and 6 (relative) with the directive cancel — the rest of the
    turn as it was: INFORM, the same code, 0 calls, no pending — and row 4b keeps the pending intact."""
    rows = {"OUT_OPERATION_CHANGE": (dict(foreign_operation=Operation.COUNT), "OPERATION_CHANGE:COUNT"),
            "OUT_REFERENT_EXTENSIONAL": (dict(referent=ReferentSignal.EXTENSIONAL), "REFERENT_EXTENSIONAL"),
            "OUT_RELATIVE_TO_PREVIOUS": (dict(relative_non_temporal=True), "RELATIVE_TO_PREVIOUS_VALUE")}
    for name in ("H6", "R3-1", "H1", "H11"):
        pending = PENDINGS[name]()
        for scope, (extra, code) in rows.items():
            d = respond(pending, turn(facts=facts(cancel_directive=True, continuation=True, **extra)))
            assert (d.kind.value, d.scope.value, d.code, d.pending, d.answer, d.outcome) == (
                "INFORM", scope, code, None, None, None), (name, scope)
            assert d.control.value == "CANCEL_PENDING" and d.cancellation.cause.value == "DIRECTIVE", (name, scope)
            assert d.anchor_eliminated is (name == "H1"), (name, scope)
            kept = respond(pending, turn(facts=facts(continuation=True, **extra)))  # no directive: intact (§8.4, §7.1)
            assert (kept.pending, kept.control, kept.code) == (pending, None, code), (name, scope)
        for ops in ((), (set_op(D.GEOGRAPHY, "z"),)):  # what else P says does not change it: row 6 with a fragment too
            d = respond(pending, turn(*ops, facts=facts(cancel_directive=True, continuation=True, relative_non_temporal=True)))
            assert (d.kind.value, d.pending, d.control.value) == ("INFORM", None, "CANCEL_PENDING"), (name, ops)
        # row 4b: «no se consumen ni se cancelan», with or without the directive
        for directive in (False, True):
            d = respond(pending, turn(facts=facts(cancel_directive=directive, continuation=True,
                                                  unsupported_motive="MULTI_REQUEST")))
            assert (d.kind.value, d.scope.value, d.pending, d.control, d.cancellation) == (
                "INFORM", "OUT_UNSUPPORTED_PROPOSAL", pending, None, None), (name, directive)


@direct
def test_respond_never_takes_no_asked_patch_in_silence():
    """M2A4-D22 (ZEUS P2-2): F24 needs the asking patch and the types persist none (§7 of the close: activation gate of
    M2A-6). ``asked_ops=()`` is not a way to say «none»: a text turn that meets a pending WITH candidates must declare it."""
    _, made = h6()
    p, t = made.pending, turn(set_op(D.GEOGRAPHY, "y"))
    now = T0 + MINUTE
    with pytest.raises(ContractError, match="ASKED_OPS_UNDECLARED"):
        P.respond(p, t, trace_id="t2", now=now, asked_ops=())
    declared = P.respond(p, t, trace_id="t2", now=now, asked_ops=(), no_asked_patch=True)
    assert declared.kind.value == "PENDING_ANSWER"  # declared: F24 has nothing to refuse
    given = P.respond(p, t, trace_id="t2", now=now, asked_ops=made.asked_ops)
    assert (given.kind.value, given.code) == ("REASK", "ANSWER_UNRESOLVED")
    for bad in (dict(asked_ops=made.asked_ops, no_asked_patch=True), dict(asked_ops=(), no_asked_patch="yes")):
        with pytest.raises(ContractError, match="INVALID_ASKED_OPS"):
            P.respond(p, t, trace_id="t2", now=now, **bad)
    # what needs no declaration: no candidate, an expired-anchor confirmation (its candidate IS the asking patch), an event
    month = authority_month()[1].pending
    assert P.respond(month, turn(set_op(D.TIME, "x")), trace_id="t2", now=now, asked_ops=()).kind.value == "PENDING_ANSWER"
    h1p = h1()[1].pending
    assert P.respond(h1p, turn(set_op(D.TIME, "y")), trace_id="t2", now=now, asked_ops=()).kind.value == "REASK"
    assert P.respond(p, P.OptionEvent(p.pending_id, "c1"), trace_id="t2", now=now, asked_ops=()).kind.value == (
        "PENDING_ANSWER")


@direct
def test_the_floor_of_d7_holds_when_there_is_no_base_too():
    """ZEUS P3 (a): with no base the rest of P is a new search only if it has an own condition, as in ``classify_turn``
    (§7.1/§7.2, D7): ``own_conditions = 0`` is never a search, whatever else the facts claim."""
    _, made = h1()
    p = made.pending
    floor = facts(cancel_directive=True, own_conditions=0, admissible_alone=True, continuation=False, frame=None,
                  anchor_expired=True)
    d = respond(p, turn(clear_op(TIME_POS), facts=floor))
    assert (d.kind.value, d.code, d.scope.value) == ("CLARIFY", "NO_ANCHOR", "OUT_NO_ANCHOR")
    assert d.control.value == "CANCEL_PENDING" and d.anchor_eliminated
    with_condition = respond(p, turn(clear_op(TIME_POS), facts=replace(floor, own_conditions=1)))
    assert with_condition.kind.value == "NEW_SEARCH"
    with pytest.raises(ContractError, match="UNDETERMINED_FACT:.*own_conditions"):
        respond(p, turn(clear_op(TIME_POS), facts=replace(floor, own_conditions=None)))


@direct
def test_ask_fresh_with_a_section_19_dependency_in_the_frame_is_that_dependency():
    """ZEUS P3 (b), U29: a frame that raises a §19 COMPOSICION dependency is that DEPENDENCY, never an INFORM, also for a new
    search that asks (here a SMMLV bound whose year is not the TIME's: DEP-SMMLV-RECALC)."""
    smmlv = fa.Provenance(unit=fa.Unit.SMMLV, source_amount="500", unit_source=fa.UnitSource.STATED, year=2025,
                          year_source=fa.YearSource.STATED)
    bound = MoneyValue(650_000_000, Comparator.GTE)
    frame = base_of((D.TOPIC, "x"), (D.TIME, "y"), (D.VALUE, bound, smmlv))  # TIME is 2024, the bound's year 2025
    question = P.AuthorityQuestion("MONTH_YEAR_AMBIGUOUS", origin("t1", 0, RAW_TURN), frozenset({TIME_POS}))
    made = P.ask_fresh(question, frame=frame, asked_trace_id="t1", now=T0, asked_ops=())
    assert (made.disposition.value, made.pending, made.dependency.dep_id) == ("DEPENDENCY", None, "DEP-SMMLV-RECALC")
    clean = base_of((D.TOPIC, "x"), (D.TIME, "x"), (D.VALUE, bound, smmlv))  # the bound's year IS the TIME's: no dependency
    assert P.ask_fresh(question, frame=clean, asked_trace_id="t1", now=T0, asked_ops=()).disposition.value == "CLARIFY"


@direct
def test_the_span_of_a_pending_is_the_hull_of_its_open_operations_whatever_their_order():
    """§8.2 (ZEUS Z7): «una pendiente nace con un span»: with several open operations it is the envelope of their citations,
    never the first one, in any order; with citations of two texts, or an event, it is born of no span."""
    base = base_of((D.TOPIC, "x"))
    first, second, third = set_op(D.TIME, "x", i=4), set_op(D.BUYER, "x", i=0), set_op(D.GEOGRAPHY, "x", i=2)  # (12,14) (0,2) (6,8)
    for ops in ((first, second, third), (third, first, second), (second, third, first)):
        made = P.pending_from_question(_question_with(base, open_ops=ops), base=base, base_trace_id="t0",
                                       asked_trace_id="t1", now=T0)
        assert (made.pending.ambiguous_span, made.pending.ambiguous_raw_sha256) == ((0, 14), RAW_TURN), ops
    other_text = condition(D.TIME, typed_value(D.TIME, "x"), "p9", origin("t1", 20, "c" * 64))
    elsewhere = fa.PatchOp(fa.PatchOpKind.SET, other_text.slot, other_text, None, other_text.origin)
    mixed = P.pending_from_question(_question_with(base, open_ops=(first, elsewhere)), base=base, base_trace_id="t0",
                                    asked_trace_id="t1", now=T0)
    assert (mixed.pending.ambiguous_span, mixed.pending.ambiguous_raw_sha256) == (None, None)
    event = fa.ConditionOrigin(fa.OriginKind.OPTION_EVENT, "t1", event_id="e1")
    evented = condition(D.TIME, typed_value(D.TIME, "x"), "p8", event)
    by_event = fa.PatchOp(fa.PatchOpKind.SET, evented.slot, evented, None, event)
    born = P.pending_from_question(_question_with(base, open_ops=(by_event,)), base=base, base_trace_id="t0",
                                   asked_trace_id="t1", now=T0)
    assert born.pending.ambiguous_span is None


# ---- the decision type ----------------------------------------------------------------------------------------------
@direct
def test_a_decision_cannot_cancel_a_pending_and_leave_it_standing():
    _, made = r31()
    cancel = P.cancel_pending(made.pending, P.CancelCause.DIRECTIVE)
    with pytest.raises(ContractError):
        P.PendingDecision(P.DecisionKind.INFORM, None, None, cancel, pending=made.pending)


@direct
def test_the_modules_pin_the_closed_vocabularies_they_copy():
    """The constants M2A-4 copies from the contract and from §19 are what the sealed text says."""
    assert P.EXPIRED_CONFIRM == "CONTEXT_EXPIRED_CONFIRM" == fo.HOST_CODES["H1"]
    assert P.RULE_OF_CODE == {code: rule for rule, code in fo.HOST_CODES.items()}
    assert fa.MAX_ASKS == 2 and P.PENDING_TTL == timedelta(minutes=30)
    assert P.DEP_M1_PENDING_FRAME == fo.Dependency("DEP-M1-PENDING-FRAME", ("M2A-1b",))


# ---- mutants: each one must be noticed by the direct tests above ---------------------------------------------------
MUTANTS = {
    # the minimum the lane promises
    "f24_off": {"foundation_pending_answer": [(
        "    if ops and not said and ops_identity(ops) == ops_identity(asked):  # step 2, F24: before normalizing (P is not",
        "    if False:  # step 2, F24: before normalizing (P is not")]},
    # D20 (HERA on ZEUS P1-1): F24 on the normalized answer, and on a part of the asking patch
    "f24_only_before_normalizing": {"foundation_pending_answer": [(
        "        if not said and adds_nothing(ops, asked):\n            return _reask(7, \"ANSWER_UNRESOLVED\")\n", "")]},
    "subset_of_the_asking_patch_accepted": {"foundation_pending_answer": [(
        "        if not said and adds_nothing(ops, asked):",
        "        if not said and ops and ops_identity(ops) == ops_identity(asked):")]},
    "f24_normalized_for_every_pending": {"foundation_pending_answer": [(  # D26: the narrowing D20 got wrong
        "    if pending.candidate_patches:  # step 3\n        ops = without_identity_keeps(pending.frame, ops, unresolved)\n",
        "    stripped = without_identity_keeps(pending.frame, ops, unresolved)\n"
        "    if stripped and not said and ops_identity(stripped) == ops_identity(asked):\n"
        "        return _reask(7, \"ANSWER_UNRESOLVED\")\n"
        "    if pending.candidate_patches:  # step 3\n        ops = without_identity_keeps(pending.frame, ops, unresolved)\n")]},
    "f24_part_counts_what_is_unresolved": {"foundation_pending_answer": [(
        "        if not said and adds_nothing(ops, asked):", "        if adds_nothing(ops, asked):")]},
    "f24_part_accepts_an_empty_answer": {"foundation_pending_answer": [(
        "    return bool(left) and not (left - asked_left)", "    return not (left - asked_left)")]},
    "directive_cancels_no_out_row": {"foundation_pending": [(
        "    if scope in _CANCELLED_BY_DIRECTIVE and facts.cancel_directive:", "    if False:")]},
    "relative_turn_cancel_ignored": {"foundation_pending": [(
        "_CANCELLED_BY_DIRECTIVE = _INFORM_ROWS - {TurnScope.OUT_UNSUPPORTED_PROPOSAL}",
        "_CANCELLED_BY_DIRECTIVE = _INFORM_ROWS - {TurnScope.OUT_UNSUPPORTED_PROPOSAL, TurnScope.OUT_RELATIVE_TO_PREVIOUS}")]},
    "rows_4_and_5_cancel_ignored": {"foundation_pending": [(
        "_CANCELLED_BY_DIRECTIVE = _INFORM_ROWS - {TurnScope.OUT_UNSUPPORTED_PROPOSAL}",
        "_CANCELLED_BY_DIRECTIVE = frozenset({TurnScope.OUT_RELATIVE_TO_PREVIOUS})")]},
    "row_4b_cancelled_by_the_directive": {"foundation_pending": [(
        "_CANCELLED_BY_DIRECTIVE = _INFORM_ROWS - {TurnScope.OUT_UNSUPPORTED_PROPOSAL}",
        "_CANCELLED_BY_DIRECTIVE = _INFORM_ROWS")]},
    "asked_ops_omission_is_silent": {"foundation_pending": [(
        "    if pending.candidate_patches and not asked_ops and not no_asked_patch and not holds_expired_anchor(pending):",
        "    if False:")]},
    "own_conditions_none_accepted": {"foundation_pending": [(
        "    for name in (\"admissible_alone\", \"continuation\", \"own_conditions\"):",
        "    for name in (\"admissible_alone\", \"continuation\"):")]},
    "ask_fresh_asked_ops_defaults_to_nothing": {"foundation_pending_create": [(
        "              now: datetime, asked_ops) -> Creation:", "              now: datetime, asked_ops=()) -> Creation:")]},
    "no_base_search_ignores_the_floor": {"foundation_pending": [(
        "    if facts.admissible_alone is False or facts.continuation is True or facts.own_conditions == 0:",
        "    if facts.admissible_alone is False or facts.continuation is True:")]},
    "fresh_frame_dependency_as_inform": {"foundation_pending_create": [(
        "    _, refused = _judge(frame, question.reason, (), asked_ops, question.candidates, question.answer_slots,\n"
        "                        asked_trace_id, question.origin)\n    if refused is not None:\n        return refused\n",
        "    found = law_violations(frame, (), question.candidates, question.answer_slots, asked_trace_id, question.origin,\n"
        "                           RULE_OF_CODE.get(question.reason))\n    if found:\n"
        "        return Creation(DeltaDisposition.INFORM, _inform_code(question.reason), asked_ops=asked_ops,\n"
        "                        violations=tuple(found))\n")]},
    "span_is_the_first_citation": {"foundation_pending_create": [(
        "                           (min(o.span[0] for o in origins), max(o.span[1] for o in origins)))", "                           first.span)")]},
    "span_ends_at_the_first_citation": {"foundation_pending_create": [(
        "(min(o.span[0] for o in origins), max(o.span[1] for o in origins))", "(min(o.span[0] for o in origins), first.span[1])")]},
    "f25_executes": {"foundation_pending_answer": [(
        "    if touched - inside:\n        return _reask(5, \"ANSWER_EDITS_OTHER_SLOT\")\n    if said or pending.reason == EXPIRED_CONFIRM:",
        "    if said or pending.reason == EXPIRED_CONFIRM:")]},
    "f29_executes": {"foundation_pending_answer": [(
        "    if not touched & inside:  # step 4 (an empty P included: F29, never a cancellation)\n"
        "        return _reask(8, \"PENDING_UNANSWERED\")\n", "")]},
    "empty_list_cancels": {"foundation_pending": [(
        "    result = answer_in_language(pending, turn.ops, no_selection=turn.no_selection, asked_ops=asked_ops,\n"
        "                                unresolved=turn.unresolved)",
        "    if not turn.ops and not turn.no_selection:\n        return _pure_cancel(pending, CancelCause.DIRECTIVE, TurnScope.IN)\n"
        "    result = answer_in_language(pending, turn.ops, no_selection=turn.no_selection, asked_ops=asked_ops,\n"
        "                                unresolved=turn.unresolved)")]},
    "answer_on_base_not_frame": {"foundation_pending_answer": [(
        "    made = compose_answer(pending.frame, ops, base_trace_id=pending.asked_trace_id, answered=answered)",
        "    made = compose_answer(BASE_OF[pending.pending_id], ops, base_trace_id=pending.asked_trace_id, "
        "answered=answered)")]},
    "answer_with_the_anchors_trace": {"foundation_pending_answer": [(
        "    made = compose_answer(pending.frame, ops, base_trace_id=pending.asked_trace_id, answered=answered)",
        "    made = compose_answer(pending.frame, ops, base_trace_id=pending.base_trace_id, answered=answered)")]},
    "expiry_off_by_one": {"foundation_pending_clock": [(
        "    return _instant(now, \"NOW\") >= deadline(pending)", "    return _instant(now, \"NOW\") > deadline(pending)")]},
    "ttl_thirty_one_minutes": {"foundation_pending_clock": [(
        "PENDING_TTL = timedelta(minutes=30)", "PENDING_TTL = timedelta(minutes=31)")]},
    "deadline_trusts_the_record": {"foundation_pending_clock": [(
        "    return min(pending.expires_at_utc, expires_at(pending.created_at_utc))", "    return pending.expires_at_utc")]},
    "asks_cap_plus_one": {"foundation_pending": [(
        "    if pending.asks >= MAX_ASKS:  # §8.3", "    if pending.asks > MAX_ASKS:  # §8.3")]},
    "asks_never_counted": {"foundation_pending": [(
        "                           pending=replace(pending, asks=pending.asks + 1))", "                           pending=pending)")]},
    "impossible_candidate_accepted": {"foundation_pending_create": [(
        "    found = law_violations(base, frame_ops, candidates, answer_slots, trace, origin, RULE_OF_CODE.get(reason))\n"
        "    if found:",
        "    found = law_violations(base, frame_ops, candidates, answer_slots, trace, origin, RULE_OF_CODE.get(reason))\n"
        "    if False:")]},
    "frame_dependency_laundered": {"foundation_pending_create": [(
        "        if obstacle is not None and obstacle.kind is FindingKind.DEPENDENCY:\n            return None, Creation(",
        "        if False:\n            return None, Creation(")]},
    "fresh_frame_not_judged": {"foundation_pending_create": [(
        "    _, refused = _judge(frame, question.reason, (), asked_ops, question.candidates, question.answer_slots,\n"
        "                        asked_trace_id, question.origin)\n    if refused is not None:\n        return refused\n",
        "    refused = None\n")]},
    # the rows
    "row_5_before_row_6_swapped": {"foundation_pending_answer": [(
        "        if touched & inside and touched - inside:\n            return _reask(5, \"ANSWER_EDITS_OTHER_SLOT\")\n"
        "        return _reask(6, \"OPTION_NOT_CHOSEN\")", "        return _reask(6, \"OPTION_NOT_CHOSEN\")")]},
    "conflict_not_decided_on_p": {"foundation_pending_answer": [(
        "    if slot_conflicts(ops, unresolved):  # step 1", "    if False:  # step 1")]},
    "normalization_without_candidates": {"foundation_pending_answer": [(
        "    if pending.candidate_patches:  # step 3\n        ops = without_identity_keeps(pending.frame, ops, unresolved)\n",
        "    if True:  # step 3\n        ops = without_identity_keeps(pending.frame, ops, unresolved)\n")]},
    "normalization_drops_every_keep": {"foundation_pending_answer": [(
        "        if op.op is PatchOpKind.KEEP and op.slot.key in present and op.slot.key not in others:",
        "        if op.op is PatchOpKind.KEEP:")]},
    "two_candidates_selected": {"foundation_pending_answer": [(
        "        if len(chosen) != 1:\n            return _reask(7, \"ANSWER_UNRESOLVED\")\n"
        "        return _compose_on_frame(pending, chosen[0].patch.ops",
        "        if not chosen:\n            return _reask(7, \"ANSWER_UNRESOLVED\")\n"
        "        return _compose_on_frame(pending, chosen[0].patch.ops")]},
    "p_composed_instead_of_the_candidate": {"foundation_pending_answer": [(
        "        return _compose_on_frame(pending, chosen[0].patch.ops, RULE_OF_CODE.get(pending.reason),",
        "        return _compose_on_frame(pending, ops, RULE_OF_CODE.get(pending.reason),")]},
    "selection_by_the_slot_only": {"foundation_pending_answer": [(
        "    return op.op.value, op.slot.key, payload", "    return op.op.value, op.slot.key, ()")]},
    "answered_rule_not_exempt": {"foundation_pending_answer": [(
        "    answered = RULE_OF_CODE.get(pending.reason)\n    made = _compose_on_frame(pending, candidate.patch.ops, answered, candidate_id)",
        "    made = _compose_on_frame(pending, candidate.patch.ops, None, candidate_id)")]},
    "still_asking_executes": {"foundation_pending_answer": [(
        "    return _reask(7, \"ANSWER_UNRESOLVED\")\n\n\ndef _compose_on_frame",
        "    return AnswerResult(AnswerKind.EXECUTE, 4, None, composed, None, ops)\n\n\ndef _compose_on_frame")]},
    "dependency_swallowed": {"foundation_pending_answer": [(
        "    if first.kind is FindingKind.DEPENDENCY:\n        return AnswerResult(AnswerKind.DEPENDENCY, 4, None, None, first.dependency, ops)\n    return _reask(7, \"ANSWER_UNRESOLVED\")",
        "    return _reask(7, \"ANSWER_UNRESOLVED\")")]},
    "unresolved_value_ignored": {"foundation_pending_answer": [(
        "    touched = covered_keys(ops) | said\n    if not touched & inside:  # step 4",
        "    touched = covered_keys(ops)\n    if not touched & inside:  # step 4")]},
    "unresolved_rest_of_a_cancel_dropped": {"foundation_pending": [(
        "    if turn.unresolved:  # the rest of P says a value no authority resolved: nothing to compose it as, and no span to ask on\n",
        "    if False:  # the rest of P says a value no authority resolved: nothing to compose it as, and no span to ask on\n")]},
    # D2 (P1-2)
    "expired_confirm_selected_in_language": {"foundation_pending_answer": [(
        "    if said or pending.reason == EXPIRED_CONFIRM:  # a value no authority resolved selects nothing (row 7); M2A4-D2:\n"
        "        return _reask(7, \"ANSWER_UNRESOLVED\")  # «continuar» is confirmed only by event\n",
        "    if said:\n        return _reask(7, \"ANSWER_UNRESOLVED\")\n")]},
    # D1 (P1-1)
    "reset_fragment_ignores_continuation": {"foundation_pending": [(
        "    if facts.admissible_alone is False or facts.continuation is True or facts.own_conditions == 0:",
        "    if facts.admissible_alone is False or facts.own_conditions == 0:")]},
    "reset_fragment_composed_on_the_expired_frame": {"foundation_pending_cancel": [(
        "    if holds_expired_anchor(pending) or pending.frame is None:", "    if pending.frame is None:")]},
    # D3 (P1-3)
    "only_a_c_d_remove_the_anchor": {"foundation_pending_cancel": [(
        "    return Cancellation(cause, pending.pending_id, pending.asked_trace_id, holds_expired_anchor(pending))",
        "    return Cancellation(cause, pending.pending_id, pending.asked_trace_id,\n"
        "                        holds_expired_anchor(pending) and cause in (CancelCause.DIRECTIVE, CancelCause.RESET,\n"
        "                                                                    CancelCause.EXHAUSTED))")]},
    "any_cancellation_removes_the_anchor": {"foundation_pending_cancel": [(
        "    return Cancellation(cause, pending.pending_id, pending.asked_trace_id, holds_expired_anchor(pending))",
        "    return Cancellation(cause, pending.pending_id, pending.asked_trace_id, True)")]},
    "reset_for_any_pending": {"foundation_pending_cancel": [(
        "    if cause is CancelCause.RESET and not holds_expired_anchor(pending):", "    if False:")]},
    # INV-M2A-23 and the events
    "new_search_without_the_cancellation": {"foundation_pending": [(
        "    return PendingDecision(DecisionKind.NEW_SEARCH, TurnScope.IN_NEW_SEARCH, None, cancel_pending(pending, cause),\n"
        "                           context_reset=context_reset)",
        "    return PendingDecision(DecisionKind.NEW_SEARCH, TurnScope.IN_NEW_SEARCH, None, None,\n"
        "                           context_reset=context_reset)")]},
    "exhausted_pending_stays": {"foundation_pending": [(
        "        return PendingDecision(DecisionKind.REASK, scope, result.code, cancel_pending(pending, CancelCause.EXHAUSTED),\n"
        "                               answer=result)",
        "        return PendingDecision(DecisionKind.REASK, scope, result.code, answer=result, pending=pending)")]},
    "stale_event_executes": {"foundation_pending": [(
        "    if event.pending_id != pending.pending_id:\n        return _stale(pending)", "    if False:\n        return _stale(pending)")]},
    "expired_pending_still_answers": {"foundation_pending": [(
        "    if pending_expired(pending, now):  # §17.2 e: the turn proceeds as if there were none; the cancellation is written",
        "    if False:")]},
    "cancelled_pending_replaced_by_nothing": {"foundation_pending": [(
        "        return PendingDecision(DecisionKind.CLARIFY, TurnScope.OUT_NO_ANCHOR, NO_ANCHOR,\n"
        "                               cancel_pending(pending, CancelCause.DIRECTIVE), created=created, pending=created.pending)",
        "        return PendingDecision(DecisionKind.CLARIFY, TurnScope.OUT_NO_ANCHOR, NO_ANCHOR,\n"
        "                               cancel_pending(pending, CancelCause.DIRECTIVE), created=created, pending=None)")]},
    "delta_after_cancel_on_a_stale_base": {"foundation_pending": [(
        "    base_trace = pending.asked_trace_id  # §17.3", "    base_trace = pending.base_trace_id or 't0'  # §17.3")]},
}


def killers(name: str) -> list[str]:
    """The direct tests that notice mutant ``name`` (loaded under private names, swapped in for ``P``)."""
    mutated = load(MUTANTS[name])
    BASES = {}
    for stem in ("foundation_pending_answer",):
        sys.modules[f"m2a4_mut_{stem}"].BASE_OF = BASES  # the injected table of ``answer_on_base_not_frame``
    saved, found = globals()["P"], []
    globals()["P"] = mutated
    try:
        if name == "answer_on_base_not_frame":
            _, made = authority_month()
            BASES[made.pending.pending_id] = base_of((D.TOPIC, "x"), (D.BUYER, "x"), (D.TIME, "x"))
        for function in DIRECT:
            try:
                function()
            except (Exception, pytest.fail.Exception):  # noqa: BLE001 - any failure of a test is a kill
                found.append(function.__name__)
    finally:
        globals()["P"] = saved
        for stem in STEMS:
            sys.modules.pop(f"m2a4_mut_{stem}", None)
    return found


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_mutant_is_killed(name):
    assert killers(name), f"mutant {name} survives every direct test"


def test_the_unmutated_modules_pass_every_direct_test():
    for function in DIRECT:
        function()
