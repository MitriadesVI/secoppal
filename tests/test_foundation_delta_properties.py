"""M2A-3 phase 2: properties of the reducer over GENERATED rich bases (Hypothesis, fixed seed in the gates).

The bases carry what the Gold's do not (``LG-BASES-WITHOUT-HOST-FIELDS``): request policies, receipt rules, condition
policies, a filled ``Provenance`` (units, SMMLV amounts and years, buyer scopes, inheritance origins, order sources),
dual anchors, opportunities requests and OR groups, with the host's real subject-to-dataset table (exact dataset check,
``LG-DATASET-VALUE``). Every property is judged by the FROZEN oracle's ``check_transition`` where it can judge:

* P1 transition: ``delta_outcome`` → ``check_transition`` (the effective it EXECUTEs, or the code it refuses with) ⇒ 0
  violations. The causes the oracle cannot see are declared (``LG-AUTHORITY-REFUSALS``, ``LG-TARGET-NOT-ADDRESSABLE``):
  a policy without owner and the opportunities lens (§6.6, §19) and TARGET_NOT_ADDRESSABLE. For those this module
  checks, on its own, that the reducer's VISIBLE causes are exactly the oracle's ``refusal_causes`` and that only an
  invisible cause of higher precedence can stand above them.
* P2 the permutation of the ops changes nothing; P3 determinism (same hash); P4 the effective is canonical;
* P5 the universal property: always a valid effective, a representable answer (an A.3 code of its disposition; a
  CLARIFY's pending constructible and answerable), or a declared §19 COMPOSICION dependency; never an exception;
* P6 a request policy stays exactly when every condition it cites is carried; P7 TARGET_NOT_ADDRESSABLE over a group;
  P8 the §6.5 effects, derived here independently from the typed values.

The reducer mutants the Gold does not discriminate (oracle manifest: ``f16_any_value``, ``set_clears_whole_dimension``,
``clear_dimension_only_positive``, ``redirect_keeps_old_kind``, H6 over-fire) and one mutant per rule are killed by
these properties (``test_every_reducer_mutant_is_killed``). The rules the generator reaches only rarely are pinned one
by one in ``test_foundation_delta_rules`` (ZEUS NO_GO on 15e72829, P2-7).

Beyond the oracle (ZEUS repair): §8.1/§8.4 the candidates each pending source carries (H1 «continuar», H6's two, R3-1's
C1) and the A.3 dimension of an UNRESOLVED_VALUE held by its ``answer_slots``; §6.6 nothing that needs a host policy
without owner executes; §6 step 3b one cause per operation. ZEUS on the integration 8066e03d: the frame of a CLARIFY,
read from the base, raises no §19 COMPOSICION dependency, and no candidate launders one (``laundering_problems``).
"""
from __future__ import annotations

import json
import sys
import types
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
from app.core.foundation_authority_values import (
    ActorValue, BuyerKind, BuyerValue, CardinalityBasis, CardinalityValue, Comparator, ContractTypeValue, GeoValue,
    ModalityValue, MoneyValue, OrderDirection, OrderKey, OrderValue, StateValue, TimeValue, TopicValue, decode_value,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import (
    ConditionExpression, ContractError, Effect, HostPolicy, LogicOperator, Polarity, ValueSide,
)
from app.core.semantic_dimensions import SemanticDimension as D
from scripts.audits.m2a_conversation import m2a_oracle_transition as transition

ROOT = Path(__file__).resolve().parents[1]
S19 = json.loads((ROOT / "docs/audits/m2a_conversation/gold/CONTRACT_S19_DEPENDENCIES.json").read_text("utf-8"))
COMPOSICION = {d["dep_id"] for d in S19["dependencies"] if d["impacto_m2a3"] == "COMPOSICION"}
PROPS = settings(max_examples=250, deadline=None,
                 suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
RAW_BASE, RAW_TURN = "a" * 64, "b" * 64
T0 = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
POS, NEG = Polarity.POSITIVE, Polarity.NEGATIVE


def dataset_of(subject: str) -> str:
    return fd.SUBJECT_DATASET[Subject(subject)]


# ---- typed values (pools small on purpose: values collide, so F16/F3/A8.1/conflicts happen) -----------------------
def _time(first: int, last: int, excluded: bool) -> TimeValue:
    return TimeValue(date(first, 1, 1), date(last + 1, 1, 1), "year" if first == last else "range", excluded)


VALUES = {
    D.TOPIC: lambda ex: [TopicValue("aseo", False, ex), TopicValue("vias", False, ex), TopicValue("obras", True, ex)],
    D.BUYER: lambda ex: [BuyerValue(n, BuyerKind.UNIT, (n,), None, ex) for n in ("ICBF", "VALLE", "MEDELLIN")],
    D.ACTOR: lambda ex: [ActorValue("900123456", "NIT", "Acme", ex), ActorValue("800222333", "NIT", "Beta", ex)],
    D.GEOGRAPHY: lambda ex: [GeoValue(GeoLevel.DEPARTMENT, "VALLE", None, "76", (), ex),
                             GeoValue(GeoLevel.DEPARTMENT, "ANTIOQUIA", None, "05", (), ex),
                             GeoValue(GeoLevel.MUNICIPALITY, "CALI", "VALLE DEL CAUCA", "76001", (), ex),
                             GeoValue(GeoLevel.REGION, "CARIBE", None, None, ("08", "13"), ex)],  # SF123: no code
    D.TIME: lambda ex: [_time(2023, 2023, ex), _time(2025, 2025, ex), _time(2026, 2026, ex), _time(2024, 2025, ex)],
    D.STATE: lambda ex: [StateValue("open", (("p6dx-8zbt", "estado_del_procedimiento", ("Abierto",)),), ex),
                         StateValue("terminated", (("jbjy-vk9h", "estado_contrato", ("Terminado",)),), ex),
                         StateValue("published", (), ex)],
    D.MODALITY: lambda ex: [ModalityValue(("minima_cuantia",), None, ex),
                            ModalityValue(("licitacion_publica",), None, ex),
                            ModalityValue(("licitacion_publica",), "modality-lp-family@1", ex)],  # needs a policy
    D.CONTRACT_TYPE_OR_NATURE: lambda ex: [ContractTypeValue(("obra",), ex)],
}
POLARIZED = tuple(VALUES)
BUYER_SCOPES = {"ICBF": "NATIONAL", "VALLE": "76", "MEDELLIN": "05001"}
MONEY = [100, 500, 900]
RULES = {D.TOPIC: ("reanchored",), D.BUYER: ("buyer.exact",), D.GEOGRAPHY: ("geo.exact",), D.TIME: ("year.from_time",),
         D.ACTOR: ("actor.authoritative",), D.VALUE: ("unit.sibling",), D.ORDER: ("order.direction_defaulted",),
         D.CARDINALITY: ("topn.capped",)}


def origin(trace: str, start: int, raw: str) -> fa.ConditionOrigin:
    return fa.ConditionOrigin(fa.OriginKind.SPAN, trace, raw, (start, start + 2))


def condition(dimension: D, value, cid: str, rids, org: fa.ConditionOrigin, provenance=None, rich=True):
    encoded = value.encode()
    slot = fa.slot_of(dimension, encoded)
    if dimension is D.VALUE:
        operator = fa.VALUE_OPERATOR_OF[value.comparator]
    elif dimension is D.ORDER:
        operator = f"{value.key.value}_{value.direction.value}"
    elif dimension is D.CARDINALITY:
        operator = "top_n"
    elif dimension is D.TOPIC:
        operator = "whole_token_folded" if value.token_mode else "contains"
    elif dimension is D.TIME:
        operator = "during_year" if value.precision == "year" else "between_dates"
    elif dimension is D.MODALITY and value.family_policy is not None:
        operator = "family"
    else:
        operator = "is"
    return fa.AcceptedCondition(
        cid, tuple(sorted(rids)), slot, dimension, operator, slot.polarity or POS,
        value.level if dimension is D.GEOGRAPHY else None, encoded, f"{dimension.value.lower()}_authority@1",
        RULES.get(dimension, ()) if rich else (), ("host-policy@1",) if rich and dimension is D.STATE else (), org,
        provenance or fa.Provenance())


@st.composite
def st_provenance(draw, dimension: D, value, rich: bool):
    if not rich:
        if dimension is D.VALUE:
            return fa.Provenance(unit=fa.Unit.PESOS, unit_source=fa.UnitSource.STATED)
        return fa.Provenance()
    if dimension is D.VALUE:
        if draw(st.booleans()):
            return fa.Provenance(unit=fa.Unit.SMMLV, source_amount="500", unit_source=fa.UnitSource.STATED,
                                 year=draw(st.sampled_from([2025, 2026])), year_source=fa.YearSource.STATED)
        if draw(st.integers(0, 3)) == 0:  # a bound of unknown unit (§15.2: comparable with nothing)
            return fa.Provenance()
        unit_source = draw(st.sampled_from(list(fa.UnitSource)))
        return fa.Provenance(unit=draw(st.sampled_from([fa.Unit.PESOS, fa.Unit.MILLONES])), source_amount="5",
                             unit_source=unit_source,
                             inherited_from="t-prev" if unit_source is fa.UnitSource.PREVIOUS_ACCEPTED_VALUE else None)
    if dimension is D.TIME:
        source = draw(st.sampled_from(list(fa.YearSource)))
        return fa.Provenance(year_source=source,
                             inherited_from="t-prev" if source is fa.YearSource.PREVIOUS_ACCEPTED_TIME else None)
    if dimension is D.BUYER:
        return fa.Provenance(buyer_scope=BUYER_SCOPES[value.canonical])
    if dimension is D.ORDER:
        return fa.Provenance(order_direction_source=draw(st.sampled_from(list(fa.OrderDirectionSource))))
    return fa.Provenance()


@st.composite
def st_value(draw, dimension: D, polarity: Polarity | None = None):
    if dimension is D.VALUE:
        return MoneyValue(draw(st.sampled_from(MONEY)), draw(st.sampled_from(list(Comparator))))
    if dimension is D.ORDER:
        return OrderValue(draw(st.sampled_from(list(OrderKey))), draw(st.sampled_from(list(OrderDirection))))
    if dimension is D.CARDINALITY:
        n = draw(st.sampled_from([10, 250]))
        return CardinalityValue(min(n, 100), CardinalityBasis.FIGURE, n)
    excluded = (polarity is NEG) if polarity is not None else draw(st.booleans())
    return draw(st.sampled_from(VALUES[dimension](excluded)))


@st.composite
def st_base(draw, rich=True):
    kind = draw(st.sampled_from(["single", "single", "dual", "opportunities"]))
    if kind == "dual":
        subjects = (Subject.PROCESSES, Subject.CONTRACTS)
    else:
        subjects = (draw(st.sampled_from([Subject.PROCESSES, Subject.CONTRACTS])),)
    universe = fa.UniverseKind.OPPORTUNITIES if kind == "opportunities" else fa.UniverseKind.RECORDS
    requests = tuple(fa.AcceptedRequest(f"r{i}", "s1", subject, universe, origin("t0", 0, RAW_BASE), None,
                                        fd.SUBJECT_DATASET[subject]) for i, subject in enumerate(subjects, 1))
    rids = tuple(r.request_id for r in requests)
    conditions, n = [], 0
    for dimension in draw(st.lists(st.sampled_from(POLARIZED), max_size=5)):
        value = draw(st_value(dimension))
        conditions.append(condition(dimension, value, f"c{n}", rids, origin("t0", 3 * n, RAW_BASE),
                                    draw(st_provenance(dimension, value, rich)), rich))
        n += 1
    for side in draw(st.sets(st.sampled_from([Comparator.GTE, Comparator.LT]))):
        value = MoneyValue(draw(st.sampled_from(MONEY)), side)
        conditions.append(condition(D.VALUE, value, f"c{n}", rids, origin("t0", 3 * n, RAW_BASE),
                                    draw(st_provenance(D.VALUE, value, rich)), rich))
        n += 1
    if draw(st.booleans()):
        value = draw(st_value(D.ORDER))
        conditions.append(condition(D.ORDER, value, f"c{n}", rids, origin("t0", 3 * n, RAW_BASE),
                                    draw(st_provenance(D.ORDER, value, rich)), rich))
        n += 1
        if draw(st.booleans()):
            conditions.append(condition(D.CARDINALITY, draw(st_value(D.CARDINALITY)), f"c{n}", rids,
                                        origin("t0", 3 * n, RAW_BASE), None, rich))
            n += 1
    logic = None
    groupable = [c.condition_id for c in conditions if c.dimension is not D.ORDER]
    if len(groupable) >= 2 and draw(st.integers(0, 3)) == 0:
        pair = draw(st.permutations(groupable))[:2]
        atoms = [ConditionExpression(LogicOperator.ATOM, cid) for cid in pair]
        rest = [ConditionExpression(LogicOperator.ATOM, c.condition_id) for c in conditions
                if c.condition_id not in pair]
        group = ConditionExpression(LogicOperator.OR, None, tuple(atoms))
        logic = ConditionExpression(LogicOperator.AND, None, (group, *rest)) if rest else group
    policies = []
    if rich:
        for policy_id, effect in (("ADR-100", Effect.EQUIVALENT), ("topn", Effect.NARROWING)):
            for cid in sorted(draw(st.sets(st.sampled_from([c.condition_id for c in conditions] or ["none"]),
                                           max_size=2))):
                if cid != "none":
                    policies.append(HostPolicy(policy_id, "1", (cid,), effect))
    return fa.canonical_interpretation(requests, tuple(conditions), logic, tuple(policies), None, "t-root")


SLOTS_FOR_OPS = [*(fa.Slot(d, p) for d in POLARIZED for p in (POS, NEG)), fa.Slot(D.VALUE, side=ValueSide.LOWER),
                 fa.Slot(D.VALUE, side=ValueSide.UPPER), fa.Slot(D.ORDER), fa.Slot(D.CARDINALITY)]
WIDE = [*(fa.Slot(d) for d in POLARIZED), fa.Slot(D.VALUE)]


@st.composite
def st_op(draw, base, index: int, rich=True):
    org = origin("t1", 3 * index, RAW_TURN)
    kind = draw(st.sampled_from(["SET", "SET", "CLEAR", "KEEP", "SET_ORDER", "REPLACE_UNIVERSE"]))
    if kind == "SET":
        reusable = [c for c in base.conditions if c.dimension is not D.ORDER]
        if reusable and draw(st.booleans()):  # reuse a base value: the same one, or its negation (F16)
            source = draw(st.sampled_from(reusable))
            dimension, value = source.dimension, decode_value(source.value)
            if dimension in POLARIZED and draw(st.booleans()):
                value = replace(value, excluded=not value.excluded)
        else:  # TOPIC weighs more: F3/F22 needs a positive TOPIC over another one
            dimension = draw(st.sampled_from([*POLARIZED, D.TOPIC, D.TOPIC, D.VALUE, D.CARDINALITY]))
            value = draw(st_value(dimension))
        prov = draw(st_provenance(dimension, value, rich))
        ids = ["p1", "p2", *(c.condition_id for c in base.conditions)]  # a patch id may collide with a base id
        cond = condition(dimension, value, draw(st.sampled_from(ids)), ("r1",), org, prov, rich)
        return fa.PatchOp(fa.PatchOpKind.SET, cond.slot, cond, None, org)
    if kind == "SET_ORDER":
        value = draw(st_value(D.ORDER))
        cond = condition(D.ORDER, value, "po", ("r1",), org, draw(st_provenance(D.ORDER, value, rich)), rich)
        return fa.PatchOp(fa.PatchOpKind.SET_ORDER, cond.slot, cond, None, org)
    if kind == "REPLACE_UNIVERSE":
        target = fa.UniverseTarget(draw(st.sampled_from([Subject.PROCESSES, Subject.CONTRACTS])),
                                   draw(st.sampled_from([fa.UniverseKind.RECORDS, fa.UniverseKind.RECORDS,
                                                         fa.UniverseKind.OPPORTUNITIES])))
        return fa.PatchOp(fa.PatchOpKind.REPLACE_UNIVERSE, fa.UNIVERSE_SLOT, None, target, org)
    held = [c.slot for c in base.conditions]
    slots = SLOTS_FOR_OPS + (WIDE if kind == "CLEAR" else [fa.UNIVERSE_SLOT]) + held * 2
    slot = draw(st.sampled_from(slots))
    return fa.PatchOp(fa.PatchOpKind[kind], slot, None, None, org)


@st.composite
def st_case(draw, rich=True):
    base = draw(st_base(rich))
    ops = tuple(draw(st_op(base, i, rich)) for i in range(draw(st.integers(1, 4))))
    return base, fa.SemanticPatch(ops, fa.PatchOriginKind.TEXT), draw(st.integers(0, 3)) == 0  # expired: 1 in 4


# ---- what the oracle cannot see ------------------------------------------------------------------------------------
INVISIBLE_CLAUSES = ("§4.2", "§19 DEP-O1B", "§6.6")  # TARGET, the O1B lens and policies without owner


def invisible(cause, base) -> bool:
    """Causes ``refusal_causes`` of the oracle does not compute (declared in its LIMITED_GUARANTEES:
    LG-TARGET-NOT-ADDRESSABLE, LG-AUTHORITY-REFUSALS), by the clause the reducer gives each cause."""
    return cause.clause.startswith(INVISIBLE_CLAUSES)


def transition_violations(delta, outcome_module, base, patch, expired) -> list[str]:
    """P1 and P5 for one case, with the given reducer modules (so mutants can be judged)."""
    out = []
    try:
        outcome = outcome_module.delta_outcome(base, patch, base_trace_id="t0", anchor_expired=expired)
    except Exception as error:  # noqa: BLE001 - the universal property: never an exception
        return [f"P5 exception {type(error).__name__}: {error}"]
    base_d, patch_d = base.to_dict(), patch.to_dict()
    causes = delta.refusal_causes(base, patch.ops)
    oracle_codes = sorted(c["code"] for c in transition.refusal_causes(base_d, patch_d))
    visible = sorted(c.code for c in causes if not invisible(c, base))
    if visible != oracle_codes:
        out.append(f"P1 visible causes {visible} != oracle {oracle_codes}")
    disposition = outcome.disposition.value
    if disposition == "EXECUTE":
        violations = transition.check_transition(base_d, patch_d, outcome.effective.to_dict(), dataset_of=dataset_of)
    elif disposition in ("CLARIFY", "INFORM"):
        violations = transition.check_transition(base_d, patch_d, None, dataset_of=dataset_of, refusal=outcome.code)
        if causes and invisible(causes[0], base):
            violations = [v for v in violations if v["rule"] != "T-REFUSAL-CODE"]
        resolution = fa.TurnResolution[disposition]
        if not fa.reason_code_allowed(outcome.code, resolution):
            out.append(f"P5 {outcome.code} is not an A.3 code of {disposition}")
    else:
        violations = []
        if outcome.dependency is None or outcome.dependency.dep_id not in COMPOSICION:
            out.append(f"P5 dependency {outcome.dependency} is not a §19 COMPOSICION row")
    out += [f"P1 {v['rule']}: {v['detail']}" for v in violations]
    if disposition == "CLARIFY":
        out += pending_problems(outcome_module, outcome)
    return out


def pending_problems(outcome_module, outcome) -> list[str]:
    """§8.4 on a CLARIFY: its pending is constructible and some answer executes (a candidate, or a direct answer)."""
    p = outcome.pending
    candidates = tuple(fa.CandidatePatch(f"k{i}", f"option_{i}", fa.SemanticPatch(ops, fa.PatchOriginKind.TEXT))
                       for i, ops in enumerate(p.candidates))
    try:
        fa.PendingClarification("p1", "t1", T0, T0 + timedelta(minutes=30), 1, p.frame, "t0", None, None, p.reason,
                                candidates, p.answer_slots, frame_ops=p.frame_ops,
                                asked_ops=(*p.frame_ops, *p.open_ops))
    except ContractError as error:
        return [f"P5 the CLARIFY's pending is not constructible: {error}"]
    answered = next((rule for rule, code in fo.HOST_CODES.items() if code == p.reason), None)
    if p.candidates and not any(outcome_module.executes(p.frame, c, "t0", answered=answered) for c in p.candidates):
        return ["P5 no candidate of the CLARIFY executes"]
    return []


def policy_problems(base, outcome) -> list[str]:
    """P6: on an executed delta, a base policy stays iff every condition it cites is carried byte-identical."""
    if outcome.disposition.value != "EXECUTE":  # by value: a mutant module has its own enum class
        return []
    by_id = {c.condition_id: c for c in base.conditions}
    carried = {c.condition_id for c in outcome.effective.conditions if c.condition_id in by_id
               and replace(c, request_ids=by_id[c.condition_id].request_ids) == by_id[c.condition_id]}
    wanted = tuple(p for p in base.policies if set(p.condition_ids) <= carried)
    return [] if outcome.effective.policies == wanted else [f"P6 policies {outcome.effective.policies} != {wanted}"]


def target_problems(delta, base, patch) -> list[str]:
    """P7 (LG-TARGET-NOT-ADDRESSABLE): an op on an atom inside an OR/NOT group is refused as TARGET_NOT_ADDRESSABLE."""
    grouped = delta.grouped_keys(base)
    hit = any(op.op in (fa.PatchOpKind.CLEAR, fa.PatchOpKind.SET, fa.PatchOpKind.KEEP)
              and {s.key for s in delta.covered(op.slot)} & grouped for op in patch.ops)
    result = delta.compose(base, patch, base_trace_id="t0")
    if hit and not (isinstance(result, delta.DeltaRefusal)
                    and any(c.refusal is delta.Refusal.TARGET_NOT_ADDRESSABLE for c in result.causes)):
        return ["P7 an op on a grouped atom is not refused as TARGET_NOT_ADDRESSABLE"]
    return []


def expected_effects(base, patch, effective) -> set[str]:
    """P8: the §6.5 effects, from the typed values: A8.1 and F16 explain a carried condition that vanished with no op
    on its slot; F3/F22 a positive TOPIC SET over a different positive TOPIC of the base."""
    written = {s.key for op in patch.ops if op.op is not fa.PatchOpKind.KEEP for s in fd.covered(op.slot)}
    kept = {c.condition_id for c in effective.conditions}
    out = set()
    for b in base.conditions:
        if b.condition_id in kept or b.slot.key in written:
            continue
        out.add("VALUE_RANGE_REPLACED" if b.dimension is D.VALUE else "POLARITY_INVERTED")
    sets = [op for op in patch.ops if op.op is fa.PatchOpKind.SET and op.slot.key == "TOPIC/POSITIVE"]
    if sets and any(b.slot.key == "TOPIC/POSITIVE" and all(b.value != s.condition.value for s in sets)
                    for b in base.conditions):
        out.add("TOPIC_REPLACED")
    return out


def _smmlv_settled(base, effective) -> bool:
    """§15.5 on an executed effective: every SMMLV bound has a single catalogued year (its TIME's, or none at all)."""
    smmlv = [c for c in effective.conditions if c.dimension is D.VALUE and c.provenance.unit is fa.Unit.SMMLV]
    times = [c for c in effective.conditions if c.dimension is D.TIME and c.polarity is POS]
    if not smmlv:
        return True
    if len(times) > 1:
        return False
    if times:
        decoded = decode_value(times[0].value)
        year = decoded.start.year if decoded.start and decoded.end_exclusive and (
            decoded.end_exclusive - timedelta(days=1)).year == decoded.start.year else None
        return year in {2024, 2025, 2026} and all(c.provenance.year == year for c in smmlv)
    return True


def spec_problems(delta, base, patch, expired, outcome) -> list[str]:
    """The contract's own words for what the oracle leaves unjudged (LG-HOST-RULES-NOT-DECIDED-HERE): §6 step 3b
    (two causes or an expired anchor ⇒ INFORM), H1, H3, H6, H8, H11, H12 and §19 DEP-O1B, written independently of
    the reducer's code paths."""
    out, ops = [], patch.ops
    disposition, code = outcome.disposition.value, outcome.code
    causes = delta.refusal_causes(base, ops)
    if causes and (len(causes) > 1 or expired) and (disposition, code) != ("INFORM", causes[0].published):
        out.append(f"3b several causes / expired must INFORM {causes[0].published}, got {disposition} {code}")
    if expired and disposition == "EXECUTE":
        out.append("H1 an expired anchor executed")
    present = {c.slot.key for c in base.conditions}
    lone_empty_clear = (len(ops) == 1 and ops[0].op is fa.PatchOpKind.CLEAR and bool(present)  # H11 precedes H12
                        and not {s.key for s in delta.covered(ops[0].slot)} & present
                        and _smmlv_settled(base, base))  # and so does H3 (an unreachable SMMLV base, D1/SF168)
    if lone_empty_clear and code != "CLEAR_ON_EMPTY_SLOT":
        out.append(f"H12 a lone CLEAR of an empty slot answered {disposition} {code}")
    if any(op.op is fa.PatchOpKind.REPLACE_UNIVERSE and op.universe.universe_kind is fa.UniverseKind.OPPORTUNITIES
           for op in ops) and disposition == "EXECUTE":
        out.append("DEP-O1B a change to opportunities executed")
    geo = [op.condition for op in ops if op.op is fa.PatchOpKind.SET and op.slot.key == "GEOGRAPHY/POSITIVE"]
    written = {s.key for op in ops if op.op is not fa.PatchOpKind.KEEP for s in delta.covered(op.slot)}
    scopes = {c.provenance.buyer_scope for c in base.conditions if c.slot.key == "BUYER/POSITIVE"
              and "BUYER/POSITIVE" not in written and c.provenance.buyer_scope not in (None, "NATIONAL")}
    conflict = any(scope != decode_value(g.value).divipola for g in geo for scope in scopes)
    if code == "TERRITORY_VS_CARRIED_BUYER" and not conflict:
        out.append("H6 fired with no carried territorial buyer of another territory")
    if disposition == "EXECUTE":
        effective = outcome.effective
        if conflict:
            out.append("H6 a new territory executed next to a carried buyer of another territory")
        if not _smmlv_settled(base, effective):
            out.append("H3 a SMMLV bound executed without a single catalogued year")
        if not effective.conditions:
            out.append("H11 an empty search executed")
        changed = any(op.op is fa.PatchOpKind.REPLACE_UNIVERSE and (len(base.requests) > 1 or (  # §14: dual reduced too
            op.universe.subject, op.universe.universe_kind) not in {(r.subject, r.universe_kind) for r in base.requests})
            for op in ops)
        dataset = effective.requests[0].dataset
        if changed and any(c.dimension is D.MODALITY or (c.dimension is D.STATE and dataset not in {
                entry[0] for entry in decode_value(c.value).literals}) for c in effective.conditions):
            out.append("H8 a STATE or MODALITY executed over a changed universe it is not known in")
    if disposition == "CLARIFY" and code != "CONTEXT_EXPIRED_CONFIRM":  # H1's frame is the expired anchor itself
        frame = outcome_frame_problem(base, outcome)
        if frame:
            out.append(frame)
    out += pending_shape_problems(delta, base, patch, outcome, conflict)
    out += laundering_problems(base, outcome)
    out += policy_owner_problems(base, patch, outcome)
    out += cause_count_problems(causes)
    return out


def _multiset(ops) -> list:
    return sorted(ops, key=repr)


def pending_shape_problems(delta, base, patch, outcome, conflict) -> list[str]:
    """§8.1 and the §8.4 table, from the contract's words: the candidates each source must carry."""
    if outcome.disposition.value != "CLARIFY":
        return []
    p, ops, out = outcome.pending, patch.ops, []
    if p.reason == "CONTEXT_EXPIRED_CONFIRM" and (  # H1: one candidate, «continuar» = the turn's patch on the anchor
            p.frame != base or [_multiset(c) for c in p.candidates] != [_multiset(ops)]):
        out.append("H1 the pending is not «continuar» over the expired anchor")
    if p.reason == "TERRITORY_VS_CARRIED_BUYER":  # H6 (F30): change the buyer / search with both
        geo = [op for op in ops if op.op is fa.PatchOpKind.SET and op.slot.key == "GEOGRAPHY/POSITIVE"]
        change = [op.op.value + op.slot.key for c in p.candidates for op in c if op.op is fa.PatchOpKind.CLEAR]
        if not conflict or len(p.candidates) != 2 or sorted(len(c) for c in p.candidates) != [1, 2] or (
                change != ["CLEARBUYER/POSITIVE"]) or any(c[-1] != geo[0] and c[0] != geo[0] for c in p.candidates):
            out.append("H6 the pending does not carry its two candidates")
    if p.reason == "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER":  # R3-1 without E-ORDER: C1 only, slots ⊇ ORDER, CARD.
        if not {"ORDER", "CARDINALITY"} <= {s.key for s in p.answer_slots}:
            out.append("R3-1 answer_slots lack ORDER/CARDINALITY")
        if all(op.op is fa.PatchOpKind.CLEAR for op in p.open_ops) and [
                sorted((op.op.value, op.slot.key) for op in c) for c in p.candidates] != [
                [("CLEAR", "CARDINALITY"), ("CLEAR", "ORDER")]]:
            out.append("R3-1 the pending of a CLEAR(ORDER) is not C1")
    if p.reason.startswith("UNRESOLVED_VALUE:") and p.reason.partition(":")[2] not in {
            fd.dimension_key(s) for s in p.answer_slots}:
        out.append(f"{p.reason} names a dimension its answer_slots do not hold")
    return out


def _needs_policy(c, subjects) -> bool:
    """The inventory of §6.6 (M2A3_PHASE1_CLOSE.md §1), read from the typed value: a MODALITY family and a negated
    Terminado over contracts are projected only with a request policy M2a has no owner for."""
    value = decode_value(c.value)
    if c.dimension is D.MODALITY:
        return c.operator == "family" or value.family_policy is not None
    return (c.dimension is D.STATE and value.excluded and value.canonical_class == "terminated"
            and Subject.CONTRACTS in subjects)


def policy_owner_problems(base, patch, outcome) -> list[str]:
    """§6.6: what needs a policy without owner never executes, placed or carried into a new universe."""
    if outcome.disposition.value != "EXECUTE":
        return []
    effective, before = outcome.effective, {c.condition_id for c in base.conditions}
    subjects = {r.subject for r in effective.requests}
    moved = not {(r.subject, r.universe_kind) for r in effective.requests} <= {  # a dual anchor reduced keeps its own
        (r.subject, r.universe_kind) for r in base.requests}
    for c in effective.conditions:
        if _needs_policy(c, subjects) and (c.condition_id not in before or moved):
            return [f"§6.6 {c.dimension.value} that needs a host policy executed without an owner"]
    if moved and any(p in effective.policies for p in base.policies):
        return ["§6.6 a policy crossed a change of universe"]
    return []


def cause_count_problems(causes) -> list[str]:
    """§6 step 3b: «una operación no direccionable o no acreditada» is ONE cause, whatever its reasons."""
    seen = set()
    for c in causes:
        if c.refusal.value in ("TARGET_NOT_ADDRESSABLE", "OP_NOT_ACCREDITED"):
            key = (c.refusal.value, repr(c.ops[0]))
            if key in seen:
                return [f"3b one operation counted as several {c.refusal.value} causes"]
            seen.add(key)
    return []


def _from_base(base, ops):
    if not ops:
        return fd.Composed(base, frozenset(), frozenset(), False)
    return fd.compose(base, fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT), base_trace_id="t0")


def outcome_frame_problem(base, outcome) -> str | None:
    """§8.4 condition 1, read FROM THE BASE: the frame of a CLARIFY composes with no cause, holds no open question and
    raises no §19 COMPOSICION dependency (that dependency decides the turn: §19, H1)."""
    frame_ops = outcome.pending.frame_ops
    composed = _from_base(base, frame_ops)
    if not isinstance(composed, fd.Composed):
        return "8.4 the frame of a CLARIFY has a refusal cause"
    findings = fo.host_findings(base, frame_ops, composed)
    if any(f.kind is fo.FindingKind.DEPENDENCY for f in findings):
        return "8.4 the frame of a CLARIFY raises a §19 dependency"
    if any(f.kind is fo.FindingKind.QUESTION and f.rule != "H11" for f in findings):
        return "8.4 the frame of a CLARIFY holds an open question"
    return None


def laundering_problems(base, outcome) -> list[str]:
    """§19 COMPOSICION is fail-closed: a candidate judged on the frame must see every dependency the same answer raises
    when the frame and the candidate are composed from the base (when that joint patch composes)."""
    if outcome.disposition.value != "CLARIFY":
        return []
    p, out = outcome.pending, []
    for candidate in p.candidates:
        joint = _from_base(base, (*p.frame_ops, *candidate))
        made = fd.compose_ops(p.frame, candidate, base_trace_id="t0")
        if not isinstance(joint, fd.Composed) or not isinstance(made, fd.Composed):
            continue
        seen = {f.dependency for f in fo.host_findings(p.frame, candidate, made) if f.dependency is not None}
        whole = {f.dependency for f in fo.host_findings(base, (*p.frame_ops, *candidate), joint)
                 if f.dependency is not None}
        if whole - seen:
            out.append(f"§19 a candidate launders {sorted(d.dep_id for d in whole - seen)} through the frame")
    return out


def violations(delta, outcome_module, case) -> list[str]:
    """Every property for one case; an exception anywhere is itself a violation (P5)."""
    try:
        return _violations(delta, outcome_module, case)
    except Exception as error:  # noqa: BLE001
        return [f"P5 exception {type(error).__name__}: {error}"]


def _violations(delta, outcome_module, case) -> list[str]:
    base, patch, expired = case
    out = transition_violations(delta, outcome_module, base, patch, expired)
    if out and out[0].startswith("P5 exception"):
        return out
    out += target_problems(delta, base, patch)
    outcome = outcome_module.delta_outcome(base, patch, base_trace_id="t0", anchor_expired=expired)
    out += spec_problems(delta, base, patch, expired, outcome)
    out += policy_problems(base, outcome)
    if outcome.disposition.value == "EXECUTE":
        if {e.value for e in outcome.effects} != expected_effects(base, patch, outcome.effective):
            out.append(f"P8 effects {sorted(e.value for e in outcome.effects)}")
        if fa.AcceptedInterpretation.from_dict(json.loads(json.dumps(outcome.effective.to_dict()))) != (
                outcome.effective):
            out.append("P4 the effective is not canonical")
    reversed_patch = fa.SemanticPatch(patch.ops[::-1], patch.origin_kind)
    again = outcome_module.delta_outcome(base, reversed_patch, base_trace_id="t0", anchor_expired=expired)
    if (again.disposition, again.code, again.effects) != (outcome.disposition, outcome.code, outcome.effects) or (
            (again.effective is None) != (outcome.effective is None)) or (
            outcome.effective is not None and fa.interpretation_hash(again.effective)
            != fa.interpretation_hash(outcome.effective)):
        out.append("P2 the order of the ops changes the outcome")
    return out


@PROPS
@given(st_case())
def test_property_rich_bases_satisfy_the_oracle_and_the_universal_property(case):
    assert violations(fd, fo, case) == []


@PROPS
@given(st_case(rich=False))
def test_property_plain_bases_too(case):
    assert violations(fd, fo, case) == []


@PROPS
@given(st_case())
def test_property_determinism(case):
    base, patch, _ = case
    first = fd.compose(base, patch, base_trace_id="t0")
    second = fd.compose(base, patch, base_trace_id="t0")
    if isinstance(first, fd.Composed):
        assert fa.interpretation_hash(first.effective) == fa.interpretation_hash(second.effective)
    else:
        assert first == second


# ---- reducer mutants: each one must be killed by the properties ----------------------------------------------------
DELTA_SOURCE = (ROOT / "app/core/foundation_delta.py").read_text("utf-8")
OUTCOME_SOURCE = (ROOT / "app/core/foundation_delta_outcome.py").read_text("utf-8")


def _module(name: str, source: str) -> types.ModuleType:
    module = types.ModuleType(name)
    module.__file__ = f"<{name}>"
    sys.modules[name] = module
    exec(compile(source, f"<{name}>", "exec"), module.__dict__)
    return module


def mutant(delta_changes=(), outcome_changes=()) -> tuple[types.ModuleType, types.ModuleType]:
    source = DELTA_SOURCE
    for old, new in delta_changes:
        assert source.count(old) == 1, f"mutation anchor not unique: {old!r}"
        source = source.replace(old, new)
    delta = _module("m2a3_delta_mutant", source)
    source = OUTCOME_SOURCE.replace("from app.core.foundation_delta import", "from m2a3_delta_mutant import")
    for old, new in outcome_changes:
        assert source.count(old) == 1, f"mutation anchor not unique: {old!r}"
        source = source.replace(old, new)
    return delta, _module("m2a3_outcome_mutant", source)


REDUCER_MUTANTS = {
    # the Gold gaps the oracle manifest names
    "f16_any_value": ([(
        "            if len(same) == 1 and canonical_value(same[0]) == canonical_value(op.condition):",
        "            if len(same) == 1:")], []),
    "set_clears_whole_dimension": ([(
        "    carried = [c for c in base.conditions if c.slot.key not in writers and c.slot.key not in clears]",
        "    carried = [c for c in base.conditions if c.slot.key not in writers and c.slot.key not in clears\n"
        "               and c.dimension not in {op.slot.dimension for op in ops if op.op in PLACERS}]")], []),
    "clear_dimension_only_positive": ([(
        "        return tuple(Slot(slot.dimension, polarity) for polarity in (Polarity.POSITIVE, Polarity.NEGATIVE))",
        "        return (Slot(slot.dimension, Polarity.POSITIVE),)")], []),
    "redirect_keeps_old_kind": ([(
        "    request = replace(touched, subject=target.subject, universe_kind=target.universe_kind,",
        "    request = replace(touched, subject=target.subject, universe_kind=touched.universe_kind,")], []),
    "h6_overfire": ([], [(
        "            if any(scope != geo_divipola(op.condition) for scope in scopes):",
        "            if any(c.slot == BUYER_POSITIVE for c in effective.conditions):")]),
    # one per rule
    "precedence_reversed": ([(
        "    return tuple(sorted(found, key=lambda c: c.order))",
        "    return tuple(sorted(found, key=lambda c: c.order, "
                                                                 "reverse=True))")], []),
    "keep_with_same_value_set_is_no_conflict": ([(
        "        if len(w) >= 2 or (k and (w or c)):",
        "        if len(w) >= 2 or (k and c):")], []),
    "keep_on_empty_slot_ignored": ([(
        "        if op.op is PatchOpKind.KEEP and op.slot.key not in present:",
        "        if op.op is PatchOpKind.KEEP and op.slot.key not in present and False:")], []),
    "cardinality_without_order_allowed": ([(
        "    if not any(CARDINALITY_KEY in slots and ORDER_KEY not in slots for slots in after.values()):",
        "    if True:")], []),
    "no_a81": ([(
        "        if kept and _comparable(op.condition, kept[0]) and _a81_collides(op.condition, kept[0]):  # A8.1",
        "        if False:  # A8.1")], []),
    "a81_ignores_units": ([(
        "            if opposite is not None and carried(opposite) and not _comparable(op.condition, "
        "standing[opposite][0]):",
        "            if False:")], []),
    "p3_ignored": ([(
        "                if same_patch or several:", "                if several:")], []),
    "f16_several_inverted": ([(
        "                several = carried(positive) and len(standing[positive]) > 1 and any(",
        "                several = False and any(")], []),
    "dual_request_ids_not_rewritten": ([(
        "    if len(requests) != len(base.requests):  # R3, P-1: the only change a carried condition admits",
        "    if False:  # R3, P-1: the only change a carried condition admits")], []),
    "stale_universe_origin": ([(
        "                      universe_origin=redirect.origin, dataset=dataset)",
        "                      universe_origin=touched.universe_origin, dataset=dataset)")], []),
    "stale_dataset": ([(
        "    dataset = touched.dataset if touched.subject is target.subject else SUBJECT_DATASET[target.subject]",
        "    dataset = touched.dataset")], []),
    "carried_reprovenanced": ([(
        "    carried = [c for c in carried if c.condition_id not in removed]",
        "    carried = [replace(c, provenance=type(c.provenance)()) if c.dimension is SemanticDimension.BUYER else c\n"
        "               for c in carried if c.condition_id not in removed]")], []),
    "policies_dropped": ([(
        "    policies = tuple(p for p in base.policies if set(p.condition_ids) <= kept_ids)",
        "    policies = ()")], []),
    "target_not_addressable_ignored": ([(
        '            hits += [(s, "§4.2") for s in covered(op.slot) if s.key in grouped]', "            pass")], []),
    "topic_replaced_not_reported": ([(
        "            effects.add(DeltaEffect.TOPIC_REPLACED)", "            pass")], []),
    "opportunities_accepted": ([(
        "            if op.universe.universe_kind is UniverseKind.OPPORTUNITIES:", "            if False:")], []),
    "placed_ids_follow_op_order": ([(
        "    for op in sets:  # ids are the reducer's: a patch id may collide with a base one",
        "    for op in [o for o in ops if o.op in PLACERS]:  # ids are the reducer's: a patch id may collide with a "
        "base one")], []),
    "placed_id_reuses_a_departed_base_id": ([(
        "    taken = {c.condition_id for c in base.conditions}  # never reuse an id of the base, even of one that left",
        "    taken = {c.condition_id for c in carried}")], []),
    "law_bypassed": ([], [(
        "    if law_violations(base, frame_ops, candidates, answer_slots, trace, origin, answered):\n"
        "        return DeltaOutcome(DeltaDisposition.INFORM, code)",
        "    if False:\n        return DeltaOutcome(DeltaDisposition.INFORM, code)")]),
    "two_causes_clarify": ([], [(
        "        if len(causes) > 1 or anchor_expired:", "        if anchor_expired:")]),
    "h12_executes": ([], [(
        "    if len(ops) == 1 and ops[0].op is PatchOpKind.CLEAR and not covered_keys(ops) & present:  # H12 (F18)",
        "    if False:  # H12 (F18)")]),
    "h3_ignored": ([], [(
        "    if status != \"ok\":  # H3: what opens it is the turn's TIME or VALUE operation",
        "    if False:  # H3: what opens it is the turn's TIME or VALUE operation")]),
    # the ZEUS repair (NO_GO on 15e72829): also killed by these properties, not only by the direct tests
    "unknown_unit_comparable": ([(
        "    return first is not None and first == unit_class(b)",
        "    second = unit_class(b)\n    return first is None or second is None or first == second")], []),
    "placed_policy_need_ignored": ([(
        "            if needs_host_policy(op.condition, subjects_after):", "            if False:")], []),
    "one_op_several_opna_causes": ([(
        "        if reasons:\n            slot, extra, clause = reasons[0]\n            out.append(",
        "        for slot, extra, clause in reasons:\n            out.append(")], []),
    "one_op_several_target_causes": ([(
        "        if hits:\n            slot, clause = min(hits, key=lambda hit: SLOT_RANK[hit[0].key])\n            found.append(",
        "        for slot, clause in hits:\n            found.append(")], []),
    "frame_dependency_laundered": ([], [(  # ZEUS on the integration 8066e03d, F1
        "                             if f.kind is FindingKind.DEPENDENCY or (f.kind is FindingKind.QUESTION and f.rule != \"H11\")),",
        "                             if (f.kind is FindingKind.QUESTION and f.rule != \"H11\")),")]),
    "h1_without_continuar": ([], [(
        "HOST_CODES[\"H1\"], base, (), tuple(ops), (tuple(ops),), _slots(ops)))",
        "HOST_CODES[\"H1\"], base, (), tuple(ops), (), _slots(ops)))")]),
}


@pytest.mark.parametrize("name", sorted(REDUCER_MUTANTS))
def test_every_reducer_mutant_is_killed(name):
    delta_changes, outcome_changes = REDUCER_MUTANTS[name]
    try:
        delta, outcome = mutant(delta_changes, outcome_changes)
        found = find(st.one_of(st_case(), st_case(rich=False)), lambda case: bool(violations(delta, outcome, case)),
                     settings=settings(max_examples=3000, database=None, derandomize=True,
                                       suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large]))
        assert violations(delta, outcome, found)
    finally:
        sys.modules.pop("m2a3_delta_mutant", None)
        sys.modules.pop("m2a3_outcome_mutant", None)
