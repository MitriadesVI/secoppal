"""M2A-1a TYPES/DIRECTIVES: closed persisted types of the M2a contract (``app/core/foundation_accepted.py``).

Invariants proved here (mission M2A-1a; contract §3, §5, §8, §9, §12, §17):

1. ``from_dict(to_dict(x)) == x`` (and through JSON text) for every persistible type — Hypothesis property.
2. Unknown or missing keys and unknown schema versions fail closed, at every nesting level.
3. ``interpretation_hash`` is deterministic and is the canonical ``_digest`` of the effective.
7. ``CandidatePatch`` is generic: one option type, any set of slots.
8. ``CANCEL_PENDING`` is explicit: an empty patch is not a value, and the control survives the round trip.

Each guarantee is checked against MUTANTS of the module (lax reader, ignored version, collapsed CANCEL_PENDING):
the mutated source is executed in a throwaway module and the same battery must report it. A check that does not
kill its mutant does not count. Boundaries (4, 5, 6) live in ``tests/test_m2a_boundaries.py``.
"""
from __future__ import annotations

import json
import sys
import types
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
from app.core.foundation_authority_values import (
    ActorValue, BuyerKind, BuyerValue, CardinalityBasis, CardinalityValue, Comparator, ContractTypeValue, GeoValue,
    ModalityValue, MoneyValue, OrderDirection, OrderKey, OrderValue, StateValue, TimeValue, TopicValue,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import (
    RESERVED_UNSUPPORTED_LABELS, ConditionExpression, ContractError, DirectiveKind, Effect, HostPolicy,
    LogicOperator, Polarity, ProposedSlotDirective, RawSpan, ValueSide,
)
from app.core.semantic_dimensions import SemanticDimension as D

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "app" / "core" / "foundation_accepted.py").read_text(encoding="utf-8")
RAW = "a" * 64
PLAN = "b" * 64
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
PROPS = settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])


# ---- sample builders, parameterized by the module so mutants build their own instances ----------------------------
VALUES = {
    D.TOPIC: [TopicValue("obras", True, False), TopicValue("vias", False, True)],
    D.BUYER: [BuyerValue("ICBF", BuyerKind.UNIT, ("ICBF",), None, False),
              BuyerValue("INVIAS", BuyerKind.UNIT, ("INVIAS",), None, True)],
    D.ACTOR: [ActorValue("900123456", "NIT", "Acme", False), ActorValue("123", "NIT", "Beta", True)],
    D.GEOGRAPHY: [GeoValue(GeoLevel.DEPARTMENT, "VALLE", None, "76", (), False),
                  GeoValue(GeoLevel.MUNICIPALITY, "CALI", "VALLE DEL CAUCA", "76001", (), True)],
    D.TIME: [TimeValue(date(2025, 1, 1), date(2026, 1, 1), "year", False),
             TimeValue(date(2024, 3, 1), date(2024, 4, 1), "month", True)],
    D.VALUE: [MoneyValue(500_000_000, Comparator.GTE), MoneyValue(100, Comparator.GT),
              MoneyValue(900_000_000, Comparator.LTE), MoneyValue(7, Comparator.LT)],
    D.STATE: [StateValue("open", (), False), StateValue("closed", (), True)],
    D.MODALITY: [ModalityValue(("minima_cuantia",), None, False)],
    D.CONTRACT_TYPE_OR_NATURE: [ContractTypeValue(("obra",), False)],
    D.ORDER: [OrderValue(OrderKey.VALUE, OrderDirection.DESC), OrderValue(OrderKey.DATE, OrderDirection.ASC)],
    D.CARDINALITY: [CardinalityValue(10, CardinalityBasis.FIGURE, 10),
                    CardinalityValue(100, CardinalityBasis.FIGURE, 250)],
}
OPERATOR = {D.TOPIC: "contains", D.BUYER: "is", D.ACTOR: "is", D.GEOGRAPHY: "is", D.TIME: "during_year",
            D.STATE: "is", D.MODALITY: "is", D.CONTRACT_TYPE_OR_NATURE: "is", D.ORDER: "value_desc",
            D.CARDINALITY: "top_n"}
VALUE_OPERATOR = {Comparator.GT: "greater_than", Comparator.GTE: "greater_or_equal",
                  Comparator.LT: "less_than", Comparator.LTE: "less_or_equal"}


def origin(m=fa, trace="t1", start=0):
    return m.ConditionOrigin(m.OriginKind.SPAN, trace, RAW, (start, start + 3))


def event_origin(m=fa, kind=None, event="ev-1"):
    return m.ConditionOrigin(kind or m.OriginKind.OPTION_EVENT, "t1", event_id=event)


def provenance_for(m, dimension, value):
    if dimension is D.VALUE:
        return m.Provenance(unit=m.Unit.MILLONES, source_amount="500", unit_source=m.UnitSource.STATED)
    if dimension is D.TIME:
        return m.Provenance(year=2025, year_source=m.YearSource.STATED)
    if dimension is D.ORDER:
        return m.Provenance(order_direction_source=m.OrderDirectionSource.DEFAULTED)
    if dimension is D.BUYER:
        return m.Provenance(buyer_scope="NATIONAL")
    return m.Provenance()


def condition(m=fa, dimension=D.TOPIC, index=0, cid="c1", request_ids=("r1",), org=None):
    value = VALUES[dimension][index]
    if dimension is D.VALUE:
        operator, polarity = VALUE_OPERATOR[value.comparator], Polarity.POSITIVE
    else:
        operator = OPERATOR[dimension]
        polarity = Polarity.NEGATIVE if getattr(value, "excluded", False) else Polarity.POSITIVE
    encoded = value.encode()
    rules = ("topn.capped",) if dimension is D.CARDINALITY and value.capped else ()
    return m.AcceptedCondition(
        cid, tuple(request_ids), m.slot_of(dimension, encoded), dimension, operator, polarity,
        value.level if dimension is D.GEOGRAPHY else None, encoded, f"{dimension.value.lower()}_authority@1",
        rules, (), org or origin(m), provenance_for(m, dimension, value))


def request(m=fa, rid="r1", subject=Subject.PROCESSES, scope="s1"):
    return m.AcceptedRequest(rid, scope, subject, m.UniverseKind.RECORDS, origin(m), None, f"ds-{subject.value}")


def interpretation(m=fa, conditions=None, dual=False, base=None):
    requests = (request(m),) if not dual else (request(m), request(m, "r2", Subject.CONTRACTS))
    rids = tuple(r.request_id for r in requests)
    conds = conditions if conditions is not None else (
        condition(m, D.TOPIC, 0, "c1", rids), condition(m, D.VALUE, 0, "c2", rids),
        condition(m, D.ORDER, 0, "c3", rids), condition(m, D.CARDINALITY, 1, "c4", rids))
    policies = (HostPolicy("topn", "1", ("c4",), Effect.NARROWING),) if any(
        c.condition_id == "c4" for c in conds) else ()
    return m.AcceptedInterpretation(requests, tuple(conds), None, policies, base, "root-1")


def patch(m=fa):
    return m.SemanticPatch((
        m.PatchOp(m.PatchOpKind.CLEAR, m.Slot(D.TIME), None, None, origin(m, start=4)),
        m.PatchOp(m.PatchOpKind.SET, m.Slot(D.VALUE, side=ValueSide.LOWER), condition(m, D.VALUE, 1, "c9"),
                  None, origin(m)),
        m.PatchOp(m.PatchOpKind.KEEP, m.Slot(D.BUYER, Polarity.POSITIVE), None, None, origin(m, start=8)),
    ), m.PatchOriginKind.TEXT)


def pending(m=fa, candidates=None):
    geo = condition(m, D.GEOGRAPHY, 0, "g1")
    time = condition(m, D.TIME, 0, "t1")
    buyer = condition(m, D.BUYER, 0, "b1")
    if candidates is None:
        candidates = (
            m.CandidatePatch("k1", "as_buyer", m.SemanticPatch((m.PatchOp(
                m.PatchOpKind.SET, buyer.slot, buyer, None, buyer.origin),), m.PatchOriginKind.TEXT)),
            # one option touching two slots (GEOGRAPHY + TIME): still the one generic type
            m.CandidatePatch("k2", "as_place_and_year", m.SemanticPatch((
                m.PatchOp(m.PatchOpKind.SET, geo.slot, geo, None, geo.origin),
                m.PatchOp(m.PatchOpKind.SET, time.slot, time, None, time.origin)), m.PatchOriginKind.TEXT)),
        )
    slots = frozenset(slot for c in candidates for slot in c.patch.slots)
    return m.PendingClarification("p1", "t1", T0, T0 + timedelta(minutes=30), 1, interpretation(m), "t0", (0, 3),
                                  RAW, "BUYER_OR_PLACE_AFTER_ACTOR", tuple(candidates), slots)


def result_set(m=fa, effective=None):
    effective = effective or interpretation(m)
    return m.ResultSetRef("t1", m.interpretation_hash(effective), (m.GroupRef(
        "r1", "ds-processes", PLAN, "value_desc", 10, ("id-1", "id-2"), 10, 42, m.TotalQuality.EXACT),))


def record(m=fa, resolution=None, control=None, with_patch=True, with_pending=False, scope=None):
    resolution = resolution or m.TurnResolution.DELTA
    effective = interpretation(m, base="t0")
    silent = resolution in (m.TurnResolution.CLARIFY, m.TurnResolution.REASK, m.TurnResolution.INFORM,
                            m.TurnResolution.HANDOFF_LEGACY)
    if resolution is m.TurnResolution.NEW_SEARCH:
        effective = interpretation(m)
    executes = not silent
    return m.AcceptedTurnRecord(
        "t1", "chat-1", "turn-1", T0, T0 - timedelta(seconds=1), "foundation_v5_1", RAW,
        scope or (m.TurnScope.IN_NEW_SEARCH if resolution is m.TurnResolution.NEW_SEARCH else m.TurnScope.IN),
        resolution, control, None if resolution is m.TurnResolution.NEW_SEARCH else "t0",
        patch(m) if with_patch else None, None if silent else effective,
        m.ExecutionReceipt(True, True, True, (PLAN,)) if executes else None,
        result_set(m, effective) if executes else None,
        pending(m) if with_pending else None,
        (m.EchoItem(m.EchoClass.CARRIED, m.Slot(D.TOPIC, Polarity.POSITIVE), ("c1",), "carried_topic"),
         m.EchoItem(m.EchoClass.CONTEXT_RESET, None, (), "context_reset")),
        "t1" if executes else "t0")


def samples(m=fa):
    """One instance of every persistible type (plus the variants the mutants target)."""
    eff = interpretation(m)
    return [
        record(m), record(m, m.TurnResolution.NEW_SEARCH, m.PendingControl.CANCEL_PENDING, with_patch=False),
        record(m, m.TurnResolution.NEW_SEARCH, None, with_patch=False),
        record(m, m.TurnResolution.CLARIFY, None, with_patch=False, with_pending=True),
        eff, interpretation(m, dual=True), eff.requests[0], eff.conditions[0], origin(m), event_origin(m),
        eff.conditions[1].provenance, patch(m), patch(m).ops[0], m.UniverseTarget(Subject.CONTRACTS,
                                                                                    m.UniverseKind.OPPORTUNITIES),
        pending(m), pending(m).candidate_patches[1], result_set(m), result_set(m).groups[0],
        m.EchoItem(m.EchoClass.TOPN_CAPPED, m.Slot(D.CARDINALITY), ("c4",), "topn"),
        m.ExecutionReceipt(True, False, False, ()), m.Slot(D.VALUE, side=ValueSide.UPPER), m.UNIVERSE_SLOT,
    ]


# ---- the reader battery (also run against every mutant) ----------------------------------------------------------
def _dict_nodes(node, path=()):
    if isinstance(node, dict):
        yield path, node
        for key, child in node.items():
            yield from _dict_nodes(child, (*path, key))
    elif isinstance(node, list):
        for index, child in enumerate(node):
            yield from _dict_nodes(child, (*path, index))


def _at(root, path):
    for step in path:
        root = root[step]
    return root


def _raises(reader, payload) -> bool:
    try:
        reader(payload)
    except ContractError:
        return True
    return False


def reader_failures(m) -> list[str]:
    """Every failure of the strict-reader contract the module shows on the fixed samples."""
    out = []
    for item in samples(m):
        kind = type(item)
        payload = item.to_dict()
        try:
            again = kind.from_dict(json.loads(json.dumps(payload)))
        except ContractError as error:
            out.append(f"{kind.__name__}: round trip raised {error}")
            continue
        if again != item or again.to_dict() != payload:
            out.append(f"{kind.__name__}: round trip changed the value")
        for path, node in _dict_nodes(payload):
            for mutate, label in ((lambda n: n.__setitem__("zz_unknown", None), "unknown key"),
                                  (lambda n: n.pop(next(iter(n))), "missing key")):
                tampered = json.loads(json.dumps(payload))
                mutate(_at(tampered, path))
                if not _raises(kind.from_dict, tampered):
                    out.append(f"{kind.__name__}: {label} at {path} accepted")
            if "schema" in node:
                tampered = json.loads(json.dumps(payload))
                current = _at(tampered, path)["schema"]
                _at(tampered, path)["schema"] = current.rsplit("/", 1)[0] + "/2"
                if not _raises(kind.from_dict, tampered):
                    out.append(f"{kind.__name__}: unknown schema {current}->/2 at {path} accepted")
    return out


def cancel_failures(m) -> list[str]:
    """CANCEL_PENDING is explicit: never an empty patch, never an operation, never lost or inferred."""
    out = []
    try:
        m.SemanticPatch((), m.PatchOriginKind.TEXT)
        out.append("an empty patch is a value")
    except ContractError:
        pass
    if "CANCEL_PENDING" in {kind.value for kind in m.PatchOpKind}:
        out.append("CANCEL_PENDING is a semantic operation")
    for control in (None, m.PendingControl.CANCEL_PENDING):
        item = record(m, m.TurnResolution.NEW_SEARCH, control, with_patch=False)
        try:
            again = type(item).from_dict(item.to_dict())
        except ContractError as error:
            out.append(f"control={control}: round trip raised {error}")
            continue
        if again.control is not control:
            out.append(f"control={control} read back as {again.control}")
    return out


def test_the_real_module_passes_the_reader_battery():
    assert reader_failures(fa) == []
    assert cancel_failures(fa) == []


def _mutant(old: str, new: str) -> types.ModuleType:
    assert SOURCE.count(old) == 1, f"mutation anchor not unique: {old!r}"
    name = "m2a_accepted_mutant"
    module = types.ModuleType(name)
    module.__file__ = "<mutant>"
    sys.modules[name] = module
    try:
        exec(compile(SOURCE.replace(old, new), "<mutant>", "exec"), module.__dict__)
    finally:
        sys.modules.pop(name, None)
    return module


MUTANTS = {
    "lax_reader_ignores_unknown_keys": (
        "    if set(data) != set(names):\n        _fail(f\"{label}_KEYS_MISMATCH\")",
        "    if not set(names) <= set(data):\n        _fail(f\"{label}_KEYS_MISMATCH\")", reader_failures),
    "lax_reader_defaults_missing_keys": (
        "    if set(data) != set(names):\n        _fail(f\"{label}_KEYS_MISMATCH\")",
        "    if not set(data) <= set(names):\n        _fail(f\"{label}_KEYS_MISMATCH\")\n"
        "    data = {name: data.get(name) for name in names}", reader_failures),
    "ignored_schema_version": (
        '    if data.get("schema") != schema:', '    if "schema" not in data:', reader_failures),
    "cancel_pending_as_empty_patch": (
        '_records(self.ops, PatchOp, "PATCH_OPS", empty=False)', '_records(self.ops, PatchOp, "PATCH_OPS")',
        cancel_failures),
    "cancel_pending_inferred_from_missing_patch": (
        '_opt(data["control"], lambda v: _enum_of(PendingControl, v, "RECORD_CONTROL"))',
        '(PendingControl.CANCEL_PENDING if data["patch"] is None else None)', cancel_failures),
    "cancel_pending_dropped_on_write": (
        '"control": None if self.control is None else self.control.value,', '"control": None,',
        cancel_failures),
}


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_mutant_is_killed(name):
    old, new, battery = MUTANTS[name]
    assert battery(_mutant(old, new)), f"mutant {name} survived"


# ---- Hypothesis: round trip over generated instances --------------------------------------------------------------
DIMS = sorted(VALUES, key=lambda d: d.value)


@st.composite
def st_origin(draw):
    kind = draw(st.sampled_from(list(fa.OriginKind)))
    trace = draw(st.text(min_size=1, max_size=8).filter(str.strip))
    if kind is fa.OriginKind.SPAN:
        start = draw(st.integers(0, 500))
        return fa.ConditionOrigin(kind, trace, draw(st.sampled_from([RAW, PLAN])),
                                  (start, start + draw(st.integers(1, 40))))
    if kind is fa.OriginKind.HOST_POLICY:
        return fa.ConditionOrigin(kind, trace)
    return fa.ConditionOrigin(kind, trace, event_id=draw(st.text(min_size=1, max_size=8).filter(str.strip)))


@st.composite
def st_condition(draw, dimension=None, cid=None, request_ids=("r1",)):
    dimension = dimension or draw(st.sampled_from(DIMS))
    index = draw(st.integers(0, len(VALUES[dimension]) - 1))
    base = condition(fa, dimension, index, cid or draw(st.sampled_from(["c1", "c2", "x"])), request_ids)
    org = draw(st_origin())
    return fa.AcceptedCondition(base.condition_id, base.request_ids, base.slot, base.dimension, base.operator,
                                base.polarity, base.level, base.value, base.authority, base.receipt_rules,
                                tuple(sorted(draw(st.sets(st.sampled_from(["actor_state@1", "topn@1"]))))),
                                org, base.provenance)


@st.composite
def st_interpretation(draw):
    dual = draw(st.booleans())
    rids = ("r1", "r2") if dual else ("r1",)
    dims = draw(st.lists(st.sampled_from([d for d in DIMS if d not in (D.ORDER, D.CARDINALITY, D.VALUE)]),
                         max_size=4))
    conds = [draw(st_condition(d, f"c{i}", rids)) for i, d in enumerate(dims)]
    if draw(st.booleans()):
        conds.append(draw(st_condition(D.ORDER, "o", rids)))
        if draw(st.booleans()):
            conds.append(draw(st_condition(D.CARDINALITY, "n", rids)))
    for side_index in draw(st.sets(st.sampled_from([0, 2]))):  # at most one bound per side
        conds.append(condition(fa, D.VALUE, side_index, f"v{side_index}", rids))
    logic = None
    if len(conds) >= 2 and not any(c.dimension is D.ORDER for c in conds) and draw(st.booleans()):
        atoms = tuple(ConditionExpression(LogicOperator.ATOM, c.condition_id) for c in conds)
        logic = ConditionExpression(LogicOperator.AND, None, (ConditionExpression(
            LogicOperator.OR, None, atoms[:2]), *atoms[2:])) if len(atoms) > 2 else ConditionExpression(
            LogicOperator.OR, None, atoms)
    requests = (request(),) if not dual else (request(), request(fa, "r2", Subject.CONTRACTS))
    return fa.AcceptedInterpretation(requests, tuple(conds), logic, (), draw(st.none() | st.just("t0")),
                                     draw(st.text(min_size=1, max_size=6).filter(str.strip)))


@st.composite
def st_patch(draw):
    text = draw(st.booleans())
    origins = st_origin().filter(lambda o: (o.kind is fa.OriginKind.SPAN) == text
                                 and o.kind is not fa.OriginKind.HOST_POLICY)
    ops = []
    for _ in range(draw(st.integers(1, 3))):
        org = draw(origins)
        kind = draw(st.sampled_from(list(fa.PatchOpKind)))
        if kind is fa.PatchOpKind.SET:
            base = draw(st_condition(draw(st.sampled_from([d for d in DIMS if d is not D.ORDER]))))
            cond = condition(fa, base.dimension, 0, base.condition_id, org=org)
            ops.append(fa.PatchOp(kind, cond.slot, cond, None, org))
        elif kind is fa.PatchOpKind.SET_ORDER:
            cond = condition(fa, D.ORDER, draw(st.integers(0, 1)), "o", org=org)
            ops.append(fa.PatchOp(kind, cond.slot, cond, None, org))
        elif kind is fa.PatchOpKind.REPLACE_UNIVERSE:
            target = fa.UniverseTarget(draw(st.sampled_from([Subject.PROCESSES, Subject.CONTRACTS])),
                                       draw(st.sampled_from(list(fa.UniverseKind))))
            ops.append(fa.PatchOp(kind, fa.UNIVERSE_SLOT, None, target, org))
        else:
            dim = draw(st.sampled_from(DIMS))
            slot = condition(fa, dim).slot
            if kind is fa.PatchOpKind.CLEAR and dim not in (D.ORDER, D.CARDINALITY) and draw(st.booleans()):
                slot = fa.Slot(dim)  # dimension-wide
            ops.append(fa.PatchOp(kind, slot, None, None, org))
    return fa.SemanticPatch(tuple(ops), fa.PatchOriginKind.TEXT if text else fa.PatchOriginKind.EVENT)


def _round_trip(item) -> None:
    payload = item.to_dict()
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, allow_nan=False)
    again = type(item).from_dict(json.loads(text))
    assert again == item
    assert again.to_dict() == payload


@PROPS
@given(st_origin())
def test_property_origin_round_trip(item):
    _round_trip(item)


@PROPS
@given(st_condition())
def test_property_condition_round_trip(item):
    _round_trip(item)


@PROPS
@given(st_interpretation())
def test_property_interpretation_round_trip_and_hash(item):
    _round_trip(item)
    again = fa.AcceptedInterpretation.from_dict(json.loads(json.dumps(item.to_dict())))
    assert fa.interpretation_hash(again) == fa.interpretation_hash(item)


@PROPS
@given(st_patch())
def test_property_patch_round_trip(item):
    _round_trip(item)
    assert fa.patch_sha256(type(item).from_dict(item.to_dict())) == fa.patch_sha256(item)


@PROPS
@given(st.lists(st_patch(), min_size=1, max_size=3), st.integers(1, 2))
def test_property_pending_round_trip(patches, asks):
    candidates = tuple(fa.CandidatePatch(f"k{i}", f"opt{i}", p) for i, p in enumerate(patches))
    slots = frozenset(s for p in patches for s in p.slots)
    item = fa.PendingClarification("p1", "t1", T0, T0 + timedelta(minutes=30), asks, None, None, None, None,
                                   "PENDING_UNANSWERED", candidates, slots)
    _round_trip(item)


@PROPS
@given(st_interpretation(), st.sampled_from([None, fa.PendingControl.CANCEL_PENDING]), st.booleans())
def test_property_record_round_trip(effective, control, executes):
    item = fa.AcceptedTurnRecord(
        "t1", "chat-1", "turn-1", T0, T0, "foundation_v5_1", None, fa.TurnScope.IN_NEW_SEARCH,
        fa.TurnResolution.NEW_SEARCH, control, None, None, effective if effective.base_trace_id is None else None,
        None, None, None, (), "t1")
    if executes and item.effective is not None:
        item = fa.AcceptedTurnRecord(
            item.trace_id, item.chat_id, item.turn_id, T0, T0, item.profile, RAW, item.scope, item.resolution,
            control, None, None, item.effective, fa.ExecutionReceipt(True, True, True, (PLAN,)),
            result_set(fa, item.effective), None, (), "t1")
    _round_trip(item)
    assert fa.AcceptedTurnRecord.from_dict(item.to_dict()).control is control


def test_every_persisted_type_is_in_the_samples():
    assert {type(item) for item in samples()} == set(fa.PERSISTED_TYPES)


# ---- interpretation_hash / digests --------------------------------------------------------------------------------
def test_interpretation_hash_is_the_canonical_projection_digest():
    from app.core.foundation_projection import _digest

    effective = interpretation()
    assert fa.interpretation_hash(effective) == _digest(effective.to_dict()) == fa.digest(effective.to_dict())
    shuffled = dict(reversed(list(effective.to_dict().items())))
    assert fa.digest(shuffled) == fa.interpretation_hash(effective)


def test_interpretation_hash_is_deterministic_and_sensitive():
    first, second = interpretation(), interpretation()
    assert first is not second and fa.interpretation_hash(first) == fa.interpretation_hash(second)
    assert fa.interpretation_hash(interpretation(base="t0")) != fa.interpretation_hash(first)
    assert fa.interpretation_hash(interpretation(dual=True)) != fa.interpretation_hash(first)


def test_the_record_recomputes_its_digests_and_rejects_a_stale_one():
    payload = record().to_dict()
    assert payload["semantic_ref"] == fa.interpretation_hash(record().effective)
    for key, value in (("record_sha256", "0" * 64), ("semantic_ref", "0" * 64), ("turn_id", "turn-2")):
        tampered = dict(payload, **{key: value})
        with pytest.raises(ContractError):
            fa.AcceptedTurnRecord.from_dict(tampered)


# ---- closed shapes ------------------------------------------------------------------------------------------------
def test_candidate_patch_is_generic():
    item = pending()
    assert {type(c) for c in item.candidate_patches} == {fa.CandidatePatch}
    assert item.candidate_patches[1].patch.slots == {fa.Slot(D.GEOGRAPHY, Polarity.POSITIVE),
                                                    fa.Slot(D.TIME, Polarity.POSITIVE)}
    no_options = fa.PendingClarification("p", "t", T0, T0 + timedelta(minutes=1), 1, None, None, None, None,
                                         "MONTH_YEAR_AMBIGUOUS", (), frozenset({fa.Slot(D.TIME, Polarity.POSITIVE)}))
    _round_trip(no_options)
    # The module's classes are a closed, pinned set: no «BUYER or GEO» / «double slot» special type can appear.
    defined = {name for name, obj in vars(fa).items() if isinstance(obj, type) and obj.__module__ == fa.__name__}
    assert defined == {
        "TurnScope", "TurnResolution", "PendingControl", "PatchOpKind", "PatchOriginKind", "OriginKind",
        "UniverseKind", "Unit", "UnitSource", "YearSource", "OrderDirectionSource", "EchoClass", "TotalQuality",
        "Slot", "ConditionOrigin", "Provenance", "AcceptedRequest", "AcceptedCondition", "AcceptedInterpretation",
        "UniverseTarget", "PatchOp", "SemanticPatch", "CandidatePatch", "PendingClarification", "GroupRef",
        "ResultSetRef", "EchoItem", "ExecutionReceipt", "AcceptedTurnRecord",
    }


def test_answer_slots_cover_the_candidates():
    item = pending()
    with pytest.raises(ContractError, match="CANDIDATE_OUTSIDE_ANSWER_SLOTS"):
        fa.PendingClarification(item.pending_id, item.asked_trace_id, T0, T0 + timedelta(minutes=1), 1, None, None,
                                None, None, item.reason, item.candidate_patches,
                                frozenset({fa.Slot(D.BUYER, Polarity.POSITIVE)}))


@pytest.mark.parametrize("build, code", [
    (lambda: fa.AcceptedRequest("r", "s", Subject.UNSPECIFIED, fa.UniverseKind.RECORDS, origin(), None, "d"),
     "VIEW_REQUIRED"),
    (lambda: interpretation(conditions=(condition(fa, D.CARDINALITY, 0, "n"),)), "CARDINALITY_WITHOUT_ORDER"),
    (lambda: interpretation(conditions=(condition(fa, D.VALUE, 0, "a"), condition(fa, D.VALUE, 1, "b"))),
     "TWO_CONDITIONS_IN_VALUE_LOWER"),
    (lambda: interpretation(conditions=(condition(fa, D.ORDER, 0, "a"), condition(fa, D.ORDER, 1, "b"))),
     "TWO_CONDITIONS_IN_ORDER"),
    (lambda: fa.AcceptedCondition("c", ("r1",), fa.Slot(D.VALUE, side=ValueSide.UPPER), D.VALUE, "greater_than",
                                  Polarity.POSITIVE, None, VALUES[D.VALUE][0].encode(), "a", (), (), origin(),
                                  fa.Provenance()), "SLOT_NOT_DERIVED_FROM_VALUE"),
    (lambda: fa.AcceptedCondition("c", ("r1",), fa.Slot(D.TOPIC, Polarity.POSITIVE), D.TOPIC, "contains",
                                  Polarity.POSITIVE, None, VALUES[D.BUYER][0].encode(), "a", (), (), origin(),
                                  fa.Provenance()), "VALUE_TYPE_OUTSIDE_DIMENSION"),
    (lambda: fa.AcceptedCondition("c", ("r1",), fa.Slot(D.TOPIC, Polarity.POSITIVE), D.TOPIC, "contains",
                                  Polarity.POSITIVE, None, VALUES[D.TOPIC][0].encode(), "a", ("made.up",), (),
                                  origin(), fa.Provenance()), "UNKNOWN_RECEIPT_RULE"),
    (lambda: fa.Provenance(unit=fa.Unit.SMMLV, unit_source=fa.UnitSource.STATED), "SMMLV_WITHOUT_AMOUNT_OR_YEAR"),
    (lambda: fa.Provenance(unit=fa.Unit.PESOS, unit_source=fa.UnitSource.PREVIOUS_ACCEPTED_VALUE),
     "INHERITANCE_WITHOUT_ORIGIN"),
    (lambda: fa.PatchOp(fa.PatchOpKind.KEEP, fa.Slot(D.TOPIC), None, None, origin()),
     "DIMENSION_WIDE_SLOT_OUTSIDE_CLEAR"),
    (lambda: fa.PatchOp(fa.PatchOpKind.SET, fa.Slot(D.ORDER), condition(fa, D.ORDER), None, origin()),
     "ORDER_NEEDS_SET_ORDER"),
    (lambda: fa.PatchOp(fa.PatchOpKind.CLEAR, fa.UNIVERSE_SLOT, None, None, origin()),
     "UNIVERSE_NEEDS_REPLACE_UNIVERSE"),
    (lambda: fa.PatchOp(fa.PatchOpKind.SET, fa.Slot(D.TOPIC, Polarity.POSITIVE), condition(), None,
                        origin(start=9)), "PATCH_CONDITION_ORIGIN_MISMATCH"),
    (lambda: fa.SemanticPatch((fa.PatchOp(fa.PatchOpKind.CLEAR, fa.Slot(D.TIME), None, None, origin()),),
                              fa.PatchOriginKind.EVENT), "PATCH_ORIGIN_KIND_MISMATCH"),
    (lambda: fa.UniverseTarget(Subject.UNSPECIFIED, fa.UniverseKind.RECORDS), "UNIVERSE_TARGET_UNSPECIFIED"),
    (lambda: fa.ExecutionReceipt(True, False, True, ()), "INCOHERENT_EXECUTION"),
    (lambda: fa.Slot(D.UNSUPPORTED), "DIMENSION_NOT_A_SLOT"),
    (lambda: fa.Slot(D.RETRIEVAL_INTENT), "DIMENSION_NOT_A_SLOT"),
])
def test_closed_shapes_fail_closed(build, code):
    with pytest.raises(ContractError, match=code):
        build()


@pytest.mark.parametrize("change, code", [
    (dict(patch=None), "PATCH_RESOLUTION_MISMATCH"),
    (dict(base_trace_id=None), "COMPOSITION_WITHOUT_BASE"),
    (dict(scope=fa.TurnScope.OUT_OPERATION_CHANGE), "OUT_SCOPE_RESOLUTION_MISMATCH"),
    (dict(result_set_ref=None, execution=fa.ExecutionReceipt(True, True, False, (PLAN,)),
          resolution=fa.TurnResolution.CLARIFY, patch=None), "SILENT_TURN_CALLED_SOURCE"),
    (dict(execution=fa.ExecutionReceipt(True, True, False, (PLAN,))), "RESULTSET_WITHOUT_SOURCE"),
])
def test_record_coherence_fails_closed(change, code):
    base = record()
    values = {name: getattr(base, name) for name in fa.AcceptedTurnRecord.__dataclass_fields__}
    values.update(change)
    with pytest.raises(ContractError, match=code):
        fa.AcceptedTurnRecord(**values)


def test_out_scopes_make_no_call_and_round_trip():
    for scope, resolution in fa.OUT_SCOPE_RESOLUTION.items():
        item = record(fa, resolution, with_patch=False, scope=scope)
        assert item.execution is None and item.result_set_ref is None
        _round_trip(item)


def test_reason_codes_are_the_closed_annex_lists():
    assert fa.reason_code_allowed("UNRESOLVED_VALUE:VALUE", fa.TurnResolution.CLARIFY)
    assert not fa.reason_code_allowed("UNRESOLVED_VALUE:WHATEVER", fa.TurnResolution.CLARIFY)
    assert fa.reason_code_allowed("OPERATION_CHANGE:FOLLOW", fa.TurnResolution.INFORM)
    assert fa.reason_code_allowed("ANCHOR_NOT_SEARCH:COUNT", fa.TurnResolution.HANDOFF_LEGACY)
    for retired in ("SMMLV_UNSUPPORTED", "INHERITED_UNIT_IMPLAUSIBLE", "TOPN_CAP_EXCEEDED",
                    "DELTA_OR_NEW_AMBIGUOUS", "SELF_CONTAINED_REQUEST"):
        assert not any(fa.reason_code_allowed(retired, r) for r in fa.TurnResolution)
    assert {e.value for e in fa.TurnScope} == {
        "IN", "IN_NEW_SEARCH", "IN_PROTOCOL", "OUT_NO_ANCHOR", "OUT_ANCHOR_NOT_SEARCH", "OUT_OPERATION_CHANGE",
        "OUT_REFERENT_EXTENSIONAL", "OUT_RELATIVE_TO_PREVIOUS"}
    assert {e.value for e in fa.PatchOpKind} == {"SET", "CLEAR", "KEEP", "SET_ORDER", "REPLACE_UNIVERSE"}


def test_datetimes_are_canonical_utc():
    payload = record().to_dict()
    for value in ("2026-10-01T12:00:00", "2026-10-01T07:00:00-05:00", "2026-10-01 12:00:00+00:00"):
        with pytest.raises(ContractError):
            fa.AcceptedTurnRecord.from_dict(dict(payload, created_at_utc=value))


# ---- wire 5.1 directives (interpretation_contract) ----------------------------------------------------------------
SPAN = (RawSpan(0, 2, "no"),)


@pytest.mark.parametrize("directive", [
    ProposedSlotDirective("d", DirectiveKind.CANCEL_PENDING, SPAN),
    ProposedSlotDirective("d", DirectiveKind.CLEAR, SPAN, D.TIME),
    ProposedSlotDirective("d", DirectiveKind.CLEAR, SPAN, D.VALUE),
    ProposedSlotDirective("d", DirectiveKind.CLEAR, SPAN, D.VALUE, side=ValueSide.UPPER),
    ProposedSlotDirective("d", DirectiveKind.KEEP, SPAN, D.BUYER, Polarity.POSITIVE),
    ProposedSlotDirective("d", DirectiveKind.KEEP, SPAN, D.ORDER),
])
def test_directive_round_trip(directive):
    assert ProposedSlotDirective.from_dict(json.loads(json.dumps(directive.to_dict()))) == directive


@pytest.mark.parametrize("build, code", [
    (lambda: ProposedSlotDirective("d", DirectiveKind.CANCEL_PENDING, SPAN, D.TIME), "CANCEL_PENDING_HAS_SLOT"),
    (lambda: ProposedSlotDirective("d", DirectiveKind.CANCEL_PENDING, ()), "MISSING_DIRECTIVE_EVIDENCE"),
    (lambda: ProposedSlotDirective("d", DirectiveKind.KEEP, SPAN, D.TOPIC), "KEEP_NEEDS_ONE_SLOT"),
    (lambda: ProposedSlotDirective("d", DirectiveKind.KEEP, SPAN, D.VALUE), "KEEP_NEEDS_ONE_SLOT"),
    (lambda: ProposedSlotDirective("d", DirectiveKind.CLEAR, SPAN), "DIRECTIVE_DIMENSION_NOT_A_SLOT"),
    (lambda: ProposedSlotDirective("d", DirectiveKind.CLEAR, SPAN, D.UNSUPPORTED), "DIRECTIVE_DIMENSION_NOT_A_SLOT"),
    (lambda: ProposedSlotDirective("d", DirectiveKind.CLEAR, SPAN, D.VALUE, Polarity.NEGATIVE),
     "VALUE_DIRECTIVE_HAS_POLARITY"),
    (lambda: ProposedSlotDirective("d", DirectiveKind.CLEAR, SPAN, D.TOPIC, side=ValueSide.LOWER),
     "SIDE_OUTSIDE_VALUE"),
])
def test_directive_shapes_fail_closed(build, code):
    with pytest.raises(ContractError, match=code):
        build()


def test_directive_reader_is_strict():
    payload = ProposedSlotDirective("d", DirectiveKind.CANCEL_PENDING, SPAN).to_dict()
    for tampered in (dict(payload, extra=1), {k: v for k, v in payload.items() if k != "side"},
                     dict(payload, kind="CANCEL"), dict(payload, kind="CLEAR"), dict(payload, evidence=[])):
        with pytest.raises(ContractError):
            ProposedSlotDirective.from_dict(tampered)


def test_directives_are_not_yet_part_of_the_proposal():
    """INV-M2A-06: the first-turn ``interpretation_hash`` digests ``InterpretationProposal.to_dict``; the 5.1
    directives must not appear there until the v5.1 wire (M2A-5) carries them."""
    from app.core.interpretation_contract import InterpretationProposal

    assert "directives" not in InterpretationProposal.__dataclass_fields__
    assert RESERVED_UNSUPPORTED_LABELS == {"relative_to_previous", "no_selection", "value_without_comparator"}
