"""M2A-1a.1 CANONICALITY HARDENING: one meaning, one persisted form (``app/core/foundation_accepted.py``).

Invariants proved here (ZEUS P2 of M2A-1a; contract §3, §4.3, §8, §12):

1. Set-like lists are sorted and unique: ``AcceptedCondition.request_ids``, ``receipt_rules`` and ``policies``, and
   ``PendingClarification.answer_slots``. A ``HostPolicy`` persisted inside an ``AcceptedInterpretation`` cites ONE
   condition (M2A-3 1b, D-CANON-2: the split form ``main`` emits), so its ``condition_ids`` has no order to fix.
   A list in any other order (or with a repeated item) is REJECTED by the constructor and by the strict reader,
   never reordered on read; so ``to_dict(from_dict(d)) == d`` holds for every
   accepted document (Hypothesis over permuted documents).
2. VALUE coherence: polarity is POSITIVE (the ``MoneyValue`` comparator absorbs the negation) and the operator names
   the value's own comparator. No new rule is generalized to other dimensions.
3. ``bool`` never counts as ``int`` (nor ``int`` as ``bool``); every persisted datetime is canonical UTC; the digest
   is deterministic with non-ASCII text and does not depend on the process hash seed.

Each battery is also run against MUTANTS of the module (the same ``exec`` technique as
``tests/test_foundation_accepted_contract.py``): a check that does not kill its mutant does not count.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
import types
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
from hypothesis import given
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
from app.core.foundation_authority_values import Comparator, MoneyValue, TopicValue
from app.core.foundation_operations import Subject
from app.core.foundation_operator_table import OPERATORS
from app.core.interpretation_contract import ContractError, Effect, HostPolicy, Polarity
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_foundation_accepted_contract import (
    PLAN, PROPS, RAW, ROOT, SOURCE, T0, _at, condition, origin, pending, record, request,
)

UNICODE_TOPIC = "vías y puentes — nariño ñandú €"  # percent-encoded inside the value tuple
UNICODE_ROOT = "raíz-ñandú-€"  # free text stays raw UTF-8 in the persisted document


# ---- builders: every set-like list holds at least two items ------------------------------------------------------
def rich_interpretation(m=fa):
    rids = ("r1", "r2")
    topic = replace(condition(m, D.TOPIC, 0, "c1", rids), value=TopicValue(UNICODE_TOPIC, True, False).encode(),
                    policies=("actor_state@1", "topn@1"))
    value = replace(condition(m, D.VALUE, 0, "c2", rids), receipt_rules=("unit.millones", "unit.sibling"))
    conditions = (topic, value, condition(m, D.ORDER, 0, "c3", rids), condition(m, D.CARDINALITY, 1, "c4", rids))
    return m.AcceptedInterpretation((request(m), request(m, "r2", Subject.CONTRACTS)), conditions, None,
                                    (HostPolicy("topn", "1", ("c1",), Effect.NARROWING),
                                     HostPolicy("topn", "1", ("c4",), Effect.NARROWING)), None, UNICODE_ROOT)


def rich_pending(m=fa):
    return replace(pending(m), frame=rich_interpretation(m))


SET_LIKE_KEYS = frozenset({"request_ids", "receipt_rules", "policies", "answer_slots"})


def set_like_paths(payload) -> list[tuple]:
    """Every set-like list of strings with two or more items in a payload (path to the list)."""
    out = []

    def walk(node, path):
        if isinstance(node, dict):
            for key, child in node.items():
                if (key in SET_LIKE_KEYS and isinstance(child, list) and len(child) >= 2
                        and all(isinstance(item, str) for item in child)):
                    out.append((*path, key))
                walk(child, (*path, key))
        elif isinstance(node, list):
            for index, child in enumerate(node):
                walk(child, (*path, index))

    walk(payload, ())
    return out


def _accepts(reader, payload) -> bool:
    try:
        reader(payload)
    except ContractError:
        return False
    return True


def _rejects(build) -> bool:
    try:
        build()
    except ContractError:
        return True
    return False


# ---- 1. canonicality ------------------------------------------------------------------------------------------------
def canonicality_failures(m) -> list[str]:
    out = []
    item = rich_pending(m)
    payload = json.loads(json.dumps(item.to_dict()))
    try:
        again = m.PendingClarification.from_dict(copy.deepcopy(payload))
        if again != item or again.to_dict() != payload:
            out.append("the canonical document does not round-trip exactly")
    except ContractError as error:
        out.append(f"the canonical document is rejected: {error}")
    paths = set_like_paths(payload)
    if {path[-1] for path in paths} != SET_LIKE_KEYS:
        out.append(f"vacuous battery: set-like keys covered {sorted({p[-1] for p in paths})}")
    for path in paths:
        for label, change in (("reversed", lambda items: items[::-1]), ("repeated", lambda items: items[:1] + items)):
            tampered = copy.deepcopy(payload)
            _at(tampered, path[:-1])[path[-1]] = change(_at(payload, path))
            if _accepts(m.PendingClarification.from_dict, tampered):
                out.append(f"reader accepts a {label} {'/'.join(map(str, path))}")
    effective = rich_interpretation(m)
    topic, value = effective.conditions[0], effective.conditions[1]
    for label, build in (
            ("request_ids", lambda: replace(topic, request_ids=("r2", "r1"))),
            ("receipt_rules", lambda: replace(value, receipt_rules=("unit.sibling", "unit.millones"))),
            ("condition policies", lambda: replace(topic, policies=("topn@1", "actor_state@1"))),
            ("multi-condition policy", lambda: replace(effective, policies=(
                HostPolicy("topn", "1", ("c1", "c4"), Effect.NARROWING),)))):
        if not _rejects(build):
            out.append(f"constructor accepts a non-canonical {label}")
    return out


def test_the_real_module_has_one_persisted_form():
    assert canonicality_failures(fa) == []


@PROPS
@given(st.data())
def test_property_a_permuted_document_is_rejected_or_identical(data):
    """``to_dict(from_dict(d)) == d`` for every accepted ``d``: a permuted set-like list is never silently fixed."""
    payload = json.loads(json.dumps(rich_pending().to_dict()))
    changed = False
    for path in set_like_paths(payload):
        node = _at(payload, path[:-1])
        permuted = data.draw(st.permutations(node[path[-1]]))
        changed |= permuted != node[path[-1]]
        node[path[-1]] = permuted
    try:
        read = fa.PendingClarification.from_dict(copy.deepcopy(payload))
    except ContractError:
        assert changed
        return
    assert not changed and read.to_dict() == payload


def test_host_policy_itself_is_not_narrowed():
    """Only the form persisted by M2a is canonical: the IR v2 ``HostPolicy`` keeps its own contract (M2A-3 1b: the
    persisted form is one application per condition; a joint policy needs its owner's express declaration)."""
    assert HostPolicy("topn", "1", ("c4", "c1"), Effect.NARROWING).condition_ids == ("c4", "c1")
    for condition_ids in (["c4", "c1"], ["c1", "c4"]):
        with pytest.raises(ContractError, match="MULTI_CONDITION_POLICY"):
            fa.policy_from_dict({"policy_id": "topn", "version": "1", "condition_ids": condition_ids,
                                 "effect": Effect.NARROWING.value})


@pytest.mark.parametrize("rules", [("unit.sibling", "unit.millones"), ("year.inferred", "buyer.exact"),
                                   ("unit.millones", "unit.millones")])
def test_receipt_rules_out_of_canonical_order_are_rejected(rules):
    base = rich_interpretation().conditions[1]
    with pytest.raises(ContractError, match="UNSORTED_RECEIPT_RULES|DUPLICATE_RECEIPT_RULES"):
        replace(base, receipt_rules=rules)
    payload = base.to_dict()
    with pytest.raises(ContractError):
        fa.AcceptedCondition.from_dict(dict(payload, receipt_rules=list(rules)))


def test_answer_slots_are_rejected_not_reordered():
    payload = rich_pending().to_dict()
    slots = payload["answer_slots"]
    assert slots == sorted(set(slots)) and len(slots) == 3
    for tampered in (slots[::-1], slots + slots[:1], [slots[1], slots[0], slots[2]]):
        with pytest.raises(ContractError, match="NONCANONICAL_ANSWER_SLOTS"):
            fa.PendingClarification.from_dict(dict(payload, answer_slots=tampered))


# ---- 2. VALUE coherence ----------------------------------------------------------------------------------------------
def _value_condition(m, comparator, operator, polarity=Polarity.POSITIVE):
    encoded = MoneyValue(500, comparator).encode()
    return m.AcceptedCondition("v", ("r1",), m.slot_of(D.VALUE, encoded), D.VALUE, operator, polarity, None,
                               encoded, "value_authority@1", (), (), origin(m), m.Provenance())


def value_failures(m) -> list[str]:
    out = []
    for comparator in Comparator:
        for operator in sorted(OPERATORS[D.VALUE]):
            for polarity in Polarity:
                coherent = polarity is Polarity.POSITIVE and operator == fa.VALUE_OPERATOR_OF[comparator]
                if _rejects(lambda: _value_condition(m, comparator, operator, polarity)) == coherent:
                    out.append(f"{comparator.value} {operator} {polarity.value}: "
                               f"{'rejected' if coherent else 'accepted'}")
    payload = _value_condition(fa, Comparator.GTE, "greater_or_equal").to_dict()
    for change in (dict(polarity=Polarity.NEGATIVE.value), dict(operator="less_than"),
                   dict(operator="greater_than")):
        if _accepts(m.AcceptedCondition.from_dict, dict(payload, **change)):
            out.append(f"reader accepts VALUE {change}")
    return out


def test_value_conditions_are_coherent():
    assert value_failures(fa) == []


def test_the_value_operator_map_is_the_closed_table():
    assert set(fa.VALUE_OPERATOR_OF) == set(Comparator)
    assert set(fa.VALUE_OPERATOR_OF.values()) == OPERATORS[D.VALUE]
    assert len(set(fa.VALUE_OPERATOR_OF.values())) == len(Comparator)


@pytest.mark.parametrize("comparator, operator, polarity, code", [
    (Comparator.LTE, "less_or_equal", Polarity.NEGATIVE, "NEGATED_VALUE"),
    (Comparator.GT, "greater_or_equal", Polarity.POSITIVE, "OPERATOR_DISAGREES_WITH_COMPARATOR"),
    (Comparator.LT, "greater_than", Polarity.POSITIVE, "OPERATOR_DISAGREES_WITH_COMPARATOR"),
])
def test_incoherent_value_fails_closed(comparator, operator, polarity, code):
    with pytest.raises(ContractError, match=code):
        _value_condition(fa, comparator, operator, polarity)


def test_value_rules_are_not_generalized():
    """A negated TOPIC (``excluded``) stays valid: the VALUE rule is about the comparator, not about negation."""
    negated = condition(fa, D.TOPIC, 1, "c1")
    assert negated.polarity is Polarity.NEGATIVE
    assert fa.AcceptedCondition.from_dict(negated.to_dict()) == negated


# ---- 3a. bool is never an int (nor an int a bool) -------------------------------------------------------------------
def int_failures(m) -> list[str]:
    out = []
    group = m.GroupRef("r1", "ds", PLAN, "value_desc", 10, ("id-1",), 10, 42, m.TotalQuality.EXACT)
    item = pending(m)
    for label, build in (
            ("provenance year", lambda: m.Provenance(year=True, year_source=m.YearSource.STATED)),
            ("origin span", lambda: m.ConditionOrigin(m.OriginKind.SPAN, "t", RAW, (False, True))),
            ("pending asks", lambda: replace(item, asks=True)),
            ("ambiguous span", lambda: replace(item, ambiguous_span=(False, True))),
            ("group page_limit", lambda: replace(group, page_limit=True)),
            ("group next_offset", lambda: replace(group, next_offset=False)),
            ("group total", lambda: replace(group, total=True)),
            ("execution flags", lambda: m.ExecutionReceipt(1, 1, 1, (PLAN,)))):
        if not _rejects(build):
            out.append(f"constructor accepts a bool/int swap in {label}")
    for label, reader, payload in (
            ("group", m.GroupRef.from_dict, dict(group.to_dict(), page_limit=True)),
            ("group", m.GroupRef.from_dict, dict(group.to_dict(), next_offset=False)),
            ("group", m.GroupRef.from_dict, dict(group.to_dict(), total=True)),
            ("pending", m.PendingClarification.from_dict, dict(item.to_dict(), asks=True)),
            ("pending", m.PendingClarification.from_dict, dict(item.to_dict(), ambiguous_span=[False, True])),
            ("origin", m.ConditionOrigin.from_dict, dict(origin(m).to_dict(), span=[False, True])),
            ("provenance", m.Provenance.from_dict,
             dict(m.Provenance().to_dict(), year=True, year_source=m.YearSource.STATED.value)),
            ("execution", m.ExecutionReceipt.from_dict,
             {"accepted": 1, "executed": 1, "source_ok": 0, "plan_hashes": [PLAN]})):
        if _accepts(reader, payload):
            out.append(f"reader accepts a bool/int swap in {label}")
    return out


def test_bool_never_counts_as_int():
    assert int_failures(fa) == []


# ---- 3b. every persisted datetime is canonical UTC ------------------------------------------------------------------
def utc_failures(m) -> list[str]:
    out = []
    item = record(m, m.TurnResolution.CLARIFY, with_patch=False, with_pending=True)
    for value in (T0.replace(tzinfo=None), T0.astimezone(timezone(timedelta(hours=-5))),
                  T0.replace(tzinfo=ZoneInfo("UTC"))):
        if not _rejects(lambda: replace(item, created_at_utc=value)):
            out.append(f"record accepts created_at_utc={value!r}")
        if not _rejects(lambda: replace(item.pending, created_at_utc=value, expires_at_utc=T0 + timedelta(days=1))):
            out.append(f"pending accepts created_at_utc={value!r}")
    payload = item.to_dict()
    written = [payload["created_at_utc"], payload["reference_instant"], payload["pending"]["created_at_utc"],
               payload["pending"]["expires_at_utc"]]
    if not all(text.endswith("+00:00") and datetime.fromisoformat(text).isoformat() == text for text in written):
        out.append(f"non-canonical UTC written: {written}")
    for text in ("2026-10-01T12:00:00Z", "2026-10-01T12:00:00", "2026-10-01T07:00:00-05:00",
                 "2026-10-01T12:00:00.000000+00:00", "2026-10-01 12:00:00+00:00"):
        if _accepts(m.AcceptedTurnRecord.from_dict, dict(payload, reference_instant=text)):
            out.append(f"record reader accepts {text}")
        pend = dict(payload["pending"], created_at_utc=text)
        if _accepts(m.PendingClarification.from_dict, pend):
            out.append(f"pending reader accepts {text}")
    return out


def test_every_persisted_datetime_is_canonical_utc():
    assert utc_failures(fa) == []


# ---- 3c. the digest is deterministic with non-ASCII text ------------------------------------------------------------
# Written by hand, not with ``json.dumps``: the canonical bytes are UTF-8, keys sorted, ", "/": " separators.
UNICODE_DOC = {"z": "€", "b": ["Ü", 1, None], "a": "vías — Nariño ñ"}
UNICODE_CANONICAL = '{"a": "vías — Nariño ñ", "b": ["Ü", 1, null], "z": "€"}'


def digest_failures(m) -> list[str]:
    out = []
    expected = hashlib.sha256(UNICODE_CANONICAL.encode("utf-8")).hexdigest()
    if m.digest(UNICODE_DOC) != expected:
        out.append("the digest is not sha256 over the canonical UTF-8 text")
    if m.digest(dict(reversed(list(UNICODE_DOC.items())))) != expected:
        out.append("the digest depends on key insertion order")
    effective = rich_interpretation(m)
    if UNICODE_ROOT not in json.dumps(effective.to_dict(), ensure_ascii=False):
        out.append("vacuous: no raw non-ASCII text in the effective")
    if m.interpretation_hash(effective) != hashlib.sha256(json.dumps(
            effective.to_dict(), sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest():
        out.append("interpretation_hash is not the canonical UTF-8 digest")
    return out


def test_the_digest_is_deterministic_with_unicode():
    assert digest_failures(fa) == []


def test_digests_do_not_depend_on_the_hash_seed():
    """``answer_slots`` is a frozenset of enum-keyed slots, whose iteration order follows ``PYTHONHASHSEED``."""
    item = rich_pending()
    expected = f"{fa.pending_sha256(item)} {fa.interpretation_hash(item.frame)}"
    code = ("from tests.test_foundation_accepted_canonicality import rich_pending, fa\n"
            "p = rich_pending()\nprint(fa.pending_sha256(p), fa.interpretation_hash(p.frame))")
    for seed in ("0", "1", "4242"):
        run = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, check=True,
                             env={**os.environ, "PYTHONHASHSEED": seed})
        assert run.stdout.strip() == expected, seed


# ---- mutants: each battery must kill its own -------------------------------------------------------------------------
def _mutant(*replacements: tuple[str, str]) -> types.ModuleType:
    source = SOURCE
    for old, new in replacements:
        assert source.count(old) == 1, f"mutation anchor not unique: {old!r}"
        source = source.replace(old, new)
    name = "m2a_canonicality_mutant"
    module = types.ModuleType(name)
    module.__file__ = "<mutant>"
    sys.modules[name] = module
    try:
        exec(compile(source, "<mutant>", "exec"), module.__dict__)
    finally:
        sys.modules.pop(name, None)
    return module


MUTANTS = {
    "request_ids_unsorted": (canonicality_failures, (
        '_texts(self.request_ids, "CONDITION_REQUEST_IDS", sorted_unique=True, empty=False)',
        '_texts(self.request_ids, "CONDITION_REQUEST_IDS", empty=False)'),),
    "receipt_rules_unsorted": (canonicality_failures, (
        '_texts(self.receipt_rules, "RECEIPT_RULES", sorted_unique=True)',
        '_texts(self.receipt_rules, "RECEIPT_RULES")'),),
    "condition_policies_unsorted": (canonicality_failures, (
        '_texts(self.policies, "CONDITION_POLICIES", sorted_unique=True)',
        '_texts(self.policies, "CONDITION_POLICIES")'),),
    "multi_condition_policy_accepted": (canonicality_failures, (
        '    if len(policy.condition_ids) != 1:\n        _fail("MULTI_CONDITION_POLICY")', '    pass'),),
    "answer_slots_reordered_on_read": (canonicality_failures, (
        '        if list(slot_keys) != sorted(set(slot_keys)):\n            _fail("NONCANONICAL_ANSWER_SLOTS")',
        '        slot_keys = tuple(sorted(set(slot_keys)))'),),
    "answer_slots_duplicates_collapsed": (canonicality_failures, (
        'if list(slot_keys) != sorted(set(slot_keys)):', 'if list(slot_keys) != sorted(slot_keys):'),),
    "answer_slots_written_unsorted": (canonicality_failures, (
        '"answer_slots": sorted(slot.key for slot in self.answer_slots)}',
        '"answer_slots": sorted((slot.key for slot in self.answer_slots), reverse=True)}'),),
    "value_negative_accepted": (value_failures, (
        '            if self.polarity is not Polarity.POSITIVE:\n                _fail("NEGATED_VALUE")',
        '            pass'),),
    "value_operator_ignores_comparator": (value_failures, (
        'if self.operator != VALUE_OPERATOR_OF[decode_value(self.value).comparator]:',
        'if self.operator not in VALUE_OPERATOR_OF.values():'),),
    "bool_counts_as_int": (int_failures, (
        '    if type(value) is not int or value < low:', '    if not isinstance(value, int) or value < low:'), (
        '    if type(value) is not int:\n        _fail(f"INVALID_{label}")\n    return value',
        '    if not isinstance(value, int):\n        _fail(f"INVALID_{label}")\n    return value')),
    "bool_span_bound": (int_failures, (
        'or any(type(bound) is not int for bound in self.span)',
        'or any(not isinstance(bound, int) for bound in self.span)'),),
    "int_counts_as_bool": (int_failures, (
        '    if type(value) is not bool:\n        _fail(f"INVALID_{label}")',
        '    if not isinstance(value, int):\n        _fail(f"INVALID_{label}")'),),
    "any_aware_datetime_accepted": (utc_failures, (
        'if type(value) is not datetime or value.tzinfo is not timezone.utc:',
        'if type(value) is not datetime or value.tzinfo is None:'),),
    "noncanonical_utc_text_accepted": (utc_failures, (
        '    if parsed.isoformat() != value:\n        _fail(f"NONCANONICAL_{label}")', '    pass'),),
    "digest_escapes_non_ascii": (digest_failures, ('ensure_ascii=False, allow_nan=False',
                                                   'ensure_ascii=True, allow_nan=False'),),
    "digest_keeps_insertion_order": (digest_failures, ('json.dumps(value, sort_keys=True,',
                                                       'json.dumps(value, sort_keys=False,'),),
}


@pytest.mark.parametrize("name", sorted(MUTANTS))
def test_every_canonicality_mutant_is_killed(name):
    battery, *replacements = MUTANTS[name]
    assert battery(_mutant(*replacements)), f"mutant {name} survived"
