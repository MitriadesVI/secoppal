"""M2A-2: the append-only store of ``AcceptedTurnRecord`` (``app/core/foundation_accepted_store.py``).

Invariants proved here (mission M2A-2; contract §3.8, INV-M2A-03, INV-M2A-09), each by a CHECK that also runs against
MUTANTS of the module (its source with one rule broken, executed in a throwaway module): a check that does not kill its
mutant does not count. A check is a plain function of ``(module, root)`` that asserts; the real module must pass it and
every mutant listed for it must fail it by assertion.

 1. append-only, one file per chat ............ check_append_only                       wb/truncate
 2. shared lock with the ENVELOPE key (F2) ..... check_lock_key_and_reentrancy (+threads/processes/paused/clear)
 3. strict reader: nothing skipped, no last-wins  check_corrupt_middle, check_duplicate_trace_ids
 4. truncated tail => invalid, never rolled back  check_truncated_tail
 5. EMPTY is not CORRUPT (typed apart) ......... check_empty_is_not_unreadable
 6. anchor: exactly one readable record, published (F5), never a fallback ... check_anchor_*
 7. clear() deletes the store with the history under the same lock, F4 race ... check_clear_*, check_stale_epoch
 8. byte identity: identical or invalid, never reordered ............. check_byte_identity,
                                                                       check_a_reorder_inside_a_slot_is_never_read_as_written
 9. no heuristic recovery ................................ (3), (4), (8) and the byte-flip property
10. personal data: no logs, no content in errors ............ check_no_content_leaks, AST pin

Fixtures: ``make_record`` is the ONLY place that builds a record (minimal shapes: 1 request, <= 3 conditions, 0 policies)
and it builds the effective with ``fa.canonical_interpretation`` (M2A-3: the type REJECTS a condition order that is not
the canonical one, it never rewrites it, so a fixture can no longer hand it conditions in any order; the constructor
groups them by slot and keeps the order given inside a slot). Consequence for the invariant «identical or invalid»:
a reorder ACROSS slots (``topic, buyer`` -> ``buyer, topic``) is no longer a different record, it is an INVALID one
(``NONCANONICAL_CONDITION_ORDER``), so a reader that does it fails closed instead of returning another record;
a reorder INSIDE a slot (two TOPIC conditions) is still valid and observable, and only the byte identity of the store
catches it. Both are proved: ``check_byte_identity`` (invalid) and ``check_a_reorder_inside_a_slot_is_never_read_as_written``
(valid but different), and each reorder mutant must die in both. Everything is synthetic and lives in ``tmp_path``;
nothing here touches ``data/``.
"""
from __future__ import annotations

import ast
import contextlib
import copy
import io
import json
import logging
import os
import stat
import subprocess
import sys
import tempfile
import threading
import types
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_accepted_store as store
from app.core import conversation_store as legacy
from app.core.conversation_store import ConversationStore
from app.core.foundation_authority_values import ActorValue, BuyerKind, BuyerValue, TopicValue
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import ContractError, Polarity
from app.core.semantic_dimensions import SemanticDimension as D
from app.core.store_lock import ConversationLockTimeout, conversation_lock
from app.core.workflow import store_epoch
from app.core.workflow.continuity_staging import stage_continuity
from tests.test_conversation_store_clear_delta import (
    ENVELOPE, HISTORY, PAGINATION, MUTANTS as LEGACY_MUTANTS, _clear_block, _with_clear, delta_problems,
)
from tests.test_m2a_boundaries import legacy_reach_problems

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "app" / "core" / "foundation_accepted_store.py").read_text(encoding="utf-8")
CHAT = "chat-1"
RAW = "a" * 64
PLAN = "b" * 64
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
SELF = object()
SENTINEL = "SENTINELA-PII-7f3c"
PROPS = settings(max_examples=80, deadline=None, suppress_health_check=[HealthCheck.too_slow])


# ---- the ONE fixture helper ---------------------------------------------------------------------------------------------
def _origin(trace: str) -> fa.ConditionOrigin:
    return fa.ConditionOrigin(fa.OriginKind.SPAN, trace, RAW, (0, 3))


def _condition(kind: str, trace: str, actor: str = SENTINEL) -> fa.AcceptedCondition:
    """``topic`` has id c2 and ``buyer`` id c1 ON PURPOSE: (topic, buyer) is canonical (TOPIC before BUYER) and NOT
    sorted by id, so a reader that sorts by id is visible (and, under the canonical type, invalid). ``topic2`` (c3) shares
    the TOPIC/POSITIVE slot with ``topic``: (topic, topic2) and (topic2, topic) are BOTH canonical and two different
    records, because inside a slot the order given is kept (and observable)."""
    dimension, value, operator, cid = {
        "topic": (D.TOPIC, TopicValue("obras", True, False), "contains", "c2"),
        "topic2": (D.TOPIC, TopicValue("salud", True, False), "contains", "c3"),
        "buyer": (D.BUYER, BuyerValue("ICBF", BuyerKind.UNIT, ("ICBF",), None, False), "is", "c1"),
        "actor": (D.ACTOR, ActorValue("900123456", "NIT", actor, False), "is", "a1"),
    }[kind]
    encoded = value.encode()
    return fa.AcceptedCondition(cid, ("r1",), fa.slot_of(dimension, encoded), dimension, operator, Polarity.POSITIVE,
                                None, encoded, f"{dimension.value.lower()}_authority@1", (), (), _origin(trace),
                                fa.Provenance())


def make_record(trace_id: str = "t1", *, chat_id: str = CHAT, kind: str = "search",
                conditions: tuple[str, ...] = ("topic",), actor: str | None = None, anchor=SELF,
                turn_id: str | None = None) -> fa.AcceptedTurnRecord:
    """``search``: a READY new search that executed (its effective); ``clarify``: a turn that asked and made no call
    (no effective). ``anchor``: SELF (default for a search), another trace id, or None. ``conditions`` may be given in
    any order: the effective is built with ``canonical_interpretation`` (grouped by slot, the order given kept inside
    a slot), the one constructor path M2A-3 allows."""
    turn_id = turn_id or f"turn-{trace_id}"
    if kind == "clarify":
        return fa.AcceptedTurnRecord(
            trace_id, chat_id, turn_id, T0, T0, "foundation_v5_1", RAW, fa.TurnScope.OUT_NO_ANCHOR,
            fa.TurnResolution.CLARIFY, None, None, None, None, None, None, None, (),
            None if anchor is SELF else anchor)
    kinds = (*conditions, *(("actor",) if actor is not None else ()))
    request = fa.AcceptedRequest("r1", "s1", Subject.PROCESSES, fa.UniverseKind.RECORDS, _origin(trace_id), None,
                                 "ds-processes")
    effective = fa.canonical_interpretation(
        (request,), tuple(_condition(k, trace_id, actor or SENTINEL) for k in kinds), None, (), None, trace_id)
    return fa.AcceptedTurnRecord(
        trace_id, chat_id, turn_id, T0, T0, "foundation_v5_1", RAW, fa.TurnScope.IN_NEW_SEARCH,
        fa.TurnResolution.NEW_SEARCH, None, None, None, effective, fa.ExecutionReceipt(True, True, True, (PLAN,)),
        None, None, (), trace_id if anchor is SELF else anchor)


def line(record: fa.AcceptedTurnRecord) -> bytes:
    """The canonical line of a record, from the REAL module (mutants are always fed real lines)."""
    return store.serialize_record(record).encode("ascii") + b"\n"


def canonical(data: dict) -> bytes:
    """The canonical text of an arbitrary dict (a modified ``to_dict``): isolates the closed-form failure from the
    byte-identity one."""
    return json.dumps(data, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii") + b"\n"


class World:
    """One real ``ConversationStore`` and the store module under test (real or mutant) for one chat."""

    def __init__(self, m, root: Path, chat: str = CHAT):
        self.m, self.chat = m, chat
        self.cs = ConversationStore(root / "conv")
        self.loc = m.AcceptedStoreLocation.for_chat(self.cs, chat)

    @property
    def path(self) -> Path:
        return self.loc.accepted_path

    def epoch(self) -> int:
        return self.cs.reserve_turn(self.chat).epoch

    def append(self, record, **kwargs):
        kwargs.setdefault("epoch", self.epoch())
        return self.m.append(self.loc, record, **kwargs)

    def read(self):
        return self.m.read(self.loc)

    def put(self, *records) -> None:
        self.path.write_bytes(b"".join(line(record) for record in records))


def publish_turn_of(w: World, trace_id: str, *, query: str = "q") -> None:
    """A published turn the way ``coordinate_turn`` does it: reserve, stage the private generation, append the Turn
    and publish the envelope INSIDE the staging (that is what makes the generation file the published history)."""
    reservation = w.cs.reserve_turn(w.chat)
    with stage_continuity(w.cs, reservation):
        w.cs.append_turn(w.chat, query, "r", {}, trace_id=trace_id)
        w.cs.publish_turn(reservation, user_query=query, response="r")


def raised(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except Exception as error:  # noqa: BLE001 - the checks assert on WHAT was raised
        return error
    return None


def read_or_fail(w: World):
    """``read`` returns a ``StoreRead`` whatever the file holds (CORRUPT carries the reason). An exception escaping it
    is a reader that stopped failing closed: a violation of the contract (an assertion), not an error of the test.
    Under the canonical type that is exactly what a reader that reorders conditions across slots does."""
    try:
        return w.read()
    except Exception as error:  # noqa: BLE001 - whatever escapes is the violation being reported
        raise AssertionError(f"read() raised {type(error).__name__}: {error} instead of returning a StoreRead") from error


CHECKS: dict = {}


def check(fn):
    CHECKS[fn.__name__] = fn
    return fn


# ---- 1. append-only ---------------------------------------------------------------------------------------------------
@check
def check_append_only(m, root):
    w = World(m, root)
    a, b = make_record("t1"), make_record("t2", conditions=("topic", "buyer"))
    w.append(a)
    first = w.path.read_bytes()
    assert first == line(a)
    w.append(b)
    assert w.path.read_bytes() == first + line(b)  # the earlier bytes are an untouched prefix
    got = w.read()
    assert got.status is m.StoreStatus.OK and got.records == (a, b)


# ---- 3. strict reader --------------------------------------------------------------------------------------------------
@check
def check_corrupt_middle(m, root):
    w = World(m, root)
    a, b = make_record("t1"), make_record("t2")
    w.path.write_bytes(line(a) + b"{not json}\n" + line(b))
    got = w.read()
    assert got.status is m.StoreStatus.CORRUPT and got.records == ()  # not [a, b]: nothing is skipped
    assert (got.reason, got.line) == ("INVALID_JSON", 2)
    assert m.anchor_of(got, {"t1", "t2"}).status is m.AnchorStatus.ANCHOR_UNREADABLE


@check
def check_duplicate_trace_ids(m, root):
    w = World(m, root)
    first, second = make_record("t1"), make_record("t1", conditions=("topic", "buyer"))
    for lines in ((first, second), (first, make_record("t2"), second)):
        w.put(*lines)
        got = w.read()
        assert got.status is m.StoreStatus.CORRUPT and got.records == ()  # never "the last one"
        assert got.reason == "DUPLICATE_TRACE_ID" and got.line == len(lines)


@check
def check_strict_reader_closed_forms(m, root):
    w = World(m, root)
    rec = make_record("t1", conditions=("topic", "buyer"), turn_id="turno-ñ")
    data = rec.to_dict()

    def edited(**changes):
        out = copy.deepcopy(data)
        out.update(changes)
        return out

    nested_key = copy.deepcopy(data)
    nested_key["effective"]["extra"] = 1
    nested_schema = copy.deepcopy(data)
    nested_schema["effective"]["schema"] = "m2a-accepted/2"
    missing_key = {k: v for k, v in data.items() if k != "echo"}
    contract_rejected = {
        "unknown top-level key": {**data, "extra": 1},
        "missing key": missing_key,
        "unknown record schema": edited(schema="m2a-record/2"),
        "wrong digest": edited(record_sha256="0" * 64),
        "edited field, stale digest": edited(profile="another_profile"),
        "unknown nested key": nested_key,
        "unknown nested schema": nested_schema,
        "not an object": [],
    }
    for label, payload in contract_rejected.items():
        w.path.write_bytes(canonical(payload))
        got = w.read()
        assert got.status is m.StoreStatus.CORRUPT and got.reason.startswith("RECORD_REJECTED"), (label, got)
    text = store.serialize_record(rec)
    parser_level = {
        "duplicate JSON key": ((text[:-1] + ',"chat_id":"chat-1"}').encode(), "DUPLICATE_JSON_KEY"),
        "NaN constant": (text.replace('"control":null', '"control":NaN').encode(), "INVALID_JSON"),
        "not JSON": (b"{broken", "INVALID_JSON"),
        "raw non-ASCII": (text.replace("\\u00f1", "ñ").encode("utf-8"), "INVALID_ENCODING"),
    }
    for label, (raw, reason) in parser_level.items():
        w.path.write_bytes(raw + b"\n")
        got = w.read()
        assert got.status is m.StoreStatus.CORRUPT and got.reason == reason, (label, got)
    w.path.write_bytes(line(rec) + b"\n" + line(make_record("t2")))
    got = w.read()
    assert (got.status, got.reason, got.line) == (m.StoreStatus.CORRUPT, "BLANK_LINE", 2)
    w.path.write_bytes(line(make_record("t1", chat_id="another-chat")))
    got = w.read()
    assert (got.status, got.reason) == (m.StoreStatus.CORRUPT, "FOREIGN_CHAT")  # a chat's file holds only its chat


# ---- 4. truncated tail -----------------------------------------------------------------------------------------------
@check
def check_truncated_tail(m, root):
    w = World(m, root)
    a, b = make_record("t1"), make_record("t2")
    full_b = line(b)
    cases = {
        "half of the next line": (line(a) + full_b[: len(full_b) // 2], 2),
        "a complete JSON without its terminator": (line(a) + full_b[:-1], 2),
        "one byte of the next line": (line(a) + b"{", 2),
        "only a partial line": (full_b[:40], 1),
    }
    for label, (data, at) in cases.items():
        w.path.write_bytes(data)
        got = w.read()
        assert got.status is m.StoreStatus.CORRUPT, label
        assert got.records == (), label  # never rolled back to the previous line
        assert (got.reason, got.line) == ("TRUNCATED_TAIL", at), label
        assert m.anchor_of(got, {"t1", "t2"}).status is m.AnchorStatus.ANCHOR_UNREADABLE, label


# ---- 5. EMPTY is not CORRUPT ---------------------------------------------------------------------------------------------
@check
def check_empty_is_not_unreadable(m, root):
    w = World(m, root)
    assert not w.path.exists()
    for setup in (lambda: None, lambda: w.path.write_bytes(b"")):
        setup()
        got = w.read()
        assert got.status is m.StoreStatus.EMPTY and got.records == () and got.reason is None
        anchor = m.resolve_anchor(w.loc, set())
        assert (anchor.status, anchor.reason) == (m.AnchorStatus.NO_ANCHOR, "STORE_EMPTY")
    w.path.write_bytes(b"\n")
    got = w.read()
    assert (got.status, got.reason) == (m.StoreStatus.CORRUPT, "BLANK_LINE")
    anchor = m.resolve_anchor(w.loc, set())
    assert (anchor.status, anchor.reason) == (m.AnchorStatus.ANCHOR_UNREADABLE, "STORE_CORRUPT")
    assert m.AnchorStatus.NO_ANCHOR is not m.AnchorStatus.ANCHOR_UNREADABLE


@check
def check_an_unreadable_file_is_corrupt_never_empty(m, root):
    w = World(m, root)
    w.path.mkdir()  # a directory where the file should be: reading it raises an OSError that is NOT "not found"
    got = w.read()
    assert (got.status, got.reason, got.line) == (m.StoreStatus.CORRUPT, "IO_ERROR", None)
    anchor = m.resolve_anchor(w.loc, {"t1"})
    assert (anchor.status, anchor.reason) == (m.AnchorStatus.ANCHOR_UNREADABLE, "STORE_CORRUPT")
    err = raised(w.append, make_record("t1"))
    assert isinstance(err, m.AcceptedStoreError) and err.code == "STORE_CORRUPT:IO_ERROR"
    assert w.path.is_dir()


@check
def check_chat_id_is_compared_literally(m, root):
    nfc, nfd = "Zo\u00eb", "Zoe\u0308"  # the same text for a human, two different strings
    assert nfc != nfd
    for location_chat, record_chat in (("Chat-1", "chat-1"), (nfc, nfd), ("chat-1", "chat-1 ")):
        w = World(m, root / f"{abs(hash((location_chat, record_chat)))}", location_chat)
        w.path.write_bytes(line(make_record("t1", chat_id=record_chat)))
        got = w.read()  # on a case-insensitive file system both names are ONE file: only the comparison keeps them apart
        assert (got.status, got.reason) == (m.StoreStatus.CORRUPT, "FOREIGN_CHAT"), (location_chat, record_chat)
        w.path.unlink()
        err = raised(w.append, make_record("t1", chat_id=record_chat))
        assert isinstance(err, m.AcceptedStoreError) and err.code == "FOREIGN_CHAT", (location_chat, record_chat)
        assert not w.path.exists()


# ---- 8. byte identity ----------------------------------------------------------------------------------------------------
@check
def check_byte_identity(m, root):
    w = World(m, root)
    # non-ASCII, an astral character, a lone surrogate and U+2028 all survive as plain ASCII escapes
    rec = make_record("t1", conditions=("topic", "buyer"), turn_id="turno-ñandú-✓ \U0001F600 \ud800 \u2028")
    w.put(rec)
    got = read_or_fail(w)
    assert got.status is m.StoreStatus.OK and got.records == (rec,)  # exactly what was written ...
    assert [c.condition_id for c in got.records[0].effective.conditions] == ["c2", "c1"]  # ... in its order
    assert store.serialize_record(got.records[0]) == store.serialize_record(rec)
    text = store.serialize_record(rec)
    data = json.loads(text)
    non_canonical = {
        "keys in another order": json.dumps(dict(reversed(list(data.items()))), ensure_ascii=True,
                                            separators=(",", ":")),
        "spaces": json.dumps(data, sort_keys=True, ensure_ascii=True),
        "another case of an escape": text.replace("\\u00f1", "\\u00F1"),
    }
    for label, raw in non_canonical.items():
        assert raw != text, label
        w.path.write_bytes(raw.encode("ascii") + b"\n")  # decodes to the SAME record, but it is not what was written
        got = read_or_fail(w)
        assert (got.status, got.reason) == (m.StoreStatus.CORRUPT, "NOT_BYTE_IDENTICAL"), (label, got)
    # A reorder ACROSS slots is not a different record under the canonical type, it is an INVALID one: a self-consistent
    # line (digests recomputed, so the order is its only defect) whose conditions are out of the canonical order is
    # refused by the type, never read as is and never read rewritten into the canonical order.
    disordered = json.loads(text)
    disordered["effective"]["conditions"].reverse()
    disordered["semantic_ref"] = fa.digest(disordered["effective"])
    disordered["record_sha256"] = fa.digest({k: v for k, v in disordered.items() if k != "record_sha256"})
    assert [c["condition_id"] for c in disordered["effective"]["conditions"]] == ["c1", "c2"]
    w.path.write_bytes(canonical(disordered))
    got = read_or_fail(w)
    assert (got.status, got.records, got.reason) == (
        m.StoreStatus.CORRUPT, (), "RECORD_REJECTED:NONCANONICAL_CONDITION_ORDER"), got
    assert m.anchor_of(got, {"t1"}).status is m.AnchorStatus.ANCHOR_UNREADABLE


@check
def check_a_reorder_inside_a_slot_is_never_read_as_written(m, root):
    """Inside a slot the order of the acceptance is kept AND observable (two TOPIC conditions), so reversing them is
    still a VALID record and a DIFFERENT one: only the store can tell. It must come back exactly as written, and a
    store that reads another order must be CORRUPT, never OK. A reader that reorders and keeps the identity check dies
    on the status (NOT_BYTE_IDENTICAL); one that reorders and has no identity check dies on the order it returns."""
    w = World(m, root)
    for kinds, ids in ((("topic", "topic2"), ["c2", "c3"]), (("topic2", "topic"), ["c3", "c2"])):
        rec = make_record("t1", conditions=kinds)
        assert [c.condition_id for c in rec.effective.conditions] == ids  # the constructor kept the order given
        w.put(rec)
        got = read_or_fail(w)
        assert got.status is m.StoreStatus.OK, (kinds, got)
        assert got.records == (rec,), kinds  # exactly what was written ...
        assert [c.condition_id for c in got.records[0].effective.conditions] == ids, kinds  # ... in its order
    assert make_record("t1", conditions=("topic", "topic2")) != make_record("t1", conditions=("topic2", "topic"))


# ---- 6. anchor ---------------------------------------------------------------------------------------------------------
@check
def check_anchor_follows_the_last_pointer(m, root):
    w = World(m, root)
    a, b = make_record("t1"), make_record("t2", kind="clarify", anchor="t1")
    w.put(a, b)
    got = m.resolve_anchor(w.loc, {"t1"})  # a clarify keeps the previous anchor; only the pointed record must be published
    assert got.status is m.AnchorStatus.ANCHOR and got.record == a and got.reason is None
    w.put(a, make_record("t3"))
    assert m.resolve_anchor(w.loc, {"t1", "t3"}).record.trace_id == "t3"  # a search anchors itself


@check
def check_anchor_requires_the_published_history(m, root):
    w = World(m, root)
    w.put(make_record("t1"), make_record("t3"))
    got = m.resolve_anchor(w.loc, {"t1"})  # t3 was written but its Turn never was (crash between both)
    assert (got.status, got.reason) == (m.AnchorStatus.ANCHOR_UNREADABLE, "ANCHOR_NOT_PUBLISHED")
    assert got.record is None  # and it does NOT fall back to t1
    got = m.resolve_anchor(w.loc, set())
    assert got.status is m.AnchorStatus.ANCHOR_UNREADABLE


@check
def check_anchor_never_falls_back(m, root):
    w = World(m, root)
    w.put(make_record("t1"), make_record("t2", kind="clarify", anchor=None))
    got = m.resolve_anchor(w.loc, {"t1", "t2"})  # the tail leaves no anchor: t1 is not resurrected
    assert (got.status, got.reason) == (m.AnchorStatus.NO_ANCHOR, "TAIL_LEAVES_NO_ANCHOR")
    w.put(make_record("t1", anchor="zz"))
    got = m.resolve_anchor(w.loc, {"t1", "zz"})
    assert (got.status, got.reason) == (m.AnchorStatus.ANCHOR_UNREADABLE, "POINTER_UNRESOLVED")
    w.put(make_record("t1", kind="clarify", anchor=None), make_record("t2", kind="clarify", anchor="t1"))
    got = m.resolve_anchor(w.loc, {"t1", "t2"})
    assert (got.status, got.reason) == (m.AnchorStatus.ANCHOR_UNREADABLE, "ANCHOR_WITHOUT_EFFECTIVE")


@check
def check_an_ambiguous_pointer_is_unreadable(m, root):
    first, second = make_record("t1"), make_record("t1", conditions=("topic", "buyer"))
    forged = m.StoreRead(m.StoreStatus.OK, (first, second))  # the reader never produces this; the function still refuses
    got = m.anchor_of(forged, {"t1"})
    assert (got.status, got.reason) == (m.AnchorStatus.ANCHOR_UNREADABLE, "POINTER_AMBIGUOUS") and got.record is None


@check
def check_published_ids_are_a_collection(m, root):
    forged = m.StoreRead(m.StoreStatus.OK, (make_record("t1"),))
    assert isinstance(raised(m.anchor_of, forged, "t1"), TypeError)  # a string would iterate its characters
    assert m.anchor_of(forged, ["t1"]).status is m.AnchorStatus.ANCHOR
    assert m.anchor_of(forged, iter(["t1"])).status is m.AnchorStatus.ANCHOR


@check
def check_published_history_is_what_get_history_returns(m, root):
    w = World(m, root)
    publish_turn_of(w, "t1")
    published = {turn.trace_id for turn in w.cs.get_history(CHAT, last_n=1000)}
    assert published == {"t1"}
    flat = w.cs.base_path / f"{CHAT}.jsonl"
    assert not flat.exists()  # F5: the history of a usable envelope is NOT <chat>.jsonl: a flat read would find nothing
    w.append(make_record("t1"))
    assert m.resolve_anchor(w.loc, published).status is m.AnchorStatus.ANCHOR
    assert m.resolve_anchor(w.loc, set()).status is m.AnchorStatus.ANCHOR_UNREADABLE  # what a flat read would feed
    generation = next((w.cs.base_path / ".generations").rglob("history.jsonl"))
    generation.write_text('{"trace_id": "t1", broken', encoding="utf-8")  # get_history omits corrupt lines in silence
    published = {turn.trace_id for turn in w.cs.get_history(CHAT, last_n=1000)}
    assert published == set()
    assert m.resolve_anchor(w.loc, published).status is m.AnchorStatus.ANCHOR_UNREADABLE  # which can only close it


# ---- append refusals ---------------------------------------------------------------------------------------------------
@check
def check_append_refuses_what_would_poison_the_store(m, root):
    w = World(m, root)
    a = make_record("t1")
    w.append(a)
    before = w.path.read_bytes()
    err = raised(w.append, make_record("t1", conditions=("topic", "buyer")))
    assert isinstance(err, m.AcceptedStoreError) and err.code == "DUPLICATE_TRACE_ID"
    err = raised(w.append, make_record("t9", chat_id="another-chat"))
    assert isinstance(err, m.AcceptedStoreError) and err.code == "FOREIGN_CHAT"
    assert isinstance(raised(m.append, w.loc, a.to_dict(), epoch=0), TypeError)
    assert isinstance(raised(m.append, w.loc, make_record("t9"), epoch=True), TypeError)  # a bool is not an epoch
    assert isinstance(raised(m.append, "not a location", make_record("t9"), epoch=0), TypeError)
    assert w.path.read_bytes() == before
    w.path.write_bytes(before + b"{garbage")  # a corrupt store: appending would bury the evidence
    corrupt = w.path.read_bytes()
    err = raised(w.append, make_record("t2"))
    assert isinstance(err, m.AcceptedStoreError) and err.code == "STORE_CORRUPT:TRUNCATED_TAIL"
    assert w.path.read_bytes() == corrupt


@check
def check_append_never_writes_a_line_the_reader_would_reject(m, root):
    w = World(m, root)
    record = make_record("t1")

    def tighter_reader(cls, data):
        raise ContractError("TIGHTER_CANONICAL_FORM")

    epoch = w.epoch()
    with mock.patch.object(fa.AcceptedTurnRecord, "from_dict", classmethod(tighter_reader)):
        err = raised(m.append, w.loc, record, epoch=epoch)
    assert isinstance(err, m.AcceptedStoreError)
    assert err.code == "RECORD_NOT_READABLE:RECORD_REJECTED:TIGHTER_CANONICAL_FORM"
    assert not w.path.exists()  # one unreadable line would invalidate the whole store


# ---- 7. epoch and clear ----------------------------------------------------------------------------------------------------
@check
def check_stale_epoch(m, root):
    w = World(m, root)
    stale = w.epoch()  # the reservation the record is prepared under
    record = make_record("t1")
    w.cs.clear(w.chat)  # a reset invalidates it
    assert w.epoch() == stale + 1
    err = raised(m.append, w.loc, record, epoch=stale)
    assert isinstance(err, m.StaleEpochError) and err.code == "STALE_EPOCH"
    assert not w.path.exists()  # nothing of the deleted chat is resurrected
    err = raised(m.append, w.loc, record, epoch=stale + 2)  # never trusts the caller: only the current epoch passes
    assert isinstance(err, m.StaleEpochError)
    assert not w.path.exists()
    m.append(w.loc, record, epoch=w.epoch())
    assert w.read().records == (record,)


@check
def check_an_unreadable_epoch_refuses_the_append(m, root):
    w = World(m, root)
    for content in ("not a number", "", "1.5", "-3"):  # garbage, empty, not an int, a negative epoch (even for epoch=-3)
        store_epoch.epoch_path(w.loc.envelope_path).write_text(content)
        err = raised(m.append, w.loc, make_record("t1"), epoch=0)
        assert isinstance(err, m.AcceptedStoreError) and err.code == "EPOCH_UNREADABLE", content
        assert not w.path.exists(), content


@check
def check_the_clear_literal_and_the_store_name_agree(m, root):
    for chat in ("tg_123", "-456", "789", "+57 300 123 4567", "wa_573001234567", "st_abc", "hello world", "Ñandú/x",
                 "a" * 100, "x-y_z"):
        w = World(m, root / "agree", chat)
        safe = legacy._sanitize_chat_id(chat)
        assert w.loc.accepted_path == w.cs.base_path / f"{safe}.accepted.jsonl"
        assert w.loc.envelope_path == w.cs.base_path / f"{safe}.envelopes.jsonl"
        w.path.write_bytes(b"x")
        w.cs.clear(chat)  # ConversationStore.clear names the file by LITERAL: it must be this one
        assert not w.path.exists(), chat


# ---- durability: flushed, fsynced, and the directory entry synced when the file is created ---------------------------------------------------
@check
def check_an_append_is_flushed_and_fsynced_before_it_returns(m, root):
    w = World(m, root)
    first, second = make_record("t1"), make_record("t2")
    epoch = w.epoch()
    seen: list[tuple[str, int]] = []
    real_fsync = os.fsync

    def observing_fsync(descriptor):
        info = os.fstat(descriptor)
        seen.append(("dir" if stat.S_ISDIR(info.st_mode) else "file", info.st_size))
        return real_fsync(descriptor)

    with mock.patch.object(os, "fsync", observing_fsync):
        m.append(w.loc, first, epoch=epoch)
        created = list(seen)
        seen.clear()
        m.append(w.loc, second, epoch=epoch)
        appended = list(seen)
    # the file is fsynced with the WHOLE line already handed to the OS (flush before fsync), then, because the file
    # was just created, its directory entry is synced
    assert [kind for kind, _ in created] == ["file", "dir"], created
    assert created[0][1] == len(line(first)), created
    assert appended and appended[0] == ("file", len(line(first)) + len(line(second))), appended


# ---- 2. lock identity (F2), reentrancy (F3), threads and processes ---------------------------------------------------------------
@check
def check_lock_key_and_reentrancy(m, root):
    w = World(m, root)
    w.append(make_record("t1"))
    before = w.path.read_bytes()
    epoch = w.epoch()
    with conversation_lock(w.loc.envelope_path):  # the key publish_turn and clear() take
        # not reentrant: inside the lock every operation waits its (short) timeout and fails; nothing is written
        assert isinstance(raised(m.append, w.loc, make_record("t2"), epoch=epoch, lock_timeout=0.2),
                          ConversationLockTimeout)
        assert isinstance(raised(m.read, w.loc, lock_timeout=0.2), ConversationLockTimeout)
        assert isinstance(raised(m.resolve_anchor, w.loc, {"t1"}, lock_timeout=0.2), ConversationLockTimeout)
    assert w.path.read_bytes() == before
    with conversation_lock(w.loc.accepted_path):  # a lock on the DATA file would exclude only by name: it does not
        assert raised(m.append, w.loc, make_record("t3"), epoch=epoch, lock_timeout=0.2) is None  # not blocked
    with conversation_lock(w.loc.envelope_path.with_suffix(".turn")):  # coordinate_turn's key is another one
        m.append(w.loc, make_record("t4"), epoch=epoch, lock_timeout=0.2)
    assert [r.trace_id for r in w.read().records] == ["t1", "t3", "t4"]


@check
def check_exclusion_across_threads(m, root):
    w = World(m, root)
    epoch = w.epoch()
    held, release = threading.Event(), threading.Event()

    def holder():
        with conversation_lock(w.loc.envelope_path):
            held.set()
            release.wait(30)

    thread = threading.Thread(target=holder, daemon=True)
    thread.start()
    try:
        assert held.wait(30)
        assert isinstance(raised(m.append, w.loc, make_record("t1"), epoch=epoch, lock_timeout=0.3),
                          ConversationLockTimeout)
        assert isinstance(raised(m.read, w.loc, lock_timeout=0.3), ConversationLockTimeout)
        assert not w.path.exists()
    finally:
        release.set()
        thread.join(30)
    m.append(w.loc, make_record("t1"), epoch=epoch)
    assert w.read().status is m.StoreStatus.OK


HOLDER = """
import sys
from pathlib import Path
from app.core.store_lock import conversation_lock
with conversation_lock(Path(sys.argv[1]), timeout=30):
    print("locked", flush=True)
    sys.stdin.read()
"""


def _child_env() -> dict:
    return {**os.environ, "PYTHONPATH": str(ROOT)}


@check
def check_exclusion_across_processes(m, root):
    w = World(m, root)
    epoch = w.epoch()
    child = subprocess.Popen([sys.executable, "-c", HOLDER, str(w.loc.envelope_path)], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, text=True, cwd=ROOT, env=_child_env())
    try:
        assert child.stdout.readline().strip() == "locked"
        assert isinstance(raised(m.append, w.loc, make_record("t1"), epoch=epoch, lock_timeout=0.5),
                          ConversationLockTimeout)
        assert isinstance(raised(m.read, w.loc, lock_timeout=0.5), ConversationLockTimeout)
        assert not w.path.exists()
    finally:
        child.stdin.close()
        child.wait(timeout=30)
    m.append(w.loc, make_record("t1"), epoch=epoch)
    assert w.read().status is m.StoreStatus.OK


class PausedAppend:
    """An ``append`` running in a thread that stops INSIDE its critical section (the lock is held, the epoch was just
    read, nothing is written yet) until released. Event-driven: no sleep decides anything."""

    def __init__(self, m, loc, record, epoch):
        self.m, self.loc, self.record, self.epoch = m, loc, record, epoch
        self.entered, self.proceed = threading.Event(), threading.Event()
        self.error = None
        self._thread = None
        self._patch = None

    def _run(self):
        try:
            self.m.append(self.loc, self.record, epoch=self.epoch, lock_timeout=30)
        except Exception as error:  # noqa: BLE001
            self.error = error

    def __enter__(self):
        real = self.m._read_epoch

        def paused(path):
            value = real(path)
            if threading.current_thread() is self._thread:
                self.entered.set()
                self.proceed.wait(30)
            return value

        self._patch = mock.patch.object(self.m, "_read_epoch", paused)
        self._patch.start()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self.entered.wait(5), "the append never reached its critical section"
        return self

    def __exit__(self, *exc):
        self.proceed.set()
        self._thread.join(30)
        self._patch.stop()


@check
def check_the_critical_section_excludes_a_second_writer(m, root):
    w = World(m, root)
    epoch = w.epoch()
    first, second = make_record("dup"), make_record("dup", conditions=("topic", "buyer"))
    with PausedAppend(m, w.loc, first, epoch) as paused:
        # the first writer is inside the critical section: a second one cannot enter
        assert isinstance(raised(m.append, w.loc, second, epoch=epoch, lock_timeout=0.3), ConversationLockTimeout)
        assert not w.path.exists()
    assert paused.error is None
    got = w.read()
    assert got.status is m.StoreStatus.OK and got.records == (first,)  # one line; the cardinality is not hidden


@contextlib.contextmanager
def short_clear_lock_timeout(store_class, seconds: float = 0.3):
    """``ConversationStore.clear`` asks for its lock with the default 10 s. Inside this context the SAME call gives up
    after ``seconds``, so "clear() cannot enter while the lock is held" is decided by an exception, not by how long a
    thread stays alive. Patches the module namespace of the class under test and nothing else."""
    namespace = store_class.clear.__globals__
    real = namespace["conversation_lock"]

    def short(path, **kwargs):
        return real(path, **{"timeout": seconds, **kwargs})

    with mock.patch.dict(namespace, {"conversation_lock": short}):
        yield


@check
def check_clear_cannot_enter_while_an_append_is_in_flight(m, root):
    w = World(m, root)
    epoch = w.epoch()
    with PausedAppend(m, w.loc, make_record("t1"), epoch) as paused:
        with short_clear_lock_timeout(ConversationStore):
            during = raised(w.cs.clear, CHAT)  # clear() takes the same lock: while the append holds it, it cannot enter
    assert isinstance(during, ConversationLockTimeout), "clear() entered while an append held the envelope lock"
    assert paused.error is None
    assert w.path.exists()  # the append finished; the clear that could not enter did nothing ...
    assert w.epoch() == epoch  # ... not even the epoch advance
    w.cs.clear(CHAT)  # once the append is done clear() runs: linearized after it, no residue of the deleted chat
    assert not w.path.exists()
    assert w.epoch() == epoch + 1


class WaitingAppend:
    """A writer that has finished ALL its pre-lock work and WAITS for the lock with a long timeout. The module's
    ``conversation_lock`` is wrapped to set an Event, in that writer's thread, the instant before it asks for the lock:
    whatever the writer decided before waiting it decided already, and whatever it decides after must be judged on the
    state the lock gave it. ``start()`` returns at that instant; ``join()`` collects the outcome. Event-driven."""

    def __init__(self, m, loc, record, epoch):
        self.m, self.loc, self.record, self.epoch = m, loc, record, epoch
        self.at_lock = threading.Event()
        self.error = None
        self._thread = None
        self._patch = None

    def _run(self):
        try:
            self.m.append(self.loc, self.record, epoch=self.epoch, lock_timeout=60)
        except Exception as error:  # noqa: BLE001
            self.error = error

    def start(self):
        real = self.m.conversation_lock

        def signalling(path, **kwargs):
            if threading.current_thread() is self._thread:
                self.at_lock.set()
            return real(path, **kwargs)

        self._patch = mock.patch.object(self.m, "conversation_lock", signalling)
        self._patch.start()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        assert self.at_lock.wait(30), "the writer never reached the lock"

    def join(self):
        self._thread.join(60)
        self._patch.stop()


@check
def check_a_waiting_writer_sees_the_duplicate_the_first_one_left(m, root):
    w = World(m, root)
    epoch = w.epoch()
    first, second = make_record("dup"), make_record("dup", conditions=("topic", "buyer"))
    waiting = WaitingAppend(m, w.loc, second, epoch)
    with PausedAppend(m, w.loc, first, epoch) as paused:  # the first writer is inside its critical section ...
        waiting.start()  # ... and the second one has done its pre-lock work and now waits for the lock
    waiting.join()
    assert paused.error is None
    assert isinstance(waiting.error, m.AcceptedStoreError) and waiting.error.code == "DUPLICATE_TRACE_ID", waiting.error
    got = w.read()
    assert got.status is m.StoreStatus.OK and got.records == (first,)  # one line, and the store stays readable


@check
def check_a_waiting_writer_sees_a_store_corrupted_while_it_waited(m, root):
    w = World(m, root)
    epoch = w.epoch()
    waiting = WaitingAppend(m, w.loc, make_record("t1"), epoch)
    with conversation_lock(w.loc.envelope_path):  # somebody else holds the lock ...
        waiting.start()
        w.path.write_bytes(b"{garbage")  # ... and a crashed writer leaves a truncated tail before releasing it
    waiting.join()
    assert isinstance(waiting.error, m.AcceptedStoreError), waiting.error
    assert waiting.error.code == "STORE_CORRUPT:TRUNCATED_TAIL"
    assert w.path.read_bytes() == b"{garbage"  # nothing was buried under it


@check
def check_a_waiting_writer_sees_the_epoch_advanced_while_it_waited(m, root):
    w = World(m, root)
    epoch = w.epoch()
    waiting = WaitingAppend(m, w.loc, make_record("t1"), epoch)
    with conversation_lock(w.loc.envelope_path):
        waiting.start()
        store_epoch.advance_epoch(w.loc.envelope_path)  # a reset completed while the writer waited
    waiting.join()
    assert isinstance(waiting.error, m.StaleEpochError), waiting.error
    assert not w.path.exists()  # the record of the deleted chat is not resurrected


@check
def check_concurrent_writers_do_not_lose_or_interleave_lines(m, root):
    w = World(m, root)
    writers, per_writer = 6, 12
    records = [[make_record(f"w{i}-{j}", conditions=("topic", "buyer")) for j in range(per_writer)]
               for i in range(writers)]
    barrier, errors = threading.Barrier(writers), []

    def work(i):
        barrier.wait(30)
        for item in records[i]:
            try:
                m.append(w.loc, item, epoch=0, lock_timeout=60)
            except Exception as error:  # noqa: BLE001
                errors.append(error)

    threads = [threading.Thread(target=work, args=(i,), daemon=True) for i in range(writers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(120)
    assert errors == []
    got = w.read()
    assert got.status is m.StoreStatus.OK and len(got.records) == writers * per_writer
    assert w.path.read_bytes().count(b"\n") == writers * per_writer
    ids = [r.trace_id for r in got.records]
    assert len(set(ids)) == len(ids)
    for i in range(writers):  # each writer's own order is kept
        assert [t for t in ids if t.startswith(f"w{i}-")] == [f"w{i}-{j}" for j in range(per_writer)]


CHILD_WRITER = """
import json, sys
from pathlib import Path
from app.core.foundation_accepted import AcceptedTurnRecord
from app.core.foundation_accepted_store import AcceptedStoreLocation, append
location = AcceptedStoreLocation(sys.argv[2], Path(sys.argv[1]))
for item in json.loads(Path(sys.argv[3]).read_text()):
    append(location, AcceptedTurnRecord.from_dict(item), epoch=0, lock_timeout=120)
"""


def test_concurrent_processes_do_not_lose_or_interleave_lines(tmp_path):
    w = World(store, tmp_path)
    procs, per_proc = 4, 10
    ids = [[f"p{i}-{j}" for j in range(per_proc)] for i in range(procs)]
    children = []
    for i in range(procs):
        batch = tmp_path / f"batch{i}.json"
        batch.write_text(json.dumps([make_record(t, conditions=("topic", "buyer")).to_dict() for t in ids[i]]))
        children.append(subprocess.Popen([sys.executable, "-c", CHILD_WRITER, str(w.loc.envelope_path), CHAT, str(batch)],
                                         cwd=ROOT, env=_child_env()))
    assert [child.wait(timeout=180) for child in children] == [0] * procs
    got = w.read()
    assert got.status is store.StoreStatus.OK and len(got.records) == procs * per_proc
    seen = [r.trace_id for r in got.records]
    for i in range(procs):
        assert [t for t in seen if t.startswith(f"p{i}-")] == ids[i]


# ---- the checks on the REAL module and the mutants that must die ---------------------------------------------------------------------
@pytest.mark.parametrize("name", sorted(CHECKS))
def test_the_real_module_passes(name, tmp_path):
    CHECKS[name](store, tmp_path)


def _mutant(mutations: tuple[tuple[str, str], ...]) -> types.ModuleType:
    source = SOURCE
    for old, new in mutations:
        assert source.count(old) == 1, f"mutation anchor not unique: {old!r}"
        source = source.replace(old, new)
    name = "m2a_store_mutant"
    module = types.ModuleType(name)
    module.__file__ = "<mutant>"
    sys.modules[name] = module
    try:
        exec(compile(source, "<mutant>", "exec"), module.__dict__)
    finally:
        sys.modules.pop(name, None)
    return module


APPEND_LOCK = ("    with conversation_lock(location.envelope_path, timeout=lock_timeout):\n"
               "        if _read_epoch")
READ_LOCK = ("    with conversation_lock(location.envelope_path, timeout=lock_timeout):\n"
             "        return _read_file(location)")
TAIL_GUARD = ('    if lines.pop() != b"":  # the terminator is part of the record: an unterminated tail is a truncated '
              'write\n        return _corrupt("TRUNCATED_TAIL", len(lines) + 1)\n')
SKIP_CORRUPT = '        if isinstance(parsed, str):\n            return _corrupt(parsed, number)\n'
LAST_WINS = ('        if parsed.trace_id in seen:  # detect the cardinality, never hide it (the LIMIT 1 lesson)\n'
             '            return _corrupt("DUPLICATE_TRACE_ID", number)\n')
IDENTICAL_TRY = "    try:\n        identical = serialize_record(record) == text\n"
IDENTICAL_RETURN = '    return record if identical else "NOT_BYTE_IDENTICAL"\n'
REORDER = ("    if record.effective is not None:\n"
           "        _dc = __import__('dataclasses')\n"
           "        record = _dc.replace(record, effective=_dc.replace(\n"
           "            record.effective, conditions=tuple(reversed(record.effective.conditions))))\n")
NO_LOCK = '    with __import__("contextlib").nullcontext():\n        if _read_epoch'
PUBLISHED_GUARD = '    if pointer not in frozenset(published_trace_ids):\n'
POINTER_LINE = "    pointer = store_read.records[-1].anchor_trace_id\n"
CODE_GUARD = ('        if code and all(char.isupper() or char.isdigit() or char == "_" for char in code):\n'
              '            return f"RECORD_REJECTED:{code}"\n        return "RECORD_REJECTED"\n')

PAYLOAD_LINE = '    payload = text.encode("ascii") + b"\\n"\n'
IN_LOCK_DUP = ('        if any(stored.trace_id == record.trace_id for stored in current.records):\n'
               '            raise AcceptedStoreError("DUPLICATE_TRACE_ID")\n')
IN_LOCK_CORRUPT = ('        if current.status is StoreStatus.CORRUPT:\n'
                   '            raise AcceptedStoreError(f"STORE_CORRUPT:{current.reason}")\n')
PRE_DUP = ('    _pre = _read_file(location)\n    if any(stored.trace_id == record.trace_id for stored in _pre.records):\n'
           '        raise AcceptedStoreError("DUPLICATE_TRACE_ID")\n')
PRE_CORRUPT = ('    _pre = _read_file(location)\n    if _pre.status is StoreStatus.CORRUPT:\n'
               '        raise AcceptedStoreError(f"STORE_CORRUPT:{_pre.reason}")\n')
# both checks move out of the lock, onto a read taken BEFORE waiting for it (ZEUS' TOCTOU mutant)
PRE_LOCK_CHECKS_OLD = (PAYLOAD_LINE + "    with conversation_lock(location.envelope_path, timeout=lock_timeout):\n"
                       "        if _read_epoch(location.envelope_path) != epoch:\n            raise StaleEpochError()\n"
                       "        current = _read_file(location)\n" + IN_LOCK_CORRUPT + IN_LOCK_DUP)
PRE_LOCK_CHECKS_NEW = (PAYLOAD_LINE + "    current = _read_file(location)\n"
                       '    if current.status is StoreStatus.CORRUPT:\n        raise AcceptedStoreError(f"STORE_CORRUPT:{current.reason}")\n'
                       '    if any(stored.trace_id == record.trace_id for stored in current.records):\n'
                       '        raise AcceptedStoreError("DUPLICATE_TRACE_ID")\n'
                       "    with conversation_lock(location.envelope_path, timeout=lock_timeout):\n"
                       "        if _read_epoch(location.envelope_path) != epoch:\n            raise StaleEpochError()\n")
EPOCH_IN_LOCK_OLD = (PAYLOAD_LINE + "    with conversation_lock(location.envelope_path, timeout=lock_timeout):\n"
                     "        if _read_epoch(location.envelope_path) != epoch:\n            raise StaleEpochError()\n")
EPOCH_BEFORE_LOCK_NEW = (PAYLOAD_LINE + "    if _read_epoch(location.envelope_path) != epoch:\n        raise StaleEpochError()\n"
                         "    with conversation_lock(location.envelope_path, timeout=lock_timeout):\n")
ALL_LOCK_CHECKS = ("check_lock_key_and_reentrancy", "check_exclusion_across_threads",
                   "check_exclusion_across_processes", "check_clear_cannot_enter_while_an_append_is_in_flight")
MUTANTS = {
    # brief: lock sobre otra ruta
    "append_locks_the_data_file_instead_of_the_envelope": (
        ((APPEND_LOCK, APPEND_LOCK.replace("location.envelope_path", "location.accepted_path")),), ALL_LOCK_CHECKS),
    "read_locks_the_data_file_instead_of_the_envelope": (
        ((READ_LOCK, READ_LOCK.replace("location.envelope_path", "location.accepted_path")),),
        ("check_lock_key_and_reentrancy", "check_exclusion_across_threads", "check_exclusion_across_processes")),
    "append_takes_no_lock": (((APPEND_LOCK, NO_LOCK),), (*ALL_LOCK_CHECKS,
                                                          "check_the_critical_section_excludes_a_second_writer")),
    # brief: el lector salta una línea corrupta
    "reader_skips_a_corrupt_line": (
        ((SKIP_CORRUPT, "        if isinstance(parsed, str):\n            continue\n"),), ("check_corrupt_middle",)),
    # brief: toma el último de dos trace_id duplicados
    "reader_keeps_the_last_of_two_duplicate_trace_ids": (
        ((LAST_WINS, "        if parsed.trace_id in seen:\n"
                     "            records = [r for r in records if r.trace_id != parsed.trace_id]\n"),),
        ("check_duplicate_trace_ids",)),
    # brief: acepta una cola truncada
    "reader_drops_the_truncated_tail_and_keeps_the_rest": (
        ((TAIL_GUARD, "    lines.pop()\n"),), ("check_truncated_tail",)),
    "reader_accepts_an_unterminated_tail_as_a_line": (
        ((TAIL_GUARD, '    tail = lines.pop()\n    if tail != b"":\n        lines.append(tail)\n'),),
        ("check_truncated_tail",)),
    # brief: ignora la época
    "append_ignores_the_epoch": (
        (("        if _read_epoch(location.envelope_path) != epoch:", "        if False:"),), ("check_stale_epoch",)),
    "append_refuses_only_an_older_epoch": (
        (("        if _read_epoch(location.envelope_path) != epoch:",
          "        if _read_epoch(location.envelope_path) > epoch:"),), ("check_stale_epoch",)),
    # brief: el store reordena condiciones al leer
    # Under the canonical type a reorder across slots is INVALID (the mutant raises: check_byte_identity), and a reorder
    # inside a slot is valid and different (the status or the order read back: the second check). Each dies in both.
    "reader_reorders_conditions_but_keeps_the_identity_check": (
        ((IDENTICAL_TRY, REORDER + IDENTICAL_TRY),),
        ("check_byte_identity", "check_a_reorder_inside_a_slot_is_never_read_as_written")),
    "reader_reorders_conditions_and_has_no_identity_check": (
        ((IDENTICAL_TRY, REORDER + IDENTICAL_TRY), (IDENTICAL_RETURN, "    return record\n")),
        ("check_byte_identity", "check_a_reorder_inside_a_slot_is_never_read_as_written")),
    "reader_has_no_identity_check": (((IDENTICAL_RETURN, "    return record\n"),), ("check_byte_identity",)),
    # the rest of the invariants
    "append_rewrites_the_file": (
        (('location.accepted_path.open("ab")', 'location.accepted_path.open("wb")'),), ("check_append_only",)),
    "empty_and_corrupt_are_the_same_anchor_state": (
        (('        return AnchorRead(AnchorStatus.ANCHOR_UNREADABLE, None, "STORE_CORRUPT")',
          '        return AnchorRead(AnchorStatus.NO_ANCHOR, None, "STORE_CORRUPT")'),),
        ("check_empty_is_not_unreadable",)),
    "anchor_ignores_the_published_history": (
        ((PUBLISHED_GUARD, "    if False:\n"),), ("check_anchor_requires_the_published_history",)),
    "anchor_falls_back_to_the_last_published_record": (
        ((POINTER_LINE, "    pointer = next((r.trace_id for r in reversed(store_read.records) if r.effective is not None "
                        "and r.trace_id in frozenset(published_trace_ids)), "
                        "store_read.records[-1].anchor_trace_id)\n"),),
        ("check_anchor_requires_the_published_history", "check_anchor_never_falls_back")),
    "anchor_takes_the_first_of_two_matches": (
        (("    if len(matches) != 1:", "    if not matches:"),), ("check_an_ambiguous_pointer_is_unreadable",)),
    "published_ids_may_be_a_string": (
        (('    if isinstance(published_trace_ids, (str, bytes)):\n        raise TypeError("published_trace_ids must be '
          'a collection of trace ids, not a string")\n', ""),), ("check_published_ids_are_a_collection",)),
    "append_allows_a_duplicate_trace_id": (
        (("        if any(stored.trace_id == record.trace_id for stored in current.records):",
          "        if False:"),), ("check_append_refuses_what_would_poison_the_store",)),
    "append_buries_a_corrupt_store": (
        (("        if current.status is StoreStatus.CORRUPT:", "        if False:"),),
        ("check_append_refuses_what_would_poison_the_store",)),
    "append_skips_the_read_back_check": (
        (("    if isinstance(checked, str):", "    if False:"),),
        ("check_append_never_writes_a_line_the_reader_would_reject",)),
    "append_accepts_a_foreign_chat": (
        (("    if record.chat_id != location.chat_id:\n        raise AcceptedStoreError(\"FOREIGN_CHAT\")\n", ""),),
        ("check_append_refuses_what_would_poison_the_store",)),
    "reader_accepts_a_foreign_chat": (
        (("        if parsed.chat_id != location.chat_id:", "        if False:"),), ("check_strict_reader_closed_forms",)),
    "epoch_garbage_reads_as_zero": (
        (('        raise AcceptedStoreError("EPOCH_UNREADABLE") from None\n    if value < 0:',
          '        return 0\n    if value < 0:'),), ("check_an_unreadable_epoch_refuses_the_append",)),
    "the_data_file_is_named_apart_from_clear": (
        (('ACCEPTED_SUFFIX = ".accepted.jsonl"', 'ACCEPTED_SUFFIX = ".accepted.json"'),),
        ("check_the_clear_literal_and_the_store_name_agree",)),
    # ZEUS (round 1) and the follow-ups of the same round
    "dup_and_corrupt_checks_on_a_pre_lock_read": (((PRE_LOCK_CHECKS_OLD, PRE_LOCK_CHECKS_NEW),), (
        "check_a_waiting_writer_sees_the_duplicate_the_first_one_left",
        "check_a_waiting_writer_sees_a_store_corrupted_while_it_waited")),
    "duplicate_check_on_a_pre_lock_read": (
        ((PAYLOAD_LINE, PAYLOAD_LINE + PRE_DUP), (IN_LOCK_DUP, "")),
        ("check_a_waiting_writer_sees_the_duplicate_the_first_one_left",)),
    "corrupt_check_on_a_pre_lock_read": (
        ((PAYLOAD_LINE, PAYLOAD_LINE + PRE_CORRUPT), (IN_LOCK_CORRUPT, "")),
        ("check_a_waiting_writer_sees_a_store_corrupted_while_it_waited",)),
    "epoch_check_before_lock": (
        ((EPOCH_IN_LOCK_OLD, EPOCH_BEFORE_LOCK_NEW),), ("check_a_waiting_writer_sees_the_epoch_advanced_while_it_waited",)),
    "io_error_reads_as_empty": (
        (('    except OSError:\n        return _corrupt("IO_ERROR", None)\n',
          '    except OSError:\n        return StoreRead(StoreStatus.EMPTY)\n'),),
        ("check_an_unreadable_file_is_corrupt_never_empty",)),
    "negative_epoch_is_accepted": (
        (('    if value < 0:\n        raise AcceptedStoreError("EPOCH_UNREADABLE")\n', ""),),
        ("check_an_unreadable_epoch_refuses_the_append",)),
    "chat_id_compared_case_insensitively_on_read": (
        (("        if parsed.chat_id != location.chat_id:", "        if parsed.chat_id.casefold() != location.chat_id.casefold():"),),
        ("check_chat_id_is_compared_literally",)),
    "chat_id_compared_case_insensitively_on_append": (
        (("    if record.chat_id != location.chat_id:\n        raise AcceptedStoreError(\"FOREIGN_CHAT\")",
          "    if record.chat_id.casefold() != location.chat_id.casefold():\n        raise AcceptedStoreError(\"FOREIGN_CHAT\")"),),
        ("check_chat_id_is_compared_literally",)),
    "chat_id_compared_after_unicode_normalization": (
        (("        if parsed.chat_id != location.chat_id:",
          "        if __import__('unicodedata').normalize('NFC', parsed.chat_id) != "
          "__import__('unicodedata').normalize('NFC', location.chat_id):"),),
        ("check_chat_id_is_compared_literally",)),
    "append_never_fsyncs_the_file": (
        (("            os.fsync(handle.fileno())\n", ""),), ("check_an_append_is_flushed_and_fsynced_before_it_returns",)),
    "append_never_syncs_the_directory": (
        (("        if created:\n            _sync_directory(location.accepted_path.parent)\n", ""),),
        ("check_an_append_is_flushed_and_fsynced_before_it_returns",)),
    "append_neither_flushes_nor_fsyncs": (
        (("            handle.flush()\n            os.fsync(handle.fileno())\n", ""),),
        ("check_an_append_is_flushed_and_fsynced_before_it_returns",)),
    "append_fsyncs_before_flushing": (
        (("            handle.flush()\n            os.fsync(handle.fileno())\n",
          "            os.fsync(handle.fileno())\n            handle.flush()\n"),),
        ("check_an_append_is_flushed_and_fsynced_before_it_returns",)),
    "error_codes_carry_the_reader_message": (
        ((CODE_GUARD, '        return f"RECORD_REJECTED:{code}"\n'),), ("check_no_content_leaks",)),
}


def _killed(name: str, module, root: Path) -> bool:
    try:
        CHECKS[name](module, root)
    except AssertionError:
        return True
    return False


MUTANT_CASES = [(mutant, name) for mutant, (_, names) in sorted(MUTANTS.items()) for name in names]


@pytest.mark.parametrize("mutant, name", MUTANT_CASES, ids=[f"{m}-{n}" for m, n in MUTANT_CASES])
def test_every_mutant_is_killed_by_each_check_listed_for_it(mutant, name, tmp_path):
    mutations, _ = MUTANTS[mutant]
    assert _killed(name, _mutant(mutations), tmp_path), f"mutant {mutant} survived {name}"


def test_every_check_has_at_least_one_mutant_or_is_a_positive_control():
    controls = {"check_concurrent_writers_do_not_lose_or_interleave_lines", "check_published_history_is_what_get_history_returns",
                "check_anchor_follows_the_last_pointer"}
    covered = {name for _, (_, names) in MUTANTS.items() for name in names}
    assert set(CHECKS) - covered <= controls, sorted(set(CHECKS) - covered - controls)


# ---- legacy: the divergence of ConversationStore.clear() -------------------------------------------------------------------------------------
def _legacy_class(source: str):
    name = "m2a_legacy_store_mutant"
    module = types.ModuleType(name)
    module.__file__ = "<mutant>"
    sys.modules[name] = module
    try:
        exec(compile(source, "<mutant>", "exec"), module.__dict__)
    finally:
        sys.modules.pop(name, None)
    return module.ConversationStore


def check_clear_deletes_the_store_under_the_envelope_lock(store_class, root: Path):
    cs = store_class(root / "conv")
    location = store.AcceptedStoreLocation.for_chat(cs, CHAT)
    store.append(location, make_record("t1"), epoch=0)
    assert location.accepted_path.exists()
    with conversation_lock(location.envelope_path):  # the lock publish_turn takes, held by someone else
        with short_clear_lock_timeout(store_class):
            during = raised(cs.clear, CHAT)
        survived = location.accepted_path.exists()
    assert isinstance(during, ConversationLockTimeout), "clear() did not wait for the envelope lock"
    assert survived, "clear() acted on the accepted store outside the envelope lock"
    cs.clear(CHAT)  # the lock is free now
    assert not location.accepted_path.exists(), "clear() left the accepted store behind"
    assert store.resolve_anchor(location, {"t1"}).status is store.AnchorStatus.NO_ANCHOR


def test_clear_deletes_the_store_under_the_envelope_lock(tmp_path):
    check_clear_deletes_the_store_under_the_envelope_lock(ConversationStore, tmp_path)


LEGACY_BEHAVIOUR_MUTANTS = {
    "clear_does_not_delete_the_store": lambda: _with_clear(_clear_block((ENVELOPE, HISTORY, PAGINATION))),
    "clear_deletes_the_store_outside_the_lock": LEGACY_MUTANTS["store_deleted_outside_the_lock"],
}


@pytest.mark.parametrize("name", sorted(LEGACY_BEHAVIOUR_MUTANTS))
def test_mutant_of_clear_is_killed_by_behaviour(name, tmp_path):
    cls = _legacy_class(LEGACY_BEHAVIOUR_MUTANTS[name]())
    with pytest.raises(AssertionError):
        check_clear_deletes_the_store_under_the_envelope_lock(cls, tmp_path)


def test_mutant_clear_importing_the_store_is_caught_by_the_legacy_pins():
    mutated = LEGACY_MUTANTS["clear_imports_the_store_lazily"]()
    assert delta_problems(mutated)
    found = legacy_reach_problems(overrides={"app.core.conversation_store": mutated})
    assert any(line_.startswith("legacy closure reaches app.core.foundation_accepted_store") for line_ in found), found


def test_clear_removes_the_store_before_the_history_so_a_failed_reset_never_orphans_personal_data(tmp_path):
    w = World(store, tmp_path)
    w.append(make_record("t1"))
    order = []
    real_unlink = Path.unlink

    def spy(self, *args, **kwargs):
        order.append(self.name)
        return real_unlink(self, *args, **kwargs)

    with mock.patch.object(Path, "unlink", spy):
        w.cs.clear(CHAT)
    assert order == [f"{CHAT}.accepted.jsonl", f"{CHAT}.envelopes.jsonl", f"{CHAT}.jsonl", f"{CHAT}.pagination.json"]


def test_a_reset_leaves_no_anchor_even_with_a_published_history(tmp_path):
    w = World(store, tmp_path)
    publish_turn_of(w, "t1")
    w.append(make_record("t1"))
    published = {turn.trace_id for turn in w.cs.get_history(CHAT, last_n=1000)}
    assert store.resolve_anchor(w.loc, published).status is store.AnchorStatus.ANCHOR
    w.cs.clear(CHAT)
    assert not w.path.exists() and w.cs.get_history(CHAT, last_n=1000) == []
    assert store.resolve_anchor(w.loc, published).status is store.AnchorStatus.NO_ANCHOR  # even with a stale snapshot


def test_constraint_for_m2a_6_inside_the_staging_get_history_already_holds_the_turn_in_flight(tmp_path):
    """NOT a contract of the store: the restriction M2A-6 inherits. ``coordinate_turn`` runs every turn with a
    reservation inside ``stage_continuity``; there ``append_turn`` writes into the PRIVATE generation, which
    ``get_history`` already reads. A snapshot of the history taken after the turn's own ``append_turn`` therefore lists
    a turn that is not published yet (and never will be if the turn crashes or loses the revision race), and
    ``resolve_anchor`` fed with it answers ANCHOR for it. The anchor must be resolved with the PUBLISHED history: the
    snapshot taken before the turn appends its own Turn, or after the publication."""
    w = World(store, tmp_path)
    publish_turn_of(w, "t1")
    w.append(make_record("t1"))
    reservation = w.cs.reserve_turn(CHAT)
    with stage_continuity(w.cs, reservation):
        store.append(w.loc, make_record("t2"), epoch=reservation.epoch)  # the record goes first (contract 3.8)
        before = {turn.trace_id for turn in w.cs.get_history(CHAT, last_n=1000)}
        w.cs.append_turn(CHAT, "q", "r", {}, trace_id="t2")  # the Turn goes into the private generation
        inside = {turn.trace_id for turn in w.cs.get_history(CHAT, last_n=1000)}
        assert before == {"t1"} and inside == {"t1", "t2"}
        assert store.resolve_anchor(w.loc, before).status is store.AnchorStatus.ANCHOR_UNREADABLE  # published only
        wrong = store.resolve_anchor(w.loc, inside)
        assert wrong.status is store.AnchorStatus.ANCHOR and wrong.record.trace_id == "t2"  # the trap
        # the publish never happens (crash, StaleRevisionError): leaving the staging discards the private copy
    outside = {turn.trace_id for turn in w.cs.get_history(CHAT, last_n=1000)}
    assert outside == {"t1"}
    assert store.resolve_anchor(w.loc, outside).status is store.AnchorStatus.ANCHOR_UNREADABLE


def test_characterization_pre_existing_debt_clear_leaves_the_generation_copies(tmp_path):
    """NOT a contract. ``docs/audits/m2a_conversation/DEBT_CLEAR_GENERATIONS_RETENTION.md`` (PRE-EXISTENT, not repaired
    here): ``clear()`` does not delete ``.generations/<chat>/``, where ``stage_continuity`` copies the history of every
    turn with a reservation. The copies keep personal data of a chat the user reset. The accepted store is NOT among
    them (it is never copied there), so M2A-2 itself leaves nothing behind. When the user authorizes the repair this
    test changes with the decision and with ``tests/test_conversation_store_clear_delta.py``."""
    w = World(store, tmp_path)
    publish_turn_of(w, "t1", query=SENTINEL)
    w.append(make_record("t1"))
    generations = w.cs.base_path / ".generations" / legacy._sanitize_chat_id(CHAT)
    assert generations.is_dir()
    w.cs.clear(CHAT)
    assert w.cs.get_history(CHAT, last_n=1000) == []
    assert not w.path.exists()
    assert generations.is_dir(), "the debt was repaired: update the debt note, the delta pin and this test"
    copies = list(generations.rglob("history.jsonl"))
    assert copies and SENTINEL in copies[0].read_text(encoding="utf-8")  # personal data survives the reset ...
    assert not list(generations.rglob("*accepted*"))  # ... but never the accepted store


# ---- conventions pinned against their owners -----------------------------------------------------------------------------------------------------
def test_the_envelope_suffix_and_the_epoch_file_are_their_owners(tmp_path):
    assert store.ENVELOPE_SUFFIX == legacy.ENVELOPE_SUFFIX
    envelope = tmp_path / f"c{legacy.ENVELOPE_SUFFIX}"
    assert str(store_epoch.epoch_path(envelope)) == f"{envelope}{store.EPOCH_SUFFIX}"
    assert store._read_epoch(envelope) == store_epoch.read_epoch(envelope) == 0
    for _ in range(3):
        store_epoch.advance_epoch(envelope)
        assert store._read_epoch(envelope) == store_epoch.read_epoch(envelope)


def test_a_location_is_validated():
    envelope = Path("/x") / "c.envelopes.jsonl"
    assert store.AcceptedStoreLocation("c", envelope).accepted_path == Path("/x") / "c.accepted.jsonl"
    for chat, path, error in (("", envelope, ValueError), (None, envelope, ValueError),
                              ("c", "/x/c.envelopes.jsonl", TypeError), ("c", Path("/x/c.jsonl"), ValueError),
                              ("c", Path("/x/.envelopes.jsonl"), ValueError), ("c", Path("/x/c.accepted.jsonl"), ValueError)):
        assert isinstance(raised(store.AcceptedStoreLocation, chat, path), error), (chat, path)


def test_for_chat_asks_the_store_for_the_lock_key_it_does_not_recompute_it(tmp_path):
    class Fake:
        def _envelope_path_for(self, chat_id):
            return tmp_path / "elsewhere" / "z.envelopes.jsonl"

    location = store.AcceptedStoreLocation.for_chat(Fake(), "any")
    assert location.envelope_path == tmp_path / "elsewhere" / "z.envelopes.jsonl"
    assert location.accepted_path == tmp_path / "elsewhere" / "z.accepted.jsonl"


# ---- 10. personal data -----------------------------------------------------------------------------------------------------------------------------
@check
def check_no_content_leaks(m, root):
    w = World(m, root)
    logged = io.StringIO()
    handler = logging.StreamHandler(logged)
    logging.getLogger().addHandler(handler)
    previous = logging.getLogger().level
    logging.getLogger().setLevel(logging.DEBUG)
    out, err = io.StringIO(), io.StringIO()
    texts: list[str] = []
    try:
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            record = make_record("t1", actor=SENTINEL, turn_id=f"turno-{SENTINEL}")
            w.append(record)
            epoch = w.epoch()
            for attempt in (lambda: m.append(w.loc, record, epoch=epoch),  # duplicate
                            lambda: m.append(w.loc, make_record("t2", chat_id="x", actor=SENTINEL), epoch=epoch),
                            lambda: m.append(w.loc, make_record("t3", actor=SENTINEL), epoch=epoch + 7)):
                error = raised(attempt)
                texts += [str(error), repr(error), getattr(error, "code", "")]
            for reader_error in (ValueError(SENTINEL), ContractError(f"BAD {SENTINEL}"), KeyError(SENTINEL)):
                def hostile(cls, data, reader_error=reader_error):
                    raise reader_error
                with mock.patch.object(fa.AcceptedTurnRecord, "from_dict", classmethod(hostile)):
                    error = raised(m.append, w.loc, make_record("t4", actor=SENTINEL), epoch=epoch)
                    got = w.read()
                texts += [str(error), repr(error), repr(got), str(got.reason)]
            good = line(record)
            for damaged in (good[:-5], good.replace(b'"t1"', b'"t9"', 1), good + good, b"{" + good):
                w.path.write_bytes(damaged)
                got = w.read()
                texts += [repr(got), str(got.reason), repr(m.anchor_of(got, {"t1"})),
                          repr(raised(w.append, make_record("t5")))]
    finally:
        logging.getLogger().removeHandler(handler)
        logging.getLogger().setLevel(previous)
    assert not any(SENTINEL in text for text in texts), [t for t in texts if SENTINEL in t]
    assert logged.getvalue() == "" and out.getvalue() == "" and err.getvalue() == ""  # nothing logged or printed


MODULE_IMPORTS = frozenset({"__future__", "json", "os", "collections.abc", "dataclasses", "enum", "pathlib",
                            "app.core.foundation_accepted", "app.core.interpretation_contract", "app.core.store_lock"})
DESTRUCTIVE = frozenset({"unlink", "remove", "rename", "replace", "truncate", "rmdir", "rmtree", "write_text",
                         "write_bytes", "touch", "symlink_to", "chmod", "seek"})


def module_problems(source: str) -> list[str]:
    """The append-only and no-logging properties of the module, by AST: a reviewed import list; no way to delete,
    rename, truncate or rewrite; the data file is opened in ``"ab"`` only; no print."""
    out = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            out += [f"import {a.name}" for a in node.names if a.name not in MODULE_IMPORTS]
        elif isinstance(node, ast.ImportFrom) and (node.module or "") not in MODULE_IMPORTS:
            out.append(f"import from {node.module}")
        elif isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id in {"print", "open", "exec", "eval"}:
                out.append(f"call {func.id}() (line {node.lineno})")
            elif isinstance(func, ast.Attribute):
                if func.attr in DESTRUCTIVE:
                    out.append(f"destructive call .{func.attr}() (line {node.lineno})")
                elif func.attr == "open":
                    if isinstance(func.value, ast.Name) and func.value.id == "os":  # the directory fsync
                        ok = len(node.args) == 2 and ast.unparse(node.args[1]) == "os.O_RDONLY"
                    else:
                        ok = len(node.args) == 1 and isinstance(node.args[0], ast.Constant) and node.args[0].value == "ab"
                    if not ok:
                        out.append(f"open() other than 'ab' (line {node.lineno})")
    return out


def test_the_module_is_append_only_and_logs_nothing_by_ast():
    assert module_problems(SOURCE) == []


@pytest.mark.parametrize("old, new", [
    ('open("ab")', 'open("wb")'), ('open("ab")', 'open("r+b")'),
    ("            handle.flush()\n", "            handle.flush()\n            handle.truncate(0)\n"),
    ("            handle.flush()\n", "            handle.flush()\n            location.accepted_path.unlink()\n"),
    ("import json\n", "import json\nimport logging\n"),
    ("import json\n", "import json\nimport shutil\n"),
    ("            handle.flush()\n", "            handle.flush()\n            print(payload)\n"),
    ("            handle.flush()\n", "            handle.flush()\n            location.accepted_path.rename(location.envelope_path)\n"),
])
def test_mutant_the_ast_pin_catches_what_breaks_append_only_or_logs(old, new):
    assert SOURCE.count(old) >= 1
    assert module_problems(SOURCE.replace(old, new, 1)), (old, new)


# ---- the store never enters git (contract §3.7: "nada del almacén va a git") ----------------------------------------------------------------------
GITIGNORE_PATTERN = "data/conversations/*.accepted.jsonl"


def _git_ignores(gitignore: str, path: str, directory: Path) -> bool:
    """Does a repo whose ``.gitignore`` is ``gitignore`` ignore ``path``? (``git check-ignore``; exit 0 = ignored)"""
    directory.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "init", "-q", str(directory)], check=True)
    (directory / ".gitignore").write_text(gitignore, encoding="utf-8")
    done = subprocess.run(["git", "-C", str(directory), "check-ignore", "-q", "--no-index", path], check=False)
    assert done.returncode in (0, 1), done.returncode
    return done.returncode == 0


def test_the_accepted_store_is_ignored_by_git_with_a_narrow_pattern(tmp_path):
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    assert GITIGNORE_PATTERN in gitignore.splitlines()
    assert _git_ignores(gitignore, "data/conversations/st_anon_0123456789ab.accepted.jsonl", tmp_path / "real")
    for path in ("data/conversations/st_anon_0123456789ab.jsonl", "data/conversations/st_anon_0123456789ab.envelopes.jsonl",
                 "data/conversations/st_anon_0123456789ab.pagination.json", "data/other/st_anon.accepted.jsonl",
                 "data/conversations/accepted.jsonl"):
        assert not _git_ignores(gitignore, path, tmp_path / "narrow"), path  # narrow: only the accepted-record store


def test_mutant_without_the_pattern_the_store_is_not_ignored(tmp_path):
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    without = "\n".join(line for line in gitignore.splitlines() if line != GITIGNORE_PATTERN) + "\n"
    assert without != gitignore
    assert not _git_ignores(without, "data/conversations/st_anon_0123456789ab.accepted.jsonl", tmp_path / "mutant")


def test_no_accepted_store_is_tracked_by_git():
    done = subprocess.run(["git", "-C", str(ROOT), "ls-files", "--", "data/conversations/*.accepted.jsonl"],
                          check=True, capture_output=True, text=True)
    assert done.stdout == ""


# ---- Hypothesis: the store never changes what it was given ---------------------------------------------------------------------------------------------------
@st.composite
def st_history(draw):
    count = draw(st.integers(1, 4))
    out = []
    for i in range(count):
        kind = draw(st.sampled_from(["search", "clarify"]))
        # ("buyer", "topic") is canonicalized by the constructor; the two same-slot orders stay two distinct valid records
        conditions = draw(st.sampled_from([(), ("topic",), ("topic", "buyer"), ("buyer", "topic"),
                                           ("topic", "topic2"), ("topic2", "topic")]))
        turn_id = draw(st.text(st.characters(blacklist_categories=("Cs",)), min_size=1, max_size=6).filter(str.strip))
        anchor = f"t{i}" if kind == "search" else (f"t{i - 1}" if i else None)
        out.append(make_record(f"t{i}", kind=kind, conditions=conditions, turn_id=turn_id, anchor=anchor))
    return out


def _fresh_location(directory: str) -> store.AcceptedStoreLocation:
    return store.AcceptedStoreLocation(CHAT, Path(directory) / f"c{legacy.ENVELOPE_SUFFIX}")


@PROPS
@given(st_history())
def test_property_what_is_read_is_exactly_what_was_written(records):
    with tempfile.TemporaryDirectory() as directory:
        location = _fresh_location(directory)
        for record in records:
            store.append(location, record, epoch=0)
        got = store.read(location)
        assert got.status is store.StoreStatus.OK and got.records == tuple(records)
        assert location.accepted_path.read_bytes() == b"".join(line(record) for record in records)
        assert [store.serialize_record(r) for r in got.records] == [store.serialize_record(r) for r in records]


@PROPS
@given(st_history(), st.data())
def test_property_no_single_byte_change_yields_a_readable_store(records, data):
    """No heuristic recovery: whatever one byte of the file becomes, the whole store is CORRUPT (the one thing it can
    never be is a silently different set of records)."""
    with tempfile.TemporaryDirectory() as directory:
        location = _fresh_location(directory)
        content = b"".join(line(record) for record in records)
        index = data.draw(st.integers(0, len(content) - 1))
        replacement = data.draw(st.integers(0, 255).filter(lambda value: value != content[index]))
        location.accepted_path.write_bytes(content[:index] + bytes([replacement]) + content[index + 1:])
        got = store.read(location)
        assert got.status is store.StoreStatus.CORRUPT and got.records == ()


@PROPS
@given(st_history(), st.data())
def test_property_a_cut_inside_a_line_is_never_a_shorter_store(records, data):
    """Cutting the file anywhere but on a line boundary is a truncated tail (a cut ON a boundary is a store that
    simply has fewer records: nothing in an append-only file can tell, and that is a declared limit)."""
    with tempfile.TemporaryDirectory() as directory:
        location = _fresh_location(directory)
        lines = [line(record) for record in records]
        content = b"".join(lines)
        boundaries = {sum(len(item) for item in lines[:i]) for i in range(len(lines) + 1)}
        cut = data.draw(st.integers(1, len(content) - 1).filter(lambda value: value not in boundaries))
        location.accepted_path.write_bytes(content[:cut])
        got = store.read(location)
        assert got.status is store.StoreStatus.CORRUPT and got.reason == "TRUNCATED_TAIL"
