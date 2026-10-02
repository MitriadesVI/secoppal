"""M2A-2: the append-only store of ``AcceptedTurnRecord`` (contract §3.8, INV-M2A-03 and INV-M2A-09).

Question it answers: can an already valid accepted record be saved and read back WITHOUT changing its meaning? It
interprets nothing. It never orders, repairs, normalizes or completes: a line is exactly the canonical text of a valid
record (``serialize_record``) or the store is invalid. Canonical order is the producer's and the reducer's (M2A-3).

Layout: one file per chat, ``<safe chat>.accepted.jsonl``, next to the envelope ``<safe chat>.envelopes.jsonl``. One
line per record: ASCII JSON with sorted keys, terminated by ``\\n`` (the terminator is part of the record).

Decisions (each one is pinned by ``tests/test_foundation_accepted_store.py``):

* API shape. The store can import neither the conversation store nor ``re`` (``tests/test_m2a_boundaries.py``). It
  receives ONE injected path, the envelope path of the chat (what ``ConversationStore._envelope_path_for`` returns),
  inside an ``AcceptedStoreLocation``; the store file is DERIVED from it, never injected apart, so the lock key and the
  data file cannot drift. ``AcceptedStoreLocation.for_chat`` takes that path from a conversation store passed in, without
  importing it. The two naming conventions this module repeats (``.envelopes.jsonl``, ``.accepted.jsonl``) and the epoch
  file are pinned equal to their owners by tests, which may import both sides.
* Lock identity. Every operation takes ``conversation_lock(envelope_path)``: the very key ``publish_turn`` and
  ``ConversationStore.clear`` take. A lock on the data file would exclude only by name.
* Reentrancy. ``append``, ``read`` and ``resolve_anchor`` take that lock THEMSELVES and the lock is not reentrant: call
  them outside any ``with conversation_lock(envelope_path)``; inside one they wait for ``lock_timeout`` and raise
  ``ConversationLockTimeout``. The turn lock of ``coordinate_turn`` (``<envelope>.turn``) is a different key and does
  not conflict.
* Epoch. ``append`` REQUIRES the epoch of the reservation the record was prepared under and refuses a stale one under
  the lock (``StaleEpochError``): a record prepared before ``clear()`` and written after it would resurrect personal data
  of a deleted chat. ``clear()`` and ``append`` are mutually exclusive, so the pair is linearizable. Readers are not
  fenced: a turn that read before a reset is invalidated by the publication fence of its reservation, as today.
* Corruption. ANY unreadable line (invalid JSON, duplicate key, unknown key or schema, wrong digest, text that is not
  the canonical form of what it decodes to, a foreign chat, a duplicate ``trace_id``, a blank line) or an unterminated
  tail makes the WHOLE store ``CORRUPT``. Nothing is skipped, nothing is rolled back to the previous line, "the last one
  wins" does not exist. ``append`` refuses a corrupt store (it would bury the evidence); the way out is a reset.
  A failed ``append`` can leave at worst an unterminated tail, which reads as ``CORRUPT``; it is never repaired here.
* Byte identity. The reader decodes a line and re-serializes it; the text must be identical to the line. A reader that
  normalized or reordered would still decode, and then fail this comparison: "identical or invalid, never reordered".
  ``append`` runs the same check on the new line before writing, so a producer bug cannot poison the whole store.
* Anchor. ``resolve_anchor`` follows the ``anchor_trace_id`` of the LAST record to exactly one readable record with an
  ``effective`` whose ``trace_id`` is in the PUBLISHED history. EMPTY store (or a last record that leaves no anchor) is
  ``NO_ANCHOR``; every other failure is ``ANCHOR_UNREADABLE``. The history is whatever ``ConversationStore.get_history``
  returns for the chat: the generation file of a usable envelope, ``.unavailable/...`` under a corrupt one, and the flat
  ``<chat>.jsonl`` only when there is no usable envelope (an EMPTY one). Never read a history file by name: ask
  ``get_history``. It silently omits corrupt lines, which can only close the anchor (a missing trace is not an anchor). A
  record written without its ``Turn`` (crash between both) is therefore not an anchor. Callers pass the WHOLE published
  history, not a window: an anchor older than the window would read as unpublished.
  RESTRICTION FOR M2A-6: inside ``stage_continuity`` (how ``coordinate_turn`` runs every turn with a reservation)
  ``append_turn`` writes into the PRIVATE generation, which ``get_history`` already reads. A snapshot taken after the
  turn's own ``append_turn`` lists a turn that is not published yet, and that never will be if the turn crashes or loses
  the revision race: ``resolve_anchor`` fed with it answers ``ANCHOR`` for that turn. The anchor must be resolved with
  the PUBLISHED history, i.e. the snapshot taken before the turn appends its own Turn (or after the publication), never
  the one taken inside the staging after ``append_turn``. (Characterized in the tests; the store cannot tell.)
* Retention. ``ConversationStore.clear`` deletes this file by literal under the same lock as the history (a declared
  legacy divergence, pinned by AST). The deletion is not done here: this module has no way to delete or truncate.
* Personal data. Nothing here logs, prints or traces; errors and reasons carry fixed codes, never record content.

Guarantee limits (same as ``store_lock``): one filesystem with ``flock``; the store does not decide anything about the
turn (scope, validity window, legacy turns in between): that is the integration step. The record and the ``Turn`` are
two critical sections in that order, not one atomic pair: a crash (or a lost revision race) between them leaves an
ORPHAN record, which is never an anchor; a consumer of any other field of the tail (a ``pending``) must apply the same
published-history test. ``chat_id`` is compared raw, as ``load_envelope`` does (its "foreign chat"). An append-only file
cannot tell a cut exactly on a line boundary from a store with fewer records; only a cut inside a line is detected.
"""

from __future__ import annotations

import json
import os
from collections.abc import Collection
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from app.core.foundation_accepted import AcceptedTurnRecord
from app.core.interpretation_contract import ContractError
from app.core.store_lock import DEFAULT_TIMEOUT_SECONDS, conversation_lock

ENVELOPE_SUFFIX = ".envelopes.jsonl"  # == conversation_store.ENVELOPE_SUFFIX (pinned)
ACCEPTED_SUFFIX = ".accepted.jsonl"  # == the literal of ConversationStore.clear (pinned)
EPOCH_SUFFIX = ".epoch"  # == store_epoch.epoch_path (pinned)


class AcceptedStoreError(RuntimeError):
    """A write was refused. ``code`` is a fixed code, never record content."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class StaleEpochError(AcceptedStoreError):
    """The record was prepared under a reservation that a reset has since invalidated."""

    def __init__(self):
        super().__init__("STALE_EPOCH")


class StoreStatus(str, Enum):
    EMPTY = "EMPTY"
    OK = "OK"
    CORRUPT = "CORRUPT"


class AnchorStatus(str, Enum):
    NO_ANCHOR = "NO_ANCHOR"
    ANCHOR = "ANCHOR"
    ANCHOR_UNREADABLE = "ANCHOR_UNREADABLE"


@dataclass(frozen=True, slots=True)
class StoreRead:
    status: StoreStatus
    records: tuple[AcceptedTurnRecord, ...] = ()
    reason: str | None = None  # a fixed code, only with CORRUPT
    line: int | None = None  # 1-based line of the first fault, only with CORRUPT

    def __post_init__(self) -> None:
        shapes = {
            StoreStatus.OK: bool(self.records) and self.reason is None and self.line is None,
            StoreStatus.EMPTY: not self.records and self.reason is None and self.line is None,
            StoreStatus.CORRUPT: not self.records and self.reason is not None,
        }
        if not shapes.get(self.status, False):
            raise ValueError("INVALID_STORE_READ")


@dataclass(frozen=True, slots=True)
class AnchorRead:
    status: AnchorStatus
    record: AcceptedTurnRecord | None = None  # the anchor, only with ANCHOR
    reason: str | None = None  # a fixed code, with NO_ANCHOR and ANCHOR_UNREADABLE

    def __post_init__(self) -> None:
        anchored = self.status is AnchorStatus.ANCHOR
        if anchored != (self.record is not None) or anchored != (self.reason is None):
            raise ValueError("INVALID_ANCHOR_READ")


@dataclass(frozen=True, slots=True)
class AcceptedStoreLocation:
    """Where the accepted records of ONE chat live. ``envelope_path`` is the lock key (F2) and the only injected path."""

    chat_id: str
    envelope_path: Path

    def __post_init__(self) -> None:
        if type(self.chat_id) is not str or not self.chat_id:
            raise ValueError("INVALID_CHAT_ID")
        if not isinstance(self.envelope_path, Path):
            raise TypeError("envelope_path must be a Path")
        name = self.envelope_path.name
        if not name.endswith(ENVELOPE_SUFFIX) or name == ENVELOPE_SUFFIX:
            raise ValueError("NOT_AN_ENVELOPE_PATH")

    @property
    def accepted_path(self) -> Path:
        stem = self.envelope_path.name.removesuffix(ENVELOPE_SUFFIX)
        return self.envelope_path.with_name(stem + ACCEPTED_SUFFIX)

    @classmethod
    def for_chat(cls, store, chat_id: str) -> AcceptedStoreLocation:
        """The location of ``chat_id`` in a conversation store (duck-typed: it is not imported). The envelope path is
        asked of the store, never recomputed, so the lock key is the one ``publish_turn`` and ``clear`` use."""
        return cls(chat_id, store._envelope_path_for(chat_id))


# ---- canonical line -------------------------------------------------------------------------------------------------
def serialize_record(record: AcceptedTurnRecord) -> str:
    """The canonical text of a record: what ``append`` writes and the only text the reader accepts for it."""
    return json.dumps(record.to_dict(), sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False)


class _DuplicateKey(ValueError):
    pass


def _no_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    keys = [key for key, _ in pairs]
    if len(set(keys)) != len(keys):
        raise _DuplicateKey("DUPLICATE_JSON_KEY")
    return dict(pairs)


def _no_constant(name: str):
    raise ValueError(f"INVALID_JSON_CONSTANT {name}")


def _code_of(error: Exception) -> str:
    """A ``ContractError`` code only if it is a plain fixed code; anything else is named by its type, never its text."""
    if isinstance(error, ContractError):
        code = str(error)
        if code and all(char.isupper() or char.isdigit() or char == "_" for char in code):
            return f"RECORD_REJECTED:{code}"
        return "RECORD_REJECTED"
    return f"READER_ERROR:{type(error).__name__}"


def _parse_line(line: bytes) -> AcceptedTurnRecord | str:
    """The record a line holds, or the fixed code of why it is not one. No line is ever partially accepted."""
    if not line:
        return "BLANK_LINE"
    try:
        text = line.decode("ascii")
    except UnicodeDecodeError:
        return "INVALID_ENCODING"
    try:
        data = json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_constant=_no_constant)
    except _DuplicateKey:
        return "DUPLICATE_JSON_KEY"
    except Exception:  # noqa: BLE001 - any parser failure (incl. RecursionError) is an unreadable line, never a skip
        return "INVALID_JSON"
    try:
        record = AcceptedTurnRecord.from_dict(data)
    except Exception as error:  # noqa: BLE001 - fail closed on ANY reader failure; the code never carries content
        return _code_of(error)
    try:
        identical = serialize_record(record) == text
    except Exception as error:  # noqa: BLE001
        return _code_of(error)
    return record if identical else "NOT_BYTE_IDENTICAL"


# ---- reading ----------------------------------------------------------------------------------------------------------
def _corrupt(reason: str, line: int | None) -> StoreRead:
    return StoreRead(StoreStatus.CORRUPT, (), reason, line)


def _read_file(location: AcceptedStoreLocation) -> StoreRead:
    """Strict read of the whole file. The caller holds the lock."""
    try:
        data = location.accepted_path.read_bytes()
    except FileNotFoundError:
        return StoreRead(StoreStatus.EMPTY)
    except OSError:
        return _corrupt("IO_ERROR", None)
    if not data:
        return StoreRead(StoreStatus.EMPTY)
    lines = data.split(b"\n")
    if lines.pop() != b"":  # the terminator is part of the record: an unterminated tail is a truncated write
        return _corrupt("TRUNCATED_TAIL", len(lines) + 1)
    records: list[AcceptedTurnRecord] = []
    seen: set[str] = set()
    for number, line in enumerate(lines, start=1):
        parsed = _parse_line(line)
        if isinstance(parsed, str):
            return _corrupt(parsed, number)
        if parsed.chat_id != location.chat_id:
            return _corrupt("FOREIGN_CHAT", number)
        if parsed.trace_id in seen:  # detect the cardinality, never hide it (the LIMIT 1 lesson)
            return _corrupt("DUPLICATE_TRACE_ID", number)
        seen.add(parsed.trace_id)
        records.append(parsed)
    return StoreRead(StoreStatus.OK, tuple(records))


def read(location: AcceptedStoreLocation, *, lock_timeout: float = DEFAULT_TIMEOUT_SECONDS) -> StoreRead:
    """Every record of the chat, EMPTY, or CORRUPT. Takes the envelope lock itself (not reentrant)."""
    _check_location(location)
    with conversation_lock(location.envelope_path, timeout=lock_timeout):
        return _read_file(location)


def anchor_of(store_read: StoreRead, published_trace_ids: Collection[str]) -> AnchorRead:
    """Pure: the anchor of an already read store against the published history (contract §3.8, §10.2)."""
    if isinstance(published_trace_ids, (str, bytes)):
        raise TypeError("published_trace_ids must be a collection of trace ids, not a string")
    if store_read.status is StoreStatus.EMPTY:
        return AnchorRead(AnchorStatus.NO_ANCHOR, None, "STORE_EMPTY")
    if store_read.status is StoreStatus.CORRUPT:
        return AnchorRead(AnchorStatus.ANCHOR_UNREADABLE, None, "STORE_CORRUPT")
    pointer = store_read.records[-1].anchor_trace_id
    if pointer is None:
        return AnchorRead(AnchorStatus.NO_ANCHOR, None, "TAIL_LEAVES_NO_ANCHOR")
    matches = [record for record in store_read.records if record.trace_id == pointer]
    if len(matches) != 1:  # 0 or >= 2 resolves nothing; the tail is never replaced by an earlier line
        reason = "POINTER_UNRESOLVED" if not matches else "POINTER_AMBIGUOUS"
        return AnchorRead(AnchorStatus.ANCHOR_UNREADABLE, None, reason)
    target = matches[0]
    if target.effective is None:
        return AnchorRead(AnchorStatus.ANCHOR_UNREADABLE, None, "ANCHOR_WITHOUT_EFFECTIVE")
    if pointer not in frozenset(published_trace_ids):
        return AnchorRead(AnchorStatus.ANCHOR_UNREADABLE, None, "ANCHOR_NOT_PUBLISHED")
    return AnchorRead(AnchorStatus.ANCHOR, target, None)


def resolve_anchor(location: AcceptedStoreLocation, published_trace_ids: Collection[str], *,
                   lock_timeout: float = DEFAULT_TIMEOUT_SECONDS) -> AnchorRead:
    """``anchor_of`` over a fresh strict read. ``published_trace_ids``: the trace ids of the history that
    ``get_history`` returns for the chat, whole, taken BEFORE this call (any race then only closes the anchor) and
    PUBLISHED, not the snapshot of a turn in flight (see RESTRICTION FOR M2A-6 in the module docstring)."""
    return anchor_of(read(location, lock_timeout=lock_timeout), published_trace_ids)


# ---- writing ----------------------------------------------------------------------------------------------------------
def _check_location(location: object) -> None:
    if not isinstance(location, AcceptedStoreLocation):
        raise TypeError("location must be an AcceptedStoreLocation")


def _read_epoch(envelope_path: Path) -> int:
    """The reset epoch of the chat, as ``store_epoch.read_epoch`` reads it (a missing file is epoch 0)."""
    try:
        text = Path(f"{envelope_path}{EPOCH_SUFFIX}").read_text()
    except FileNotFoundError:
        return 0
    try:
        value = int(text)
    except ValueError:
        raise AcceptedStoreError("EPOCH_UNREADABLE") from None
    if value < 0:
        raise AcceptedStoreError("EPOCH_UNREADABLE")
    return value


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def append(location: AcceptedStoreLocation, record: AcceptedTurnRecord, *, epoch: int,
           lock_timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
    """Append ONE record as ONE complete line (flush + fsync), under the envelope lock, or raise and write nothing.

    ``epoch``: the epoch of the reservation the record was prepared under. Refused: a stale epoch, a record of another
    chat, a ``trace_id`` already stored, a record the strict reader would not read back identically, and a store that
    is already CORRUPT. Takes the lock itself (not reentrant)."""
    _check_location(location)
    if not isinstance(record, AcceptedTurnRecord):
        raise TypeError("record must be an AcceptedTurnRecord")
    if type(epoch) is not int or epoch < 0:
        raise TypeError("epoch must be the non-negative int of the reservation")
    if record.chat_id != location.chat_id:
        raise AcceptedStoreError("FOREIGN_CHAT")
    text = serialize_record(record)
    checked = _parse_line(text.encode("ascii"))
    if isinstance(checked, str):  # never write a line the reader would reject: one bad line invalidates the store
        raise AcceptedStoreError(f"RECORD_NOT_READABLE:{checked}")
    payload = text.encode("ascii") + b"\n"
    with conversation_lock(location.envelope_path, timeout=lock_timeout):
        if _read_epoch(location.envelope_path) != epoch:
            raise StaleEpochError()
        current = _read_file(location)
        if current.status is StoreStatus.CORRUPT:
            raise AcceptedStoreError(f"STORE_CORRUPT:{current.reason}")
        if any(stored.trace_id == record.trace_id for stored in current.records):
            raise AcceptedStoreError("DUPLICATE_TRACE_ID")
        created = not location.accepted_path.exists()
        with location.accepted_path.open("ab") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if created:
            _sync_directory(location.accepted_path.parent)


__all__ = [
    "ACCEPTED_SUFFIX", "ENVELOPE_SUFFIX", "EPOCH_SUFFIX", "AcceptedStoreError", "AcceptedStoreLocation",
    "AnchorRead", "AnchorStatus", "StaleEpochError", "StoreRead", "StoreStatus", "anchor_of", "append", "read",
    "resolve_anchor", "serialize_record",
]
