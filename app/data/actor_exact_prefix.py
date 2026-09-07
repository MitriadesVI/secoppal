"""Bounded exact CC prefix evidence, independent of fuzzy retrieval (ADR-094).

Schema 1 uses UTF-8 TEXT and BINARY collation, including the existing UNIQUE
(normalized_alias, document_type, base_number) autoindex. Unicode scalar order
and UTF-8 byte order agree. N2 retains alphanumerics and single ASCII spaces;
its query plus a space therefore always has a successor. No schema migration.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from time import perf_counter

from app.core.actor_identity import IdentityKey
from app.core.actor_name import ActorResolutionCandidate, ActorResolutionResult

# Fixed from BEFORE production + dense synthetic measurements, before AFTER:
# artifacts/audits/actor_exact_personal_prefix_collision_v1/budget_contract.json
EXISTENCE_BUDGET_MS = 5.0
ENUMERATION_BUDGET_MS = 20.0
MAX_CHALLENGERS = 9
ENUMERATION_SQL = (
    "SELECT normalized_alias,raw_alias,raw_aliases_json,document_type,"
    "base_number,occurrences,first_seen,last_seen "
    "FROM actor_aliases INDEXED BY sqlite_autoindex_actor_aliases_1 "
    "WHERE normalized_alias >= ? AND normalized_alias < ? AND document_type = 'CC' "
    "AND base_number != ? ORDER BY normalized_alias,document_type,base_number"
)
EXISTENCE_SQL = ENUMERATION_SQL + " LIMIT 1"
_READ_ERRORS = (OSError, ValueError, TypeError, sqlite3.DatabaseError)
CandidateFactory = Callable[..., ActorResolutionCandidate]


class ExactPrefixCheckStatus:
    ABSENT_CONFIRMED = "ABSENT_CONFIRMED"
    FOUND = "FOUND"
    INCONCLUSIVE = "INCONCLUSIVE"


@dataclass(frozen=True)
class ExactPrefixCheck:
    status: str
    witness: ActorResolutionCandidate | None = None
    reason: str | None = None

    def __post_init__(self):
        if self.status not in {ExactPrefixCheckStatus.ABSENT_CONFIRMED,
                               ExactPrefixCheckStatus.FOUND, ExactPrefixCheckStatus.INCONCLUSIVE}:
            raise ValueError("unknown exact prefix check status")
        if (self.status == ExactPrefixCheckStatus.FOUND) != (self.witness is not None):
            raise ValueError("only FOUND requires a retained challenger")
        if self.status == ExactPrefixCheckStatus.INCONCLUSIVE and not self.reason:
            raise ValueError("INCONCLUSIVE requires a reason")


def lexicographic_successor(value: str) -> str:
    """Smallest exclusive upper prefix bound in SQLite BINARY scalar order.

    Carry across maximum scalar values; skip the surrogate interval, which
    cannot occur in UTF-8 SQLite text or N2. Empty/all-maximum have no bound.
    """
    for offset in range(len(value) - 1, -1, -1):
        scalar = ord(value[offset])
        if scalar < 0x10FFFF:
            successor = scalar + 1
            if 0xD800 <= successor <= 0xDFFF:
                successor = 0xE000
            return value[:offset] + chr(successor)
    raise ValueError("prefix has no finite lexicographic successor")


def prefix_bounds(normalized_query: str) -> tuple[str, str]:
    if not normalized_query:
        raise ValueError("prefix query must not be empty")
    prefix = normalized_query + " "
    return prefix, lexicographic_successor(prefix)


class _ReadDeadline:
    """Cooperative VM interruption plus Python iteration/materialization checks.

    Does not increase SQLite's busy timeout. OS descheduling or a blocking disk
    read cannot be hard-preempted by SQLite's progress callback; the next check
    still refuses to claim successful absence/enumeration after the deadline.
    """
    def __init__(self, connection: sqlite3.Connection, budget_ms: float):
        self.connection = connection
        self.until = perf_counter() + budget_ms / 1000
        self.interrupted = False

    def check(self):
        if perf_counter() >= self.until:
            raise TimeoutError("exact prefix read budget exhausted")

    def __enter__(self):
        self.check()
        self.connection.set_progress_handler(self._progress, 100)
        return self

    def _progress(self):
        self.interrupted = perf_counter() >= self.until
        return int(self.interrupted)

    def __exit__(self, exception_type, exception, traceback):
        self.connection.set_progress_handler(None, 0)
        if self.interrupted and isinstance(exception, sqlite3.DatabaseError):
            raise TimeoutError("exact prefix VM budget exhausted") from exception


def _candidate(row, normalized_query, original, factory, rank):
    identity = IdentityKey(str(row["document_type"]), str(row["base_number"]))
    alias = str(row["normalized_alias"])
    prefix = normalized_query + " "
    if identity.document_type != "CC" or identity == original.identity or not (
        alias.startswith(prefix) and len(alias) > len(prefix)
    ):
        raise ValueError("invalid structural challenger evidence")
    return factory(identity, [row], rank=rank)


def find_prefix_challenger(connection, query, original, factory) -> ExactPrefixCheck:
    """Exclude the original CC before LIMIT 1 and retain presentable evidence."""
    try:
        with _ReadDeadline(connection, EXISTENCE_BUDGET_MS) as deadline:
            cursor = connection.execute(EXISTENCE_SQL, (*prefix_bounds(query), original.identity.base_number))
            try:
                row = cursor.fetchone()
            finally:
                cursor.close()
            deadline.check()
            witness = _candidate(row, query, original, factory, 2) if row is not None else None
            deadline.check()
        return ExactPrefixCheck(ExactPrefixCheckStatus.FOUND, witness) if witness else ExactPrefixCheck(ExactPrefixCheckStatus.ABSENT_CONFIRMED)
    except _READ_ERRORS as error:
        return ExactPrefixCheck(ExactPrefixCheckStatus.INCONCLUSIVE, reason=type(error).__name__)


def enumerate_prefix_challengers(connection, query, original, factory):
    """Stream ordered aliases, dedup identities, then stop at the tenth key.

    The first row for each identity is its minimum normalized_alias. No alias
    LIMIT precedes dedup. Full SQL sorting and candidate materialization are
    inside this phase's budget; partial results never imply exhaustiveness.
    """
    candidates = []
    seen = set()
    with _ReadDeadline(connection, ENUMERATION_BUDGET_MS) as deadline:
        cursor = connection.execute(ENUMERATION_SQL, (*prefix_bounds(query), original.identity.base_number))
        try:
            for row in cursor:
                deadline.check()
                key = (row["document_type"], row["base_number"])
                if key not in seen:
                    candidates.append(_candidate(row, query, original, factory, len(candidates) + 2))
                    seen.add(key)
                    if len(candidates) == MAX_CHALLENGERS + 1:
                        break
            deadline.check()
        finally:
            cursor.close()
    return tuple(candidates)


def resolve_exact_personal_prefix(connection, query, original, factory) -> ActorResolutionResult:
    """Return only authority justified by completed checks and retained witness."""
    try:
        check = find_prefix_challenger(connection, query, original, factory)
    except _READ_ERRORS as error:
        check = ExactPrefixCheck(ExactPrefixCheckStatus.INCONCLUSIVE, reason=type(error).__name__)
    if check.status == ExactPrefixCheckStatus.INCONCLUSIVE:
        return ActorResolutionResult("INCONCLUSIVE", query, method="exact_prefix_check_inconclusive",
                                     reason="PREFIX_EXISTENCE_" + str(check.reason))
    if check.status == ExactPrefixCheckStatus.ABSENT_CONFIRMED:
        return ActorResolutionResult("RESOLVED", query, identity=original.identity, method="exact_unique_alias")
    assert check.witness is not None
    incomplete = False
    reason = None
    try:
        challengers = enumerate_prefix_challengers(connection, query, original, factory)
        # Immutable reads must agree. Missing/replaced witness is incomplete
        # evidence, even if a broken enumerator reports a successful empty set.
        if not challengers or challengers[0] != check.witness:
            raise ValueError("prefix enumeration contradicts retained witness")
        has_more = len(challengers) > MAX_CHALLENGERS
        challengers = challengers[:MAX_CHALLENGERS]
    except _READ_ERRORS as error:
        challengers = (check.witness,)
        has_more = None
        incomplete = True
        reason = "PREFIX_ENUMERATION_" + type(error).__name__
    return ActorResolutionResult("AMBIGUOUS", query, candidates=(original, *challengers),
                                 method="exact_prefix_ambiguous", reason=reason,
                                 candidates_has_more=has_more, enumeration_incomplete=incomplete)
