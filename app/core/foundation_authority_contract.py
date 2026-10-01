"""Contract between the Foundation core and the per-dimension host authorities (spine v5, C0 §5.5).

FROZEN in C1.1: six lanes (D-TIME, D-MONEY, D-STATE/MOD, D-GEO, D-BUYER, D-ACTOR) write against
it, so a change here is a METIS decision (design §10.1), not an implementation detail.

An authority is a PURE function ``resolve(evidence, ctx) -> AuthorityVerdict``.  It never sees the
raw turn: only a ``DimensionEvidence`` whose ``EvidenceWindow`` carries the cited quote plus at
most ``WINDOW_TOKENS`` folded tokens on each side (built by ``foundation_evidence_window``, the
only constructor).  The context outside the quote may only WITHDRAW authority (INV-C0-7); the
instant arrives in ``AuthorityContext`` and the clock is never read (INV-C0-8); a resolved value
is one of the closed ``ExecutableValue`` types and round-trips (INV-C0-9).  The conformance kit
``tests/foundation_authority_kit.py`` checks all of it.

Nothing here is imported by runtime code in C1.1.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from app.core.foundation_authority_values import (  # noqa: F401  (the contract re-exports the union)
    MAX_TOP_N, STATE_CLASSES, ActorValue, BuyerValue, CardinalityValue, Comparator,
    ContractTypeValue, ExecutableValue, GeoValue, ModalityValue, MoneyValue, OrderDirection,
    OrderKey, OrderValue, StateValue, TimeValue, TopicValue, VALUE_TYPES, decode_value,
    is_executable_value,
)
from app.core.foundation_geo_level import GeoLevel
from app.core.interpretation_contract import (
    ContractError, Effect, HostResolution, Polarity, RawSpan, ResolutionDecision,
)
from app.core.semantic_dimensions import SemanticDimension

WINDOW_TOKENS = 4  # the longest closed qualifier chain of M1-H0 («a partir de el año») fits

_TOKEN = re.compile(r"[a-z0-9]+\Z")
_VERSIONED_ID = re.compile(r"[a-z][a-z0-9_-]*@[0-9][0-9A-Za-z.]*\Z")
_RULE = re.compile(r"[A-Za-z][A-Za-z0-9_.:@-]{0,63}\Z")
_LABEL = re.compile(r"[a-z][a-z0-9_]{0,31}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
MAX_RECEIPT_RULES = 32
MAX_RECEIPT_DIGESTS = 16

# Dimensions that have an authority.  ORDER / CARDINALITY join SemanticDimension in C1.2; matching
# by NAME keeps this contract unchanged when they do.  UNSUPPORTED, CONTROL_LANGUAGE and
# RETRIEVAL_INTENT (host-written, H7) never reach an authority.
AUTHORITY_DIMENSION_NAMES = frozenset({
    "TOPIC", "BUYER", "ACTOR", "GEOGRAPHY", "TIME", "VALUE", "STATE", "MODALITY",
    "CONTRACT_TYPE_OR_NATURE", "ORDER", "CARDINALITY",
})

_PER_DIMENSION_CODES = (
    "UNRESOLVED_HOST_DIMENSION", "AMBIGUOUS_HOST_DIMENSION", "UNCITED_QUALIFIER",
    "OPERATOR_EVIDENCE_MISMATCH", "NEGATION_UNSUPPORTED", "AUTHORITY_UNAVAILABLE", "RESIDUE_UNREAD",
)
# Pre-existing VALUE_* codes of foundation_messages.py, kept as aliases (a test pins them to it).
VALUE_ALIASES = frozenset({
    "VALUE_OPERATOR_EVIDENCE_MISMATCH", "VALUE_OPERATOR_BOUNDARY_MISMATCH",
    "VALUE_OPERATOR_UNGROUNDED", "VALUE_OPERATOR_AMBIGUOUS",
})
AUTHORITY_REASONS = frozenset({
    *(f"{code}:{name}" for code in _PER_DIMENSION_CODES for name in AUTHORITY_DIMENSION_NAMES),
    "LEVEL_MISMATCH:GEOGRAPHY", "GENERIC_IDENTITY:BUYER", "GENERIC_IDENTITY:ACTOR",
    *VALUE_ALIASES,
})


def _identifier(value, label: str) -> None:
    if type(value) is not str or not value.strip():
        raise ContractError(f"INVALID_{label}")


def _versioned(value, label: str) -> None:
    if type(value) is not str or not _VERSIONED_ID.fullmatch(value):
        raise ContractError(f"INVALID_{label}")


@dataclass(frozen=True, slots=True)
class EvidenceWindow:
    """The cited quote plus at most ``WINDOW_TOKENS`` folded tokens on each side.

    ``left`` / ``right`` are the tokens immediately before the first and after the last cited
    span, nearest-last / nearest-first.  Nothing else of the turn is in here.  Build it only with
    ``foundation_evidence_window.build_window``.
    """

    spans: tuple[RawSpan, ...]
    quote: str
    left: tuple[str, ...]
    right: tuple[str, ...]

    def __post_init__(self) -> None:
        if (type(self.spans) is not tuple or not self.spans
                or any(type(span) is not RawSpan for span in self.spans)):
            raise ContractError("INVALID_WINDOW_SPANS")
        _identifier(self.quote, "WINDOW_QUOTE")
        if not unicodedata.is_normalized("NFC", self.quote):
            raise ContractError("INVALID_WINDOW_QUOTE")
        for side in (self.left, self.right):
            if (type(side) is not tuple or len(side) > WINDOW_TOKENS
                    or any(type(token) is not str or not _TOKEN.fullmatch(token) for token in side)):
                raise ContractError("INVALID_WINDOW_CONTEXT")

    def truncated(self, left: int, right: int) -> EvidenceWindow:
        """The same window with LESS context (the nearest ``left`` / ``right`` tokens only)."""
        if (type(left) is not int or type(right) is not int
                or not 0 <= left <= len(self.left) or not 0 <= right <= len(self.right)):
            raise ContractError("INVALID_WINDOW_TRUNCATION")
        return EvidenceWindow(self.spans, self.quote, self.left[len(self.left) - left:],
                              self.right[:right])

    def stripped(self) -> EvidenceWindow:
        """The quote alone: the context emptied (the INV-C0-7 reference point)."""
        return self.truncated(0, 0)


@dataclass(frozen=True, slots=True)
class AuthorityContext:
    reference_instant: datetime  # injected, timezone-aware, mandatory even if the dimension ignores it
    datasets: frozenset[str]  # datasets of the requests carrying the condition; empty with UNSPECIFIED

    def __post_init__(self) -> None:
        instant = self.reference_instant
        if (type(instant) is not datetime or instant.tzinfo is None
                or instant.tzinfo.utcoffset(instant) is None):
            raise ContractError("INVALID_REFERENCE_INSTANT")
        if type(self.datasets) is not frozenset:
            raise ContractError("INVALID_CONTEXT_DATASETS")
        for dataset in self.datasets:
            _identifier(dataset, "CONTEXT_DATASET")


@dataclass(frozen=True, slots=True)
class DimensionEvidence:
    condition_id: str
    dimension: SemanticDimension
    operator: str
    polarity: Polarity
    level: GeoLevel | None
    window: EvidenceWindow

    def __post_init__(self) -> None:
        _identifier(self.condition_id, "CONDITION_ID")
        if (type(self.dimension) is not SemanticDimension
                or self.dimension.value not in AUTHORITY_DIMENSION_NAMES):
            raise ContractError("INVALID_AUTHORITY_DIMENSION")
        _identifier(self.operator, "OPERATOR")
        if type(self.polarity) is not Polarity:
            raise ContractError("INVALID_POLARITY")
        if self.level is not None and type(self.level) is not GeoLevel:
            raise ContractError("INVALID_LEVEL")
        if (self.level is not None) != (self.dimension is SemanticDimension.GEOGRAPHY):
            raise ContractError("LEVEL_REQUIRED_EXACTLY_FOR_GEOGRAPHY")
        if type(self.window) is not EvidenceWindow:
            raise ContractError("INVALID_EVIDENCE_WINDOW")


@dataclass(frozen=True, slots=True)
class AuthorityReceipt:
    """What fired, how many candidates, which index versions: identifiers and digests, no free text."""

    rules: tuple[str, ...] = ()
    candidates: int = 0
    digests: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if (type(self.rules) is not tuple or len(self.rules) > MAX_RECEIPT_RULES
                or any(type(rule) is not str or not _RULE.fullmatch(rule) for rule in self.rules)):
            raise ContractError("INVALID_RECEIPT_RULES")
        if type(self.candidates) is not int or self.candidates < 0:
            raise ContractError("INVALID_RECEIPT_CANDIDATES")
        if (type(self.digests) is not tuple or len(self.digests) > MAX_RECEIPT_DIGESTS
                or any(type(item) is not tuple or len(item) != 2
                       or type(item[0]) is not str or not _LABEL.fullmatch(item[0])
                       or type(item[1]) is not str or not _SHA256.fullmatch(item[1])
                       for item in self.digests)):
            raise ContractError("INVALID_RECEIPT_DIGESTS")


@dataclass(frozen=True, slots=True)
class AuthorityVerdict:
    decision: ResolutionDecision
    authority: str  # "<id>@<version>"
    value: ExecutableValue | None = None  # present if and only if RESOLVED
    reason: str | None = None  # present if and only if not RESOLVED; a member of AUTHORITY_REASONS
    effect: Effect = Effect.EQUIVALENT
    policies: tuple[str, ...] = ()  # "<policy>@<v>" applied; required to cover a non-EQUIVALENT effect
    receipt: AuthorityReceipt = AuthorityReceipt()

    def __post_init__(self) -> None:
        if type(self.decision) is not ResolutionDecision:
            raise ContractError("INVALID_VERDICT_DECISION")
        _versioned(self.authority, "VERDICT_AUTHORITY")
        if type(self.effect) is not Effect:
            raise ContractError("INVALID_VERDICT_EFFECT")
        if type(self.policies) is not tuple:
            raise ContractError("INVALID_VERDICT_POLICIES")
        for policy in self.policies:
            _versioned(policy, "VERDICT_POLICY")
        if type(self.receipt) is not AuthorityReceipt:
            raise ContractError("INVALID_VERDICT_RECEIPT")
        if self.decision is ResolutionDecision.RESOLVED:
            if not is_executable_value(self.value):
                raise ContractError("RESOLVED_WITHOUT_VALUE")
            if self.reason is not None:
                raise ContractError("RESOLVED_WITH_REASON")
            if self.effect is not Effect.EQUIVALENT and not self.policies:
                raise ContractError("UNCOVERED_EFFECT")
            return
        if self.value is not None:
            raise ContractError("UNRESOLVED_WITH_VALUE")
        if self.reason not in AUTHORITY_REASONS:
            raise ContractError("REASON_OUTSIDE_CLOSED_SET")
        if self.effect is not Effect.EQUIVALENT:
            raise ContractError("UNRESOLVED_WITH_EFFECT")
        if self.reason.startswith("AUTHORITY_UNAVAILABLE:") and self.decision is not ResolutionDecision.BLOCK:
            raise ContractError("UNAVAILABLE_MUST_BLOCK")

    def to_resolution(self, target_id: str) -> HostResolution:
        """The host record for this verdict; the encoded value rides in ``resolved_ids``."""
        resolved = self.value.encode() if self.decision is ResolutionDecision.RESOLVED else ()
        return HostResolution(target_id, self.decision, self.authority, resolved)


@runtime_checkable
class DimensionAuthority(Protocol):
    authority_id: str
    version: str
    dimensions: frozenset[SemanticDimension]
    operators: frozenset[str]

    def resolve(self, evidence: DimensionEvidence, ctx: AuthorityContext) -> AuthorityVerdict: ...
