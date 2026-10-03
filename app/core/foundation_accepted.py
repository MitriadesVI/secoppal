"""M2a persisted types: the accepted interpretation of a Foundation turn and its conversational records.

Normative authority: ``docs/decisions/M2A_CONVERSATION_CONTRACT_V1.md`` (§3–§5, §8, §9, §11, §12, §17, annex A).
This module is M2A-1a TYPES/DIRECTIVES: closed, immutable types with a canonical ``to_dict`` and a STRICT reader
(``from_dict``). It has no consumer yet; composition (M2A-3), store (M2A-2), pending protocol (M2A-4) and runtime
(M2A-6) live elsewhere. ``from_acceptance`` (contract §4.1) is M2A-1b: it waits for ``HostViewDecision`` (wave F)
and the host receipt rules/metadata, which do not exist yet; no provisional shape is invented here.

It consumes closed TYPES only: ``foundation_authority_values``/``foundation_geo_level`` to validate and decode
already-accredited ``ExecutableValue``s and ``GeoLevel`` (the one exception to the C1.1 import pin). It never imports
an authority, the registry or the evidence window, and never calls ``resolve``.

Reader contract, for every type: unknown or missing key, unknown schema, wrong JSON type, value outside a closed
list or wrong digest ⇒ ``ContractError``. ``from_dict(x.to_dict()) == x`` and ``to_dict(from_dict(d)) == d`` hold for
every accepted value, and ``to_dict`` is plain JSON (``json.dumps`` without ``default``).

Canonicality: one meaning, one persisted form. Set-like lists (``request_ids``, ``receipt_rules``, a condition's
``policies``, a persisted policy's ``condition_ids``, ``answer_slots``) are sorted and unique; a list in any other
order is REJECTED, never reordered on read. Every persisted datetime is ``timezone.utc`` and every integer is a real
``int`` (``bool`` is not).

M2A-3 (``docs/audits/m2a_conversation/M2A3_CANONICAL_FORM.md``; user decisions 2026-10-02): only an order PROVEN
unobservable is canonicalized. ``conditions`` are grouped by slot in the closed table ``CONDITION_SLOT_ORDER`` and keep
the acceptance order inside a slot (observable: the projection's TOPIC terms); ``policies`` are split (one condition
each) and sorted; a pure conjunction is ``logic = None`` and any tree with OR/NOT is kept as given; ``requests`` keep
their order (the runtime's groups). ``canonical_interpretation`` is the one constructor path of every effective
(``compose`` and the future ``from_acceptance``); the type itself rejects any other form.

Barriers (INV-M2A-04, INV-M2A-24): nothing here translates to ``parsed_params`` or to any legacy input, imports a
legacy reader or a lexicon, or reads the turn's text. Values exist only as ``ExecutableValue.encode()`` tuples.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from app.core.foundation_authority_values import (
    ActorValue, BuyerValue, CardinalityValue, ContractTypeValue, GeoValue, ModalityValue, MoneyValue, OrderValue,
    StateValue, TimeValue, TopicValue, decode_value,
)
from app.core.foundation_authority_values import Comparator as _Comparator
from app.core.foundation_geo_level import GeoLevel
from app.core.foundation_operations import Operation, Subject
from app.core.foundation_operator_table import operator_allowed
from app.core.interpretation_contract import (
    POLARIZED_SLOT_DIMENSIONS, SLOT_DIMENSIONS, ConditionExpression, ContractError, Effect, HostPolicy,
    LogicOperator, Polarity, ValueSide,
)
from app.core.semantic_dimensions import SemanticDimension

RECORD_SCHEMA = "m2a-record/1"
ACCEPTED_SCHEMA = "m2a-accepted/1"
PATCH_SCHEMA = "m2a-patch/1"
PENDING_SCHEMA = "m2a-pending/1"
RESULTSET_SCHEMA = "m2a-resultset/1"

_HEX = frozenset("0123456789abcdef")


# ---- canonical digest (contract §2, §12): the same canonicalization as ``foundation_projection._digest`` ----------
def digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8", errors="surrogatepass")).hexdigest()


# ---- closed vocabularies -------------------------------------------------------------------------------------------
class TurnScope(str, Enum):
    """Contract §7.1 / A.1: every turn gets exactly one."""

    IN = "IN"
    IN_NEW_SEARCH = "IN_NEW_SEARCH"
    IN_PROTOCOL = "IN_PROTOCOL"
    OUT_NO_ANCHOR = "OUT_NO_ANCHOR"
    OUT_ANCHOR_NOT_SEARCH = "OUT_ANCHOR_NOT_SEARCH"
    OUT_OPERATION_CHANGE = "OUT_OPERATION_CHANGE"
    OUT_UNSUPPORTED_PROPOSAL = "OUT_UNSUPPORTED_PROPOSAL"  # §7.1 row 4b (R3, D-6)
    OUT_REFERENT_EXTENSIONAL = "OUT_REFERENT_EXTENSIONAL"
    OUT_RELATIVE_TO_PREVIOUS = "OUT_RELATIVE_TO_PREVIOUS"


class TurnResolution(str, Enum):
    """Contract §5.4."""

    NEW_SEARCH = "NEW_SEARCH"
    DELTA = "DELTA"
    PENDING_ANSWER = "PENDING_ANSWER"
    PROTOCOL_PAGE = "PROTOCOL_PAGE"
    PROTOCOL_EVENT = "PROTOCOL_EVENT"
    CLARIFY = "CLARIFY"
    REASK = "REASK"
    INFORM = "INFORM"
    HANDOFF_LEGACY = "HANDOFF_LEGACY"


class PendingControl(str, Enum):
    """Contract §17: the one and only way a pending question stops existing. Never inferred from an empty list."""

    CANCEL_PENDING = "CANCEL_PENDING"


class PatchOpKind(str, Enum):
    """Contract §5.2: the five semantic operations. ``CANCEL_PENDING`` is control, never an operation."""

    SET = "SET"
    CLEAR = "CLEAR"
    KEEP = "KEEP"
    SET_ORDER = "SET_ORDER"
    REPLACE_UNIVERSE = "REPLACE_UNIVERSE"


class PatchOriginKind(str, Enum):
    TEXT = "TEXT"
    EVENT = "EVENT"


class OriginKind(str, Enum):
    SPAN = "SPAN"
    OPTION_EVENT = "OPTION_EVENT"
    VIEW_ACTION = "VIEW_ACTION"
    HOST_POLICY = "HOST_POLICY"


class UniverseKind(str, Enum):
    RECORDS = "records"
    OPPORTUNITIES = "opportunities"


class Unit(str, Enum):
    PESOS = "PESOS"
    MILES = "MILES"
    MILLONES = "MILLONES"
    MIL_MILLONES = "MIL_MILLONES"
    BILLONES = "BILLONES"
    SMMLV = "SMMLV"


class UnitSource(str, Enum):
    STATED = "STATED"
    SIBLING_IN_RANGE = "SIBLING_IN_RANGE"
    PREVIOUS_ACCEPTED_VALUE = "PREVIOUS_ACCEPTED_VALUE"


class YearSource(str, Enum):
    STATED = "STATED"
    CLOCK_INFERRED = "CLOCK_INFERRED"
    RELATIVE_TO_CLOCK = "RELATIVE_TO_CLOCK"
    PREVIOUS_ACCEPTED_TIME = "PREVIOUS_ACCEPTED_TIME"


class OrderDirectionSource(str, Enum):
    STATED = "STATED"
    DEFAULTED = "DEFAULTED"


class EchoClass(str, Enum):
    """Contract §11 (closed list)."""

    CARRIED = "CARRIED"
    UNIT_INHERITED = "UNIT_INHERITED"
    UNIT_SHARED_IN_RANGE = "UNIT_SHARED_IN_RANGE"
    YEAR_INHERITED = "YEAR_INHERITED"
    YEAR_CLOCK_INFERRED = "YEAR_CLOCK_INFERRED"
    RELATIVE_WINDOW_RESOLVED = "RELATIVE_WINDOW_RESOLVED"
    VALUE_RANGE_REPLACED = "VALUE_RANGE_REPLACED"
    TOPIC_REPLACED = "TOPIC_REPLACED"
    POLARITY_INVERTED = "POLARITY_INVERTED"
    VIEW_DERIVED = "VIEW_DERIVED"
    VIEW_REDERIVED = "VIEW_REDERIVED"
    UNIVERSE_CHANGED = "UNIVERSE_CHANGED"
    TOPN_CARRIED = "TOPN_CARRIED"
    TOPN_CAPPED = "TOPN_CAPPED"
    ORDER_DIRECTION_DEFAULTED = "ORDER_DIRECTION_DEFAULTED"
    CONTEXT_RESET = "CONTEXT_RESET"
    CONTEXT_EXPIRED = "CONTEXT_EXPIRED"


class TotalQuality(str, Enum):
    EXACT = "exact"
    LOWER_BOUND = "lower_bound"
    NOT_APPLICABLE = "not_applicable"


# Contract annex A.3 (amended R3/R3b/R3c). Parameterized codes: ``UNRESOLVED_VALUE:<DIM>``, ``OPERATION_CHANGE:<op>``,
# ``UNSUPPORTED_PROPOSAL:<motivo>``, ``ANCHOR_NOT_SEARCH:<op>``.
CLARIFY_CODES = frozenset({
    "CONTEXT_EXPIRED_CONFIRM", "BUYER_OR_PLACE_AFTER_ACTOR", "REGION_NOT_IN_LIST", "SMMLV_YEAR_AMBIGUOUS",
    "RELATIVE_TO_PREVIOUS_VALUE", "UNIT_AMBIGUOUS", "MONTH_YEAR_AMBIGUOUS", "EMPTY_SEARCH_AFTER_DELTA",
    "STATE_NOT_IN_UNIVERSE", "TERRITORY_VS_CARRIED_BUYER", "NO_ANCHOR", "ANCHOR_UNREADABLE",
    "TARGET_NOT_ADDRESSABLE", "VALUE_WITHOUT_COMPARATOR", "DELTA_SLOT_CONFLICT", "KEEP_ON_EMPTY_SLOT",
    "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER",
})
REASK_CODES = frozenset({"OPTION_NOT_CHOSEN", "ANSWER_EDITS_OTHER_SLOT", "PENDING_UNANSWERED", "ANSWER_UNRESOLVED"})
INFORM_CODES = frozenset({"CLEAR_ON_EMPTY_SLOT", "REFERENT_EXTENSIONAL", "STALE_OPTION"})
# A.3 «INFORM con un código de CLARIFY» (R3b, U20): the only CLARIFY codes also valid with INFORM (their pending may be
# impossible, §8.4; or §6 step 3b gathers several causes; or H1). Plus ``UNRESOLVED_VALUE:<DIM>``. Never
# ``CONTEXT_EXPIRED_CONFIRM``, ``NO_ANCHOR`` nor ``ANCHOR_UNREADABLE``: their pending is always possible.
INFORM_CAPABLE_CLARIFY_CODES = frozenset({
    "DELTA_SLOT_CONFLICT", "TARGET_NOT_ADDRESSABLE", "KEEP_ON_EMPTY_SLOT", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER",
    "SMMLV_YEAR_AMBIGUOUS", "RELATIVE_TO_PREVIOUS_VALUE", "BUYER_OR_PLACE_AFTER_ACTOR", "TERRITORY_VS_CARRIED_BUYER",
    "REGION_NOT_IN_LIST", "STATE_NOT_IN_UNIVERSE", "UNIT_AMBIGUOUS", "MONTH_YEAR_AMBIGUOUS",
    "EMPTY_SEARCH_AFTER_DELTA", "VALUE_WITHOUT_COMPARATOR",
})
# ``<DIM>`` (A.3, R3-3): the dimension keys of the §4.2 slots — the eleven slot dimensions and UNIVERSE; never a
# dimension that is not a slot. ``<motivo>`` (§7.1 row 4b, D-6): its closed list.
DIMENSION_KEYS = frozenset({dimension.value for dimension in SLOT_DIMENSIONS} | {"UNIVERSE"})
UNSUPPORTED_PROPOSAL_MOTIVES = frozenset({
    "MULTI_REQUEST", "UNSUPPORTED_CONDITION", "UNSUPPORTED_SOURCE", "INVALID_PROPOSAL", "NO_ACTION",
})


def reason_code_allowed(code: str, resolution: TurnResolution) -> bool:
    """Whether ``code`` belongs to the closed annex A.3 list of this resolution."""
    if type(code) is not str:
        return False
    head, sep, tail = code.partition(":")
    unresolved = sep == ":" and head == "UNRESOLVED_VALUE" and tail in DIMENSION_KEYS
    if resolution is TurnResolution.CLARIFY:
        return code in CLARIFY_CODES or unresolved
    if resolution is TurnResolution.REASK:
        return code in REASK_CODES
    if resolution is TurnResolution.INFORM:
        return (code in INFORM_CODES or code in INFORM_CAPABLE_CLARIFY_CODES or unresolved
                or (sep == ":" and head == "OPERATION_CHANGE" and tail in {op.value for op in Operation})
                or (sep == ":" and head == "UNSUPPORTED_PROPOSAL" and tail in UNSUPPORTED_PROPOSAL_MOTIVES))
    if resolution is TurnResolution.HANDOFF_LEGACY:
        return sep == ":" and head == "ANCHOR_NOT_SEARCH" and tail in {op.value for op in Operation}
    return False


# Receipt rules with host effect (ADR-103 §4.3 ``HOST_SIGNIFICANT_RULES`` + enm. 5 ``time.relative_to_previous``;
# contract A.7). ``unit.<escala>`` and ``buyer.territory.<código>`` are the two parameterized families.
FIXED_RECEIPT_RULES = frozenset({
    "buyer.exact", "actor.authoritative", "geo.exact", "reanchored", "topn.capped", "unit.sibling", "unit.inherited",
    "unit.smmlv", "year.from_time", "year.inferred", "year.inherited", "order.direction_defaulted", "buyer.national",
    "time.relative_to_previous",
})


def receipt_rule_allowed(rule: str) -> bool:
    if type(rule) is not str:
        return False
    if rule in FIXED_RECEIPT_RULES:
        return True
    if rule.startswith("unit."):
        return rule[len("unit."):] in {unit.value.lower() for unit in Unit}
    if rule.startswith("buyer.territory."):
        code = rule[len("buyer.territory."):]
        return code.isascii() and code.isdigit() and len(code) in (2, 5)
    return False


# The typed value each slot dimension carries (contract §2).
VALUE_TYPE_OF = {
    SemanticDimension.TOPIC: TopicValue, SemanticDimension.BUYER: BuyerValue, SemanticDimension.ACTOR: ActorValue,
    SemanticDimension.GEOGRAPHY: GeoValue, SemanticDimension.TIME: TimeValue, SemanticDimension.VALUE: MoneyValue,
    SemanticDimension.STATE: StateValue, SemanticDimension.MODALITY: ModalityValue,
    SemanticDimension.CONTRACT_TYPE_OR_NATURE: ContractTypeValue, SemanticDimension.ORDER: OrderValue,
    SemanticDimension.CARDINALITY: CardinalityValue,
}
_LOWER = frozenset({_Comparator.GT, _Comparator.GTE})


# ---- strict reader helpers ----------------------------------------------------------------------------------------
def _fail(code: str):
    raise ContractError(code)


def _keys(data, names: tuple[str, ...], label: str) -> dict:
    if type(data) is not dict:
        _fail(f"INVALID_{label}")
    if set(data) != set(names):
        _fail(f"{label}_KEYS_MISMATCH")
    return data


def _schema(data, schema: str, label: str) -> None:
    if type(data) is not dict:
        _fail(f"INVALID_{label}")
    if data.get("schema") != schema:
        _fail(f"UNSUPPORTED_{label}_SCHEMA")


def _list(value, label: str) -> list:
    if type(value) is not list:
        _fail(f"INVALID_{label}")
    return value


def _opt(value, reader):
    return None if value is None else reader(value)


def _enum_of(kind: type, value, label: str):
    if type(value) is not str:
        _fail(f"INVALID_{label}")
    try:
        return kind(value)
    except ValueError:
        _fail(f"INVALID_{label}")


def _text(value, label: str) -> None:
    if type(value) is not str or not value.strip():
        _fail(f"INVALID_{label}")


def _opt_text(value, label: str) -> None:
    if value is not None:
        _text(value, label)


def _sha(value, label: str) -> None:
    if type(value) is not str or len(value) != 64 or not set(value) <= _HEX:
        _fail(f"INVALID_{label}")


def _texts(values, label: str, *, sorted_unique: bool = False, empty: bool = True) -> None:
    if type(values) is not tuple or any(type(item) is not str or not item.strip() for item in values):
        _fail(f"INVALID_{label}")
    if not empty and not values:
        _fail(f"EMPTY_{label}")
    if len(set(values)) != len(values):
        _fail(f"DUPLICATE_{label}")
    if sorted_unique and list(values) != sorted(values):
        _fail(f"UNSORTED_{label}")


def _int(value, label: str, low: int) -> None:
    if type(value) is not int or value < low:
        _fail(f"INVALID_{label}")


def _flag(value, label: str) -> None:
    if type(value) is not bool:
        _fail(f"INVALID_{label}")


def _instance(value, kind: type, label: str) -> None:
    if type(value) is not kind:
        _fail(f"INVALID_{label}")


def _records(values, kind: type, label: str, *, empty: bool = True) -> None:
    if type(values) is not tuple or any(type(item) is not kind for item in values):
        _fail(f"INVALID_{label}")
    if not empty and not values:
        _fail(f"EMPTY_{label}")


def _utc(value, label: str) -> None:
    if type(value) is not datetime or value.tzinfo is not timezone.utc:
        _fail(f"INVALID_{label}")


def _read_utc(value, label: str) -> datetime:
    if type(value) is not str:
        _fail(f"INVALID_{label}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        _fail(f"INVALID_{label}")
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        _fail(f"INVALID_{label}")
    parsed = parsed.replace(tzinfo=timezone.utc)
    if parsed.isoformat() != value:
        _fail(f"NONCANONICAL_{label}")
    return parsed


def _read_texts(value, label: str) -> tuple[str, ...]:
    items = _list(value, label)
    if any(type(item) is not str for item in items):
        _fail(f"INVALID_{label}")
    return tuple(items)


def _read_int(value, label: str) -> int:
    if type(value) is not int:
        _fail(f"INVALID_{label}")
    return value


def _canonical_decimal(text: str) -> bool:
    """A plain non-negative decimal: digits, at most one dot, no leading zero but «0», no trailing dot."""
    if type(text) is not str or not text or not text.isascii():
        return False
    whole, dot, frac = text.partition(".")
    if not whole.isdigit() or (dot and (not frac or not frac.isdigit())):
        return False
    return whole == "0" or not whole.startswith("0")


# ---- Slot (contract §4.2) -----------------------------------------------------------------------------------------
_UNIVERSE_KEY = "UNIVERSE"


@dataclass(frozen=True, slots=True)
class Slot:
    """The addressable unit of a patch. ``dimension is None`` is the UNIVERSE slot.

    A polarized dimension (TOPIC, BUYER, ...) with ``polarity is None`` and VALUE with ``side is None`` are
    dimension-wide: only ``CLEAR`` may address them (§5.1). Serialized as a canonical key: ``TOPIC/POSITIVE``,
    ``VALUE/lower``, ``VALUE``, ``ORDER``, ``UNIVERSE``.
    """

    dimension: SemanticDimension | None
    polarity: Polarity | None = None
    side: ValueSide | None = None

    def __post_init__(self) -> None:
        if self.polarity is not None and type(self.polarity) is not Polarity:
            _fail("INVALID_SLOT_POLARITY")
        if self.side is not None and type(self.side) is not ValueSide:
            _fail("INVALID_SLOT_SIDE")
        if self.dimension is None:
            if self.polarity is not None or self.side is not None:
                _fail("INVALID_UNIVERSE_SLOT")
            return
        if type(self.dimension) is not SemanticDimension or self.dimension not in SLOT_DIMENSIONS:
            _fail("DIMENSION_NOT_A_SLOT")
        if self.dimension in POLARIZED_SLOT_DIMENSIONS:
            if self.side is not None:
                _fail("SIDE_OUTSIDE_VALUE")
        elif self.dimension is SemanticDimension.VALUE:
            if self.polarity is not None:
                _fail("VALUE_SLOT_HAS_POLARITY")
        elif self.polarity is not None or self.side is not None:
            _fail("UNPOLARIZED_SLOT_HAS_QUALIFIER")

    @property
    def is_universe(self) -> bool:
        return self.dimension is None

    @property
    def dimension_wide(self) -> bool:
        return ((self.dimension in POLARIZED_SLOT_DIMENSIONS and self.polarity is None)
                or (self.dimension is SemanticDimension.VALUE and self.side is None))

    @property
    def key(self) -> str:
        if self.dimension is None:
            return _UNIVERSE_KEY
        qualifier = self.polarity.value if self.polarity is not None else (
            self.side.value if self.side is not None else None)
        return self.dimension.value if qualifier is None else f"{self.dimension.value}/{qualifier}"

    def to_dict(self) -> str:
        return self.key

    @classmethod
    def from_dict(cls, data) -> Slot:
        if type(data) is not str:
            _fail("INVALID_SLOT")
        if data == _UNIVERSE_KEY:
            return cls(None)
        head, sep, tail = data.partition("/")
        dimension = _enum_of(SemanticDimension, head, "SLOT")
        try:
            if not sep:
                slot = cls(dimension)
            elif dimension in POLARIZED_SLOT_DIMENSIONS:
                slot = cls(dimension, Polarity(tail))
            else:
                slot = cls(dimension, side=ValueSide(tail))
        except ValueError:
            _fail("INVALID_SLOT")
        if slot.key != data:
            _fail("NONCANONICAL_SLOT")
        return slot


UNIVERSE_SLOT = Slot(None)

_POS, _NEG = Polarity.POSITIVE, Polarity.NEGATIVE
# The canonical order of an interpretation's conditions BY SLOT (user decision 2026-10-02, D-CANON-1): closed, explicit
# and in the order contract §4.2 lists the slots; never alphabetical. Within one slot the acceptance order is kept: it
# is observable (TOPIC/POSITIVE terms change the soql; M2A3_CANONICAL_FORM.md). UNIVERSE is the request-level slot of
# §4.2: no condition occupies it (``slot_of`` never returns it), so it has no rank here.
CONDITION_SLOT_ORDER = (
    Slot(SemanticDimension.TOPIC, _POS), Slot(SemanticDimension.TOPIC, _NEG),
    Slot(SemanticDimension.BUYER, _POS), Slot(SemanticDimension.BUYER, _NEG),
    Slot(SemanticDimension.ACTOR, _POS), Slot(SemanticDimension.ACTOR, _NEG),
    Slot(SemanticDimension.GEOGRAPHY, _POS), Slot(SemanticDimension.GEOGRAPHY, _NEG),
    Slot(SemanticDimension.TIME, _POS), Slot(SemanticDimension.TIME, _NEG),
    Slot(SemanticDimension.STATE, _POS), Slot(SemanticDimension.STATE, _NEG),
    Slot(SemanticDimension.MODALITY, _POS), Slot(SemanticDimension.MODALITY, _NEG),
    Slot(SemanticDimension.CONTRACT_TYPE_OR_NATURE, _POS), Slot(SemanticDimension.CONTRACT_TYPE_OR_NATURE, _NEG),
    Slot(SemanticDimension.VALUE, side=ValueSide.LOWER), Slot(SemanticDimension.VALUE, side=ValueSide.UPPER),
    Slot(SemanticDimension.ORDER),
    Slot(SemanticDimension.CARDINALITY),
)
_SLOT_RANK = {slot: rank for rank, slot in enumerate(CONDITION_SLOT_ORDER)}


def slot_of(dimension: SemanticDimension, value: tuple[str, ...]) -> Slot:
    """The slot a typed value occupies: VALUE by its comparator, polarized dimensions by ``excluded``."""
    decoded = decode_value(value)
    expected = VALUE_TYPE_OF.get(dimension)
    if expected is None or type(decoded) is not expected:
        _fail("VALUE_TYPE_OUTSIDE_DIMENSION")
    if dimension is SemanticDimension.VALUE:
        return Slot(dimension, side=ValueSide.LOWER if decoded.comparator in _LOWER else ValueSide.UPPER)
    if dimension in POLARIZED_SLOT_DIMENSIONS:
        return Slot(dimension, Polarity.NEGATIVE if decoded.excluded else Polarity.POSITIVE)
    return Slot(dimension)


# ---- ConditionOrigin / Provenance (contract §3.2) -----------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ConditionOrigin:
    kind: OriginKind
    trace_id: str
    raw_sha256: str | None = None
    span: tuple[int, int] | None = None
    event_id: str | None = None

    def __post_init__(self) -> None:
        _instance(self.kind, OriginKind, "ORIGIN_KIND")
        _text(self.trace_id, "ORIGIN_TRACE_ID")
        if self.kind is OriginKind.SPAN:
            _sha(self.raw_sha256, "ORIGIN_RAW_SHA256")
            if (type(self.span) is not tuple or len(self.span) != 2
                    or any(type(bound) is not int for bound in self.span)
                    or self.span[0] < 0 or self.span[1] <= self.span[0]):
                _fail("INVALID_ORIGIN_SPAN")
            if self.event_id is not None:
                _fail("SPAN_ORIGIN_HAS_EVENT")
        elif self.kind in (OriginKind.OPTION_EVENT, OriginKind.VIEW_ACTION):
            _text(self.event_id, "ORIGIN_EVENT_ID")
            if self.raw_sha256 is not None or self.span is not None:
                _fail("EVENT_ORIGIN_HAS_SPAN")
        elif (self.raw_sha256, self.span, self.event_id) != (None, None, None):
            _fail("POLICY_ORIGIN_HAS_EVIDENCE")

    def to_dict(self) -> dict:
        return {"kind": self.kind.value, "trace_id": self.trace_id, "raw_sha256": self.raw_sha256,
                "span": None if self.span is None else list(self.span), "event_id": self.event_id}

    @classmethod
    def from_dict(cls, data) -> ConditionOrigin:
        data = _keys(data, ("kind", "trace_id", "raw_sha256", "span", "event_id"), "ORIGIN")
        span = data["span"]
        if span is not None:
            span = tuple(_list(span, "ORIGIN_SPAN"))
        return cls(_enum_of(OriginKind, data["kind"], "ORIGIN_KIND"), data["trace_id"], data["raw_sha256"], span,
                   data["event_id"])


_PROVENANCE_FIELDS = ("unit", "source_amount", "unit_source", "year", "year_source", "buyer_scope",
                      "order_direction_source", "inherited_from")


@dataclass(frozen=True, slots=True)
class Provenance:
    """Closed facts about where a condition's value came from; never free text."""

    unit: Unit | None = None
    source_amount: str | None = None
    unit_source: UnitSource | None = None
    year: int | None = None
    year_source: YearSource | None = None
    buyer_scope: str | None = None  # a DIVIPOLA territory code, or "NATIONAL"
    order_direction_source: OrderDirectionSource | None = None
    inherited_from: str | None = None

    def __post_init__(self) -> None:
        for name, kind in (("unit", Unit), ("unit_source", UnitSource), ("year_source", YearSource),
                           ("order_direction_source", OrderDirectionSource)):
            value = getattr(self, name)
            if value is not None:
                _instance(value, kind, f"PROVENANCE_{name.upper()}")
        if (self.unit is None) != (self.unit_source is None):
            _fail("UNIT_WITHOUT_SOURCE")
        if (self.year is None) != (self.year_source is None):
            _fail("YEAR_WITHOUT_SOURCE")
        if self.year is not None:
            _int(self.year, "PROVENANCE_YEAR", 1)
        if self.source_amount is not None and not _canonical_decimal(self.source_amount):
            _fail("INVALID_PROVENANCE_SOURCE_AMOUNT")
        if self.source_amount is not None and self.unit is None:
            _fail("SOURCE_AMOUNT_WITHOUT_UNIT")
        if self.unit is Unit.SMMLV and (self.source_amount is None or self.year is None):
            _fail("SMMLV_WITHOUT_AMOUNT_OR_YEAR")  # §15.5: recalculated from source_amount, never from COP
        if self.buyer_scope is not None and self.buyer_scope != "NATIONAL" and not (
                type(self.buyer_scope) is str and self.buyer_scope.isascii() and self.buyer_scope.isdigit()
                and len(self.buyer_scope) in (2, 5)):
            _fail("INVALID_PROVENANCE_BUYER_SCOPE")
        _opt_text(self.inherited_from, "PROVENANCE_INHERITED_FROM")
        inherited = (self.unit_source is UnitSource.PREVIOUS_ACCEPTED_VALUE
                     or self.year_source is YearSource.PREVIOUS_ACCEPTED_TIME)
        if inherited != (self.inherited_from is not None):
            _fail("INHERITANCE_WITHOUT_ORIGIN")

    def check_dimension(self, dimension: SemanticDimension) -> None:
        if self.unit is not None and dimension is not SemanticDimension.VALUE:
            _fail("UNIT_OUTSIDE_VALUE")
        if self.year is not None and dimension not in (SemanticDimension.VALUE, SemanticDimension.TIME):
            _fail("YEAR_OUTSIDE_VALUE_OR_TIME")
        if self.buyer_scope is not None and dimension is not SemanticDimension.BUYER:
            _fail("BUYER_SCOPE_OUTSIDE_BUYER")
        if self.order_direction_source is not None and dimension is not SemanticDimension.ORDER:
            _fail("ORDER_SOURCE_OUTSIDE_ORDER")

    def to_dict(self) -> dict:
        return {name: (value.value if isinstance(value, Enum) else value)
                for name in _PROVENANCE_FIELDS for value in (getattr(self, name),)}

    @classmethod
    def from_dict(cls, data) -> Provenance:
        data = _keys(data, _PROVENANCE_FIELDS, "PROVENANCE")
        year = data["year"]
        if year is not None:
            _read_int(year, "PROVENANCE_YEAR")
        return cls(
            _opt(data["unit"], lambda v: _enum_of(Unit, v, "PROVENANCE_UNIT")),
            data["source_amount"],
            _opt(data["unit_source"], lambda v: _enum_of(UnitSource, v, "PROVENANCE_UNIT_SOURCE")),
            year,
            _opt(data["year_source"], lambda v: _enum_of(YearSource, v, "PROVENANCE_YEAR_SOURCE")),
            data["buyer_scope"],
            _opt(data["order_direction_source"],
                 lambda v: _enum_of(OrderDirectionSource, v, "PROVENANCE_ORDER_SOURCE")),
            data["inherited_from"],
        )


# ---- IR v2 records reused as-is (ConditionExpression, HostPolicy): canonical dict + strict reader ----------------
def expression_to_dict(node: ConditionExpression) -> dict:
    return {"operator": node.operator.value, "condition_id": node.condition_id,
            "children": [expression_to_dict(child) for child in node.children]}


def expression_from_dict(data) -> ConditionExpression:
    data = _keys(data, ("operator", "condition_id", "children"), "EXPRESSION")
    children = tuple(expression_from_dict(child) for child in _list(data["children"], "EXPRESSION_CHILDREN"))
    return ConditionExpression(_enum_of(LogicOperator, data["operator"], "EXPRESSION_OPERATOR"),
                               data["condition_id"], children)


def policy_to_dict(policy: HostPolicy) -> dict:
    return {"policy_id": policy.policy_id, "version": policy.version,
            "condition_ids": list(policy.condition_ids), "effect": policy.effect.value}


def _canonical_policy(policy: HostPolicy) -> HostPolicy:
    """A ``HostPolicy`` persisted here is ONE application of a policy to ONE condition: the split form ``main`` emits
    (every ``HostPolicy`` of ``foundation_admission`` cites one condition). Independent applications are never merged
    by projection equivalence; a genuinely joint policy would need an express declaration by its owner and none exists,
    so it is refused (user decision 2026-10-02, D-CANON-2). The IR v2 type itself is not narrowed; only its persisted
    form inside M2a is."""
    if len(policy.condition_ids) != 1:
        _fail("MULTI_CONDITION_POLICY")
    return policy


def policy_from_dict(data) -> HostPolicy:
    data = _keys(data, ("policy_id", "version", "condition_ids", "effect"), "POLICY")
    return _canonical_policy(HostPolicy(
        data["policy_id"], data["version"], _read_texts(data["condition_ids"], "POLICY_CONDITION_IDS"),
        _enum_of(Effect, data["effect"], "POLICY_EFFECT")))


# ---- canonical form shared by every constructor of an effective (M2A-3; M2A3_CANONICAL_FORM.md) --------------------
def _policy_key(policy: HostPolicy) -> tuple:
    return policy.policy_id, policy.version, policy.condition_ids, policy.effect.value


def _check_policies(policies: tuple[HostPolicy, ...], condition_ids: set[str]) -> None:
    """An interpretation's ``HostPolicy`` list (contract §3.2, §6.6), the way the projection reads it.

    Every cited condition exists (a ``CLEAR`` never leaves a dangling policy). An application (policy ref, condition)
    appears once (``(p1, p1)`` included): the projection admits exactly one policy per condition and ref. One ref, one
    effect: the projection maps ``policy_id@version`` to ONE effect, so two effects would make the persisted order
    observable in ``plan_hash``. With those, the list is set-like and is sorted.
    """
    effects: dict[str, Effect] = {}
    cited: set[tuple[str, str]] = set()
    for policy in policies:
        if not set(policy.condition_ids) <= condition_ids:
            _fail("FOREIGN_POLICY_CONDITION")
        ref = f"{policy.policy_id}@{policy.version}"
        if effects.setdefault(ref, policy.effect) is not policy.effect:
            _fail("CONFLICTING_POLICY_EFFECT")
        for condition_id in policy.condition_ids:
            if (ref, condition_id) in cited:
                _fail("DUPLICATE_POLICY")
            cited.add((ref, condition_id))
    if list(policies) != sorted(policies, key=_policy_key):
        _fail("UNSORTED_POLICIES")


def canonical_policies(policies: tuple[HostPolicy, ...]) -> tuple[HostPolicy, ...]:
    """The one order of a set-like policy list. Nothing is merged or dropped: a duplicate is refused by the type."""
    _records(policies, HostPolicy, "POLICIES")
    return tuple(sorted(policies, key=_policy_key))


def _slot_rank(slot: Slot) -> int:
    rank = _SLOT_RANK.get(slot)
    if rank is None:
        _fail("SLOT_WITHOUT_RANK")
    return rank


def canonical_conditions(conditions: tuple[AcceptedCondition, ...]) -> tuple[AcceptedCondition, ...]:
    """Conditions grouped by slot in ``CONDITION_SLOT_ORDER``; inside a slot, the order given (stable: never sorted)."""
    _records(conditions, AcceptedCondition, "CONDITIONS")
    return tuple(sorted(conditions, key=lambda condition: _slot_rank(condition.slot)))


def _conjunction_only(node: ConditionExpression) -> bool:
    return node.operator is LogicOperator.ATOM or (
        node.operator is LogicOperator.AND and all(_conjunction_only(child) for child in node.children))


def canonical_logic(logic: ConditionExpression | None) -> ConditionExpression | None:
    """Contract §3.2: a pure conjunction is ``None`` (the AND of all). Any tree with OR/NOT is kept as given, its
    structure and order untouched: nothing executes it, so «same meaning ⇒ same bytes» is NOT guaranteed inside
    OR/NOT until a consumer proves which rewrites are safe (limited guarantee, user decision 2026-10-02)."""
    if logic is None:
        return None
    _instance(logic, ConditionExpression, "LOGIC")
    return None if _conjunction_only(logic) else logic


# ---- AcceptedRequest / AcceptedCondition / AcceptedInterpretation (contract §3.2, §4) ----------------------------
_REQUEST_FIELDS = ("request_id", "scope_id", "subject", "universe_kind", "universe_origin", "view", "dataset")


@dataclass(frozen=True, slots=True)
class AcceptedRequest:
    """``view`` is the wave-F ``HostViewDecision``. That type does not exist in ``main`` yet (contract §14: M2a
    consumes it, never defines it), so ``view`` only admits ``None`` and ``subject = unspecified`` (which requires a
    view) is not representable: fail-closed until wave F lands."""

    request_id: str
    scope_id: str
    subject: Subject
    universe_kind: UniverseKind | None
    universe_origin: ConditionOrigin | None
    view: None
    dataset: str

    def __post_init__(self) -> None:
        _text(self.request_id, "REQUEST_ID")
        _text(self.scope_id, "SCOPE_ID")
        _instance(self.subject, Subject, "SUBJECT")
        if self.view is not None:
            _fail("VIEW_TYPE_UNAVAILABLE")
        if self.subject is Subject.UNSPECIFIED:
            _fail("VIEW_REQUIRED")
        _instance(self.universe_kind, UniverseKind, "UNIVERSE_KIND")
        _instance(self.universe_origin, ConditionOrigin, "UNIVERSE_ORIGIN")
        _text(self.dataset, "DATASET")

    def to_dict(self) -> dict:
        return {"request_id": self.request_id, "scope_id": self.scope_id, "subject": self.subject.value,
                "universe_kind": None if self.universe_kind is None else self.universe_kind.value,
                "universe_origin": None if self.universe_origin is None else self.universe_origin.to_dict(),
                "view": None, "dataset": self.dataset}

    @classmethod
    def from_dict(cls, data) -> AcceptedRequest:
        data = _keys(data, _REQUEST_FIELDS, "REQUEST")
        return cls(data["request_id"], data["scope_id"], _enum_of(Subject, data["subject"], "SUBJECT"),
                   _opt(data["universe_kind"], lambda v: _enum_of(UniverseKind, v, "UNIVERSE_KIND")),
                   _opt(data["universe_origin"], ConditionOrigin.from_dict), data["view"], data["dataset"])


# VALUE: the comparator of the ``MoneyValue`` already carries the executable direction (and absorbs any negation:
# «no más de X» is ``<= X``), so the condition's operator must name that same comparator and its polarity is POSITIVE.
VALUE_OPERATOR_OF = {_Comparator.GT: "greater_than", _Comparator.GTE: "greater_or_equal",
                     _Comparator.LT: "less_than", _Comparator.LTE: "less_or_equal"}

_CONDITION_FIELDS = ("condition_id", "request_ids", "slot", "dimension", "operator", "polarity", "level", "value",
                     "authority", "receipt_rules", "policies", "origin", "provenance")


@dataclass(frozen=True, slots=True)
class AcceptedCondition:
    condition_id: str
    request_ids: tuple[str, ...]
    slot: Slot
    dimension: SemanticDimension
    operator: str
    polarity: Polarity
    level: GeoLevel | None
    value: tuple[str, ...]  # ExecutableValue.encode(): the only source of the value
    authority: str
    receipt_rules: tuple[str, ...]
    policies: tuple[str, ...]
    origin: ConditionOrigin  # INV-M2A-14: exactly one origin per condition
    provenance: Provenance

    def __post_init__(self) -> None:
        _text(self.condition_id, "CONDITION_ID")
        _texts(self.request_ids, "CONDITION_REQUEST_IDS", sorted_unique=True, empty=False)
        _instance(self.slot, Slot, "CONDITION_SLOT")
        _instance(self.dimension, SemanticDimension, "CONDITION_DIMENSION")
        if self.dimension not in SLOT_DIMENSIONS:
            _fail("CONDITION_DIMENSION_NOT_A_SLOT")
        if type(self.operator) is not str or not operator_allowed(self.dimension, self.operator):
            _fail("OPERATOR_OUTSIDE_DIMENSION")
        _instance(self.polarity, Polarity, "CONDITION_POLARITY")
        if self.dimension is SemanticDimension.GEOGRAPHY:
            _instance(self.level, GeoLevel, "CONDITION_LEVEL")
        elif self.level is not None:
            _fail("LEVEL_OUTSIDE_GEOGRAPHY")
        if self.slot != slot_of(self.dimension, self.value):  # also decodes the value strictly
            _fail("SLOT_NOT_DERIVED_FROM_VALUE")
        if self.dimension in POLARIZED_SLOT_DIMENSIONS and self.slot.polarity is not self.polarity:
            _fail("POLARITY_DISAGREES_WITH_VALUE")
        if self.dimension in (SemanticDimension.ORDER, SemanticDimension.CARDINALITY) and (
                self.polarity is not Polarity.POSITIVE):
            _fail("NEGATED_ORDER_OR_CARDINALITY")  # ORDER and CARDINALITY admit no negation
        if self.dimension is SemanticDimension.VALUE:
            if self.polarity is not Polarity.POSITIVE:
                _fail("NEGATED_VALUE")  # the comparator already absorbed the negation
            if self.operator != VALUE_OPERATOR_OF[decode_value(self.value).comparator]:
                _fail("OPERATOR_DISAGREES_WITH_COMPARATOR")
        _text(self.authority, "CONDITION_AUTHORITY")
        _texts(self.receipt_rules, "RECEIPT_RULES", sorted_unique=True)
        if not all(receipt_rule_allowed(rule) for rule in self.receipt_rules):
            _fail("UNKNOWN_RECEIPT_RULE")
        _texts(self.policies, "CONDITION_POLICIES", sorted_unique=True)
        _instance(self.origin, ConditionOrigin, "CONDITION_ORIGIN")
        _instance(self.provenance, Provenance, "CONDITION_PROVENANCE")
        self.provenance.check_dimension(self.dimension)

    def to_dict(self) -> dict:
        return {"condition_id": self.condition_id, "request_ids": list(self.request_ids),
                "slot": self.slot.to_dict(), "dimension": self.dimension.value, "operator": self.operator,
                "polarity": self.polarity.value, "level": None if self.level is None else self.level.value,
                "value": list(self.value), "authority": self.authority, "receipt_rules": list(self.receipt_rules),
                "policies": list(self.policies), "origin": self.origin.to_dict(),
                "provenance": self.provenance.to_dict()}

    @classmethod
    def from_dict(cls, data) -> AcceptedCondition:
        data = _keys(data, _CONDITION_FIELDS, "CONDITION")
        return cls(
            data["condition_id"], _read_texts(data["request_ids"], "CONDITION_REQUEST_IDS"),
            Slot.from_dict(data["slot"]), _enum_of(SemanticDimension, data["dimension"], "CONDITION_DIMENSION"),
            data["operator"], _enum_of(Polarity, data["polarity"], "CONDITION_POLARITY"),
            _opt(data["level"], lambda v: _enum_of(GeoLevel, v, "CONDITION_LEVEL")),
            _read_texts(data["value"], "CONDITION_VALUE"), data["authority"],
            _read_texts(data["receipt_rules"], "RECEIPT_RULES"), _read_texts(data["policies"], "CONDITION_POLICIES"),
            ConditionOrigin.from_dict(data["origin"]), Provenance.from_dict(data["provenance"]),
        )


_ACCEPTED_FIELDS = ("schema", "requests", "conditions", "logic", "policies", "base_trace_id", "root_trace_id")
_SINGLE_SLOT = frozenset({SemanticDimension.ORDER, SemanticDimension.CARDINALITY, SemanticDimension.VALUE})


@dataclass(frozen=True, slots=True)
class AcceptedInterpretation:
    """The effective accepted interpretation (``m2a-accepted/1``). ``logic is None`` means the AND of all
    conditions; with OR/NOT groups it is the IR v2 expression over exactly these condition ids."""

    requests: tuple[AcceptedRequest, ...]
    conditions: tuple[AcceptedCondition, ...]
    logic: ConditionExpression | None
    policies: tuple[HostPolicy, ...]
    base_trace_id: str | None
    root_trace_id: str

    def __post_init__(self) -> None:
        _records(self.requests, AcceptedRequest, "REQUESTS", empty=False)
        _records(self.conditions, AcceptedCondition, "CONDITIONS")
        _records(self.policies, HostPolicy, "POLICIES")
        for policy in self.policies:
            _canonical_policy(policy)
        _opt_text(self.base_trace_id, "BASE_TRACE_ID")
        _text(self.root_trace_id, "ROOT_TRACE_ID")
        request_ids = [request.request_id for request in self.requests]
        if len(set(request_ids)) != len(request_ids):
            _fail("DUPLICATE_REQUEST_ID")
        if len(self.requests) > 2:
            _fail("TOO_MANY_REQUESTS")
        if len(self.requests) == 2:  # dual anchor: processes + contracts, SHARES_SCOPE, same conditions
            first, second = self.requests
            if ({first.subject, second.subject} != {Subject.PROCESSES, Subject.CONTRACTS}
                    or first.scope_id != second.scope_id):
                _fail("INVALID_DUAL_ANCHOR")
            if any(set(condition.request_ids) != set(request_ids) for condition in self.conditions):
                _fail("DUAL_ANCHOR_CONDITIONS_DIFFER")
        condition_ids = [condition.condition_id for condition in self.conditions]
        if len(set(condition_ids)) != len(condition_ids):
            _fail("DUPLICATE_CONDITION_ID")
        for condition in self.conditions:
            if not set(condition.request_ids) <= set(request_ids):
                _fail("FOREIGN_CONDITION_REQUEST")
        _check_policies(self.policies, set(condition_ids))
        ranks = [_slot_rank(condition.slot) for condition in self.conditions]
        if ranks != sorted(ranks):
            _fail("NONCANONICAL_CONDITION_ORDER")  # rejected, never rewritten: build with ``canonical_interpretation``
        for request_id in request_ids:
            taken: set[Slot] = set()
            for condition in self.conditions:
                if request_id in condition.request_ids and condition.dimension in _SINGLE_SLOT:
                    if condition.slot in taken:
                        _fail(f"TWO_CONDITIONS_IN_{condition.slot.key.replace('/', '_').upper()}")
                    taken.add(condition.slot)
            if Slot(SemanticDimension.CARDINALITY) in taken and Slot(SemanticDimension.ORDER) not in taken:
                _fail("CARDINALITY_WITHOUT_ORDER")
        if self.logic is not None:
            _instance(self.logic, ConditionExpression, "LOGIC")
            atoms = self.logic.atoms()
            if len(atoms) != len(set(atoms)) or set(atoms) != set(condition_ids):
                _fail("LOGIC_ATOMS_MISMATCH")
            if any(self.logic.has_nonconjunctive_atom(c.condition_id)
                   for c in self.conditions if c.dimension is SemanticDimension.ORDER):
                _fail("NONCONJUNCTIVE_ORDER")
            if canonical_logic(self.logic) != self.logic:
                _fail("NONCANONICAL_LOGIC")  # rejected, never rewritten: build it with ``canonical_interpretation``

    def to_dict(self) -> dict:
        return {"schema": ACCEPTED_SCHEMA, "requests": [request.to_dict() for request in self.requests],
                "conditions": [condition.to_dict() for condition in self.conditions],
                "logic": None if self.logic is None else expression_to_dict(self.logic),
                "policies": [policy_to_dict(policy) for policy in self.policies],
                "base_trace_id": self.base_trace_id, "root_trace_id": self.root_trace_id}

    @classmethod
    def from_dict(cls, data) -> AcceptedInterpretation:
        _schema(data, ACCEPTED_SCHEMA, "ACCEPTED")
        data = _keys(data, _ACCEPTED_FIELDS, "ACCEPTED")
        return cls(
            tuple(AcceptedRequest.from_dict(item) for item in _list(data["requests"], "REQUESTS")),
            tuple(AcceptedCondition.from_dict(item) for item in _list(data["conditions"], "CONDITIONS")),
            _opt(data["logic"], expression_from_dict),
            tuple(policy_from_dict(item) for item in _list(data["policies"], "POLICIES")),
            data["base_trace_id"], data["root_trace_id"],
        )


def canonical_interpretation(requests: tuple[AcceptedRequest, ...], conditions: tuple[AcceptedCondition, ...],
                             logic: ConditionExpression | None, policies: tuple[HostPolicy, ...],
                             base_trace_id: str | None, root_trace_id: str) -> AcceptedInterpretation:
    """THE constructor path of every effective (contract §4.1: ``compose`` and ``from_acceptance``).

    It reorders only what is proven unobservable: conditions across slots (``CONDITION_SLOT_ORDER``; inside a slot the
    order given is kept) and the split ``policies``; a pure conjunction becomes ``logic = None``. ``requests`` and any
    OR/NOT tree are kept as given. It never deduplicates, merges or drops: the type validates the result and refuses
    anything else. A cross-slot or policy permutation gives the same bytes and the same ``interpretation_hash``.
    """
    return AcceptedInterpretation(requests, canonical_conditions(conditions), canonical_logic(logic),
                                  canonical_policies(policies), base_trace_id, root_trace_id)


def interpretation_hash(effective: AcceptedInterpretation) -> str:
    """Contract §4.3/§12: the effective's identity; it is the ``semantic_ref``."""
    _instance(effective, AcceptedInterpretation, "EFFECTIVE")
    return digest(effective.to_dict())


# ---- SemanticPatch / PatchOp (contract §5) -------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class UniverseTarget:
    """``REPLACE_UNIVERSE``: the universe becomes stated, so ``unspecified`` is never a target."""

    subject: Subject
    universe_kind: UniverseKind

    def __post_init__(self) -> None:
        _instance(self.subject, Subject, "TARGET_SUBJECT")
        if self.subject is Subject.UNSPECIFIED:
            _fail("UNIVERSE_TARGET_UNSPECIFIED")
        _instance(self.universe_kind, UniverseKind, "TARGET_UNIVERSE_KIND")

    def to_dict(self) -> dict:
        return {"subject": self.subject.value, "universe_kind": self.universe_kind.value}

    @classmethod
    def from_dict(cls, data) -> UniverseTarget:
        data = _keys(data, ("subject", "universe_kind"), "UNIVERSE_TARGET")
        return cls(_enum_of(Subject, data["subject"], "TARGET_SUBJECT"),
                   _enum_of(UniverseKind, data["universe_kind"], "TARGET_UNIVERSE_KIND"))


_OP_FIELDS = ("op", "slot", "condition", "universe", "origin")


@dataclass(frozen=True, slots=True)
class PatchOp:
    op: PatchOpKind
    slot: Slot
    condition: AcceptedCondition | None
    universe: UniverseTarget | None
    origin: ConditionOrigin

    def __post_init__(self) -> None:
        _instance(self.op, PatchOpKind, "PATCH_OP")
        _instance(self.slot, Slot, "PATCH_SLOT")
        _instance(self.origin, ConditionOrigin, "PATCH_ORIGIN")
        if self.origin.kind is OriginKind.HOST_POLICY:
            _fail("PATCH_ORIGIN_IS_POLICY")  # a patch comes from the turn or from an event, never from the host
        writes = self.op in (PatchOpKind.SET, PatchOpKind.SET_ORDER)
        if writes:
            _instance(self.condition, AcceptedCondition, "PATCH_CONDITION")
            if self.condition.slot != self.slot:
                _fail("PATCH_CONDITION_OUTSIDE_SLOT")
            if self.condition.origin != self.origin:
                _fail("PATCH_CONDITION_ORIGIN_MISMATCH")
        elif self.condition is not None:
            _fail("PATCH_OP_HAS_CONDITION")
        if self.op is PatchOpKind.REPLACE_UNIVERSE:
            if not self.slot.is_universe:
                _fail("REPLACE_UNIVERSE_OUTSIDE_UNIVERSE")
            _instance(self.universe, UniverseTarget, "PATCH_UNIVERSE")
        elif self.universe is not None:
            _fail("PATCH_OP_HAS_UNIVERSE")
        if self.op is PatchOpKind.SET_ORDER and self.slot.dimension is not SemanticDimension.ORDER:
            _fail("SET_ORDER_OUTSIDE_ORDER")
        if self.op is PatchOpKind.SET and self.slot.dimension is SemanticDimension.ORDER:
            _fail("ORDER_NEEDS_SET_ORDER")
        if self.op is not PatchOpKind.REPLACE_UNIVERSE and self.op is not PatchOpKind.KEEP and self.slot.is_universe:
            _fail("UNIVERSE_NEEDS_REPLACE_UNIVERSE")
        if self.slot.dimension_wide and self.op is not PatchOpKind.CLEAR:
            _fail("DIMENSION_WIDE_SLOT_OUTSIDE_CLEAR")

    def to_dict(self) -> dict:
        return {"op": self.op.value, "slot": self.slot.to_dict(),
                "condition": None if self.condition is None else self.condition.to_dict(),
                "universe": None if self.universe is None else self.universe.to_dict(),
                "origin": self.origin.to_dict()}

    @classmethod
    def from_dict(cls, data) -> PatchOp:
        data = _keys(data, _OP_FIELDS, "PATCH_OP")
        return cls(_enum_of(PatchOpKind, data["op"], "PATCH_OP"), Slot.from_dict(data["slot"]),
                   _opt(data["condition"], AcceptedCondition.from_dict),
                   _opt(data["universe"], UniverseTarget.from_dict), ConditionOrigin.from_dict(data["origin"]))


_PATCH_FIELDS = ("schema", "ops", "origin_kind")
_EVENT_ORIGINS = frozenset({OriginKind.OPTION_EVENT, OriginKind.VIEW_ACTION})


@dataclass(frozen=True, slots=True)
class SemanticPatch:
    """An atomic set of operations (``m2a-patch/1``). Never empty: an empty list is not a patch and never means
    «cancel» (§17.1). Slot conflicts are a composition refusal (M2A-3), not a type error."""

    ops: tuple[PatchOp, ...]
    origin_kind: PatchOriginKind

    def __post_init__(self) -> None:
        _records(self.ops, PatchOp, "PATCH_OPS", empty=False)
        _instance(self.origin_kind, PatchOriginKind, "PATCH_ORIGIN_KIND")
        allowed = {OriginKind.SPAN} if self.origin_kind is PatchOriginKind.TEXT else _EVENT_ORIGINS
        if any(op.origin.kind not in allowed for op in self.ops):
            _fail("PATCH_ORIGIN_KIND_MISMATCH")

    @property
    def slots(self) -> frozenset[Slot]:
        return frozenset(op.slot for op in self.ops)

    def to_dict(self) -> dict:
        return {"schema": PATCH_SCHEMA, "ops": [op.to_dict() for op in self.ops],
                "origin_kind": self.origin_kind.value}

    @classmethod
    def from_dict(cls, data) -> SemanticPatch:
        _schema(data, PATCH_SCHEMA, "PATCH")
        data = _keys(data, _PATCH_FIELDS, "PATCH")
        return cls(tuple(PatchOp.from_dict(item) for item in _list(data["ops"], "PATCH_OPS")),
                   _enum_of(PatchOriginKind, data["origin_kind"], "PATCH_ORIGIN_KIND"))


def patch_sha256(patch: SemanticPatch) -> str:
    _instance(patch, SemanticPatch, "PATCH")
    return digest(patch.to_dict())


# ---- PendingClarification / CandidatePatch (contract §8) -----------------------------------------------------------
@dataclass(frozen=True, slots=True)
class CandidatePatch:
    """Generic by construction: one option is one complete patch, whatever slots it touches (§8.1)."""

    candidate_id: str
    display_key: str
    patch: SemanticPatch

    def __post_init__(self) -> None:
        _text(self.candidate_id, "CANDIDATE_ID")
        _text(self.display_key, "CANDIDATE_DISPLAY_KEY")
        _instance(self.patch, SemanticPatch, "CANDIDATE_PATCH")

    def to_dict(self) -> dict:
        return {"candidate_id": self.candidate_id, "display_key": self.display_key, "patch": self.patch.to_dict()}

    @classmethod
    def from_dict(cls, data) -> CandidatePatch:
        data = _keys(data, ("candidate_id", "display_key", "patch"), "CANDIDATE")
        return cls(data["candidate_id"], data["display_key"], SemanticPatch.from_dict(data["patch"]))


_PENDING_FIELDS = ("schema", "pending_id", "asked_trace_id", "created_at_utc", "expires_at_utc", "asks", "frame",
                   "base_trace_id", "ambiguous_span", "ambiguous_raw_sha256", "reason", "candidate_patches",
                   "answer_slots")
MAX_ASKS = 2


@dataclass(frozen=True, slots=True)
class PendingClarification:
    pending_id: str
    asked_trace_id: str
    created_at_utc: datetime
    expires_at_utc: datetime
    asks: int
    frame: AcceptedInterpretation | None
    base_trace_id: str | None
    ambiguous_span: tuple[int, int] | None
    ambiguous_raw_sha256: str | None
    reason: str
    candidate_patches: tuple[CandidatePatch, ...]
    answer_slots: frozenset[Slot]

    def __post_init__(self) -> None:
        _text(self.pending_id, "PENDING_ID")
        _text(self.asked_trace_id, "ASKED_TRACE_ID")
        _utc(self.created_at_utc, "PENDING_CREATED_AT")
        _utc(self.expires_at_utc, "PENDING_EXPIRES_AT")
        if self.expires_at_utc <= self.created_at_utc:
            _fail("PENDING_EXPIRES_BEFORE_CREATED")
        if type(self.asks) is not int or not 1 <= self.asks <= MAX_ASKS:
            _fail("INVALID_PENDING_ASKS")
        if self.frame is not None:
            _instance(self.frame, AcceptedInterpretation, "PENDING_FRAME")
        _opt_text(self.base_trace_id, "PENDING_BASE_TRACE_ID")
        if (self.ambiguous_span is None) != (self.ambiguous_raw_sha256 is None):
            _fail("AMBIGUOUS_SPAN_WITHOUT_RAW")
        if self.ambiguous_span is not None:
            span = self.ambiguous_span
            if (type(span) is not tuple or len(span) != 2 or any(type(bound) is not int for bound in span)
                    or span[0] < 0 or span[1] <= span[0]):
                _fail("INVALID_AMBIGUOUS_SPAN")
            _sha(self.ambiguous_raw_sha256, "AMBIGUOUS_RAW_SHA256")
        if not (reason_code_allowed(self.reason, TurnResolution.CLARIFY)
                or reason_code_allowed(self.reason, TurnResolution.REASK)):
            _fail("UNKNOWN_PENDING_REASON")
        _records(self.candidate_patches, CandidatePatch, "CANDIDATE_PATCHES")
        candidate_ids = [candidate.candidate_id for candidate in self.candidate_patches]
        if len(set(candidate_ids)) != len(candidate_ids):
            _fail("DUPLICATE_CANDIDATE_ID")
        if type(self.answer_slots) is not frozenset or any(type(slot) is not Slot for slot in self.answer_slots):
            _fail("INVALID_ANSWER_SLOTS")
        if not all(candidate.patch.slots <= self.answer_slots for candidate in self.candidate_patches):
            _fail("CANDIDATE_OUTSIDE_ANSWER_SLOTS")  # §8.1: answer_slots ⊇ the candidates' slots

    def to_dict(self) -> dict:
        return {"schema": PENDING_SCHEMA, "pending_id": self.pending_id, "asked_trace_id": self.asked_trace_id,
                "created_at_utc": self.created_at_utc.isoformat(), "expires_at_utc": self.expires_at_utc.isoformat(),
                "asks": self.asks, "frame": None if self.frame is None else self.frame.to_dict(),
                "base_trace_id": self.base_trace_id,
                "ambiguous_span": None if self.ambiguous_span is None else list(self.ambiguous_span),
                "ambiguous_raw_sha256": self.ambiguous_raw_sha256, "reason": self.reason,
                "candidate_patches": [candidate.to_dict() for candidate in self.candidate_patches],
                "answer_slots": sorted(slot.key for slot in self.answer_slots)}

    @classmethod
    def from_dict(cls, data) -> PendingClarification:
        _schema(data, PENDING_SCHEMA, "PENDING")
        data = _keys(data, _PENDING_FIELDS, "PENDING")
        span = data["ambiguous_span"]
        if span is not None:
            span = tuple(_list(span, "AMBIGUOUS_SPAN"))
        slot_keys = _read_texts(data["answer_slots"], "ANSWER_SLOTS")
        if list(slot_keys) != sorted(set(slot_keys)):
            _fail("NONCANONICAL_ANSWER_SLOTS")
        return cls(
            data["pending_id"], data["asked_trace_id"], _read_utc(data["created_at_utc"], "PENDING_CREATED_AT"),
            _read_utc(data["expires_at_utc"], "PENDING_EXPIRES_AT"), _read_int(data["asks"], "PENDING_ASKS"),
            _opt(data["frame"], AcceptedInterpretation.from_dict), data["base_trace_id"], span,
            data["ambiguous_raw_sha256"], data["reason"],
            tuple(CandidatePatch.from_dict(item) for item in _list(data["candidate_patches"], "CANDIDATE_PATCHES")),
            frozenset(Slot.from_dict(key) for key in slot_keys),
        )


def pending_sha256(pending: PendingClarification) -> str:
    _instance(pending, PendingClarification, "PENDING")
    return digest(pending.to_dict())


# ---- ResultSetRef (contract §9) ------------------------------------------------------------------------------------
_GROUP_FIELDS = ("request_id", "dataset", "plan_hash", "order_contract", "page_limit", "shown_ids", "next_offset",
                 "total", "total_quality")


@dataclass(frozen=True, slots=True)
class GroupRef:
    request_id: str
    dataset: str
    plan_hash: str
    order_contract: str
    page_limit: int
    shown_ids: tuple[str, ...]
    next_offset: int | None
    total: int | None
    total_quality: TotalQuality

    def __post_init__(self) -> None:
        _text(self.request_id, "GROUP_REQUEST_ID")
        _text(self.dataset, "GROUP_DATASET")
        _sha(self.plan_hash, "GROUP_PLAN_HASH")
        _text(self.order_contract, "GROUP_ORDER_CONTRACT")
        _int(self.page_limit, "GROUP_PAGE_LIMIT", 1)
        _texts(self.shown_ids, "GROUP_SHOWN_IDS")
        if self.next_offset is not None:
            _int(self.next_offset, "GROUP_NEXT_OFFSET", 0)
        _instance(self.total_quality, TotalQuality, "GROUP_TOTAL_QUALITY")
        if (self.total is None) != (self.total_quality is TotalQuality.NOT_APPLICABLE):
            _fail("GROUP_TOTAL_QUALITY_MISMATCH")
        if self.total is not None:
            _int(self.total, "GROUP_TOTAL", 0)

    def to_dict(self) -> dict:
        return {"request_id": self.request_id, "dataset": self.dataset, "plan_hash": self.plan_hash,
                "order_contract": self.order_contract, "page_limit": self.page_limit,
                "shown_ids": list(self.shown_ids), "next_offset": self.next_offset, "total": self.total,
                "total_quality": self.total_quality.value}

    @classmethod
    def from_dict(cls, data) -> GroupRef:
        data = _keys(data, _GROUP_FIELDS, "GROUP")
        for name in ("next_offset", "total"):
            if data[name] is not None:
                _read_int(data[name], f"GROUP_{name.upper()}")
        return cls(data["request_id"], data["dataset"], data["plan_hash"], data["order_contract"],
                   _read_int(data["page_limit"], "GROUP_PAGE_LIMIT"), _read_texts(data["shown_ids"], "SHOWN_IDS"),
                   data["next_offset"], data["total"],
                   _enum_of(TotalQuality, data["total_quality"], "GROUP_TOTAL_QUALITY"))


@dataclass(frozen=True, slots=True)
class ResultSetRef:
    """The manifest of a call plus its cursor (``m2a-resultset/1``): rows and pages, never composition (§9.4)."""

    trace_id: str
    interpretation_hash: str
    groups: tuple[GroupRef, ...]

    def __post_init__(self) -> None:
        _text(self.trace_id, "RESULTSET_TRACE_ID")
        _sha(self.interpretation_hash, "RESULTSET_INTERPRETATION_HASH")
        _records(self.groups, GroupRef, "RESULTSET_GROUPS", empty=False)
        request_ids = [group.request_id for group in self.groups]
        if len(set(request_ids)) != len(request_ids):
            _fail("DUPLICATE_GROUP_REQUEST")

    def to_dict(self) -> dict:
        return {"schema": RESULTSET_SCHEMA, "trace_id": self.trace_id, "interpretation_hash": self.interpretation_hash,
                "groups": [group.to_dict() for group in self.groups]}

    @classmethod
    def from_dict(cls, data) -> ResultSetRef:
        _schema(data, RESULTSET_SCHEMA, "RESULTSET")
        data = _keys(data, ("schema", "trace_id", "interpretation_hash", "groups"), "RESULTSET")
        return cls(data["trace_id"], data["interpretation_hash"],
                   tuple(GroupRef.from_dict(item) for item in _list(data["groups"], "RESULTSET_GROUPS")))


# ---- EchoItem (contract §3.6, §11) ---------------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class EchoItem:
    echo_class: EchoClass
    slot: Slot | None
    condition_ids: tuple[str, ...]
    display_key: str  # the display itself comes from the typed value, never from here

    def __post_init__(self) -> None:
        _instance(self.echo_class, EchoClass, "ECHO_CLASS")
        if self.slot is not None:
            _instance(self.slot, Slot, "ECHO_SLOT")
        _texts(self.condition_ids, "ECHO_CONDITION_IDS")
        _text(self.display_key, "ECHO_DISPLAY_KEY")

    def to_dict(self) -> dict:
        return {"echo_class": self.echo_class.value, "slot": None if self.slot is None else self.slot.to_dict(),
                "condition_ids": list(self.condition_ids), "display_key": self.display_key}

    @classmethod
    def from_dict(cls, data) -> EchoItem:
        data = _keys(data, ("echo_class", "slot", "condition_ids", "display_key"), "ECHO")
        return cls(_enum_of(EchoClass, data["echo_class"], "ECHO_CLASS"), _opt(data["slot"], Slot.from_dict),
                   _read_texts(data["condition_ids"], "ECHO_CONDITION_IDS"), data["display_key"])


# ---- ExecutionReceipt / AcceptedTurnRecord (contract §3.1) --------------------------------------------------------
@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    accepted: bool
    executed: bool
    source_ok: bool
    plan_hashes: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("accepted", "executed", "source_ok"):
            _flag(getattr(self, name), f"EXECUTION_{name.upper()}")
        if type(self.plan_hashes) is not tuple:
            _fail("INVALID_EXECUTION_PLAN_HASHES")
        for plan_hash in self.plan_hashes:
            _sha(plan_hash, "EXECUTION_PLAN_HASHES")
        if (self.executed and not self.accepted) or (self.source_ok and not self.executed):
            _fail("INCOHERENT_EXECUTION")
        if self.executed and not self.plan_hashes:
            _fail("EXECUTED_WITHOUT_PLAN")

    def to_dict(self) -> dict:
        return {"accepted": self.accepted, "executed": self.executed, "source_ok": self.source_ok,
                "plan_hashes": list(self.plan_hashes)}

    @classmethod
    def from_dict(cls, data) -> ExecutionReceipt:
        data = _keys(data, ("accepted", "executed", "source_ok", "plan_hashes"), "EXECUTION")
        return cls(data["accepted"], data["executed"], data["source_ok"],
                   _read_texts(data["plan_hashes"], "EXECUTION_PLAN_HASHES"))


_PATCH_RESOLUTIONS = frozenset({TurnResolution.DELTA, TurnResolution.PENDING_ANSWER, TurnResolution.PROTOCOL_EVENT})
_SILENT_RESOLUTIONS = frozenset({TurnResolution.CLARIFY, TurnResolution.REASK, TurnResolution.INFORM,
                                 TurnResolution.HANDOFF_LEGACY})
_BASED_RESOLUTIONS = _PATCH_RESOLUTIONS | {TurnResolution.PROTOCOL_PAGE}
# Contract §7.1: the resolution each OUT_* scope leads to.
OUT_SCOPE_RESOLUTION = {
    TurnScope.OUT_NO_ANCHOR: TurnResolution.CLARIFY,
    TurnScope.OUT_ANCHOR_NOT_SEARCH: TurnResolution.HANDOFF_LEGACY,
    TurnScope.OUT_OPERATION_CHANGE: TurnResolution.INFORM,
    TurnScope.OUT_UNSUPPORTED_PROPOSAL: TurnResolution.INFORM,
    TurnScope.OUT_REFERENT_EXTENSIONAL: TurnResolution.INFORM,
    TurnScope.OUT_RELATIVE_TO_PREVIOUS: TurnResolution.CLARIFY,
}
_RECORD_FIELDS = ("schema", "trace_id", "chat_id", "turn_id", "created_at_utc", "reference_instant", "profile",
                  "prompt_sha256", "scope", "resolution", "control", "base_trace_id", "patch", "effective",
                  "semantic_ref", "execution", "result_set_ref", "pending", "echo", "anchor_trace_id",
                  "record_sha256")


@dataclass(frozen=True, slots=True)
class AcceptedTurnRecord:
    """One line per Foundation turn (``m2a-record/1``). ``semantic_ref`` and ``record_sha256`` are derived, never
    stored independently: the reader recomputes both and rejects a record whose stored digests disagree."""

    trace_id: str
    chat_id: str
    turn_id: str
    created_at_utc: datetime
    reference_instant: datetime  # injected; never recomputed
    profile: str
    prompt_sha256: str | None  # None for event turns (no LLM)
    scope: TurnScope
    resolution: TurnResolution
    control: PendingControl | None
    base_trace_id: str | None
    patch: SemanticPatch | None
    effective: AcceptedInterpretation | None
    execution: ExecutionReceipt | None
    result_set_ref: ResultSetRef | None
    pending: PendingClarification | None
    echo: tuple[EchoItem, ...]
    anchor_trace_id: str | None

    def __post_init__(self) -> None:
        for name in ("trace_id", "chat_id", "turn_id", "profile"):
            _text(getattr(self, name), f"RECORD_{name.upper()}")
        _utc(self.created_at_utc, "RECORD_CREATED_AT")
        _utc(self.reference_instant, "RECORD_REFERENCE_INSTANT")
        if self.prompt_sha256 is not None:
            _sha(self.prompt_sha256, "RECORD_PROMPT_SHA256")
        _instance(self.scope, TurnScope, "RECORD_SCOPE")
        _instance(self.resolution, TurnResolution, "RECORD_RESOLUTION")
        if self.control is not None:
            _instance(self.control, PendingControl, "RECORD_CONTROL")
        _opt_text(self.base_trace_id, "RECORD_BASE_TRACE_ID")
        _opt_text(self.anchor_trace_id, "RECORD_ANCHOR_TRACE_ID")
        for name, kind in (("patch", SemanticPatch), ("effective", AcceptedInterpretation),
                           ("execution", ExecutionReceipt), ("result_set_ref", ResultSetRef),
                           ("pending", PendingClarification)):
            value = getattr(self, name)
            if value is not None:
                _instance(value, kind, f"RECORD_{name.upper()}")
        _records(self.echo, EchoItem, "RECORD_ECHO")
        if len(set(self.echo)) != len(self.echo):
            _fail("DUPLICATE_ECHO_ITEM")
        if (self.patch is not None) != (self.resolution in _PATCH_RESOLUTIONS):
            _fail("PATCH_RESOLUTION_MISMATCH")  # §3.1: a patch exactly with DELTA / PENDING_ANSWER / PROTOCOL_EVENT
        if self.resolution is TurnResolution.NEW_SEARCH and self.base_trace_id is not None:
            _fail("NEW_SEARCH_WITH_BASE")  # §5.4
        if self.resolution is TurnResolution.NEW_SEARCH and self.effective is not None and (
                self.effective.base_trace_id is not None):
            _fail("NEW_SEARCH_WITH_BASE")
        if self.resolution in _BASED_RESOLUTIONS and self.base_trace_id is None:
            _fail("COMPOSITION_WITHOUT_BASE")
        expected = OUT_SCOPE_RESOLUTION.get(self.scope)
        if expected is not None and self.resolution is not expected:
            _fail("OUT_SCOPE_RESOLUTION_MISMATCH")
        if (expected is not None or self.resolution in _SILENT_RESOLUTIONS) and (
                self.execution is not None or self.result_set_ref is not None):
            _fail("SILENT_TURN_CALLED_SOURCE")  # INV-M2A-20: OUT_* / CLARIFY / REASK / INFORM make 0 calls
        if self.execution is not None and self.effective is None:
            _fail("EXECUTION_WITHOUT_EFFECTIVE")
        if self.result_set_ref is not None:
            if self.execution is None or not self.execution.source_ok:
                _fail("RESULTSET_WITHOUT_SOURCE")  # §9: only if the source answered
            if self.result_set_ref.interpretation_hash != self.semantic_ref:
                _fail("RESULTSET_OF_ANOTHER_EFFECTIVE")

    @property
    def semantic_ref(self) -> str | None:
        return None if self.effective is None else interpretation_hash(self.effective)

    def _body(self) -> dict:
        return {"schema": RECORD_SCHEMA, "trace_id": self.trace_id, "chat_id": self.chat_id, "turn_id": self.turn_id,
                "created_at_utc": self.created_at_utc.isoformat(),
                "reference_instant": self.reference_instant.isoformat(), "profile": self.profile,
                "prompt_sha256": self.prompt_sha256, "scope": self.scope.value, "resolution": self.resolution.value,
                "control": None if self.control is None else self.control.value,
                "base_trace_id": self.base_trace_id,
                "patch": None if self.patch is None else self.patch.to_dict(),
                "effective": None if self.effective is None else self.effective.to_dict(),
                "semantic_ref": self.semantic_ref,
                "execution": None if self.execution is None else self.execution.to_dict(),
                "result_set_ref": None if self.result_set_ref is None else self.result_set_ref.to_dict(),
                "pending": None if self.pending is None else self.pending.to_dict(),
                "echo": [item.to_dict() for item in self.echo], "anchor_trace_id": self.anchor_trace_id}

    @property
    def record_sha256(self) -> str:
        return digest(self._body())

    def to_dict(self) -> dict:
        body = self._body()
        body["record_sha256"] = digest(body)
        return body

    @classmethod
    def from_dict(cls, data) -> AcceptedTurnRecord:
        _schema(data, RECORD_SCHEMA, "RECORD")
        data = _keys(data, _RECORD_FIELDS, "RECORD")
        record = cls(
            data["trace_id"], data["chat_id"], data["turn_id"],
            _read_utc(data["created_at_utc"], "RECORD_CREATED_AT"),
            _read_utc(data["reference_instant"], "RECORD_REFERENCE_INSTANT"), data["profile"],
            data["prompt_sha256"], _enum_of(TurnScope, data["scope"], "RECORD_SCOPE"),
            _enum_of(TurnResolution, data["resolution"], "RECORD_RESOLUTION"),
            _opt(data["control"], lambda v: _enum_of(PendingControl, v, "RECORD_CONTROL")),
            data["base_trace_id"], _opt(data["patch"], SemanticPatch.from_dict),
            _opt(data["effective"], AcceptedInterpretation.from_dict),
            _opt(data["execution"], ExecutionReceipt.from_dict),
            _opt(data["result_set_ref"], ResultSetRef.from_dict),
            _opt(data["pending"], PendingClarification.from_dict),
            tuple(EchoItem.from_dict(item) for item in _list(data["echo"], "RECORD_ECHO")),
            data["anchor_trace_id"],
        )
        if data["semantic_ref"] != record.semantic_ref:
            _fail("SEMANTIC_REF_MISMATCH")
        if data["record_sha256"] != record.record_sha256:
            _fail("RECORD_DIGEST_MISMATCH")
        return record


# Persistible types and their strict readers (the round-trip property and the boundary pins enumerate this).
PERSISTED_TYPES = (AcceptedTurnRecord, AcceptedInterpretation, AcceptedRequest, AcceptedCondition, ConditionOrigin,
                   Provenance, SemanticPatch, PatchOp, UniverseTarget, PendingClarification, CandidatePatch,
                   ResultSetRef, GroupRef, EchoItem, ExecutionReceipt, Slot)
