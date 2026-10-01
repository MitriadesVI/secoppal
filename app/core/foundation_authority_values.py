"""Typed executable values an authority may return (spine v5, C0 §5.5.1; INV-C0-9).

``ExecutableValue`` is a CLOSED union.  Each member is an immutable record that validates itself
and round-trips through a tagged, versioned, canonical tuple of strings::

    ("topic@1", "phrase_folded=obras", "token_mode=1")

so ``HostResolution.resolved_ids`` keeps its shape (a non-empty tuple of distinct, non-blank
strings; every item after the tag carries its field name, which also makes the items distinct).
``decode_value`` is strict: an unknown tag or version, a missing, extra or reordered field and any
non-canonical spelling are ``ContractError``; ``decode(encode(v)) == v`` and
``encode(decode(items)) == items`` hold for everything accepted.

Every value whose projection is a membership or an interval carries ``excluded`` (last field,
encoded ``0``/``1``): the NEGATIVE the authority honoured (INV-C1.1-EXCL).  MONEY absorbs its
negation in the comparator; ORDER and CARDINALITY admit none.

Free text never travels in a value except where the dimension *is* a name (topic phrase, display).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from enum import Enum
from urllib.parse import quote, unquote

from app.core.foundation_geo_level import GeoLevel
from app.core.interpretation_contract import ContractError

MAX_TOP_N = 100  # P8 (ADR-103): «los 500 más caros» exceeds it and is clarified, never clamped
STATE_CLASSES = ("open", "published", "closed", "awarded", "terminated", "in_execution",
                 "cancelled", "draft", "in_evaluation", "signed", "liquidated", "suspended", "ceded")

_NONE = "!"  # `quote` escapes "!", so a bare "!" can never be a spelled value
_LABEL = re.compile(r"[a-z][a-z0-9_]{0,31}\Z")
_POLICY = re.compile(r"[a-z][a-z0-9_-]*@[0-9][0-9A-Za-z.]*\Z")
_MEMBERSHIP = _POLICY  # "<group_id>@<version>" (ADR-074 §4)
_DOCUMENT_KIND = re.compile(r"[A-Z][A-Z0-9_]{0,15}\Z")
_DOCUMENT = re.compile(r"[A-Z0-9]{1,20}\Z")
_DIVIPOLA = {GeoLevel.DEPARTMENT: re.compile(r"\d{2}\Z"), GeoLevel.MUNICIPALITY: re.compile(r"\d{5}\Z")}


# ---- validation helpers -------------------------------------------------------------------
def _text(value, label: str) -> None:
    """A non-blank string that UTF-8 can carry."""
    if type(value) is not str or not value.strip():
        raise ContractError(f"INVALID_{label}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ContractError(f"INVALID_{label}") from error


def _texts(values, label: str, *, empty: bool = False) -> None:
    if type(values) is not tuple or (not values and not empty):
        raise ContractError(f"INVALID_{label}")
    for value in values:
        _text(value, label)
    if len(set(values)) != len(values):
        raise ContractError(f"DUPLICATE_{label}")


def _enum(value, kind: type, label: str) -> None:
    if type(value) is not kind:
        raise ContractError(f"INVALID_{label}")


def _flag(value, label: str) -> None:
    if type(value) is not bool:
        raise ContractError(f"INVALID_{label}")


def _int(value, label: str, low: int, high: int | None = None) -> None:
    if type(value) is not int or value < low or (high is not None and value > high):
        raise ContractError(f"INVALID_{label}")


def _match(pattern: re.Pattern, value, label: str) -> None:
    if type(value) is not str or not pattern.fullmatch(value):
        raise ContractError(f"INVALID_{label}")


# ---- canonical field codecs ---------------------------------------------------------------
def _enc(text: str) -> str:
    return quote(text, safe="")


def _dec(text: str) -> str:
    return unquote(text, errors="strict")


def _enc_opt(text: str | None) -> str:
    return _NONE if text is None else _enc(text)


def _dec_opt(text: str) -> str | None:
    return None if text == _NONE else _dec(text)


def _enc_list(values) -> str:
    return ",".join(_enc(item) for item in values)


def _dec_list(text: str) -> tuple[str, ...]:
    return tuple(_dec(item) for item in text.split(",")) if text else ()


def _enc_flag(value: bool) -> str:
    return "1" if value else "0"


def _dec_flag(text: str) -> bool:
    if text not in ("0", "1"):
        raise ContractError("INVALID_VALUE_FIELDS")
    return text == "1"


def _enc_date(value: date | None) -> str:
    return _NONE if value is None else value.isoformat()


def _dec_date(text: str) -> date | None:
    return None if text == _NONE else date.fromisoformat(text)


def _fields(items, tag: str, names: tuple[str, ...]) -> dict[str, str]:
    """Strictly split ``items`` into ``{name: encoded}``: exact tag, exact names, exact order."""
    if type(items) is not tuple or any(type(item) is not str for item in items) or not items:
        raise ContractError("INVALID_VALUE_ENCODING")
    if items[0] != tag:
        raise ContractError("UNSUPPORTED_VALUE_TAG")
    pairs = [item.partition("=") for item in items[1:]]
    if [name for name, sep, _ in pairs if sep == "="] != list(names) or len(pairs) != len(names):
        raise ContractError("INVALID_VALUE_FIELDS")
    return {name: raw for name, _, raw in pairs}


def _canonical(value, items) -> object:
    """Reject any spelling other than the canonical one (and any decode that does not round-trip)."""
    if value.encode() != tuple(items):
        raise ContractError("NONCANONICAL_VALUE_ENCODING")
    return value


def _line(tag: str, **fields: str) -> tuple[str, ...]:
    return (tag, *(f"{name}={raw}" for name, raw in fields.items()))


# ---- the closed union ---------------------------------------------------------------------
class _Codec:
    """``decode`` is total: any malformed input is ``ContractError``, never a bare ValueError."""

    __slots__ = ()

    @classmethod
    def decode(cls, items):
        try:
            return cls._decode(items)
        except ContractError:
            raise
        except (ValueError, UnicodeError) as error:  # bad enum member, date, percent-escape
            raise ContractError("INVALID_VALUE_FIELDS") from error


@dataclass(frozen=True, slots=True)
class TopicValue(_Codec):
    TAG = "topic@1"
    phrase_folded: str
    token_mode: bool
    excluded: bool

    def __post_init__(self) -> None:
        _text(self.phrase_folded, "TOPIC_PHRASE")
        if type(self.token_mode) is not bool:
            raise ContractError("INVALID_TOPIC_TOKEN_MODE")
        _flag(self.excluded, "TOPIC_EXCLUDED")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, phrase_folded=_enc(self.phrase_folded),
                     token_mode=_enc_flag(self.token_mode), excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("phrase_folded", "token_mode", "excluded"))
        return _canonical(cls(_dec(raw["phrase_folded"]), _dec_flag(raw["token_mode"]),
                              _dec_flag(raw["excluded"])), items)


class BuyerKind(str, Enum):
    UNIT = "unit"
    GROUP = "group"


@dataclass(frozen=True, slots=True)
class BuyerValue(_Codec):
    """A BuyerUnit or a declared, versioned BuyerGroup (ADR-074 §4); never derived from NIT/territory.

    UNIT:  ``members == (canonical,)`` and no membership.
    GROUP: two or more sorted, distinct members (``canonical`` is the group's name, not a member) and
           ``membership = "<group_id>@<version>"``.  The projection must cover EVERY member or BLOCK.
    An alias with several owners is the authority's AMBIGUOUS_HOST_DIMENSION, never a value.
    """

    TAG = "buyer@1"
    canonical: str
    kind: BuyerKind
    members: tuple[str, ...]
    membership: str | None
    excluded: bool

    def __post_init__(self) -> None:
        _text(self.canonical, "BUYER_CANONICAL")
        _enum(self.kind, BuyerKind, "BUYER_KIND")
        _texts(self.members, "BUYER_MEMBERS")
        _flag(self.excluded, "BUYER_EXCLUDED")
        if self.kind is BuyerKind.UNIT:
            if self.members != (self.canonical,):
                raise ContractError("INVALID_BUYER_MEMBERS")
            if self.membership is not None:
                raise ContractError("INVALID_BUYER_MEMBERSHIP")
            return
        if len(self.members) < 2:
            raise ContractError("BUYER_GROUP_TOO_SMALL")  # one member is a unit, not a group
        if list(self.members) != sorted(self.members):
            raise ContractError("UNSORTED_BUYER_MEMBERS")
        if self.canonical in self.members:
            raise ContractError("INVALID_BUYER_MEMBERS")
        _match(_MEMBERSHIP, self.membership, "BUYER_MEMBERSHIP")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, canonical=_enc(self.canonical), kind=self.kind.value,
                     members=_enc_list(self.members), membership=_enc_opt(self.membership),
                     excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("canonical", "kind", "members", "membership", "excluded"))
        return _canonical(cls(_dec(raw["canonical"]), BuyerKind(raw["kind"]), _dec_list(raw["members"]),
                              _dec_opt(raw["membership"]), _dec_flag(raw["excluded"])), items)


@dataclass(frozen=True, slots=True)
class ActorValue(_Codec):
    TAG = "actor@1"
    document: str
    document_kind: str
    display: str
    excluded: bool

    def __post_init__(self) -> None:
        _match(_DOCUMENT, self.document, "ACTOR_DOCUMENT")
        _match(_DOCUMENT_KIND, self.document_kind, "ACTOR_DOCUMENT_KIND")
        if self.document_kind == "UNKNOWN":
            raise ContractError("INVALID_ACTOR_DOCUMENT_KIND")  # an unknown kind is not an identity
        _text(self.display, "ACTOR_DISPLAY")
        _flag(self.excluded, "ACTOR_EXCLUDED")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, document=self.document, document_kind=self.document_kind,
                     display=_enc(self.display), excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("document", "document_kind", "display", "excluded"))
        return _canonical(cls(raw["document"], raw["document_kind"], _dec(raw["display"]),
                              _dec_flag(raw["excluded"])), items)


@dataclass(frozen=True, slots=True)
class GeoValue(_Codec):
    """``department`` names the department of a MUNICIPALITY (its SoQL needs both); ``members`` is REGION's."""

    TAG = "geo@1"
    level: GeoLevel
    canonical: str
    department: str | None
    divipola: str | None
    members: tuple[str, ...]
    excluded: bool

    def __post_init__(self) -> None:
        _enum(self.level, GeoLevel, "GEO_LEVEL")
        if self.level is GeoLevel.UNKNOWN:
            raise ContractError("INVALID_GEO_LEVEL")  # a resolved place has a level
        _text(self.canonical, "GEO_CANONICAL")
        if (self.department is not None) != (self.level is GeoLevel.MUNICIPALITY):
            raise ContractError("INVALID_GEO_DEPARTMENT")  # a department exactly for a municipality
        if self.department is not None:
            _text(self.department, "GEO_DEPARTMENT")
        if self.divipola is not None:  # 2 digits for a department, 5 for a municipality, none for a region
            pattern = _DIVIPOLA.get(self.level)
            if type(self.divipola) is not str or pattern is None or not pattern.fullmatch(self.divipola):
                raise ContractError("INVALID_GEO_DIVIPOLA")
        _texts(self.members, "GEO_MEMBERS", empty=True)
        if bool(self.members) != (self.level is GeoLevel.REGION):
            raise ContractError("INVALID_GEO_MEMBERS")  # members exactly when the level is a region
        _flag(self.excluded, "GEO_EXCLUDED")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, level=self.level.value, canonical=_enc(self.canonical),
                     department=_enc_opt(self.department), divipola=_enc_opt(self.divipola),
                     members=_enc_list(self.members), excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG,
                      ("level", "canonical", "department", "divipola", "members", "excluded"))
        return _canonical(cls(GeoLevel(raw["level"]), _dec(raw["canonical"]), _dec_opt(raw["department"]),
                              _dec_opt(raw["divipola"]), _dec_list(raw["members"]),
                              _dec_flag(raw["excluded"])), items)


@dataclass(frozen=True, slots=True)
class TimeValue(_Codec):
    """A half-open interval ``[start, end_exclusive)``; one open bound is «desde» / «hasta»."""

    TAG = "time@1"
    start: date | None
    end_exclusive: date | None
    precision: str  # open label for the echo and the receipt; the projection never reads it
    excluded: bool

    def __post_init__(self) -> None:
        for bound in (self.start, self.end_exclusive):
            if bound is not None and type(bound) is not date:
                raise ContractError("INVALID_TIME_BOUND")
        if self.start is None and self.end_exclusive is None:
            raise ContractError("UNBOUNDED_TIME_VALUE")
        if (self.start is not None and self.end_exclusive is not None
                and self.start >= self.end_exclusive):
            raise ContractError("EMPTY_TIME_INTERVAL")
        _match(_LABEL, self.precision, "TIME_PRECISION")
        _flag(self.excluded, "TIME_EXCLUDED")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, start=_enc_date(self.start), end_exclusive=_enc_date(self.end_exclusive),
                     precision=self.precision, excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("start", "end_exclusive", "precision", "excluded"))
        return _canonical(cls(_dec_date(raw["start"]), _dec_date(raw["end_exclusive"]),
                              raw["precision"], _dec_flag(raw["excluded"])), items)


class Comparator(str, Enum):
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


@dataclass(frozen=True, slots=True)
class MoneyValue(_Codec):
    TAG = "money@1"
    amount_cop: int
    comparator: Comparator

    def __post_init__(self) -> None:
        _int(self.amount_cop, "MONEY_AMOUNT", 0)
        _enum(self.comparator, Comparator, "MONEY_COMPARATOR")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, amount_cop=str(self.amount_cop), comparator=self.comparator.value)

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("amount_cop", "comparator"))
        if not raw["amount_cop"].isascii() or not raw["amount_cop"].isdigit():
            raise ContractError("INVALID_VALUE_FIELDS")
        return _canonical(cls(int(raw["amount_cop"]), Comparator(raw["comparator"])), items)


@dataclass(frozen=True, slots=True)
class StateValue(_Codec):
    """``literals`` are ``(dataset, column, values)`` triples sorted and unique by ``(dataset, column)``.

    Semantics: AND between the columns of one dataset, OR within ``values`` (the process funnel filters
    ``estado_del_procedimiento`` AND ``estado_de_apertura_del_proceso``).  A tuple, not a dict: a dict
    is mutable and unhashable.
    """

    TAG = "state@1"
    canonical_class: str
    literals: tuple[tuple[str, str, tuple[str, ...]], ...]
    excluded: bool

    def __post_init__(self) -> None:
        if type(self.canonical_class) is not str or self.canonical_class not in STATE_CLASSES:
            raise ContractError("INVALID_STATE_CLASS")
        if type(self.literals) is not tuple:
            raise ContractError("INVALID_STATE_LITERALS")
        keys = []
        for entry in self.literals:
            if type(entry) is not tuple or len(entry) != 3:
                raise ContractError("INVALID_STATE_LITERALS")
            _text(entry[0], "STATE_DATASET")
            _text(entry[1], "STATE_COLUMN")
            _texts(entry[2], "STATE_LITERALS")
            keys.append((entry[0], entry[1]))
        if keys != sorted(set(keys)):
            raise ContractError("UNSORTED_STATE_LITERALS")  # sorted and unique: one spelling per value
        _flag(self.excluded, "STATE_EXCLUDED")

    @classmethod
    def of(cls, canonical_class: str, literals: Mapping[str, Mapping[str, tuple[str, ...]]],
           excluded: bool = False) -> StateValue:
        return cls(canonical_class,
                   tuple(sorted((dataset, column, tuple(values))
                                for dataset, columns in literals.items()
                                for column, values in columns.items())),
                   excluded)

    def literals_for(self, dataset: str) -> tuple[tuple[str, tuple[str, ...]], ...] | None:
        found = tuple((column, values) for name, column, values in self.literals if name == dataset)
        return found or None

    def encode(self) -> tuple[str, ...]:
        mapped = ";".join(f"{_enc(name)}:{_enc(column)}:{_enc_list(values)}"
                          for name, column, values in self.literals)
        return _line(self.TAG, canonical_class=self.canonical_class, literals=mapped,
                     excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("canonical_class", "literals", "excluded"))
        triples = []
        for chunk in (raw["literals"].split(";") if raw["literals"] else ()):
            parts = chunk.split(":")
            if len(parts) != 3:
                raise ContractError("INVALID_VALUE_FIELDS")
            triples.append((_dec(parts[0]), _dec(parts[1]), _dec_list(parts[2])))
        return _canonical(cls(raw["canonical_class"], tuple(triples), _dec_flag(raw["excluded"])), items)


@dataclass(frozen=True, slots=True)
class ModalityValue(_Codec):
    """At least one of ``procedure_ids`` / ``family_policy``; ``excluded`` is the NEGATIVE honoured."""

    TAG = "modality@1"
    procedure_ids: tuple[str, ...]
    family_policy: str | None
    excluded: bool

    def __post_init__(self) -> None:
        _texts(self.procedure_ids, "MODALITY_PROCEDURES", empty=True)
        if self.family_policy is not None:
            _match(_POLICY, self.family_policy, "MODALITY_FAMILY_POLICY")
        if not self.procedure_ids and self.family_policy is None:
            raise ContractError("EMPTY_MODALITY_VALUE")
        _flag(self.excluded, "MODALITY_EXCLUDED")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, procedure_ids=_enc_list(self.procedure_ids),
                     family_policy=_enc_opt(self.family_policy), excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("procedure_ids", "family_policy", "excluded"))
        return _canonical(cls(_dec_list(raw["procedure_ids"]), _dec_opt(raw["family_policy"]),
                              _dec_flag(raw["excluded"])), items)


@dataclass(frozen=True, slots=True)
class ContractTypeValue(_Codec):
    """The residue of the quote must be empty: an authority that leaves residue returns CLARIFY."""

    TAG = "contract_type@1"
    type_ids: tuple[str, ...]
    excluded: bool

    def __post_init__(self) -> None:
        _texts(self.type_ids, "CONTRACT_TYPE_IDS")
        _flag(self.excluded, "CONTRACT_TYPE_EXCLUDED")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, type_ids=_enc_list(self.type_ids), excluded=_enc_flag(self.excluded))

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("type_ids", "excluded"))
        return _canonical(cls(_dec_list(raw["type_ids"]), _dec_flag(raw["excluded"])), items)


class OrderKey(str, Enum):
    VALUE = "value"
    DATE = "date"


class OrderDirection(str, Enum):
    ASC = "asc"
    DESC = "desc"


@dataclass(frozen=True, slots=True)
class OrderValue(_Codec):
    TAG = "order@1"
    key: OrderKey
    direction: OrderDirection

    def __post_init__(self) -> None:
        _enum(self.key, OrderKey, "ORDER_KEY")
        _enum(self.direction, OrderDirection, "ORDER_DIRECTION")

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, key=self.key.value, direction=self.direction.value)

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("key", "direction"))
        return _canonical(cls(OrderKey(raw["key"]), OrderDirection(raw["direction"])), items)


class CardinalityBasis(str, Enum):
    FIGURE = "figure"  # the user wrote a number: P8 announces the cap
    SINGULAR = "singular"  # «el contrato de mayor valor»: always exactly one


@dataclass(frozen=True, slots=True)
class CardinalityValue(_Codec):
    TAG = "cardinality@1"
    n: int
    basis: CardinalityBasis

    def __post_init__(self) -> None:
        _int(self.n, "CARDINALITY_N", 1, MAX_TOP_N)
        _enum(self.basis, CardinalityBasis, "CARDINALITY_BASIS")
        if self.basis is CardinalityBasis.SINGULAR and self.n != 1:
            raise ContractError("INVALID_CARDINALITY_SINGULAR")  # a singular is one, never «2»

    def encode(self) -> tuple[str, ...]:
        return _line(self.TAG, n=str(self.n), basis=self.basis.value)

    @classmethod
    def _decode(cls, items):
        raw = _fields(items, cls.TAG, ("n", "basis"))
        if not raw["n"].isascii() or not raw["n"].isdigit():
            raise ContractError("INVALID_VALUE_FIELDS")
        return _canonical(cls(int(raw["n"]), CardinalityBasis(raw["basis"])), items)


ExecutableValue = (TopicValue | BuyerValue | ActorValue | GeoValue | TimeValue | MoneyValue
                   | StateValue | ModalityValue | ContractTypeValue | OrderValue | CardinalityValue)
VALUE_TYPES: tuple[type, ...] = (TopicValue, BuyerValue, ActorValue, GeoValue, TimeValue, MoneyValue,
                                 StateValue, ModalityValue, ContractTypeValue, OrderValue,
                                 CardinalityValue)
_BY_TAG = {kind.TAG: kind for kind in VALUE_TYPES}


def is_executable_value(value: object) -> bool:
    return type(value) in VALUE_TYPES


def decode_value(items: tuple[str, ...]) -> ExecutableValue:
    """Decode a tagged tuple produced by ``encode``; anything else is ``ContractError``."""
    if type(items) is not tuple or not items or type(items[0]) is not str:
        raise ContractError("INVALID_VALUE_ENCODING")
    kind = _BY_TAG.get(items[0])
    if kind is None:
        raise ContractError("UNSUPPORTED_VALUE_TAG")
    return kind.decode(items)
