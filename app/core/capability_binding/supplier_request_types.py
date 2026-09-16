"""Nominal evidence emitted by the SUPPLIERS request authority."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class SupplierNoun(str, Enum):
    PROVEEDOR = "PROVEEDOR"
    CONTRATISTA = "CONTRATISTA"


class Polarity(str, Enum):
    POSITIVE = "POSITIVE"
    NEGATED = "NEGATED"
    MIXED = "MIXED"
    UNKNOWN = "UNKNOWN"


class CardinalityRelation(str, Enum):
    NONE = "NONE"
    EXACT = "EXACT"
    LOWER = "LOWER"
    UPPER = "UPPER"
    RANGE = "RANGE"
    NEGATED = "NEGATED"
    UNKNOWN = "UNKNOWN"


class CardinalityResidueReason(str, Enum):
    UNKNOWN_OPERATOR = "UNKNOWN_OPERATOR"
    UNKNOWN_QUANTITY = "UNKNOWN_QUANTITY"
    AMBIGUOUS_ATTACHMENT = "AMBIGUOUS_ATTACHMENT"
    OVERSIZE_INTEGER = "OVERSIZE_INTEGER"
    UNACCREDITED_CONTEXT = "UNACCREDITED_CONTEXT"
    SEGMENT_BOUNDARY = "SEGMENT_BOUNDARY"


class Authorization(str, Enum):
    EXECUTABLE = "EXECUTABLE"
    NOT_ACCREDITED = "NOT_ACCREDITED"


@dataclass(frozen=True, slots=True, order=True)
class EvidenceSpan:
    """One half-open span in normalized and original Python-char coordinates."""

    normalized: tuple[int, int]
    original: tuple[int, int]


@dataclass(frozen=True, slots=True)
class SupplierMention:
    noun: SupplierNoun
    span: EvidenceSpan
    polarity: Polarity


@dataclass(frozen=True, slots=True)
class CardinalityEvidence:
    relation: CardinalityRelation
    values: tuple[int, ...] = ()
    operator_spans: tuple[EvidenceSpan, ...] = ()
    quantity_spans: tuple[EvidenceSpan, ...] = ()
    residue_spans: tuple[EvidenceSpan, ...] = ()
    residue_reason: CardinalityResidueReason | None = None


@dataclass(frozen=True, slots=True)
class SupplierRequestEvidence:
    mentions: tuple[SupplierMention, ...]
    polarity: Polarity
    cardinality: CardinalityEvidence
    consumed_spans: tuple[EvidenceSpan, ...]
    complete: bool
    authorization: Authorization


def ordered_span_set(spans: tuple[EvidenceSpan, ...]) -> tuple[EvidenceSpan, ...]:
    """Return a stable, non-overlapping SpanSet or fail closed."""
    ordered = tuple(sorted(set(spans), key=lambda span: span.normalized))
    if any(
        left.normalized[1] > right.normalized[0]
        for left, right in zip(ordered, ordered[1:])
    ):
        raise ValueError("overlapping supplier evidence spans")
    return ordered
