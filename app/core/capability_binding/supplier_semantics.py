"""A polarity sidecar that can only withhold authority, never grant it.

The deterministic authority reads negation from an enumerated vocabulary, so a
construction it does not know ("salvo", "omitiendo", "quitando", "nada de")
reads as POSITIVE and opens SUPPLIERS against the user's stated intent. That is
the same fail-open class the cardinal admission already closed, in the one
dimension where no positional anchor exists: negation scope is unbounded.

X1 measured a dedicated call on 70 adversarial cases, 3 runs each: seven hard
gates at zero, against nine safety failures for the deterministic path on the
same cases. It also measured that the model never abstained -- UNKNOWN 0 of 258
-- so its safety could not be said to rest on declining to answer.

This wiring makes that reservation moot. The sidecar is consulted only once the
deterministic authority already intends to execute, and it can only veto:

    deterministic EXECUTABLE + sidecar says POSITIVE   -> execute
    deterministic EXECUTABLE + sidecar says anything else -> clarify
    deterministic NOT_ACCREDITED                        -> clarify (never asked)

So a wrong NEGATED costs coverage and a wrong POSITIVE cannot grant anything
the deterministic path was not already granting. The model is a second lock on
a door, not the key.
"""
from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from app.core.capability_binding.supplier_request_types import (
    Polarity,
    SupplierRequestEvidence,
)

LABELS = frozenset({"POSITIVE", "NEGATED", "MIXED", "UNKNOWN"})
# The relation the text establishes with a quantity the host already grounded.
# The model never supplies the number, only what the sentence does with it.
RELATIONS = frozenset({"EXACT", "LOWER_BOUND", "UPPER_BOUND", "RANGE", "NONE", "UNKNOWN"})


class VetoOutcome(str, Enum):
    CONFIRMED = "CONFIRMED"
    VETOED = "VETOED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class MentionRef:
    """One host-owned mention. The sidecar classifies these; it never finds them."""

    quote: str
    occurrence: int


@dataclass(frozen=True, slots=True)
class QuantityRef:
    """One literal quantity the host found. The model may only point at these."""

    quantity_ref: int
    quote: str
    value: int


@dataclass(frozen=True, slots=True)
class VetoVerdict:
    outcome: VetoOutcome
    turn_polarity: str
    detail: str = ""
    # Present only when the host asked about a quantity it could not read.
    relation: str = "NONE"
    relation_value: int | None = None

    @property
    def blocks(self) -> bool:
        return self.outcome is not VetoOutcome.CONFIRMED


def mention_refs(
    user_query: str,
    evidence: SupplierRequestEvidence,
) -> tuple[MentionRef, ...]:
    """The exhaustive ref list, taken from the authority's own mention spans."""
    refs: list[MentionRef] = []
    for mention in evidence.mentions:
        start, end = mention.span.original
        quote = user_query[start:end]
        if not quote:
            return ()
        # Word-bounded, because "proveedor" occurs inside "proveedores" and a
        # substring count would hand the model an occurrence that does not exist.
        # Case-sensitive as well as word-bounded: counting "Proveedores y
        # proveedores" without case gives the second mention occurrence 2,
        # which is not where that exact surface appears, and the host would
        # send the model a ref that does not exist as written.
        pattern = rf"(?<!\w){re.escape(quote)}(?!\w)"
        prior = len(re.findall(pattern, user_query[:start]))
        refs.append(MentionRef(quote, prior + 1))
    return tuple(refs)


def quantity_refs(
    user_query: str,
    evidence: SupplierRequestEvidence,
) -> tuple[QuantityRef, ...]:
    """Literal quantities the authority already located and resolved.

    They come from the authority's own spans, so the model is handed numbers
    that provably occur in the text and cannot introduce one of its own.
    """
    refs: list[QuantityRef] = []
    for index, span in enumerate(evidence.cardinality.quantity_spans, start=1):
        start, end = span.original
        quote = user_query[start:end]
        value = _resolve(quote)
        if not quote or value is None:
            return ()
        refs.append(QuantityRef(index, quote, value))
    return tuple(refs)


def _resolve(quote: str) -> int | None:
    from app.core.capability_binding.supplier_request_lexing import (
        Token,
        TokenKind,
        number_word_value,
    )

    text = quote.strip().lower()
    if text.isdecimal():
        return int(text)
    return number_word_value(Token(TokenKind.NUMBER_WORD, text, 0, len(text)))


def reduce_turn(labels: tuple[str, ...]) -> str:
    """Host-owned reduction. The sidecar never emits a turn state.

    A MIXED mention resolves to a MIXED turn rather than to any uniform state:
    it fits none of the other rules, and resolving it conservatively keeps the
    reduction unable to manufacture POSITIVE.
    """
    states = set(labels)
    if not labels or "UNKNOWN" in states:
        return "UNKNOWN"
    if "MIXED" in states:
        return "MIXED"
    if "POSITIVE" in states and "NEGATED" in states:
        return "MIXED"
    return labels[0]


def adjudicate(
    refs: tuple[MentionRef, ...],
    payload: object,
    quantities: tuple[QuantityRef, ...] = (),
) -> VetoVerdict:
    """Validate the sidecar's answer mechanically, then reduce it to a verdict.

    The host does not re-read the Spanish. It checks that the answer is a
    bijection onto the refs it sent and that every label is in the enum. Any
    breach is UNAVAILABLE, which blocks exactly like a veto: a malformed answer
    must never be softer than a negative one.
    """
    if not refs:
        return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "NO_REFS")
    if not isinstance(payload, dict):
        return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "NOT_AN_OBJECT")
    items = payload.get("mentions")
    if not isinstance(items, list):
        return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "MENTIONS_NOT_A_LIST")

    expected = {(ref.quote, ref.occurrence) for ref in refs}
    labels: dict[tuple[str, int], str] = {}
    for item in items:
        if not isinstance(item, dict):
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "MENTION_NOT_AN_OBJECT")
        quote, occurrence, label = item.get("quote"), item.get("occurrence"), item.get("polarity")
        if not isinstance(quote, str) or not isinstance(occurrence, int) or isinstance(occurrence, bool):
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "INVALID_REF_TYPES")
        if label not in LABELS:
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", f"INVALID_LABEL:{label!r}")
        key = (quote, occurrence)
        if key not in expected:
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "INVENTED_MENTION")
        if key in labels:
            return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "DUPLICATE_MENTION")
        labels[key] = label
    if set(labels) != expected:
        return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", "MISSING_MENTION")

    turn = reduce_turn(tuple(labels[(ref.quote, ref.occurrence)] for ref in refs))
    relation, value, detail = _cardinality(payload.get("cardinality"), quantities)
    if detail:
        return VetoVerdict(VetoOutcome.UNAVAILABLE, "UNKNOWN", detail)
    if turn != Polarity.POSITIVE.value:
        return VetoVerdict(VetoOutcome.VETOED, turn, relation=relation, relation_value=value)
    return VetoVerdict(VetoOutcome.CONFIRMED, turn, relation=relation, relation_value=value)


def _cardinality(
    claim: object,
    quantities: tuple[QuantityRef, ...],
) -> tuple[str, int | None, str]:
    """Validate the relation mechanically: it may only point at a host quantity.

    The host asked about a quantity only when it handed one over, so a claim
    that arrives unasked, or points at a number that was never sent, is a
    breach of the contract and blocks rather than being ignored.
    """
    if not quantities:
        # Nothing was asked; an unsolicited claim is not silently dropped.
        if claim not in (None, {}):
            return "UNKNOWN", None, "UNSOLICITED_CARDINALITY"
        return "NONE", None, ""
    if claim is None:
        return "UNKNOWN", None, "MISSING_QUANTITY_REF"
    if not isinstance(claim, dict):
        return "UNKNOWN", None, "CARDINALITY_NOT_AN_OBJECT"
    relation = claim.get("relation")
    if relation not in RELATIONS:
        return "UNKNOWN", None, f"INVALID_RELATION:{relation!r}"
    pointer = claim.get("quantity_ref")
    if isinstance(pointer, bool) or not isinstance(pointer, int):
        return "UNKNOWN", None, "INVALID_QUANTITY_REF"
    match = next((q for q in quantities if q.quantity_ref == pointer), None)
    if match is None:
        return "UNKNOWN", None, "INVENTED_QUANTITY"
    return relation, match.value, ""


# The seam the contract depends on: a callable, so the wiring can be replaced
# by a stub in tests without the contract ever importing a network client.
PolarityVeto = Callable[
    [str, tuple[MentionRef, ...], tuple[QuantityRef, ...]], VetoVerdict,
]
