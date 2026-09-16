"""DOGFOOD_CARDINAL_AMBIGUITY_GUARD — a temporary belt, not a new authority.

Experiment a2-supplier-semantics-x2 measured the cardinality half of the
sidecar on 50 surfaces, 3 runs each, and adjudicated NO_GO on one gate:
``UNKNOWN_AS_EXACT_5``, 11 of 360 calls. Every opening of an executable route
came from the same family — a quantity modified by an approximate or
non-equality phrase the local grammar does not model, which the sidecar then
read as an exact request for that number:

    diferente de 5 proveedores …   distinto de 5 proveedores …
    alrededor de 5 proveedores …   unos 5 proveedores …
    casi 5 proveedores …           5 proveedores más o menos …

The dogfood decision is to *not solve* them. This module does not interpret
those phrases and does not widen the schema: it recognizes the surfaces that
were observed to fail and blocks the turn before the sidecar is ever asked, so
they end in a host-authored clarification with zero source calls.

Two properties keep it honest:

- It only ever blocks. A phrase it fails to recognize is handled exactly as it
  was before this module existed, so nothing here can grant authority.
- It is bound to a quantity the request authority already located, so it cannot
  fire on a number that belongs to some other dimension.

It is scoped to the personal dogfood pilot and is expected to be removed once a
measured cardinality authority replaces it. Nothing is added to these sets to
"understand" Spanish — only because a corpus turn was observed using it.
"""
from __future__ import annotations

from app.core.capability_binding.supplier_request_lexing import (
    Token,
    TokenKind,
    hard_segments,
    tokenize,
)
from app.core.capability_binding.supplier_request_types import (
    EvidenceSpan,
    SupplierRequestEvidence,
)
from app.core.entity_profile.text_offsets import normalize_with_offsets

APPROXIMATE = "APPROXIMATE"
NOT_EQUAL = "NOT_EQUAL"
OPEN_ENDED = "OPEN_ENDED"

# Immediately left of the quantity (soft punctuation aside). x2: K37-K40, K42-K43.
_PREFIXES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("diferente", "de"), NOT_EQUAL),
    (("diferentes", "de"), NOT_EQUAL),
    (("distinto", "de"), NOT_EQUAL),
    (("distinta", "de"), NOT_EQUAL),
    (("distintos", "de"), NOT_EQUAL),
    (("distintas", "de"), NOT_EQUAL),
    (("alrededor", "de"), APPROXIMATE),
    (("unos",), APPROXIMATE),
    (("unas",), APPROXIMATE),
    (("casi",), APPROXIMATE),
)

# Immediately after the quantity's noun group ("5 proveedores más o menos"),
# not anywhere in the segment: "5 proveedores de 2020 en adelante" opens a
# different dimension, and the phrase there modifies the year, not the count.
# x2: K44, and K21 ("en adelante"), which the grammar read as EXACT.
_SUFFIXES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("mas", "o", "menos"), APPROXIMATE),
    (("en", "adelante"), OPEN_ENDED),
)
_NOUN_GROUP = frozenset({"principales", "primeros", "mayores", "grandes"})


def _significant(segment: tuple[Token, ...]) -> tuple[Token, ...]:
    return tuple(token for token in segment if token.kind is not TokenKind.SOFT_PUNCT)


def _prefix(tokens: tuple[Token, ...], quantity: int) -> str | None:
    for phrase, label in _PREFIXES:
        start = quantity - len(phrase)
        if start >= 0 and tuple(t.text for t in tokens[start:quantity]) == phrase:
            return label
    return None


def _suffix(tokens: tuple[Token, ...], quantity: int) -> str | None:
    index = quantity + 1
    while index < len(tokens) and (
        tokens[index].kind is TokenKind.SUPPLIER_NOUN or tokens[index].text in _NOUN_GROUP
    ):
        index += 1
    for phrase, label in _SUFFIXES:
        if tuple(t.text for t in tokens[index:index + len(phrase)]) == phrase:
            return label
    return None


def ambiguous_cardinal_surface(
    user_query: str,
    evidence: SupplierRequestEvidence,
) -> str | None:
    """The ambiguity class around a located quantity, or ``None``.

    ``None`` means only that nothing was recognized; it is never a claim that
    the cardinality is clear.
    """
    located: set[EvidenceSpan] = set(evidence.cardinality.quantity_spans)
    if not located:
        return None
    source = normalize_with_offsets(user_query)
    if not source.faithful or not source.text:
        # Spans could not be trusted upstream either; the authority already
        # failed closed, so there is nothing here to protect.
        return None
    normalized = {span.normalized for span in located}
    for segment in hard_segments(tokenize(source.text)):
        tokens = _significant(segment)
        for index, token in enumerate(tokens):
            if (token.start, token.end) not in normalized:
                continue
            label = _prefix(tokens, index) or _suffix(tokens, index)
            if label is not None:
                return label
    return None
