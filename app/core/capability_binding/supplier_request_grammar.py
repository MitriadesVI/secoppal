"""Explicit local productions for SUPPLIERS cardinality requests.

Only ``SOFT_PUNCT`` disappears between terminals.  Every other token either
participates in a production or bounds it; no number is attached by proximity.
"""
from __future__ import annotations

from dataclasses import dataclass, replace

from app.core.capability_binding.supplier_cardinal_admission import (
    unaccredited_context,
)
from app.core.capability_binding.supplier_request_lexing import (
    Token,
    TokenKind,
    number_word_value,
)
from app.core.capability_binding.supplier_request_types import (
    CardinalityRelation,
    CardinalityResidueReason,
)


# ``unos/unas`` are excluded on purpose: they approximate the quantity they
# precede, so skipping them would hide a modifier inside the claim.
_DETERMINERS = frozenset({"el", "la", "los", "las", "un", "una"})
_RANK_MODIFIERS = frozenset({"principales", "primeros", "mayores", "grandes"})
_EXACT_OP = (("exactamente",), ("precisamente",), ("justo",), ("top",))
_LOWER_PRE = (
    ("como", "minimo"),
    ("al", "menos"),
    ("mas", "de"),
    ("minimo", "de"),
    ("minimo",),
)
_UPPER_PRE = (
    ("a", "lo", "sumo"),
    ("como", "maximo"),
    ("menos", "de"),
    ("maximo", "de"),
    ("hasta",),
    ("maximo",),
)
_LOWER_POST = (("por", "lo", "menos"), ("como", "minimo"), ("o", "mas"))
_UPPER_POST = (("a", "lo", "sumo"), ("como", "maximo"), ("o", "menos"))
_RANGE = (("entre", "y"), ("de", "a"))
_RANGE_HEAD = (("entre",),)
_DANGLING_PREFIX = frozenset({
    "mas", "menos", "minimo", "maximo", "hasta", "entre", "sumo",
    "exactamente", "justo", "precisamente",
})
_DANGLING_POST = (("por", "lo"), ("a", "lo"), ("o",))
_QUANTITY_KINDS = frozenset({TokenKind.BOUNDED_INTEGER, TokenKind.NUMBER_WORD})
_INVALID_QUANTITY_KINDS = frozenset({
    TokenKind.OVERSIZE_INTEGER,
    TokenKind.QUANTITY_PLACEHOLDER,
    TokenKind.NON_DECIMAL_DIGIT,
})


@dataclass(frozen=True, slots=True)
class CardinalityClaim:
    relation: CardinalityRelation
    values: tuple[int, ...] = ()
    operators: tuple[Token, ...] = ()
    quantities: tuple[Token, ...] = ()
    residue: tuple[Token, ...] = ()
    residue_reason: CardinalityResidueReason | None = None
    complete: bool = True
    start: int = 0
    end: int = 0


def _significant(segment: tuple[Token, ...]) -> tuple[Token, ...]:
    return tuple(token for token in segment if token.kind is not TokenKind.SOFT_PUNCT)


def _value(token: Token) -> int | None:
    if token.kind is TokenKind.BOUNDED_INTEGER:
        return int(token.text)
    return number_word_value(token)


def _quantity(token: Token) -> bool:
    return token.kind in _QUANTITY_KINDS and _value(token) is not None


def _match_left(
    tokens: tuple[Token, ...],
    end: int,
    phrases: tuple[tuple[str, ...], ...],
) -> tuple[int, tuple[Token, ...]] | None:
    for phrase in phrases:
        start = end - len(phrase) + 1
        if start >= 0 and tuple(token.text for token in tokens[start:end + 1]) == phrase:
            return start, tokens[start:end + 1]
    return None


def _match_right(
    tokens: tuple[Token, ...],
    start: int,
    phrases: tuple[tuple[str, ...], ...],
) -> tuple[int, tuple[Token, ...]] | None:
    for phrase in phrases:
        end = start + len(phrase)
        if tuple(token.text for token in tokens[start:end]) == phrase:
            return end, tokens[start:end]
    return None


def _nominal_start(tokens: tuple[Token, ...], mention: int) -> int:
    start = mention
    while start > 0 and tokens[start - 1].text in _RANK_MODIFIERS:
        start -= 1
    return start


def _determiner_start(tokens: tuple[Token, ...], start: int) -> int:
    while start > 0 and tokens[start - 1].text in _DETERMINERS:
        start -= 1
    return start


def _base_claim(tokens: tuple[Token, ...], mention: int) -> CardinalityClaim:
    nominal = _nominal_start(tokens, mention)
    quantity_index = nominal - 1
    if quantity_index < 0 or not _quantity(tokens[quantity_index]):
        start = _determiner_start(tokens, nominal)
        top = _match_left(tokens, start - 1, (("top",),))
        if top:
            start = _determiner_start(tokens, top[0])
        return CardinalityClaim(CardinalityRelation.NONE, start=start, end=mention + 1)

    quantity = tokens[quantity_index]
    quantity_value = _value(quantity)

    for relation, phrases in (
        (CardinalityRelation.LOWER, _LOWER_PRE),
        (CardinalityRelation.UPPER, _UPPER_PRE),
    ):
        operator = _match_left(tokens, quantity_index - 1, phrases)
        if operator:
            start = _determiner_start(tokens, operator[0])
            return CardinalityClaim(
                relation,
                (quantity_value,),
                operator[1],
                (quantity,),
                start=start,
                end=mention + 1,
            )

    for head, link in _RANGE:
        link_match = _match_left(tokens, quantity_index - 1, ((link,),))
        if not link_match:
            continue
        first_index = link_match[0] - 1
        if first_index < 0 or not _quantity(tokens[first_index]):
            continue
        head_match = _match_left(tokens, first_index - 1, ((head,),))
        if not head_match:
            continue
        start = _determiner_start(tokens, head_match[0])
        first = tokens[first_index]
        return CardinalityClaim(
            CardinalityRelation.RANGE,
            (_value(first), quantity_value),
            (*head_match[1], *link_match[1]),
            (first, quantity),
            start=start,
            end=mention + 1,
        )

    exact_operator = _match_left(tokens, quantity_index - 1, _EXACT_OP)
    start = exact_operator[0] if exact_operator else quantity_index
    start = _determiner_start(tokens, start)
    # A rank modifier sits between the quantity and the noun and is part of the
    # same cardinal claim ("5 principales proveedores"), so the claim owns it:
    # the grammar already knows these tokens -- _nominal_start walks them -- and
    # leaving them outside the claim made the host read them as material.
    modifiers = tuple(
        token for token in tokens[quantity_index + 1:mention]
        if token.text in _RANK_MODIFIERS
    )
    claim = CardinalityClaim(
        CardinalityRelation.EXACT,
        (quantity_value,),
        exact_operator[1] if exact_operator else (),
        (quantity, *modifiers),
        start=start,
        end=mention + 1,
    )
    lower_post = _match_right(tokens, mention + 1, _LOWER_POST)
    upper_post = _match_right(tokens, mention + 1, _UPPER_POST)
    if lower_post and upper_post:
        return _unknown(
            claim,
            (*lower_post[1], *upper_post[1]),
            CardinalityResidueReason.UNKNOWN_OPERATOR,
        )
    if lower_post:
        return replace(
            claim,
            relation=CardinalityRelation.LOWER,
            operators=(*claim.operators, *lower_post[1]),
            end=lower_post[0],
        )
    if upper_post:
        return replace(
            claim,
            relation=CardinalityRelation.UPPER,
            operators=(*claim.operators, *upper_post[1]),
            end=upper_post[0],
        )
    return claim


def _unknown(
    claim: CardinalityClaim,
    residue: tuple[Token, ...],
    reason: CardinalityResidueReason,
) -> CardinalityClaim:
    return replace(
        claim,
        relation=CardinalityRelation.UNKNOWN,
        values=(),
        residue=tuple(dict.fromkeys((*claim.residue, *residue))),
        residue_reason=reason,
        complete=False,
    )


def _residue(
    tokens: tuple[Token, ...],
    mention: int,
    claim: CardinalityClaim,
) -> tuple[tuple[Token, ...], CardinalityResidueReason | None]:
    neighbors = range(max(0, claim.start - 1), min(len(tokens), claim.end + 1))
    comparators = tuple(tokens[index] for index in neighbors
                        if tokens[index].kind is TokenKind.COMPARATOR_SYMBOL)
    if comparators:
        return comparators, CardinalityResidueReason.UNKNOWN_OPERATOR

    nominal = _nominal_start(tokens, mention)
    pre_slot = nominal - 1
    post_slot = mention + 1
    for index in (pre_slot, post_slot):
        if 0 <= index < len(tokens) and tokens[index].kind in _INVALID_QUANTITY_KINDS:
            reason = (
                CardinalityResidueReason.OVERSIZE_INTEGER
                if tokens[index].kind is TokenKind.OVERSIZE_INTEGER
                else CardinalityResidueReason.UNKNOWN_QUANTITY
            )
            return (tokens[index],), reason
        if 0 <= index < len(tokens) and tokens[index].kind is TokenKind.NUMBER_WORD:
            if _value(tokens[index]) is None:
                return (tokens[index],), CardinalityResidueReason.UNKNOWN_QUANTITY

    adjacent_quantities = tuple(
        tokens[index]
        for index in (claim.start - 1, claim.end)
        if 0 <= index < len(tokens) and (
            _quantity(tokens[index]) or tokens[index].kind in _INVALID_QUANTITY_KINDS
        )
    )
    if adjacent_quantities:
        reason = (
            CardinalityResidueReason.OVERSIZE_INTEGER
            if any(token.kind is TokenKind.OVERSIZE_INTEGER for token in adjacent_quantities)
            else CardinalityResidueReason.AMBIGUOUS_ATTACHMENT
        )
        return adjacent_quantities, reason

    if claim.quantities:
        quantity_index = tokens.index(claim.quantities[-1])
        if (
            quantity_index >= 2
            and tokens[quantity_index - 1].text == "que"
            and tokens[quantity_index - 2].kind is TokenKind.WORD
        ):
            return (
                tokens[quantity_index - 2],
                tokens[quantity_index - 1],
            ), CardinalityResidueReason.AMBIGUOUS_ATTACHMENT

    left_operator = _match_left(
        tokens,
        claim.start - 1,
        (*_EXACT_OP, *_LOWER_PRE, *_UPPER_PRE, *_RANGE_HEAD),
    )
    lower_post = _match_right(tokens, claim.end, _LOWER_POST)
    upper_post = _match_right(tokens, claim.end, _UPPER_POST)
    if lower_post and lower_post[0] < len(tokens) and _quantity(tokens[lower_post[0]]):
        if tuple(token.text for token in lower_post[1]) in _LOWER_PRE:
            lower_post = None
    if upper_post and upper_post[0] < len(tokens) and _quantity(tokens[upper_post[0]]):
        if tuple(token.text for token in upper_post[1]) in _UPPER_PRE:
            upper_post = None
    if left_operator or lower_post or upper_post:
        residue = (
            left_operator[1] if left_operator
            else (lower_post[1] if lower_post else upper_post[1])
        )
        return residue, CardinalityResidueReason.UNKNOWN_OPERATOR
    if claim.start > 0 and tokens[claim.start - 1].text in _DANGLING_PREFIX:
        return (tokens[claim.start - 1],), CardinalityResidueReason.UNKNOWN_OPERATOR
    dangling_post = _match_right(tokens, claim.end, _DANGLING_POST)
    if dangling_post:
        return dangling_post[1], CardinalityResidueReason.UNKNOWN_OPERATOR
    return (), None


def parse_cardinality(
    segment: tuple[Token, ...],
    mention_token: Token,
    *,
    leading: bool,
) -> CardinalityClaim:
    """Parse the one local cardinal/nominal production owned by a mention.

    ``leading`` says whether this segment opens the text. It is required rather
    than defaulted: the permissive value is the one that accredits a claim, and
    a caller that forgets it must not be the one that grants authority.
    """
    tokens = _significant(segment)
    mention = tokens.index(mention_token)
    claim = _base_claim(tokens, mention)
    residue, reason = _residue(tokens, mention, claim)
    if not reason and claim.relation is not CardinalityRelation.NONE:
        admission = unaccredited_context(
            tokens, claim.start, claim.end, leading=leading,
        )
        residue = admission.residue
        # A boundary breach is named apart from ordinary residue because the
        # contract must not defer it to the sidecar: no evidence was ever
        # gathered on what the model does with an operator across a period.
        reason = (
            CardinalityResidueReason.SEGMENT_BOUNDARY if admission.segment_boundary
            else CardinalityResidueReason.AMBIGUOUS_ATTACHMENT if admission.competing_quantity
            else (CardinalityResidueReason.UNACCREDITED_CONTEXT if residue else None)
        )
    return _unknown(claim, residue, reason) if reason else claim
