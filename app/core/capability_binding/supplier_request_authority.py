"""Pure deterministic admission authority for Natural Alpha SUPPLIERS proposals."""
from __future__ import annotations

from dataclasses import replace

from app.core.capability_binding.supplier_request_grammar import (
    CardinalityClaim,
    parse_cardinality,
)
from app.core.capability_binding.supplier_request_lexing import (
    Token,
    TokenKind,
    hard_segments,
    tokenize,
)
from app.core.capability_binding.supplier_request_types import (
    Authorization,
    CardinalityEvidence,
    CardinalityRelation,
    CardinalityResidueReason,
    EvidenceSpan,
    Polarity,
    SupplierMention,
    SupplierNoun,
    SupplierRequestEvidence,
    ordered_span_set,
)
from app.core.entity_profile.supplier_ranking_request import (
    DEFAULT_SUPPLIER_LIMIT,
    MAX_SUPPLIER_LIMIT,
)
from app.core.entity_profile.text_offsets import NormalizedText, normalize_with_offsets


_NOUNS = {
    "proveedor": SupplierNoun.PROVEEDOR,
    "proveedores": SupplierNoun.PROVEEDOR,
    "contratista": SupplierNoun.CONTRATISTA,
    "contratistas": SupplierNoun.CONTRATISTA,
}
_NEGATION = frozenset({
    "no", "sin", "ningun", "ninguno", "ninguna", "ningunos", "ningunas",
    "ni", "excepto", "excluye", "excluir", "excluido", "excluida",
    "excluidos", "excluidas", "exclusion",
})


def _span(source: NormalizedText, token: Token) -> EvidenceSpan | None:
    original = source.to_original((token.start, token.end))
    return EvidenceSpan((token.start, token.end), original) if original else None


def _polarity(segment: tuple[Token, ...], mention_indexes: tuple[int, ...]):
    mentions: dict[int, Polarity] = {}
    operators: list[Token] = []
    pending: Token | None = None
    ambiguous_pending = False
    complete = True
    mention_set = set(mention_indexes)
    for index, token in enumerate(segment):
        if token.text in _NEGATION:
            if index > 0 and index - 1 in mention_set and token.text == "no":
                mentions[index - 1] = Polarity.UNKNOWN
                operators.append(token)
            elif pending is not None:
                pending = token
                ambiguous_pending = True
                complete = False
                operators.append(token)
            else:
                pending = token
                operators.append(token)
        elif index in mention_set:
            mentions[index] = (
                Polarity.UNKNOWN
                if ambiguous_pending
                else (Polarity.NEGATED if pending else Polarity.POSITIVE)
            )
            pending = None
            ambiguous_pending = False
    if pending is not None and mention_indexes:
        mentions[mention_indexes[-1]] = Polarity.UNKNOWN
    complete = pending is None and complete and all(
        value is not Polarity.UNKNOWN for value in mentions.values()
    )
    return mentions, tuple(operators), complete


def _aggregate_polarity(values: tuple[Polarity, ...]) -> Polarity:
    states = set(values)
    if not values or Polarity.UNKNOWN in states:
        return Polarity.UNKNOWN
    if len(states) > 1:
        return Polarity.MIXED
    return values[0]


def _empty() -> SupplierRequestEvidence:
    return SupplierRequestEvidence(
        (),
        Polarity.UNKNOWN,
        CardinalityEvidence(CardinalityRelation.UNKNOWN),
        (),
        False,
        Authorization.NOT_ACCREDITED,
    )


def _reduce_claims(
    claims: tuple[CardinalityClaim, ...],
) -> tuple[
    CardinalityRelation,
    tuple[int, ...],
    bool,
    CardinalityResidueReason | None,
]:
    unknown = next((claim for claim in claims if not claim.complete), None)
    if unknown:
        return CardinalityRelation.UNKNOWN, (), False, unknown.residue_reason
    cardinal = tuple(
        claim for claim in claims if claim.relation is not CardinalityRelation.NONE
    )
    if len(cardinal) > 1:
        return (
            CardinalityRelation.UNKNOWN,
            (),
            False,
            CardinalityResidueReason.AMBIGUOUS_ATTACHMENT,
        )
    if not cardinal:
        return CardinalityRelation.NONE, (), True, None
    return cardinal[0].relation, cardinal[0].values, True, None


def _spans(
    source: NormalizedText,
    tokens: tuple[Token, ...],
) -> tuple[EvidenceSpan, ...] | None:
    spans = tuple(_span(source, token) for token in dict.fromkeys(tokens))
    if any(span is None for span in spans):
        return None
    return tuple(span for span in spans if span)


def assess_supplier_request(user_query: str) -> SupplierRequestEvidence:
    """Admit only complete positive NONE or exact configured-limit requests."""
    if not isinstance(user_query, str) or not user_query.strip():
        return _empty()
    source = normalize_with_offsets(user_query)
    if not source.faithful or not source.text:
        return _empty()

    mentions: list[SupplierMention] = []
    polarity_operators: list[Token] = []
    claims: list[CardinalityClaim] = []
    complete = True
    for position, segment in enumerate(hard_segments(tokenize(source.text))):
        indexes = tuple(
            index for index, token in enumerate(segment)
            if token.kind is TokenKind.SUPPLIER_NOUN
        )
        if not indexes:
            continue
        local_polarity, local_operators, polarity_complete = _polarity(segment, indexes)
        polarity_operators.extend(local_operators)
        complete &= polarity_complete
        for index in indexes:
            token = segment[index]
            mention_span = _span(source, token)
            if mention_span is None:
                return _empty()
            mentions.append(SupplierMention(_NOUNS[token.text], mention_span, local_polarity[index]))
            claim = parse_cardinality(segment, token, leading=position == 0)
            if local_polarity[index] is Polarity.NEGATED and claim.relation is not CardinalityRelation.NONE:
                claim = replace(claim, relation=CardinalityRelation.NEGATED)
            claims.append(claim)

    if not mentions:
        return _empty()

    polarity = _aggregate_polarity(tuple(mention.polarity for mention in mentions))
    relation, values, cardinal_complete, residue_reason = _reduce_claims(tuple(claims))
    complete &= cardinal_complete
    operator_tokens = tuple(dict.fromkeys((
        *polarity_operators,
        *(token for claim in claims for token in claim.operators),
    )))
    quantity_tokens = tuple(dict.fromkeys(
        token for claim in claims for token in claim.quantities
    ))
    residue_tokens = tuple(dict.fromkeys(
        token for claim in claims for token in claim.residue
    ))
    if (
        relation is CardinalityRelation.UNKNOWN
        and residue_reason is CardinalityResidueReason.AMBIGUOUS_ATTACHMENT
        and not residue_tokens
    ):
        residue_tokens = tuple(dict.fromkeys((*operator_tokens, *quantity_tokens)))
    operator_spans = _spans(source, operator_tokens)
    quantity_spans = _spans(source, quantity_tokens)
    residue_spans = _spans(source, residue_tokens)
    if operator_spans is None or quantity_spans is None or residue_spans is None:
        return _empty()

    cardinality = CardinalityEvidence(
        relation,
        values,
        operator_spans,
        quantity_spans,
        residue_spans,
        residue_reason,
    )
    try:
        consumed = ordered_span_set((
            *(mention.span for mention in mentions),
            *operator_spans,
            *quantity_spans,
            *residue_spans,
        ))
    except ValueError:
        return _empty()
    executable = (
        polarity is Polarity.POSITIVE
        and complete
        and (
            relation is CardinalityRelation.NONE
            or (
                relation is CardinalityRelation.EXACT
                and len(values) == 1
                and 1 <= values[0] <= MAX_SUPPLIER_LIMIT
            )
        )
    )
    return SupplierRequestEvidence(
        tuple(mentions),
        polarity,
        cardinality,
        consumed,
        complete,
        Authorization.EXECUTABLE if executable else Authorization.NOT_ACCREDITED,
    )
