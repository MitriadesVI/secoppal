"""Positive admission of the context surrounding a cardinal claim.

The grammar in :mod:`supplier_request_grammar` recognizes a closed set of
cardinal operators.  Recognition alone cannot decide admission: a query whose
operator is *not* in that set still leaves a bare quantity next to the noun, and
reading it as ``EXACT`` grants authority from ignorance.  Proving the absence of
an unmodelled operator would require enumerating Spanish.

So the direction of proof is inverted here.  A cardinal claim is admitted only
when both of its immediate boundaries are accredited:

- on the left, the start of the segment or a request opener;
- on the right, the end of the segment or a link into another dimension.

Anything else is residue, and residue fails closed.  A missing opener costs
coverage (the turn asks for clarification); a missing operator in the old
blacklist cost authority (the turn answered a different question in silence).
Only that asymmetry makes an incomplete list safe, so nothing is added to these
sets to "understand" a phrase — only because a real turn was observed using it.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.capability_binding.supplier_request_lexing import Token, TokenKind

# Observed opening the request in the dogfood corpus, plus same-lemma forms.
# These do not modify a quantity; they introduce the ask that carries it.
_REQUEST_OPENERS = frozenset({
    "busca", "buscame", "buscar",
    "cual", "cuales", "es", "sean", "son",
    "dame", "danos", "dar", "decir", "dime",
    "lista", "listame", "listar",
    "mostrar", "muestra", "muestrame",
    "necesitar", "necesito",
    "quiero", "quisiera",
    "vea", "veo", "ver",
})

# Observed continuing the mention into another dimension (buyer, period,
# ranking criterion).  A quantity never continues a mention through these.
_DIMENSION_LINKS = frozenset({
    "con", "de", "del", "e", "en", "para", "por", "que", "segun", "sobre", "y",
})

_TRANSPARENT = frozenset({
    "el", "la", "los", "las", "un", "una",
    "principales", "primeros", "mayores", "grandes",
})

# A coordination bounds the claim symmetrically with the right-hand side, so
# that "5 contratistas y proveedores" and its permutation are read alike.  It
# stops accrediting when it coordinates two quantities ("cuatro y cinco
# proveedores"), which is a competing cardinal, not a boundary.
_COORDINATORS = frozenset({"e", "o", "u", "y"})
_QUANTITY_KINDS = frozenset({
    TokenKind.BOUNDED_INTEGER,
    TokenKind.NUMBER_WORD,
    TokenKind.OVERSIZE_INTEGER,
    TokenKind.QUANTITY_PLACEHOLDER,
    TokenKind.NON_DECIMAL_DIGIT,
})


@dataclass(frozen=True, slots=True)
class ContextAdmission:
    """What the claim's boundaries leave unaccredited.

    ``segment_boundary`` has no surface of its own -- the unaccredited thing is
    on the other side of a hard break -- so it is reported apart from residue
    tokens instead of being given a span it does not have.
    """

    residue: tuple[Token, ...] = ()
    segment_boundary: bool = False
    # A second quantity bounding the claim is a competing cardinal, not context:
    # the host hands the sidecar only the claim's own quantity, so deferring
    # such a turn would ask the model about one number while the person wrote two.
    competing_quantity: bool = False


def _left_boundary(
    tokens: tuple[Token, ...],
    start: int,
    leading: bool,
) -> Token | None | bool:
    """The first token left of the claim that nothing accredits.

    Running out of tokens accredits the claim only in the *leading* segment.
    A later segment begins after a hard break, and a hard break is punctuation,
    not proof of absence: "superior a. 5 proveedores" and "máx. 5 proveedores"
    leave the operator on the other side of a period, where nothing in this
    segment can see it. Reading those as EXACT is the argument from ignorance
    this module exists to refuse, so the boundary itself is unaccredited --
    signalled as ``True``, since it has no token to point at.
    """
    index = start - 1
    while index >= 0 and tokens[index].text in _TRANSPARENT:
        index -= 1
    if index < 0:
        return None if leading else True
    token = tokens[index]
    # A previous mention bounds the claim: a noun cannot modify a quantity.
    if token.kind is TokenKind.SUPPLIER_NOUN or token.text in _REQUEST_OPENERS:
        return None
    if token.text in _COORDINATORS:
        previous = tokens[index - 1] if index > 0 else None
        if previous is not None and previous.kind in _QUANTITY_KINDS:
            return previous
        return None
    return token


def _right_boundary(tokens: tuple[Token, ...], end: int) -> Token | None:
    """Return the token right of the claim that nothing accredits."""
    if end >= len(tokens) or tokens[end].text in _DIMENSION_LINKS:
        return None
    return tokens[end]


def unaccredited_context(
    tokens: tuple[Token, ...],
    start: int,
    end: int,
    *,
    leading: bool,
) -> ContextAdmission:
    """Every boundary of the cardinal span that no authority accredits."""
    left = _left_boundary(tokens, start, leading)
    right = _right_boundary(tokens, end)
    residue = tuple(
        token for token in (left, right)
        if token is not None and token is not True
    )
    return ContextAdmission(
        residue,
        left is True,
        any(token.kind in _QUANTITY_KINDS for token in residue),
    )
