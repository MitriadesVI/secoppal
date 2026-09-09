"""Ocurrencias acreditadas del turno, con sus relaciones explícitas.

Un claim es evidencia de ENTRADA: dice que una autoridad concreta reconoció
un valor tipado en un tramo concreto del texto que la persona escribió. No
decide identidades, no elige fuentes y no ejecuta nada.

Lo que este módulo aporta sobre la representación de V1.1 es el **vínculo**.
V1.1 guardaba listas paralelas —fuentes por un lado, eventos por otro— y quien
las consumía volvía a adivinar quién gobernaba a quién. Con un escalar por
dimensión, "contratos publicados y procesos firmados" y "procesos publicados y
contratos firmados" producían el mismo estado, porque la asociación vivía sólo
en el texto y el texto ya no estaba.

Aquí la asociación es un dato: ``EVENT --HAS_SUBJECT--> SOURCE``. Si una
autoridad no puede acreditarla, el turno lleva una cuestión sin resolver. No se
inventa el vínculo que haría ejecutable la consulta: ésa es exactamente la
sustitución que la revisión independiente encontró y que bloquea el compilador.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from app.core.entity_profile.frame_values import (
    Period,
    Selection,
    SourceKind,
    StatePredicate,
    TemporalEventKind,
    TopicNode,
    ValueConstraint,
)


class ClaimDimension(str, Enum):
    BUYER = "BUYER"
    SOURCE = "SOURCE"
    EVENT = "EVENT"
    PERIOD = "PERIOD"
    CURRENT_STATE = "CURRENT_STATE"
    CONTRACT_UNIVERSE = "CONTRACT_UNIVERSE"
    TOPIC = "TOPIC"
    MODALITY = "MODALITY"
    CONTRACT_TYPE = "CONTRACT_TYPE"
    VALUE = "VALUE"
    FOCUS = "FOCUS"
    OPERATION = "OPERATION"


class RelationKind(str, Enum):
    HAS_SUBJECT = "HAS_SUBJECT"
    MODIFIES = "MODIFIES"
    PERIOD_OF = "PERIOD_OF"
    COORDINATES = "COORDINATES"
    EXCLUDES = "EXCLUDES"


# Las relaciones que expresan GOBIERNO. Un ciclo aquí significa que dos
# ocurrencias se reclaman mutuamente como sujeto, y eso no es una lectura
# ambigua sino una estructura imposible.
_GOVERNING = (RelationKind.HAS_SUBJECT, RelationKind.PERIOD_OF, RelationKind.MODIFIES)

_VALUE_TYPE: dict[ClaimDimension, type | tuple[type, ...]] = {
    ClaimDimension.BUYER: str,
    ClaimDimension.SOURCE: SourceKind,
    ClaimDimension.EVENT: TemporalEventKind,
    ClaimDimension.PERIOD: Period,
    ClaimDimension.CURRENT_STATE: StatePredicate,
    ClaimDimension.CONTRACT_UNIVERSE: str,
    ClaimDimension.TOPIC: TopicNode,
    ClaimDimension.MODALITY: Selection,
    ClaimDimension.CONTRACT_TYPE: Selection,
    ClaimDimension.VALUE: ValueConstraint,
    ClaimDimension.FOCUS: str,
    ClaimDimension.OPERATION: str,
}


class ClaimValidationError(ValueError):
    """Un conjunto de claims mal formado se rechaza; nunca se repara."""


@dataclass(frozen=True, slots=True)
class ClaimRelation:
    kind: RelationKind
    target_claim_id: str


@dataclass(frozen=True, slots=True)
class SemanticClaim:
    """Una ocurrencia: qué reconoció qué autoridad, y dónde."""

    id: str
    turn_id: str
    span: tuple[int, int]
    dimension: ClaimDimension
    canonical_value: object
    authority: str
    authority_version: str
    relations: tuple[ClaimRelation, ...] = ()

    def related(self, kind: RelationKind) -> tuple[str, ...]:
        return tuple(r.target_claim_id for r in self.relations if r.kind is kind)


@dataclass(frozen=True, slots=True)
class UnresolvedIssue:
    """Lo que una autoridad NO pudo acreditar, con su motivo."""

    code: str
    detail: str = ""
    terms: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ClaimSet:
    """Todas las ocurrencias de un turno, validadas como conjunto."""

    turn_id: str
    text: str
    claims: tuple[SemanticClaim, ...] = ()
    unresolved: tuple[UnresolvedIssue, ...] = ()
    _index: dict[str, SemanticClaim] = field(
        default_factory=dict, compare=False, repr=False,
    )

    def __post_init__(self) -> None:
        index: dict[str, SemanticClaim] = {}
        for claim in self.claims:
            _validate_claim(claim, self.turn_id, len(self.text))
            if claim.id in index:
                raise ClaimValidationError(f"duplicate claim id: {claim.id}")
            index[claim.id] = claim
        for claim in self.claims:
            for relation in claim.relations:
                if relation.target_claim_id == claim.id:
                    raise ClaimValidationError(f"self relation on {claim.id}")
                if relation.target_claim_id not in index:
                    raise ClaimValidationError(
                        f"relation to unknown claim: {relation.target_claim_id}",
                    )
        _reject_governing_cycles(self.claims, index)
        object.__setattr__(self, "_index", index)

    def get(self, claim_id: str) -> SemanticClaim:
        return self._index[claim_id]

    def by_dimension(self, dimension: ClaimDimension) -> tuple[SemanticClaim, ...]:
        return tuple(c for c in self.claims if c.dimension is dimension)

    def subject_of(self, claim: SemanticClaim) -> SemanticClaim | None:
        """La ocurrencia que gobierna a ésta, si alguna la reclamó."""
        targets = claim.related(RelationKind.HAS_SUBJECT)
        return self._index[targets[0]] if len(targets) == 1 else None

    def governed_by(
        self, claim: SemanticClaim, kind: RelationKind,
    ) -> tuple[SemanticClaim, ...]:
        """Las ocurrencias que declaran a ésta como su objetivo."""
        return tuple(
            other for other in self.claims
            if claim.id in other.related(kind)
        )

    @property
    def has_unresolved(self) -> bool:
        return bool(self.unresolved)

    def issue(self, code: str) -> UnresolvedIssue | None:
        for item in self.unresolved:
            if item.code == code:
                return item
        return None


def _validate_claim(claim: SemanticClaim, turn_id: str, length: int) -> None:
    if claim.turn_id != turn_id:
        raise ClaimValidationError(
            f"claim {claim.id} belongs to turn {claim.turn_id!r}, not {turn_id!r}",
        )
    if not claim.authority or not claim.authority_version:
        raise ClaimValidationError(f"claim {claim.id} has no accredited authority")
    _validate_span(claim, length)
    expected = _VALUE_TYPE[claim.dimension]
    if not isinstance(claim.canonical_value, expected):
        raise ClaimValidationError(
            f"claim {claim.id} carries {type(claim.canonical_value).__name__} "
            f"for dimension {claim.dimension.value}",
        )


def _validate_span(claim: SemanticClaim, length: int) -> None:
    span = claim.span
    if not isinstance(span, tuple) or len(span) != 2:
        raise ClaimValidationError(f"claim {claim.id} has a malformed span")
    start, end = span
    # ``bool`` es subclase de ``int``: sin esta comprobación ``True`` pasaría
    # como offset 1 y un span inventado quedaría acreditado.
    if isinstance(start, bool) or isinstance(end, bool):
        raise ClaimValidationError(f"claim {claim.id} uses a bool as offset")
    if not isinstance(start, int) or not isinstance(end, int):
        raise ClaimValidationError(f"claim {claim.id} has non-integer offsets")
    if not 0 <= start < end <= length:
        raise ClaimValidationError(
            f"claim {claim.id} span {span} is outside the turn text",
        )


def _reject_governing_cycles(
    claims: tuple[SemanticClaim, ...], index: dict[str, SemanticClaim],
) -> None:
    edges = {
        claim.id: tuple(
            r.target_claim_id for r in claim.relations if r.kind in _GOVERNING
        )
        for claim in claims
    }
    state: dict[str, int] = {}

    def walk(node: str) -> None:
        if state.get(node) == 1:
            raise ClaimValidationError(f"governing cycle through {node}")
        if state.get(node) == 2:
            return
        state[node] = 1
        for target in edges.get(node, ()):
            walk(target)
        state[node] = 2

    for claim_id in index:
        walk(claim_id)
