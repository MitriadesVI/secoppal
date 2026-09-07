"""Contrato puro para normalizar y representar nombres de actores — AP3-B.

Los nombres de SECOP son observaciones hechas por compradores, no razones
sociales registrales. Este módulo no consulta fuentes, no abre el índice y no
decide intención conversacional: congela la normalización N2 elegida por AP3-A
y los tipos que el índice/resolver intercambian.

Cambiar :data:`ACTOR_NAME_NORMALIZATION_VERSION` o la función asociada exige
reconstruir/migrar el índice. Una edición aparentemente inocua aquí cambia qué
aliases son exactos y, por tanto, cuáles pueden constituir autoridad.
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass

from app.core.actor_identity import IdentityKey

__all__ = [
    "ACTOR_NAME_NORMALIZATION_VERSION",
    "ActorResolutionCandidate",
    "ActorResolutionResult",
    "ActorResolutionStatus",
    "format_actor_identity_for_user",
    "mask_identity_key",
    "normalize_actor_name",
]


ACTOR_NAME_NORMALIZATION_VERSION = "N2"


def normalize_actor_name(value: str | None) -> str:
    """Normalización N2 de AP3-A: casefold, tildes/puntuación y whitespace.

    No retira formas societarias. En particular, ``S.A.S.``, ``LTDA`` y las
    marcas ``FUNDACIÓN``, ``UNIÓN TEMPORAL`` y ``CONSORCIO`` siguen formando
    parte del alias normalizado.
    """
    # NFKD va ANTES de casefold: la descomposicion de compatibilidad puede
    # reintroducir mayusculas ya plegadas. Con el orden inverso, '\u2116'
    # ("numero") daba 'No' en la primera pasada y 'no' en la segunda, y la
    # normalizacion dejaba de ser idempotente — un alias indexado no
    # encontraba su propia consulta. Lo encontro Hypothesis.
    decomposed = unicodedata.normalize("NFKD", (value or "").strip())
    folded_case = decomposed.casefold()
    without_accents = "".join(
        character
        for character in folded_case
        if not unicodedata.combining(character)
    )
    punctuation_as_space = "".join(
        character if character.isalnum() or character.isspace() else " "
        for character in without_accents
    )
    return " ".join(punctuation_as_space.split())


class ActorResolutionStatus:
    """Vocabulario cerrado del resolver local.

    Se usan constantes string, como en ``app.core.outcome``, para que el valor
    sea serializable sin adaptadores y no se mezcle con estados de routing.
    """

    RESOLVED = "RESOLVED"
    AMBIGUOUS = "AMBIGUOUS"
    CANDIDATES = "CANDIDATES"
    UNRESOLVED = "UNRESOLVED"
    INDEX_UNAVAILABLE = "INDEX_UNAVAILABLE"
    INCONCLUSIVE = "INCONCLUSIVE"


def mask_identity_key(identity: IdentityKey) -> str:
    """Representación humana que no vuelca una cédula completa."""
    if identity.document_type != "CC":
        return f"{identity.document_type}:{identity.base_number}"
    tail = identity.base_number[-4:]
    return f"CC:{'*' * max(0, len(identity.base_number) - len(tail))}{tail}"


def format_actor_identity_for_user(identity: IdentityKey) -> str:
    """Render a typed Actor Profile identity for intentional user display.

    Actor Profile uses the complete document as disambiguation evidence.  This
    deliberately separate primitive keeps ``mask_identity_key`` suitable for
    reprs, logs, exceptions, and other diagnostic surfaces.  The type comes
    only from ``IdentityKey``; an unknown document is never relabelled by its
    length.
    """
    if identity.document_type in {"NIT", "CC"}:
        return f"{identity.document_type} {identity.base_number}"
    return f"Documento {identity.base_number}"


@dataclass(frozen=True, slots=True, repr=False)
class ActorResolutionCandidate:
    """Un actor recuperado por alias exacto o por similitud léxica local.

    ``matched_alias`` es la forma N2 almacenada que produjo el match;
    ``observed_name`` conserva una escritura cruda observada en SECOP.
    ``retrieval_score`` es BM25 cuando aplica: sirve para ordenar candidatos,
    nunca expresa confianza de identidad.
    """

    identity: IdentityKey
    matched_alias: str
    observed_name: str
    occurrences: int
    first_seen: str | None
    last_seen: str | None
    rank: int
    retrieval_score: float | None = None
    alias_evidence: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.rank < 1:
            raise ValueError("candidate rank must be positive")
        if self.occurrences < 0:
            raise ValueError("candidate occurrences cannot be negative")

    @property
    def masked_identity(self) -> str:
        return mask_identity_key(self.identity)

    def __repr__(self) -> str:
        return (
            "ActorResolutionCandidate("
            f"identity={self.masked_identity!r}, matched_alias={self.matched_alias!r}, "
            f"observed_name={self.observed_name!r}, rank={self.rank!r})"
        )


@dataclass(frozen=True, slots=True, repr=False)
class ActorResolutionResult:
    """Resultado precision-first; sus invariantes impiden estados incoherentes."""

    status: str
    normalized_query: str
    identity: IdentityKey | None = None
    candidates: tuple[ActorResolutionCandidate, ...] = ()
    method: str | None = None
    reason: str | None = None
    candidates_has_more: bool | None = None
    enumeration_incomplete: bool = False

    def __post_init__(self) -> None:
        allowed = {
            ActorResolutionStatus.RESOLVED,
            ActorResolutionStatus.AMBIGUOUS,
            ActorResolutionStatus.CANDIDATES,
            ActorResolutionStatus.UNRESOLVED,
            ActorResolutionStatus.INDEX_UNAVAILABLE,
            ActorResolutionStatus.INCONCLUSIVE,
        }
        if self.status not in allowed:
            raise ValueError(f"invalid actor resolution status: {self.status!r}")
        if self.status == ActorResolutionStatus.RESOLVED:
            if self.identity is None:
                raise ValueError("RESOLVED requires identity")
            if self.candidates:
                raise ValueError("RESOLVED must not also expose candidates")
        elif self.identity is not None:
            raise ValueError("only RESOLVED may expose identity")
        if self.status in {
            ActorResolutionStatus.AMBIGUOUS,
            ActorResolutionStatus.CANDIDATES,
        } and not self.candidates:
            raise ValueError(f"{self.status} requires candidates")
        if self.status in {
            ActorResolutionStatus.UNRESOLVED,
            ActorResolutionStatus.INDEX_UNAVAILABLE,
            ActorResolutionStatus.INCONCLUSIVE,
        } and self.candidates:
            raise ValueError(f"{self.status} must not expose candidates")

        if self.status == ActorResolutionStatus.INCONCLUSIVE and (
            self.method != "exact_prefix_check_inconclusive" or not self.reason
        ):
            raise ValueError("INCONCLUSIVE requires prefix-check method and reason")
        if self.method == "exact_prefix_ambiguous":
            self._validate_prefix_candidates()

    def _validate_prefix_candidates(self):
        if self.status != ActorResolutionStatus.AMBIGUOUS or not 2 <= len(self.candidates) <= 10:
            raise ValueError("prefix ambiguity requires 2-10 visible candidates")
        if self.candidates_has_more is not None and type(self.candidates_has_more) is not bool:
            raise ValueError("has_more must be boolean or unknown")
        if type(self.enumeration_incomplete) is not bool:
            raise ValueError("enumeration_incomplete must be boolean")
        if self.candidates_has_more is True and len(self.candidates) != 10:
            raise ValueError("has_more requires all ten visible positions")
        if self.enumeration_incomplete and len(self.candidates) != 2:
            raise ValueError("incomplete enumeration preserves exact plus witness")
        if self.enumeration_incomplete != (self.candidates_has_more is None):
            raise ValueError("incomplete enumeration must have unknown has_more")
        if len({c.identity for c in self.candidates}) != len(self.candidates):
            raise ValueError("prefix candidates must have distinct identities")
        if self.candidates[0].matched_alias != self.normalized_query:
            raise ValueError("original exact alias must be first")
        for rank, candidate in enumerate(self.candidates, 1):
            if candidate.identity.document_type != "CC" or candidate.rank != rank:
                raise ValueError("prefix candidates require ordered CC ranks")
            if rank > 1 and not candidate.matched_alias.startswith(self.normalized_query + " "):
                raise ValueError("challenger must extend exact N2 prefix")
        ordered = sorted(self.candidates[1:], key=lambda c: (
            c.matched_alias, c.identity.document_type, c.identity.base_number,
        ))
        if list(self.candidates[1:]) != ordered:
            raise ValueError("challengers require deterministic representative order")

    def __repr__(self) -> str:
        identity = mask_identity_key(self.identity) if self.identity else None
        return (
            "ActorResolutionResult("
            f"status={self.status!r}, normalized_query={self.normalized_query!r}, "
            f"identity={identity!r}, candidates={self.candidates!r}, "
            f"method={self.method!r}, reason={self.reason!r})"
        )
