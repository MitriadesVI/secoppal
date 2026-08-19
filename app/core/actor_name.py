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
    folded_case = (value or "").casefold().strip()
    decomposed = unicodedata.normalize("NFKD", folded_case)
    without_accents = "".join(
        character
        for character in decomposed
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


def mask_identity_key(identity: IdentityKey) -> str:
    """Representación humana que no vuelca una cédula completa."""
    if identity.document_type != "CC":
        return f"{identity.document_type}:{identity.base_number}"
    tail = identity.base_number[-4:]
    return f"CC:{'*' * max(0, len(identity.base_number) - len(tail))}{tail}"


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

    def __post_init__(self) -> None:
        allowed = {
            ActorResolutionStatus.RESOLVED,
            ActorResolutionStatus.AMBIGUOUS,
            ActorResolutionStatus.CANDIDATES,
            ActorResolutionStatus.UNRESOLVED,
            ActorResolutionStatus.INDEX_UNAVAILABLE,
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
        } and self.candidates:
            raise ValueError(f"{self.status} must not expose candidates")

    def __repr__(self) -> str:
        identity = mask_identity_key(self.identity) if self.identity else None
        return (
            "ActorResolutionResult("
            f"status={self.status!r}, normalized_query={self.normalized_query!r}, "
            f"identity={identity!r}, candidates={self.candidates!r}, "
            f"method={self.method!r}, reason={self.reason!r})"
        )
