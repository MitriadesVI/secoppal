"""Contrato inmutable de salida del resolver lingüístico de modalidades M2.

El módulo contiene únicamente enums y dataclasses. No importa parser, builder,
QueryFrame, HTTP, Radar ni componentes de ejecución. ``issues`` es la única
fuente almacenada para problemas locales; ``ambiguities`` es una proyección de
solo lectura derivada de esos issues.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ResolutionStatus(str, Enum):
    """Estado global de una resolución."""

    NO_MATCH = "no_match"
    RESOLVED = "resolved"
    RESOLVED_WITH_ASSUMPTION = "resolved_with_assumption"
    PARTIAL = "partial"
    AMBIGUOUS = "ambiguous"
    CONFLICT = "conflict"
    NOT_SEARCHABLE = "not_searchable"
    DATASET_INAPPLICABLE = "dataset_inapplicable"


class IssueType(str, Enum):
    """Clase cerrada de problema local conservado en una resolución."""

    AMBIGUITY = "ambiguity"
    CONFLICT = "conflict"
    NOT_SEARCHABLE = "not_searchable"
    DATASET_INAPPLICABLE = "dataset_inapplicable"


class MatchPolarity(str, Enum):
    INCLUDE = "include"
    EXCLUDE = "exclude"


class TargetKind(str, Enum):
    RETRIEVAL_FAMILY = "retrieval_family"
    PROCEDURE = "procedure"


class MatchStrength(str, Enum):
    EXACT_LEGAL = "exact_legal"
    EXACT_COMMON = "exact_common"
    COLLOQUIAL = "colloquial"
    CONTEXTUAL = "contextual"
    LEGACY_RAW = "legacy_raw"


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


@dataclass(frozen=True)
class ModalityMatch:
    """Un objetivo canónico reconocido, con span exacto del texto original."""

    target_kind: TargetKind
    target_id: str
    retrieval_family: str
    procedure_ids: tuple[str, ...]
    legal_selection_family: str | None
    procedure_variant: str | None
    submission_mode: str | None
    record_kind: str | None
    polarity: MatchPolarity

    source_text: str
    normalized_alias: str
    original_start: int
    original_end: int
    rule_id: str

    strength: MatchStrength
    confidence: Confidence
    assumption_code: str | None
    expansion_policy: str | None


@dataclass(frozen=True)
class ResolutionIssue:
    """Problema local, trazable y no destructivo de una resolución."""

    issue_type: IssueType
    source_text: str
    original_start: int
    original_end: int
    target_ids: tuple[str, ...]
    code: str
    explanation: str


@dataclass(frozen=True)
class ModalityAmbiguity:
    """Vista de compatibilidad derivada de un ``ResolutionIssue`` ambiguo."""

    source_text: str
    original_start: int
    original_end: int
    candidate_target_ids: tuple[str, ...]
    ambiguity_code: str
    explanation: str


@dataclass(frozen=True)
class ModalityResolution:
    """Resultado canónico, determinístico e inmutable del resolver.

    ``includes`` tiene semántica ``any_of``. ``issues`` conserva problemas
    locales junto a matches válidos, lo que permite un estado ``partial`` sin
    perder información sana de otras cláusulas o coordinaciones.
    """

    status: ResolutionStatus
    normalized_text: str

    includes: tuple[ModalityMatch, ...] = ()
    excludes: tuple[ModalityMatch, ...] = ()
    issues: tuple[ResolutionIssue, ...] = ()

    consumed_spans: tuple[tuple[int, int], ...] = ()
    residual_text: str = ""

    needs_clarification: bool = False
    clarification_code: str | None = None

    assumption_codes: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = field(default_factory=tuple)

    @property
    def ambiguities(self) -> tuple[ModalityAmbiguity, ...]:
        """Proyección read-only; nunca constituye una segunda fuente de verdad."""
        return tuple(
            ModalityAmbiguity(
                source_text=issue.source_text,
                original_start=issue.original_start,
                original_end=issue.original_end,
                candidate_target_ids=issue.target_ids,
                ambiguity_code=issue.code,
                explanation=issue.explanation,
            )
            for issue in self.issues
            if issue.issue_type is IssueType.AMBIGUITY
        )
