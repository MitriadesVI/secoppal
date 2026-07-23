"""Adapter puro M2 → contrato ejecutable de modalidad (M3B-1).

Traduce el resultado del resolver (``ModalityResolution``) a un envelope
específico ``ModalityInterpretation`` que separa lo ejecutable (``ModalityFilter``)
del diagnóstico lingüístico (issues, ambigüedades, aclaración). La traducción es
determinista y pura: no consulta el catálogo, no lee dataset, no construye SoQL
raw, no muta params externos y no importa QueryRouter, QueryFrame ni SoQLBuilder.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from app.core.modality_filter import ModalityFilter
from app.core.modality_resolution_models import (
    ModalityMatch,
    ModalityResolution,
    ResolutionIssue,
    ResolutionStatus,
)

# Estados cuyos matches sanos se aplican de inmediato (executable_filter).
_EXECUTABLE_STATUSES = frozenset({
    ResolutionStatus.RESOLVED,
    ResolutionStatus.RESOLVED_WITH_ASSUMPTION,
})
# Estado que conserva matches sanos pero NO los aplica todavía (candidate_filter).
_CANDIDATE_STATUSES = frozenset({ResolutionStatus.PARTIAL})


class ModalityAdaptationError(ValueError):
    """El contrato M2 recibido es incoherente para adaptar (p. ej. overlap)."""


@dataclass(frozen=True)
class ModalityInterpretation:
    """Envelope específico de modalidad: filtro ejecutable + diagnóstico.

    ``executable_filter`` es lo que una capa posterior aplicaría; ``candidate_filter``
    conserva matches sanos que aún no se aplican (solo ``PARTIAL``). El resto de
    campos son diagnóstico y NUNCA constituyen un filtro.
    """

    resolution_status: ResolutionStatus
    executable_filter: ModalityFilter
    candidate_filter: ModalityFilter
    issues: tuple[ResolutionIssue, ...] = ()
    assumption_codes: tuple[str, ...] = ()
    diagnostics: tuple[str, ...] = ()
    needs_clarification: bool = False
    clarification_code: str | None = None
    consumed_spans: tuple[tuple[int, int], ...] = ()
    residual_text: str = ""

    @property
    def has_executable_filter(self) -> bool:
        return not self.executable_filter.is_empty()


def _flatten_procedure_ids(matches: Iterable[ModalityMatch]) -> tuple[str, ...]:
    """Aplana, ordena y deduplica los procedure_ids de un grupo de matches."""
    ids: set[str] = set()
    for match in matches:
        ids.update(match.procedure_ids)
    return tuple(sorted(ids))


def _build_filter(resolution: ModalityResolution) -> ModalityFilter:
    """Construye el filtro a partir de los matches sanos, detectando overlap."""
    include_ids = _flatten_procedure_ids(resolution.includes)
    exclude_ids = _flatten_procedure_ids(resolution.excludes)
    overlap = set(include_ids) & set(exclude_ids)
    if overlap:
        raise ModalityAdaptationError(
            f"M2 entregó procedure_ids solapados entre include y exclude: {sorted(overlap)}"
        )
    if not include_ids and not exclude_ids:
        return ModalityFilter()
    return ModalityFilter(
        include_procedure_ids=include_ids,
        exclude_procedure_ids=exclude_ids,
    )


def adapt_modality_resolution(resolution: ModalityResolution) -> ModalityInterpretation:
    """Convierte un ``ModalityResolution`` de M2 en un ``ModalityInterpretation``.

    Política por status (los estados sin matches — NO_MATCH, AMBIGUOUS, CONFLICT,
    NOT_SEARCHABLE, DATASET_INAPPLICABLE — dejan ambos filtros vacíos; jamás se
    convierten candidatos ambiguos en filtro):

    * RESOLVED / RESOLVED_WITH_ASSUMPTION → executable con IDs, candidate vacío.
    * PARTIAL                             → executable vacío, candidate con IDs sanos.
    * resto                               → ambos vacíos.
    """
    if not isinstance(resolution, ModalityResolution):
        raise ModalityAdaptationError(
            f"se esperaba ModalityResolution, recibido {type(resolution).__name__}"
        )

    resolved_filter = _build_filter(resolution)
    empty = ModalityFilter()

    status = resolution.status
    if status in _EXECUTABLE_STATUSES:
        executable, candidate = resolved_filter, empty
    elif status in _CANDIDATE_STATUSES:
        executable, candidate = empty, resolved_filter
    else:
        executable, candidate = empty, empty

    return ModalityInterpretation(
        resolution_status=status,
        executable_filter=executable,
        candidate_filter=candidate,
        issues=tuple(resolution.issues),
        assumption_codes=tuple(resolution.assumption_codes),
        diagnostics=tuple(resolution.diagnostics),
        needs_clarification=resolution.needs_clarification,
        clarification_code=resolution.clarification_code,
        consumed_spans=tuple(resolution.consumed_spans),
        residual_text=resolution.residual_text,
    )
