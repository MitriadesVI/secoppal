"""Política fail-closed para ejecutar una interpretación de modalidad."""
from __future__ import annotations

from dataclasses import dataclass

from app.core.modality_frame_adapter import ModalityInterpretation
from app.core.modality_resolution_models import ResolutionStatus


@dataclass(frozen=True)
class ModalityExecutionDecision:
    should_execute: bool
    reason: str
    missing_slots: tuple[str, ...] = ()
    risk_flags: tuple[str, ...] = ()
    clarification_question: str | None = None


_EXECUTABLE_REASONS = {
    ResolutionStatus.NO_MATCH: "modality_absent",
    ResolutionStatus.RESOLVED: "modality_resolved",
    ResolutionStatus.RESOLVED_WITH_ASSUMPTION:
        "modality_resolved_with_assumption",
}

_BLOCKED = {
    ResolutionStatus.PARTIAL: (
        "modality_partial_requires_clarification",
        "Entendí parte de la modalidad, pero necesito que aclares la otra "
        "opción antes de buscar.",
    ),
    ResolutionStatus.AMBIGUOUS: (
        "modality_ambiguous",
        "La modalidad indicada puede significar varias opciones. "
        "¿Cuál quisiste decir?",
    ),
    ResolutionStatus.CONFLICT: (
        "modality_conflict",
        "La modalidad indicada combina condiciones incompatibles. "
        "¿Puedes reformularla?",
    ),
    ResolutionStatus.NOT_SEARCHABLE: (
        "modality_not_searchable",
        "Esa modalidad no está disponible como filtro de búsqueda. "
        "¿Quieres indicar otra?",
    ),
    ResolutionStatus.DATASET_INAPPLICABLE: (
        "modality_dataset_inapplicable",
        "Esa modalidad no está disponible en el conjunto de datos solicitado. "
        "¿Quieres cambiar el conjunto o la modalidad?",
    ),
}


def evaluate_modality_execution(
    interpretation: ModalityInterpretation | None,
) -> ModalityExecutionDecision:
    """Ausencia ejecuta; toda mención no ejecutable bloquea la búsqueda amplia."""
    if interpretation is None:
        return ModalityExecutionDecision(True, "modality_absent")
    if not isinstance(interpretation, ModalityInterpretation):
        raise ValueError(
            "interpretation debe ser ModalityInterpretation o None"
        )

    executable_reason = _EXECUTABLE_REASONS.get(
        interpretation.resolution_status
    )
    if executable_reason:
        return ModalityExecutionDecision(True, executable_reason)

    reason, question = _BLOCKED[interpretation.resolution_status]
    return ModalityExecutionDecision(
        should_execute=False,
        reason=reason,
        missing_slots=("modalidad",),
        risk_flags=(reason,),
        clarification_question=question,
    )
