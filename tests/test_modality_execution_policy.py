from __future__ import annotations

import pytest

from app.core.modality_execution_policy import evaluate_modality_execution
from app.core.modality_filter import ModalityFilter
from app.core.modality_frame_adapter import ModalityInterpretation
from app.core.modality_resolution_models import ResolutionStatus


def _interpretation(
    status: ResolutionStatus,
    *,
    candidate: ModalityFilter | None = None,
) -> ModalityInterpretation:
    return ModalityInterpretation(
        resolution_status=status,
        executable_filter=ModalityFilter(),
        candidate_filter=candidate or ModalityFilter(),
    )


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (ResolutionStatus.NO_MATCH, "modality_absent"),
        (ResolutionStatus.RESOLVED, "modality_resolved"),
        (
            ResolutionStatus.RESOLVED_WITH_ASSUMPTION,
            "modality_resolved_with_assumption",
        ),
    ],
)
def test_executable_status_matrix(status, reason):
    decision = evaluate_modality_execution(_interpretation(status))
    assert decision.should_execute is True
    assert decision.reason == reason
    assert decision.missing_slots == ()
    assert decision.clarification_question is None


def test_none_is_absence_and_never_blocks():
    decision = evaluate_modality_execution(None)
    assert decision.should_execute is True
    assert decision.reason == "modality_absent"


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (
            ResolutionStatus.PARTIAL,
            "modality_partial_requires_clarification",
        ),
        (ResolutionStatus.AMBIGUOUS, "modality_ambiguous"),
        (ResolutionStatus.CONFLICT, "modality_conflict"),
        (ResolutionStatus.NOT_SEARCHABLE, "modality_not_searchable"),
        (
            ResolutionStatus.DATASET_INAPPLICABLE,
            "modality_dataset_inapplicable",
        ),
    ],
)
def test_blocked_status_matrix(status, reason):
    decision = evaluate_modality_execution(_interpretation(status))
    assert decision.should_execute is False
    assert decision.reason == reason
    assert decision.missing_slots == ("modalidad",)
    assert decision.risk_flags == (reason,)
    assert decision.clarification_question
    assert "procedure_id" not in decision.clarification_question


def test_partial_candidate_is_preserved_but_not_executed():
    candidate = ModalityFilter(
        include_procedure_ids=("licitacion_publica",)
    )
    interpretation = _interpretation(
        ResolutionStatus.PARTIAL, candidate=candidate
    )
    decision = evaluate_modality_execution(interpretation)

    assert interpretation.candidate_filter == candidate
    assert interpretation.executable_filter.is_empty()
    assert decision.should_execute is False


def test_wrong_interpretation_type_fails_closed():
    with pytest.raises(ValueError):
        evaluate_modality_execution({})  # type: ignore[arg-type]
