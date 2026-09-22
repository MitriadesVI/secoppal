"""Independent R1 review regressions, frozen before runtime remediation."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from app.core.analytic_control import AnalyticControlKind, AnalyticDimension, ControlPolarity
from app.core.analytic_operation import AnalyticOperationKind, derive_operation
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.models import AdmissionStatus
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS


ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _parse(raw: str):
    return ROUTER.parse(raw, today=date(2026, 9, 22))


def test_p1_a_distinct_galapa_subject_must_block_buyer_count() -> None:
    raw = "¿Cuántos contratos sobre Galapa firmó la Alcaldía de Galapa?"
    parsed = _parse(raw)
    assert "galapa" in parsed.material_residual
    admission = admit_contract_activity(
        raw, BUYER_PARAMS,
        analytic_controls=parsed.analytic_controls,
        material_residual=parsed.material_residual,
    )
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.reason == "UNSUPPORTED_MATERIAL_FILTER"


def test_p1_b_quoted_object_text_is_content_not_group() -> None:
    parsed = _parse(
        "¿Cuántos contratos cuyo objeto contenga 'por modalidad' firmó la Alcaldía de Galapa?"
    )
    assert derive_operation(parsed.analytic_controls).kind is AnalyticOperationKind.COUNT
    assert not any(control.kind is AnalyticControlKind.GROUP for control in parsed.analytic_controls)
    assert "por modalidad" in " ".join(parsed.material_residual)


def test_p1_c_tampoco_preserves_negative_group_and_positive_count() -> None:
    parsed = _parse(
        "No agrupes por modalidad; tampoco por estado reportado; "
        "cuenta los contratos de la Alcaldía de Galapa"
    )
    dimensions = [
        (control.kind, control.dimension, control.polarity)
        for control in parsed.analytic_controls
    ]
    assert dimensions == [
        (AnalyticControlKind.GROUP, AnalyticDimension.MODALITY, ControlPolarity.NEGATIVE),
        (AnalyticControlKind.GROUP, AnalyticDimension.REPORTED_STATE, ControlPolarity.NEGATIVE),
        (AnalyticControlKind.COUNT, None, ControlPolarity.POSITIVE),
    ]
    assert derive_operation(parsed.analytic_controls).kind is AnalyticOperationKind.COUNT
    assert "tampoco" not in parsed.material_residual
