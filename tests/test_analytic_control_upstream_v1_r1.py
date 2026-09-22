"""Independent R1 review regressions, frozen before runtime remediation."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import json
import re
import sys

import pytest
from hypothesis import given, strategies as st

from app.core.analytic_control import AnalyticControlKind, AnalyticDimension, ControlPolarity
from app.core.analytic_operation import AnalyticOperationKind, derive_operation
from app.core.analytics.contract_activity import handle_contract_activity
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.models import AdmissionStatus, DecisionKind
from app.core.entity_resolver import EntityResolver
from app.core.explicit_content import ContentOccurrence, explicit_content_occurrences
from app.core.material_residual import MaterialEvidence, material_evidence
from app.core.query_router import QueryRouter
from app.core.query_text_consumption import mask_consumed_spans
from app.utils.money import normalize_text
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS
from tests.test_contract_activity_admission_v2 import RecordingContractSource


ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
CORPUS = json.loads(Path("tests/fixtures/analytic_control_upstream_v1_r1/adversarial.json").read_text())
assert len(CORPUS) >= 30


def _parse(raw: str):
    return ROUTER.parse(raw, today=date(2026, 9, 22))


def test_p1_a_distinct_galapa_subject_must_block_buyer_count() -> None:
    raw = "¿Cuántos contratos sobre Galapa firmó la Alcaldía de Galapa?"
    parsed = _parse(raw)
    assert "galapa" in parsed.material_residual
    admission = admit_contract_activity(
        raw, BUYER_PARAMS,
        semantic_evidence=parsed.semantic_evidence,
    )
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.reason == "UNSUPPORTED_MATERIAL_FILTER"


@pytest.mark.parametrize("word,buyer", [
    ("Galapa", "la Alcaldía de Galapa"),
    ("Barranquilla", "la Alcaldía de Barranquilla"),
    ("Atlántico", "la Gobernación del Atlántico"),
])
def test_same_lexeme_has_separate_buyer_and_content_spans(word: str, buyer: str) -> None:
    parsed = _parse(f"¿Cuántos contratos sobre {word} firmó {buyer}?")
    assert parsed.material_evidence is not None
    ownership = parsed.material_evidence.ownership
    assert ownership is not None and ownership.valid()
    assert len(ownership.content_spans) == 1
    (content_start, content_end), = ownership.content_spans
    buyer_start, buyer_end = ownership.buyer_span
    assert ownership.snapshot[content_start:content_end] == normalize_text(word)
    assert normalize_text(word) in ownership.snapshot[buyer_start:buyer_end]
    assert content_end <= buyer_start
    assert normalize_text(word) in parsed.material_residual
    assert MaterialEvidence.from_state(parsed.material_evidence.to_state()) == parsed.material_evidence


@pytest.mark.parametrize("raw,unowned,forbidden_tail", [
    ("contratos de CONCEJO MUNICIPIO DE CARMEN DE CARUPA", "concejo", "carupa"),
    ("contratos de ALCALDIA MUNICIPIO DE SAN FRANCISCO", "alcaldia", "francisco"),
])
def test_unresolved_full_buyer_span_does_not_leave_a_truncated_tail(
    raw: str, unowned: str, forbidden_tail: str,
) -> None:
    parsed = _parse(raw)
    assert unowned in parsed.material_residual
    assert forbidden_tail not in parsed.material_residual
    assert forbidden_tail not in (parsed.params.get("objeto") or ())


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


@pytest.mark.parametrize("raw", [
    "¿Cuántos contratos sobre Galapa firmó la Alcaldía de Galapa?",
    "¿Cuántos contratos cuyo objeto contenga 'por modalidad' firmó la Alcaldía de Galapa?",
])
def test_direct_ca_reuses_upstream_evidence_and_makes_zero_source_calls(raw: str) -> None:
    source = RecordingContractSource()
    decision = handle_contract_activity(
        raw, BUYER_PARAMS, source, semantic_evidence=_parse(raw).semantic_evidence,
    )
    assert decision.kind is DecisionKind.CLARIFY
    assert source.calls == []


def test_direct_ca_negative_coordination_executes_only_count() -> None:
    source = RecordingContractSource()
    decision = handle_contract_activity(
        "No agrupes por modalidad; tampoco por estado reportado; "
        "cuenta los contratos de la Alcaldía de Galapa",
        BUYER_PARAMS, source,
        semantic_evidence=_parse(
            "No agrupes por modalidad; tampoco por estado reportado; "
            "cuenta los contratos de la Alcaldía de Galapa"
        ).semantic_evidence,
    )
    assert decision.kind is DecisionKind.ANSWER
    assert decision.request is not None and decision.request.operation.value == "TOTAL"


@pytest.mark.parametrize("case", CORPUS, ids=lambda case: case["id"])
def test_adversarial_corpus(case: dict) -> None:
    parsed = _parse(case["raw"])
    controls = parsed.analytic_controls
    positive_groups = [
        control for control in controls
        if control.kind is AnalyticControlKind.GROUP
        and control.polarity is ControlPolarity.POSITIVE
        and control.dimension is not None
    ]
    negative_dimensions = [
        control.dimension.value for control in controls
        if control.kind is AnalyticControlKind.GROUP
        and control.polarity is ControlPolarity.NEGATIVE
    ]
    material = " ".join(parsed.material_residual)
    if case.get("material"):
        assert case["material"] in material, (case["id"], parsed.material_residual)
    elif "material" in case:
        assert not parsed.material_residual, (case["id"], parsed.material_residual)
    if "neg_dimensions" in case:
        assert negative_dimensions == case["neg_dimensions"]
        assert not positive_groups
    if case.get("group"):
        assert [control.dimension.value for control in positive_groups] == [case["group"]]
    elif case["family"] == "literal_content":
        assert not positive_groups
    if case.get("count"):
        assert derive_operation(controls).kind is AnalyticOperationKind.COUNT
    if case.get("no_count"):
        assert not any(control.kind is AnalyticControlKind.COUNT for control in controls)
    if case.get("state_filter"):
        assert parsed.params.get("estado_contrato")
    if case["family"] == "same_lexeme":
        admission = admit_contract_activity(
            case["raw"], parsed.params,
            semantic_evidence=parsed.semantic_evidence,
        )
        expected = (
            AdmissionStatus(case["admission"]) if case.get("admission")
            else AdmissionStatus.CLARIFY if case.get("material") else AdmissionStatus.READY
        )
        assert admission.status is expected


@given(st.sampled_from(["Galapa", "Barranquilla", "Atlántico", "Lunaria"]))
def test_property_distinct_offsets_do_not_collapse_owner(word: str) -> None:
    text = normalize_text(f"contratos sobre {word} de la Alcaldía de {word}")
    (content,) = explicit_content_occurrences(text)
    spans = [match.span() for match in re.finditer(normalize_text(word), text)]
    assert len(spans) == 2
    assert content.span == spans[0]
    assert content.span != spans[1]


@given(st.sampled_from(["por modalidad", "por estado reportado", "cuántos", "agrupa"]))
def test_property_accredited_literal_changes_control_to_content(phrase: str) -> None:
    raw = f"¿Cuántos contratos cuyo objeto contenga '{phrase}' firmó la Alcaldía de Galapa?"
    parsed = _parse(raw)
    assert normalize_text(phrase) in " ".join(parsed.material_residual)
    assert not any(
        control.kind is AnalyticControlKind.GROUP
        for control in parsed.analytic_controls
    )


@given(st.sampled_from(["modalidad", "estado reportado"]))
def test_property_compatible_negative_coordinator_cannot_flip_polarity(dimension: str) -> None:
    other = "estado reportado" if dimension == "modalidad" else "modalidad"
    parsed = _parse(f"No por {dimension}; tampoco por {other}; cuenta los contratos de Galapa")
    groups = [control for control in parsed.analytic_controls if control.kind is AnalyticControlKind.GROUP]
    assert len(groups) == 2
    assert all(control.polarity is ControlPolarity.NEGATIVE for control in groups)
    assert derive_operation(parsed.analytic_controls).kind is AnalyticOperationKind.COUNT


@given(st.sampled_from(["Galapa", "Barranquilla", "Atlántico", "Lunaria"]))
def test_property_masking_buyer_span_keeps_identical_material(word: str) -> None:
    text = normalize_text(f"contratos sobre {word} firmó la Alcaldía de {word}")
    buyer = text.index("alcaldia de ")
    residual = mask_consumed_spans(text, ((buyer, len(text)),))
    (content,) = explicit_content_occurrences(residual)
    assert residual[content.start:content.end] == normalize_text(word)
    assert material_evidence(residual).occurrences


def test_mutation_a_last_equal_surface_would_fail_offset_property(monkeypatch) -> None:
    def wrong_owner(text: str) -> tuple[ContentOccurrence, ...]:
        start = text.rindex("galapa")
        return (ContentOccurrence(start, start + len("galapa"), "galapa"),)

    monkeypatch.setattr(sys.modules[__name__], "explicit_content_occurrences", wrong_owner)
    with pytest.raises(AssertionError):
        test_property_distinct_offsets_do_not_collapse_owner.hypothesis.inner_test("Galapa")


def test_mutation_b_unprotected_literal_would_fail_content_property(monkeypatch) -> None:
    import app.core.query_router as query_router

    monkeypatch.setattr(query_router, "explicit_content_occurrences", lambda _text: ())
    with pytest.raises(AssertionError):
        test_property_accredited_literal_changes_control_to_content.hypothesis.inner_test(
            "por modalidad"
        )


def test_mutation_c_missing_coordinator_would_fail_polarity_property(monkeypatch) -> None:
    import app.core.analytic_control as analytic_control

    monkeypatch.setattr(analytic_control, "_NEGATIVE_CONTINUATION", {})
    with pytest.raises(AssertionError):
        test_property_compatible_negative_coordinator_cannot_flip_polarity.hypothesis.inner_test(
            "modalidad"
        )


def test_mutation_d_value_wide_buyer_deletion_would_fail_material_property(monkeypatch) -> None:
    monkeypatch.setattr(
        sys.modules[__name__], "mask_consumed_spans",
        lambda text, _spans: text.replace("galapa", " " * len("galapa")),
    )
    with pytest.raises(AssertionError):
        test_property_masking_buyer_span_keeps_identical_material.hypothesis.inner_test("Galapa")
