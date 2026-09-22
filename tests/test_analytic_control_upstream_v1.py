"""ANALYTIC_CONTROL_UPSTREAM_V1 — analytic instructions are control, not content.

Frozen before the runtime change (A1): against the WIP baseline these cases
reproduced ``objeto=[desglosa, modalidad, contratos]``, ``objeto_exclude=
[agrupes, modalidad]``, ``ordering_signal=fecha_desc`` from "actual", an
active-state family from "vigente", and the silent loss of "gas"/"TIC".
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest
from hypothesis import given, settings, strategies as st

from app.config import Settings, get_settings
from app.core.analytic_control import (
    AnalyticControlKind as K,
    AnalyticControlSpan,
    AnalyticDimension as D,
    ControlPolarity as P,
    controls_from_state,
    locate_analytic_controls,
)
from app.core.analytic_operation import AnalyticOperationKind, derive_operation
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.models import AdmissionStatus
from app.core.capability_binding.ranking_criterion import ranking_criterion_spans
from app.core.conversation_store import ConversationStore
from app.core.entity_resolver import EntityResolver
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.utils.money import normalize_text
from tests.test_contract_activity_admission_v2 import RecordingContractSource
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS
from tests.test_natural_alpha_contract import IDENTITY

TODAY = date(2026, 9, 22)
FIXTURE = Path("tests/fixtures/contract_activity_admission_v2/frozen_payloads.jsonl")
ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
TOPIC_KEYS = ("objeto", "objeto_or", "objeto_phrases", "objeto_exclude")
CONTROL_WORDS = {
    "desglosa", "desglosados", "distribuye", "distribucion", "agrupa", "agrupes",
    "agrupados", "agrupalos", "reparto", "cuenta", "cuentame", "cuente", "cuantos",
    "modalidad", "modalidades", "estado", "reportado", "vigente", "actual",
    "figuran", "observado", "consultar", "listar", "total", "numero", "cada",
}
ASPECT_OPERATION = {
    "CONTRACT_COUNT": (AnalyticOperationKind.COUNT, None),
    "CONTRACTS_BY_MODALITY": (AnalyticOperationKind.GROUP, D.MODALITY),
    "CONTRACTS_BY_STATE": (AnalyticOperationKind.GROUP, D.REPORTED_STATE),
}


def _parse(raw: str):
    return ROUTER.parse(raw, today=TODAY)


def _topic_tokens(params: dict) -> set[str]:
    tokens: set[str] = set()
    for key in TOPIC_KEYS:
        for item in params.get(key) or ():
            for value in item if isinstance(item, list) else (item,):
                tokens.update(normalize_text(str(value)).split())
    return tokens


def _shape(controls) -> list[tuple]:
    return [(c.kind, c.dimension, c.polarity, c.evidence_text) for c in controls]


def _frozen_positives() -> list[tuple[str, str, str]]:
    cases = []
    for line in FIXTURE.read_text().splitlines():
        record = json.loads(line)
        if record.get("id", "")[:2] in {"cc", "cm", "cs"}:
            (job,) = json.loads(record["payload"])["jobs"]
            cases.append((record["id"], record["raw"], job["aspect"]))
    assert len(cases) == 18
    return cases


# ── A. The 18 frozen positives ──────────────────────────────────────────────

@pytest.mark.parametrize(("case_id", "raw", "aspect"), _frozen_positives())
def test_frozen_positive_derives_alpha_operation_and_leaks_no_control(
    case_id: str, raw: str, aspect: str,
) -> None:
    parsed = _parse(raw)
    operation = derive_operation(parsed.analytic_controls)
    assert (operation.kind, operation.dimension) == ASPECT_OPERATION[aspect], case_id
    assert not operation.reason, case_id
    assert not (_topic_tokens(parsed.params) & CONTROL_WORDS), (case_id, parsed.params)
    assert not parsed.params.get("objeto_exclude"), case_id
    assert not parsed.params.get("ordering_signal"), case_id
    assert not parsed.params.get("estado_family"), case_id
    assert parsed.material_residual == (), case_id
    assert not [r for r in parsed.negation_requests if r.dimension.value == "OBJECT"], case_id


def test_frozen_positive_spans_are_typed_with_polarity() -> None:
    by_id = {case_id: raw for case_id, raw, _ in _frozen_positives()}
    assert _shape(_parse(by_id["cc05"]).analytic_controls) == [
        (K.GROUP, D.MODALITY, P.NEGATIVE, "no los agrupes por modalidad"),
        (K.COUNT, None, P.POSITIVE, "cuentame"),
    ]
    assert _shape(_parse(by_id["cc06"]).analytic_controls) == [
        (K.LIST, None, P.NEGATIVE, "sin listar cada contrato"),
        (K.COUNT, None, P.POSITIVE, "cuantos"),
    ]
    assert _shape(_parse(by_id["cm05"]).analytic_controls) == [
        (K.TOTAL_ONLY, None, P.NEGATIVE, "no me des solo el total"),
        (K.GROUP, None, P.POSITIVE, "agrupa"),
        (K.GROUP, D.MODALITY, P.POSITIVE, "por modalidad"),
    ]
    assert _shape(_parse(by_id["cs04"]).analytic_controls) == [
        (K.GROUP, None, P.POSITIVE, "agrupa"),
        (K.GROUP, D.REPORTED_STATE, P.POSITIVE, "por estado reportado"),
        (K.GROUP, D.MODALITY, P.NEGATIVE, "no por modalidad"),
    ]


def test_reported_state_breakdown_does_not_filter_active_states_or_order() -> None:
    cs06 = _parse(
        "Quiero el reparto por estado vigente reportado de los contratos de "
        "Cartagena firmados en 2025."
    )
    assert "estado_contrato" not in cs06.params
    cs01 = _parse("Desglosa por estado reportado actual los contratos de la Alcaldía de Galapa.")
    assert "ordering_signal" not in cs01.params
    cs02 = _parse("¿Cuántos contratos de la Gobernación del Atlántico figuran hoy en cada estado?")
    # "hoy" reports currentness of the state; it is not a one-day signing period.
    assert "fecha_desde" not in cs02.params and "fecha_hasta" not in cs02.params


def test_frozen_negatives_keep_their_disposition() -> None:
    neg01 = _parse("¿Cuántas contrataciones hizo la Alcaldía de Galapa en 2025?")
    assert derive_operation(neg01.analytic_controls).kind is AnalyticOperationKind.COUNT
    neg04 = _parse("Agrupa los contratos de Soledad por modalidad y por estado reportado.")
    assert derive_operation(neg04.analytic_controls).reason == "MULTIPLE_BREAKDOWNS"
    neg06 = _parse("¿En qué estado estaban estos contratos al cierre de 2024?")
    assert neg06.analytic_controls == ()


# ── B. Mandatory cases ──────────────────────────────────────────────────────

@pytest.mark.parametrize(
    ("raw", "operation", "material"),
    [
        ("Desglosa por modalidad los contratos de gas de la Alcaldía de Galapa",
         (AnalyticOperationKind.GROUP, D.MODALITY), ("gas",)),
        ("¿Cuántos contratos de TIC firmó la Alcaldía de Galapa en 2025?",
         (AnalyticOperationKind.COUNT, None), ("tic",)),
    ],
)
def test_short_material_survives_even_when_objeto_is_empty(raw, operation, material) -> None:
    parsed = _parse(raw)
    derived = derive_operation(parsed.analytic_controls)
    assert (derived.kind, derived.dimension) == operation
    assert parsed.material_residual == material
    # The lossy extractor drops it; the evidence may not.
    assert not parsed.params.get("objeto")
    admission = admit_contract_activity(
        raw, parsed.params,
        semantic_evidence=parsed.semantic_evidence,
    )
    assert admission.status is AdmissionStatus.CLARIFY
    assert admission.reason in {"UNSUPPORTED_MATERIAL_FILTER", "BUYER_SCOPE_REQUIRED"}
    # Same turn, same buyer, without the material: Contract Activity would run.
    forced = admit_contract_activity(
        raw, BUYER_PARAMS, semantic_evidence=parsed.semantic_evidence,
    )
    assert forced.reason == "UNSUPPORTED_MATERIAL_FILTER"


def test_negation_stops_at_a_later_analytic_instruction() -> None:
    parsed = _parse("contratos sin alimentación desglosados por modalidad de la Alcaldía de Galapa")
    assert parsed.params["objeto_exclude"] == ["alimentacion"]
    (request,) = parsed.negation_requests
    assert request.value == "alimentacion"
    assert _shape(parsed.analytic_controls) == [
        (K.GROUP, None, P.POSITIVE, "desglosados"),
        (K.GROUP, D.MODALITY, P.POSITIVE, "por modalidad"),
    ]


def test_negated_group_and_positive_count_are_two_occurrences() -> None:
    parsed = _parse("No los agrupes por modalidad; cuéntame los contratos de la Alcaldía de Galapa")
    assert _shape(parsed.analytic_controls) == [
        (K.GROUP, D.MODALITY, P.NEGATIVE, "no los agrupes por modalidad"),
        (K.COUNT, None, P.POSITIVE, "cuentame"),
    ]
    assert derive_operation(parsed.analytic_controls).kind is AnalyticOperationKind.COUNT
    assert "objeto_exclude" not in parsed.params
    assert parsed.negation_requests == ()


@pytest.mark.parametrize(
    "raw",
    ["proveedores con mayor número de contratos", "qué entidad tiene mayor número de contratos"],
)
def test_ranking_metric_is_never_an_independent_count(raw: str) -> None:
    normalized = normalize_text(raw)
    assert ranking_criterion_spans(normalized)
    assert locate_analytic_controls(
        normalized, protected_spans=ranking_criterion_spans(normalized),
    ) == ()
    assert _parse(raw).analytic_controls == ()


@pytest.mark.parametrize(
    ("raw", "material"),
    [
        ("contratos de distribución de agua potable en Malambo", {"distribucion", "agua"}),
        ("contratos de manejo de cuentas bancarias de la Alcaldía de Galapa",
         {"cuentas", "bancarias"}),
        ("contratos para el estudio del estado de la malla vial de Barranquilla",
         {"estado", "malla", "vial"}),
        ("Busca contratos cuyo objeto mencione modalidades de contratación", {"modalidades"}),
        ("cuenta de cobro de contratos de prestación de servicios en Soledad", {"cuenta", "cobro"}),
        ("contratos de la Distribuidora del Caribe", {"distribuidora"}),
    ],
)
def test_dimension_and_command_words_stay_content_without_a_command(raw, material) -> None:
    parsed = _parse(raw)
    assert parsed.analytic_controls == ()
    assert material <= set(parsed.material_residual)


def test_modality_value_after_criterion_is_a_filter_not_a_breakdown() -> None:
    parsed = _parse("contratos de la Alcaldía de Galapa por modalidad de contratación directa")
    assert parsed.analytic_controls == ()
    assert parsed.params["modalidad_include_procedure_ids"]


def test_dimension_name_inside_municipal_candidate_is_still_the_dimension() -> None:
    parsed = _parse(
        "Distribuye los contratos de la Gobernación del Atlántico según su modalidad de contratación."
    )
    assert derive_operation(parsed.analytic_controls).dimension is D.MODALITY
    assert not parsed.params.get("objeto")


def test_negated_object_without_instruction_keeps_its_old_extent() -> None:
    parsed = _parse("contratos sin obras nuevas en Cali")
    assert parsed.analytic_controls == ()
    assert [request.value for request in parsed.negation_requests] == ["obras nuevas"]


def test_coordinated_imperatives_are_two_operations() -> None:
    parsed = _parse("Cuenta los contratos de Galapa en 2026 y agrúpalos por modalidad.")
    assert derive_operation(parsed.analytic_controls).reason == "MULTIPLE_OPERATIONS"
    single = _parse("Cuenta por modalidad los contratos de Galapa en 2026.")
    assert derive_operation(single.analytic_controls).kind is AnalyticOperationKind.GROUP


def test_controls_round_trip_through_workflow_state() -> None:
    parsed = _parse("No los agrupes por modalidad; cuéntame los contratos de la Alcaldía de Galapa")
    state = [control.to_state() for control in parsed.analytic_controls]
    assert json.loads(json.dumps(state)) == state
    assert controls_from_state(state) == parsed.analytic_controls
    assert all(isinstance(c, AnalyticControlSpan) for c in controls_from_state(state))


# ── C. Properties ───────────────────────────────────────────────────────────

_BASES = [
    "contratos de aseo de la Alcaldía de Galapa",
    "contratos de gas de la Alcaldía de Galapa",
    "contratos de TIC de la Alcaldía de Galapa",
    "contratos de distribución de agua de la Alcaldía de Galapa",
    "contratos de vigilancia de Barranquilla",
    "contratos de mantenimiento de vías de Soledad",
]
_CLAUSES = [
    ("{base} desglosados por modalidad", (AnalyticOperationKind.GROUP, D.MODALITY)),
    ("{base} agrupados por estado reportado", (AnalyticOperationKind.GROUP, D.REPORTED_STATE)),
    ("cuántos {base} hay", (AnalyticOperationKind.COUNT, None)),
    ("Desglosa por modalidad los {base}", (AnalyticOperationKind.GROUP, D.MODALITY)),
    ("Cuenta los {base}", (AnalyticOperationKind.COUNT, None)),
]


def _material(parsed) -> tuple:
    return (
        parsed.material_residual,
        parsed.params.get("objeto"),
        parsed.params.get("objeto_or"),
        parsed.params.get("objeto_phrases"),
        parsed.params.get("objeto_exclude"),
        parsed.params.get("estado_contrato"),
        parsed.params.get("ordering_signal"),
    )


@settings(max_examples=60, deadline=None)
@given(base=st.sampled_from(_BASES), clause=st.sampled_from(_CLAUSES))
def test_adding_an_analytic_clause_never_changes_the_subject(base, clause) -> None:
    template, operation = clause
    plain = _parse(base)
    instructed = _parse(template.format(base=base))
    assert _material(instructed) == _material(plain)
    derived = derive_operation(instructed.analytic_controls)
    assert (derived.kind, derived.dimension) == operation


_OWNED_SURFACES = st.sampled_from([
    "mayor número de contratos", "mayor cantidad de contratos", "más contratos",
])
_FILLERS = st.sampled_from(["proveedores con", "qué entidad tiene", "cuáles contratistas tienen"])


@settings(max_examples=40, deadline=None)
@given(prefix=_FILLERS, owned=_OWNED_SURFACES)
def test_a_span_owned_by_ranking_is_never_claimed(prefix, owned) -> None:
    normalized = normalize_text(f"{prefix} {owned} en Galapa")
    protected = ranking_criterion_spans(normalized)
    controls = locate_analytic_controls(normalized, protected_spans=protected)
    for control in controls:
        assert not any(
            control.start < end and start < control.end for start, end in protected
        )


@settings(max_examples=40, deadline=None)
@given(
    text=st.text(alphabet="abcdeilmnoprstuy ", min_size=0, max_size=60),
    protected=st.lists(st.tuples(st.integers(0, 60), st.integers(0, 60)), max_size=3),
)
def test_no_control_ever_overlaps_a_protected_span(text, protected) -> None:
    spans = [(min(a, b), max(a, b)) for a, b in protected]
    for control in locate_analytic_controls(text, protected_spans=spans):
        assert not any(control.start < end and start < control.end for start, end in spans)
        assert text[control.start:control.end] == control.evidence_text


# ── D. Owner or limitation (no silent downgrade) ────────────────────────────

@pytest.fixture
def _config(monkeypatch):
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _flow(tmp_path: Path) -> SecopalWorkflow:
    settings_ = Settings(
        _env_file=None,
        deepseek_api_key=None,
        secoppal_auth_enabled=True,
        secoppal_natural_alpha_mode="off",
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.db",
    )
    flow = SecopalWorkflow(settings_)
    flow.conv_store = ConversationStore(tmp_path / "conversations")
    flow.secop_client = RecordingContractSource()
    flow.natural_alpha_provider = Mock(observe=Mock(return_value=ProviderResult("{}", 0)))
    return flow


@pytest.mark.parametrize(
    ("raw", "notice"),
    [
        ("¿Cuántos contratos de obra firmó Barranquilla en 2025?",
         "ℹ️ No tengo un conteo certificado"),
        ("contratos de aseo de Barranquilla agrupados por modalidad",
         "ℹ️ No puedo agrupar esta consulta por modalidad"),
    ],
)
def test_search_never_silently_answers_an_analytic_instruction(
    tmp_path: Path, _config, raw: str, notice: str,
) -> None:
    flow = _flow(tmp_path)
    result = flow.run_query(raw, channel="streamlit", user_identity=IDENTITY, today=TODAY)
    assert result.get("analytical_intent") != "contract_activity"
    assert result["needs_clarification"] is False
    assert result["response"].startswith(notice)


def test_unowned_process_count_stops_or_declares(tmp_path: Path, _config) -> None:
    flow = _flow(tmp_path)
    result = flow.run_query(
        "cuántos procesos abiertos hay en atlantico", channel="streamlit",
        user_identity=IDENTITY, today=TODAY,
    )
    # Today the opportunity policy asks for a topic first (fail-closed); if it
    # ever answers, the answer must carry the limitation.
    assert result["needs_clarification"] is True or result["response"].startswith(
        "ℹ️ No tengo un conteo certificado"
    )


def test_plain_search_carries_no_limitation(tmp_path: Path, _config) -> None:
    flow = _flow(tmp_path)
    result = flow.run_query(
        "contratos de aseo de Barranquilla", channel="streamlit",
        user_identity=IDENTITY, today=TODAY,
    )
    assert "ℹ️" not in result["response"]


# ── E. Adjacent upstream owners the removed CA word list was hiding ──────────

@pytest.mark.parametrize("tail", ["", "?", "??", "."])
def test_state_word_is_typed_regardless_of_trailing_punctuation(tail: str) -> None:
    parsed = _parse(f"¿Cuántos contratos de Galapa están actualmente suspendidos{tail}")
    assert parsed.params.get("estado_contrato") == ["Suspendido"]
    assert "suspendidos" not in _topic_tokens(parsed.params)


def test_copula_reporting_a_typed_state_is_not_a_topic() -> None:
    parsed = _parse("¿Cuántos contratos de Galapa figuran actualmente suspendidos?")
    assert parsed.params.get("estado_contrato") == ["Suspendido"]
    assert parsed.material_residual == ()
    assert "figuran" not in _topic_tokens(parsed.params)
    # Without a typed state after it, the same verb is left to content.
    assert "figuran" in _parse("contratos que figuran en el plan de Galapa").material_residual


def test_only_the_predicating_copula_is_consumed() -> None:
    parsed = _parse("contratos que figuran en el plan de Galapa y figuran actualmente suspendidos")
    assert parsed.params.get("estado_contrato") == ["Suspendido"]
    assert "figuran" in parsed.material_residual
