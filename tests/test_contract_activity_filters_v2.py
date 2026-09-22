from __future__ import annotations

from datetime import date

from hypothesis import given, settings, strategies as st

from app.core.analytics.contract_activity import handle_contract_activity
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.models import AdmissionStatus, Operation
from app.core.analytics.contract_activity.planning import build_plan
from app.core.capability_binding.contract_activity_binding import ContractActivityBinding
from app.core.natural_alpha.models import GroundedJob
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS, CorpusClient


TODAY = date(2026, 9, 20)


def _binding(raw: str, aspect: str, quote: str | None = None) -> ContractActivityBinding:
    quote = quote or raw
    start = raw.index(quote)
    job = GroundedJob("ANALYZE", aspect, start, start + len(quote))
    binding = ContractActivityBinding.from_job(job)
    assert binding is not None
    return binding


def _admit(raw: str, params: dict, aspect: str = "CONTRACT_COUNT"):
    return admit_contract_activity(
        raw,
        params,
        binding=_binding(raw, aspect),
        today=TODAY,
    )


def test_strict_and_inclusive_money_survive_request_plan_and_fingerprint() -> None:
    strict = _admit(
        "¿Cuántos contratos superiores a 200 millones firmó Galapa?",
        {**BUYER_PARAMS, "valor_min": 200_000_000},
    )
    inclusive = _admit(
        "¿Cuántos contratos de al menos 200 millones firmó Galapa?",
        {**BUYER_PARAMS, "valor_min": 200_000_000},
    )
    assert strict.status is AdmissionStatus.READY and strict.request is not None
    assert inclusive.status is AdmissionStatus.READY and inclusive.request is not None
    strict_plan = build_plan(strict.request)
    inclusive_plan = build_plan(inclusive.request)
    assert strict.request.money.operator == ">"
    assert inclusive.request.money.operator == ">="
    assert "valor_del_contrato > 200000000" in strict_plan.where
    assert "valor_del_contrato >= 200000000" in inclusive_plan.where
    assert strict_plan.fingerprint != inclusive_plan.fingerprint


def test_complete_signing_period_survives_and_missing_period_stays_absent() -> None:
    period = _admit(
        "¿Cuántos contratos firmó Galapa en agosto de 2026?",
        {**BUYER_PARAMS, "fecha_desde": "2026-08-01", "fecha_hasta": "2026-08-31"},
    )
    absent = _admit("¿Cuántos contratos firmó Galapa?", BUYER_PARAMS)
    assert period.request is not None and absent.request is not None
    assert (period.request.date_from, period.request.date_to) == (
        "2026-08-01", "2026-08-31",
    )
    assert absent.request.date_from is None and absent.request.date_to is None
    assert "fecha_de_firma >=" not in build_plan(absent.request).where
    assert "fecha_de_firma <=" not in build_plan(absent.request).where


def test_positive_modality_and_current_state_filters_survive() -> None:
    modality = _admit(
        "¿Cuántos contratos por contratación directa firmó Galapa?",
        {**BUYER_PARAMS, "modalidad_include_procedure_ids": [
            "contratacion_directa", "contratacion_directa_con_ofertas",
        ]},
    )
    state = _admit(
        "¿Cuántos contratos de Galapa figuran actualmente suspendidos?",
        {**BUYER_PARAMS, "estado_contrato": ["Suspendido"]},
    )
    assert modality.request is not None and modality.request.modality_values
    assert state.request is not None and state.request.reported_states == ("Suspendido",)
    assert "modalidad_de_contratacion" in build_plan(modality.request).where
    assert "estado_contrato = 'Suspendido'" in build_plan(state.request).where


def test_historical_state_forced_to_bound_operation_is_terminal_clarify() -> None:
    raw = "¿En qué estado estaban los contratos de Galapa al cierre de 2024?"
    admission = _admit(raw, BUYER_PARAMS, "CONTRACTS_BY_STATE")
    assert admission.status is AdmissionStatus.CLARIFY
    # ANALYTIC_CONTROL_UPSTREAM_V1: the host no longer lets Alpha's label stand
    # in for a breakdown it cannot ground ("en qué estado estaban" names no
    # criterion), so the refusal happens one step earlier than the historical
    # check.  Still terminal, still zero source calls.
    assert admission.reason == "HOST_OPERATION_NOT_ACCREDITED"
    assert admission.request is None


def test_unknown_material_inside_overbroad_quote_remains_blocking_residual() -> None:
    raw = "¿Cuántos contratos de aseo firmó Galapa?"
    admission = _admit(raw, {**BUYER_PARAMS, "objeto": ["aseo"]})
    assert admission.status is AdmissionStatus.CLARIFY
    # Material is now judged from upstream evidence on both paths, with the
    # reason the legacy path already used (no per-operation artifact list).
    assert admission.reason == "UNSUPPORTED_MATERIAL_FILTER"


def test_unsupported_exclusion_and_multiple_money_constraints_fail_closed() -> None:
    modality = _admit(
        "¿Cuántos contratos sin contratación directa firmó Galapa?",
        {**BUYER_PARAMS, "modalidad_exclude_procedure_ids": ["contratacion_directa"]},
    )
    money = _admit(
        "¿Cuántos contratos mayores a 100 millones y menores a 500 millones firmó Galapa?",
        BUYER_PARAMS,
    )
    assert modality.status is AdmissionStatus.CLARIFY
    assert money.status is AdmissionStatus.CLARIFY
    assert modality.request is None and money.request is None


def test_alpha_operation_that_the_host_grounds_differently_never_runs() -> None:
    """Alpha proposes; the host accredits.  Disagreement is a clarification.

    Before ANALYTIC_CONTROL_UPSTREAM_V1 this test pinned the opposite: Alpha's
    CONTRACT_COUNT on "¿cuántos contratos hay en cada modalidad?" executed a
    bare TOTAL, i.e. an incomplete reading of what the user asked.
    """
    raw = "¿Cuántos contratos hay en cada modalidad para Galapa?"
    total = _admit(raw, BUYER_PARAMS, "CONTRACT_COUNT")
    grouped = _admit(raw, BUYER_PARAMS, "CONTRACTS_BY_MODALITY")
    assert total.status is AdmissionStatus.CLARIFY
    assert total.reason == "ALPHA_HOST_OPERATION_MISMATCH"
    assert total.request is None
    assert grouped.request is not None
    assert grouped.request.operation is Operation.BY_MODALITY


def test_operation_changes_the_plan_fingerprint_but_not_the_where() -> None:
    total = _admit("¿Cuántos contratos firmó Galapa?", BUYER_PARAMS, "CONTRACT_COUNT")
    grouped = _admit(
        "Desglosa por modalidad los contratos que firmó Galapa.",
        BUYER_PARAMS,
        "CONTRACTS_BY_MODALITY",
    )
    assert total.request is not None and grouped.request is not None
    total_plan = build_plan(total.request)
    grouped_plan = build_plan(grouped.request)
    assert total_plan.groups_sql is None and grouped_plan.groups_sql is not None
    assert total_plan.where == grouped_plan.where
    assert total_plan.fingerprint != grouped_plan.fingerprint


def test_alpha_receipt_is_separate_from_request_plan_and_sql() -> None:
    raw = "¿Cuántos contratos firmó Galapa?"
    quote = "Cuántos contratos"
    binding = _binding(raw, "CONTRACT_COUNT", quote)
    start = raw.index(quote)
    decision = handle_contract_activity(
        raw, BUYER_PARAMS, CorpusClient(), binding=binding, today=TODAY,
    )
    assert decision.request is not None and decision.plan is not None
    assert decision.binding_receipt == {
        "capability": "analytics.contract_activity",
        "action": "ANALYZE",
        "aspect": "CONTRACT_COUNT",
        "operation": "TOTAL",
        "evidence_interval": [start, start + len(quote)],
        "raw_fingerprint": decision.binding_receipt["raw_fingerprint"],
    }
    assert len(decision.binding_receipt["raw_fingerprint"]) == 64
    assert "CONTRACT_COUNT" not in decision.plan.scalar_sql
    assert "evidence_interval" not in decision.plan.scalar_sql


@settings(max_examples=35, deadline=None)
@given(
    amount=st.integers(min_value=1, max_value=999_999),
    inclusive=st.booleans(),
    unknown_topic=st.booleans(),
)
def test_filter_combinatorics_preserve_comparator_or_absorb_unknown_material(
    amount: int, inclusive: bool, unknown_topic: bool,
) -> None:
    comparator = "al menos" if inclusive else "más de"
    raw = f"¿Cuántos contratos de {comparator} {amount} pesos firmó Galapa?"
    params = {**BUYER_PARAMS, "valor_min": amount}
    if unknown_topic:
        params["objeto"] = ["residuoz"]
    admission = _admit(raw, params)
    if unknown_topic:
        assert admission.status is AdmissionStatus.CLARIFY
        assert admission.request is None
    else:
        assert admission.request is not None and admission.request.money is not None
        assert admission.request.money.operator == (">=" if inclusive else ">")
