"""O1B — current opportunity retrieval policy (tests RED first).

These tests exercise the retrieval predicate before LIMIT and the visible
certainty guard.  The injected reference instant makes every expectation
deterministic; production must capture it once upstream and reuse it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from unittest.mock import patch

from burr.core import State

from app.core.process_operational_state import summarize_process_operational_state
from app.core.opportunity_retrieval_policy import (
    DEFAULT_STATES,
    REFERENCE_INSTANT_PARAM,
    evaluate_current_opportunity_policy,
)
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.execution import degrade_query
from app.core.workflow.actions.planning import plan_query


PROCESOS_ID = SoQLBuilder.PROCESOS_DATASET
REFERENCE_ISO = "2026-08-17T12:00:00Z"
REFERENCE = datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc)
BASELINE_DEFAULT_STATES = [
    "Publicado",
    "Borrador",
    "En aprobación",
    "Aprobado",
    "Abierto",
]


def _current_params(**overrides: object) -> dict:
    params = {
        "dataset": "procesos",
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "estado_del_procedimiento": list(BASELINE_DEFAULT_STATES),
        "objeto": ["alimentacion"],
        "_opportunity_reference_instant": REFERENCE_ISO,
    }
    params.update(overrides)
    return params


def _soql(**overrides: object) -> str:
    return SoQLBuilder().build(PROCESOS_ID, _current_params(**overrides))


def _offer_phase_row(deadline: str | None) -> dict:
    row = {
        "id_del_proceso": "CO1.NTC.O1B",
        "estado_de_apertura_del_proceso": "Abierto",
        "estado_del_procedimiento": "Publicado",
        "fase": "Presentación de oferta",
        "fecha_de_publicacion_del": "2026-08-01T00:00:00.000",
    }
    if deadline is not None:
        row["fecha_de_recepcion_de"] = deadline
    return row


def test_red_1_current_default_excludes_borrador_before_limit() -> None:
    assert "Borrador" not in _soql()


def test_red_2_current_default_excludes_approval_states_before_limit() -> None:
    soql = _soql()
    assert "En aprobación" not in soql
    assert "Aprobado" not in soql


def test_red_3_unknown_deadline_uses_recent_publication_fallback() -> None:
    soql = _soql()
    assert "fecha_de_recepcion_de IS NULL" in soql
    assert "fecha_de_publicacion_del >= '2026-05-19T12:00:00.000'" in soql


def test_red_4_future_deadline_is_not_subject_to_hard_90_day_gate() -> None:
    soql = _soql()
    expected = (
        "case(fecha_de_recepcion_de IS NULL, "
        "fecha_de_publicacion_del >= '2026-05-19T12:00:00.000', "
        "true, fecha_de_recepcion_de > '2026-08-17T12:00:00.000')"
    )
    assert expected in soql
    assert soql.count(
        "fecha_de_publicacion_del >= '2026-05-19T12:00:00.000'"
    ) == 1


def test_red_5_known_expired_deadline_is_excluded() -> None:
    soql = _soql()
    assert "fecha_de_recepcion_de > '2026-08-17T12:00:00.000'" in soql
    assert "fecha_de_recepcion_de <=" not in soql


def test_red_6_explicit_user_time_preserves_baseline_query_semantics() -> None:
    soql = _soql(fecha_desde="2025-01-01", fecha_hasta="2025-12-31")
    assert "fecha_de_publicacion_del >= '2025-01-01'" in soql
    assert "fecha_de_publicacion_del <= '2025-12-31'" in soql
    assert "fecha_de_recepcion_de >" not in soql
    assert "Borrador" in soql


def test_red_7_explicit_user_state_is_not_replaced_by_opportunity_default() -> None:
    soql = _soql(
        estado="Evaluación",
        estado_field="estado_del_procedimiento",
        estado_family="oferta_abierta",  # P11 currently rewrites the family.
        estado_del_procedimiento=["Evaluación", "Seleccionado"],
    )
    assert "estado_del_procedimiento IN ('Evaluación', 'Seleccionado')" in soql
    assert "fecha_de_recepcion_de >" not in soql
    assert "'Publicado', 'Abierto'" not in soql


def test_red_8_offer_phase_without_deadline_does_not_claim_reception() -> None:
    summary = summarize_process_operational_state(
        _offer_phase_row(None), reference_instant=REFERENCE,
    )
    assert summary.state == "offer_phase_observed"
    assert summary.label == "Publicado · Vigencia por verificar"
    assert summary.icon == "🔵"
    assert "Recepción de ofertas" not in summary.label


def test_red_9_zero_results_do_not_trigger_automatic_broader_query() -> None:
    params = _current_params(departamento_resolved="Atlántico")
    state = State({
        "results": [],
        "query_error": "",
        "resolved_params": params,
        "dataset_id": PROCESOS_ID,
        "followup": False,
        "analytical_response": "",
        "timings_ms": {},
    })
    client = MagicMock()
    client.query.return_value = []
    client.count.return_value = 0

    out = degrade_query(state, client, SoQLBuilder())

    assert client.query.call_count == 0
    assert client.count.call_count == 0
    assert out["degraded"] is False


def test_red_10_ordinary_process_search_is_byte_for_byte_unchanged() -> None:
    builder = SoQLBuilder()
    params = {"dataset": "procesos", "objeto": ["alimentacion"]}
    baseline = builder.build(PROCESOS_ID, params)
    with_o1b_metadata = builder.build(
        PROCESOS_ID,
        {**params, "_opportunity_reference_instant": REFERENCE_ISO},
    )
    assert with_o1b_metadata == baseline


def test_policy_decision_is_pure_and_exposes_the_90_day_boundary() -> None:
    params = _current_params()
    snapshot = {**params, "estado_del_procedimiento": list(params["estado_del_procedimiento"])}
    decision = evaluate_current_opportunity_policy(
        params, PROCESOS_ID, reference_instant=REFERENCE,
    )

    assert decision.applies is True
    assert decision.allowed_states == DEFAULT_STATES
    assert decision.reference_instant == REFERENCE
    assert decision.unknown_deadline_publication_since == datetime(
        2026, 5, 19, 12, 0, tzinfo=timezone.utc,
    )
    assert params == snapshot


def test_policy_does_not_apply_to_contracts_or_ordinary_process_search() -> None:
    contract = evaluate_current_opportunity_policy(
        _current_params(), SoQLBuilder.CONTRATOS_DATASET,
        reference_instant=REFERENCE,
    )
    ordinary = evaluate_current_opportunity_policy(
        {"dataset": "procesos", "objeto": ["alimentacion"]},
        PROCESOS_ID,
        reference_instant=REFERENCE,
    )
    assert contract.applies is False
    assert contract.reason == "not_processes_dataset"
    assert ordinary.applies is False
    assert ordinary.reason == "not_opportunity_search"


def test_plan_query_captures_one_reference_instant_for_current_default() -> None:
    state = State({
        "parsed_params": {
            "dataset": "procesos",
            "intent_type": "opportunity_search",
            "estado_family": "oferta_abierta",
            "estado_del_procedimiento": ["Publicado", "Borrador", "Abierto"],
            "objeto": ["alimentacion"],
        },
        "raw_query": "oportunidades de alimentación",
        "prev_params": None,
        "turn_index": 0,
        "modality_interpretation": None,
    })
    with patch("app.core.workflow.actions.planning.datetime") as clock:
        clock.now.return_value = REFERENCE
        planned = plan_query(state)

    clock.now.assert_called_once_with(timezone.utc)
    assert planned["parsed_params"][REFERENCE_INSTANT_PARAM] == REFERENCE.isoformat()
    assert (
        planned["query_plan"]["params"][REFERENCE_INSTANT_PARAM]
        == REFERENCE.isoformat()
    )


def test_plan_query_does_not_read_clock_for_explicit_time_or_state() -> None:
    variants = (
        _current_params(fecha_desde="2025-01-01", fecha_hasta="2025-12-31"),
        _current_params(
            estado="Evaluación",
            estado_field="estado_del_procedimiento",
            estado_del_procedimiento=["Evaluación", "Seleccionado"],
        ),
    )
    for params in variants:
        params.pop(REFERENCE_INSTANT_PARAM, None)
        state = State({
            "parsed_params": params,
            "raw_query": "oportunidades de alimentación",
            "prev_params": None,
            "turn_index": 0,
            "modality_interpretation": None,
        })
        with patch("app.core.workflow.actions.planning.datetime") as clock:
            planned = plan_query(state)
        clock.now.assert_not_called()
        assert REFERENCE_INSTANT_PARAM not in planned["parsed_params"]


def test_future_deadline_presentation_is_green_and_shows_limit_date() -> None:
    summary = summarize_process_operational_state(
        _offer_phase_row("2026-09-30T10:00:00.000"),
        reference_instant=REFERENCE,
    )
    assert summary.label == "Recepción de ofertas · límite 2026-09-30"
    assert summary.icon == "🟢"


def test_past_deadline_presentation_is_neutral_not_reception() -> None:
    summary = summarize_process_operational_state(
        _offer_phase_row("2026-08-01T10:00:00.000"),
        reference_instant=REFERENCE,
    )
    assert summary.label == "Plazo de recepción reportado vencido"
    assert summary.icon == "🟠"
    assert "Recepción de ofertas" not in summary.label
