"""MPV2 parser → planning → SECOP spies → faithful presentation."""
from unittest.mock import Mock

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.response_policy import _build_money_filter_interpretation
from app.core.soql_builder import SoQLBuilder


DEMO = (
    "contratos firmados por distrito de barranquilla "
    "en agosto de 2026 por mas 200 millones"
)


@pytest.fixture
def workflow():
    flow = SecopalWorkflow(Settings())
    flow.secop_client.query = Mock(return_value=[{
        "id_contrato": "MPV2-001", "referencia_del_contrato": "MPV2-001",
        "nombre_entidad": "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA",
        "objeto_del_contrato": "ALIMENTACION", "valor_del_contrato": "250000000",
        "fecha_de_firma": "2026-08-15T00:00:00.000", "estado_contrato": "En ejecución",
        "urlproceso": "https://example.test/mpv2",
    }])
    flow.secop_client.count = Mock(return_value=1)
    flow.llm_handler.parse = Mock(side_effect=AssertionError("MPV2 must use no LLM"))
    flow.narrator.narrate_with_grounding = Mock(return_value=None)
    return flow


def test_canonical_demo_preserves_scope_dates_and_lower_soql(workflow):
    parsed = QueryRouter().parse(DEMO)
    assert parsed.route_reason == "heuristic_only"
    assert parsed.needs_llm is False
    result = workflow.run_query(DEMO, channel="streamlit")
    params = result["resolved_params"]
    assert params["dataset"] == "contratos"
    assert params["entidad_resolved"] == "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"
    assert (params["fecha_desde"], params["fecha_hasta"]) == ("2026-08-01", "2026-08-31")
    assert params["valor_min"] == 200_000_000
    assert "valor_max" not in params
    assert "valor_del_contrato >= 200000000" in result["soql_query"]
    assert "valor_del_contrato <= 200000000" not in result["soql_query"]
    assert "2026-08-01" in result["soql_query"] and "2026-08-31" in result["soql_query"]
    workflow.secop_client.query.assert_called_once()
    workflow.llm_handler.parse.assert_not_called()


@pytest.mark.parametrize("dataset,field", [("contratos", "valor_del_contrato"), ("procesos", "precio_base")])
@pytest.mark.parametrize("phrase,operator,symbol", [
    ("por mas 200 millones", ">=", "≥"),
    ("no supera 200 millones", "<=", "≤"),
    ("no menor 200 millones", ">=", "≥"),
    ("por debajo 200 millones", "<=", "≤"),
])
def test_both_datasets_project_and_present_authoritative_direction(dataset, field, phrase, operator, symbol):
    params = QueryRouter().parse(f"{dataset} de software {phrase}").params
    soql = SoQLBuilder().build(SoQLBuilder.dataset_id_for(dataset), params)
    assert f"{field} {operator} 200000000" in soql
    assert f"{symbol} $200,000,000" in _build_money_filter_interpretation(params)


@pytest.mark.parametrize("query", [
    "contratos de alimentación de 200 millones",
    "contratos de alimentación 200 millones",
    "procesos con 500 millones",
    "quiero más contratos de alimentación de 200 millones",
    "contratos de alimentación más de 100 millones y cerca de 200 millones",
    "contratos de software no rondan 200 millones",
    "contratos de software no exactamente más de 200 millones",
    "contratos de software no mas contratos por 200 millones",
])
def test_unknown_blocks_all_secop_and_llm_calls(workflow, query):
    result = workflow.run_query(query, channel="streamlit")
    plan = result["query_plan"]
    assert plan["should_execute"] is False
    assert plan["reason"] == "money_interpretation_uncertain"
    assert "money_interpretation_uncertain" in plan["risk_flags"]
    assert result["needs_clarification"] is True
    assert plan["params"].get("money_clarification")
    assert "valor_min" not in plan["params"] and "valor_max" not in plan["params"]
    workflow.secop_client.query.assert_not_called()
    workflow.secop_client.count.assert_not_called()
    workflow.llm_handler.parse.assert_not_called()


@pytest.mark.parametrize("old_max,expected_max", [(None, None), (500, 500), (100, None)])
def test_followup_keeps_topic_and_scope_with_existing_intersection_policy(workflow, old_max, expected_max):
    first = "contratos de alimentación en Barranquilla"
    if old_max:
        first += f" por menos de {old_max} millones"
    before = workflow.run_query(first, channel="streamlit", chat_id="mpv2-followup")
    after = workflow.run_query("ahora por mas 200 millones", channel="streamlit", chat_id="mpv2-followup")
    params = after["resolved_params"]
    assert after["followup"] is True
    assert params["valor_min"] == 200_000_000
    assert params.get("valor_max") == (expected_max * 1_000_000 if expected_max else None)
    assert params["objeto"] == before["resolved_params"]["objeto"]
    assert params["ciudad"] == before["resolved_params"]["ciudad"]
    workflow.llm_handler.parse.assert_not_called()


@pytest.mark.parametrize("followup", ["ahora de 200 millones", "ahora por mas de un millón y medio"])
def test_unknown_followup_cannot_execute_inherited_money(workflow, followup):
    workflow.run_query("contratos de alimentación por menos de 500 millones", channel="streamlit", chat_id="mpv2-unknown")
    workflow.secop_client.query.reset_mock()
    workflow.secop_client.count.reset_mock()
    result = workflow.run_query(followup, channel="streamlit", chat_id="mpv2-unknown")
    assert result["query_plan"]["reason"] == "money_interpretation_uncertain"
    assert result["query_plan"]["should_execute"] is False
    assert "valor_max" not in result["query_plan"]["params"]
    workflow.secop_client.query.assert_not_called()
    workflow.secop_client.count.assert_not_called()


def test_generic_uncertainty_with_valid_smmlv_metadata_is_not_a_year_error():
    params = {"objeto": ["software"], "valor_min": 1, "money_clarification": "¿Qué relación?",
              "money_metadata": {"unit": "SMMLV", "reference_year": 2026}}
    plan = compute_query_plan(params, "software", enabled_rules={"P16"})
    assert plan.reason == "money_interpretation_uncertain"
    assert "valor_min" not in plan.params
    assert params["valor_min"] == 1  # caller-owned input is unchanged


@pytest.mark.parametrize("topic,expected", [("alimentacion", "alimentacion"), ("mantenimiento", "mantenimiento"),
                                           ("software", "software"), ("adulto mayor", "adulto_mayor")])
@pytest.mark.parametrize("phrase", ["por mas 200 millones", "supera los 500 millones", "no exceda 300 millones"])
def test_consumed_money_does_not_pollute_or_erase_topic(topic, expected, phrase):
    params = QueryRouter().parse(f"contratos de {topic} {phrase}").params
    assert params["objeto"] == [expected]
