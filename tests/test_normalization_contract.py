"""HCA-R3: normalization preserves directional and open-bound intent."""

from __future__ import annotations

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter


CONTRADICTORY_MONEY_QUERY = (
    "contratos de aseo en cali por mas de 500 millones "
    "y menos de 100 millones"
)


def _workflow(tmp_path):
    workflow = SecopalWorkflow(Settings())
    calls: list[tuple[str, str]] = []

    def _query(dataset_id, soql_query, **_kwargs):
        calls.append((dataset_id, soql_query))
        return [{
            "objeto_del_contrato": "Aseo",
            "valor_del_contrato": "200000000",
            "fecha_de_firma": "2025-06-01T00:00:00.000",
        }]

    def _count(dataset_id, soql_query, **_kwargs):
        calls.append((dataset_id, soql_query))
        return 1

    workflow.secop_client.query = _query  # type: ignore[method-assign]
    workflow.secop_client.count = _count  # type: ignore[method-assign]
    workflow.narrator.narrate_with_grounding = lambda **_kwargs: None  # type: ignore[method-assign]
    workflow.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return workflow, calls


def test_money_compound_polarity_preserves_bound_direction():
    params = QueryRouter().parse(CONTRADICTORY_MONEY_QUERY).params

    assert params["valor_min"] == 500_000_000
    assert params["valor_max"] == 100_000_000


def test_money_contradiction_reaches_p8_and_prevents_query_execution(tmp_path):
    workflow, calls = _workflow(tmp_path)

    result = workflow.run_query(
        CONTRADICTORY_MONEY_QUERY,
        channel="telegram",
        chat_id="hca-r3-money-contradiction",
    )

    assert result["parsed_params"]["valor_min"] == 500_000_000
    assert result["parsed_params"]["valor_max"] == 100_000_000
    assert result["query_plan"]["reason"] == "invalid_value_range"
    assert result["query_plan"]["should_execute"] is False
    assert result.get("needs_clarification") is True
    assert not result.get("soql_query")
    assert calls == []


def test_valid_compound_money_range_remains_executable():
    raw_query = (
        "contratos de aseo en cali por mas de 100 millones "
        "y menos de 500 millones"
    )
    params = QueryRouter().parse(raw_query).params
    plan = compute_query_plan(params, raw_query)

    assert params["valor_min"] == 100_000_000
    assert params["valor_max"] == 500_000_000
    assert plan.should_execute is True
    assert "invalid_range" not in plan.risk_flags


def test_en_year_is_closed_but_desde_year_is_open():
    router = QueryRouter()

    closed = router.parse("contratos de aseo en cali en 2025").params
    opened = router.parse("contratos de aseo en cali desde 2025").params

    assert closed["fecha_desde"] == "2025-01-01"
    assert closed["fecha_hasta"] == "2025-12-31"
    assert opened["fecha_desde"] == "2025-01-01"
    assert "fecha_hasta" not in opened
    assert {
        key: closed.get(key) for key in ("fecha_desde", "fecha_hasta")
    } != {
        key: opened.get(key) for key in ("fecha_desde", "fecha_hasta")
    }


def test_desde_year_soql_has_no_artificial_upper_bound(tmp_path):
    workflow, _calls = _workflow(tmp_path)

    result = workflow.run_query(
        "contratos de aseo en cali desde 2025",
        channel="telegram",
        chat_id="hca-r3-time-open-year",
    )

    assert result.get("needs_clarification") is not True
    assert result["parsed_params"]["fecha_desde"] == "2025-01-01"
    assert "fecha_hasta" not in result["parsed_params"]
    assert ">= '2025-01-01'" in result["soql_query"]
    assert "<= '2025-12-31'" not in result["soql_query"]


def test_en_year_soql_keeps_closed_upper_bound(tmp_path):
    workflow, _calls = _workflow(tmp_path)

    result = workflow.run_query(
        "contratos de aseo en cali en 2025",
        channel="telegram",
        chat_id="hca-r3-time-closed-year",
    )

    assert result["parsed_params"]["fecha_desde"] == "2025-01-01"
    assert result["parsed_params"]["fecha_hasta"] == "2025-12-31"
    assert ">= '2025-01-01'" in result["soql_query"]
    assert "<= '2025-12-31'" in result["soql_query"]
