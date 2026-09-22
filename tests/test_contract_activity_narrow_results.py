from __future__ import annotations

from datetime import date
from pathlib import Path

from burr.core import State

from app.core.analytics.contract_activity import as_analytics_payload, handle_contract_activity
from app.core.analytics.contract_activity.models import DecisionKind
from app.core.analytics.contract_activity.presentation import render_result
from app.core.analytics.contract_activity.runtime import BLANK_GROUP, NULL_GROUP
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.execution import execute_query
from app.core.workflow.result_ownership import (
    ResultOwner,
    owns_execution_and_presentation,
    result_owner,
)
from tests.contract_activity_evidence import accredited_turn
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS, CorpusClient


def _handle(raw: str, client):
    parsed = accredited_turn(raw)
    return handle_contract_activity(
        raw, parsed.params, client, semantic_evidence=parsed.semantic_evidence,
    )


def test_complete_groups_publish_null_and_blank_buckets_visibly() -> None:
    modality = _handle(
        "Desglosa los contratos de la Alcaldía de Galapa por modalidad.", CorpusClient()
    )
    state = _handle(
        "Desglosa por estado reportado los contratos de la Alcaldía de Galapa.",
        CorpusClient(),
    )
    assert modality.kind is DecisionKind.ANSWER
    assert state.kind is DecisionKind.ANSWER
    assert NULL_GROUP in render_result(modality.request, modality.result)  # type: ignore[arg-type]
    assert BLANK_GROUP in render_result(state.request, state.result)  # type: ignore[arg-type]


def test_group_mismatch_never_publishes_scalar_as_an_answer() -> None:
    class MismatchClient(CorpusClient):
        def aggregate(self, dataset: str, sql: str) -> list[dict]:
            rows = super().aggregate(dataset, sql)
            if "GROUP BY modalidad_de_contratacion" in sql:
                rows[0]["distinct_contract_count"] = "1"
                rows[0]["row_count"] = "1"
                rows[0]["id_count"] = "1"
            return rows

    decision = _handle(
        "Desglosa los contratos de la Alcaldía de Galapa por modalidad.", MismatchClient()
    )
    payload = as_analytics_payload(decision)
    assert decision.kind is DecisionKind.CLARIFY
    assert payload is not None and payload["needs_clarification"] is True
    assert "como cero" in payload["response"]


def test_malformed_scalar_and_source_error_never_become_zero_answers() -> None:
    class MalformedClient(CorpusClient):
        def aggregate(self, dataset: str, sql: str):
            self.calls.append((dataset, sql))
            if "HAVING" in sql:
                return []
            return [{"distinct_contract_count": "0"}]

    class ErrorClient(CorpusClient):
        def aggregate(self, dataset: str, sql: str):
            self.calls.append((dataset, sql))
            raise TimeoutError("source timeout")

    for client in (MalformedClient(), ErrorClient()):
        decision = _handle("¿Cuántos contratos tiene la Alcaldía de Galapa?", client)
        payload = as_analytics_payload(decision)
        assert decision.kind is DecisionKind.CLARIFY
        assert payload is not None and payload["needs_clarification"] is True
        assert payload["total_count"] is None
        assert payload["route_reason"] == "analytics_contract_activity_clarify"
        assert "Encontré 0" not in payload["response"]


def test_legitimate_zero_requires_both_successful_calls() -> None:
    class ZeroClient(CorpusClient):
        def aggregate(self, dataset: str, sql: str) -> list[dict]:
            self.calls.append((dataset, sql))
            if "HAVING" in sql:
                return []
            return [{"row_count": "0", "id_count": "0", "distinct_contract_count": "0"}]

    client = ZeroClient()
    decision = _handle("¿Cuántos contratos tiene la Alcaldía de Galapa?", client)
    assert decision.kind is DecisionKind.ANSWER
    assert decision.result is not None and decision.result.total == 0
    assert len(client.calls) == 2


def test_product_router_timeout_is_terminal_and_preserves_unknown_total() -> None:
    raw = "¿Cuántos contratos tiene el Municipio de Galapa?"
    parsed = QueryRouter(
        EntityResolver(Path("app/data/aliases_db.json"))
    ).parse(raw, today=date(2026, 9, 20))
    params = parsed.params

    class TimeoutClient:
        def __init__(self) -> None:
            self.aggregate_calls: list[tuple[str, str]] = []

        def with_timeout(self, _seconds: int):
            return self

        def aggregate(self, dataset: str, sql: str):
            self.aggregate_calls.append((dataset, sql))
            raise TimeoutError("source timeout")

        def query(self, _dataset: str, _sql: str):
            raise AssertionError("tabular query expansion is forbidden")

        def count(self, _dataset: str, _sql: str):
            raise AssertionError("tabular count expansion is forbidden")

    client = TimeoutClient()
    state = State({
        "resolved_params": params,
        "dataset_id": "jbjy-vk9h",
        "soql_query": "UNEXECUTED TABULAR QUERY",
        "user_query": raw,
        "semantic_evidence": parsed.semantic_evidence.to_state(),
        "timings_ms": {},
    })
    result = execute_query(state, client, SoQLBuilder())

    assert result["needs_clarification"] is True
    assert result["analytical_intent"] == "contract_activity"
    assert result["route_reason"] == "analytics_contract_activity_clarify"
    assert result["total_count"] is None
    assert len(client.aggregate_calls) == 1
    assert result_owner(result) is ResultOwner.CONTRACT_ACTIVITY
    assert owns_execution_and_presentation(result, {}) is True
