from __future__ import annotations

from app.core.analytics.contract_activity import as_analytics_payload, handle_contract_activity
from app.core.analytics.contract_activity.models import DecisionKind
from app.core.analytics.contract_activity.presentation import render_result
from app.core.analytics.contract_activity.runtime import BLANK_GROUP, NULL_GROUP
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS, CorpusClient


def test_complete_groups_publish_null_and_blank_buckets_visibly() -> None:
    modality = handle_contract_activity(
        "Desglosa los contratos de Galapa por modalidad.", BUYER_PARAMS, CorpusClient()
    )
    state = handle_contract_activity(
        "Desglosa por estado reportado los contratos de Galapa.",
        BUYER_PARAMS,
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

    decision = handle_contract_activity(
        "Desglosa los contratos de Galapa por modalidad.",
        BUYER_PARAMS,
        MismatchClient(),
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
        decision = handle_contract_activity(
            "¿Cuántos contratos tiene Galapa?", BUYER_PARAMS, client
        )
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
    decision = handle_contract_activity(
        "¿Cuántos contratos tiene Galapa?", BUYER_PARAMS, client
    )
    assert decision.kind is DecisionKind.ANSWER
    assert decision.result is not None and decision.result.total == 0
    assert len(client.calls) == 2
