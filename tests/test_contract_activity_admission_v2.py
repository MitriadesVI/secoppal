from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pytest

from app.config import Settings, get_settings
from app.core.conversation_store import ConversationStore
from app.core.natural_alpha.provider import ProviderResult
from app.core.orchestrator import SecopalWorkflow
from tests.test_natural_alpha_contract import IDENTITY


TODAY = date(2026, 9, 20)
FIXTURE = Path("tests/fixtures/contract_activity_admission_v2/frozen_payloads.jsonl")


class RecordingContractSource:
    timeout = 15

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def with_timeout(self, _timeout: int):
        return self

    derived_for_timeout = with_timeout

    def aggregate(self, dataset: str, soql: str, **_kwargs):
        self.calls.append(("aggregate", dataset, soql))
        if "HAVING" in soql:
            return []
        if "GROUP BY modalidad_de_contratacion" in soql:
            return [{
                "group_label": "Contratación directa",
                "row_count": "2",
                "id_count": "2",
                "distinct_contract_count": "2",
            }]
        if "GROUP BY estado_contrato" in soql:
            return [{
                "group_label": "Suspendido",
                "row_count": "2",
                "id_count": "2",
                "distinct_contract_count": "2",
            }]
        row = {
            "row_count": "2",
            "id_count": "2",
            "distinct_contract_count": "2",
        }
        if "group_non_null_count" in soql:
            row.update(group_non_null_count="2", distinct_group_count="1")
        return [row]

    def query(self, dataset: str, soql: str, **_kwargs):
        self.calls.append(("query", dataset, soql))
        return []

    def count(self, dataset: str, soql: str, **_kwargs):
        self.calls.append(("count", dataset, soql))
        return 0


def _records() -> dict[str, dict]:
    meta, *records = [json.loads(line) for line in FIXTURE.read_text().splitlines()]
    assert meta == {
        "record_type": "meta",
        "source": "experiments/contract_activity_option_c_verdict",
        "manifest_sha256": "4fe2427b924293309830cf06f802c13f1c83e4a3a5000b6ff473ce6b4e989520",
        "provider_calls_replayed": 0,
    }
    return {record["id"]: record for record in records}


@pytest.fixture(autouse=True)
def _isolated_config(monkeypatch) -> Iterator[None]:
    monkeypatch.setenv("SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _flow(tmp_path: Path, record: dict, *, enabled: bool = True) -> SecopalWorkflow:
    settings = Settings(
        _env_file=None,
        deepseek_api_key=None,
        secoppal_auth_enabled=True,
        secoppal_natural_alpha_mode="alpha",
        secoppal_natural_alpha_email="alpha@example.test",
        secoppal_contract_activity_binding_enabled=enabled,
        secoppal_a2_suppliers_binding_enabled=False,
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.db",
    )
    flow = SecopalWorkflow(settings)
    flow.conv_store = ConversationStore(tmp_path / "conversations")
    flow.secop_client = RecordingContractSource()
    flow.natural_alpha_provider = Mock(observe=Mock(return_value=ProviderResult(
        record["payload"], provider_calls=1,
    )))
    return flow


def _run(flow: SecopalWorkflow, raw: str) -> dict:
    return flow.run_query(
        raw,
        channel="streamlit",
        user_identity=IDENTITY,
        today=TODAY,
    )


@pytest.mark.parametrize(
    ("case_id", "operation", "call_count", "group_field"),
    [
        ("cc01", "TOTAL", 2, None),
        ("cm01", "BY_MODALITY", 3, "modalidad_de_contratacion"),
        ("cs01", "BY_REPORTED_STATE", 3, "estado_contrato"),
    ],
)
def test_first_milestone_reaches_run_query_with_typed_plan_and_visible_answer(
    tmp_path: Path,
    case_id: str,
    operation: str,
    call_count: int,
    group_field: str | None,
) -> None:
    record = _records()[case_id]
    flow = _flow(tmp_path / case_id, record)

    result = _run(flow, record["raw"])

    assert result["route_reason"] == "analytics_contract_activity"
    assert result["analytical_intent"] == "contract_activity"
    assert result["needs_clarification"] is False
    assert "Encontré 2 contratos distintos" in result["response"]
    assert result["total_count"] == 2
    alpha_receipt = result["natural_alpha_jobs"][0]
    assert alpha_receipt["action"] == "ANALYZE"
    assert alpha_receipt["evidence_interval"][0] < alpha_receipt["evidence_interval"][1]
    expected_aspect = {
        "TOTAL": "CONTRACT_COUNT",
        "BY_MODALITY": "CONTRACTS_BY_MODALITY",
        "BY_REPORTED_STATE": "CONTRACTS_BY_STATE",
    }[operation]
    assert alpha_receipt["aspect"] == expected_aspect
    calls = flow.secop_client.calls
    assert len(calls) == call_count
    assert {dataset for _, dataset, _ in calls} == {"jbjy-vk9h"}
    assert all("count(distinct id_contrato)" in soql or "HAVING" in soql for *_, soql in calls)
    if group_field:
        assert any(f"GROUP BY {group_field}" in soql for *_, soql in calls)


def test_neg01_is_terminal_clarification_with_zero_source_calls(tmp_path: Path) -> None:
    record = _records()["neg01"]
    flow = _flow(tmp_path, record)

    result = _run(flow, record["raw"])

    assert result["route_reason"] == "analytics_contract_activity_clarify"
    assert result["needs_clarification"] is True
    assert result["total_count"] is None
    assert flow.secop_client.calls == []


def test_real_frozen_positive_payloads_replay_without_provider_oracle_rewrites(
    tmp_path: Path,
) -> None:
    records = _records()
    executable = {"cc01", "cc04", "cc06", "cm01", "cm04", "cs01", "cs04"}
    observed: dict[str, tuple[str, int]] = {}
    for case_id in [f"{family}{index:02}" for family in ("cc", "cm", "cs") for index in range(1, 7)]:
        record = records[case_id]
        flow = _flow(tmp_path / case_id, record)
        result = _run(flow, record["raw"])
        calls = len(flow.secop_client.calls)
        observed[case_id] = (result["route_reason"], calls)
        assert result["natural_alpha_status"] == "MATCHED"
        assert result["natural_alpha_provider_calls"] == 1
        if case_id in executable:
            assert result["analytical_intent"] == "contract_activity"
            assert result["route_reason"] == "analytics_contract_activity"
            assert calls in {2, 3}
        else:
            # Existing buyer/source-name authority legitimately withholds bare
            # city, unresolved Malambo and unprojected department identities.
            assert result["needs_clarification"] is True
            assert calls == 0
    assert set(observed) == {
        f"{family}{index:02}"
        for family in ("cc", "cm", "cs")
        for index in range(1, 7)
    }


@pytest.mark.parametrize("case_id", ["neg03", "neg04"])
def test_frozen_multi_job_conflicts_keep_terminal_owner_and_zero_source(
    tmp_path: Path, case_id: str,
) -> None:
    record = _records()[case_id]
    flow = _flow(tmp_path, record)

    result = _run(flow, record["raw"])

    assert result["route_reason"] == "natural_alpha_multi_job_stepwise"
    assert result["needs_clarification"] is True
    assert flow.secop_client.calls == []


def test_frozen_neg06_search_job_does_not_enter_contract_activity(tmp_path: Path) -> None:
    record = _records()["neg06"]
    flow = _flow(tmp_path, record)

    result = _run(flow, record["raw"])

    assert result.get("analytical_intent") != "contract_activity"
    assert result["route_reason"] != "analytics_contract_activity"
    assert "contract_activity_binding_receipt" not in result


@pytest.mark.parametrize(
    ("raw", "aspect", "quote"),
    [
        (
            "Cuenta los contratos firmados por el Municipio de Galapa durante 2025.",
            "CONTRACT_COUNT", "Cuenta los contratos",
        ),
        (
            "Distribuye por modalidad los contratos de la Alcaldía de Soledad.",
            "CONTRACTS_BY_MODALITY", "Distribuye por modalidad los contratos",
        ),
        (
            "Distribuye los contratos de Galapa por estado reportado actual.",
            "CONTRACTS_BY_STATE", "Distribuye los contratos",
        ),
    ],
)
def test_committed_daily_paraphrases_continue_through_product_route(
    tmp_path: Path, raw: str, aspect: str, quote: str,
) -> None:
    record = {
        "raw": raw,
        "payload": json.dumps({"jobs": [{
            "action": "ANALYZE",
            "aspect": aspect,
            "evidence_quote": quote,
            "occurrence": 1,
        }]}, ensure_ascii=False),
    }
    flow = _flow(tmp_path, record)
    result = _run(flow, raw)
    assert result["route_reason"] == "analytics_contract_activity"
    assert result["needs_clarification"] is False
    assert flow.secop_client.calls
