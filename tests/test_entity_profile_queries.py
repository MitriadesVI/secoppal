from __future__ import annotations

from datetime import date
from pathlib import Path
import threading

import pytest

from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_profile.models import SectionStatus
from app.core.entity_profile.query_runner import (
    ENTITY_PROFILE_QUERY_BUDGET,
    jobs_for_focus,
)
from app.core.entity_profile.turn import classify_entity_profile_turn
from app.core.entity_resolver import EntityResolver
from app.core.deadline import begin_deadline, end_deadline, reset_clock, set_clock
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


class FakeClient:
    def __init__(self, fail: str | None = None):
        self.fail = fail
        self.calls: list[tuple[str, str, str]] = []
        self.timeouts: list[int] = []
        self._lock = threading.Lock()

    def derived_for_timeout(self, timeout: int):
        with self._lock:
            self.timeouts.append(timeout)
        return self

    def _record(self, operation: str, dataset: str, soql: str) -> None:
        with self._lock:
            self.calls.append((operation, dataset, soql))
        if self.fail and self.fail in soql:
            raise TimeoutError("source timed out")

    def aggregate(self, dataset: str, soql: str) -> list[dict]:
        self._record("aggregate", dataset, soql)
        if "GROUP BY modalidad_de_contratacion" in soql:
            return [
                {"modalidad_de_contratacion": "Contratación directa", "process_count": "7"},
                {"modalidad_de_contratacion": "Selección abreviada", "process_count": "3"},
            ]
        if "AS contract_count" in soql and "GROUP BY" not in soql:
            return [{
                "contract_count": "4",
                "total_contract_value": "1000000000",
                "average_contract_value": "250000000",
            }]
        if "GROUP BY documento_proveedor" in soql:
            if "ORDER BY contract_count" in soql:
                return [{
                    "documento_proveedor": "900123456",
                    "tipodocproveedor": "NIT",
                    "supplier_name": "PROVEEDOR FRECUENTE SAS",
                    "contract_count": "3",
                    "total_contract_value": "300000000",
                }]
            return [{
                "documento_proveedor": "800123456",
                "tipodocproveedor": "NIT",
                "supplier_name": "PROVEEDOR MAYOR SAS",
                "contract_count": "1",
                "total_contract_value": "700000000",
            }]
        if "date_trunc_y" in soql:
            if dataset == "p6dx-8zbt":
                return [
                    {"period": "2025-01-01T00:00:00.000", "row_count": "8"},
                    {"period": "2026-01-01T00:00:00.000", "row_count": "10"},
                ]
            return [
                {"period": "2025-01-01T00:00:00.000", "row_count": "2", "contract_value": "400"},
                {"period": "2026-01-01T00:00:00.000", "row_count": "4", "contract_value": "1000"},
            ]
        raise AssertionError(f"unexpected aggregate: {dataset} {soql}")

    def query(self, dataset: str, soql: str) -> list[dict]:
        self._record("query", dataset, soql)
        if dataset == "p6dx-8zbt":
            return [{
                "id_del_proceso": "CO1.REQ.1",
                "nombre_del_procedimiento": "MANTENIMIENTO DE VÍAS",
                "fecha_de_publicacion_del": "2026-08-01T00:00:00.000",
                "precio_base": "200000000",
                "urlproceso": "https://community.secop.gov.co/process/1",
            }]
        return [{
            "id_contrato": "CO1.PCCNTR.1",
            "objeto_del_contrato": "OBRA PÚBLICA",
            "fecha_de_firma": "2026-07-15T00:00:00.000",
            "valor_del_contrato": "700000000",
            "urlproceso": "https://community.secop.gov.co/contract/1",
        }]


@pytest.fixture(scope="module")
def baranoa_turn():
    router = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
    query = "analiza la alcaldia de baranoa en 2026"
    parsed = router.parse(query, today=date(2026, 9, 5)).params
    turn = classify_entity_profile_turn(query, parsed)
    assert turn.scope is not None and turn.identity is not None
    return turn


def _profile(turn, client: FakeClient, focus: str = "overview"):
    return build_entity_profile(
        scope=turn.scope,
        identity=turn.identity,
        params=turn.params,
        focus=focus,
        secop_client=client,
        soql_builder=SoQLBuilder(),
    )


def test_overview_uses_six_bounded_calls_and_separate_denominators(baranoa_turn):
    client = FakeClient()
    profile = _profile(baranoa_turn, client)

    assert len(client.calls) == ENTITY_PROFILE_QUERY_BUDGET == 6
    assert profile.source_calls.calls_total == 6
    assert profile.source_calls.calls_by_aspect == {
        "activity_modalities": 1,
        "activity_values": 1,
        "suppliers": 2,
        "recent": 2,
    }
    assert profile.activity.fact("process_count") == 10
    assert profile.activity.fact("contract_count") == 4
    assert profile.values.fact("total_contract_value") == 1_000_000_000
    assert profile.values.fact("average_contract_value") == 250_000_000
    assert dict(profile.values.coverage)["median"] == "UNAVAILABLE_V1"
    assert profile.partial is False


def test_modalities_and_supplier_rankings_are_distinct_and_stable(baranoa_turn):
    profile = _profile(baranoa_turn, FakeClient())
    modalities = profile.modalities.fact("top_modalities")
    assert modalities == (
        ("Contratación directa", 7, 0.7),
        ("Selección abreviada", 3, 0.3),
    )
    by_count = profile.suppliers.fact("top_by_contract_count")
    by_value = profile.suppliers.fact("top_by_contract_value")
    assert by_count[0].observed_name == "PROVEEDOR FRECUENTE SAS"
    assert by_value[0].observed_name == "PROVEEDOR MAYOR SAS"
    assert by_count[0].actor_identity == "NIT:900123456"
    assert by_count[0].document_type == "NIT"


def test_recent_rows_keep_process_and_contract_stable_identity(baranoa_turn):
    profile = _profile(baranoa_turn, FakeClient())
    process = profile.recent.fact("processes")[0]
    contract = profile.recent.fact("contracts")[0]
    assert process.stable_identity == "CO1.REQ.1"
    assert contract.stable_identity == "CO1.PCCNTR.1"
    assert process.identity_status == contract.identity_status == "stable"


def test_queries_are_server_side_bounded_deterministic_and_scoped(baranoa_turn):
    client = FakeClient()
    _profile(baranoa_turn, client)
    for _, _, soql in client.calls:
        assert "ALCALDIA MUNICIPAL DE BARANOA" in soql
        assert "2026-01-01" in soql and "2026-12-31" in soql
        assert "analiza" not in soql.lower()
    modality = next(soql for _, _, soql in client.calls if "GROUP BY modalidad" in soql)
    assert "ORDER BY process_count DESC, modalidad_de_contratacion ASC" in modality
    recent = [soql for operation, _, soql in client.calls if operation == "query"]
    assert all("LIMIT 3" in soql for soql in recent)
    assert any("id_del_proceso ASC" in soql for soql in recent)
    assert any("id_contrato ASC" in soql for soql in recent)


@pytest.mark.parametrize(
    ("focus", "expected"),
    [("overview", 6), ("suppliers", 2), ("modalities", 1), ("values", 1), ("recent", 2), ("temporal", 2)],
)
def test_every_focus_stays_within_budget(baranoa_turn, focus, expected):
    client = FakeClient()
    profile = _profile(baranoa_turn, client, focus)
    assert len(client.calls) == expected
    assert profile.source_calls.calls_total == expected
    assert len(jobs_for_focus(focus)) <= ENTITY_PROFILE_QUERY_BUDGET


def test_temporal_focus_keeps_dataset_denominators_per_period(baranoa_turn):
    profile = _profile(baranoa_turn, FakeClient(), "temporal")
    periods = profile.temporal.fact("periods")
    assert [(p.period, p.process_count, p.contract_count, p.contract_value) for p in periods] == [
        ("2025", 8, 2, 400.0),
        ("2026", 10, 4, 1000.0),
    ]


def test_one_supplier_timeout_makes_only_that_section_partial(baranoa_turn):
    profile = _profile(baranoa_turn, FakeClient(fail="ORDER BY total_contract_value"))
    assert profile.activity.status is SectionStatus.SUPPORTED
    assert profile.values.status is SectionStatus.SUPPORTED
    assert profile.recent.status is SectionStatus.SUPPORTED
    assert profile.suppliers.status is SectionStatus.PARTIAL
    assert profile.partial is True
    assert profile.suppliers.fact("top_by_contract_count")
    assert profile.suppliers.fact("top_by_contract_value") is None
    assert "fuente no respondió" in profile.suppliers.warnings[0]


def test_contract_timeout_preserves_processes_and_never_fabricates_zero(baranoa_turn):
    profile = _profile(baranoa_turn, FakeClient(fail="AS contract_count"))
    assert profile.activity.status is SectionStatus.PARTIAL
    assert profile.activity.fact("process_count") == 10
    assert profile.activity.fact("contract_count") is None
    assert profile.values.status is SectionStatus.UNAVAILABLE
    assert profile.values.fact("total_contract_value") is None
    assert profile.modalities.status is SectionStatus.SUPPORTED
    assert profile.source_calls.calls_total == 6


def test_source_calls_honor_remaining_turn_deadline(baranoa_turn):
    client = FakeClient()
    set_clock(lambda: 10.0)
    tokens = begin_deadline(0.0, budget_s=14.5, reserve_s=4.0)
    try:
        profile = _profile(baranoa_turn, client)
    finally:
        end_deadline(tokens)
        reset_clock()

    assert client.calls == []
    assert client.timeouts == []
    assert profile.source_calls.calls_total == 0
    assert {call.status for call in profile.source_calls.calls} == {
        "BUDGET_EXHAUSTED",
    }
    assert profile.activity.status is SectionStatus.UNAVAILABLE
    assert profile.partial is True


def test_source_timeout_is_capped_by_available_turn_budget(baranoa_turn):
    client = FakeClient()
    set_clock(lambda: 2.0)
    tokens = begin_deadline(0.0, budget_s=11.5, reserve_s=4.0)
    try:
        profile = _profile(baranoa_turn, client, "values")
    finally:
        end_deadline(tokens)
        reset_clock()

    assert profile.source_calls.calls_total == 1
    assert client.timeouts == [5]
