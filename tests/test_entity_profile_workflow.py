from __future__ import annotations

from pathlib import Path
import threading

import pytest
import yaml

from app.config import Settings
from app.core.entity_profile.turn import PROFILE_ACTIVE_PARAM
from app.core.orchestrator import SecopalWorkflow


CORPUS = Path("tests/fixtures/entity_profile_corpus_v1.yaml")


class WorkflowClient:
    def __init__(self, fail_supplier_value: bool = False):
        self.fail_supplier_value = fail_supplier_value
        self.calls: list[tuple[str, str, str]] = []
        self._lock = threading.Lock()

    def derived_for_timeout(self, _timeout: int):
        return self

    def with_timeout(self, _timeout: int):
        return self

    def _record(self, operation: str, dataset: str, soql: str) -> None:
        with self._lock:
            self.calls.append((operation, dataset, soql))
        if self.fail_supplier_value and "ORDER BY total_contract_value" in soql:
            raise TimeoutError("supplier value timeout")

    def aggregate(self, dataset: str, soql: str) -> list[dict]:
        self._record("aggregate", dataset, soql)
        if "SUM(" in soql and "total_value" in soql:
            return [{"total_value": "1000000000", "total_count": "4"}]
        if "avg(" in soql and "min(" in soql:
            return [{"mean": "250000000", "min_val": "100", "max_val": "700000000"}]
        if "date_trunc_y" in soql and " AS yr" in soql:
            return [{"yr": "2026-01-01T00:00:00.000", "cnt": "10"}]
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
        if "date_trunc_y" in soql and " AS period" in soql:
            row = {"period": "2026-01-01T00:00:00.000", "row_count": "10"}
            if dataset == "jbjy-vk9h":
                row.update(row_count="4", contract_value="1000000000")
            return [row]
        return []

    def query(self, dataset: str, soql: str) -> list[dict]:
        self._record("query", dataset, soql)
        if dataset == "p6dx-8zbt":
            return [{
                "id_del_proceso": "CO1.REQ.1",
                "referencia_del_proceso": "LP-001-2026",
                "entidad": "ALCALDIA MUNICIPAL DE BARANOA",
                "nombre_del_procedimiento": "MANTENIMIENTO DE VÍAS",
                "fecha_de_publicacion_del": "2026-08-01T00:00:00.000",
                "precio_base": "200000000",
                "urlproceso": "https://community.secop.gov.co/process/1",
            }]
        return [{
            "id_contrato": "CO1.PCCNTR.1",
            "referencia_del_contrato": "C-001-2026",
            "nombre_entidad": "ALCALDIA MUNICIPAL DE BARANOA",
            "objeto_del_contrato": "OBRA PÚBLICA",
            "fecha_de_firma": "2026-07-15T00:00:00.000",
            "valor_del_contrato": "700000000",
            "proveedor_adjudicado": "PROVEEDOR MAYOR SAS",
            "documento_proveedor": "800123456",
            "tipodocproveedor": "NIT",
            "urlproceso": "https://community.secop.gov.co/contract/1",
        }]

    def count(self, dataset: str, soql: str) -> int:
        self._record("count", dataset, soql)
        return 1


def _workflow(client: WorkflowClient | None = None) -> tuple[SecopalWorkflow, WorkflowClient]:
    workflow = SecopalWorkflow(Settings())
    fake = client or WorkflowClient()
    workflow.secop_client = fake
    workflow.narrator.narrate_with_grounding = lambda **_kwargs: None
    return workflow, fake


def test_overview_reaches_product_response_without_advisor_rewrite():
    workflow, client = _workflow()
    result = workflow.run_query(
        "analiza la alcaldia de baranoa en 2026", channel="streamlit",
    )
    assert result["route_reason"] == "entity_profile_overview"
    assert result["analytical_intent"] == "entity_profile"
    assert result["needs_clarification"] is False
    assert result["response"].startswith("ALCALDIA MUNICIPAL DE BARANOA — 2026")
    assert "Publicó 10 procesos" in result["response"]
    assert "Se observan 4 contratos" in result["response"]
    assert "SECOP II Procesos" in result["response"]
    assert "SECOP II Contratos" in result["response"]
    assert result["response"].count("•") <= 8
    assert len(client.calls) == 6
    assert result["timings_ms"]["entity_profile_calls_total"] == 6
    assert result["timings_ms"]["entity_profile_total_latency_ms"] >= 0
    assert result["timings_ms"]["entity_profile_suppliers_calls"] == 2
    assert result["timings_ms"]["entity_profile_recent_calls"] == 2


def test_product_response_does_not_expose_internal_profile_jargon():
    workflow, _ = _workflow()
    text = workflow.run_query("analiza la alcaldia de baranoa")["response"]
    for forbidden in ("BuyerUnit", "EntityProfile", "aggregate_sum", "CR5", "HHI"):
        assert forbidden not in text


def test_territory_only_canary_clarifies_without_source_calls():
    workflow, client = _workflow()
    result = workflow.run_query("analiza Baranoa")
    assert result["needs_clarification"] is True
    assert "Alcaldía de Baranoa" in result["response"]
    assert "territorio institucional de Baranoa" in result["response"]
    assert client.calls == []


def test_topic_without_buyer_is_not_captured_by_entity_profile():
    workflow, _ = _workflow()
    result = workflow.run_query("analiza contratos de alimentacion")
    assert result["route_reason"] != "entity_profile_overview"
    assert result["analytical_intent"] != "entity_profile"


def test_specific_analytics_keeps_precedence_over_profile_context():
    workflow, _ = _workflow()
    chat_id = "entity-profile-analytics"
    workflow.run_query("analiza la alcaldia de baranoa", chat_id=chat_id)
    result = workflow.run_query("cuanto suma lo contratado", chat_id=chat_id)
    assert result["analytical_intent"] == "aggregate_sum"
    assert not result["route_reason"].startswith("entity_profile")
    assert PROFILE_ACTIVE_PARAM not in result["resolved_params"]


def test_profile_followup_reuses_buyer_and_limits_calls_to_focus():
    workflow, client = _workflow()
    chat_id = "entity-profile-suppliers"
    first = workflow.run_query(
        "analiza la alcaldia de baranoa en 2026", chat_id=chat_id,
    )
    client.calls.clear()
    second = workflow.run_query("y los principales proveedores", chat_id=chat_id)
    assert first["resolved_params"][PROFILE_ACTIVE_PARAM] is True
    assert second["route_reason"] == "entity_profile_suppliers"
    assert second["resolved_params"]["entidad_resolved"] == "ALCALDIA MUNICIPAL DE BARANOA"
    assert "Principales contratistas por número" in second["response"]
    assert len(client.calls) == 2


def test_time_followup_replaces_year_and_keeps_buyer():
    workflow, client = _workflow()
    chat_id = "entity-profile-year"
    workflow.run_query("analiza la alcaldia de baranoa en 2026", chat_id=chat_id)
    client.calls.clear()
    result = workflow.run_query("y el año pasado", chat_id=chat_id)
    assert result["route_reason"] == "entity_profile_overview"
    assert result["resolved_params"]["fecha_desde"] == "2025-01-01"
    assert result["resolved_params"]["fecha_hasta"] == "2025-12-31"
    assert len(client.calls) == 6
    assert all("2025-01-01" in soql for _, _, soql in client.calls)


def test_modality_followup_keeps_scope_and_adds_only_accredited_projection():
    workflow, client = _workflow()
    chat_id = "entity-profile-modality"
    workflow.run_query("analiza la alcaldia de baranoa en 2026", chat_id=chat_id)
    client.calls.clear()
    result = workflow.run_query("solo contratación directa", chat_id=chat_id)
    assert result["route_reason"] == "entity_profile_overview"
    assert result["resolved_params"]["modalidad_include_procedure_ids"]
    assert len(client.calls) == 6
    assert all("modalidad_de_contratacion" in soql for _, _, soql in client.calls)


def test_partial_supplier_failure_keeps_other_facts_and_says_unknown():
    workflow, _ = _workflow(WorkflowClient(fail_supplier_value=True))
    result = workflow.run_query("analiza la alcaldia de baranoa")
    text = result["response"]
    assert "Publicó 10 procesos" in text
    assert "Se observan 4 contratos" in text
    assert "No pude calcular los proveedores" in text
    assert "no tiene proveedores" not in text.lower()


def _route(result: dict) -> str:
    reason = result.get("route_reason", "")
    if reason.startswith("entity_profile_"):
        return reason.removeprefix("entity_profile_")
    if result.get("analytical_intent"):
        return "analytics"
    return "normal"


def test_entity_profile_multiturn_corpus_has_fifteen_conversations():
    conversations = yaml.safe_load(CORPUS.read_text(encoding="utf-8"))["conversations"]
    assert len(conversations) == 15
    failures: list[str] = []
    for conversation in conversations:
        workflow, _ = _workflow()
        for index, turn in enumerate(conversation["turns"]):
            result = workflow.run_query(
                turn["text"], chat_id=conversation["id"],
            )
            actual = _route(result)
            if actual != turn["route"]:
                failures.append(
                    f"{conversation['id']}/{index}: {actual} != {turn['route']}"
                )
    assert failures == []
