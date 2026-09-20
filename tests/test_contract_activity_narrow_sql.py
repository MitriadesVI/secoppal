from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from app.core.analytics import maybe_handle_analytical_query
from app.core.analytics.contract_activity import handle_contract_activity
from app.core.analytics.contract_activity.admission import admit_contract_activity
from app.core.analytics.contract_activity.models import AdmissionStatus, DecisionKind
from app.core.analytics.contract_activity.planning import build_plan
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.result_ownership import ResultOwner, result_owner
from tests.test_contract_activity_narrow_corpus import BUYER_PARAMS, CorpusClient


def test_plan_owns_exact_shared_where_and_has_no_limit() -> None:
    raw = "¿Cuántos contratos superiores a 200 millones firmó Galapa en agosto de 2026?"
    admission = admit_contract_activity(raw, BUYER_PARAMS)
    assert admission.status is AdmissionStatus.READY and admission.request is not None
    plan = build_plan(admission.request)

    assert plan.where in plan.scalar_sql
    assert plan.groups_sql is None
    assert "LIMIT" not in plan.identity_sql + plan.scalar_sql
    assert "fecha_de_firma >= '2026-08-01'" not in plan.identity_where
    assert "fecha_de_firma <= '2026-08-31'" not in plan.identity_where
    assert "valor_del_contrato" not in plan.identity_where
    assert "nombre_entidad = 'MUNICIPIO DE GALAPA.'" in plan.identity_where
    assert "fecha_de_firma >= '2026-08-01'" in plan.where
    assert "fecha_de_firma <= '2026-08-31'" in plan.where
    assert "valor_del_contrato > 200000000" in plan.where


def test_group_jobs_share_the_exact_where_and_fingerprint() -> None:
    admission = admit_contract_activity(
        "Distribuye por modalidad los contratos firmados por Galapa en 2025.",
        BUYER_PARAMS,
    )
    assert admission.request is not None
    plan = build_plan(admission.request)

    assert plan.groups_sql is not None
    assert plan.where in plan.scalar_sql and plan.where in plan.groups_sql
    assert plan.fingerprint == build_plan(admission.request).fingerprint
    assert "LIMIT" not in plan.groups_sql


def test_known_modality_and_current_reported_state_reach_the_where() -> None:
    modality = admit_contract_activity(
        "¿Cuántos contratos por contratación directa firmó Galapa en 2026?",
        BUYER_PARAMS,
    )
    state = admit_contract_activity(
        "¿Cuántos contratos de Galapa figuran actualmente suspendidos?",
        BUYER_PARAMS,
    )
    assert modality.request is not None and state.request is not None
    modality_where = build_plan(modality.request).where
    state_where = build_plan(state.request).where
    assert "modalidad_de_contratacion" in modality_where
    assert "Contratación directa" in modality_where
    assert "estado_contrato = 'Suspendido'" in state_where


def test_request_plan_and_result_are_immutable() -> None:
    decision = handle_contract_activity(
        "¿Cuántos contratos tiene Galapa?", BUYER_PARAMS, CorpusClient()
    )
    assert decision.kind is DecisionKind.ANSWER
    with pytest.raises(FrozenInstanceError):
        decision.request.buyer_display = "otro"  # type: ignore[misc,union-attr]
    with pytest.raises(FrozenInstanceError):
        decision.plan.where = "1=1"  # type: ignore[misc,union-attr]
    with pytest.raises(FrozenInstanceError):
        decision.result.total = 0  # type: ignore[misc,union-attr]


def test_identity_witness_failure_stops_after_one_call() -> None:
    class DuplicateClient(CorpusClient):
        def aggregate(self, dataset: str, sql: str) -> list[dict]:
            self.calls.append((dataset, sql))
            return [{"id_contrato": "DUP", "row_count": "2"}]

    client = DuplicateClient()
    decision = handle_contract_activity(
        "¿Cuántos contratos tiene Galapa?", BUYER_PARAMS, client
    )
    assert decision.kind is DecisionKind.CLARIFY
    assert decision.reason == "SOURCE_GRAIN_UNACCREDITED"
    assert len(client.calls) == 1


def test_registry_hook_is_first_and_contract_activity_is_terminal_owner() -> None:
    client = CorpusClient()
    payload = maybe_handle_analytical_query(
        "¿Cuántos contratos tiene Galapa?",
        BUYER_PARAMS,
        "p6dx-8zbt",
        client,
        SoQLBuilder(),
    )
    assert payload is not None
    assert payload["analytical_intent"] == "contract_activity"
    assert payload["dataset_id"] == "jbjy-vk9h"
    assert result_owner(payload) is ResultOwner.CONTRACT_ACTIVITY
    assert len(client.calls) == 2


def test_unowned_neighbors_still_handoff_without_source_calls() -> None:
    for raw in (
        "Lista los contratos de Galapa firmados en 2026.",
        "¿Cuánto suman los contratos de Galapa firmados en 2026?",
        "¿Qué proveedores contrataron con Galapa en 2026?",
    ):
        client = CorpusClient()
        decision = handle_contract_activity(raw, BUYER_PARAMS, client)
        assert decision.kind is DecisionKind.HANDOFF
        assert client.calls == []


def test_unowned_material_topic_clarifies_without_source_calls() -> None:
    client = CorpusClient()
    params = {**BUYER_PARAMS, "objeto": ["aseo"]}
    decision = handle_contract_activity(
        "¿Cuántos contratos de aseo firmó Galapa en 2026?", params, client
    )
    assert decision.kind is DecisionKind.CLARIFY
    assert decision.reason == "UNSUPPORTED_MATERIAL_FILTER"
    assert client.calls == []
