"""Proyección por fuente: ningún predicado sobrevive al esquema equivocado."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core.entity_profile.aspects.common import build_where
from app.core.entity_profile.immutability import mutable_copy
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.semantic_request import build_semantic_request
from app.core.entity_profile.source_plan import (
    DATE_FIELD,
    VALUE_FIELD,
    project_sources,
    value_magnitudes_differ,
)
from app.core.entity_profile.temporal_event import TemporalEvent
from app.core.entity_profile.turn import profile_query_params
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


TODAY = date(2026, 9, 5)

PROCESS_ONLY_FIELDS = ("estado_del_procedimiento", "estado_de_apertura_del_proceso")
CONTRACT_ONLY_FIELDS = ("estado_contrato",)


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _plans(router: QueryRouter, text: str, focus: str = "overview"):
    params = profile_query_params(router.parse(text, today=TODAY).params)
    scope = resolve_profile_scope(params).scope
    assert scope is not None, text
    request = build_semantic_request(
        user_query=text, params=params, scope=scope, focus=focus,
    )
    return request, project_sources(request, params)


def _where(plan) -> str:
    _, where = build_where(
        SoQLBuilder(), plan.dataset_id, mutable_copy(plan.params),
    )
    return where


# ── Compatibilidad de predicados ────────────────────────────────────────────

@pytest.mark.parametrize("text", [
    "analiza procesos de vias publicados por alcaldia de baranoa en agosto",
    "analiza contratos firmados en agosto y actualmente terminados de alcaldia de baranoa",
    "analiza procesos y contratos actualmente publicados de alcaldia de baranoa",
    "analiza alcaldia de baranoa en 2026",
])
def test_no_predicate_crosses_into_the_other_schema(router, text):
    _, (process_plan, contract_plan) = _plans(router, text)
    if process_plan.should_execute:
        where = _where(process_plan)
        assert not any(field in where for field in CONTRACT_ONLY_FIELDS)
    if contract_plan.should_execute:
        where = _where(contract_plan)
        assert not any(field in where for field in PROCESS_ONLY_FIELDS)


def test_process_state_is_purged_from_the_contract_plan(router):
    _, (_, contract_plan) = _plans(
        router, "analiza procesos y contratos actualmente publicados de alcaldia de baranoa",
    )
    for key in PROCESS_ONLY_FIELDS:
        assert key not in contract_plan.params


def test_contract_state_is_purged_from_the_process_plan(router):
    _, (process_plan, _) = _plans(
        router,
        "analiza procesos y contratos actualmente terminados de alcaldia de baranoa",
    )
    for key in CONTRACT_ONLY_FIELDS:
        assert key not in process_plan.params


def test_orchestration_signals_never_reach_the_builder(router):
    _, plans = _plans(router, "analiza procesos y contratos de alcaldia de baranoa")
    for plan in plans:
        assert "dataset_scope" not in plan.params
        assert "dataset_explicit" not in plan.params
        assert plan.params["dataset"] == plan.dataset_name


# ── Anclaje temporal ────────────────────────────────────────────────────────

def test_each_source_anchors_the_period_to_its_own_date(router):
    _, (process_plan, contract_plan) = _plans(
        router, "analiza alcaldia de baranoa en 2026",
    )
    assert process_plan.date_field == DATE_FIELD[PROCESS_DATASET]
    assert contract_plan.date_field == DATE_FIELD[CONTRACT_DATASET]
    assert "fecha_de_publicacion_del >=" in _where(process_plan)
    assert "fecha_de_firma >=" in _where(contract_plan)


def test_a_named_event_does_not_execute_on_the_other_schema(router):
    request, (process_plan, contract_plan) = _plans(
        router, "analiza procesos y contratos firmados en agosto de alcaldia de baranoa",
    )
    assert request.event is TemporalEvent.CONTRACT_SIGNATURE
    assert contract_plan.should_execute is True
    assert process_plan.should_execute is False
    assert process_plan.unsupported_dimensions == ("CONTRACT_SIGNATURE",)


def test_an_unsupported_event_blocks_every_source(router):
    _, plans = _plans(
        router, "analiza contratos terminados en agosto de alcaldia de baranoa",
    )
    assert all(plan.should_execute is False for plan in plans)
    assert all(
        any(dim.startswith("UNSUPPORTED_EVENT:") for dim in plan.unsupported_dimensions)
        for plan in plans
    )


def test_publication_event_drops_the_current_state_it_was_read_from(router):
    _, (process_plan, _) = _plans(
        router, "analiza procesos de vias publicados por alcaldia de baranoa en agosto",
    )
    where = _where(process_plan)
    assert "estado_del_procedimiento" not in where
    assert "estado_de_apertura_del_proceso" not in where
    assert "fecha_de_publicacion_del >=" in where
    assert "%vias%" in where


def test_an_independent_current_state_survives_its_event(router):
    _, (_, contract_plan) = _plans(
        router,
        "analiza contratos firmados en agosto y actualmente terminados "
        "de alcaldia de baranoa",
    )
    where = _where(contract_plan)
    assert "estado_contrato" in where
    assert "fecha_de_firma >=" in where


def test_shared_state_metadata_does_not_outlive_the_filter_it_described(router):
    _, (process_plan, _) = _plans(
        router, "analiza procesos de vias publicados por alcaldia de baranoa en agosto",
    )
    for key in ("estado", "estado_family", "estado_field", "intent_type"):
        assert key not in process_plan.params


# ── Semántica de valor ──────────────────────────────────────────────────────

def test_each_source_declares_its_own_value_field(router):
    _, (process_plan, contract_plan) = _plans(router, "analiza alcaldia de baranoa")
    assert process_plan.value_field == VALUE_FIELD[PROCESS_DATASET] == "precio_base"
    assert contract_plan.value_field == VALUE_FIELD[CONTRACT_DATASET]


def test_a_value_filter_over_both_sources_is_flagged_as_two_magnitudes(router):
    request, _ = _plans(
        router, "analiza procesos y contratos de mas de 100 millones de alcaldia de baranoa",
    )
    assert request.value_min == 100_000_000
    assert value_magnitudes_differ(request) is True


def test_a_value_filter_over_one_source_is_a_single_magnitude(router):
    request, _ = _plans(
        router, "analiza contratos de mas de 100 millones de alcaldia de baranoa",
    )
    assert value_magnitudes_differ(request) is False


# ── Fuente solicitada ───────────────────────────────────────────────────────

def test_a_contracts_request_leaves_the_process_plan_unexecuted(router):
    _, (process_plan, contract_plan) = _plans(
        router, "analizame que contratos firmo alcaldia de baranoa en agosto",
    )
    assert process_plan.should_execute is False
    assert contract_plan.should_execute is True


def test_a_processes_request_leaves_the_contract_plan_unexecuted(router):
    _, (process_plan, contract_plan) = _plans(
        router, "analiza procesos de vias publicados por alcaldia de baranoa en agosto",
    )
    assert process_plan.should_execute is True
    assert contract_plan.should_execute is False
