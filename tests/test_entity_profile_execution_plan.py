"""Matriz foco × fuentes y compuertas de llamada: una sola autoridad decide."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core.entity_profile.execution_plan import (
    FOCUS_CAPABILITY,
    build_execution_plan,
)
from app.core.entity_profile.models import (
    CONTRACT_DATASET,
    PROCESS_DATASET,
    SectionStatus,
)
from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.semantic_request import build_semantic_request
from app.core.entity_profile.source_request import RequestedSources
from app.core.entity_profile.turn import classify_entity_profile_turn, profile_query_params
from app.core.entity_profile.assembler import build_entity_profile
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from test_entity_profile_queries import FakeClient


TODAY = date(2026, 9, 5)
BUYER = "analiza alcaldia de baranoa"


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _plan(router: QueryRouter, text: str, focus: str):
    params = profile_query_params(router.parse(text, today=TODAY).params)
    scope = resolve_profile_scope(params).scope
    assert scope is not None, text
    request = build_semantic_request(
        user_query=text, params=params, scope=scope, focus=focus,
    )
    return build_execution_plan(request, params)


PROCESSES_TEXT = "analiza procesos de alcaldia de baranoa"
CONTRACTS_TEXT = "analiza contratos de alcaldia de baranoa"
BOTH_TEXT = "analiza procesos y contratos de alcaldia de baranoa"


# ── Matriz foco × fuentes ───────────────────────────────────────────────────

@pytest.mark.parametrize("text,focus,expected", [
    (BOTH_TEXT, "overview", RequestedSources.BOTH),
    (CONTRACTS_TEXT, "overview", RequestedSources.CONTRACTS),
    (PROCESSES_TEXT, "overview", RequestedSources.PROCESSES),
    (BOTH_TEXT, "suppliers", RequestedSources.CONTRACTS),
    (CONTRACTS_TEXT, "suppliers", RequestedSources.CONTRACTS),
    (BOTH_TEXT, "recent", RequestedSources.BOTH),
    (BOTH_TEXT, "temporal", RequestedSources.BOTH),
    (BOTH_TEXT, "modalities", RequestedSources.PROCESSES),
    (BOTH_TEXT, "values", RequestedSources.CONTRACTS),
    (BUYER, "overview", RequestedSources.BOTH),
])
def test_effective_sources_are_the_requested_ones_narrowed_by_capability(
    router, text, focus, expected,
):
    plan = _plan(router, text, focus)
    assert plan.clarification is None
    assert plan.effective_sources is expected


def test_focus_never_rewrites_the_requested_source(router):
    plan = _plan(router, BOTH_TEXT, "suppliers")
    assert plan.request.sources is RequestedSources.BOTH
    assert plan.effective_sources is RequestedSources.CONTRACTS


@pytest.mark.parametrize("text,focus", [
    (PROCESSES_TEXT, "suppliers"),
    (PROCESSES_TEXT, "values"),
    (CONTRACTS_TEXT, "modalities"),
])
def test_an_empty_capability_intersection_asks_instead_of_widening(router, text, focus):
    plan = _plan(router, text, focus)
    assert plan.clarification is not None
    assert plan.jobs == ()
    assert plan.effective_sources is None


def test_the_clarification_names_both_sides_of_the_contradiction(router):
    plan = _plan(router, PROCESSES_TEXT, "suppliers")
    assert "procesos" in plan.clarification
    assert "contratos" in plan.clarification


def test_every_focus_declares_a_capability(router):
    from app.core.entity_profile.query_runner import jobs_for_focus

    for focus in FOCUS_CAPABILITY:
        assert jobs_for_focus(focus)


def test_an_unknown_focus_is_refused(router):
    with pytest.raises(ValueError, match="focus"):
        _plan(router, BUYER, "inventado")


# ── Compuertas de llamada a la fuente ───────────────────────────────────────

def _calls(router: QueryRouter, text: str):
    turn = classify_entity_profile_turn(text, router.parse(text, today=TODAY).params)
    client = FakeClient()
    if turn.plan is None or turn.clarification:
        return turn, client
    build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=client, soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    return turn, client


def test_a_contracts_only_request_issues_zero_process_calls(router):
    _, client = _calls(router, "analizame que contratos firmo alcaldia de baranoa en agosto")
    assert [d for _, d, _ in client.calls if d == PROCESS_DATASET] == []
    assert [d for _, d, _ in client.calls if d == CONTRACT_DATASET]


def test_a_processes_only_request_issues_zero_contract_calls(router):
    _, client = _calls(router, "analiza procesos de alcaldia de baranoa")
    assert [d for _, d, _ in client.calls if d == CONTRACT_DATASET] == []
    assert [d for _, d, _ in client.calls if d == PROCESS_DATASET]


@pytest.mark.parametrize("text", [
    "analiza procesos de alcaldia de baranoa con proveedores",
    "analiza contratos terminados en agosto de alcaldia de baranoa",
])
def test_an_unanswerable_combination_issues_zero_calls(router, text):
    turn, client = _calls(router, text)
    assert turn.clarification is not None
    assert client.calls == []


def test_a_both_request_keeps_its_six_historical_calls(router):
    _, client = _calls(router, "analiza alcaldia de baranoa en 2026")
    assert len(client.calls) == 6


# ── Secciones NO SOLICITADAS ────────────────────────────────────────────────

def test_a_source_that_was_not_requested_is_not_zero_and_not_a_failure(router):
    turn = classify_entity_profile_turn(
        "analizame que contratos firmo alcaldia de baranoa en agosto",
        router.parse(
            "analizame que contratos firmo alcaldia de baranoa en agosto", today=TODAY,
        ).params,
    )
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=FakeClient(), soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    assert profile.modalities.status is SectionStatus.UNAVAILABLE
    assert profile.modalities.attempted is False
    assert dict(profile.modalities.coverage)["reason"] == "NOT_REQUESTED"
    assert profile.activity.fact("process_count") is None
    assert profile.activity.fact("contract_count") == 4
    assert profile.partial is False
    assert "modalities" not in profile.failed_sections


def test_an_executed_section_still_declares_only_its_real_sources(router):
    turn = classify_entity_profile_turn(
        "analizame que contratos firmo alcaldia de baranoa en agosto",
        router.parse(
            "analizame que contratos firmo alcaldia de baranoa en agosto", today=TODAY,
        ).params,
    )
    profile = build_entity_profile(
        scope=turn.scope, identity=turn.identity, params=turn.params,
        focus=turn.focus, secop_client=FakeClient(), soql_builder=SoQLBuilder(),
        plan=turn.plan,
    )
    assert profile.activity.source == (CONTRACT_DATASET,)
    assert profile.recent.source == (CONTRACT_DATASET,)


# ── Listado vs perfil ───────────────────────────────────────────────────────

@pytest.mark.parametrize("text,is_profile", [
    ("que contratos firmo alcaldia de baranoa en agosto", False),
    ("muestrame los contratos que firmo alcaldia de baranoa", False),
    ("analizame que contratos firmo alcaldia de baranoa", True),
    ("analiza contratos de mantenimiento de alcaldia de baranoa", True),
])
def test_only_an_analytical_request_becomes_a_profile(router, text, is_profile):
    turn = classify_entity_profile_turn(text, router.parse(text, today=TODAY).params)
    assert turn.detected is is_profile
