"""AP3-F frozen dogfood transcripts for natural actor conversation.

These tests deliberately use the production actor directory: the names and
candidate ranks are the evidence exposed by manual dogfood, not synthetic
identity fixtures.  Tests that exercise index-unavailable behavior live in the
isolated actor suites.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from burr.core import State

from app.config import Settings
from app.core.actor_name import ActorResolutionStatus
from app.core.actor_resolution import METHOD_USER_SELECTED_CANDIDATE
from app.core.orchestrator import SecopalWorkflow
from app.core.query_router import QueryRouter
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.data.actor_alias_resolver import ActorNameResolver


PRODUCTION_INDEX = (Path("data") / "actor_alias_index.sqlite").resolve()


def _require_production_index() -> Path:
    if not PRODUCTION_INDEX.is_file():
        pytest.skip("production actor index is not available")
    return PRODUCTION_INDEX


def _workflow(tmp_path: Path) -> SecopalWorkflow:
    workflow = SecopalWorkflow(Settings(
        secop_actor_alias_index_path=_require_production_index(),
        secop_feedback_path=tmp_path / "feedback.jsonl",
        secop_feedback_db_path=tmp_path / "feedback.sqlite",
        secop_feedback_sqlite_enabled=False,
    ))
    workflow.secop_client.query = lambda *args, **kwargs: []
    workflow.secop_client.count = lambda *args, **kwargs: 0
    workflow.narrator.narrate_with_grounding = lambda **kwargs: None
    return workflow


def _resolved_parse(query: str) -> dict:
    params = dict(QueryRouter().parse(query).params)
    state = State({"parsed_params": params, "raw_query": query})
    resolver = ActorNameResolver(_require_production_index())
    return dict(resolve_actor_name(state, resolver)["parsed_params"])


def test_d1_con_means_buyer_and_preserves_actor_and_year(tmp_path):
    workflow = _workflow(tmp_path)

    first = workflow.run_query("busca proveedor funsocom", chat_id="ap3f-d1")
    second = workflow.run_query("que ha hecho en 2026?", chat_id="ap3f-d1")
    third = workflow.run_query("con cordoba?", chat_id="ap3f-d1")

    first_params = first["resolved_params"]
    second_params = second["resolved_params"]
    params = third["resolved_params"]
    assert first_params["actor_scope"]["observed_name"] == "funsocom"
    assert second_params["actor_scope"]["base_number"] == "900334689"
    assert params["actor_scope"]["base_number"] == "900334689"
    assert params["fecha_desde"] == "2026-01-01"
    assert params["fecha_hasta"] == "2026-12-31"
    assert params["entidad_resolved"] == "DEPARTAMENTO DE CORDOBA"
    assert params.get("departamento_resolved") is None


def test_d2_candidate_two_has_selection_precedence_and_no_framing_leak(tmp_path):
    workflow = _workflow(tmp_path)

    pending = workflow.run_query(
        "buscate al proveedor jorge verbel", chat_id="ap3f-d2"
    )
    selected = workflow.run_query("2", chat_id="ap3f-d2")

    assert pending["resolved_params"]["actor_resolution_status"] == (
        ActorResolutionStatus.CANDIDATES
    )
    params = selected["resolved_params"]
    assert params["actor_resolution_method"] == METHOD_USER_SELECTED_CANDIDATE
    assert params["actor_scope"]["base_number"] == "1020727022"
    assert params.get("objeto") in (None, [])


def test_d3_actor_and_buyer_are_separate_roles_and_not_topic():
    params = _resolved_parse(
        "contrato de jose cantillo con alcaldia de barranquilla"
    )

    assert params["actor_resolution_status"] in {
        ActorResolutionStatus.CANDIDATES,
        ActorResolutionStatus.AMBIGUOUS,
        ActorResolutionStatus.RESOLVED,
    }
    pending = params.get("actor_pending", {})
    actor_name = pending.get("actor_name") or params.get("actor_scope", {}).get(
        "observed_name"
    )
    assert actor_name == "jose cantillo"
    assert params["entidad_resolved"] == (
        "DISTRITO ESPECIAL INDUSTRIAL Y PORTUARIO DE BARRANQUILLA"
    )
    assert params.get("objeto") in (None, [])


def test_d4_distinctive_actor_token_cannot_degrade_to_topic():
    params = _resolved_parse("contratos de lmp ingieneria en 2025")

    assert params["fecha_desde"] == "2025-01-01"
    assert params["fecha_hasta"] == "2025-12-31"
    assert params["actor_resolution_status"] in {
        ActorResolutionStatus.CANDIDATES,
        ActorResolutionStatus.AMBIGUOUS,
        ActorResolutionStatus.UNRESOLVED,
    }
    assert params.get("actor_pending", {}).get("actor_name") == "lmp ingieneria"
    assert params.get("objeto") in (None, [])
