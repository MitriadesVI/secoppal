"""La conversación del perfil, con disco real y un workflow nuevo por turno.

No hay merge simulado: cada turno crea su `SecopalWorkflow` y el
`ConversationStore` lee el JSONL que dejó el anterior. Es la única forma de
comprobar qué se GUARDÓ, que es donde estaba el defecto — V1 verificaba que un
diccionario de entrada no se mutara.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.core.entity_profile.conversation_state import (
    CONVERSATION_STATE_VERSION,
    STATE_PARAM,
    EntityProfileConversationState,
)
from app.core.entity_profile.source_request import RequestedSources, SourceProvenance
from test_entity_profile_workflow import _workflow


TODAY = date(2026, 9, 7)
BUYER = "alcaldia de baranoa"


def _run(queries, chat_id):
    """Un workflow nuevo por turno; el estado sólo puede viajar por disco."""
    seen = []
    for query in queries:
        workflow, client = _workflow()
        result = workflow.run_query(query, chat_id=chat_id, today=TODAY)
        seen.append({
            "query": query,
            "params": result.get("resolved_params") or {},
            "clarify": bool(result.get("needs_clarification")),
            "reason": result.get("clarification_reason") or "",
            "route": result.get("route_reason"),
            "calls": list(client.calls),
        })
    return seen


def _state(observation) -> EntityProfileConversationState:
    return EntityProfileConversationState.from_dict(
        observation["params"].get(STATE_PARAM),
    )


# ── 4A: esquema serializable y versionado ──────────────────────────────────

def test_the_persisted_state_is_versioned_and_json_safe():
    import json

    turns = _run([f"analiza contratos de {BUYER} en agosto"], "tx-version")
    raw = turns[0]["params"][STATE_PARAM]
    assert json.loads(json.dumps(raw)) == raw
    assert raw["version"] == CONVERSATION_STATE_VERSION
    assert raw["pending"] is None


def test_a_state_from_another_version_is_discarded_and_never_guessed():
    revived = EntityProfileConversationState.from_dict(
        {"version": CONVERSATION_STATE_VERSION + 1, "confirmed": {"focus": "x"}},
    )
    assert revived.confirmed is None and revived.pending is None


# ── 4D / 4H / 4I: follow-up ortogonal ──────────────────────────────────────

def test_a_period_followup_preserves_focus_and_event():
    turns = _run([
        f"analiza contratos firmados de {BUYER} en agosto",
        "proveedores",
        "en 2025",
    ], "tx-orthogonal")
    for turn in turns:
        confirmed = _state(turn).confirmed
        assert confirmed is not None
        assert confirmed.sources is RequestedSources.CONTRACTS
        assert confirmed.event.value == "CONTRACT_SIGNATURE"
    assert _state(turns[-1]).confirmed.focus == "suppliers"
    assert _state(turns[-1]).confirmed.time_from == "2025-01-01"


def test_a_focus_followup_preserves_source_and_period():
    turns = _run([
        f"analiza contratos de {BUYER} en agosto",
        "y los proveedores",
    ], "tx-focus")
    confirmed = _state(turns[-1]).confirmed
    assert confirmed.focus == "suppliers"
    assert confirmed.sources is RequestedSources.CONTRACTS
    assert confirmed.time_from == "2026-08-01"


# ── 4K: el origen sobrevive a la herencia ──────────────────────────────────

def test_an_explicit_origin_is_not_downgraded_by_inheritance():
    turns = _run([
        f"analiza contratos de {BUYER}",
        "en agosto",
    ], "tx-origin")
    confirmed = _state(turns[-1]).confirmed
    assert confirmed.sources_provenance is SourceProvenance.INHERITED
    assert confirmed.sources_origin is SourceProvenance.EXPLICIT


# ── 4B / 4G: la aclaración es transaccional ────────────────────────────────

def test_a_pending_clarification_never_replaces_the_confirmed_request():
    turns = _run([
        f"analiza procesos de {BUYER}",
        "en agosto",
        "proveedores",
    ], "tx-pending")
    assert turns[2]["clarify"] and not turns[2]["calls"]
    held = _state(turns[2])
    assert held.has_pending
    assert held.pending.focus == "suppliers"
    # Lo confirmado sigue siendo lo último que la persona pidió Y se pudo hacer.
    assert held.confirmed.focus == "overview"
    assert held.confirmed.sources is RequestedSources.PROCESSES
    assert held.confirmed.time_from == "2026-08-01"


def test_an_ambiguous_yes_reasks_instead_of_choosing_for_the_person():
    turns = _run([
        f"analiza procesos de {BUYER}",
        "proveedores",
        "si",
    ], "tx-yes")
    assert turns[1]["clarify"]
    assert turns[2]["clarify"], "un «sí» a secas no selecciona ninguna alternativa"
    assert turns[2]["reason"] == turns[1]["reason"]
    assert not turns[2]["calls"]
    assert _state(turns[2]).confirmed.sources is RequestedSources.PROCESSES


def test_an_explicit_choice_resolves_the_clarification():
    turns = _run([
        f"analiza procesos de {BUYER}",
        "proveedores",
        "solo contratos",
    ], "tx-choice")
    assert turns[1]["clarify"]
    assert not turns[2]["clarify"]
    confirmed = _state(turns[2]).confirmed
    assert confirmed.sources is RequestedSources.CONTRACTS
    assert not _state(turns[2]).has_pending


# ── 4J: la recarga real conserva el contrato ───────────────────────────────

@pytest.mark.parametrize("fields", [
    ("objeto", "objeto_or", "objeto_phrases", "objeto_exclude"),
])
def test_topic_operators_survive_a_real_reload(fields):
    turns = _run([
        f"analiza contratos de mantenimiento o vias excepto interventoria de {BUYER}",
        "en agosto",
    ], "tx-operators")
    first = {key: turns[0]["params"].get(key) for key in fields}
    assert any(first.values())
    assert {key: turns[-1]["params"].get(key) for key in fields} == first
