from __future__ import annotations

from dataclasses import replace

import pytest

from app.core.conversation_envelope import EnvelopeRead, EnvelopeStatus
from app.core.entity_profile.frame_values import SourceKind
from app.core.entity_profile.profile_state import ProfileConversationState
from app.core.workflow.profile_action_admission import admit_profile_action
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import _workflow


ACTION_CASES = (
    (
        "suppliers", "Muéstrame sus principales proveedores",
        "entity_profile_suppliers", {SourceKind.CONTRACTS.dataset_id}, 2,
    ),
    (
        "modalities", "Muéstrame sus modalidades",
        "entity_profile_modalities", {SourceKind.PROCESSES.dataset_id}, 1,
    ),
    (
        "temporal", "Muéstrame su evolución",
        "entity_profile_temporal",
        {SourceKind.PROCESSES.dataset_id, SourceKind.CONTRACTS.dataset_id}, 2,
    ),
    (
        "recent_contracts", "Muéstrame sus contratos recientes",
        "entity_profile_recent", {SourceKind.CONTRACTS.dataset_id}, 1,
    ),
)


def _reference(result):
    return {
        "kind": "FRAME_ENVELOPE",
        "chat_id": result["chat_id"],
        "result_id": result["result_id"],
        "conversation_revision": result["conversation_revision"],
    }


def _event(action_id, reference):
    return {"schema_version": 1, "action_id": action_id, "reference": reference}


@pytest.mark.parametrize("action_id,phrase,route,datasets,calls", ACTION_CASES)
def test_four_actions_use_only_current_published_frame(
    frame_v12, action_id, phrase, route, datasets, calls,
):
    chat = Conversation(f"frame-action-{action_id}")
    first = chat.say("analiza la alcaldia de baranoa en 2026")
    assert [action["id"] for action in first.result["entity_profile_view"]["actions"]] == [
        "suppliers", "modalities", "temporal", "recent_contracts",
    ]
    workflow, client = chat.workflow()
    result = workflow.select_entity_profile_action(
        action_id, _reference(first.result), channel="streamlit", chat_id=chat.chat_id,
    )
    assert result["profile_action_phrase"] == phrase
    assert result["route_reason"] == route
    assert len(client.calls) == calls
    assert {dataset for _, dataset, _ in client.calls} == datasets
    assert all("ALCALDIA MUNICIPAL DE BARANOA" in sql for _, _, sql in client.calls)
    assert all("2026-01-01" in sql and "2026-12-31" in sql for _, _, sql in client.calls)
    confirmed = chat.profile_state().confirmed_frame
    assert confirmed.buyer_scope.canonical_buyers == ("ALCALDIA MUNICIPAL DE BARANOA",)


def test_recent_contracts_keeps_contract_source_and_period(frame_v12):
    chat = Conversation("frame-action-recent-scope")
    first = chat.say("analiza la alcaldia de baranoa en 2026")
    workflow, client = chat.workflow()
    result = workflow.select_entity_profile_action(
        "recent_contracts", _reference(first.result), chat_id=chat.chat_id,
    )
    assert result["route_reason"] == "entity_profile_recent"
    assert [dataset for _, dataset, _ in client.calls] == [SourceKind.CONTRACTS.dataset_id]
    confirmed = chat.profile_state().confirmed_frame
    assert confirmed.source_selection.requested == frozenset({SourceKind.CONTRACTS})
    period = confirmed.predicates_for(SourceKind.CONTRACTS).temporal.period
    assert (period.lower, period.upper) == ("2026-01-01", "2026-12-31")


@pytest.mark.parametrize(
    "mutation",
    (
        lambda ref: {**ref, "result_id": "old"},
        lambda ref: {**ref, "chat_id": "foreign"},
        lambda ref: {**ref, "conversation_revision": True},
        lambda ref: {**ref, "extra": "forged"},
    ),
)
def test_stale_foreign_bool_revision_and_extra_fields_make_zero_calls(
    frame_v12, mutation,
):
    chat = Conversation(f"frame-action-invalid-{id(mutation)}")
    first = chat.say("analiza la alcaldia de baranoa")
    workflow, Yen = chat.workflow()
    result = workflow.run_query(
        "/reset",
        chat_id=chat.chat_id,
        seed_resolved_params={"entidad_resolved": "FORGED"},
        actor_candidate_selection={"operation": "FORGED"},
        profile_action_selection=_event("suppliers", mutation(_reference(first.result))),
    )
    assert result["needs_clarification"]
    assert Yen.calls == []


def test_unknown_action_makes_zero_calls(frame_v12):
    chat = Conversation("frame-action-unknown")
    first = chat.say("analiza la alcaldia de baranoa")
    workflow, client = chat.workflow()
    result = workflow.select_entity_profile_action(
        "unknown", _reference(first.result), chat_id=chat.chat_id,
    )
    assert result["needs_clarification"]
    assert client.calls == []


def test_invalid_action_is_rejected_before_parser_or_external_routing(
    frame_v12, monkeypatch,
):
    chat = Conversation("frame-action-before-parser")
    first = chat.say("analiza la alcaldia de baranoa")
    workflow, client = chat.workflow()

    def forbidden(*_args, **_kwargs):
        pytest.fail("parser or foreign route ran before action validation")

    monkeypatch.setattr(workflow.query_router, "parse", forbidden)
    monkeypatch.setattr(workflow, "_dispatch", forbidden)
    bad = _reference(first.result)
    bad["conversation_revision"] = True
    result = workflow.run_query(
        "/reset",
        chat_id=chat.chat_id,
        profile_action_selection=_event("suppliers", bad),
    )
    assert result["needs_clarification"]
    assert client.calls == []


def test_corrupt_or_absent_envelope_is_never_an_action_authority():
    event = _event("suppliers", {
        "kind": "FRAME_ENVELOPE", "chat_id": "c", "result_id": "r",
        "conversation_revision": 1,
    })
    for read in (None, EnvelopeRead(EnvelopeStatus.CORRUPT)):
        result = admit_profile_action(
            event,
            chat_id="c",
            read=read,
            state=ProfileConversationState(),
            contract=None,
        )
        assert not result.admitted
        assert result.response


def test_foreign_owner_and_capability_forgery_make_zero_calls(frame_v12):
    chat = Conversation("frame-action-owner")
    first = chat.say("analiza solo procesos de la alcaldia de baranoa en 2026")
    workflow, client = chat.workflow()
    forged = workflow.select_entity_profile_action(
        "suppliers", _reference(first.result), chat_id=chat.chat_id,
    )
    assert forged["needs_clarification"]
    assert client.calls == []

    replacement = chat.say("contratos de alimentación en bogotá")
    current = _reference(replacement.result)
    workflow, client = chat.workflow()
    foreign_owner = workflow.select_entity_profile_action(
        "suppliers", current, chat_id=chat.chat_id,
    )
    assert foreign_owner["needs_clarification"]
    assert client.calls == []


def test_pending_profile_rejects_action_without_calls(frame_v12):
    chat = Conversation("frame-action-pending")
    chat.say("analiza procesos y contratos de la alcaldia de baranoa en agosto")
    pending = chat.say("proveedores")
    assert pending.clarifies
    workflow, client = chat.workflow()
    result = workflow.select_entity_profile_action(
        "suppliers", _reference(pending.result), chat_id=chat.chat_id,
    )
    assert result["needs_clarification"]
    assert client.calls == []


def test_action_reference_is_single_use_replay_fails_closed(frame_v12):
    chat = Conversation("frame-action-replay")
    first = chat.say("analiza la alcaldia de baranoa en 2026")
    reference = _reference(first.result)
    workflow, _ = chat.workflow()
    assert not workflow.select_entity_profile_action(
        "modalities", reference, chat_id=chat.chat_id,
    )["needs_clarification"]
    workflow, client = chat.workflow()
    replay = workflow.select_entity_profile_action(
        "modalities", reference, chat_id=chat.chat_id,
    )
    assert replay["needs_clarification"]
    assert client.calls == []


def test_frame_off_keeps_v1_and_rejects_selection_without_dispatch(monkeypatch):
    workflow, client = _workflow()
    first = workflow.run_query("analiza la alcaldia de baranoa", chat_id="frame-off")
    assert "actions" not in first["entity_profile_view"]
    before = len(client.calls)
    monkeypatch.setattr(workflow, "_dispatch", lambda *_args: pytest.fail("dispatch called"))
    result = workflow.select_entity_profile_action(
        "suppliers",
        {"kind": "FRAME_ENVELOPE", "chat_id": "frame-off", "result_id": "x", "conversation_revision": 1},
        chat_id="frame-off",
    )
    assert result["needs_clarification"]
    assert len(client.calls) == before


def test_publication_failure_and_anonymous_profile_never_expose_actions(
    frame_v12, monkeypatch,
):
    chat = Conversation("frame-action-publication-failure")
    workflow, _ = chat.workflow()

    def fail_publish(*_args, **_kwargs):
        raise OSError("publication unavailable")

    monkeypatch.setattr(workflow.conv_store, "publish_turn", fail_publish)
    failed = workflow.run_query(
        "analiza la alcaldia de baranoa", chat_id=chat.chat_id,
    )
    assert failed["continuation_published"] is False
    assert "actions" not in failed["entity_profile_view"]

    anonymous_workflow, _ = chat.workflow()
    anonymous = anonymous_workflow.run_query("analiza la alcaldia de baranoa")
    assert "actions" not in anonymous["entity_profile_view"]


def test_route_none_clarifies_without_dispatch(frame_v12, monkeypatch):
    chat = Conversation("frame-action-route-none")
    first = chat.say("analiza la alcaldia de baranoa")
    workflow, client = chat.workflow()
    monkeypatch.setattr(
        "app.core.workflow.profile_route.run_profile_route",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(workflow, "_dispatch", lambda *_args: pytest.fail("dispatch called"))
    result = workflow.select_entity_profile_action(
        "suppliers", _reference(first.result), chat_id=chat.chat_id,
    )
    assert result["needs_clarification"]
    assert result["route_reason"] == "admission_profile_action_route_unavailable"
    assert client.calls == []


@pytest.mark.parametrize("hostile_text", ("/reset", "guardar radar", "más resultados", "opción 1"))
def test_action_lane_omits_reset_radar_pagination_actor_and_dispatch(
    frame_v12, monkeypatch, hostile_text,
):
    chat = Conversation("frame-action-exclusive-" + str(abs(hash(hostile_text))))
    first = chat.say("analiza la alcaldia de baranoa en 2026")
    workflow, client = chat.workflow()

    def forbidden(*_args, **_kwargs):
        pytest.fail("foreign route was called")

    monkeypatch.setattr(workflow, "_dispatch", forbidden)
    monkeypatch.setattr("app.core.radar.commands.parse_radar_command", forbidden)
    monkeypatch.setattr("app.core.workflow.handlers.pagination._requested_group", forbidden)
    result = workflow.run_query(
        hostile_text,
        chat_id=chat.chat_id,
        actor_candidate_selection={"operation": "FORGED"},
        seed_resolved_params={"offset": 999},
        profile_action_selection=_event("modalities", _reference(first.result)),
    )
    assert result["route_reason"] == "entity_profile_modalities"
    assert len(client.calls) == 1
