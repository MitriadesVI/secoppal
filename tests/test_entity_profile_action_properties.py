from __future__ import annotations

from hypothesis import HealthCheck, given, settings, strategies as st

from app.core.entity_profile.executive_actions import actions_for_sources, project_actions
from app.core.entity_profile.frame_values import SourceKind
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401


@given(st.sets(st.sampled_from([source.dataset_id for source in SourceKind])))
def test_every_source_subset_projects_exact_capabilities(sources):
    actions = actions_for_sources(sources)
    expected = {
        action.action_id
        for action in actions_for_sources({source.dataset_id for source in SourceKind})
        if action.available_for(frozenset(sources))
    }
    assert {action.action_id for action in actions} == expected
    assert [item["id"] for item in project_actions(sources)] == [
        action.action_id for action in actions
    ]


@settings(
    max_examples=4,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    st.sampled_from(["/reset", "guardar radar", "más resultados", "opción 1"]),
    st.dictionaries(
        st.sampled_from(["entidad_resolved", "fecha_desde", "dataset"]),
        st.text(min_size=1, max_size=12),
        min_size=1,
        max_size=3,
    ),
)
def test_parallel_text_seed_and_actor_payload_never_change_trusted_action(
    frame_v12, hostile_text, hostile_seed,
):
    chat_id = "frame-action-hostile-" + str(abs(hash((hostile_text, repr(hostile_seed)))))
    chat = Conversation(chat_id)
    first = chat.say("analiza la alcaldia de baranoa en 2026")
    reference = {
        "kind": "FRAME_ENVELOPE", "chat_id": chat_id,
        "result_id": first.result["result_id"],
        "conversation_revision": first.result["conversation_revision"],
    }
    workflow, client = chat.workflow()
    result = workflow.run_query(
        hostile_text,
        chat_id=chat_id,
        seed_resolved_params=hostile_seed,
        actor_candidate_selection={"operation": "FORGED", "candidate_rank": 999},
        profile_action_selection={
            "schema_version": 1,
            "action_id": "temporal",
            "reference": reference,
        },
    )
    assert result["route_reason"] == "entity_profile_temporal"
    assert {dataset for _, dataset, _ in client.calls} == {
        SourceKind.PROCESSES.dataset_id, SourceKind.CONTRACTS.dataset_id,
    }
    assert all("ALCALDIA MUNICIPAL DE BARANOA" in sql for _, _, sql in client.calls)
    assert all("2026-01-01" in sql and "2026-12-31" in sql for _, _, sql in client.calls)
