"""Authorized Actor port: independent direct/API states and consumed-token rejection."""
import pytest

from app.core.actor_resolution import pending_selection_token
from app.core.actor_conversation import ActorConversationOperation
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow
from app.core.workflow.handlers.actor_selection import handle_actor_selection
from entity_profile_frame_b_harness import BUYER, frame_v12  # noqa: F401
from test_actor_dogfood_hardening_v2 import workflow  # noqa: F401


def prepare(flow, chat, pending):
    first = flow.run_query('contratos de beatriz solano con alcaldia de barranquilla', chat_id=chat)
    actor = first['resolved_params']['actor_pending']
    token = actor.get('selection_token') or pending_selection_token(actor)
    flow.run_query(f'analiza contratos de {BUYER} en agosto', chat_id=chat)
    if pending:
        flow.run_query(f'analiza contratos publicados de {BUYER} en agosto', chat_id=chat)
    return token


@pytest.mark.parametrize('pending', [False, True])
def test_actor_direct_control_with_own_unconsumed_token(frame_v12, workflow, pending):
    chat = 'b1-actor-direct'
    token = prepare(workflow, chat, pending)
    ctx = workflow._make_ctx('opción 2', chat_id=chat, actor_candidate_selection={
        'operation': ActorConversationOperation.SELECT_ACTOR_CANDIDATE,
        'candidate_rank': 2, 'pending_token': token,
    })
    result = handle_actor_selection(workflow.deps, ctx)
    assert result['resolved_params']['actor_resolution_status'] == 'RESOLVED'
    assert result['resolved_params']['contratista'] == ['70000012']
    assert result['resolved_params']['actor_scope']['document_type'] == 'CC'


@pytest.mark.parametrize('pending', [False, True])
def test_actor_api_publishes_and_rejects_consumed_token(frame_v12, workflow, pending):
    chat = 'b1-actor-api'
    token = prepare(workflow, chat, pending)
    selected = workflow.select_actor_candidate(2, token, chat_id=chat)
    assert selected['resolved_params']['actor_resolution_status'] == 'RESOLVED'
    assert selected['resolved_params']['contratista'] == ['70000012']
    assert selected['resolved_params']['actor_scope']['document_type'] == 'CC'
    assert selected['continuation_published']
    assert selected['continuation']['owner'] == 'ACTOR'
    reloaded = SecopalWorkflow(workflow.settings)
    reloaded.secop_client = workflow.secop_client
    reloaded.narrator.narrate_with_grounding = lambda **kw: None
    read = reloaded.conv_store.load_envelope(chat)
    assert read.usable and read.envelope.result_id == selected['result_id']
    assert read.envelope.response == selected['response']
    assert ConversationStore().get_history(chat, 1)[0].parsed_params['contratista'] == ['70000012']
    before = len(workflow.source_calls)
    replay = reloaded.select_actor_candidate(2, token, chat_id=chat)
    assert replay['needs_clarification']
    assert replay['resolved_params'].get('actor_resolution_status') != 'RESOLVED'
    assert len(workflow.source_calls) == before
