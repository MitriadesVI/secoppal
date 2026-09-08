"""Positive continuity controls and independent token/owner boundaries."""
import hashlib
import json
from types import SimpleNamespace

import pytest

from app.config import get_settings
from app.core.conversation_store import ConversationStore
from app.core.radar.store import RadarStore
from entity_profile_frame_b_harness import BUYER, Conversation, frame_v12  # noqa: F401
from test_actor_dogfood_hardening_v2 import workflow  # noqa: F401
from test_v12b1_actor_port import prepare
from test_v12b_independent_boundaries import Pages


def test_publication_selects_exactly_the_prepared_generation(frame_v12, monkeypatch):
    from app.core.workflow import continuity_staging
    chat = Conversation('b1-independent-generation', client_factory=Pages)
    chat.say('contratos de obra en baranoa')
    store = ConversationStore()
    original = continuity_staging.prepare_generation
    prepared = []

    def observe(store, reservation):
        generation = original(store, reservation)
        history = store._path_for(chat.chat_id)
        buffer = store._pagination_path_for(chat.chat_id)
        prepared.append((generation, hashlib.sha256(history.read_bytes()).hexdigest(),
                         hashlib.sha256(buffer.read_bytes()).hexdigest()))
        return generation

    with monkeypatch.context() as patch:
        patch.setattr(continuity_staging, 'prepare_generation', observe)
        second = chat.say('procesos de alimentacion en bogota')
    envelope = chat.published().envelope
    assert prepared == [(envelope.continuity_generation,
                         hashlib.sha256(store._path_for(chat.chat_id).read_bytes()).hexdigest(),
                         hashlib.sha256(store._pagination_path_for(chat.chat_id).read_bytes()).hexdigest())]
    assert envelope.continuity_generation == second.result['result_id']
    assert all(group['generation'] == envelope.continuity_generation
               for group in envelope.continuation['tabular_groups'].values())
    assert chat.say('más').result['dataset_id'] == 'p6dx-8zbt'


@pytest.mark.parametrize('reference', ['absent', 'incomplete', 'old', 'foreign_chat', 'valid'])
def test_ui_reference_is_checked_on_real_reload(frame_v12, reference):
    chat = Conversation('b1-independent-ui')
    first = chat.say('contratos de obra en baranoa')
    second = chat.say('contratos de alimentacion en bogota')
    ref = {'chat_id': chat.chat_id, 'result_id': second.result['result_id'],
           'conversation_revision': second.revision}
    if reference == 'old':
        ref.update(result_id=first.result['result_id'], conversation_revision=first.revision)
    elif reference == 'incomplete':
        ref.pop('conversation_revision')
    elif reference == 'foreign_chat':
        ref['chat_id'] = 'different-chat'
    seed = {'dataset': 'procesos', 'objeto': ['pavimentacion']}
    if reference != 'absent':
        seed['_continuation_ref'] = ref
    turn = chat.say('aplicar filtro', seed_resolved_params=seed)
    if reference == 'valid':
        assert turn.calls and not turn.clarifies
    else:
        assert turn.clarifies and not turn.calls


@pytest.mark.parametrize('damage', ['token', 'ttl', 'version', 'identity', 'disabled'])
def test_actor_port_does_not_weaken_existing_guards(frame_v12, workflow, monkeypatch, damage):
    chat = 'b1-independent-actor'
    token = prepare(workflow, chat, pending=True)
    store = ConversationStore()
    path = store._path_for(chat)
    lines = path.read_text().splitlines()
    raw = json.loads(lines[-1])
    pending = raw['parsed_params']['actor_pending']
    if damage == 'token':
        token = 'foreign-token'
    elif damage == 'ttl':
        raw['timestamp'] = '2000-01-01T00:00:00+00:00'
    elif damage == 'version':
        pending['version'] = -1
    elif damage == 'identity':
        selected = next(candidate for candidate in pending['candidates'] if candidate['rank'] == 2)
        selected['document_type'] = 'UNSUPPORTED'
    else:
        monkeypatch.setenv('SECOPPAL_ACTOR_PROFILE_ENABLED', 'false')
        get_settings.cache_clear()
    path.write_text('\n'.join([*lines[:-1], json.dumps(raw)]) + '\n')
    before = len(workflow.source_calls)
    result = workflow.select_actor_candidate(2, token, chat_id=chat)
    print('ACTOR_GUARD', damage, result['route_reason'], result['resolved_params'])
    assert result['needs_clarification']
    assert result['resolved_params'].get('actor_resolution_status') != 'RESOLVED'
    assert len(workflow.source_calls) == before


@pytest.mark.parametrize('owner', ['owner-a', 'owner-b'])
def test_radar_pending_respects_real_owner_lookup(frame_v12, tmp_path, monkeypatch, owner):
    from app.core.workflow.handlers import radar
    store = RadarStore(tmp_path / 'synthetic-radar.sqlite')
    store.create_radar(name='Sintetico', owner='owner-a', query_frame_json='{}')
    chat = Conversation('b1-independent-radar')
    chat.say(f'analiza contratos publicados de {BUYER} en agosto')
    flow, client = chat.workflow()
    flow.radar_store = store
    effects = []

    def run(deps, real_store, record):
        effects.append((record.owner, record.name))
        return SimpleNamespace(status='ok', total=0, digest={'text': 'Synthetic only'}, outcome='completed')

    monkeypatch.setattr(radar, 'run_radar', run)
    result = flow.run_query('ejecutar radar Sintetico', chat_id=chat.chat_id,
                            user_identity={'channel_user_key': owner})
    assert not client.calls
    if owner == 'owner-a':
        assert result['route_reason'] == 'radar_run_ok' and effects == [('owner-a', 'Sintetico')]
    else:
        assert result['route_reason'] == 'radar_run_not_found' and effects == []
