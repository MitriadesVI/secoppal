"""Independent workflow counterexamples; assertions are the frozen contract."""
import json

import pytest

from app.core.conversation_store import ConversationStore
from entity_profile_frame_b_harness import BUYER, Conversation, frame_v12  # noqa: F401


def observe(chat, turn):
    read = chat.published()
    print(json.dumps({'query': turn.query, 'result': turn.result, 'calls': turn.calls,
                      'envelope': read.envelope.to_dict() if read.envelope else None},
                     default=str, ensure_ascii=False))


@pytest.mark.parametrize('interruption', ['Más.', '1', 'más repetido', 'old-chip'])
@pytest.mark.parametrize('pending', [False, True])
def test_clarification_preserves_the_profile_it_offers_to_continue(frame_v12, interruption, pending):
    chat = Conversation('ind-continuity')
    observe(chat, chat.say(f'Analiza contratos de {BUYER} en agosto.'))
    if pending:
        chat.say(f'Analiza contratos publicados en agosto de {BUYER}.')
    before = chat.profile_state().to_dict()
    kwargs = {}
    if interruption == 'old-chip':
        kwargs['seed_resolved_params'] = {'objeto': ['obra'], '_continuation_ref': {'result_id': 'old', 'conversation_revision': 0}}
    query = 'más' if interruption == 'más repetido' else interruption
    turn = chat.say(query, **kwargs)
    observe(chat, turn)
    after = chat.profile_state().to_dict()
    if interruption == 'más repetido':
        observe(chat, chat.say('más'))
    third = chat.say('Proveedores.')
    observe(chat, third)
    assert turn.calls == ()
    assert after == before, 'A clarification erased confirmed/pending state'
    if not pending:
        assert third.route == 'entity_profile_suppliers'
        assert len(third.calls) == 2


@pytest.mark.parametrize('initial', [None, BUYER, 'alcaldia de galapa'])
@pytest.mark.parametrize('replacement', [BUYER, 'alcaldia de galapa'])
def test_full_reformulation_exits_a_pending_without_alternatives(frame_v12, initial, replacement):
    chat = Conversation('ind-reformulate')
    if initial:
        chat.say(f'analiza contratos de {initial} en julio')
    first = chat.say(f'Analiza contratos publicados en agosto de {BUYER}.')
    observe(chat, first)
    assert chat.profile_state().pending is not None
    assert not chat.profile_state().pending.alternatives
    second = chat.say(f'Analiza contratos firmados en agosto de {replacement}.')
    observe(chat, second)
    assert not second.clarifies, 'Supported complete reformulation stayed UNRESOLVED'
    assert second.calls
    assert chat.profile_state().pending is None


def test_explicit_cancel_is_a_control_not_a_required_workaround(frame_v12):
    chat = Conversation('ind-cancel')
    chat.say(f'analiza contratos de {BUYER} en julio')
    before = chat.profile_state().confirmed_frame
    chat.say(f'analiza contratos publicados de {BUYER} en agosto')
    cancel = chat.say('cancela ese cambio')
    assert not cancel.calls
    assert chat.profile_state().confirmed_frame == before
    assert chat.profile_state().pending is None
    assert chat.say('proveedores').calls


@pytest.mark.parametrize('pending', [False, True])
@pytest.mark.parametrize('query,route', [
    ('mis radares', 'radar_'),
    ('ejecutar radar Sintetico', 'radar_'),
    ('CO1.PCCNTR.12345', 'direct_lookup'),
    ('contratos del proveedor CC 70000011', 'actor'),
    ('procesos de pavimentacion en bogota', 'tabular'),
    ('/reset', 'reset'),
])
def test_explicit_external_commands_keep_their_handler(frame_v12, pending, query, route):
    chat = Conversation('ind-external')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    if pending:
        chat.say(f'analiza contratos publicados de {BUYER} en agosto')
    turn = chat.say(query)
    observe(chat, turn)
    assert not turn.route.startswith('entity_profile'), 'Profile captured an explicit external command'
    if route == 'radar_':
        assert turn.route.startswith(route)
    elif route == 'reset':
        assert turn.route == 'reset'
        assert not chat.published().usable
    elif route == 'direct_lookup':
        assert 'lookup' in turn.route
    elif route == 'actor':
        assert turn.result.get('resolved_params', {}).get('actor_scope')
    else:
        assert turn.calls and not turn.owns_profile


@pytest.mark.parametrize('damage', ['plan_ref', 'evidence_ref', 'frame_ref', 'result_id', 'conversation_revision', 'execution_record', 'chat_id', 'record_version', 'catalog_version'])
def test_cross_references_are_validated_on_real_reload(frame_v12, damage):
    chat = Conversation('ind-broken-ref')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    path = ConversationStore()._envelope_path_for(chat.chat_id)
    raw = json.loads(path.read_text().splitlines()[-1])
    if damage in ('plan_ref', 'evidence_ref'):
        raw['continuation']['confirmed_profile'][damage] = 'foreign'
    elif damage == 'frame_ref':
        raw['continuation']['confirmed_profile'][damage] = ['foreign', 999]
    elif damage in ('result_id', 'conversation_revision'):
        raw['continuation'][damage] = 'foreign' if damage == 'result_id' else 999
    elif damage == 'record_version':
        raw['execution_record']['schema_version'] = 99
    elif damage == 'catalog_version':
        raw['execution_record']['catalog_version'] = 'unknown'
    elif damage == 'execution_record':
        raw[damage] = None
    else:
        raw[damage] = 'another-conversation'
    path.write_text(json.dumps(raw) + '\n')
    turn = chat.say('proveedores')
    observe(chat, turn)
    assert not turn.calls, f'Broken {damage} accepted and executed after reload'
    assert turn.clarifies
