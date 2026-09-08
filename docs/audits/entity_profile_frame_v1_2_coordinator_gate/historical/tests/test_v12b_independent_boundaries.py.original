"""Real continuation, compatibility and delegated authority boundaries."""
import json

import pytest
from hypothesis import given, settings, strategies as st

from app.config import get_settings
from app.core.actor_resolution import pending_selection_token
from app.core.conversation_envelope import EnvelopeStatus
from app.core.conversation_store import ConversationStore
from app.core.workflow.admission_rules import AdmissionRoute, classify
from app.core.workflow.continuation import ContinuationContract, ContinuationOwner, ResultKind, TabularGroup
from entity_profile_frame_b_harness import BUYER, Conversation, frame_v12  # noqa: F401
from test_actor_dogfood_hardening_v2 import workflow  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


class Pages(WorkflowClient):
    def query(self, dataset, soql):
        template = super().query(dataset, soql)[0]
        return [{**template, 'id_contrato': f'CO1.PCCNTR.{i}', 'objeto_del_contrato': 'OBRA PUBLICA',
                 'referencia_del_contrato': f'C-{i}'} for i in range(60)]

    def count(self, dataset, soql):
        self._record('count', dataset, soql)
        return 200


@pytest.mark.parametrize('reference', [None, {}, {'result_id': 'foreign', 'conversation_revision': 1}, 'valid'])
def test_ui_seed_requires_current_result_reference(frame_v12, reference):
    chat = Conversation('ind-seed')
    first = chat.say('contratos de obra en baranoa')
    seed = {'dataset': 'procesos', 'objeto': ['pavimentacion']}
    if reference == 'valid':
        reference = {'result_id': first.result['result_id'], 'conversation_revision': first.revision}
    if reference is not None:
        seed['_continuation_ref'] = reference
    turn = chat.say('aplicar filtro', seed_resolved_params=seed)
    print('SEED', reference, turn.route, len(turn.calls))
    if reference and reference.get('result_id') == first.result['result_id']:
        assert turn.calls
    else:
        assert turn.clarifies and not turn.calls


@pytest.mark.parametrize('field,value', [
    ('cursor_id', 'foreign'), ('generation', 'foreign'), ('fingerprint', 'foreign'),
    ('order_contract', 'foreign'), ('expiration', '2000-01-01T00:00:00+00:00'),
    ('group', 'foreign'),
])
def test_pagination_consumes_the_published_capability(frame_v12, field, value):
    chat = Conversation('ind-cursor', client_factory=Pages)
    first = chat.say('contratos de obra en baranoa')
    assert first.result['rows']
    path = ConversationStore()._envelope_path_for(chat.chat_id)
    raw = json.loads(path.read_text().splitlines()[-1])
    assert raw['continuation']['tabular_groups']
    for group in raw['continuation']['tabular_groups'].values():
        group[field] = value
    path.write_text(json.dumps(raw) + '\n')
    turn = chat.say('muestrame mas')
    print('CURSOR', field, turn.route, len(turn.calls), len(turn.result.get('rows', [])))
    assert turn.clarifies and not turn.result.get('rows'), 'Mismatched published capability did not govern pagination'


@pytest.mark.parametrize('flag', ['false', 'true'])
@pytest.mark.parametrize('profile_pending', [False, True])
def test_current_actor_api_token_reaches_actor_handler(frame_v12, workflow, monkeypatch, flag, profile_pending):
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', flag)
    get_settings.cache_clear()
    first = workflow.run_query('contratos de beatriz solano con alcaldia de barranquilla', chat_id='ind-actor')
    pending = first['resolved_params']['actor_pending']
    token = pending.get('selection_token') or pending_selection_token(pending)
    # B does not append profile turns to legacy: the pending Actor token is still current there.
    workflow.run_query(f'analiza contratos de {BUYER} en agosto', chat_id='ind-actor')
    if profile_pending:
        workflow.run_query(f'analiza contratos publicados de {BUYER} en agosto', chat_id='ind-actor')
    selected = workflow.select_actor_candidate(2, token, chat_id='ind-actor')
    print('ACTOR_TOKEN', flag, selected['route_reason'], selected.get('resolved_params'))
    if flag == 'true':
        from app.core.workflow.handlers.actor_selection import handle_actor_selection
        from app.core.actor_conversation import ActorConversationOperation
        ctx = workflow._make_ctx('opción 2', chat_id='ind-actor', actor_candidate_selection={
            'operation': ActorConversationOperation.SELECT_ACTOR_CANDIDATE,
            'candidate_rank': 2, 'pending_token': token,
        })
        authority = handle_actor_selection(workflow.deps, ctx)
        assert authority['resolved_params'].get('actor_resolution_status') == 'RESOLVED'
        print('ACTOR_AUTHORITY_CONTROL', authority['route_reason'])
        assert selected['resolved_params'].get('actor_resolution_status') == 'RESOLVED'
        assert selected['resolved_params'].get('contratista') == ['70000012']
    else:
        assert not selected.get('continuation')


def test_rollback_does_not_reactivate_a_preprofile_tabular_cursor(frame_v12, monkeypatch):
    chat = Conversation('ind-rollback', client_factory=Pages)
    chat.say('contratos de obra en baranoa')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'false')
    get_settings.cache_clear()
    turn = chat.say('muestrame mas')
    print('ROLLBACK', turn.route, len(turn.result.get('rows', [])), len(turn.calls))
    assert turn.clarifies and not turn.result.get('rows')


def test_legacy_profile_migrates_or_blocks_in_the_real_route(frame_v12, monkeypatch):
    chat = Conversation('ind-migration')
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'false')
    get_settings.cache_clear()
    chat.say(f'analiza contratos de {BUYER} en agosto')
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'true')
    get_settings.cache_clear()
    turn = chat.say('proveedores')
    print('MIGRATION', turn.route, chat.profile_state(), turn.sql)
    assert not turn.calls or chat.profile_state().confirmed_frame is not None, 'B ON executed inherited legacy profile without migrating it'


@settings(max_examples=15, derandomize=True, deadline=None)
@given(st.text(alphabet='abcdef', min_size=1, max_size=12))
def test_reference_free_seed_never_acquires_tabular_authority(result_id):
    contract = ContinuationContract(1, 'turn', result_id, ResultKind.TABULAR_SEARCH, ContinuationOwner.TABULAR,
                                    tabular_groups={'contratos': TabularGroup('contratos', 'cursor', 'fp', 'exact_order', 'gen')})
    decision = classify(user_query='aplicar', seed_resolved_params={'objeto': ['obra']},
                        envelope_status=EnvelopeStatus.OK, contract=contract, conversation_revision=1,
                        profile_owns_text=False, profile_has_options=False)
    assert decision.route is AdmissionRoute.CLARIFY


@pytest.mark.parametrize('pending', [False, True])
def test_radar_run_preserves_owner_and_reaches_only_synthetic_effect(frame_v12, monkeypatch, pending):
    from types import SimpleNamespace
    from app.core.workflow.handlers import radar

    chat = Conversation('ind-radar-run')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    if pending:
        chat.say(f'analiza contratos publicados de {BUYER} en agosto')
    flow, _ = chat.workflow()
    effects = []
    synthetic = SimpleNamespace(id='synthetic-radar', name='Sintetico')

    class Store:
        def get_radar_by_name(self, owner, name):
            effects.append(('lookup', owner, name))
            return synthetic

    flow.radar_store = Store()

    def run(deps, store, selected):
        effects.append(('run', selected.id))
        return SimpleNamespace(status='ok', total=0, digest={'text': 'Synthetic digest'}, outcome='completed')

    monkeypatch.setattr(radar, 'run_radar', run)
    result = flow.run_query('ejecutar radar Sintetico', chat_id=chat.chat_id,
                            user_identity={'channel_user_key': 'synthetic-owner'})
    print('RADAR_RUN', result['route_reason'], effects)
    assert effects == [('lookup', 'synthetic-owner', 'Sintetico'), ('run', 'synthetic-radar')]


def test_radar_save_cannot_export_an_older_tabular_search_over_profile(frame_v12, monkeypatch):
    from app.core.workflow.handlers import radar

    chat = Conversation('ind-radar-save')
    chat.say('contratos de obra en baranoa')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    effects = []

    def save(store, **kwargs):
        effects.append(kwargs)
        return 'synthetic-only'

    monkeypatch.setattr(radar, 'save_radar_from_params', save)
    result = chat.say('guardar esta búsqueda como Radar Sintetico')
    print('RADAR_SAVE', result.route, effects)
    assert not effects, 'Profile exported a previous tabular universe without an explicit referent'


@pytest.mark.parametrize('query,owner', [('mis radares', 'RADAR'), ('contratos del proveedor CC 70000011', 'ACTOR'), ('CO1.PCCNTR.12345', 'NONE')])
def test_delegated_results_publish_their_actual_owner(frame_v12, query, owner):
    chat = Conversation('ind-owner')
    turn = chat.say(query)
    contract = chat.contract()
    print('DELEGATED_OWNER', query, turn.route, contract.to_dict())
    assert contract.owner.value == owner
    assert not contract.can_paginate


def test_real_sidecar_expiry_does_not_silently_repeat_a_search(frame_v12):
    chat = Conversation('ind-expiry', client_factory=Pages)
    chat.say('contratos de obra en baranoa')
    path = ConversationStore()._pagination_path_for(chat.chat_id)
    raw = json.loads(path.read_text())

    def expire(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key == 'expires_at_utc':
                    value[key] = '2000-01-01T00:00:00+00:00'
                else:
                    expire(item)
        elif isinstance(value, list):
            for item in value:
                expire(item)

    expire(raw)
    path.write_text(json.dumps(raw))
    turn = chat.say('muestrame mas')
    print('REAL_EXPIRY', turn.route, len(turn.calls), len(turn.result.get('rows', [])))
    assert not turn.calls and turn.clarifies, 'Expired cursor refetched a search without requesting repetition'


def test_bare_more_uses_the_same_tabular_cursor_as_the_full_phrase(frame_v12):
    chat = Conversation('ind-bare-more', client_factory=Pages)
    first = chat.say('contratos de obra en baranoa')
    more = chat.say('más')
    print('BARE_MORE', more.route, len(more.calls), len(more.result.get('rows', [])))
    assert more.route == 'pagination_more'
    assert more.result['rows'] != first.result['rows']
