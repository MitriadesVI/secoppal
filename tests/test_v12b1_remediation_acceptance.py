"""B.1 acceptance: real workflow/reload, only external effects are synthetic."""
import json
from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
import threading

import pytest
from hypothesis import given, settings, strategies as st

from app.config import get_settings
from app.core.conversation_store import ConversationStore
from app.core.entity_profile.frame_response import build_profile_sections
from app.core.entity_profile.frame_compiler import compile_profile
from app.core.entity_profile.frame_runner import execute_plan
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_b_harness import BUYER, TODAY, Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient
from test_v12b_independent_boundaries import Pages


def test_serial_execution_reserves_the_next_revision(frame_v12):
    """Port A: pause inside execution, never rendezvous after BOTH reservations."""
    entered, release, second_started = (threading.Event() for _ in range(3))
    chat = Conversation('b1-serial')

    class Paused(WorkflowClient):
        def aggregate(self, dataset, sql):
            if not entered.is_set():
                entered.set()
                assert release.wait(5)
            return super().aggregate(dataset, sql)

    first, _ = chat.workflow(Paused())
    second, second_client = chat.workflow()

    def run_second():
        second_started.set()
        return second.run_query('proveedores', chat_id=chat.chat_id, today=TODAY)

    with ThreadPoolExecutor(2) as pool:
        a = pool.submit(first.run_query, f'analiza contratos de {BUYER} en agosto', chat_id=chat.chat_id, today=TODAY)
        assert entered.wait(5)
        b = pool.submit(run_second)
        assert second_started.wait(5)
        release.set()
        results = a.result(10), b.result(10)
    assert [r['conversation_revision'] for r in results] == [1, 2]
    assert results[1]['route_reason'] == 'entity_profile_suppliers'
    assert len(second_client.calls) == 2
    assert all('2026-08-01' in sql for _, _, sql in second_client.calls)
    assert chat.say('montos').calls


def test_other_chat_executes_while_first_chat_is_paused(frame_v12):
    entered, release = threading.Event(), threading.Event()

    class Paused(WorkflowClient):
        def aggregate(self, dataset, sql):
            entered.set()
            assert release.wait(5)
            return super().aggregate(dataset, sql)

    a, b = Conversation('b1-a'), Conversation('b1-b')
    flow, _ = a.workflow(Paused())
    with ThreadPoolExecutor(1) as pool:
        first = pool.submit(flow.run_query, f'analiza contratos de {BUYER}', chat_id=a.chat_id)
        try:
            assert entered.wait(5)
            assert b.say(f'analiza contratos de {BUYER}').result['continuation_published']
        finally:
            release.set()
        assert first.result(10)['continuation_published']


def test_radar_current_table_exports_exact_filters_and_owner(frame_v12, monkeypatch):
    from app.core.workflow.handlers import radar
    chat = Conversation('b1-save')
    first = chat.say('contratos de obra en baranoa')
    params = ConversationStore().get_history(chat.chat_id, 1)[0].parsed_params
    effects = []
    monkeypatch.setattr(radar, 'save_radar_from_params', lambda store, **kw: effects.append(kw) or 'synthetic-only')
    saved = chat.say('guardar esta búsqueda como Radar Sintetico', user_identity={'channel_user_key': 'b1-owner'})
    assert saved.route == 'radar_saved'
    assert effects[0]['owner'] == 'b1-owner'
    assert effects[0]['params'] == params
    assert first.result['continuation']['export_ref']
    assert chat.contract().owner.value == 'RADAR'


@pytest.mark.parametrize('damage', ['missing', 'period', 'event'])
def test_activation_blocks_incomplete_legacy(frame_v12, monkeypatch, damage):
    chat = Conversation('b1-migration-bad')
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'false')
    get_settings.cache_clear()
    chat.say(f'analiza contratos de {BUYER} en agosto')
    path = ConversationStore()._path_for(chat.chat_id)
    raw = json.loads(path.read_text().splitlines()[-1])
    params = raw['parsed_params']
    if damage == 'missing':
        params.pop('_entity_profile_request')
    elif damage == 'period':
        params['fecha_desde'] = '2026-07-01'
    else:
        params['_entity_profile_request']['unsupported_event'] = 'contratos terminados'
    path.write_text(json.dumps(raw) + '\n')
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'true')
    get_settings.cache_clear()
    turn = chat.say('proveedores')
    assert turn.clarifies and not turn.calls


def test_activation_migrates_equivalent_legacy_and_roundtrips(frame_v12, monkeypatch):
    chat = Conversation('b1-migration-good')
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'false')
    get_settings.cache_clear()
    chat.say(f'analiza contratos de {BUYER} en agosto')
    monkeypatch.setenv('SECOPPAL_ENTITY_PROFILE_FRAME_ENABLED', 'true')
    get_settings.cache_clear()
    turn = chat.say('proveedores')
    assert turn.calls and not turn.clarifies
    frame = chat.profile_state().confirmed_frame
    assert ('legacy_migration', '1') in frame.provenance.authorities
    assert chat.say('montos').calls


@pytest.mark.parametrize('field', ['focus', 'expected_sections', 'turn_id', 'source_predicates', 'sql'])
def test_current_record_damage_blocks_before_calls(frame_v12, field):
    chat = Conversation('b1-record')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    path = ConversationStore()._envelope_path_for(chat.chat_id)
    raw = json.loads(path.read_text().splitlines()[-1])
    record = raw['execution_record']
    if field == 'turn_id':
        raw['continuation']['visible_turn_id'] = 'foreign'
    elif field == 'source_predicates':
        record['authorized_plan']['source_plans']['CONTRACTS']['fingerprint'] = 'foreign'
    elif field == 'sql':
        record['calls'][0]['sql'] = 'SELECT count(*) WHERE 1=1'
    else:
        record[field] = 'values' if field == 'focus' else ['values']
    path.write_text(json.dumps(raw) + '\n')
    turn = chat.say('proveedores')
    assert turn.clarifies and not turn.calls


@pytest.mark.parametrize('pending', [False, True])
def test_clarification_retains_execution_without_new_calls(frame_v12, pending):
    chat = Conversation('b1-kept-record')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    before = chat.execution_record()
    if pending:
        assert chat.say(f'analiza contratos publicados de {BUYER} en agosto').clarifies
    assert not chat.say('1').calls
    assert chat.execution_record() == before
    if pending:
        assert not chat.say('cancela ese cambio').calls
    assert chat.say('proveedores').calls


def test_assembler_refuses_foreign_revision(frame_v12):
    chat = Conversation('b1-assembler')
    chat.say(f'analiza contratos de {BUYER}')
    frame = chat.profile_state().confirmed_frame
    builder = SoQLBuilder()
    plan = compile_profile(frame, soql_builder=builder).plan
    evidence = execute_plan(plan, WorkflowClient())
    assert build_profile_sections(frame, plan, evidence, builder)
    with pytest.raises(ValueError):
        build_profile_sections(replace(frame, revision=frame.revision + 1), plan, evidence, builder)


@pytest.mark.parametrize('phrase', ['más', 'muéstrame más'])
def test_exhausted_cursor_never_refetches(frame_v12, phrase):
    chat = Conversation('b1-exhausted')
    chat.say('contratos de obra en baranoa')
    more = chat.say(phrase)
    assert not more.calls
    assert more.route == 'pagination_exhausted'


def test_dual_selection_uses_only_offered_group_after_clarification(frame_v12):
    chat = Conversation('b1-dual', client_factory=Pages)
    chat.say('procesos y contratos de obra en baranoa')
    more = chat.say('más')
    assert more.clarifies and not more.calls
    selected = chat.say('más contratos')
    assert selected.route == 'pagination_more'
    assert selected.result['dataset_id'] == 'jbjy-vk9h'
    assert all(dataset == 'jbjy-vk9h' for _, dataset, _ in selected.calls)


@settings(max_examples=20, derandomize=True, deadline=None)
@given(st.sampled_from(['result_id', 'conversation_revision', 'chat_id']), st.text(alphabet='abcdef', min_size=1, max_size=12))
def test_seed_foreign_reference_property(field, value):
    from app.core.workflow.admission_rules import classify, AdmissionRoute
    from app.core.workflow.continuation import ContinuationContract, ContinuationOwner, ResultKind
    from app.core.conversation_envelope import EnvelopeStatus
    contract = ContinuationContract(1, 'turn', 'result', ResultKind.TABULAR_SEARCH, ContinuationOwner.TABULAR)
    reference = {'result_id': 'result', 'conversation_revision': 1}
    if field == 'chat_id':
        # Pure classifier does not own chat identity: omit required result ref instead.
        reference.pop('result_id')
    else:
        reference[field] = value
    decision = classify(user_query='aplicar', seed_resolved_params={'objeto': ['obra'], '_continuation_ref': reference},
                        envelope_status=EnvelopeStatus.OK, contract=contract, conversation_revision=1,
                        profile_owns_text=False, profile_has_options=False)
    assert decision.route is AdmissionRoute.CLARIFY
