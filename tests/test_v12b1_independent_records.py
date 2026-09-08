"""Independent corruption and valid-publication controls on real reload."""
from dataclasses import replace
import json

import pytest
from hypothesis import given, settings, strategies as st

from app.core.conversation_store import ConversationStore
from app.core.entity_profile.frame_compiler import compile_profile
from app.core.entity_profile.frame_response import build_profile_sections, render_frame_profile
from app.core.entity_profile.frame_runner import execute_plan
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_b_harness import BUYER, Conversation, FailingClient, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


@pytest.mark.parametrize('consumer', ['assembler', 'renderer'])
def test_foreign_buyer_with_reused_ids_is_rejected(frame_v12, consumer):
    chat, other = Conversation('b1-independent-render'), Conversation('b1-independent-foreign')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    other.say('analiza contratos de alcaldia de galapa en agosto')
    frame = chat.profile_state().confirmed_frame
    builder = SoQLBuilder()
    plan = compile_profile(frame, soql_builder=builder).plan
    client = WorkflowClient()
    evidence = execute_plan(plan, client)
    valid = render_frame_profile(frame, plan, evidence, builder)
    assert 'BARANOA' in valid and '4 contratos' in valid and len(client.calls) == 4
    wrong = replace(frame, buyer_scope=other.profile_state().confirmed_frame.buyer_scope)
    assert wrong.source_predicates == frame.source_predicates
    assert wrong.buyer_scope.canonical_buyers != frame.buyer_scope.canonical_buyers
    function = build_profile_sections if consumer == 'assembler' else render_frame_profile
    with pytest.raises(ValueError):
        result = function(wrong, plan, evidence, builder)
        print('FOREIGN_BUYER', consumer, result, 'SQL', [sql for _, _, sql in client.calls])


@pytest.mark.parametrize('damage', [
    'chat', 'revision', 'turn_id', 'result_id', 'plan_ref', 'evidence_ref', 'frame_ref',
    'schema', 'catalog', 'snapshot', 'job_removed', 'job_added', 'job_sql',
    'call_removed', 'call_added', 'call_duplicate', 'call_sql', 'call_dataset',
    'confirmed_buyer', 'pending_base',
])
def test_individual_record_damage_rejects_before_io(frame_v12, damage):
    chat = Conversation('b1-independent-record')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    if damage == 'pending_base':
        chat.say(f'analiza contratos publicados de {BUYER} en agosto')
    path = ConversationStore()._envelope_path_for(chat.chat_id)
    lines = path.read_text().splitlines()
    raw = json.loads(lines[-1])
    record = raw['execution_record']
    if damage in ('chat', 'revision', 'turn_id', 'result_id'):
        key = 'chat_id' if damage == 'chat' else damage
        raw[key] = 888 if key == 'revision' else 'foreign'
    elif damage in ('plan_ref', 'evidence_ref', 'frame_ref'):
        raw['continuation']['confirmed_profile'][damage] = ['foreign', 1] if damage == 'frame_ref' else 'foreign'
    elif damage == 'schema':
        record['schema_version'] = 99
    elif damage == 'catalog':
        record['catalog_version'] = 'unknown'
    elif damage == 'snapshot':
        record['authorized_plan']['focus'] = 'values'
    elif damage == 'job_removed':
        record['authorized_plan']['jobs'].pop()
    elif damage == 'job_added':
        record['authorized_plan']['jobs'].append(dict(record['authorized_plan']['jobs'][0], job_id='foreign'))
    elif damage == 'job_sql':
        record['authorized_plan']['jobs'][0]['sql'] = 'SELECT count(*) WHERE 1=1'
    elif damage == 'call_removed':
        record['calls'].pop()
    elif damage == 'call_added':
        record['calls'].append(dict(record['calls'][0], job_id='foreign'))
    elif damage == 'call_duplicate':
        record['calls'].append(dict(record['calls'][0]))
    elif damage == 'call_sql':
        record['calls'][0]['sql'] = 'SELECT count(*) WHERE 1=1'
    elif damage == 'call_dataset':
        record['calls'][0]['dataset_id'] = 'p6dx-8zbt'
    elif damage == 'confirmed_buyer':
        record['confirmed_frame']['buyer_scope']['canonical_buyers'] = ['foreign']
    else:
        raw['profile_state']['pending']['base_frame_ref'] = ['foreign', 1]
    path.write_text('\n'.join([*lines[:-1], json.dumps(raw)]) + '\n')
    after = chat.say('proveedores')
    print('RECORD_DAMAGE', damage, after.route, len(after.calls))
    assert after.clarifies and not after.calls, 'An incompatible execution record authorized a new query'


@pytest.mark.parametrize('state', ['executed', 'partial', 'total', 'pending_initial', 'pending_keeps_execution'])
def test_valid_publications_remain_reloadable(frame_v12, state):
    chat = Conversation('b1-independent-valid')
    client = FailingClient() if state == 'total' else WorkflowClient(fail_supplier_value=state == 'partial')
    query = (f'analiza contratos publicados de {BUYER} en agosto' if state == 'pending_initial'
             else f'analiza contratos de {BUYER} en agosto')
    first = chat.say(query, client=client)
    assert first.result['continuation_published']
    record = chat.execution_record()
    if state == 'pending_initial':
        assert record is None and not first.calls and chat.profile_state().pending
        assert chat.say(f'analiza contratos firmados de {BUYER} en agosto').calls
        return
    assert record and first.calls
    if state == 'pending_keeps_execution':
        clarification = chat.say(f'analiza contratos publicados de {BUYER} en agosto')
        assert clarification.clarifies and not clarification.calls
        assert chat.execution_record() == record
        chat.say('cancela ese cambio')
    if state in ('partial', 'total'):
        assert any(call['status'] != 'SUCCESS' for call in record['calls'])
    later = chat.say('proveedores')
    assert later.route == 'entity_profile_suppliers' and len(later.calls) == 2


@settings(max_examples=30, derandomize=True, deadline=None)
@given(st.lists(st.text(alphabet='abcd', min_size=1, max_size=6), min_size=1, max_size=6, unique=True))
def test_record_call_membership_preserves_multiplicity(job_ids):
    """The invariant is about event multiplicity, not set equality alone."""
    from types import SimpleNamespace
    from app.core.entity_profile.record_validation import snapshot_digest, validate_record
    from app.core.entity_profile.frame import BuyerScope, BuyerScopeKind, EntityProfileFrame, SourcePredicates, SourceSelection
    from app.core.entity_profile.frame_values import SourceKind, SourceOrigin
    from app.core.entity_profile.record_validation import snapshot
    from app.core.entity_profile.execution_record import RecordedCall
    source = SourceKind.CONTRACTS
    predicates = SourcePredicates(source)
    frame = EntityProfileFrame('f', 1, BuyerScope(BuyerScopeKind.BUYER_UNIT, 'buyer', ('buyer',), 'test', '1'),
                               'values', SourceSelection(frozenset({source}), SourceOrigin.EXPLICIT), {source: predicates})
    plan = compile_profile(frame, soql_builder=SoQLBuilder()).plan
    assert plan is not None
    authorized = snapshot(plan)
    template = authorized['jobs'][0]
    authorized['jobs'] = [dict(template, job_id=job_id) for job_id in job_ids]
    calls = tuple(RecordedCall(job_id, template['call_id'], template['dataset_id'],
                              predicates.fingerprint(), template['sql'], 'SUCCESS', True) for job_id in job_ids)
    record = SimpleNamespace(catalog_version=plan.catalog_version, frame_ref=('f', 1), focus='values',
        plan_id=plan.plan_id, record_id='e', authorized_plan=authorized, plan_digest=snapshot_digest(authorized),
        confirmed_frame=frame.to_dict(), expected_sections=plan.expected_sections,
        not_requested_sections=plan.not_requested_sections, calls=calls)
    reference = SimpleNamespace(frame_ref=('f', 1), plan_ref=plan.plan_id, evidence_ref='e')
    validate_record(frame, reference, record)
    record.calls = (*calls, calls[0])
    with pytest.raises(ValueError):
        validate_record(frame, reference, record)
