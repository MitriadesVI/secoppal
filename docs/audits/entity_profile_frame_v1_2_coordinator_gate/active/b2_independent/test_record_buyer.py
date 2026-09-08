"""Fresh persisted corruptions with digest recomputed; positive real records first."""
import copy
from dataclasses import replace
import json

import pytest

from app.core.conversation_store import ConversationStore
from app.core.entity_profile.frame import EntityProfileFrame, BuyerScopeKind
from app.core.entity_profile.frame_compiler import compile_profile
from app.core.entity_profile.frame_response import build_profile_sections, render_frame_profile
from app.core.entity_profile.frame_runner import execute_plan
from app.core.entity_profile.record_validation import snapshot_digest
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


def damage_last(chat, change):
    path = ConversationStore()._envelope_path_for(chat.chat_id)
    lines = path.read_text().splitlines()
    raw = json.loads(lines[-1])
    change(raw['execution_record'])
    path.write_text('\n'.join([*lines[:-1], json.dumps(raw)]) + '\n')
    return path.read_bytes()


@pytest.mark.parametrize('damage', ['exact_copy', 'changed_same_job', 'replace_with_duplicate', 'absent', 'foreign', 'duplicate_jobs'])
def test_observations_reject_multiplicity_and_membership_on_reload(frame_v12, damage):
    chat = Conversation('audit-observations')
    assert chat.say('analiza contratos de alcaldia de baranoa en agosto').result['continuation_published']
    before = chat.execution_record()
    assert len(before['calls']) == len(before['authorized_plan']['jobs']) == 4

    def change(record):
        calls = record['calls']
        duplicate = copy.deepcopy(calls[0])
        if damage == 'exact_copy':
            calls.append(duplicate)
        elif damage == 'changed_same_job':
            duplicate.update(status='BUDGET_EXHAUSTED', attempted=False, metrics=[])
            calls.append(duplicate)
        elif damage == 'replace_with_duplicate':
            calls[-1] = duplicate
        elif damage == 'absent':
            calls.pop()
        elif damage == 'foreign':
            calls[0]['job_id'] = 'foreign-job'
        else:
            record['authorized_plan']['jobs'].append(copy.deepcopy(record['authorized_plan']['jobs'][0]))
        record['plan_digest'] = snapshot_digest(record['authorized_plan'])

    damaged = damage_last(chat, change)
    following = chat.say('proveedores')
    assert following.clarifies and not following.calls
    path = ConversationStore()._envelope_path_for(chat.chat_id)
    assert path.read_bytes().startswith(damaged), 'rejection must not silently repair stored evidence'


BUYERS = ['analiza contratos de alcaldia de baranoa en agosto', 'perfil de la alcaldía mayor de Bogotá en 2026']
FIELDS = ['kind', 'display_name', 'canonical_buyers', 'authority', 'authority_version', 'group_id', 'absent_link']


@pytest.mark.parametrize('query', BUYERS)
@pytest.mark.parametrize('field', FIELDS)
def test_each_authorized_buyer_field_rejected_after_digest_recalculation(frame_v12, query, field):
    chat = Conversation('audit-buyer-snapshot')
    assert chat.say(query).result['continuation_published']

    def change(record):
        buyer = record['authorized_plan']['buyer_scope']
        if field == 'absent_link':
            del record['authorized_plan']['buyer_scope']
        elif field == 'kind':
            buyer[field] = 'BUYER_GROUP' if buyer[field] == 'BUYER_UNIT' else 'BUYER_UNIT'
        elif field == 'canonical_buyers':
            buyer[field] = [*buyer[field], 'FOREIGN BUYER']
        else:
            buyer[field] = 'foreign'
        record['plan_digest'] = snapshot_digest(record['authorized_plan'])

    damaged = damage_last(chat, change)
    following = chat.say('proveedores')
    assert following.clarifies and not following.calls
    assert ConversationStore()._envelope_path_for(chat.chat_id).read_bytes().startswith(damaged)


def test_kind_is_authority_in_both_consumers_with_equivalent_clone_control(frame_v12):
    chat = Conversation('audit-buyer-kind')
    assert chat.say(BUYERS[1]).result['continuation_published']
    frame = chat.profile_state().confirmed_frame
    assert frame.buyer_scope.kind == BuyerScopeKind.BUYER_GROUP
    builder = SoQLBuilder()
    plan = compile_profile(frame, soql_builder=builder).plan
    client = WorkflowClient()
    evidence = execute_plan(plan, client)
    calls_before = tuple(client.calls)
    clone = EntityProfileFrame.from_dict(json.loads(json.dumps(frame.to_dict())))
    wrong = replace(clone, buyer_scope=replace(clone.buyer_scope, kind=BuyerScopeKind.BUYER_UNIT))
    for consumer in (build_profile_sections, render_frame_profile):
        assert consumer(frame, plan, evidence, builder) == consumer(clone, plan, evidence, builder)
        with pytest.raises(ValueError, match='buyer'):
            consumer(wrong, plan, evidence, builder)
    assert tuple(client.calls) == calls_before
