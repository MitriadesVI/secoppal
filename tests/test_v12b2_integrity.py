"""Structural buyer authority and cardinality, without resolving on reload."""
from dataclasses import replace
import json

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_profile.execution_record import build_execution_record
from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.frame_compiler import compile_profile
from app.core.entity_profile.frame_response import build_profile_sections, render_frame_profile
from app.core.entity_profile.frame_runner import execute_plan
from app.core.entity_profile.record_validation import validate_record
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.continuation import ConfirmedProfileRef
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


@pytest.mark.parametrize('query', ['analiza contratos de alcaldia de baranoa en agosto',
                                  'perfil de la alcaldía mayor de Bogotá en 2026'])
@pytest.mark.parametrize('field', ['display_name', 'authority', 'authority_version', 'canonical_buyers', 'group_id'])
def test_full_buyer_scope_roundtrip_and_individual_damage(frame_v12, query, field):
    chat = Conversation('b2-buyers')
    first = chat.say(query)
    assert first.result['continuation_published'] and first.calls
    frame = chat.profile_state().confirmed_frame
    builder = SoQLBuilder()
    plan = compile_profile(frame, soql_builder=builder).plan
    evidence = execute_plan(plan, WorkflowClient())
    clone = EntityProfileFrame.from_dict(json.loads(json.dumps(frame.to_dict())))
    assert clone is not frame and clone == frame
    assert build_profile_sections(clone, plan, evidence, builder)
    assert render_frame_profile(clone, plan, evidence, builder)
    record = build_execution_record(plan, evidence, clone)
    ref = ConfirmedProfileRef((frame.frame_id, frame.revision), plan.plan_id, record.record_id)
    validate_record(clone, ref, record)
    assert record.authorized_plan['buyer_scope'] == clone.buyer_scope.to_dict()
    assert all(buyer in ' '.join(job.sql for job in plan.jobs)
               for buyer in clone.buyer_scope.canonical_buyers)
    value = ('foreign',) if field == 'canonical_buyers' else 'foreign'
    wrong = replace(clone, buyer_scope=replace(clone.buyer_scope, **{field: value}))
    for consumer in (build_profile_sections, render_frame_profile):
        with pytest.raises(ValueError, match='buyer'):
            consumer(wrong, plan, evidence, builder)
    assert chat.say('recientes').calls


@settings(max_examples=24, derandomize=True, deadline=None)
@given(st.lists(st.booleans(), min_size=1, max_size=6))
def test_duplicate_observation_invalid_even_if_not_attempted(attempts):
    from types import SimpleNamespace
    from app.core.entity_profile.record_validation import snapshot, snapshot_digest
    from app.core.entity_profile.execution_record import RecordedCall
    from app.core.entity_profile.frame import BuyerScope, BuyerScopeKind, SourcePredicates, SourceSelection
    from app.core.entity_profile.frame_values import SourceKind, SourceOrigin
    source = SourceKind.CONTRACTS
    frame = EntityProfileFrame('f', 1, BuyerScope(BuyerScopeKind.BUYER_UNIT, 'buyer', ('buyer',), 'test', '1'),
                               'values', SourceSelection(frozenset({source}), SourceOrigin.EXPLICIT),
                               {source: SourcePredicates(source)})
    plan = compile_profile(frame, soql_builder=SoQLBuilder()).plan
    authorized = snapshot(plan)
    template = authorized['jobs'][0]
    authorized['jobs'] = [dict(template, job_id=str(i)) for i in range(len(attempts))]
    fingerprint = plan.source_plans[plan.jobs[0].source].fingerprint
    calls = tuple(RecordedCall(str(i), template['call_id'], template['dataset_id'], fingerprint,
                              template['sql'], 'SUCCESS' if attempted else 'BUDGET_EXHAUSTED', attempted)
                  for i, attempted in enumerate(attempts))
    record = SimpleNamespace(catalog_version=plan.catalog_version, frame_ref=plan.frame_ref, focus=plan.focus,
        plan_id=plan.plan_id, record_id='e', authorized_plan=authorized, plan_digest=snapshot_digest(authorized),
        confirmed_frame=frame.to_dict(), expected_sections=plan.expected_sections,
        not_requested_sections=plan.not_requested_sections, calls=calls)
    ref = SimpleNamespace(frame_ref=plan.frame_ref, plan_ref=plan.plan_id, evidence_ref='e')
    validate_record(frame, ref, record)
    record.calls = (*calls, calls[-1])
    with pytest.raises(ValueError, match='membership'):
        validate_record(frame, ref, record)


def test_buyer_snapshot_rejected_even_with_recomputed_digest(frame_v12):
    from app.core.conversation_store import ConversationStore
    from app.core.entity_profile.record_validation import snapshot_digest

    chat = Conversation('b2-buyer-snapshot')
    chat.say('analiza contratos de alcaldia de baranoa en agosto')
    path = ConversationStore()._envelope_path_for(chat.chat_id)
    lines = path.read_text().splitlines()
    raw = json.loads(lines[-1])
    record = raw['execution_record']
    record['authorized_plan']['buyer_scope']['authority_version'] = 'foreign'
    record['plan_digest'] = snapshot_digest(record['authorized_plan'])
    path.write_text('\n'.join([*lines[:-1], json.dumps(raw)]) + '\n')
    later = chat.say('proveedores')
    assert later.clarifies and not later.calls


def test_real_unattempted_execution_is_valid_on_reload(frame_v12, monkeypatch):
    from app.core.entity_profile import frame_runner

    chat = Conversation('b2-unattempted')
    with monkeypatch.context() as patch:
        patch.setattr(frame_runner, '_fetch_budget', lambda: None)
        result = chat.say('analiza contratos de alcaldia de baranoa en agosto')
    assert result.result['continuation_published'] and not result.calls
    record = chat.execution_record()
    assert len(record['calls']) == 4 and not any(call['attempted'] for call in record['calls'])
    assert all(call['status'] == 'BUDGET_EXHAUSTED' for call in record['calls'])
    later = chat.say('proveedores')
    assert later.calls and not later.clarifies
