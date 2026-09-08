"""Independent positive execution controls and a mismatched Frame boundary."""
from dataclasses import replace
from datetime import date

import pytest

from app.core.entity_profile.frame_compiler import compile_profile
from app.core.entity_profile.frame_response import render_frame_profile
from app.core.entity_profile.frame_runner import execute_plan
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_b_harness import BUYER, Conversation, frame_v12  # noqa: F401
from test_entity_profile_workflow import WorkflowClient


@pytest.mark.parametrize('focus,budget', [('analiza', 6), ('proveedores', 2), ('montos', 1),
                                         ('modalidades', 1), ('evolucion', 2), ('recientes', 2)])
def test_published_execution_matches_sent_sql_and_its_confirmed_frame(frame_v12, focus, budget):
    chat = Conversation('ind-positive')
    turn = chat.say(f'analiza {BUYER} en agosto')
    if focus != 'analiza':
        turn = chat.say(focus)
    assert turn.calls and not turn.clarifies
    record = chat.execution_record()
    frame = chat.profile_state().confirmed_frame
    assert record['frame_ref'] == [frame.frame_id, frame.revision]
    contract = chat.contract().confirmed_profile
    assert contract.plan_ref == record['plan_id']
    assert contract.evidence_ref == record['record_id']
    assert sorted((c['dataset_id'], c['sql']) for c in record['calls']) == sorted((d, s) for _, d, s in turn.calls)
    assert len(turn.calls) <= budget
    assert all('2026-08-01' in sql for sql in turn.sql)


@pytest.mark.parametrize('foreign_buyer', [False, True])
def test_renderer_rejects_a_frame_other_than_the_authorized_revision(frame_v12, foreign_buyer):
    chat = Conversation('ind-render-frame')
    chat.say(f'analiza contratos de {BUYER} en agosto')
    frame = chat.profile_state().confirmed_frame
    builder = SoQLBuilder()
    plan = compile_profile(frame, soql_builder=builder).plan
    evidence = execute_plan(plan, WorkflowClient())
    wrong = replace(frame, revision=frame.revision + 1)
    if foreign_buyer:
        other = Conversation('ind-render-other')
        other.say('analiza contratos de alcaldia de galapa en julio')
        wrong = other.profile_state().confirmed_frame
    with pytest.raises(ValueError):
        text = render_frame_profile(wrong, plan, evidence, builder)
        print('WRONG_FRAME', plan.frame_ref, (wrong.frame_id, wrong.revision), text)


def test_relative_clock_remains_resolved_after_reload_in_another_year(frame_v12):
    chat = Conversation('ind-clock')
    flow, _ = chat.workflow()
    flow.run_query(f'analiza contratos de {BUYER} en agosto', chat_id=chat.chat_id, today=date(2024, 9, 7))
    flow, client = chat.workflow()
    result = flow.run_query('proveedores', chat_id=chat.chat_id, today=date(2026, 9, 7))
    assert result['route_reason'] == 'entity_profile_suppliers'
    assert all('2024-08-01' in sql and '2026-08-01' not in sql for _, _, sql in client.calls)


def test_clear_preserves_a_legitimate_topic_exclusion(frame_v12):
    chat = Conversation('ind-clear')
    first = chat.say(f'analiza contratos de {BUYER} en agosto excepto interventoria')
    assert first.calls
    cleared = chat.say('sin filtro de fecha')
    later = chat.say('proveedores')
    for turn in (cleared, later):
        assert turn.calls
        assert all('interventoria' in sql.lower() and 'NOT' in sql for sql in turn.sql)
        assert all('2026-08-01' not in sql and '%filtro%' not in sql for sql in turn.sql)
