"""Bound the two B/B.1 hunks before authorizing their parity digests."""
import ast
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.core.actor_candidate_ui import actor_candidate_options
from entity_profile_frame_b_harness import Conversation, frame_v12  # noqa: F401
import test_pagination_buffer as legacy

APP = Path(__file__).parents[1] / 'streamlit_app.py'


def ui_code():
    """Execute the actual helpers/chip block without launching Streamlit."""
    tree = ast.parse(APP.read_text())
    names = {
        '_clean_resolved',
        '_continuation_ref',
        '_profile_action_ref',
        '_assistant_history_item',
    }
    functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    blocks = [node for node in tree.body if isinstance(node, ast.If)
              and any(isinstance(part, ast.Attribute) and part.attr == '_pending_refinement'
                      for part in ast.walk(node.test))]
    assert len(functions) == 4 and len(blocks) == 1
    namespace = {'actor_candidate_options': actor_candidate_options}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(APP), 'exec'), namespace)
    return namespace, compile(ast.Module(body=blocks, type_ignores=[]), str(APP), 'exec')


def chip(namespace, block, history):
    session = SimpleNamespace(history=history, _pending_refinement={
        'category': 'monto', 'label': 'Más de 1M', 'value': 1000000})
    namespace.update(st=SimpleNamespace(session_state=session), seed_resolved_params=None)
    exec(block, namespace)
    assert not hasattr(session, '_pending_refinement')
    return namespace['incoming_query'], namespace['seed_resolved_params']


@pytest.mark.parametrize('payload,expected', [
    ({}, None), ({'result_id': 'r'}, None), ({'conversation_revision': 2}, None),
    ({'result_id': 'r', 'conversation_revision': 2}, {'result_id': 'r', 'conversation_revision': 2}),
    ({'result_id': 'r', 'conversation_revision': 2, 'chat_id': 'c'},
     {'result_id': 'r', 'conversation_revision': 2, 'chat_id': 'c'}),
])
def test_ui_reference_transports_only_published_identifiers(payload, expected):
    namespace, _ = ui_code()
    result = {**payload, 'resolved_params': {'objeto': ['obra'], 'secret_metadata': 'not a reference'}}
    item = namespace['_assistant_history_item'](result)
    revived = json.loads(json.dumps(item))
    assert revived['continuation_ref'] == expected
    assert result['resolved_params'] == revived['debug']['resolved_params']


@pytest.mark.parametrize('reference', ['valid', 'absent', 'incomplete', 'old', 'foreign_chat'])
def test_actual_ui_chip_reference_is_consumed_on_real_reload(frame_v12, reference):
    namespace, block = ui_code()
    chat = Conversation('guard-ui-chip')
    first = chat.say('contratos de obra en baranoa')
    second = chat.say('contratos de alimentacion en bogota')
    history = [namespace['_assistant_history_item'](turn.result) for turn in (first, second)]
    current = history[-1]
    if reference == 'absent':
        current.pop('continuation_ref')
    elif reference == 'incomplete':
        current['continuation_ref'].pop('conversation_revision')
    elif reference == 'old':
        current['continuation_ref'] = history[0]['continuation_ref']
    elif reference == 'foreign_chat':
        current['continuation_ref']['chat_id'] = 'another-chat'
    before = deepcopy(history)
    query, seed = chip(namespace, block, history)
    assert history == before
    expected = {**before[-1]['debug']['resolved_params'], 'valor_min': 1000000}
    if current.get('continuation_ref'):
        expected['_continuation_ref'] = current['continuation_ref']
    assert seed == expected
    outcome = chat.say(query, seed_resolved_params=seed)
    if reference == 'valid':
        assert outcome.calls and not outcome.clarifies
        assert outcome.result['resolved_params']['valor_min'] == 1000000
        assert outcome.result['resolved_params']['objeto'] == second.result['resolved_params']['objeto']
    else:
        assert outcome.clarifies and not outcome.calls


def test_ui_empty_latest_context_does_not_reuse_an_older_search():
    namespace, block = ui_code()
    history = [{'debug': {'resolved_params': {'objeto': ['obra']}}, 'continuation_ref': {'result_id': 'old'}},
               {'debug': {'resolved_params': {}}, 'continuation_ref': None}]
    query, seed = chip(namespace, block, history)
    assert query is None and seed is None


@pytest.mark.parametrize('control', [
    legacy.test_exclusions_and_source_ties_keep_exact_concatenated_prefix,
    legacy.test_pagination_keeps_source_without_post_query_ecosystem,
    legacy.test_buffer_survives_workflow_restart_without_refetch,
])
def test_validated_referent_keeps_legacy_universe_and_order(frame_v12, tmp_path, monkeypatch, control):
    """Reuse the exact established assertion bodies with admission enabled."""
    control(tmp_path, monkeypatch)
