"""Dogfood shapes with synthetic identities and deterministic source EMPTY."""
import socket

import pytest

from test_actor_conversation import _row, _workflow
from test_actor_profile_scope import RecordingClient, CASES


@pytest.fixture
def workflow(tmp_path, monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError('dogfood regression attempted network')
    monkeypatch.setattr(socket.socket, 'connect', blocked)
    workflow = _workflow(tmp_path, [
        _row('CC', '10000001', 'JOSE DAVID CANTILLO ARCON', 8),
        _row('CC', '10000002', 'JOSE DAVID CANTILLO SOLANO', 3),
        _row('CC', '10000003', 'FRANKLIN ALBERTO PEREZ', 8),
        _row('NIT', '900111222', 'FUNDACARIBE', 9),
        _row('CC', '10000004', 'TRANSPORTE', 1),
    ])
    workflow.secop_client = RecordingClient()
    return workflow


def test_jose_cantillo_selection_preserves_identity_buyer_and_qualifies_zero(workflow):
    workflow.run_query('reset', chat_id='scope')
    pending = workflow.run_query('contratos del proveedor jose cantillo con alcaldia de barranquilla', chat_id='scope')
    params = pending['resolved_params']
    assert params['actor_resolution_status'] == 'CANDIDATES'
    chosen = params['actor_pending']['candidates'][0]
    workflow.secop_client.calls.clear()
    selected = workflow.run_query('1', chat_id='scope')
    assert selected['route_reason'] == 'actor_profile'
    scope = selected['resolved_params']['actor_scope']
    assert (scope['document_type'], scope['base_number']) == (chosen['document_type'], chosen['base_number'])
    buyer = CASES['buyer']['params']['entidad_resolved']
    assert selected['resolved_params']['entidad_resolved'] == buyer
    assert len(workflow.secop_client.calls) == 8
    assert all(buyer in c['soql'] for c in workflow.secop_client.calls)
    assert buyer in selected['response']
    assert '- 0 contratos observados dentro de estos filtros' in selected['response']


def test_fundacaribe_year_retains_actor_and_exposes_period(workflow):
    first = workflow.run_query('contratos del proveedor fundacaribe', chat_id='year')
    workflow.secop_client.calls.clear()
    result = workflow.run_query('ahora muestrame 2026', chat_id='year')
    assert result['resolved_params']['actor_scope'] == first['resolved_params']['actor_scope']
    assert result['route_reason'] == 'actor_profile'
    assert 'Periodo: 2026' in result['response']
    assert len(workflow.secop_client.calls) == 8
    assert all('2026-01-01' in c['soql'] and '2026-12-31' in c['soql'] for c in workflow.secop_client.calls)


def test_arcon_full_name_already_passing(workflow):
    workflow.run_query('reset', chat_id='arcon')
    result = workflow.run_query('contratos de jose david cantillo arcon', chat_id='arcon')
    assert result['route_reason'] == 'actor_profile'
    assert result['resolved_params']['actor_resolution_method'] == 'exact_unique_alias'
    assert result['resolved_params']['actor_scope']['base_number'] == '10000001'


def test_franklin_to_fundacaribe_pending_already_fixed(workflow):
    pending = workflow.run_query('contratos del proveedor franklin', chat_id='pending')
    assert pending['resolved_params']['actor_resolution_status'] == 'CANDIDATES'
    result = workflow.run_query('contratos del proveedor fundacaribe', chat_id='pending')
    params = result['resolved_params']
    assert not params.get('actor_pending')
    assert params['actor_conversation_operation'] == 'SET_ACTOR'
    assert params['actor_resolution_status'] == 'RESOLVED'
    assert params['actor_scope']['base_number'] == '900111222'
    assert result['route_reason'] == 'actor_profile'


@pytest.mark.parametrize('topic', ['mantenimiento', 'alimentacion', 'adulto mayor', 'software', 'vigilancia', 'aseo'])
def test_thematic_baseline_stays_thematic(workflow, topic):
    result = workflow.run_query(f'contratos de {topic}')
    assert not result['resolved_params'].get('actor_pending')
    assert result['resolved_params'].get('objeto')
    assert result.get('route_reason') != 'actor_profile'


def test_transporte_exact_collision_is_ambiguous_not_thematic_only(workflow):
    result = workflow.run_query('contratos de transporte')
    assert result['query_plan']['reason'] == 'actor_or_topic_ambiguous'
    assert not workflow.secop_client.calls
