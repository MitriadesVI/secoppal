"""Acceptance observes SQL results and published response, not only params."""
import pytest
from tests.contract_universe_support import offline_universe, source  # noqa: F401
from tests.buyer_actor_independent_support import SEEDS, alias, index_at, workflow, say


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('query,mention,buyer,minimum', SEEDS)
def test_seeds_before_limit(tmp_path, monkeypatch, offline_universe, frame,
                            query, mention, buyer, minimum):
    flow = workflow(tmp_path, monkeypatch, index_at(tmp_path, [alias('FUNDACARIBE')]), frame)
    conn = source(flow, monkeypatch, buyer, minimum)
    try:
        result = say(flow, query)
        assert not result.get('needs_clarification'), result
        assert {row['id_contrato'] for row in result['results']} == {f'VALID-{i}' for i in range(8)}
        assert result['total_count'] == 8
        assert 'Contratos celebrados según las señales publicadas en SECOP.' in result['response']
        assert result['resolved_params']['entidad_resolved'] == buyer
        assert result['resolved_params']['valor_min'] == minimum
    finally:
        conn.close()


@pytest.mark.parametrize('text,state', [
    ('registros cancelados de Cartagena', 'Cancelado'),
    ('borradores de Cartagena', 'Borrador'),
    ('registros enviados al proveedor de Cartagena', 'enviado Proveedor'),
    ('contratos suspendidos de Cartagena', 'Suspendido'),
])
def test_explicit_records(tmp_path, monkeypatch, offline_universe, text, state):
    _, _, buyer, minimum = SEEDS[0]
    flow = workflow(tmp_path, monkeypatch, index_at(tmp_path, []), False)
    conn = source(flow, monkeypatch, buyer, minimum)
    try:
        result = say(flow, text)
        assert not result.get('needs_clarification'), result
        assert result['results'], result
        assert {r['estado_contrato'] for r in result['results']} == {state}
        assert ('Registros de la fuente' if state != 'Suspendido' else 'Contratos celebrados') in result['response']
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_actor_effective_readers(tmp_path, monkeypatch, offline_universe, frame):
    _, _, buyer, minimum = SEEDS[0]
    flow = workflow(tmp_path, monkeypatch, index_at(tmp_path, [alias('FUNDACARIBE')]), frame)
    conn = source(flow, monkeypatch, buyer, minimum)
    conn.execute("UPDATE contracts SET documento_proveedor='71000001', proveedor_adjudicado='FUNDACARIBE'")
    try:
        result = say(flow, 'qué ha contratado FUNDACARIBE')
        assert result['route_reason'] == 'actor_profile', result
        assert result['total_count'] == 10, result
        assert flow.audit_trace['flags']['actor_reader'] is True
        assert flow.audit_trace['flags']['frame_reader'] is frame
        assert 'Contratos celebrados' in result['response']
        assert all('estado_contrato' in c['soql'] for c in flow.audit_trace['sql'])
        core = next(rows[0] for rows in flow.audit_trace['source_rows'] if rows and 'total_count' in rows[0])
        assert core['total_value'] == 9 * (minimum + 100) + 1
        assert all(r['id_contrato'].startswith(('VALID', 'WRONG')) for rows in flow.audit_trace['source_rows']
                   for r in rows if 'id_contrato' in r)
    finally:
        conn.close()


def test_frame_effective_plan_and_reload(tmp_path, monkeypatch, offline_universe):
    from app.core.entity_profile.frame import EntityProfileFrame
    from app.core.entity_profile.frame_values import SourceKind
    from app.core.entity_profile.frame_turn import run_profile_turn
    from app.core.entity_profile.profile_state import ProfileConversationState
    from datetime import date
    _, _, buyer, minimum = SEEDS[0]
    index = index_at(tmp_path, [])
    flow = workflow(tmp_path, monkeypatch, index, True)
    conn = source(flow, monkeypatch, buyer, minimum)
    try:
        result = say(flow, 'analiza contratos de alcaldia de cartagena por mas de 500 millones')
        assert result['route_reason'].startswith('entity_profile'), result
        values = next(rows[0] for rows in flow.audit_trace['source_rows'] if rows and 'average_contract_value' in rows[0])
        assert values['contract_count'] == 8
        assert values['total_contract_value'] == 8 * (minimum + 100)
        assert 'Contratos celebrados' in result['response']
        envelope = flow.conv_store.load_envelope('audit')
        assert envelope.usable
        published = envelope.envelope.profile_state['confirmed_frame']
        assert published['schema_version'] == 2
        assert published['source_predicates']['CONTRACTS']['contract_universe'] == 'celebrated'
        # Same public frame path with an inspectable typed outcome and serialized reload.
        query = 'analiza contratos de alcaldia de cartagena por mas de 500 millones'
        turn = run_profile_turn(user_query=query, params=flow.query_router.parse(query).params,
                                state=ProfileConversationState(), turn_id='seed',
                                soql_builder=flow.soql_builder, secop_client=flow.secop_client,
                                today=date(2026, 9, 8))
        restored = EntityProfileFrame.from_dict(turn.frame.to_dict())
        assert restored.fingerprint() == turn.frame.fingerprint()
        assert restored.predicates_for(SourceKind.CONTRACTS).contract_universe == 'celebrated'
        refined = run_profile_turn(user_query='solo 2026', params=flow.query_router.parse('solo 2026').params,
                                   state=ProfileConversationState().confirm(restored), turn_id='next',
                                   soql_builder=flow.soql_builder, secop_client=flow.secop_client,
                                   today=date(2026, 9, 8))
        assert refined.answered, refined
        assert '2026' in refined.response and 'Contratos celebrados' in refined.response
        assert all('estado_contrato' in j.sql and str(minimum) in j.sql for j in refined.plan.jobs)
    finally:
        conn.close()


def test_actor_off_control(tmp_path, monkeypatch, offline_universe):
    from app.config import actor_profile_enabled
    query, _, buyer, minimum = SEEDS[1]
    flow = workflow(tmp_path, monkeypatch, index_at(tmp_path, []), False)
    monkeypatch.setenv('SECOPPAL_ACTOR_PROFILE_ENABLED', 'false')
    from app.config import get_settings
    get_settings.cache_clear()
    conn = source(flow, monkeypatch, buyer, minimum)
    try:
        assert actor_profile_enabled() is False
        result = say(flow, query)
        assert result['total_count'] == 8
    finally:
        conn.close()


def test_explicit_frame_records_reload(tmp_path, monkeypatch, offline_universe):
    _, _, buyer, minimum = SEEDS[0]
    index = index_at(tmp_path, [])
    for text in ('analiza registros cancelados de alcaldia de cartagena', 'solo 2026'):
        flow = workflow(tmp_path, monkeypatch, index, True)
        conn = source(flow, monkeypatch, buyer, minimum)
        try:
            result = say(flow, text)
            assert not result.get('needs_clarification'), result
            assert result['route_reason'].startswith('entity_profile'), result
            assert 'Registros de la fuente' in result['response']
            assert all("'Cancelado'" in c['soql'] for c in flow.audit_trace['sql']), flow.audit_trace
            metric = next(rows[0] for rows in flow.audit_trace['source_rows'] if rows and 'average_contract_value' in rows[0])
            assert metric['contract_count'] == (5 if text == 'solo 2026' else 10)
        finally:
            conn.close()


def test_only_excluded_actor_remains_resolvable(tmp_path, monkeypatch, offline_universe):
    _, _, buyer, minimum = SEEDS[0]
    flow = workflow(tmp_path, monkeypatch, index_at(tmp_path, [alias('EXCLUDED HUGE', '88888888')]), True)
    conn = source(flow, monkeypatch, buyer, minimum)
    try:
        result = say(flow, 'qué ha contratado EXCLUDED HUGE')
        assert result['route_reason'] == 'actor_profile', result
        assert result['resolved_params']['actor_scope']['base_number'] == '88888888'
        assert result['total_count'] == 0
        assert 'Contratos celebrados' in result['response']
        assert 'no existe' not in result['response'].lower()
    finally:
        conn.close()


def test_entity_profile_frame_off_values(tmp_path, monkeypatch, offline_universe):
    _, _, buyer, minimum = SEEDS[0]
    flow = workflow(tmp_path, monkeypatch, index_at(tmp_path, []), False)
    conn = source(flow, monkeypatch, buyer, minimum)
    try:
        result = say(flow, 'analiza contratos de alcaldia de cartagena por mas de 500 millones')
        assert result['route_reason'].startswith('entity_profile'), result
        values = next(rows[0] for rows in flow.audit_trace['source_rows'] if rows and 'average_contract_value' in rows[0])
        assert values['contract_count'] == 8
        assert values['total_contract_value'] == 4_000_000_800
        assert 'Contratos celebrados' in result['response']
    finally:
        conn.close()


def test_current_record_profile_migration_keeps_universe(tmp_path, monkeypatch, offline_universe):
    _, _, buyer, minimum = SEEDS[0]
    index = index_at(tmp_path, [])
    for enabled, text, expected in (
        (False, 'analiza todos los registros del dataset de alcaldia de cartagena por mas de 500 millones', 63),
        (True, 'solo 2026', 35),
    ):
        flow = workflow(tmp_path, monkeypatch, index, enabled)
        conn = source(flow, monkeypatch, buyer, minimum)
        try:
            result = say(flow, text)
            assert not result.get('needs_clarification'), result
            rows = next(rows for rows in flow.audit_trace['source_rows'] if rows and 'average_contract_value' in rows[0])
            assert rows[0]['contract_count'] == expected, result
            assert 'Registros de la fuente' in result['response']
        finally:
            conn.close()
