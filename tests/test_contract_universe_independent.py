"""Policy oracles are authored independently; failures remain red for review."""
from dataclasses import asdict, replace
from datetime import date
from io import BytesIO

import pytest
from hypothesis import given, settings, strategies as st

from app.core.contract_universe import stamp
from app.core.contract_universe_intent import apply_request, request
from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import (
    ADMITTED, DESCRIPTION, capture, ids, record, setup,
)


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    yield flow, conn
    conn.close()


@pytest.mark.parametrize('seed', SEEDS)
@pytest.mark.parametrize('frame', [False, True])
def test_independent_sql_matrix(tmp_path, monkeypatch, seed, frame):
    query, _, buyer, minimum = seed
    flow, conn = setup(tmp_path, monkeypatch, buyer, minimum, frame)
    try:
        result = say(flow, query)
        capture(flow, 'matrix', result)
        expected = {f'A-{i}-{s}' for i in range(7) for s in ('dated', 'absent', 'invalid')}
        assert ids(result) - {f'A-7-{s}' for s in ('dated', 'absent', 'invalid')} == expected
        assert result['total_count'] == 24  # Observed candidate rule includes 3 Aprobado.
        assert sum(int(r['valor_del_contrato']) for r in result['results']) == 24 * (minimum + 100)
        assert result['resolved_params']['entidad_resolved'] == buyer
        assert result['resolved_params']['valor_min'] == minimum
        from app.core.analytics.aggregate_sum import build_aggregate_sum_query
        sql = build_aggregate_sum_query(flow.soql_builder, 'jbjy-vk9h', result['resolved_params'])
        metrics = flow.secop_client.aggregate('jbjy-vk9h', sql)[0]
        assert metrics == {'total_value': 24 * (minimum + 100), 'total_count': 24}
        record('matrix-aggregate', dict(sql=sql, metrics=metrics, buyer=buyer))
        assert not flow.audit_trace['errors']
    finally:
        conn.close()


def test_object_borradores_is_not_state(fixture):
    flow, conn = fixture
    result = say(flow, 'contratos de elaboración de borradores de documentos de la Alcaldía de Cartagena')
    capture(flow, 'object-borradores', result)
    assert result['resolved_params'].get('contract_universe') == 'celebrated'
    assert ids(result) and all(i.startswith('A-') or i == 'LOW-VALUE' for i in ids(result))
    assert 'borrador' in str(result['resolved_params'].get('objeto'))


@pytest.mark.parametrize('text', [
    'registros cancelados y borradores de la Alcaldía de Cartagena',
    'registros borradores y cancelados de la Alcaldía de Cartagena',
])
def test_two_states_not_silently_partial(fixture, text):
    flow, conn = fixture
    result = say(flow, text)
    capture(flow, 'two-states', result)
    if result.get('needs_clarification'):
        assert not flow.audit_trace['sql']
    else:
        assert {r['estado_contrato'] for r in result['results']} == {'Cancelado', 'Borrador'}


@pytest.mark.parametrize('text', [
    'contratos de la Alcaldía de Cartagena sin registros cancelados',
    'contratos de la Alcaldía de Cartagena no quiero borradores',
    'contratos de la Alcaldía de Cartagena excluyendo los registros cancelados',
])
def test_negated_records_do_not_widen(fixture, text):
    flow, conn = fixture
    result = say(flow, text)
    capture(flow, 'negation', result)
    assert result.get('needs_clarification') or result['resolved_params'].get('contract_universe') == 'celebrated'


@pytest.mark.parametrize('params', [
    dict(dataset='contratos', estado=['Cancelado'], estado_field='estado_del_procedimiento'),
    dict(dataset='contratos', estado_contrato=['Cancelado'], raw_query='sin cancelados'),
    dict(dataset='contratos', estado=['Cancelado'], estado_family='cancelled', raw_query='contratos suspendidos'),
])
def test_materialized_fields_are_not_consent(params):
    result = apply_request(params, request(params.get('raw_query', '')))
    record('materialized-authority', dict(input=params, output=result))
    assert result.get('contract_universe') != 'source_records'


@pytest.mark.parametrize('frame', [False, True])
def test_published_description_not_rewritten(tmp_path, monkeypatch, frame):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame)
    try:
        result = say(flow, 'analiza registros cancelados de alcaldia de cartagena')
        capture(flow, 'published-content', result)
        assert DESCRIPTION in result['response']
        assert 'borradores de documentos y registros de consultoría' not in result['response']
    finally:
        conn.close()


@pytest.mark.parametrize('records', [False, True])
def test_complete_continuity_and_export(fixture, records):
    from app.core.export_xlsx import build_results_xlsx
    from openpyxl import load_workbook
    flow, conn = fixture
    flow.formatter.max_results = 3
    first = say(flow, SEEDS[0][0])
    if records:
        first = say(flow, 'todos los registros del dataset')
        assert first['total_count'] == 94
        assert first['resolved_params']['valor_min'] == SEEDS[0][3]
    refined = say(flow, 'solo 2026')
    first_ids = {r['id_contrato'] for r in refined['rows']}
    more = say(flow, 'muéstrame más')
    capture(flow, 'continuity-records' if records else 'continuity-celebrated', more)
    assert first_ids.isdisjoint({r['id_contrato'] for r in more['rows']})
    assert more['resolved_params']['valor_min'] == SEEDS[0][3]
    assert more['resolved_params']['entidad_resolved'] == SEEDS[0][2]
    assert more['resolved_params']['contract_universe'] == ('source_records' if records else 'celebrated')
    blob = build_results_xlsx(rows=more['rows'], query='muéstrame más',
                              resolved_params=more['resolved_params'], dataset_id='jbjy-vk9h',
                              total_count=more['total_count'])
    book = load_workbook(BytesIO(blob))
    assert 'contract_universe' in str(list(book.worksheets[1].values))


def test_frame_fingerprint_scope_changes(fixture):
    from app.core.entity_profile.frame import EntityProfileFrame
    from app.core.entity_profile.frame_turn import run_profile_turn
    from app.core.entity_profile.frame_values import SourceKind
    from app.core.entity_profile.profile_state import ProfileConversationState
    flow, conn = fixture
    text = 'analiza contratos de alcaldia de cartagena por mas de 500 millones'
    turn = run_profile_turn(user_query=text, params=flow.query_router.parse(text).params,
                            state=ProfileConversationState(), turn_id='independent',
                            soql_builder=flow.soql_builder, secop_client=flow.secop_client,
                            today=date(2026, 9, 8))
    frame = EntityProfileFrame.from_dict(turn.frame.to_dict())
    assert frame.fingerprint() == turn.frame.fingerprint()
    changed = replace(frame, source_predicates={**frame.source_predicates,
                      SourceKind.CONTRACTS: replace(frame.predicates_for(SourceKind.CONTRACTS),
                                                   contract_universe='source_records')})
    assert changed.fingerprint() != frame.fingerprint()
    from app.core.entity_profile.frame_compiler import compile_profile
    from app.core.entity_profile.frame_runner import execute_plan
    plan = compile_profile(changed, soql_builder=flow.soql_builder).plan
    evidence = execute_plan(plan, flow.secop_client)
    metrics = next(rs[0] for rs in evidence.rows.values() if rs and 'average_contract_value' in rs[0])
    assert metrics['contract_count'] == 94
    assert metrics['total_contract_value'] == 24 * 500_000_100 + 70 * 900_000_000_000
    record('fingerprints', dict(before=frame.to_dict(), after=changed.to_dict(),
                               sql=[j.sql for j in turn.plan.jobs],
                               changed_sql=[j.sql for j in plan.jobs], changed_metrics=metrics))


def test_direct_record_does_not_leak_to_search(fixture):
    flow, conn = fixture
    conn.execute("UPDATE contracts SET id_contrato='CO1.PCCNTR.1234567' WHERE id_contrato='X-000'")
    direct = say(flow, 'CO1.PCCNTR.1234567')
    assert direct['results'][0]['estado_contrato'] == 'Cancelado'
    result = say(flow, SEEDS[0][0])
    capture(flow, 'direct-then-search', result)
    assert result['total_count'] == 24
    assert result['resolved_params']['contract_universe'] == 'celebrated'


def test_radar_cancellation_event_and_digest(fixture, tmp_path):
    from app.core.radar.builder import save_radar_from_params
    from app.core.radar.runner import run_radar
    from app.core.radar.store import RadarStore
    flow, conn = fixture
    store = RadarStore(tmp_path / 'radar.sqlite')
    params = stamp(dict(dataset='contratos', entidad_resolved=SEEDS[0][2], valor_min=SEEDS[0][3]))
    radar_id = save_radar_from_params(store, owner='independent', name='independent', params=params)
    first = run_radar(flow, store, store.get_radar(radar_id))
    conn.execute("UPDATE contracts SET estado_contrato='Cancelado' WHERE id_contrato='A-0-dated'")
    second = run_radar(flow, store, store.get_radar(radar_id))
    record('radar-cancellation', dict(first=asdict(first), second=asdict(second), source=flow.audit_trace))
    assert first.total == 24 and second.total == 23
    assert 'Cancelado' in second.digest['text']
    assert 'salieron del universo' in second.digest['text']
    assert len(second.diff.changed_state) == 1
    assert second.diff.changed_state[0].key == 'A-0-dated'
    assert second.diff.changed_state[0].prev_status == 'En ejecución'
    assert second.diff.changed_state[0].fields['status'] == 'Cancelado'


@settings(max_examples=30, derandomize=True, deadline=None)
@given(st.sampled_from(ADMITTED), st.one_of(st.none(), st.text(max_size=30)))
def test_admitted_state_independent_of_date(state, date_value):
    from app.core.contract_universe import eligible
    row = dict(estado_contrato=state, fecha_de_firma=date_value)
    before = dict(row)
    assert eligible(row)
    assert row == before
