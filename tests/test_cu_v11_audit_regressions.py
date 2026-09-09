"""Minimum regression surface for Contract Universe V1.1, independently authored."""
from io import BytesIO

import pytest

from tests.buyer_actor_independent_support import SEEDS, alias, assert_buyer, say
from tests.contract_universe_independent_support import ids, setup

CELEBRATED = {f'A-{i}-{s}' for i in range(7) for s in ('dated', 'absent', 'invalid')} | {'A-7-dated'}
PREDICATE_MARK = 'estado_contrato'


@pytest.mark.parametrize('seed', SEEDS)
@pytest.mark.parametrize('frame', [False, True])
def test_buyer_actor_seeds_still_hold(tmp_path, monkeypatch, seed, frame):
    query, _, buyer, minimum = seed
    flow, conn = setup(tmp_path, monkeypatch, buyer, minimum, frame)
    try:
        result = say(flow, query)
        assert_buyer(flow, result, buyer, minimum)
        assert ids(result) == CELEBRATED
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_actor_resolution_is_unaffected(tmp_path, monkeypatch, frame):
    name = 'FUNDACARIBE'
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame,
                       aliases=[alias(name)])
    try:
        result = say(flow, f'qué ha contratado {name}')
        assert result['resolved_params']['actor_scope']['base_number'] == '71000001'
        assert result['resolved_params'].get('contract_universe', 'celebrated') == 'celebrated'
    finally:
        conn.close()


def test_search_count_sum_and_ranking_share_one_universe(tmp_path, monkeypatch):
    from app.core.analytics.aggregate_sum import build_aggregate_sum_query
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], False)
    try:
        result = say(flow, SEEDS[0][0])
        params = result['resolved_params']
        assert result['total_count'] == 22
        sql = build_aggregate_sum_query(flow.soql_builder, 'jbjy-vk9h', params)
        metrics = flow.secop_client.aggregate('jbjy-vk9h', sql)[0]
        assert metrics['total_count'] == 22
        assert metrics['total_value'] == 22 * (SEEDS[0][3] + 100)
        ranking = flow.soql_builder.build_query(
            'jbjy-vk9h', dict(params), select='proveedor_adjudicado, count(*) AS cnt',
            group_by='proveedor_adjudicado') if hasattr(flow.soql_builder, 'build_query') else None
        if ranking:
            assert 'APROBADO' in ranking.upper()
    finally:
        conn.close()


def test_ordinary_processes_never_receive_a_contract_predicate(tmp_path, monkeypatch):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], False)
    try:
        say(flow, 'procesos de alcaldia de cartagena')
        calls = [c for c in flow.audit_trace['sql'] if c['dataset'] == 'p6dx-8zbt']
        assert calls
        for call in calls:
            assert PREDICATE_MARK not in call['soql']
            assert 'APROBADO' not in call['soql'].upper()
            assert 'contract_universe' not in call['soql']
    finally:
        conn.close()


@pytest.mark.parametrize('records', [False, True])
def test_pagination_and_export_keep_the_universe(tmp_path, monkeypatch, records):
    from app.core.export_xlsx import build_results_xlsx
    from openpyxl import load_workbook
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], False)
    try:
        flow.formatter.max_results = 3
        first = say(flow, SEEDS[0][0])
        if records:
            first = say(flow, 'todos los registros del dataset')
            assert first['total_count'] == 94
        refined = say(flow, 'solo 2026')
        first_ids = {r['id_contrato'] for r in refined['rows']}
        more = say(flow, 'muéstrame más')
        assert first_ids.isdisjoint({r['id_contrato'] for r in more['rows']})
        assert more['resolved_params']['contract_universe'] == (
            'source_records' if records else 'celebrated')
        assert more['resolved_params']['contract_universe_version'] == 'contract-universe-1.1'
        blob = build_results_xlsx(rows=more['rows'], query='muéstrame más',
                                  resolved_params=more['resolved_params'],
                                  dataset_id='jbjy-vk9h', total_count=more['total_count'])
        book = load_workbook(BytesIO(blob))
        assert 'contract_universe' in str(list(book.worksheets[1].values))
    finally:
        conn.close()


def test_synthetic_radar_round_trip(tmp_path, monkeypatch):
    from app.core.contract_universe import stamp
    from app.core.radar.builder import save_radar_from_params
    from app.core.radar.runner import run_radar
    from app.core.radar.store import RadarStore
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], False)
    try:
        store = RadarStore(tmp_path / 'radar.sqlite')
        params = stamp(dict(dataset='contratos', entidad_resolved=SEEDS[0][2],
                            valor_min=SEEDS[0][3]))
        assert params['contract_universe_version'] == 'contract-universe-1.1'
        rid = save_radar_from_params(store, owner='independent', name='r', params=params)
        run = run_radar(flow, store, store.get_radar(rid))
        assert run.status == 'ok'
        assert run.total == 22
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_entity_frame_off_on_agree_on_the_universe(tmp_path, monkeypatch, frame):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame)
    try:
        result = say(flow, 'analiza contratos de alcaldia de cartagena por mas de 500 millones')
        assert not result.get('needs_clarification')
        contract_calls = [c for c in flow.audit_trace['sql'] if c['dataset'] == 'jbjy-vk9h']
        assert contract_calls
        for call in contract_calls:
            assert "= 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL" in call['soql']
        assert 'Contratos celebrados' in result['response']
    finally:
        conn.close()
