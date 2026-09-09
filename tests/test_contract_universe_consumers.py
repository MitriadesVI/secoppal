"""Execute actual consumer builders against the common synthetic database."""
from dataclasses import asdict
import json
from types import SimpleNamespace
import pytest
from app.core.soql_builder import SoQLBuilder
from app.core.contract_universe import ContractUniverse, POLICY_VERSION, stamp
from tests.contract_universe_support import offline_universe, source  # noqa: F401
from tests.buyer_actor_independent_support import SEEDS, workflow, index_at, say


@pytest.fixture
def fixture(tmp_path, monkeypatch, offline_universe):
    _, _, buyer, minimum = SEEDS[0]
    flow = workflow(tmp_path, monkeypatch, index_at(tmp_path, []), False)
    conn = source(flow, monkeypatch, buyer, minimum)
    yield flow, conn, stamp(dict(dataset='contratos', entidad_resolved=buyer, valor_min=minimum))
    conn.close()


def test_count_analytics_observer_and_rankings(fixture):
    from app.core.analytics.aggregate_sum import build_aggregate_sum_query
    from app.core.entity_profile.aspects import build_contract_values_query, build_supplier_query
    flow, conn, params = fixture
    b, ds, client = flow.soql_builder, 'jbjy-vk9h', flow.secop_client
    assert client.count(ds, b.build_count(ds, params)) == 8
    aggregate = client.aggregate(ds, build_aggregate_sum_query(b, ds, params))[0]
    assert aggregate['total_count'] == 8
    assert aggregate['total_value'] == 4_000_000_800
    stats = client.aggregate(ds, b.build_value_stats(ds, params))[0]
    assert 999_000_000_000 not in stats.values(), stats
    for build in (b.build_top_entities, b.build_top_modalities, b.build_top_territories):
        rows = client.aggregate(ds, build(ds, params))
        assert sum(r['cnt'] for r in rows) == 8
    values = client.aggregate(ds, build_contract_values_query(b, params))[0]
    assert values['contract_count'] == 8 and values['total_contract_value'] == 4_000_000_800
    for order in ('count', 'value'):
        rows = client.aggregate(ds, build_supplier_query(b, params, order))
        assert len(rows) == 1 and 'EXCLUDED HUGE' not in str(rows)
    assert not flow.audit_trace['errors']


def test_reload_pages_order_and_export(fixture, tmp_path, monkeypatch):
    from app.core.export_xlsx import build_results_xlsx
    flow, conn, params = fixture
    flow.formatter.max_results = 3
    first = say(flow, SEEDS[0][0])
    ids1 = {row['id_contrato'] for row in first['rows']}
    assert len(ids1) == 3
    # Reload the on-disk conversation in a fresh workflow with the same SQL source.
    restored = workflow(tmp_path, monkeypatch, index_at(tmp_path, []), False)
    restored.formatter.max_results = 3
    second_conn = source(restored, monkeypatch, params['entidad_resolved'], params['valor_min'])
    try:
        second = say(restored, 'muéstrame más')
        ids2 = {row['id_contrato'] for row in second['rows']}
        assert len(ids2) == 3 and ids1.isdisjoint(ids2), second
        assert second['resolved_params']['contract_universe'] == 'celebrated'
        assert second['total_count'] == 8
        # Export the published rows with their effective filter metadata.
        content = build_results_xlsx(rows=second['rows'], query=SEEDS[0][0],
                                     resolved_params=second['resolved_params'],
                                     dataset_id='jbjy-vk9h', total_count=8)
        from io import BytesIO
        from openpyxl import load_workbook
        book = load_workbook(BytesIO(content))
        assert 'contract_universe' in str(list(book.worksheets[1].values))
        refined = say(restored, 'solo 2026')
        assert refined['total_count'] == 5, refined
        assert refined['resolved_params']['valor_min'] == params['valor_min']
        assert refined['resolved_params']['contract_universe'] == 'celebrated'
    finally:
        second_conn.close()


def test_radar_roundtrip_and_legacy_review(fixture):
    from app.core.query_frame import frame_from_params
    from app.core.radar.runner import params_from_radar, _validate_frame_json
    flow, conn, params = fixture
    def radar(p):
        raw = asdict(frame_from_params(p))
        return SimpleNamespace(query_frame_json=json.dumps(raw), dataset='contratos', frame_dict=lambda: raw)
    current = radar(params)
    assert _validate_frame_json(current) is None
    restored = params_from_radar(current)
    assert restored['contract_universe_version'] == POLICY_VERSION
    assert flow.secop_client.count('jbjy-vk9h', flow.soql_builder.build_count('jbjy-vk9h', restored)) == 8
    records = radar(dict(params, contract_universe=ContractUniverse.SOURCE_RECORDS.value))
    assert flow.secop_client.count('jbjy-vk9h', flow.soql_builder.build_count('jbjy-vk9h', params_from_radar(records))) == 63
    old = radar({k: v for k, v in params.items() if not k.startswith('contract_universe')})
    assert _validate_frame_json(old).reason == 'contract_universe_review_required'


def test_unknown_default_is_not_scope():
    from app.core.soql_builder import UnsafeGlobalQueryError
    with pytest.raises(UnsafeGlobalQueryError):
        SoQLBuilder().build('jbjy-vk9h', stamp({'dataset': 'contratos'}))


def test_explicit_universe_switch_preserves_scope_and_restarts(fixture):
    flow, conn, params = fixture
    first = say(flow, SEEDS[0][0])
    assert first['total_count'] == 8
    changed = say(flow, 'todos los registros del dataset')
    assert changed['resolved_params'].get('contract_universe') == 'source_records', changed
    assert changed['resolved_params'].get('entidad_resolved') == params['entidad_resolved'], changed
    assert changed['resolved_params'].get('valor_min') == params['valor_min'], changed
    assert changed['total_count'] == 63, changed
    assert any(r['id_contrato'].startswith('EXCLUDED') for r in changed['results'])


def test_negation_does_not_widen(fixture):
    flow, conn, params = fixture
    result = say(flow, 'contratos de alcaldia de cartagena sin cancelados por mas de 500 millones')
    assert result['resolved_params'].get('contract_universe', 'celebrated') == 'celebrated'
    assert not any(r['estado_contrato'] == 'Cancelado' for r in result['results'])


def test_contradictory_request_clarifies(fixture):
    flow, conn, params = fixture
    result = say(flow, 'contratos celebrados cancelados de alcaldia de cartagena')
    assert result['needs_clarification'], result
    assert not flow.audit_trace['sql']


def test_direct_id_preserves_excluded_record(fixture):
    flow, conn, params = fixture
    conn.execute("UPDATE contracts SET id_contrato='CO1.PCCNTR.1234567' WHERE id_contrato='EXCLUDED-000'")
    result = say(flow, 'CO1.PCCNTR.1234567')
    assert result['results'][0]['estado_contrato'] == 'Cancelado', result
    assert 'Registros de la fuente' in result['response']


def test_legacy_pagination_is_rejected(fixture):
    from app.core.workflow.tabular_capability import capability, validate_tabular
    from app.core.pagination_buffer import PaginationBuffer, pagination_fingerprint
    flow, conn, params = fixture
    old = {k: v for k, v in params.items() if not k.startswith('contract_universe')}
    buffer = PaginationBuffer.create(query_fingerprint=pagination_fingerprint(old), group='contratos',
                                     dataset_id='jbjy-vk9h', query_params=old, candidate_buffer=[],
                                     validated_ranked_buffer=[], display_cursor=0, source_cursor=50,
                                     source_candidate_total=0)
    reservation = SimpleNamespace(chat_id='audit', previous=SimpleNamespace(envelope=SimpleNamespace(continuity_generation='g')))
    store = SimpleNamespace(_pagination_path_for=lambda chat: 'path',
                            _read_pagination_payload=lambda path: {'contratos': buffer.to_dict()})
    contract = SimpleNamespace(tabular_groups={'contratos': capability(buffer, 'g')})
    with pytest.raises(ValueError, match='universe'):
        validate_tabular(store, reservation, contract, 'muéstrame más')


def test_records_universe_survives_reload_values(fixture, tmp_path, monkeypatch):
    flow, conn, params = fixture
    say(flow, SEEDS[0][0])
    assert say(flow, 'todos los registros del dataset')['total_count'] == 63
    reloaded = workflow(tmp_path, monkeypatch, index_at(tmp_path, []), False)
    reload_conn = source(reloaded, monkeypatch, params['entidad_resolved'], params['valor_min'])
    try:
        result = say(reloaded, 'solo 2026')
        assert result['total_count'] == 35, result
        assert result['resolved_params']['contract_universe'] == 'source_records'
    finally:
        reload_conn.close()


def test_saved_radar_executes_contract_universe(fixture, tmp_path):
    from app.core.radar.builder import save_radar_from_params
    from app.core.radar.runner import run_radar
    from app.core.radar.store import RadarStore
    flow, conn, params = fixture
    store = RadarStore(tmp_path / 'contract-radar.sqlite')
    radar_id = save_radar_from_params(store, owner='offline', name='Contracts', params=params)
    radar = store.get_radar(radar_id)
    first = run_radar(flow, store, radar)
    assert first.total == 8 and first.status == 'ok', first
    assert first.digest['contract_universe'] == 'celebrated'
    assert 'Contratos celebrados' in first.digest['text']
    conn.execute("UPDATE contracts SET estado_contrato='Cancelado' WHERE id_contrato='VALID-0'")
    second = run_radar(flow, store, store.get_radar(radar_id))
    assert second.total == 7 and second.status == 'ok', second
    assert not flow.audit_trace['errors']


def test_dual_child_is_stamped_without_changing_processes(fixture):
    from app.core.workflow.dual_dataset import params_for_dataset, execute_dataset_child
    from app.core.contract_universe import compatible
    flow, conn, params = fixture
    common = {k: v for k, v in params.items() if not k.startswith('contract_universe')}
    common.update(dataset='procesos', dataset_scope=['procesos', 'contratos'])
    child = params_for_dataset(common, 'contratos')
    assert compatible(child)
    group = execute_dataset_child('contratos', common, flow)
    assert group.counts['total_count'] == 8
    assert len(group.candidate_rows) == 8
    assert 'contract_universe' not in params_for_dataset(params, 'procesos')


def test_universe_switch_with_incompatible_state_clarifies(fixture):
    flow, conn, params = fixture
    first = say(flow, 'registros cancelados de Cartagena')
    assert first['results']
    flow.audit_trace['sql'].clear()
    result = say(flow, 'todos los contratos')
    assert result['needs_clarification'], result
    assert not flow.audit_trace['sql']


def test_legacy_search_refinement_and_export_require_review(fixture):
    from app.core.export_xlsx import build_results_xlsx
    flow, conn, params = fixture
    old = {key: value for key, value in params.items() if not key.startswith('contract_universe')}
    flow.conv_store.append_turn(chat_id='audit', user_query=SEEDS[0][0], response='Old search',
                               parsed_params=old, result_ids=['old'], trace_id='old')
    result = say(flow, 'solo 2026')
    assert result['needs_clarification'], result
    assert not flow.audit_trace['sql']
    with pytest.raises(ValueError, match='universo'):
        build_results_xlsx(rows=[], query=SEEDS[0][0], resolved_params=old,
                           dataset_id='jbjy-vk9h')
    # A complete fresh query remains available, even beside that old history.
    refreshed = say(flow, SEEDS[0][0])
    assert not refreshed['needs_clarification'], refreshed
    assert refreshed['total_count'] == 8


def test_stale_export_ui_explains_without_building_workbook():
    import ast
    from pathlib import Path
    tree = ast.parse(Path('streamlit_app.py').read_text())
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_render_excel_export')
    messages = []
    namespace = {'st': SimpleNamespace(info=messages.append)}
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'streamlit_export_only', 'exec'), namespace)
    namespace['_render_excel_export']({'rows': [{'id_contrato': 'old'}],
                                       'debug': {'dataset_id': 'jbjy-vk9h', 'resolved_params': {}}}, 1)
    assert len(messages) == 1 and 'requiere revisión' in messages[0]
