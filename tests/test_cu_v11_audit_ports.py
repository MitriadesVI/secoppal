"""Independent ports of the ten frozen reds. Oracles authored here, not copied
from the remediation ports. Each port keeps every assertion of the original and
adds the explicit Aprobado composition the original count could not express."""
from dataclasses import asdict, replace
from datetime import date
from io import BytesIO

import pytest

from app.core.contract_universe import stamp
from tests.buyer_actor_independent_support import SEEDS, alias, say
from tests.contract_universe_independent_support import capture, ids, record, setup

# Independent expectation, derived from the V1.1 policy text, not from runtime:
# seven posterior states admitted regardless of date (7 x 3 rows) plus Aprobado
# only with a usable date (1 row). Aprobado absent/invalid stay out.
POSTERIOR = {f'A-{i}-{s}' for i in range(7) for s in ('dated', 'absent', 'invalid')}
APROBADO_IN = {'A-7-dated'}
APROBADO_OUT = {'A-7-absent', 'A-7-invalid'}
CELEBRATED = POSTERIOR | APROBADO_IN
assert len(CELEBRATED) == 22


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    yield flow, conn
    conn.close()


# ---- ports 1-4: test_independent_sql_matrix -------------------------------
@pytest.mark.parametrize('seed', SEEDS)
@pytest.mark.parametrize('frame', [False, True])
def test_port_independent_sql_matrix(tmp_path, monkeypatch, seed, frame):
    query, _, buyer, minimum = seed
    flow, conn = setup(tmp_path, monkeypatch, buyer, minimum, frame)
    try:
        result = say(flow, query)
        # original assertion 1 (unchanged, insensitive to Aprobado)
        expected = {f'A-{i}-{s}' for i in range(7) for s in ('dated', 'absent', 'invalid')}
        assert ids(result) - {f'A-7-{s}' for s in ('dated', 'absent', 'invalid')} == expected
        # NEW: the whole admitted set, stating the Aprobado composition
        assert ids(result) == CELEBRATED
        assert not (ids(result) & APROBADO_OUT)
        # original assertion 2, adjudicated 24 -> 22
        assert result['total_count'] == 22
        # original assertions 3..6, unchanged in kind
        assert sum(int(r['valor_del_contrato']) for r in result['results']) == 22 * (minimum + 100)
        assert result['resolved_params']['entidad_resolved'] == buyer
        assert result['resolved_params']['valor_min'] == minimum
        from app.core.analytics.aggregate_sum import build_aggregate_sum_query
        sql = build_aggregate_sum_query(flow.soql_builder, 'jbjy-vk9h', result['resolved_params'])
        metrics = flow.secop_client.aggregate('jbjy-vk9h', sql)[0]
        assert metrics == {'total_value': 22 * (minimum + 100), 'total_count': 22}
        assert not flow.audit_trace['errors']
    finally:
        conn.close()


# ---- port 5: test_direct_record_does_not_leak_to_search -------------------
def test_port_direct_record_does_not_leak_to_search(fixture):
    flow, conn = fixture
    conn.execute("UPDATE contracts SET id_contrato='CO1.PCCNTR.1234567' WHERE id_contrato='X-000'")
    direct = say(flow, 'CO1.PCCNTR.1234567')
    assert direct['results'][0]['estado_contrato'] == 'Cancelado'
    result = say(flow, SEEDS[0][0])
    assert result['total_count'] == 22
    assert ids(result) == CELEBRATED
    assert result['resolved_params']['contract_universe'] == 'celebrated'


# ---- port 6: test_radar_cancellation_event_and_digest ---------------------
def test_port_radar_cancellation_event_and_digest(fixture, tmp_path):
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
    assert first.total == 22 and second.total == 21
    assert 'Cancelado' in second.digest['text']
    assert 'salieron del universo' in second.digest['text']
    assert len(second.diff.changed_state) == 1
    assert second.diff.changed_state[0].key == 'A-0-dated'
    assert second.diff.changed_state[0].prev_status == 'En ejecución'
    assert second.diff.changed_state[0].fields['status'] == 'Cancelado'


# ---- ports 7-8: test_actor_name_state_word_has_no_universe_authority ------
@pytest.mark.parametrize('frame', [False, True])
def test_port_actor_name_state_word_has_no_universe_authority(tmp_path, monkeypatch, frame):
    name = 'FUNDACION BORRADORES'
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame,
                       aliases=[alias(name)])
    conn.execute('UPDATE contracts SET proveedor_adjudicado=?, documento_proveedor=?', (name, '71000001'))
    try:
        result = say(flow, 'qué ha contratado FUNDACION BORRADORES')
        assert result['resolved_params']['actor_scope']['base_number'] == '71000001'
        assert result['resolved_params'].get('contract_universe', 'celebrated') == 'celebrated'
        # 22 admitted + the two controls the Actor scope does not filter out
        assert result['total_count'] == 24
        # The actor profile returns aggregates, not rows: re-evaluate the last
        # row SELECT the runtime actually issued to state the row composition.
        import re as _re
        selects = [c['soql'] for c in flow.audit_trace['sql'] if 'id_contrato,' in c['soql']]
        assert selects, flow.audit_trace['sql']
        evaluated = _re.sub(r'\bWHERE\b', 'FROM contracts WHERE', selects[-1], count=1)
        evaluated = _re.sub(r'\sLIMIT\s+\d+', '', evaluated)
        observed = {row['id_contrato'] for row in conn.execute(evaluated)}
        assert observed == CELEBRATED | {'OTHER-BUYER', 'LOW-VALUE'}
        assert not (observed & APROBADO_OUT)
    finally:
        conn.close()


# ---- port 9: test_materialized_dual_children_and_source_names[celebrated] -
@pytest.mark.parametrize('universe,expected', [('celebrated', 22), ('source_records', 94)])
def test_port_materialized_dual_children_and_source_names(fixture, universe, expected):
    from app.core.workflow.dual_dataset import execute_dataset_child
    flow, conn = fixture
    params = dict(dataset_scope=['contratos', 'procesos'], dataset='contratos',
                  entidad_resolved=SEEDS[0][2], valor_min=SEEDS[0][3], contract_universe=universe)
    groups = {kind: execute_dataset_child(kind, params, flow) for kind in ['contratos', 'procesos']}
    response, rows = flow.formatter.format_grouped(groups, params=params, channel='streamlit')
    assert groups['contratos'].counts['total_count'] == expected
    assert groups['procesos'].status == 'ok'
    assert 'estado_contrato' not in groups['procesos'].soql
    assert 'Contratos' in response and 'Procesos' in response
    assert all('estado_contrato' not in x['soql'] for x in flow.audit_trace['sql'] if x['dataset'] == 'p6dx-8zbt')


# ---- port 10: test_real_base_conversation_requires_review -----------------
def test_port_real_base_conversation_requires_review(tmp_path, monkeypatch):
    import json
    from pathlib import Path
    historical = json.loads(
        Path('docs/audits/contract_universe_v1_independent/base_serialized.json').read_text())
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    path = tmp_path / 'conversations/audit.jsonl'
    path.write_text(historical['conversation'])
    original = path.read_bytes()
    try:
        result = say(flow, 'solo 2026')
        assert result['needs_clarification']
        assert not flow.audit_trace['sql']
        assert path.read_bytes().startswith(original)
        fresh = say(flow, SEEDS[0][0])
        assert fresh['total_count'] == 22
        assert ids(fresh) == CELEBRATED
        assert path.read_bytes().startswith(original)
    finally:
        conn.close()
