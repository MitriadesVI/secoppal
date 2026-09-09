"""Additional consumer boundary contrasts including full dual response."""
from dataclasses import asdict

import pytest

from tests.buyer_actor_independent_support import SEEDS, alias, say
from tests.contract_universe_independent_support import capture, record, setup


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3])
    yield flow, conn
    conn.close()


@pytest.mark.parametrize('text,state', [
    ('registros cancelados de la alcaldia de cartagena', 'Cancelado'),
    ('borradores de la alcaldia de cartagena', 'Borrador'),
    ('registros enviados al proveedor de la alcaldia de cartagena', 'enviado Proveedor'),
    ('contratos suspendidos de la alcaldia de cartagena', 'Suspendido'),
])
def test_explicit_control_with_independent_rows(fixture, text, state):
    flow, conn = fixture
    result = say(flow, text)
    capture(flow, 'explicit-state-control', result)
    assert not result.get('needs_clarification')
    assert {r['estado_contrato'] for r in result['results']} == {state}
    assert result['total_count'] == (3 if state == 'Suspendido' else 10)


@pytest.mark.parametrize('frame', [False, True])
def test_actor_name_state_word_has_no_universe_authority(tmp_path, monkeypatch, frame):
    name = 'FUNDACION BORRADORES'
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame,
                       aliases=[alias(name)])
    conn.execute('UPDATE contracts SET proveedor_adjudicado=?, documento_proveedor=?', (name, '71000001'))
    try:
        result = say(flow, 'qué ha contratado FUNDACION BORRADORES')
        capture(flow, 'actor-state-word', result)
        assert result['resolved_params']['actor_scope']['base_number'] == '71000001'
        assert result['resolved_params'].get('contract_universe', 'celebrated') == 'celebrated'
        assert result['total_count'] == 24
    finally:
        conn.close()


@pytest.mark.parametrize('universe,expected', [('celebrated', 22), ('source_records', 94)])
def test_materialized_dual_children_and_source_names(fixture, universe, expected):
    from app.core.workflow.dual_dataset import execute_dataset_child
    flow, conn = fixture
    params = dict(dataset_scope=['contratos', 'procesos'], dataset='contratos',
                  entidad_resolved=SEEDS[0][2], valor_min=SEEDS[0][3], contract_universe=universe)
    groups = {kind: execute_dataset_child(kind, params, flow) for kind in ['contratos', 'procesos']}
    response, rows = flow.formatter.format_grouped(groups, params=params, channel='streamlit')
    record('dual-materialized', dict(groups={k: asdict(g) for k, g in groups.items()},
                                      response=response, rows=rows, source=flow.audit_trace))
    assert groups['contratos'].counts['total_count'] == expected
    assert groups['procesos'].status == 'ok'
    assert 'estado_contrato' not in groups['procesos'].soql
    assert 'Contratos' in response and 'Procesos' in response
    assert all('estado_contrato' not in x['soql'] for x in flow.audit_trace['sql'] if x['dataset'] == 'p6dx-8zbt')


@pytest.mark.parametrize('frame', [False, True])
def test_dual_raw_explicit_records_not_silently_dropped(tmp_path, monkeypatch, frame):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame)
    try:
        result = say(flow, 'analiza procesos y todos los registros del dataset de alcaldia de cartagena por mas de 500 millones')
        capture(flow, 'dual-raw-records', result)
        if result.get('needs_clarification'):
            assert not flow.audit_trace['sql']
        else:
            values = [rs[0] for rs in flow.audit_trace.get('source_rows', [])
                      if rs and 'average_contract_value' in rs[0]]
            assert values and values[0]['contract_count'] == 94
            assert 'Registros de la fuente' in result['response']
    finally:
        conn.close()


def test_invalid_date_is_not_published_as_a_date(fixture):
    flow, conn = fixture
    rows = [dict(r) for r in conn.execute("SELECT * FROM contracts WHERE id_contrato LIKE '%invalid'")]
    rendered = flow.formatter.to_rows(rows, 'jbjy-vk9h', params={'dataset': 'contratos'})
    record('invalid-dates-render', rendered)
    assert '2026-02-30' not in str(rendered)
    assert all(r['fecha_label'] == 'Fecha de firma no publicada' for r in rendered)
