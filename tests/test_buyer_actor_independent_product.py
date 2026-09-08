"""Positive product expectations independent of evaluated parser output."""
import pytest

from tests.buyer_actor_independent_support import (
    SEEDS, CARTAGENA, alias, index_at, workflow, attach_source, say, persist, assert_buyer,
)


@pytest.mark.parametrize('frame', [False, True], ids=['off', 'on'])
@pytest.mark.parametrize('mode', [0, 1, 4, 'absent', 'incompatible'])
@pytest.mark.parametrize('seed', SEEDS, ids=['cartagena', 'barranquilla'])
def test_seed_universe_and_reload(tmp_path, monkeypatch, frame, mode, seed):
    query, mention, buyer, amount = seed
    rows = [alias(mention, str(71000001 + i)) for i in range(mode)] if isinstance(mode, int) else []
    index = index_at(tmp_path, rows, mode)
    traces, pages = [], []
    try:
        for text in (query, 'solo 2026', 'muéstrame más'):
            flow = workflow(tmp_path, monkeypatch, index, frame)
            conn = attach_source(flow, monkeypatch, buyer, amount)
            result = say(flow, text)
            traces.append(flow.audit_trace)
            assert_buyer(flow, result, buyer, amount)
            assert not flow.audit_trace['resolver'], flow.audit_trace
            if text != query:
                assert result['resolved_params']['fecha_desde'] == '2026-01-01'
                assert all(row['fecha'].startswith('2026') for row in result['rows'])
                pages.append({row['id_contrato'] for row in result['rows']})
            conn.close()
        assert pages[0] and pages[1] and pages[0].isdisjoint(pages[1])
    finally:
        persist(f'seed-{mention.split()[-1]}-{mode}-{frame}', traces)


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('cardinality', [1, 2])
@pytest.mark.parametrize('query,name,buyer', [
    ('contratos de FUNDACARIBE', 'FUNDACARIBE', None),
    ('qué ha contratado FUNDACARIBE', 'FUNDACARIBE', None),
    ('contratos del proveedor FUNDACARIBE', 'FUNDACARIBE', None),
    ('contratos del proveedor CC 71000001', 'FUNDACARIBE', None),
    ('contratos del proveedor FUNDACARIBE con la Alcaldía de Cartagena', 'FUNDACARIBE', CARTAGENA),
    ('contratos del proveedor alcaldia de cartagena', 'alcaldia de cartagena', None),
    ('contratos del proveedor UNIVERSIDAD INDUSTRIAL DE SANTANDER', 'UNIVERSIDAD INDUSTRIAL DE SANTANDER', None),
    ('contratos del proveedor alcaldia de cartagena servicios integrados', 'alcaldia de cartagena servicios integrados', None),
])
def test_positive_actors(tmp_path, monkeypatch, frame, cardinality, query, name, buyer):
    index = index_at(tmp_path, [alias(name, str(71000001 + i)) for i in range(cardinality)])
    flow = workflow(tmp_path, monkeypatch, index, frame)
    conn = attach_source(flow, monkeypatch, supplier=name)
    try:
        result = say(flow, query)
        p = result['resolved_params']
        if cardinality == 2 and 'CC 71000001' not in query:
            assert p.get('actor_pending') and result.get('needs_clarification'), result
            assert not flow.audit_trace['sql']
        else:
            assert p.get('actor_scope', {}).get('base_number') == '71000001', result
            assert p.get('entidad_resolved') == buyer, result
            assert flow.audit_trace['sql'], result
            for call in flow.audit_trace['sql']:
                where = call['soql'].split(' WHERE ', 1)[-1]
                assert '71000001' in where and 'documento_proveedor' in where, where
                if buyer:
                    assert buyer in where, where
            assert name.lower() in result['response'].lower(), result
            assert 'No pude' not in result['response'], result
            recent = [row for rows in flow.audit_trace['source_rows'] for row in rows if 'id_contrato' in row]
            assert recent and all(row['documento_proveedor'] == '71000001' for row in recent)
    finally:
        persist(f'actor-{frame}-{cardinality}-{name}-{query[:28]}', flow.audit_trace)
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('pending', [False, True])
@pytest.mark.parametrize('seed', SEEDS, ids=['cartagena', 'barranquilla'])
def test_new_search_after_actor(tmp_path, monkeypatch, frame, pending, seed):
    query, mention, buyer, amount = seed
    rows = [alias('FUNDACARIBE'), alias(mention, '71000009')]
    if pending:
        rows.append(alias('FUNDACARIBE', '71000002'))
    index = index_at(tmp_path, rows)
    traces = []
    for text in ('contratos del proveedor FUNDACARIBE', query, 'solo 2026', 'muéstrame más'):
        flow = workflow(tmp_path, monkeypatch, index, frame)
        conn = attach_source(flow, monkeypatch, buyer, amount)
        result = say(flow, text)
        traces.append(flow.audit_trace)
        persist(f'new-{frame}-{pending}-{mention.split()[-1]}', traces)
        if text.startswith('contratos del proveedor'):
            key = 'actor_pending' if pending else 'actor_scope'
            assert result['resolved_params'].get(key), result
        else:
            assert_buyer(flow, result, buyer, amount)
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('text', ['solo 2026', 'con la alcaldia de cartagena'])
def test_dependent_actor_refinement(tmp_path, monkeypatch, frame, text):
    index = index_at(tmp_path, [alias('FUNDACARIBE')])
    traces = []
    for query in ('contratos del proveedor FUNDACARIBE', text):
        flow = workflow(tmp_path, monkeypatch, index, frame)
        conn = attach_source(flow, monkeypatch)
        result = say(flow, query)
        traces.append(flow.audit_trace)
        persist(f'dependent-{frame}-{text}', traces)
        assert result['resolved_params'].get('actor_scope', {}).get('base_number') == '71000001', result
        assert flow.audit_trace['sql'], result
        if query == text and text.startswith('con'):
            assert result['resolved_params'].get('entidad_resolved') == CARTAGENA, result
        conn.close()
