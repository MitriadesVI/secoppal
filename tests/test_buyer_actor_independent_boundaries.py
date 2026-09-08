"""Independent nearby grammar, provenance, negation and property checks."""
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

from burr.core import State
from hypothesis import given, settings, strategies as st
import pytest

from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.actions.actor_resolution import resolve_actor_name
from app.data.actor_alias_resolver import ActorNameResolver
from tests.buyer_actor_independent_support import (
    SEEDS, CARTAGENA, alias, index_at, workflow, attach_source, say, persist, assert_buyer,
)


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('query,mention,buyer', [
    ('CONTRATOS DE LA ALCALDÍA DE CARTAGENA por más de 500 millones', 'alcaldia de cartagena', CARTAGENA),
    ('qué ha contratado la alcaldía de Cartagena por mas de 500 millones', 'alcaldia de cartagena', CARTAGENA),
    ('contratos de software de alcaldia de cartagena en 2026 por mas de 500 millones', 'alcaldia de cartagena', CARTAGENA),
    ('contratos de alcaldia de cartagena por mas de 500 millones por mayor valor', 'alcaldia de cartagena', CARTAGENA),
    ('qué ha contratado alcaldia de galapa por mas de 500 millones', 'alcaldia de galapa', 'MUNICIPIO DE GALAPA.'),
    ('contratos de alcaldia de baranoa por mas de 500 millones', 'alcaldia de baranoa', 'ALCALDIA MUNICIPAL DE BARANOA'),
])
def test_variants(tmp_path, monkeypatch, frame, query, mention, buyer):
    index = index_at(tmp_path, [alias(mention), alias(mention, '71000002')])
    flow = workflow(tmp_path, monkeypatch, index, frame)
    conn = attach_source(flow, monkeypatch, buyer)
    try:
        result = say(flow, query)
        assert_buyer(flow, result, buyer, 500_000_000)
        if 'software' in query:
            assert result['resolved_params']['objeto'] == ['software']
            assert result['resolved_params']['fecha_desde'] == '2026-01-01'
            assert 'software' in result['soql_query'].lower()
        if 'mayor valor' in query:
            assert result['resolved_params']['ordering_signal'] == 'valor_desc'
    finally:
        persist(f'variant-{frame}-{mention}-{query[:30]}', flow.audit_trace)
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('name', ['alcaldia de cartagena servicios integrados', 'UNIVERSIDAD SOLANO'])
def test_partial_or_fuzzy_buyer_cannot_veto_whole_mention(tmp_path, monkeypatch, frame, name):
    index = index_at(tmp_path, [alias(name)])
    flow = workflow(tmp_path, monkeypatch, index, frame)
    conn = attach_source(flow, monkeypatch, supplier=name)
    try:
        result = say(flow, 'contratos de ' + name)
        p = result['resolved_params']
        assert p.get('actor_intent_conflict') or p.get('actor_scope'), result
        if p.get('actor_intent_conflict'):
            assert result.get('needs_clarification') and not flow.audit_trace['sql'], result
        else:
            assert p['actor_scope'].get('base_number') == '71000001', result
            assert flow.audit_trace['sql'], result
    finally:
        persist(f'provenance-{frame}-{name}', flow.audit_trace)
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('query,mention,kind', [
    ('contratos de bogota', 'bogota', 'geography'),
    ('contratos de software', 'software', 'topic'),
    ('contratos no de alcaldia de cartagena', 'alcaldia de cartagena', 'negation'),
    ('contratos donde el proveedor no sea FUNDACARIBE', 'FUNDACARIBE', 'negation'),
])
def test_geography_topic_negation(tmp_path, monkeypatch, frame, query, mention, kind):
    index = index_at(tmp_path, [alias(mention)])
    flow = workflow(tmp_path, monkeypatch, index, frame)
    conn = attach_source(flow, monkeypatch)
    try:
        result = say(flow, query)
        p = result['resolved_params']
        if kind == 'geography':
            assert not p.get('actor_scope') and not p.get('entidad_resolved'), result
            assert p.get('departamento_resolved') == 'Distrito Capital de Bogotá', result
        else:
            assert result.get('needs_clarification'), result
            assert not flow.audit_trace['sql'], result
    finally:
        persist(f'boundary-{frame}-{kind}-{mention}', flow.audit_trace)
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_short_negation_characterization(tmp_path, monkeypatch, frame):
    """Record an incorrect inherited behavior, NOT an acceptance test."""
    index = index_at(tmp_path, [alias('FUNDACARIBE')])
    flow = workflow(tmp_path, monkeypatch, index, frame)
    conn = attach_source(flow, monkeypatch)
    try:
        result = say(flow, 'contratos sin proveedor FUNDACARIBE')
        assert result['resolved_params'].get('objeto_exclude') == ['fundacaribe'], result
        assert flow.audit_trace['sql'], result
        for call in flow.audit_trace['sql']:
            where = call['soql'].split(' WHERE ', 1)[-1].lower()
            assert 'not (' in where and 'fundacaribe' in where and 'objeto_del_contrato' in where, where
            assert 'documento_proveedor' not in where, where
        assert any(row['proveedor_adjudicado'] == 'FUNDACARIBE' for row in result['results'])
    finally:
        persist(f'short-negation-{frame}', flow.audit_trace)
        conn.close()


@settings(max_examples=35, derandomize=True, deadline=None)
@given(kinds=st.lists(st.sampled_from(['CC', 'NIT']), max_size=5),
       reverse=st.booleans(), frequency=st.integers(1, 900))
def test_positive_buyer_index_invariance(kinds, reverse, frequency):
    for query, mention, buyer, minimum in SEEDS:
        rows = [alias(mention, str(71000001 + i), kind, frequency) for i, kind in enumerate(kinds)]
        rows.append(alias('UNRELATED AUDIT', '72000001'))
        if reverse:
            rows.reverse()
        with TemporaryDirectory(prefix='buyer-independent-property-') as temporary:
            index = index_at(Path(temporary), rows)
            params = QueryRouter().parse(query, today=date(2026, 9, 8)).params
            final = resolve_actor_name(State({'parsed_params': params, 'raw_query': query}),
                                       ActorNameResolver(index))['parsed_params']
            assert final.get('dataset') == 'contratos'
            assert final.get('entidad_resolved') == buyer
            assert final.get('valor_min') == minimum
            assert not any(final.get(k) for k in ('actor_scope', 'actor_pending', 'contratista'))
            sql = SoQLBuilder().build('jbjy-vk9h', final).split(' WHERE ', 1)[-1]
            assert buyer in sql and f'valor_del_contrato >= {minimum}' in sql
            assert 'documento_proveedor' not in sql.lower()


@pytest.mark.parametrize('frame', [False, True])
def test_city_autopromotion_does_not_suppress_actor(tmp_path, monkeypatch, frame):
    index = index_at(tmp_path, [alias('barranquilla')])
    flow = workflow(tmp_path, monkeypatch, index, frame)
    conn = attach_source(flow, monkeypatch, supplier='barranquilla')
    try:
        bare = 'contratos de barranquilla'
        initial = QueryRouter().parse(bare).params
        assert initial['entidad_resolution']['method'] == 'city_to_entity'
        geography = say(flow, bare, chat='bare-city')
        assert not geography['resolved_params'].get('actor_scope'), geography
        assert not geography['resolved_params'].get('entidad_resolved'), geography
        persist(f'city-promoted-geography-{frame}', flow.audit_trace)
        flow.audit_trace['sql'].clear()
        result = say(flow, 'qué ha contratado barranquilla')
        assert result['resolved_params'].get('actor_scope', {}).get('base_number') == '71000001'
        assert not result['resolved_params'].get('entidad_resolved'), result
        assert flow.audit_trace['sql']
        assert all('71000001' in call['soql'].split(' WHERE ', 1)[-1] for call in flow.audit_trace['sql'])
    finally:
        persist(f'city-autopromotion-{frame}', flow.audit_trace)
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_explicit_institution_profile_preserves_owner(tmp_path, monkeypatch, frame):
    index = index_at(tmp_path, [alias('FUNDACARIBE')])
    traces = []
    for query in ('contratos del proveedor FUNDACARIBE',
                  'analiza contratos de alcaldia de cartagena en 2026'):
        flow = workflow(tmp_path, monkeypatch, index, frame)
        conn = attach_source(flow, monkeypatch)
        try:
            result = say(flow, query)
            traces.append(flow.audit_trace)
            if query.startswith('analiza'):
                assert result.get('analytical_intent') == 'entity_profile', result
                assert not result.get('needs_clarification'), result
                assert flow.audit_trace['sql'], result
                assert CARTAGENA in result['response'], result
                assert all('71000001' not in call['soql'] for call in flow.audit_trace['sql'])
                if frame:
                    assert result['continuation']['owner'] == 'ENTITY_PROFILE', result
        finally:
            persist(f'profile-after-actor-{frame}', traces)
            conn.close()
