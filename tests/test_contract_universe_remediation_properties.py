"""V1.1 adjudication and bounded occurrence invariants; no external I/O."""
import itertools
from dataclasses import replace

import pytest
from hypothesis import given, settings, strategies as st

from app.core.contract_universe import POLICY_VERSION, eligible, compatible
from app.core.contract_universe_intent import apply_request, request
from tests.buyer_actor_independent_support import SEEDS, alias, say
from tests.contract_universe_independent_support import DESCRIPTION, setup
from tests.test_contract_universe_properties import database, snapshot


@pytest.mark.parametrize('value,expected', [
    ('2026-03-01T00:00:00.000', True), ('2024-02-29', True),
    (None, False), ('', False), ('No Definido', False), ('No observado', False),
    ('2026-02-30', False), ('2026-13-01', False), ({'presence': 'not_observed'}, False),
])
def test_aprobado_date_adjudication(value, expected):
    row = dict(estado_contrato='Aprobado', fecha_de_firma=value)
    assert eligible(row) is expected
    if not isinstance(value, dict):
        conn = database([('A', 'Aprobado', value, 1, 'buyer')])
        try:
            assert bool(snapshot(conn)[0]) is expected
        finally:
            conn.close()


@settings(max_examples=70, derandomize=True, deadline=None)
@given(st.sampled_from(['cancelado', 'borrador', 'borradores', 'cancelados']),
       st.text(alphabet='abcdefghijklmnpqrstuvwxyz', min_size=2, max_size=20))
def test_words_inside_names_and_objects_do_not_authorize(word, name):
    for text in (f'qué ha contratado FUNDACION {word} {name}',
                 f'contratos de elaboración de {word} {name} de documentos de la Alcaldía de Cartagena'):
        assert request(text) is None
        result = apply_request(dict(dataset='contratos'), request(text))
        assert result['contract_universe'] == 'celebrated'


@settings(max_examples=12, derandomize=True, deadline=None)
@given(st.sampled_from(list(itertools.permutations(['cancelados', 'borradores']))),
       st.sampled_from(['registros', 'contratos']))
def test_two_state_permutations_never_execute_a_partial_state(states, noun):
    outcomes = [request(f'{noun} {a} y {b} de la Alcaldía de Cartagena')
                for a, b in (states, states[::-1])]
    assert all(o is not None and o.conflict and o.state is None for o in outcomes)
    assert outcomes[0].universe == outcomes[1].universe


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('text', [
    'registros cancelados y borradores de la Alcaldía de Cartagena',
    'registros borradores y cancelados de la Alcaldía de Cartagena',
    'registros cancelados y registros borradores de la Alcaldía de Cartagena',
    'contratos celebrados y registros cancelados de la Alcaldía de Cartagena',
    'registros cancelados y contratos celebrados de la Alcaldía de Cartagena',
    'analiza procesos y registros cancelados de alcaldia de cartagena por mas de 500 millones',
    'analiza registros cancelados y procesos de alcaldia de cartagena por mas de 500 millones',
    'analiza todos los registros del dataset y procesos de alcaldia de cartagena por mas de 500 millones',
])
def test_compositions_clarify_without_any_child_sql(tmp_path, monkeypatch, frame, text):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame)
    try:
        result = say(flow, text)
        assert result.get('needs_clarification'), result
        assert not flow.audit_trace['sql']
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('text', [
    'contratos de elaboración de borradores de documentos de la Alcaldía de Cartagena',
    'analiza contratos de elaboración de borradores de documentos de la Alcaldía de Cartagena',
])
def test_object_preserved_all_profile_paths(tmp_path, monkeypatch, frame, text):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame)
    try:
        result = say(flow, text)
        assert not result.get('needs_clarification'), result
        assert result['resolved_params'].get('contract_universe', 'celebrated') == 'celebrated'
        assert any('borrador' in sql['soql'] for sql in flow.audit_trace['sql'])
        assert DESCRIPTION in result['response']
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_search_source_string_and_names_remain_exact(tmp_path, monkeypatch, frame):
    name = 'FUNDACION BORRADORES'
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame, aliases=[alias(name)])
    try:
        result = say(flow, 'registros en borrador de la Alcaldía de Cartagena')
        assert not result.get('needs_clarification'), result
        assert result['resolved_params']['estado_contrato'] == ['Borrador']
        assert result['results'] and all(r['objeto_del_contrato'] == DESCRIPTION for r in result['results'])
        assert DESCRIPTION in result['response']
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('sources', ['procesos y contratos', 'contratos y procesos'])
def test_ordinary_dual_preserves_both_sources(tmp_path, monkeypatch, frame, sources):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame)
    try:
        result = say(flow, f'analiza {sources} de alcaldia de cartagena por mas de 500 millones')
        assert not result.get('needs_clarification'), result
        sql = flow.audit_trace['sql']
        assert {s['dataset'] for s in sql} == {'p6dx-8zbt', 'jbjy-vk9h'}
        assert all('estado_contrato' not in s['soql'] for s in sql if s['dataset'] == 'p6dx-8zbt')
        assert all('APROBADO' in s['soql'] for s in sql if s['dataset'] == 'jbjy-vk9h')
    finally:
        conn.close()


def test_f4_incompatible_materialized_family_clarifies():
    result = apply_request(dict(dataset='contratos', estado_family='en_evaluacion'), None)
    assert result['contract_universe'] == 'celebrated'
    assert result['contract_universe_error']


def test_v1_artifacts_keep_bytes_and_require_review():
    from app.core.entity_profile.frame import SourcePredicates
    from app.core.entity_profile.frame_values import SourceKind
    old = dict(dataset='contratos', contract_universe='source_records',
               contract_universe_version='contract-universe-1')
    before = dict(old)
    assert not compatible(old)
    with pytest.raises(ValueError, match='revisión'):
        apply_request(old, None)
    assert old == before
    current = SourcePredicates(source=SourceKind.CONTRACTS)
    assert current.contract_universe_version == POLICY_VERSION
    with pytest.raises(ValueError, match='revisión'):
        replace(current, contract_universe_version='contract-universe-1')


def test_unversioned_records_are_not_structured_authority():
    result = apply_request(dict(dataset='contratos', contract_universe='source_records'), None)
    assert result['contract_universe'] == 'celebrated'
    assert result['contract_universe_error']
    valid = apply_request(dict(dataset='contratos', contract_universe='source_records',
                               contract_universe_version=POLICY_VERSION), None)
    assert valid['contract_universe'] == 'source_records'
    assert 'contract_universe_error' not in valid


def test_recognized_buyer_surface_owns_its_internal_construction():
    text = 'contratos de REGISTROS CANCELADOS de Cartagena'
    assert request(text, protected_surfaces=('REGISTROS CANCELADOS',)) is None


@pytest.mark.parametrize('frame', [False, True])
def test_actor_owns_a_complete_record_phrase(tmp_path, monkeypatch, frame):
    name = 'REGISTROS CANCELADOS'
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame, aliases=[alias(name)])
    conn.execute('UPDATE contracts SET proveedor_adjudicado=?, documento_proveedor=?', (name, '71000001'))
    try:
        result = say(flow, f'qué ha contratado {name}')
        assert result['resolved_params']['actor_scope']['base_number'] == '71000001'
        assert result['resolved_params'].get('contract_universe', 'celebrated') == 'celebrated'
        assert result['total_count'] == 24
    finally:
        conn.close()
