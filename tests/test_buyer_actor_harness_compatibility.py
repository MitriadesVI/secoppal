"""R2B: field-owned oracle, dialect fidelity, and unchanged synthetic controls."""
import sqlite3
from types import SimpleNamespace

import pytest
from hypothesis import given, settings, strategies as st

from app.core.contract_universe import eligible, predicate
from app.core.soql_builder import SoQLBuilder
from tests.buyer_actor_object_predicates import assert_no_request_language_in_object_predicates
from tests.buyer_actor_sql_dialect import unaccent
from tests.contract_universe_sql_dates import register_dates
from tests.test_buyer_actor_role_arbitration_v1 import source_for


@pytest.mark.parametrize('value,expected', [('En ejecución', 'En ejecucion'), (None, None)])
def test_sqlite_unaccent(value, expected):
    with sqlite3.connect(':memory:') as conn:
        conn.create_function('unaccent', 1, unaccent)
        assert conn.execute('SELECT unaccent(?)', (value,)).fetchone()[0] == expected


@pytest.mark.parametrize('condition', [
    "nombre_entidad = 'ALCALDIA MUNICIPAL DE BARANOA' AND valor_del_contrato >= 500000000",
    "objeto_del_contrato LIKE '%software%' AND nombre_entidad = 'CONTRATADO ALCALDIA'",
    "nombre_entidad = 'objeto_del_contrato LIKE ''%alcaldia%'''",
])
def test_buyer_literals_are_not_object_contamination(condition):
    assert_no_request_language_in_object_predicates('SELECT objeto_del_contrato WHERE ' + condition)


@pytest.mark.parametrize('condition', [
    "objeto_del_contrato LIKE '%alcaldia%'",
    "descripcion_del_proceso LIKE '%contratado%'",
    "unaccent(UPPER(objeto_del_contrato)) LIKE unaccent(UPPER('%alcaldía%'))",
    "coalesce(unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%contratado%')), false)",
    "descripcion_del_proceso = 'contratado'",
    "'alcaldia' = objeto_del_contrato",
    "objeto_del_contrato NOT LIKE '%alcaldia%'",
    "objeto_del_contrato LIKE '%O''Brien AND alcaldia%'",
])
def test_object_contamination_is_rejected(condition):
    with pytest.raises(AssertionError, match='request language as topic'):
        assert_no_request_language_in_object_predicates('SELECT * WHERE ' + condition)


@settings(max_examples=100, derandomize=True, deadline=None)
@given(st.lists(st.sampled_from([
    '(A AND B)', '(C OR (D AND E))', predicate(),
    "nombre_entidad = 'ALCALDIA MUNICIPAL DE BARANOA'",
    "nombre_entidad = 'AND OR contratado'", 'valor_del_contrato >= 500000000',
]), max_size=12), st.sampled_from(['AND', 'OR']), st.booleans(), st.booleans())
def test_boolean_siblings_do_not_change_field_ownership(siblings, conjunction, bad, before):
    target = "unaccent(UPPER(descripcion_del_proceso)) LIKE unaccent(UPPER('%" + (
        'contratado' if bad else 'software') + "%'))"
    clauses = [*siblings, target] if before else [target, *siblings]
    sql = 'SELECT * WHERE (' + f' {conjunction} '.join(clauses) + ')'
    if bad:
        with pytest.raises(AssertionError, match='request language as topic'):
            assert_no_request_language_in_object_predicates(sql)
    else:
        assert_no_request_language_in_object_predicates(sql)


@pytest.mark.parametrize('term,bad', [('alcaldia', True), ('contratado', True), ('software', False)])
def test_real_compiler_object_wrappers(term, bad):
    sql = SoQLBuilder().build_count('jbjy-vk9h', {
        'entidad_resolved': 'ALCALDIA MUNICIPAL DE BARANOA', 'objeto': [term],
    })
    if bad:
        with pytest.raises(AssertionError, match='request language as topic'):
            assert_no_request_language_in_object_predicates(sql)
    else:
        assert_no_request_language_in_object_predicates(sql)


def test_fixture_eligibility_and_decoys(monkeypatch):
    original = sqlite3.connect
    connections = []

    def connect(*args, **kwargs):
        conn = original(*args, **kwargs)
        register_dates(conn)
        connections.append(conn)
        return conn

    monkeypatch.setattr(sqlite3, 'connect', connect)
    methods = ('query', 'aggregate', 'count', 'with_timeout', 'derived_for_timeout')
    flow = SimpleNamespace(secop_client=SimpleNamespace(**dict.fromkeys(methods)))
    source_for(flow, monkeypatch, {}, 'ALCALDIA MUNICIPAL DE BARANOA', 500000000)
    conn = connections[0]
    try:
        rows = [dict(row) for row in conn.execute('SELECT * FROM contracts ORDER BY id_contrato')]
        assert len(rows) == 62
        assert all(eligible(row) for row in rows)
        assert all(row['estado_contrato'] != 'Firmado' for row in rows)
        assert rows[60]['valor_del_contrato'] == 1
        assert rows[61]['nombre_entidad'] == 'DECOY BUYER'
        base = 'SELECT id_contrato FROM contracts WHERE ' + predicate()
        buyer = "nombre_entidad = 'ALCALDIA MUNICIPAL DE BARANOA'"
        amount = 'valor_del_contrato >= 500000000'
        ids = lambda sql: {row[0] for row in conn.execute(sql)}
        assert len(ids(base)) == 62  # The universe does not exclude either decoy.
        assert ids(base + ' AND ' + buyer + ' AND ' + amount) == {
            f'SYNTH-{n:03d}' for n in range(60)}
        assert ids(base + ' AND ' + buyer) - ids(base + ' AND ' + buyer + ' AND ' + amount) == {'SYNTH-060'}
        assert ids(base + ' AND ' + amount) - ids(base + ' AND ' + buyer + ' AND ' + amount) == {'SYNTH-061'}
    finally:
        conn.close()
