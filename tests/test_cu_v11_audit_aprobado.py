"""Independent Aprobado V1.1 matrix: Python eligible() vs the SQL predicate.

The oracle is written from POLICY_DELTA.md, not read back from the runtime:
  - Aprobado + usable temporal value  -> included
  - Aprobado without a usable value   -> indeterminate, not included
  - seven posterior states            -> included regardless of date
  - four excluded states              -> excluded even with a date
  - unknown / absent state            -> not included
"""
import sqlite3
import unicodedata

import pytest

from app.core.contract_universe import (
    CELEBRATED_STATES, EXCLUDED_STATES, ContractUniverse, eligible, predicate,
)
from tests.contract_universe_sql_dates import register_dates

POSTERIOR = ('En ejecución', 'Cerrado', 'Modificado', 'terminado', 'cedido',
             'Suspendido', 'Prorrogado')
EXCLUDED = ('Cancelado', 'Borrador', 'enviado Proveedor', 'En aprobación')
DATES = [
    ('2026-03-01T00:00:00.000', True),
    ('2020-07-15', True),
    ('2026-03-01T00:00:00.000-05:00', True),
    ('2026-02-30', False),
    ('2026-13-01', False),
    ('pendiente', False),
    ('', False),
    ('   ', False),
    (None, False),
]


def _connect():
    conn = sqlite3.connect(':memory:')
    conn.row_factory = sqlite3.Row
    register_dates(conn)
    conn.create_function('unaccent', 1, lambda x: None if x is None else ''.join(
        c for c in unicodedata.normalize('NFD', x) if not unicodedata.combining(c)))
    conn.create_function('upper', 1, lambda x: x.upper() if x is not None else None)
    conn.create_function('trim', 1, lambda x: x.strip() if x is not None else None)
    conn.execute('CREATE TABLE contracts (id TEXT, estado_contrato TEXT, fecha_de_firma TEXT)')
    return conn


def _sql_admits(conn, state, date_value):
    conn.execute('DELETE FROM contracts')
    conn.execute('INSERT INTO contracts VALUES (?,?,?)', ('R', state, date_value))
    rows = conn.execute('SELECT id FROM contracts WHERE ' + predicate()).fetchall()
    return len(rows) == 1


def test_policy_table_is_what_the_audit_expects():
    assert set(CELEBRATED_STATES) == set(POSTERIOR) | {'Aprobado'}
    assert set(EXCLUDED_STATES) == set(EXCLUDED)


@pytest.mark.parametrize('date_value,usable', DATES)
def test_aprobado_requires_a_usable_date(date_value, usable):
    assert eligible({'estado_contrato': 'Aprobado', 'fecha_de_firma': date_value}) is usable


@pytest.mark.parametrize('state', POSTERIOR)
@pytest.mark.parametrize('date_value,_usable', DATES)
def test_posterior_states_do_not_need_a_date(state, date_value, _usable):
    assert eligible({'estado_contrato': state, 'fecha_de_firma': date_value}) is True


@pytest.mark.parametrize('state', EXCLUDED)
@pytest.mark.parametrize('date_value,_usable', DATES)
def test_excluded_states_are_not_rescued_by_a_date(state, date_value, _usable):
    assert eligible({'estado_contrato': state, 'fecha_de_firma': date_value}) is False


@pytest.mark.parametrize('state', [None, '', 'ESTADO NUEVO', 'Firmado', 'Celebrado'])
@pytest.mark.parametrize('date_value,_usable', DATES)
def test_unknown_states_never_enter(state, date_value, _usable):
    assert eligible({'estado_contrato': state, 'fecha_de_firma': date_value}) is False


def test_source_records_admits_everything():
    for state in (*POSTERIOR, *EXCLUDED, 'Aprobado', None, 'ESTADO NUEVO'):
        assert eligible({'estado_contrato': state, 'fecha_de_firma': None},
                        ContractUniverse.SOURCE_RECORDS) is True


ALL_STATES = (*POSTERIOR, 'Aprobado', *EXCLUDED, None, '', 'ESTADO NUEVO', 'Firmado')


@pytest.mark.parametrize('state', ALL_STATES)
@pytest.mark.parametrize('date_value,_usable', DATES)
def test_python_and_sql_agree(state, date_value, _usable):
    """Parity: whatever eligible() decides, the SQL predicate must decide too."""
    conn = _connect()
    try:
        python_says = eligible({'estado_contrato': state, 'fecha_de_firma': date_value})
        sql_says = _sql_admits(conn, state, date_value)
        assert python_says == sql_says, (state, date_value, python_says, sql_says)
    finally:
        conn.close()


def test_predicate_shape_is_parenthesised_and_binds_correctly():
    text = predicate()
    assert text.startswith('(') and text.endswith(')')
    assert "= 'APROBADO' AND date_extract_y(fecha_de_firma) IS NOT NULL" in text
    assert "'APROBADO'" not in text.split(' IN (')[1]


def test_predicate_does_not_infer_a_year_or_a_signature():
    """The function is only used for presence, never to stamp a year."""
    assert 'date_extract_y' in predicate()
    assert predicate().count('date_extract_y') == 1


def test_aprobado_no_longer_infers_a_signature_without_a_date():
    """Signature inference by state alone stops for Aprobado, stays for the rest."""
    from app.core.truth_layer import resolve_contract_signature
    from tests.test_truth_layer_contract_signature import _signals

    assert resolve_contract_signature(_signals('Aprobado')).signature_status == 'unknown'
    assert resolve_contract_signature(_signals('Aprobado')).signature_date_status == 'missing'
    for state in ('En ejecución', 'Cerrado', 'terminado', 'Modificado', 'Prorrogado',
                  'cedido', 'Suspendido'):
        assert resolve_contract_signature(_signals(state)).signature_status != 'unknown', state
