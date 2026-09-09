"""SQL and pure eligibility parity; excluded insertions cannot perturb metrics."""
import sqlite3
import unicodedata
from hypothesis import given, settings, strategies as st
from app.core.contract_universe import (
    CELEBRATED_STATES, EXCLUDED_STATES, ContractUniverse, eligible, predicate,
)


def database(rows):
    conn = sqlite3.connect(':memory:')
    from tests.contract_universe_sql_dates import register_dates
    register_dates(conn)
    conn.create_function('unaccent', 1, lambda x: None if x is None else ''.join(
        c for c in unicodedata.normalize('NFD', x) if not unicodedata.combining(c)))
    conn.create_function('upper', 1, lambda x: x.upper() if x is not None else None)
    conn.execute('CREATE TABLE contracts (id TEXT, estado_contrato TEXT, fecha_de_firma TEXT, value INTEGER, buyer TEXT)')
    conn.executemany('INSERT INTO contracts VALUES (?,?,?,?,?)', rows)
    return conn


def snapshot(conn, universe=ContractUniverse.CELEBRATED):
    where = predicate(universe) or '1=1'
    return (
        conn.execute(f'SELECT id FROM contracts WHERE {where} ORDER BY id LIMIT 100').fetchall(),
        conn.execute(f'SELECT count(*),sum(value) FROM contracts WHERE {where}').fetchall(),
        conn.execute(f'SELECT buyer,count(*),sum(value) FROM contracts WHERE {where} GROUP BY buyer ORDER BY sum(value) DESC,buyer').fetchall(),
    )


@settings(max_examples=60, derandomize=True, deadline=None)
@given(st.lists(st.tuples(st.sampled_from((*EXCLUDED_STATES, None, '', 'Futuro', 'Firmado')),
                          st.one_of(st.none(), st.sampled_from(['', 'No Definido', '1900-01-01', '2026-03-01'])),
                          st.integers(min_value=0, max_value=10**15)), max_size=75))
def test_excluded_insertion_and_reordering_invariant(excluded):
    base = [(str(i), state, None, i + 1, str(i % 2)) for i, state in enumerate(CELEBRATED_STATES)]
    conn = database(base)
    expected = snapshot(conn)
    additions = [(f'E{i}', state, date, value, 'huge') for i, (state, date, value) in enumerate(excluded)]
    conn.executemany('INSERT INTO contracts VALUES (?,?,?,?,?)', reversed(additions))
    assert snapshot(conn) == expected
    inverse = snapshot(conn, ContractUniverse.SOURCE_RECORDS)
    assert inverse[1][0][0] == len(base) + len(additions)
    assert inverse[1][0][1] == sum(r[3] for r in base + additions)
    conn.close()


@settings(max_examples=100, derandomize=True, deadline=None)
@given(st.sampled_from((*CELEBRATED_STATES, *EXCLUDED_STATES, '', None, 'desconocido', 'Firmado')),
       st.sampled_from(['original', 'upper', 'lower', 'unaccent']), st.booleans(),
       st.one_of(st.none(), st.sampled_from(['', 'No Definido', '2026-03-01'])))
def test_classifier_sql_parity(state, variant, padded, date):
    if state is not None:
        if variant == 'upper':
            state = state.upper()
        elif variant == 'lower':
            state = state.lower()
        elif variant == 'unaccent':
            state = ''.join(c for c in unicodedata.normalize('NFD', state) if not unicodedata.combining(c))
        if padded:
            state = ' ' + state + ' '
    conn = database([('ID', state, date, 5, 'buyer')])
    for universe in ContractUniverse:
        assert bool(snapshot(conn, universe)[0]) == eligible({'estado_contrato': state, 'fecha_de_firma': date}, universe)
    conn.close()


@settings(max_examples=30, derandomize=True)
@given(st.sampled_from([None, '', 'No Definido', 'not-a-date', '2026-02-30']))
def test_absent_or_unusable_signature_does_not_mean_pending_publication(date):
    from app.core.formatter import Formatter
    row = {'estado_contrato': 'terminado', 'fecha_de_firma': date}
    assert eligible(row)
    assert Formatter._best_date_for_row(row, 'jbjy-vk9h') == ('', 'Fecha de firma no publicada')
    assert row['fecha_de_firma'] == date
    row['fecha_de_inicio_del_contrato'] = '2026-03-01'
    assert Formatter._best_date_for_row(row, 'jbjy-vk9h') == ('2026-03-01', 'Inicio')
