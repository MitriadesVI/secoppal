"""Hypothesis properties authored for this review."""
import sqlite3
import unicodedata

from hypothesis import given, settings, strategies as st

from app.core.contract_universe import (
    CELEBRATED_STATES, EXCLUDED_STATES, ContractUniverse, eligible, predicate,
)
from app.core.contract_universe_intent import request
from tests.contract_universe_sql_dates import register_dates

POSTERIOR = tuple(s for s in CELEBRATED_STATES if s != 'Aprobado')
ANY_STATE = (*CELEBRATED_STATES, *EXCLUDED_STATES, 'ESTADO NUEVO', '', 'Firmado')
DATE_TEXT = st.one_of(
    st.none(),
    st.text(max_size=24),
    st.sampled_from(['2026-03-01T00:00:00.000', '2020-07-15', '2026-02-30', '   ']),
)


@settings(max_examples=60, derandomize=True, deadline=None)
@given(st.sampled_from(POSTERIOR), DATE_TEXT)
def test_posterior_states_never_depend_on_the_date(state, date_value):
    row = {'estado_contrato': state, 'fecha_de_firma': date_value}
    before = dict(row)
    assert eligible(row) is True
    assert row == before                      # the classifier never mutates the row


@settings(max_examples=60, derandomize=True, deadline=None)
@given(st.sampled_from(EXCLUDED_STATES), DATE_TEXT)
def test_no_date_ever_rescues_an_excluded_state(state, date_value):
    assert eligible({'estado_contrato': state, 'fecha_de_firma': date_value}) is False


@settings(max_examples=80, derandomize=True, deadline=None)
@given(st.sampled_from(ANY_STATE), DATE_TEXT)
def test_python_and_sql_never_disagree(state, date_value):
    conn = sqlite3.connect(':memory:')
    try:
        register_dates(conn)
        conn.create_function('unaccent', 1, lambda x: None if x is None else ''.join(
            c for c in unicodedata.normalize('NFD', x) if not unicodedata.combining(c)))
        conn.create_function('upper', 1, lambda x: x.upper() if x is not None else None)
        conn.create_function('trim', 1, lambda x: x.strip() if x is not None else None)
        conn.execute('CREATE TABLE c (estado_contrato TEXT, fecha_de_firma TEXT)')
        conn.execute('INSERT INTO c VALUES (?,?)', (state, date_value))
        rows = conn.execute('SELECT 1 FROM c WHERE ' + predicate()).fetchall()
        assert eligible({'estado_contrato': state, 'fecha_de_firma': date_value}) == bool(rows)
    finally:
        conn.close()


@settings(max_examples=40, derandomize=True, deadline=None)
@given(st.sampled_from(['servicios', 'obras', 'consultoria', 'suministro']),
       st.sampled_from(['cancelados', 'borradores', 'registros', 'aprobacion']))
def test_state_words_inside_an_object_never_widen_the_universe(topic, word):
    text = f'contratos de elaboración de {word} de {topic} de la Alcaldía de Cartagena'
    intent = request(text)
    assert intent is None or intent.universe is ContractUniverse.CELEBRATED, (text, intent)


@settings(max_examples=40, derandomize=True, deadline=None)
@given(st.sampled_from(['CANCELADOS', 'BORRADORES', 'REGISTROS', 'APROBACION']))
def test_state_words_inside_a_protected_actor_name_are_identity(word):
    surface = f'FUNDACION {word}'
    intent = request(f'qué ha contratado {surface}', protected_surfaces=(surface,))
    assert intent is None or intent.universe is ContractUniverse.CELEBRATED, (surface, intent)
