"""Independent F2.1 final delta: executed conditions, residuals and catalog parity."""
import itertools
import json
from pathlib import Path

import pytest
from hypothesis import given, settings, strategies as st

from app.core.contract_conditions import CELEBRATED_UNIVERSE, _DENOTES, read_conditions
from app.core.contract_universe import CELEBRATED_STATES, EXCLUDED_STATES
from app.core.contract_universe_intent import request
from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import setup

OUT = Path('docs/audits/contract_universe_v1_1_f2_1_final/observations.jsonl')
ORIGINAL = [('celebrados', 'cancelados'), ('celebrados', 'borradores'),
            ('firmados', 'cancelados'), ('suscritos', 'cancelados'),
            ('suspendidos', 'cancelados'), ('aprobados', 'cancelados')]
ADMITTED = [('suspendidos', 'aprobados'), ('terminados', 'cedidos'),
            ('cerrados', 'prorrogados')]


def observe(tmp_path, monkeypatch, text):
    flow, conn = setup(tmp_path, monkeypatch, SEEDS[0][2], SEEDS[0][3], False)
    try:
        result = say(flow, text)
        params = result['resolved_params']
        sql = [c['soql'] for c in flow.audit_trace['sql']]
        evidence = dict(query=text, clarification=bool(result.get('needs_clarification')),
                        sql=sql, rows=len(result.get('results') or []),
                        params=params, states=sorted({r['estado_contrato']
                                                     for r in result.get('results') or []}))
        with OUT.open('a') as stream:
            stream.write(json.dumps(evidence, ensure_ascii=False, default=str) + '\n')
        return evidence
    finally:
        conn.close()


@pytest.mark.parametrize('a,b', ORIGINAL + ADMITTED)
def test_complete_clarification_both_orders(tmp_path, monkeypatch, a, b):
    noun = 'registros' if (a, b) in ORIGINAL else 'contratos'
    observations = []
    for i, (first, second) in enumerate(((a, b), (b, a))):
        e = observe(tmp_path / str(i), monkeypatch,
                    f'{noun} {first} y {second} de la Alcaldía de Cartagena')
        assert e['clarification'] and not e['sql'] and e['rows'] == 0, e
        assert not e['params'].get('objeto'), e
        observations.append((e['clarification'], len(e['sql']), e['rows']))
    assert observations[0] == observations[1]


@pytest.mark.parametrize('word,state', [('suspendidos', 'Suspendido'), ('cerrados', 'Cerrado')])
@pytest.mark.parametrize('reverse', [False, True])
def test_universe_plus_state_executes(tmp_path, monkeypatch, word, state, reverse):
    pair = (word, 'celebrados') if reverse else ('celebrados', word)
    e = observe(tmp_path, monkeypatch,
                f'contratos {pair[0]} y {pair[1]} de la Alcaldía de Cartagena')
    assert not e['clarification'] and e['sql'] and e['rows'] > 0, e
    assert e['params']['contract_universe'] == 'celebrated', e
    assert e['params']['estado_contrato'] == [state], e
    assert not e['params'].get('objeto'), e
    assert e['states'] == [state], e
    assert any('unaccent(trim(estado_contrato))' in sql for sql in e['sql']), e


def test_denotation_catalog_parity():
    actual = {value for _, value in _DENOTES} - {CELEBRATED_UNIVERSE}
    assert actual == set(CELEBRATED_STATES) | set(EXCLUDED_STATES)


@settings(max_examples=40, derandomize=True, deadline=None)
@given(st.sampled_from(['suspendidos', 'cerrados', 'cancelados', 'borradores']))
def test_repeated_denotation_has_no_spurious_conflict(word):
    text = f'registros {word} y {word} de la Alcaldía de Cartagena'
    assert read_conditions(text).representable
    intent = request(text)
    assert intent is None or not intent.conflict, (text, intent)


@pytest.mark.parametrize('a,b', list(itertools.permutations(['celebrados', 'prorrogados'], 2)))
def test_representable_composition_does_not_lose_prorrogado(tmp_path, monkeypatch, a, b):
    e = observe(tmp_path, monkeypatch, f'contratos {a} y {b} de la Alcaldía de Cartagena')
    if e['clarification']:
        assert not e['sql'] and not e['rows'], e
        return
    assert e['params'].get('estado_contrato') == ['Prorrogado'], e
    assert not e['params'].get('objeto'), e


def test_builder_transports_supplied_list_without_creating_authority():
    from app.core.soql_builder import SoQLBuilder
    clauses = []
    SoQLBuilder._add_estado_clause(clauses, 'estado_contrato', ['Suspendido', 'Aprobado'])
    assert clauses == ["estado_contrato IN ('Suspendido', 'Aprobado')"]


def test_duplicate_excluded_condition_executes_once(tmp_path, monkeypatch):
    e = observe(tmp_path, monkeypatch,
                'registros cancelados y cancelados de la Alcaldía de Cartagena')
    assert not e['clarification'], e
    assert e['sql'] and e['params']['estado_contrato'] == ['Cancelado'], e
