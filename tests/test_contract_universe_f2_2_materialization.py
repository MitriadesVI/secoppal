"""F2.2: every composed condition needs an effective producer and actual SQL."""
import itertools
import json
from pathlib import Path
import re
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings, strategies as st

from app.core.contract_conditions import CELEBRATED_UNIVERSE, _DENOTES, read_conditions
from app.core.contract_universe import CELEBRATED_STATES, EXCLUDED_STATES
from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import setup

OUT = Path('docs/audits/contract_universe_v1_1_f2_2/product_trace.jsonl')
TAIL = 'de la Alcaldía de Cartagena'
# Independent expected denotations. Neither side is derived from _DENOTES.
STATES = [('suspendidos', 'Suspendido'), ('cerrados', 'Cerrado'),
          ('terminados', 'terminado'), ('cedidos', 'cedido'),
          ('prorrogados', 'Prorrogado'), ('aprobados', 'Aprobado'),
          ('en ejecucion', 'En ejecución'), ('modificados', 'Modificado')]
SUPPORTED = STATES[:4]
REPEATED = [('registros', 'cancelados', 'Cancelado'),
            ('registros', 'borradores', 'Borrador'),
            ('contratos', 'suspendidos', 'Suspendido'),
            ('contratos', 'terminados', 'terminado')]
VARIANTS = [('registros', 'cancelados', 'cancelado', 'Cancelado'),
            ('registros', 'borradores', 'en borrador', 'Borrador'),
            ('contratos', 'suspendidos', 'suspendida', 'Suspendido'),
            ('contratos', 'terminados', 'terminado', 'terminado')]


def observe(root, monkeypatch, text, frame=False):
    flow, conn = setup(root, monkeypatch, SEEDS[0][2], SEEDS[0][3], frame)
    try:
        result = say(flow, text)
        sql = [c['soql'] for c in flow.audit_trace['sql']]
        filters = [fragment for query in sql for fragment in query.split(' AND ')
                   if 'estado_contrato' in fragment
                   and 'unaccent(trim(estado_contrato))' not in fragment]
        evidence = dict(query=text, clarification=bool(result.get('needs_clarification')),
                        sql=sql, state_filter=filters, rows=result.get('results') or [],
                        params=result['resolved_params'])
        with OUT.open('a') as stream:
            stream.write(json.dumps(evidence, ensure_ascii=False, default=str) + '\n')
        return evidence
    finally:
        conn.close()


def assert_clarifies(e):
    assert e['clarification'], e
    assert not e['sql'] and not e['rows'], e
    assert not e['params'].get('objeto'), e


def assert_effective(e, state):
    assert not e['clarification'] and e['sql'], e
    assert e['params'].get('estado_contrato') == [state], e
    assert not e['params'].get('objeto'), e
    # A literal in the universe predicate is explicitly insufficient.
    assert any(re.search(r"\bestado_contrato\s*=\s*'" + re.escape(state) + "'", f)
               for f in e['state_filter']), e
    assert e['rows'] and {r['estado_contrato'] for r in e['rows']} == {state}, e


@pytest.mark.parametrize('word', ['prorrogados', 'aprobados'])
@pytest.mark.parametrize('reverse', [False, True])
def test_unmaterializable_universe_state_clarifies(tmp_path, monkeypatch, word, reverse):
    words = [word, 'celebrados'] if reverse else ['celebrados', word]
    assert_clarifies(observe(tmp_path, monkeypatch, f"contratos {' y '.join(words)} {TAIL}"))


@pytest.mark.parametrize('word,state', SUPPORTED)
@pytest.mark.parametrize('reverse', [False, True])
def test_materializable_universe_state_executes(tmp_path, monkeypatch, word, state, reverse):
    words = [word, 'celebrados'] if reverse else ['celebrados', word]
    e = observe(tmp_path, monkeypatch, f"contratos {' y '.join(words)} {TAIL}")
    assert_effective(e, state)
    assert e['params']['contract_universe'] == 'celebrated'
    assert any('unaccent(trim(estado_contrato))' in sql for sql in e['sql'])


@pytest.mark.parametrize('noun,word,state', REPEATED)
def test_duplicate_equals_single_execution(tmp_path, monkeypatch, noun, word, state):
    single = observe(tmp_path / 'single', monkeypatch, f'{noun} {word} {TAIL}')
    repeated = observe(tmp_path / 'repeated', monkeypatch, f'{noun} {word} y {word} {TAIL}')
    assert_effective(single, state)
    assert_effective(repeated, state)
    assert repeated['sql'] == single['sql']
    assert repeated['rows'] == single['rows']
    assert repeated['params']['contract_universe'] == single['params']['contract_universe']
    assert len(repeated['sql']) == 1


@pytest.mark.parametrize('noun,a,b,state', VARIANTS)
def test_equivalent_surfaces_in_both_orders(tmp_path, monkeypatch, noun, a, b, state):
    seen = []
    for i, words in enumerate(((a, b), (b, a))):
        e = observe(tmp_path / str(i), monkeypatch, f"{noun} {' y '.join(words)} {TAIL}")
        assert_effective(e, state)
        seen.append(e)
    assert seen[0]['sql'] == seen[1]['sql']
    assert seen[0]['rows'] == seen[1]['rows']


@pytest.mark.parametrize('words', list(itertools.permutations(['cancelados', 'borradores'])))
def test_distinct_states_are_never_deduplicated(tmp_path, monkeypatch, words):
    assert_clarifies(observe(tmp_path, monkeypatch, f"registros {' y '.join(words)} {TAIL}"))


def test_denotation_table_equals_independent_policy_catalog():
    produced = {state for _, state in _DENOTES} - {CELEBRATED_UNIVERSE}
    assert produced == set(CELEBRATED_STATES) | set(EXCLUDED_STATES)


@pytest.mark.parametrize('resolution', [None, {'filters': {}},
    {'filters': {'estado_contrato': []}},
    {'filters': {'estado_del_procedimiento': ['Suspendido']}},
    {'filters': {'estado_contrato': ['Cerrado']}},
    {'filters': {'estado_contrato': ['Suspendido', 'Cerrado']}},
])
def test_absent_wrong_field_or_broad_authority_cannot_execute(tmp_path, monkeypatch, resolution):
    from app.core import estado_families
    original = estado_families.resolve_estado
    monkeypatch.setattr(estado_families, 'resolve_estado',
                        lambda token, dataset: resolution if token == 'suspendidos'
                        else original(token, dataset))
    assert_clarifies(observe(tmp_path, monkeypatch, f'contratos celebrados y suspendidos {TAIL}'))


@settings(max_examples=24, derandomize=True, deadline=None)
@given(st.sampled_from(VARIANTS), st.integers(min_value=2, max_value=4), st.booleans())
def test_property_repetition_preserves_single_filter(case, count, reverse):
    noun, a, b, state = case
    words = [a] * (count - 1) + [b]
    if reverse:
        words.reverse()
    with TemporaryDirectory() as root, pytest.MonkeyPatch.context() as patch:
        e = observe(Path(root), patch, f"{noun} {' y '.join(words)} {TAIL}")
        assert_effective(e, state)
        assert len(e['sql']) == 1


@settings(max_examples=40, derandomize=True, deadline=None)
@given(st.sampled_from(STATES), st.sampled_from(['celebrados', 'firmados', 'suscritos']),
       st.booleans())
def test_property_composition_executes_only_with_effective_filter(case, universe, reverse):
    word, state = case
    words = [word, universe] if reverse else [universe, word]
    text = f"contratos {' y '.join(words)} {TAIL}"
    assert read_conditions(text).representable
    with TemporaryDirectory() as root, pytest.MonkeyPatch.context() as patch:
        e = observe(Path(root), patch, text)
        if e['clarification']:
            assert_clarifies(e)
        else:
            assert_effective(e, state)


def test_composition_control_cannot_erase_an_intervening_object(tmp_path, monkeypatch):
    e = observe(tmp_path, monkeypatch,
                f'contratos suspendidos de mantenimiento y suspendidos {TAIL}')
    if e['clarification']:
        assert not e['sql'] and not e['rows']
    else:
        assert 'mantenimiento' in ' '.join(e['params'].get('objeto') or []), e
        assert any('mantenimiento' in sql.lower() for sql in e['sql']), e


@pytest.mark.parametrize('word', ['aprobados', 'prorrogados'])
def test_contract_gate_does_not_claim_a_process_only_composition(word):
    from app.core.contract_universe_intent import request
    assert request(f'procesos {word} y {word} {TAIL}') is None
