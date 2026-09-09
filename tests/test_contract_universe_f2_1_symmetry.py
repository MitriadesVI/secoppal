"""F2.1 — la respuesta no puede depender del orden de las condiciones.

Cada par A/B se ejecuta en los dos órdenes a través del workflow completo, y se
compara el resultado observable: si aclara, cuántas consultas SQL se emitieron,
qué estado quedó filtrado y qué universo se estampó. Comparar sólo `params`
dejaría pasar exactamente el defecto que esto vigila — la mitad ejecutada.
"""
import pytest
from hypothesis import given, settings, strategies as st

from app.core.contract_conditions import read_conditions
from app.core.contract_universe_intent import request
from app.core.entity_profile.text_offsets import normalize_with_offsets
from app.core.source_occurrence import source_request_occurrences
from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import setup

CARTAGENA, MIN = SEEDS[0][2], SEEDS[0][3]
TAIL = 'de la Alcaldía de Cartagena'

# Una condición admitida/celebrada junto a una excluida, o dos excluidas: el
# adaptador transporta un estado dentro de un universo, así que ninguna de estas
# composiciones es representable y todas deben aclarar.
UNSUPPORTED_PAIRS = [
    ('celebrados', 'cancelados'),
    ('celebrados', 'borradores'),
    ('firmados', 'cancelados'),
    ('suscritos', 'cancelados'),
    ('suspendidos', 'cancelados'),
    ('aprobados', 'cancelados'),
    ('prorrogados', 'borradores'),
    ('terminados', 'cancelados'),
    ('cancelados', 'borradores'),
]


def observe(flow, text):
    """Lo que el usuario puede ver de un turno, no sólo los params."""
    result = say(flow, text)
    params = result['resolved_params']
    return {
        'clarifies': bool(result.get('needs_clarification')),
        'sql_calls': len(flow.audit_trace['sql']),
        'rows': len(result.get('results') or []),
        'universe': params.get('contract_universe'),
        'estado_contrato': tuple(params.get('estado_contrato') or ()),
        'states_returned': tuple(sorted(
            {r['estado_contrato'] for r in result.get('results', [])})),
    }


@pytest.mark.parametrize('first,second', UNSUPPORTED_PAIRS)
def test_unsupported_pair_clarifies_in_both_orders(tmp_path, monkeypatch, first, second):
    direct = f'registros {first} y {second} {TAIL}'
    inverse = f'registros {second} y {first} {TAIL}'
    seen = {}
    for label, text in (('direct', direct), ('inverse', inverse)):
        flow, conn = setup(tmp_path / label, monkeypatch, CARTAGENA, MIN, False)
        try:
            seen[label] = observe(flow, text)
        finally:
            conn.close()
    assert seen['direct'] == seen['inverse'], seen
    assert seen['direct']['clarifies'], seen['direct']
    assert seen['direct']['sql_calls'] == 0, seen['direct']
    assert seen['direct']['rows'] == 0, seen['direct']


@pytest.mark.parametrize('first,second', UNSUPPORTED_PAIRS)
def test_no_condition_is_dropped_from_the_span(first, second):
    """Ambas condiciones quedan dentro del control consumido, en los dos órdenes."""
    for text in (f'registros {first} y {second} {TAIL}',
                 f'registros {second} y {first} {TAIL}'):
        intent = request(text)
        assert intent is not None and intent.conflict, text
        control = text[slice(*intent.span)].lower()
        assert first[:6] in control, (text, control)
        assert second[:6] in control, (text, control)
        # El sustantivo de la petición se consume con ella: no queda de tema.
        assert control.startswith('registros'), (text, control)


@pytest.mark.parametrize('first,second', UNSUPPORTED_PAIRS)
def test_the_reader_is_order_blind(first, second):
    """La lectura de condiciones es simétrica antes de cualquier selección."""
    def conditions(text):
        normalized = normalize_with_offsets(text)
        composition = read_conditions(
            normalized.text, occurrences=source_request_occurrences(normalized.text))
        return (composition.representable,
                frozenset(c.surface for c in composition.conditions))

    assert (conditions(f'registros {first} y {second} {TAIL}')
            == conditions(f'registros {second} y {first} {TAIL}'))


SUPPORTED = [
    ('registros cancelados de la Alcaldía de Cartagena', 'source_records', 'Cancelado', 10),
    ('registros en borrador de la Alcaldía de Cartagena', 'source_records', 'Borrador', 10),
    ('registros enviados al proveedor de la Alcaldía de Cartagena',
     'source_records', 'enviado Proveedor', 10),
    ('contratos suspendidos de la Alcaldía de Cartagena', 'celebrated', 'Suspendido', 3),
]


@pytest.mark.parametrize('text,universe,state,total', SUPPORTED)
def test_single_condition_still_executes(tmp_path, monkeypatch, text, universe, state, total):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        assert not result.get('needs_clarification'), result['response']
        assert result['resolved_params'].get('contract_universe') == universe
        assert {r['estado_contrato'] for r in result['results']} == {state}
        assert result['total_count'] == total
    finally:
        conn.close()


@pytest.mark.parametrize('text,universe', [
    ('contratos celebrados de la Alcaldía de Cartagena', 'celebrated'),
    ('todos los contratos de la Alcaldía de Cartagena', 'celebrated'),
    ('todos los registros del dataset de la Alcaldía de Cartagena', 'source_records'),
])
def test_whole_universe_requests_still_execute(tmp_path, monkeypatch, text, universe):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        assert not result.get('needs_clarification'), result['response']
        assert result['resolved_params'].get('contract_universe') == universe
        assert flow.audit_trace['sql']
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
@pytest.mark.parametrize('text', [
    'contratos de elaboración de borradores de documentos de la Alcaldía de Cartagena',
    'contratos de custodia de registros cancelados de archivo de la Alcaldía de Cartagena',
    # Dos palabras de estado dentro del mismo objeto: sin propiedad de cláusula
    # se leerían como una composición y producirían una aclaración espuria.
    'contratos de custodia de borradores cancelados de archivo de la Alcaldía de Cartagena',
    'contratos de digitalización de borradores y actas canceladas de la Alcaldía de Cartagena',
])
def test_a_word_inside_the_object_is_not_a_second_condition(tmp_path, monkeypatch,
                                                            frame, text):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame)
    try:
        result = say(flow, text)
        assert not result.get('needs_clarification'), result['response']
        assert result['resolved_params'].get('contract_universe',
                                             'celebrated') == 'celebrated'
        assert not {r['estado_contrato'] for r in result.get('results', ())} & {
            'Borrador', 'Cancelado', 'enviado Proveedor', 'En aprobación'}
    finally:
        conn.close()


@pytest.mark.parametrize('frame', [False, True])
def test_a_word_inside_an_actor_name_is_not_a_second_condition(tmp_path, monkeypatch, frame):
    from tests.buyer_actor_independent_support import alias
    name = 'FUNDACION BORRADORES CANCELADOS'
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, frame, aliases=[alias(name)])
    conn.execute('UPDATE contracts SET proveedor_adjudicado=?, documento_proveedor=?',
                 (name, '71000001'))
    try:
        result = say(flow, f'qué ha contratado {name}')
        assert not result.get('needs_clarification'), result['response']
        assert result['resolved_params']['actor_scope']['base_number'] == '71000001'
        assert result['resolved_params'].get('contract_universe',
                                             'celebrated') == 'celebrated'
    finally:
        conn.close()


ADMITTED_WORDS = ['celebrados', 'firmados', 'suscritos', 'suspendidos', 'aprobados',
                  'prorrogados', 'cerrados', 'terminados', 'cedidos', 'modificados']
EXCLUDED_WORDS = ['cancelados', 'borradores', 'en borrador', 'enviados al proveedor',
                  'en aprobacion']


@settings(max_examples=120, derandomize=True, deadline=None)
@given(st.sampled_from(ADMITTED_WORDS + EXCLUDED_WORDS),
       st.sampled_from(ADMITTED_WORDS + EXCLUDED_WORDS))
def test_property_reading_two_conditions_is_order_blind(first, second):
    """Para todo par A/B: leer «A y B» y «B y A» da la misma composición."""
    def read(text):
        normalized = normalize_with_offsets(text)
        composition = read_conditions(
            normalized.text, occurrences=source_request_occurrences(normalized.text))
        return (composition.representable,
                frozenset(c.surface for c in composition.conditions))

    assert (read(f'registros {first} y {second} {TAIL}')
            == read(f'registros {second} y {first} {TAIL}'))


@settings(max_examples=120, derandomize=True, deadline=None)
@given(st.sampled_from(ADMITTED_WORDS), st.sampled_from(EXCLUDED_WORDS))
def test_property_admitted_plus_excluded_always_clarifies(admitted, excluded):
    """Cruzar la frontera del universo nunca se resuelve eligiendo una mitad."""
    for text in (f'registros {admitted} y {excluded} {TAIL}',
                 f'registros {excluded} y {admitted} {TAIL}'):
        intent = request(text)
        assert intent is not None, text
        assert intent.conflict, (text, intent)
        assert intent.state is None or intent.conflict, (text, intent)
