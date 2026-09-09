"""F2.1 — representable=True obliga a conservar TODAS las condiciones.

Pertenecer al mismo universo no basta: el predicado de universo celebrado ya
lleva los siete estados posteriores, de modo que buscar el literal en el SoQL
completo daría un verde falso. Aquí el literal debe aparecer en un filtro
`estado_contrato` propio, y el predicado de universo se comprueba aparte.

La referencia de cada condición es ella misma ejecutada sola: si el runtime no
sabe filtrar una palabra por su cuenta —hueco léxico preexistente de
`estado_families`, ajeno a esta remediación— tampoco se le exige a la
composición. Lo que sí se exige es que componer no degrade lo que el caso
simple conservaba.
"""
import itertools

import pytest

from app.core.contract_conditions import (
    CELEBRATED_UNIVERSE, denotes, read_conditions,
)
from app.core.entity_profile.text_offsets import normalize_with_offsets
from app.core.source_occurrence import source_request_occurrences
from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import setup

CARTAGENA, MIN = SEEDS[0][2], SEEDS[0][3]
TAIL = 'de la Alcaldía de Cartagena'
UNIVERSE_MARK = 'unaccent(trim(estado_contrato))'
WORDS = ['celebrados', 'firmados', 'suscritos', 'en ejecucion', 'cerrados',
         'modificados', 'terminados', 'cedidos', 'suspendidos', 'prorrogados',
         'aprobados', 'cancelados', 'borradores', 'en borrador',
         'enviados al proveedor', 'en aprobacion']


def _composition(text):
    normalized = normalize_with_offsets(text)
    return read_conditions(normalized.text,
                           occurrences=source_request_occurrences(normalized.text))


def _run(tmp_path, monkeypatch, text):
    """Ejecuta el turno y devuelve lo observable del filtro, no los params."""
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        sqls = [call['soql'] for call in flow.audit_trace['sql']]
        state_filter = ' '.join(
            fragment for sql in sqls for fragment in sql.split(' AND ')
            if 'estado_contrato' in fragment and UNIVERSE_MARK not in fragment)
        return {
            'clarifies': bool(result.get('needs_clarification')),
            'sql_calls': len(sqls),
            'state_filter': state_filter,
            'universe_predicate': any(UNIVERSE_MARK in sql for sql in sqls),
        }
    finally:
        conn.close()


def _holds(observed, value):
    if value is CELEBRATED_UNIVERSE:
        return observed['universe_predicate']
    return value.upper() in observed['state_filter'].upper()


REPRESENTABLE_PAIRS = [
    (a, b) for a, b in itertools.permutations(WORDS, 2)
    if (composition := _composition(f'contratos {a} y {b} {TAIL}')).representable
    and len(composition.conditions) > 1
]
UNSUPPORTED_PAIRS = [
    (a, b) for a, b in itertools.permutations(WORDS, 2)
    if not _composition(f'contratos {a} y {b} {TAIL}').representable
]


# La referencia se calcula con el mismo `monkeypatch` de función que exige la
# guarda de persistencia del repo, y se cachea para no repetir 16 turnos por caso.
_ALONE: dict[str, dict] = {}


def reference(tmp_path, monkeypatch, word):
    """Qué conserva esa condición ejecutada sola."""
    if word not in _ALONE:
        _ALONE[word] = _run(tmp_path / f'alone-{abs(hash(word))}', monkeypatch,
                            f'contratos {word} {TAIL}')
    return _ALONE[word]


def test_the_reference_run_is_not_vacuous(tmp_path, monkeypatch):
    """Al menos ocho condiciones se conservan solas: si no, la propiedad sería hueca."""
    kept = [word for word in WORDS
            if _holds(reference(tmp_path, monkeypatch, word), denotes(word))]
    assert len(kept) >= 8, kept


@pytest.mark.parametrize('first,second', REPRESENTABLE_PAIRS)
def test_representable_keeps_every_condition(tmp_path, monkeypatch, first, second):
    text = f'contratos {first} y {second} {TAIL}'
    observed = _run(tmp_path, monkeypatch, text)
    if observed['clarifies']:
        assert observed['sql_calls'] == 0, observed      # nunca media ejecución
        return
    for word in (first, second):
        value = denotes(word)
        if not _holds(reference(tmp_path, monkeypatch, word), value):
            continue                                     # hueco léxico preexistente
        assert _holds(observed, value), (text, value, observed)


@pytest.mark.parametrize('first,second', UNSUPPORTED_PAIRS)
def test_unsupported_never_executes(tmp_path, monkeypatch, first, second):
    observed = _run(tmp_path, monkeypatch, f'contratos {first} y {second} {TAIL}')
    assert observed['clarifies'], (first, second, observed)
    assert observed['sql_calls'] == 0, (first, second, observed)


@pytest.mark.parametrize('first,second', [
    ('suspendidos', 'aprobados'), ('aprobados', 'suspendidos'),
    ('terminados', 'cedidos'), ('cedidos', 'terminados'),
    ('cerrados', 'prorrogados'), ('prorrogados', 'cerrados'),
])
def test_two_admitted_states_clarify(tmp_path, monkeypatch, first, second):
    """Los seis casos del probe: dos estados admitidos distintos no se ejecutan."""
    text = f'contratos {first} y {second} {TAIL}'
    observed = _run(tmp_path, monkeypatch, text)
    assert observed['clarifies'], observed
    assert observed['sql_calls'] == 0, observed
    assert observed['state_filter'] == '', observed


def test_the_universe_alone_is_not_a_filter():
    """El predicado de universo lleva los siete posteriores: no acredita un filtro."""
    from app.core.contract_universe import predicate
    for state in ('CERRADO', 'TERMINADO', 'CEDIDO', 'SUSPENDIDO', 'PRORROGADO'):
        assert state in predicate().upper()
