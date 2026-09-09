"""Reproduced non-compliances of Contract Universe V1.1. These stay RED.

No xfail, skip or deselect: the order asked for reproduced failures, not for a
green batch. Each test states the product behaviour a user would observe.
"""
import pytest

from app.core.contract_universe_intent import request
from tests.buyer_actor_independent_support import SEEDS, say
from tests.contract_universe_independent_support import setup

CARTAGENA, MIN = SEEDS[0][2], SEEDS[0][3]


# F2 residual: an admitted/celebrated state placed BEFORE an excluded one is
# silently dropped and the excluded half executes. The reverse order clarifies.
@pytest.mark.parametrize('text', [
    'registros celebrados y cancelados de la Alcaldía de Cartagena',
    'registros celebrados y borradores de la Alcaldía de Cartagena',
    'registros firmados y cancelados de la Alcaldía de Cartagena',
    'registros suscritos y cancelados de la Alcaldía de Cartagena',
    'registros suspendidos y cancelados de la Alcaldía de Cartagena',
    'registros aprobados y cancelados de la Alcaldía de Cartagena',
])
def test_finding_first_state_wins_when_admitted_state_comes_first(tmp_path, monkeypatch, text):
    flow, conn = setup(tmp_path, monkeypatch, CARTAGENA, MIN, False)
    try:
        result = say(flow, text)
        assert result.get('needs_clarification'), (
            'executed half a composition: universe='
            f"{result['resolved_params'].get('contract_universe')} "
            f"estado={result['resolved_params'].get('estado_contrato')} "
            f"sql_calls={len(flow.audit_trace['sql'])}")
        assert not flow.audit_trace['sql']
    finally:
        conn.close()


@pytest.mark.parametrize('text', [
    'registros celebrados y cancelados de la Alcaldía de Cartagena',
    'registros suspendidos y cancelados de la Alcaldía de Cartagena',
])
def test_finding_order_symmetry_of_the_universe_request(text):
    """Reversing the two states must not change whether we clarify."""
    reversed_text = text.replace(' y ', ' Y_TMP ')
    left, right = reversed_text.split(' Y_TMP ')
    first = left.split(' ', 1)[1]
    noun = left.split(' ', 1)[0]
    tail = right.split(' de la ', 1)[1]
    second = right.split(' de la ', 1)[0]
    inverse = f'{noun} {second} y {first} de la {tail}'
    direct_intent = request(text)
    inverse_intent = request(inverse)
    assert direct_intent.conflict == inverse_intent.conflict, (
        f'{text!r} -> conflict={direct_intent.conflict}; '
        f'{inverse!r} -> conflict={inverse_intent.conflict}')
