"""INV-SYSTEM-METADATA-DOES-NOT-CHANGE-SPEECH-ACT."""
from itertools import combinations

import pytest
from hypothesis import given, settings, strategies as st

from app.core.actor_pending_document import match_pending_document
from app.core.contract_universe import POLICY_VERSION
from app.core.query_router import QueryRouter

NEUTRAL = {'contract_universe': 'celebrated', 'contract_universe_version': POLICY_VERSION}
SUBSETS = [dict(items) for n in range(3) for items in combinations(NEUTRAL.items(), n)]
INTENT = [
    ('contract_universe', 'source_records'), ('contract_universe_error', 'clarify'),
    ('contract_universe_version', 'unknown'), ('dataset_explicit', True),
    ('fecha_desde', '2024-01-01'), ('fecha_hasta', '2024-12-31'),
    ('estado_contrato', ['Cancelado']), ('estado', ['Activo']),
    ('valor_min', 100), ('valor_max', 200), ('entidad', ['Buyer']),
    ('objeto', 'alimentos'), ('modalidad', ['licitacion']),
    ('source_request_occurrences', ['explicit']),
]


def base(text):
    return {k: v for k, v in QueryRouter().parse(text).params.items() if k not in NEUTRAL}


@pytest.mark.parametrize('text,kind,number', [
    ('CC 70000003', 'CC', '70000003'),
    ('NIT 900429803', 'NIT', '900429803'),
    ('NIT 900429803-5', 'NIT', '900429803'),
])
@pytest.mark.parametrize('metadata', SUBSETS)
def test_default_metadata_preserves_identity(text, kind, number, metadata):
    candidates = [{'document_type': kind, 'base_number': number, 'rank': 1}]
    params = base(text)
    expected = match_pending_document(params, candidates)
    assert expected.outcome == 'MATCH'
    assert match_pending_document(params | metadata, candidates) == expected


@settings(derandomize=True, deadline=None, max_examples=80)
@given(metadata=st.sampled_from(SUBSETS), number=st.integers(10000000, 99999999),
       count=st.integers(0, 2))
def test_neutral_metadata_invariant(metadata, number, count):
    params = base(f'CC {number}')
    candidates = [{'document_type': 'CC', 'base_number': str(number), 'rank': i + 1}
                  for i in range(count)]
    expected = match_pending_document(params, candidates)
    assert expected.outcome == ['NO_MATCH', 'MATCH', 'INCOMPATIBLE'][count]
    assert match_pending_document(params | metadata, candidates) == expected


@pytest.mark.parametrize('constraint', INTENT)
@settings(derandomize=True, deadline=None)
@given(metadata=st.sampled_from(SUBSETS))
def test_intent_constraints_never_neutral(constraint, metadata):
    params = base('CC 70000003') | metadata | dict([constraint])
    candidates = [{'document_type': 'CC', 'base_number': '70000003', 'rank': 1}]
    assert match_pending_document(params, candidates).outcome == 'NO_DOCUMENT'


@pytest.mark.parametrize('text', [
    'CC 70000003 en 2024', 'procesos del CC 70000003',
    'contratos del CC 70000003', 'registros cancelados del CC 70000003',
    'todos los registros del dataset del CC 70000003',
    'contratos celebrados del CC 70000003',
])
def test_explicit_request_not_document_only(text):
    candidates = [{'document_type': 'CC', 'base_number': '70000003', 'rank': 1}]
    assert match_pending_document(QueryRouter().parse(text).params, candidates).outcome == 'NO_DOCUMENT'


def test_fresh_document_keeps_contract_universe():
    params = QueryRouter().parse('CC 70000003').params
    assert params['dataset'] == 'contratos'
    assert params['dataset_explicit'] is False
    assert params.items() >= NEUTRAL.items()
