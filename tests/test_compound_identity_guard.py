"""M1A.2 predictions frozen before the fix, against both complete catalogs."""
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.municipal_territory import get_municipal_territories
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError
from scripts.audits.gate_divipola_buyer_matrix import (
    SNAPSHOT, buyer_oracle, classify, discovery, evaluate,
)
from scripts.audits.promote_municipal_m1a import encode, read


@pytest.fixture(scope='module', params=['baseline', 'candidate'])
def catalog(request, tmp_path_factory):
    candidate = request.param == 'candidate'
    records = read(SNAPSHOT / 'aliases_db.json.gz' if candidate else Path('app/data/aliases_db.json'))
    sidecar = read(SNAPSHOT / 'buyer_source_names.json.gz' if candidate else Path('app/data/buyer_source_names.json'))
    path = tmp_path_factory.mktemp('identity') / 'aliases.json'
    path.write_bytes(encode(records))
    return QueryRouter(EntityResolver(path)), sidecar, buyer_oracle(records, sidecar, candidate)


@pytest.mark.parametrize('town', [
    'San Andrés de Sotavento', 'Villa de Leyva', 'San Andrés de Cuerquía',
    'San Andrés de Tumaco', 'El Carmen de Viboral', 'El Carmen de Bolívar',
    'Belén de Umbría', 'Santander de Quilichao',
])
def test_full_identity_never_resolves_a_fragment(catalog, town):
    router, _, oracle = catalog
    query = f'contratos del municipio de {town}'
    params = router.parse(query).params
    owner = params.get('entidad_resolved')
    expected = get_municipal_territories().lookup(town)[0].divipola
    if owner:
        assert oracle[owner]['codes'] == [expected], (query, params)
        assert not params.get('objeto')
    else:
        assert params['entidad_resolution']['clarification_needed']
        assert not compute_query_plan(params, query).should_execute
        with pytest.raises((ValueError, UnsafeGlobalQueryError)):
            SoQLBuilder().build_count('jbjy-vk9h', params)


def test_every_discovered_compound_and_homonym(catalog):
    router, sidecar, oracle = catalog
    report = evaluate(router, sidecar, oracle, relevant=True)
    assert report['status'] == 'PASS', [r for r in report['results'] if r['classification'] not in {
        'CORRECT_BUYER', 'CLARIFICATION', 'FAIL_CLOSED'}][:10]


@pytest.mark.parametrize('query', [
    'gobernación de Antioquia de 2025',
    'alcaldía de Medellín de obras de infraestructura',
])
def test_legitimate_trailing_fallback(catalog, query):
    router, _, _ = catalog
    params = router.parse(query).params
    assert params.get('entidad_resolved'), params
    assert compute_query_plan(params, query).should_execute


def test_discovery_is_reproducible():
    assert discovery() == read(Path('artifacts/audits/municipal_m1a_2/municipal_identity_matrix.json'))


def test_review_is_never_pass_and_homonym_cannot_select_known_owner():
    assert classify('unadjudicated', [], '00001', False, True, None, False) == 'REVIEW'
    assert classify('known', ['00001'], '00001', True, True, None, False) == 'WRONG_BUYER'
    assert classify('foreign', ['00002'], '00001', False, True, None, False) == 'WRONG_DIVIPOLA'
