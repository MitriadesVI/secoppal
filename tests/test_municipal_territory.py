"""DIVIPOLA geography cannot promote a buyer or guess a homonymous municipality."""
import json

import pytest

from app.core.municipal_territory import MunicipalTerritoryRegistry, PATH, get_municipal_territories
from app.core.query_frame import frame_from_params, params_from_frame
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from scripts.audits.build_municipal_territories import build


@pytest.fixture(scope='module')
def router():
    return QueryRouter()


def test_frozen_authority_and_all_qualified_exact_lookups():
    assert build() == PATH.read_bytes()
    registry = get_municipal_territories()
    assert len(registry.by_code) == 1103
    assert len(registry.by_alias) == 1021
    assert sum(len(v) > 1 for v in registry.by_alias.values()) == 64
    for t in registry.by_code.values():
        assert registry.lookup(t.municipality, t.department) == (t,)


@pytest.mark.parametrize('mutation', ['duplicate', 'count', 'alias', 'version', 'provenance'])
def test_invalid_registry_fails_closed(tmp_path, mutation):
    data = json.loads(PATH.read_bytes())
    if mutation == 'duplicate':
        data['territories'][1] = data['territories'][0]
    elif mutation == 'count':
        data['territories'].pop()
    elif mutation == 'alias':
        data['territories'][0]['aliases'] = ['invented']
    elif mutation == 'version':
        data['schema_version'] = 999
    else:
        data['provenance']['uncompressed_sha256'] = ''
    path = tmp_path / 'registry.json'
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError):
        MunicipalTerritoryRegistry(path)


@pytest.mark.parametrize('name', [
    'baranoa', 'puerto gaitan', 'magangue', 'los patios', 'tocancipa', 'aguazul', 'ipiales',
    'medellin', 'santa marta', 'el carmen de viboral', 'la victoria, departamento boyaca',
    'san juan de rioseco', 'santa rosalia', 'nuevo colon', 'nuevo belen de bajira',
    'san vicente ferrer',
])
def test_explicit_municipal_geography_pipeline(router, name):
    query = 'contratos en ' + name
    params = router.parse(query).params
    plan = compute_query_plan(params, query)
    assert plan.should_execute, plan
    assert params['municipal_territory']['status'] == 'resolved'
    assert not params.get('entidad') and not params.get('entidad_resolved')
    assert not params.get('objeto') and not params.get('ordering_signal')
    for ds in ['p6dx-8zbt', 'jbjy-vk9h']:
        soql = SoQLBuilder().build_count(ds, params)
        assert 'LIKE' not in soql
        assert params['departamento_resolved'] in soql


@pytest.mark.parametrize('name', ['la union', 'belen', 'bolivar', 'cordoba', 'armenia'])
def test_homonyms_clarify_and_builder_cannot_bypass_plan(router, name):
    query = 'contratos en ' + name
    params = router.parse(query).params
    plan = compute_query_plan(params, query)
    assert not plan.should_execute
    assert 'departamento' in plan.clarification_question
    assert not params.get('ciudad') and not params.get('entidad_resolved')
    with pytest.raises(ValueError):
        SoQLBuilder().build_count('jbjy-vk9h', params)
    restored = params_from_frame(frame_from_params(params))
    assert not compute_query_plan(restored, query).should_execute


@pytest.mark.parametrize('name,department,code', [
    ('belen', 'boyaca', '15087'), ('belen', 'narino', '52083'),
    ('la union', 'valle del cauca', '76400'), ('bolivar', 'santander', '68101'),
])
def test_explicit_department_selects_exact_code(router, name, department, code):
    query = f'contratos en {name}, departamento {department}'
    params = router.parse(query).params
    assert params['municipal_territory']['divipola'] == code
    assert compute_query_plan(params, query).should_execute


def test_incompatible_department_does_not_silently_drop_qualification(router):
    query = 'contratos en baranoa, departamento boyaca'
    params = router.parse(query).params
    assert not compute_query_plan(params, query).should_execute
    assert params['municipal_territory']['status'] == 'department_mismatch'


def test_buyer_intent_is_still_buyer_intent_without_promotion(router):
    for name in ['baranoa', 'nuevo colon']:
        params = router.parse('contratos de la alcaldia de ' + name).params
        assert params.get('entidad')
        assert params['entidad_resolution']['clarification_needed']
        assert not params.get('ciudad') and not params.get('municipal_territory')
        assert not params.get('ordering_signal')
