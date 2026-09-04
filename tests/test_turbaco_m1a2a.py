"""Regression: human Turbaco aliases must query its actual SECOP BuyerUnit."""
from pathlib import Path
import pytest
from app.core.buyer_source_names import project_buyer_names
from app.core.entity_resolver import EntityResolver
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
ROOT = Path(__file__).resolve().parents[1]
B = 'ALCALDIA MUNICIPAL DE TURBACO BOLIVAR'


@pytest.mark.parametrize('query', ['municipio de turbaco', 'alcaldía de turbaco',
                                   'contratos de la alcaldía de turbaco', 'contratos del municipio de turbaco'])
def test_turbaco_queries_reach_only_the_accredited_physical_buyer(query):
    resolver = EntityResolver(ROOT / 'app/data/aliases_db.json')
    params = QueryRouter(resolver).parse(query).params
    assert params['entidad_resolved'] == B
    assert compute_query_plan(params, query).should_execute
    for dataset, column in [('p6dx-8zbt', 'entidad'), ('jbjy-vk9h', 'nombre_entidad')]:
        assert project_buyer_names(B, dataset) == (B,)
        soql = SoQLBuilder().build_count(dataset, params)
        assert f"{column} = '{B}'" in soql
        assert 'LIKE' not in soql and 'nit_entidad' not in soql
        assert 'ALCALDIA MUNICIPAL DE TURBACO 1' not in soql

