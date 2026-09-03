"""Cross-interaction: a new shorter buyer cannot steal an accredited longer name."""
import json
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


@pytest.mark.parametrize('with_mayoralty', [False, True])
def test_existing_cienaga_hospital_keeps_owner_after_joint_promotion(tmp_path, with_mayoralty):
    records = json.loads(Path('app/data/aliases_db.json').read_text())
    hospital = next(r for r in records if r['official_name'] ==
                    'HOSPITAL SAN CRISTOBAL DE CIENAGA EMPRESA SOCIAL DEL ESTADO')
    records = [hospital]
    if with_mayoralty:
        records.append(dict(official_name='MUNICIPIO DE CIENAGA', aliases=[
            'alcaldia de cienaga', 'municipio de cienaga']))
    path = tmp_path / 'aliases.json'
    path.write_text(json.dumps(records))
    query = 'contratos de alcaldía de cienaga hospital san cristobal'
    params = QueryRouter(EntityResolver(path)).parse(query).params
    assert params['entidad_resolved'] == hospital['official_name']


@pytest.mark.parametrize('sector', ['hospital', 'universidad', 'ese'])
def test_longer_accredited_span_blocks_only_its_prefix_rewrites(tmp_path, sector):
    name = sector + ' nacional de prueba'
    path = tmp_path / 'aliases.json'
    path.write_text(json.dumps([
        dict(official_name='MUNICIPIO DE VILLA CLARA', aliases=[
            'alcaldia de villa clara', 'municipio de villa clara']),
        dict(official_name=name.upper(), aliases=[
            name, 'alcaldia de villa clara ' + name]),
    ]))
    router = QueryRouter(EntityResolver(path))
    params = router.parse('contratos de alcaldia de villa clara ' + name).params
    assert params['entidad_resolved'] == name.upper()
    # A real clause outside the buyer still permits the shorter buyer.
    params = router.parse('contratos de alcaldia de villa clara para construccion').params
    assert params['entidad_resolved'] == 'MUNICIPIO DE VILLA CLARA'
