"""Ordering must respect accredited names without deleting its vocabulary."""
import gzip
import json
from pathlib import Path

import pytest

from app.core.control_language import interpret_control_language, locate_control_span
from app.core.entity_resolver import EntityResolver
from app.core.proper_name_spans import ExactNameIndex
from app.core.query_router import QueryRouter
from app.utils.money import normalize_text


def control_token_municipalities():
    raw = gzip.decompress(Path('artifacts/audits/municipal_m0_v1/sources/divipola.json.gz').read_bytes())
    return [r['attributes'] for r in json.loads(raw)['features']
            if r['attributes']['MPIO_TIPO'] == 'MUNICIPIO'
            and locate_control_span(normalize_text(r['attributes']['MPIO_CNMBRE']))]


@pytest.fixture(scope='module')
def named_router(tmp_path_factory):
    # These are test-only identities, not a DIVIPOLA -> production BuyerUnit transform.
    records = [dict(official_name='ALCALDIA DE ' + r['MPIO_CNMBRE'], aliases=[
        'alcaldia de ' + normalize_text(r['MPIO_CNMBRE']),
        'municipio de ' + normalize_text(r['MPIO_CNMBRE']),
    ]) for r in control_token_municipalities()]
    records.append(dict(official_name='ALCALDIA DE BARANOA', aliases=['alcaldia de baranoa']))
    path = tmp_path_factory.mktemp('ordering_names') / 'aliases.json'
    path.write_text(json.dumps(records))
    return QueryRouter(EntityResolver(path))


@pytest.mark.parametrize('municipality', control_token_municipalities(), ids=lambda r: r['MPIO_CDPMP'])
@pytest.mark.parametrize('head', ['contratos de la alcaldia', 'procesos del municipio'])
def test_automatic_divipola_adversarial_matrix(named_router, municipality, head):
    query = head + ' de ' + municipality['MPIO_CNMBRE']
    params = named_router.parse(query).params
    assert params.get('entidad_resolved') == 'ALCALDIA DE ' + municipality['MPIO_CNMBRE']
    assert not params.get('ordering_signal')
    assert not params.get('objeto')


@pytest.mark.parametrize('query', [
    'contratos nuevos de la alcaldia de baranoa',
    'los contratos más nuevos de la alcaldia de baranoa',
    'contratos nuevos de la alcaldia de nuevo colon',
    'contratos de la alcaldia de nuevo colon ordenados por mayor valor',
])
def test_ordering_outside_name_survives(named_router, query):
    params = named_router.parse(query).params
    assert params['ordering_signal'] == ('valor_desc' if 'mayor valor' in query else 'fecha_desc')
    assert params.get('entidad_resolved')
    assert not params.get('objeto')


def test_shielding_is_positional_and_cannot_bridge_a_name():
    text = 'nueva granada nueva'
    result = interpret_control_language(text, protected_spans=((0, 13),))
    assert result.ordering_signal == 'fecha_desc'
    assert result.residual_text == 'nueva granada'
    assert result.consumed_spans == ('nueva',)
    text = 'mayor nueva granada valor'
    assert not interpret_control_language(text, protected_spans=((6, 19),)).ordering_signal


def test_disabled_scan_names_can_be_shielded_without_enabling_scan(tmp_path):
    path = tmp_path / 'aliases.json'
    path.write_text(json.dumps([dict(
        official_name='UNIVERSIDAD NUEVA GRANADA', aliases=['universidad nueva granada'],
        scan_disabled_aliases=['universidad nueva granada'],
    )]))
    resolver = EntityResolver(path)
    text = 'contratos de universidad nueva granada'
    assert resolver.scan_entity(text) is None
    spans = resolver.proper_names.spans_in(text)
    assert not interpret_control_language(text, protected_spans=tuple(spans)).ordering_signal


def test_longest_exact_name_offsets():
    index = ExactNameIndex(['nuevo', 'nuevo colon', 'nueva granada'])
    text = 'nuevos nuevo colon nueva granada'
    assert [text[a:b] for a, b in index.spans_in(text)] == ['nuevo colon', 'nueva granada']
