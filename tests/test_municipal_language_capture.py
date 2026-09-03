"""M1A.1 predictive capture cases, frozen before changing runtime."""

import pytest

from app.core.entity_types import rewrite_entity


@pytest.mark.parametrize('head', ['alcaldia de', 'municipio de'])
@pytest.mark.parametrize('name', [
    'susacon', 'nemocon', 'belen', 'valparaiso',
    'el carmen de viboral', 'el carmen de bolivar',
])
def test_municipal_name_is_not_cut_inside_a_word(head, name):
    query = f'{head} {name}'
    assert rewrite_entity(query)[0].consumed_span == query


@pytest.mark.parametrize('head', [
    'alcaldia de', 'municipio de', 'gobernacion del',
    'gobierno departamental del', 'ese', 'hospital', 'universidad',
])
@pytest.mark.parametrize('name', ['carmen', 'susacon', 'valparaiso', 'paraiso'])
def test_shared_boundary_preserves_whole_tokens_in_every_family(head, name):
    query = f'{head} {name}'
    assert rewrite_entity(query)[0].consumed_span == query


def test_existing_nonmunicipal_fixture_carmen_emilia_ospina():
    assert rewrite_entity('comprador ese carmen emilia ospina perfil')[0].consumed_span == (
        'ese carmen emilia ospina perfil'
    )


@pytest.mark.parametrize('query,expected', [
    ('alcaldia de baranoa para contratos de obra', 'alcaldia de baranoa'),
    ('municipio de yopal en 2025', 'municipio de yopal'),
    ('gobernacion del atlantico con contratos', 'gobernacion del atlantico'),
    ('universidad nacional sobre obras', 'universidad nacional'),
    ('hospital central por mayor valor', 'hospital central'),
    ('ese central donde hay obras', 'ese central'),
])
def test_independent_clause_tokens_still_end_names(query, expected):
    assert rewrite_entity(query)[0].consumed_span == expected
