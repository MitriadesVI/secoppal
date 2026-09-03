"""El ruido físico de SECOP no convierte una clase en BuyerUnit."""
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.generic_buyer import is_unqualified_buyer_name
from app.core.query_router import QueryRouter


@pytest.fixture(scope="module")
def resolver():
    return EntityResolver(Path("app/data/aliases_db.json"))


@pytest.mark.parametrize("phrase", [
    "ALCALDIA*", "GOBERNACION+", "MUNICIPIO..", "E.S.E.", "E.S.E",
    "ese", "EMPRESA SOCIAL DEL ESTADO", "la alcaldía***",
])
def test_punctuation_does_not_qualify_a_generic_buyer(resolver, phrase):
    assert is_unqualified_buyer_name(phrase)
    result = resolver.resolve_entidad(phrase)
    assert result.value is None
    assert result.like_value is None
    assert result.clarification_needed
    assert resolver.scan_entity(f"contratos de la {phrase}") is None


@pytest.mark.parametrize("phrase", ["ALCALDIA*", "GOBERNACION+", "E.S.E."])
def test_generic_in_query_stays_unresolved_buyer_not_topic(phrase):
    params = QueryRouter().parse(f"contratos de la {phrase} en 2026").params
    assert not params.get("entidad_resolved")
    assert params["entidad_resolution"]["clarification_needed"]
    assert not params.get("objeto")


@pytest.mark.parametrize("phrase", [
    "alcaldia de yopal", "gobernacion del tolima", "ESE Hospital San Rafael",
])
def test_named_institutions_are_not_bare_classes(phrase):
    assert not is_unqualified_buyer_name(phrase)
