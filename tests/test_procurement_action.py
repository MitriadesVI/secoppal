"""Tests PROCUREMENT-ACTION-001 — scrub verbos contractuales del objeto."""
from __future__ import annotations

import pytest

from app.core.query_router import QueryRouter


@pytest.fixture
def router():
    return QueryRouter()


def test_adquisicion_de_uniformes_scrubs_verb(router):
    """'adquisición de uniformes' → objeto=['uniformes'], no ['adquisicion','uniformes']"""
    r = router.parse("adquisicion de uniformes")
    obj = r.params.get("objeto", [])
    # "adquisicion" is an action verb, should be scrubbed when substantive noun exists
    assert "adquisicion" not in obj
    assert "uniformes" in obj


def test_compra_de_equipos_scrubs_verb(router):
    """'compra de equipos de computo' → objeto=['equipos','computo']"""
    r = router.parse("compra de equipos de computo")
    obj = r.params.get("objeto", [])
    assert "compra" not in obj
    assert "equipos" in obj


def test_contratar_servicios_de_aseo_scrubs_verb(router):
    """'contratar servicios de aseo' → objeto=['servicios','aseo']"""
    r = router.parse("contratar servicios de aseo")
    obj = r.params.get("objeto", [])
    assert "contratar" not in obj
    assert "servicios" in obj


def test_verb_alone_without_noun_is_kept(router):
    """'oportunidades de contratacion' → verbo solo no rompe"""
    r = router.parse("oportunidades de contratacion")
    obj = r.params.get("objeto", [])
    assert isinstance(obj, list)


def test_adquisiciones_multiples_scrubs_all(router):
    """'adquisiciones de mobiliario y equipos' → mobiliario, equipos"""
    r = router.parse("adquisiciones de mobiliario y equipos")
    obj = r.params.get("objeto", [])
    assert "adquisiciones" not in obj
    assert "mobiliario" in obj


def test_compra_de_medicamentos_en_atlantico(router):
    """'compra de medicamentos en Atlantico' → medicamentos, no compra"""
    r = router.parse("compra de medicamentos en Atlantico")
    obj = r.params.get("objeto", [])
    assert "compra" not in obj
    assert "medicamentos" in obj
