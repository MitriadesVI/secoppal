"""La cola del territorio de un comprador no es un filtro territorial.

La gramática de comprador corta la captura en " de/del ":
``gobernación del Valle del Cauca`` sólo consume ``gobernación del Valle`` y
deja ``del Cauca`` en el texto. El scan territorial lo leía después como el
departamento del **Cauca** — que el usuario nunca nombró — y el WHERE salía
con el territorio equivocado sin avisar (INV-NO-SILENT-SEMANTIC-FALLBACK).

El descarte es estricto: sólo se retira el departamento escaneado cuando es
parte del territorio que el propio comprador ya declaró. Una intersección
legítima ("la gobernación del Cauca en Antioquia") se conserva.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.generic_buyer import is_territory_fragment_of_buyer
from app.core.query_router import QueryRouter

ALIASES_DB = Path("app/data/aliases_db.json")


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(entity_resolver=EntityResolver(ALIASES_DB))


class TestFragmentPredicate:
    @pytest.mark.parametrize(
        ("scanned", "buyer"),
        [
            ("Cauca", "Valle del Cauca"),
            ("Santander", "Norte de Santander"),
            ("Valle del Cauca", "Valle del Cauca en algo"),
        ],
    )
    def test_strict_fragments_are_detected(self, scanned: str, buyer: str) -> None:
        assert is_territory_fragment_of_buyer(scanned, buyer)

    @pytest.mark.parametrize(
        ("scanned", "buyer"),
        [
            ("Antioquia", "Cauca"),
            ("Cauca", "Cauca"),
            ("Valle del Cauca", "Cauca"),
            ("", "Cauca"),
            ("Cauca", ""),
            # No es fragmento por compartir una letra: se comparan tokens.
            ("Santander", "Valle del Cauca"),
        ],
    )
    def test_non_fragments_are_kept(self, scanned: str, buyer: str) -> None:
        assert not is_territory_fragment_of_buyer(scanned, buyer)

    def test_token_boundaries_are_respected(self) -> None:
        """"Norte" no es fragmento de "Norte de Santander" por subcadena."""
        assert is_territory_fragment_of_buyer("Norte", "Norte de Santander")
        assert not is_territory_fragment_of_buyer("Nort", "Norte de Santander")


class TestRouterDoesNotInventTerritory:
    def test_valle_del_cauca_tail_never_becomes_cauca(self, router: QueryRouter) -> None:
        params = router.parse("contratos de gobernación del Valle del Cauca en 2025").params
        assert params.get("departamento_resolved") is None
        assert params.get("entidad_resolved") is None

    def test_legitimate_intersection_survives(self, router: QueryRouter) -> None:
        params = router.parse("contratos de la gobernación del Cauca en Antioquia").params
        assert params.get("entidad_resolved") == "GOBERNACIÓN DEL CAUCA"
        assert params.get("departamento_resolved") == "Antioquia"

    def test_plain_territory_query_is_unaffected(self, router: QueryRouter) -> None:
        params = router.parse("contratos en el departamento del Cauca").params
        assert params.get("departamento_resolved") == "Cauca"
        assert not params.get("entidad_resolved")

    def test_buyer_without_tail_keeps_no_territory(self, router: QueryRouter) -> None:
        params = router.parse("contratos de la gobernación del Cauca").params
        assert params.get("entidad_resolved") == "GOBERNACIÓN DEL CAUCA"
        assert params.get("departamento_resolved") is None
