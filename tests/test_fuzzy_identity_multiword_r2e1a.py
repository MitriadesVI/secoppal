"""R2E.1A — el territorio pedido se compara como frase, no como tokens sueltos.

La puerta de R2E.1 preguntaba, por cada token de la petición, si ese token era
una clave del catálogo territorial. El catálogo, en cambio, está indexado por
FRASE: `norte de santander`, `san andres y providencia`, `puerto carreño`,
`santa marta`, `distrito capital de bogota`. De ahí dos fallos distintos:

* **Territorio invisible.** Ninguna palabra de `santa marta` o `puerto carreño`
  es clave por separado, así que la petición no imponía requisito alguno y el
  candidato podía perder el territorio entero.
* **Territorio confundido.** `norte de santander` quedaba reducido a
  `santander`, que es otro departamento con otro NIT. Peor que no mirar: mirar
  mal.

Los oráculos de este archivo son de la PUERTA, no del resolver: `expected` dice
si el candidato conserva la identidad pedida, no qué comprador debe salir.
Varios casos con `expected = False` los rechaza la regla de cabeza de R2E.1 y
no la territorial; están para fijar que tocar el territorio no los mueve.
"""

from __future__ import annotations

import pytest

from app.core.entity_resolver import TERRITORY_PHRASES, EntityResolver
from app.core.entity_types import name_tokens
from app.core.fuzzy_identity_guard import (
    FILLER_TOKENS,
    candidate_keeps_requested_identity,
    requested_identity_tokens,
)
from app.core.territory_phrase_index import TerritoryPhraseIndex, contains_span
from app.data.departamentos import DEPARTAMENTOS

ALIASES_DB = "app/data/aliases_db.json"

CGR = "CGR-NIVEL CENTRAL"
CONTRALORIA_BOGOTA = "CONTRALORIA DE BOGOTA"
NORTE_SANTANDER = "Gobernación Norte de Santander*"
SAN_ANDRES = (
    "GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA"
)

# El candidato pierde el territorio que la frase nombra.
TERRITORY_LOSS = (
    ("norte de santander vs santander", "contraloria departamental de norte de santander",
     "contraloria general de santander", "CONTRALORIA GENERAL DE SANTANDER"),
    ("san andres", "contraloria de san andres", "contraloria", CGR),
    ("puerto carreno", "contraloria de puerto carreno", "contraloria", CGR),
    ("santa marta vs magdalena", "contraloria de santa marta",
     "contraloria general del magdalena",
     "CONTRALORIA GENERAL DEL DEPARTAMENTO DEL MAGDALENA"),
    ("valle del cauca vs cauca", "contraloria departamental del valle del cauca",
     "contraloria del cauca", "CONTRALORIA GENERAL DEL CAUCA"),
    ("santander vs norte de santander", "contraloria de santander",
     "gobernacion norte de santander", NORTE_SANTANDER),
    ("san andres y providencia", "contraloria de san andres y providencia",
     "contraloria general", CGR),
    ("distrito capital hacia otro dueño", "contraloria del distrito capital de bogota",
     "bogota distrito capital", "UNIVERSIDAD PEDAGÓGICA NACIONAL"),
)

# El candidato conserva el territorio pedido, aunque lo escriba de otra forma.
TERRITORY_KEPT = (
    ("norte de santander", "gobernacion de norte de santander",
     "gobernacion norte de santander", NORTE_SANTANDER),
    ("san andres", "gobernacion de san andres", "san andres", SAN_ANDRES),
    ("valle abreviado", "gobernacion del valle del cauca", "el valle",
     "GOBERNACION DEL VALLE DEL CAUCA - SECRETARIA DE EDUCACION"),
    ("distrito capital de bogota", "contraloria del distrito capital de bogota",
     "contraloria del distrito capital", CONTRALORIA_BOGOTA),
    ("distrito capital por bogota", "contraloria del distrito capital",
     "contraloria de bogota", CONTRALORIA_BOGOTA),
    ("santa marta", "contraloria de santa marta", "contraloria de santa marta",
     "CONTRALORIA DISTRITAL DE SANTA MARTA"),
    ("sin territorio en la frase", "contraloria general de la republica de colombia",
     "la contraloria general de la republica", CGR),
)

# Cabeza distinta: los rechaza la regla de R2E.1, no la territorial.
HEAD_MISMATCH = (
    ("contraloria de norte de santander", "gobernacion norte de santander", NORTE_SANTANDER),
    ("contraloria del valle del cauca", "el valle",
     "GOBERNACION DEL VALLE DEL CAUCA - SECRETARIA DE EDUCACION"),
)

# Frases del catálogo cuyos tokens no son claves por separado.
MULTIWORD_PHRASES = (
    ("norte de santander", "Norte de Santander"),
    ("san andres", "San Andrés, Providencia y Santa Catalina"),
    ("santa marta", "Santa Marta"),
    ("puerto carreño", "Puerto Carreño"),
    ("valle del cauca", "Valle del Cauca"),
    ("distrito capital de bogota", "Distrito Capital de Bogotá"),
)


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


def keeps(query: str, alias: str, official: str) -> bool:
    return candidate_keeps_requested_identity(query, alias, official, TERRITORY_PHRASES)


class TestPhraseIndex:
    """El índice reconoce la frase más larga y sus formas equivalentes."""

    @pytest.mark.parametrize("phrase,canonical", MULTIWORD_PHRASES)
    def test_phrase_is_recognized_whole(self, phrase: str, canonical: str) -> None:
        tokens = tuple(requested_identity_tokens(phrase))
        assert TERRITORY_PHRASES.spans_in(tokens) == [tokens]

    def test_longest_match_wins_over_its_own_fragment(self) -> None:
        # "norte de santander" contiene "santander", que es OTRO departamento.
        # Reportar el fragmento sería reportar un territorio que nadie pidió.
        tokens = tuple(requested_identity_tokens("contraloria de norte de santander"))
        assert TERRITORY_PHRASES.spans_in(tokens) == [("norte", "santander")]

    def test_equivalent_forms_cover_wrappers_and_order(self) -> None:
        forms = TERRITORY_PHRASES.equivalent_forms(("distrito", "capital", "bogota"))
        assert ("bogota",) in forms
        assert ("distrito", "capital") in forms

    def test_equivalent_forms_reject_proxies(self) -> None:
        # `cucuta` resuelve a Norte de Santander y `cundi` a Cundinamarca: son
        # atajos para filtrar, no nombres del territorio.
        assert ("cucuta",) not in TERRITORY_PHRASES.equivalent_forms(("norte", "santander"))
        assert ("cundi",) not in TERRITORY_PHRASES.equivalent_forms(("cundinamarca",))

    def test_neighbouring_department_is_not_an_equivalent_form(self) -> None:
        assert ("santander",) not in TERRITORY_PHRASES.equivalent_forms(("norte", "santander"))
        assert ("cauca",) not in TERRITORY_PHRASES.equivalent_forms(("valle", "cauca"))

    def test_index_owns_no_geography_of_its_own(self) -> None:
        empty = TerritoryPhraseIndex.from_alias_maps(connectors=FILLER_TOKENS)
        assert empty.spans_in(("bogota", "santander")) == []

    def test_contains_span_is_contiguous(self) -> None:
        assert contains_span(("a", "b", "c"), ("b", "c"))
        assert not contains_span(("a", "b", "c"), ("a", "c"))


class TestMultiwordTerritoryLoss:
    """MULTIWORD_TERRITORY_LOSS = 0."""

    @pytest.mark.parametrize(
        "case,query,alias,official", TERRITORY_LOSS, ids=[c[0] for c in TERRITORY_LOSS]
    )
    def test_candidate_that_drops_the_territory_is_rejected(
        self, case: str, query: str, alias: str, official: str
    ) -> None:
        assert keeps(query, alias, official) is False


class TestNoNewFalseRejections:
    """NEW_FALSE_REJECTIONS = 0."""

    @pytest.mark.parametrize(
        "case,query,alias,official", TERRITORY_KEPT, ids=[c[0] for c in TERRITORY_KEPT]
    )
    def test_candidate_that_keeps_the_territory_is_accepted(
        self, case: str, query: str, alias: str, official: str
    ) -> None:
        assert keeps(query, alias, official) is True

    @pytest.mark.parametrize("query,alias,official", HEAD_MISMATCH)
    def test_head_rule_still_decides_before_territory(
        self, query: str, alias: str, official: str
    ) -> None:
        assert keeps(query, alias, official) is False

    def test_institutional_vocabulary_did_not_become_territorial(self) -> None:
        # Volcar los tokens de las claves multiword en un conjunto plano habría
        # vuelto territoriales `departamento`, `distrito`, `san`, `santa`,
        # `norte`, `capital` y `puerto`, que son vocabulario institucional
        # corriente.  Ninguna de esas palabras impone requisito por sí sola.
        for token in ("departamento", "distrito", "san", "santa", "norte",
                      "capital", "puerto", "providencia", "marta", "andres"):
            assert TERRITORY_PHRASES.spans_in((token,)) == [], token


class TestResolverBehaviour:
    """La puerta actúa en el resolver, y sólo en los tiers inferenciales."""

    @pytest.mark.parametrize(
        "query",
        (
            "Contraloría Departamental de Norte de Santander",
            "Contraloría de San Andrés",
            "Contraloría de Puerto Carreño",
            "Contraloría de Santa Marta",
            "Contraloría Departamental del Valle del Cauca",
        ),
    )
    def test_multiword_territory_falls_closed(
        self, resolver: EntityResolver, query: str
    ) -> None:
        result = resolver.resolve_entidad(query)
        assert result.value is None
        assert result.method == "like"

    def test_distrito_capital_still_reaches_its_buyer(
        self, resolver: EntityResolver
    ) -> None:
        assert (
            resolver.resolve_entidad("Contraloría del Distrito Capital de Bogotá").value
            == CONTRALORIA_BOGOTA
        )

    @pytest.mark.parametrize(
        "query,expected",
        (
            ("Contraloría de Bogotá", CONTRALORIA_BOGOTA),
            ("Contraloría General de la República", CGR),
            ("Banrep", "BANCO DE LA REPUBLICA"),
            ("Banco de la República", "BANCO DE LA REPUBLICA"),
            ("Auditoría General de la República", "AUDITORIA GENERAL DE LA REPUBLICA"),
            ("Procuraduría General de la Nación", "PROCURADURIA GENERAL DE LA NACION"),
            ("Defensoría del Pueblo", "DEFENSORÍA DEL PUEBLO"),
        ),
    )
    def test_exact_ownership_untouched(
        self, resolver: EntityResolver, query: str, expected: str
    ) -> None:
        result = resolver.resolve_entidad(query)
        assert result.value == expected
        assert result.method == "exact"


class TestCatalogShape:
    """Las claves multiword existen y el índice las cubre todas."""

    def test_catalog_has_multiword_keys(self) -> None:
        multiword = [k for k in DEPARTAMENTOS if len(name_tokens(k)) > 1]
        assert len(multiword) >= 10

    def test_every_multiword_core_is_reachable_as_one_span(self) -> None:
        wrappers = FILLER_TOKENS | {"depto", "departamento", "dpto"}
        for key in DEPARTAMENTOS:
            core = tuple(t for t in name_tokens(key) if t not in wrappers)
            if len(core) > 1:
                assert TERRITORY_PHRASES.spans_in(core) == [core], key
