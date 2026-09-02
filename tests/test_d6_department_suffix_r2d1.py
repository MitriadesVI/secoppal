"""D6-R2D1 — geografía vs institución que termina en geografía.

`_is_unqualified_dept` decidía si un alias era topónimo con
``alias.endswith(clave)``. Sin frontera de palabra, *corpocaldas* "terminaba
en" Caldas; con él caían 2.104 aliases institucionales legítimos y once de las
33 CAR quedaban inalcanzables por el router (R2D §7).

La regla nueva es pertenencia al catálogo territorial sobre el alias o sobre su
forma despojada de wrappers **iniciales**. Los dos extremos están congelados
aquí como mutaciones ejecutables: restaurar ``endswith`` vuelve a bloquear a las
instituciones, y exigir igualdad estricta —sin el despojo inicial— convierte
"el Cauca" en la Universidad del Cauca. Ninguna de las dos es decoración.

Evidencia: artifacts/audits/d6_department_suffix_r2d1.json
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.generic_buyer import department_root_buyer_target
from app.core.geography_alias import geography_forms, is_geography_alias
from app.core.query_router import QueryRouter
from app.data.departamentos import DEPARTAMENTOS
from app.utils.money import normalize_text

ALIASES_DB = Path("app/data/aliases_db.json")


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def router(resolver: EntityResolver) -> QueryRouter:
    return QueryRouter(entity_resolver=resolver)


def _entity(router: QueryRouter, phrase: str) -> str | None:
    return router.parse(phrase).params.get("entidad_resolved")


def _department(router: QueryRouter, phrase: str) -> str | None:
    return router.parse(phrase).params.get("departamento_resolved")


# ---------------------------------------------------------------------------
# El helper puro
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        ("antioquia", True),
        ("en antioquia", True),
        ("el cauca", True),
        ("en el meta", True),
        ("el valle del cauca", True),
        ("el distrito capital de bogota", True),
        # Instituciones cuyo último token o última sílaba es un departamento.
        ("corpocaldas", False),
        ("carsucre", False),
        ("corponarino", False),
        ("universidad del cauca", False),
        ("acueducto de bogota", False),
        ("secretaria general de la alcaldia mayor de bogota", False),
    ],
)
def test_geography_membership_not_textual_suffix(alias: str, expected: bool) -> None:
    assert is_geography_alias(alias) is expected


def test_wrappers_are_stripped_only_from_the_head() -> None:
    """El despojo nunca toca tokens internos ni finales."""
    assert geography_forms("en el meta") == ["en el meta", "el meta", "meta"]
    assert geography_forms("universidad del cauca") == ["universidad del cauca"]
    assert geography_forms("de bogota") == ["de bogota", "bogota"]


def test_every_catalog_key_is_geography() -> None:
    """La regla nueva no puede dejar escapar una clave territorial."""
    assert [k for k in DEPARTAMENTOS if not is_geography_alias(k)] == []


# ---------------------------------------------------------------------------
# Canarios territoriales — RC2 no se reabre
# ---------------------------------------------------------------------------

TERRITORY_CANARIES = [
    ("licitaciones en el Cauca", "Cauca"),
    ("licitaciones en el Valle del Cauca", "Valle del Cauca"),
    ("licitaciones en el Distrito Capital de Bogotá", "Distrito Capital de Bogotá"),
    ("licitaciones en el Arauca", "Arauca"),
    ("contratos en Meta", "Meta"),
    ("contratos en el Meta", "Meta"),
    ("contratos en Antioquia", "Antioquia"),
    ("procesos en Santander", "Santander"),
    ("contratos en Norte de Santander", "Norte de Santander"),
    ("procesos en San Andrés", "San Andrés, Providencia y Santa Catalina"),
    ("contratos en San Andrés", "San Andrés, Providencia y Santa Catalina"),
]


@pytest.mark.parametrize(("phrase", "department"), TERRITORY_CANARIES)
def test_territory_stays_territory(router: QueryRouter, phrase: str, department: str) -> None:
    assert _entity(router, phrase) is None
    assert _department(router, phrase) == department


# ---------------------------------------------------------------------------
# Canarios de comprador raíz — R1C/R1D/R2A intactos
# ---------------------------------------------------------------------------

BUYER_ROOT_CANARIES = [
    ("contratos de la Gobernación de Santander", "DEPARTAMENTO DE SANTANDER"),
    ("contratos de la Gobernación del Cauca", "GOBERNACIÓN DEL CAUCA"),
    ("contratos del Gobierno Departamental del Tolima", "GOBIERNO DEPARTAMENTAL"),
    (
        "contratos de la Gobernación de San Andrés",
        "GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA Y SANTA CATALINA",
    ),
]


@pytest.mark.parametrize(("phrase", "physical"), BUYER_ROOT_CANARIES)
def test_root_buyer_grammar_survives(router: QueryRouter, phrase: str, physical: str) -> None:
    assert _entity(router, phrase) == physical
    assert _department(router, phrase) is None


def test_root_buyer_keeps_precedence_over_geography(resolver: EntityResolver) -> None:
    """La gramática raíz se evalúa antes que el catálogo territorial."""
    for phrase in ("departamento de antioquia", "gobernacion de narino"):
        assert department_root_buyer_target(phrase) in DEPARTAMENTOS
        assert resolver._is_unqualified_dept(phrase, phrase) is False


def test_hospital_san_andres_is_a_hospital(router: QueryRouter) -> None:
    """RC2: un topónimo desnudo no se convierte en entidad arbitraria, y una
    entidad que lo lleva en el nombre no se convierte en territorio."""
    assert _entity(router, "contratos del Hospital San Andrés") == "HOSPITAL SAN ANDRES"
    assert _department(router, "contratos del Hospital San Andrés") is None


# ---------------------------------------------------------------------------
# Instituciones desbloqueadas — dentro y fuera del universo CAR
# ---------------------------------------------------------------------------

NON_CAR_FREED = [
    (
        "contratos de la secretaría general de la alcaldía mayor de Bogotá",
        "SECRETARÍA GENERAL DE LA ALCALDÍA MAYOR DE BOGOTÁ",
    ),
    (
        "contratos de la Empresa de Acueducto y Alcantarillado de Bogotá",
        "EMPRESA DE ACUEDUCTO Y ALCANTARILLADO DE BOGOTÁ - E.S.P.",
    ),
    (
        "contratos del Instituto de Caminos y Construcciones de Cundinamarca",
        "INSTITUTO DE CAMINOS Y CONSTRUCCIONES DE CUNDINAMARCA - ICCU",
    ),
    ("contratos de Canal Teleantioquia", "SOCIEDAD TELEVISION DE ANTIOQUIA LTDA"),
    ("contratos de la Gobernación de Boyacá", "GOBERNACIÓN DE BOYACÁ"),
]


@pytest.mark.parametrize(("phrase", "physical"), NON_CAR_FREED)
def test_institution_ending_in_geography_is_reachable(
    router: QueryRouter, phrase: str, physical: str
) -> None:
    assert _entity(router, phrase) == physical
    assert _department(router, phrase) is None


def test_terminal_santa_marta_resolves_to_its_specific_buyer(
    router: QueryRouter,
) -> None:
    """R2D.3 keeps D6 open while replacing the old wrong owner."""
    phrase = "contratos del Terminal de Transportes de Santa Marta Magdalena"
    assert _entity(router, phrase) == "CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA."
    assert _department(router, phrase) == "Magdalena"


def test_ese_del_meta_stops_resolving_to_the_department(resolver: EntityResolver) -> None:
    """El caso histórico de D6 dejó de entregar al comprador equivocado.

    Con ``endswith`` el scan descartaba el alias propio de la ESE y caía en el
    topónimo calificado por "departamento de" — es decir, el departamento del
    Meta con ``confidence: high``. El router sigue cerrando la frase por el
    guard de comprador genérico (deuda ajena a D6), pero el resolver ya no
    entrega un dueño ajeno.
    """
    scanned = resolver.scan_entity(normalize_text("ESE del departamento de Meta"))
    assert scanned is not None
    assert scanned.official_name == "EMPRESA SOCIAL DEL ESTADO DEL DEPARTAMENTO DEL META E.S.E"


# ---------------------------------------------------------------------------
# Mutaciones: cada mitad de la regla sostiene un invariante distinto
# ---------------------------------------------------------------------------

def _mutate(monkeypatch: pytest.MonkeyPatch, is_geo) -> None:
    monkeypatch.setattr("app.core.entity_resolver.is_geography_alias", is_geo)


# Instituciones que dependen de la regla nueva por su ÚNICO alias alcanzable.
# Las demás de ``NON_CAR_FREED`` sobreviven a la mutación por una sigla o una
# forma que no termina en geografía (EAAB, ICCU, "gobernación de…"), y por eso
# no sirven como testigo del sufijo textual.
SUFFIX_DEPENDENT = [
    "contratos de la secretaría general de la alcaldía mayor de Bogotá",
    "contratos de Canal Teleantioquia",
    "contratos de CORPOCALDAS en 2025",
]


def test_mutation_textual_suffix_reblocks_institutions(
    router: QueryRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Restaurar ``endswith`` vuelve a bloquear a las instituciones D6."""
    assert [p for p in SUFFIX_DEPENDENT if _entity(router, p) is None] == []
    _mutate(monkeypatch, lambda a: any(k == a.strip() or a.strip().endswith(k)
                                       for k in DEPARTAMENTOS))
    assert [p for p in SUFFIX_DEPENDENT if _entity(router, p) is not None] == []


def test_mutation_exact_only_turns_territory_into_buyers(
    router: QueryRouter, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Quitar el despojo inicial reabre RC2 en cuatro frases territoriales."""
    _mutate(monkeypatch, lambda a: a.strip() in DEPARTAMENTOS)
    hijacked = [
        phrase for phrase, _ in TERRITORY_CANARIES if _entity(router, phrase) is not None
    ]
    assert hijacked == [
        "licitaciones en el Cauca",
        "licitaciones en el Valle del Cauca",
        "licitaciones en el Distrito Capital de Bogotá",
        "licitaciones en el Arauca",
    ]
    assert _entity(router, "licitaciones en el Cauca") == "UNIVERSIDAD DEL CAUCA"


def test_root_buyer_precedence_is_no_longer_load_bearing(resolver: EntityResolver) -> None:
    """La precedencia raíz quedó redundante bajo la regla nueva, y se conserva.

    Con ``endswith`` era la única defensa de "gobierno departamental del
    Tolima". Bajo pertenencia al catálogo, ningún alias del gazetteer depende
    ya de ella: o no es geografía, o empieza por un calificador de entidad. Se
    mantiene como autoridad semántica para aliases futuros; el test afirma el
    hecho medido, no una mutación que no muerde.
    """
    decisive = [
        alias for alias in resolver.owners_by_alias
        if (target := department_root_buyer_target(alias))
        and target in DEPARTAMENTOS
        and is_geography_alias(alias)
        and (alias.split()[0] if alias else "") not in resolver._ENTITY_QUALIFIERS
    ]
    assert decisive == []
