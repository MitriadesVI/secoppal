"""R2D — núcleo oficial de 33 CAR/CDS con identidad SECOP II verificada.

Los canarios se escribieron contra el HEAD previo a la mutación del gazetteer:
cada resolución insegura de la matriz R2D queda congelada aquí con el dueño
equivocado que producía, para que la corrección no pueda deshacerse en
silencio.

Tres siglas quedan deliberadamente fuera del gazetteer. ``CRA`` y ``CRC`` son
las siglas normativas de la Comisión de Regulación de Agua Potable y de la
Comisión de Regulación de Comunicaciones —ambas compradoras activas en SECOP
II—, y ``CDA`` nombra tanto un grupo administrativo del SENA como los centros
de diagnóstico automotor. Sus corporaciones entran igual por nombre
institucional: fallar cerrado en la sigla es preferible a inventar un dueño.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.utils.money import normalize_text

ALIASES_DB = Path("app/data/aliases_db.json")

# (sigla, nombre institucional oficial, nombre físico SECOP II, sigla admitida)
CAR_CORE_33: list[tuple[str, str, str, bool]] = [
    ("CAM", "Corporación Autónoma Regional del Alto Magdalena",
     "CORPORACION AUTONOMA REGIONAL DEL ALTO MAGDALENA", True),
    ("CAR", "Corporación Autónoma Regional de Cundinamarca",
     "CORPORACION AUTONOMA REGIONAL DE CUNDINAMARCA - CAR.", True),
    ("CARDER", "Corporación Autónoma Regional de Risaralda",
     "CORPORACION AUTONOMA REGIONAL DE RISARALDA", True),
    ("CARDIQUE", "Corporación Autónoma Regional del Canal del Dique",
     "CORPORACIÓN AUTÓNOMA REGIONAL DEL CANAL DEL DIQUE", True),
    ("CARSUCRE", "Corporación Autónoma Regional de Sucre",
     "CORPORACION AUTONOMA REGIONAL DE SUCRE", True),
    ("CAS", "Corporación Autónoma Regional de Santander",
     "CORPORACION AUTONOMA REGIONAL DE SANTANDER+", True),
    ("CDA", "Corporación para el Desarrollo Sostenible del Norte y Oriente Amazónico",
     "CORPORACION PARA EL DESARROLLO SOSTENIBLE DEL NORTE Y DEL ORIENTE AMAZONICO", False),
    ("CDMB", "Corporación Autónoma Regional para la Defensa de la Meseta de Bucaramanga",
     "CORPORACIÓN AUTÓNOMA REGIONAL PARA LA DEFENSA DE LA MESETA DE BUCARAMANGA", True),
    ("CODECHOCÓ", "Corporación Autónoma Regional para el Desarrollo Sostenible del Chocó",
     "CORPORACIÓN AUTÓNOMA REGIONAL PARA EL DESARROLLO SOSTENIBLE DEL CHOCO", True),
    ("CORALINA", "Corporación para el Desarrollo Sostenible del Archipiélago de San Andrés, "
                 "Providencia y Santa Catalina",
     "CORPORACION PARA EL DESARROLLO SOSTENIBLE DEL ARCHIPIELAGO DE SAN ANDRES PROVIDENCIA "
     "Y SANTA CATALINA - CORALINA", True),
    ("CORANTIOQUIA", "Corporación Autónoma Regional del Centro de Antioquia",
     "CORPORACIÓN AUTÓNOMA REGIONAL DEL CENTRO DE ANTIOQUIA", True),
    ("CORMACARENA", "Corporación para el Desarrollo Sostenible del Área de Manejo Especial "
                    "La Macarena", "CORMACARENA", True),
    ("CORNARE", "Corporación Autónoma Regional de las Cuencas de los Ríos Negro y Nare",
     "CORPORACION AUTONOMA REGIONAL DE LAS CUENCAS DE LOS RIOS NEGRO - NARE", True),
    ("CORPAMAG", "Corporación Autónoma Regional del Magdalena",
     "CORPORACION AUTONOMA REGIONAL DEL MAGDALENA", True),
    ("CORPOAMAZONIA", "Corporación para el Desarrollo Sostenible del Sur de la Amazonia",
     "CORPOAMAZONIA - CORPORACIÓN PARA EL DESARROLLO SOSTENIBLE DEL SUR DE LA AMAZONIA1", True),
    ("CORPOBOYACÁ", "Corporación Autónoma Regional de Boyacá", "CORPOBOYACA", True),
    ("CORPOCALDAS", "Corporación Autónoma Regional de Caldas", "CORPOCALDAS.", True),
    ("CORPOCESAR", "Corporación Autónoma Regional del Cesar", "CORPOCESAR", True),
    ("CORPOCHIVOR", "Corporación Autónoma Regional de Chivor",
     "CORPORACIÓN AUTONOMA REGIONAL DE CHIVOR CORPOCHIVOR", True),
    ("CORPOGUAJIRA", "Corporación Autónoma Regional de La Guajira",
     "CORPORACION AUTONOMA REGIONAL DE LA GUAJIRA", True),
    ("CORPOGUAVIO", "Corporación Autónoma Regional del Guavio",
     "CORPORACION AUTONOMA REGIONAL DEL GUAVIO - CORPOGUAVIO (OFICIAL)", True),
    ("CORPOMOJANA", "Corporación para el Desarrollo Sostenible de La Mojana y el San Jorge",
     "CORPORACION PARA EL DESARROLLO SOSTENIBLE DE LA MOJANA Y EL SAN JORGE CORPOMOJANA", True),
    ("CORPONARIÑO", "Corporación Autónoma Regional de Nariño",
     "CORPORACION AUTONOMA REGIONAL DE NARIÑO", True),
    ("CORPONOR", "Corporación Autónoma Regional de la Frontera Nororiental",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+", True),
    ("CORPORINOQUIA", "Corporación Autónoma Regional de la Orinoquia",
     "CORPORACIÓN AUTÓNOMA REGIONAL DE LA ORINOQUIA", True),
    ("CORPOURABÁ", "Corporación para el Desarrollo Sostenible del Urabá",
     "CORPORACION PARA EL DESARROLLO SOSTENIBLE DEL URABA", True),
    ("CORTOLIMA", "Corporación Autónoma Regional del Tolima", "CORTOLIMA", True),
    ("CRA", "Corporación Autónoma Regional del Atlántico",
     "CORPORACIÓN AUTÓNOMA REGIONAL DEL ATLÁNTICO", False),
    ("CRC", "Corporación Autónoma Regional del Cauca",
     "CORPORACION AUTONOMA REGIONAL DEL CAUCA  12", False),
    ("CRQ", "Corporación Autónoma Regional del Quindío",
     "CORPORACION AUTONOMA REGIONAL DEL QUINDIO", True),
    ("CSB", "Corporación Autónoma Regional del Sur de Bolívar", "CSB", True),
    ("CVC", "Corporación Autónoma Regional del Valle del Cauca", "CVC", True),
    ("CVS", "Corporación Autónoma Regional de los Valles del Sinú y del San Jorge",
     "CORPORACIÓN AUTÓNOMA REGIONAL DE LOS VALLES DEL SINÚ Y DEL SAN JORGE - CVS", True),
]

INSTITUTIONAL_NAMES = [(acronym, name, physical) for acronym, name, physical, _ in CAR_CORE_33]
ADMITTED_ACRONYMS = [(acronym, physical) for acronym, _, physical, ok in CAR_CORE_33 if ok]
BLOCKED_ACRONYMS = [acronym for acronym, _, _, ok in CAR_CORE_33 if not ok]

# Dueño equivocado observado en el HEAD previo, por consulta. Ninguna de estas
# frases puede volver a resolver a esa entidad.
PREVIOUS_WRONG_OWNERS: list[tuple[str, str]] = [
    ("CAM", "CAMARA DE REPRESENTANTES"),
    ("Corporación Autónoma Regional del Alto Magdalena",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CARDIQUE", "CORPORACION AUTONOMA REGIONAL DE CUNDINAMARCA - CAR."),
    ("Corporación Autónoma Regional del Canal del Dique",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CARSUCRE", "CORPORACION AUTONOMA REGIONAL DE CUNDINAMARCA - CAR."),
    ("Corporación Autónoma Regional de Sucre",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("Corporación para el Desarrollo Sostenible del Norte y Oriente Amazónico",
     "CORPOAMAZONIA - CORPORACIÓN PARA EL DESARROLLO SOSTENIBLE DEL SUR DE LA AMAZONIA1"),
    ("Corporación Autónoma Regional para el Desarrollo Sostenible del Chocó",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CORALINA", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("Corporación para el Desarrollo Sostenible del Archipiélago de San Andrés, "
     "Providencia y Santa Catalina", "(Secretaría Distrital de Integración Social)"),
    ("CORANTIOQUIA", "AGENCIA NACIONAL DE TIERRAS - ANT"),
    ("Corporación Autónoma Regional del Centro de Antioquia", "DEPARTAMENTO DE ANTIOQUIA//"),
    ("CORNARE", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("CORPAMAG", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("Corporación Autónoma Regional del Magdalena",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CORPOCALDAS", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("Corporación Autónoma Regional de Caldas",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CORPOCESAR", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("Corporación Autónoma Regional del Cesar",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CORPOCHIVOR", "CORPOBOYACA"),
    ("Corporación Autónoma Regional de Chivor", "CORPOBOYACA"),
    ("CORPOGUAJIRA", "UNIATLANTICO"),
    ("Corporación Autónoma Regional de La Guajira",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CORPOGUAVIO", "UNIATLANTICO"),
    ("Corporación Autónoma Regional del Guavio",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("CORPOMOJANA", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("Corporación para el Desarrollo Sostenible de La Mojana y el San Jorge",
     "CORPOAMAZONIA - CORPORACIÓN PARA EL DESARROLLO SOSTENIBLE DEL SUR DE LA AMAZONIA1"),
    ("CORPONARIÑO", "GOBERNACION DE NARIÑO"),
    ("CORPONOR", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("CORPORINOQUIA", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("CORPOURABÁ", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("Corporación para el Desarrollo Sostenible del Urabá",
     "CORPOAMAZONIA - CORPORACIÓN PARA EL DESARROLLO SOSTENIBLE DEL SUR DE LA AMAZONIA1"),
    ("Corporación Autónoma Regional del Tolima",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("Corporación Autónoma Regional del Atlántico",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("Corporación Autónoma Regional del Sur de Bolívar",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("Corporación Autónoma Regional de los Valles del Sinú y del San Jorge",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
]

# La jurisdicción ambiental no convierte a una corporación en gobierno
# territorial, ni al contrario.
TERRITORIAL_CANARIES: list[tuple[str, str]] = [
    ("Gobernación del Cauca", "GOBERNACIÓN DEL CAUCA"),
    ("Gobernación del Atlántico", "DEPARTAMENTO DEL ATLANTICO**"),
    ("Gobernación de Nariño", "GOBERNACION DE NARIÑO"),
    ("Gobernación de Boyacá", "GOBERNACIÓN DE BOYACÁ"),
    ("Gobernación de Antioquia", "DEPARTAMENTO DE ANTIOQUIA//"),
    ("Gobernación de Sucre", "GOBERNACION DE SUCRE"),
    ("Gobernación de Córdoba", "DEPARTAMENTO DE CORDOBA"),
    ("Gobernación de Santander", "DEPARTAMENTO DE SANTANDER"),
    ("Gobernación del Magdalena", "GOBERNACION DEL MAGDALENA"),
    ("Ministerio de Ambiente y Desarrollo Sostenible",
     "MINISTERIO DE AMBIENTE Y DESARROLLO SOSTENIBLE"),
    ("Minambiente", "MINISTERIO DE AMBIENTE Y DESARROLLO SOSTENIBLE"),
    ("Cámara de Representantes", "CAMARA DE REPRESENTANTES"),
    ("Agencia Nacional de Tierras", "AGENCIA NACIONAL DE TIERRAS - ANT"),
    ("Universidad del Atlántico", "UNIATLANTICO"),
    ("DAGMA", "DEPARTAMENTO ADMINISTRATIVO DE GESTION DEL MEDIO AMBIENTE DAGMA"),
    ("Secretaría Distrital de Ambiente", "SECRETARIA DISTRITAL DE AMBIENTE"),
]

CAR_PHYSICAL_NAMES = {physical for _, _, physical, _ in CAR_CORE_33}

# Aliases retirados: nombraban a otra entidad o a la categoría entera.
REMOVED_ALIASES: list[tuple[str, str]] = [
    ("corpochivor", "CORPOBOYACA"),
    ("corpochivor boyacá", "CORPOBOYACA"),
    ("corporación autónoma regional corpochivor", "CORPOBOYACA"),
    ("corporación autónoma regional corpochivor boyacá", "CORPOBOYACA"),
    ("corporación autónoma regional corpochivor de boyacá", "CORPOBOYACA"),
    ("corporación autónoma regional de chivor", "CORPOBOYACA"),
    ("corporación corpochivor", "CORPOBOYACA"),
    ("el corpochivor", "CORPOBOYACA"),
    ("la corpochivor", "CORPOBOYACA"),
    ("cor", "CORPORACION AUTONOMA REGIONAL DE NARIÑO"),
    ("corporación autónoma regional",
     "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("corporación regional", "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
    ("la corporación autónoma", "CORPORACION AUTONOMA REGIONAL DE LA FRONTERA NORORIENTAL+"),
]


# Alcanzables por el ``QueryRouter`` con el nombre institucional. El resto sólo
# llega por sigla o sólo por ``resolve_entidad``; ver ``ROUTER_BLOCKED_BY_D6``.
ROUTER_REACHABLE = [
    (acronym, name, physical)
    for acronym, name, physical, _ in CAR_CORE_33
    if acronym in {
        "CARDIQUE", "CDA", "CDMB", "CORALINA", "CORMACARENA", "CORNARE", "CORPOAMAZONIA",
        "CORPOCHIVOR", "CORPOGUAVIO", "CORPOMOJANA", "CORPONOR", "CORPORINOQUIA",
        "CORPOURABÁ", "CVS",
    }
]

# Corporaciones que ningún formulario alcanza por el router: su sigla o su
# nombre termina con el nombre de un departamento y el scan las descarta.
ROUTER_BLOCKED_BY_D6 = {
    "CARSUCRE", "CODECHOCÓ", "CORANTIOQUIA", "CORPOBOYACÁ", "CORPOCALDAS", "CORPOCESAR",
    "CORPOGUAJIRA", "CORPONARIÑO", "CORTOLIMA", "CRA", "CRC",
}


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def router(resolver: EntityResolver) -> QueryRouter:
    return QueryRouter(entity_resolver=resolver)


@pytest.mark.parametrize(("acronym", "name", "physical"), INSTITUTIONAL_NAMES)
def test_institutional_name_resolves_to_its_own_buyer(
    resolver: EntityResolver, acronym: str, name: str, physical: str
) -> None:
    assert resolver.owners_by_alias.get(normalize_text(name)) == {physical}
    result = resolver.resolve_entidad(name)
    assert result.value == physical
    assert result.method == "exact"


@pytest.mark.parametrize(("acronym", "physical"), ADMITTED_ACRONYMS)
def test_admitted_acronym_has_one_exact_owner(
    resolver: EntityResolver, acronym: str, physical: str
) -> None:
    assert resolver.owners_by_alias.get(normalize_text(acronym)) == {physical}
    result = resolver.resolve_entidad(acronym)
    assert result.value == physical
    assert result.method == "exact"


@pytest.mark.parametrize("acronym", BLOCKED_ACRONYMS)
def test_blocked_acronym_is_owned_by_nobody(resolver: EntityResolver, acronym: str) -> None:
    """Una sigla ambigua no entra: ni para la corporación ni para nadie más."""
    assert resolver.owners_by_alias.get(normalize_text(acronym), set()) == set()


@pytest.mark.parametrize("acronym", ["CDA", "CRA"])
def test_ambiguous_short_acronym_fails_closed(resolver: EntityResolver, acronym: str) -> None:
    assert resolver.resolve_entidad(acronym).value is None


@pytest.mark.parametrize(("phrase", "forbidden"), PREVIOUS_WRONG_OWNERS)
def test_previous_wrong_owner_never_returns(
    resolver: EntityResolver, phrase: str, forbidden: str
) -> None:
    assert resolver.resolve_entidad(phrase).value != forbidden


@pytest.mark.parametrize(("acronym", "name", "physical"), INSTITUTIONAL_NAMES)
def test_router_never_returns_a_foreign_owner(
    router: QueryRouter, acronym: str, name: str, physical: str
) -> None:
    """Por la vía del producto, una CAR resuelve a sí misma o a nadie."""
    resolved = router.parse(f"contratos de {name} en 2025").params.get("entidad_resolved")
    assert resolved in (physical, None)


@pytest.mark.parametrize(("acronym", "physical"), ADMITTED_ACRONYMS)
def test_router_never_returns_a_foreign_owner_for_an_acronym(
    router: QueryRouter, acronym: str, physical: str
) -> None:
    resolved = router.parse(f"contratos de {acronym} en 2025").params.get("entidad_resolved")
    assert resolved in (physical, None)


@pytest.mark.parametrize(("acronym", "name", "physical"), ROUTER_REACHABLE)
def test_router_reaches_the_buyer_without_fabricating_territory(
    router: QueryRouter, acronym: str, name: str, physical: str
) -> None:
    params = router.parse(f"contratos de {name} en 2025").params
    assert params.get("entidad_resolved") == physical
    assert not params.get("departamento_resolved")
    resolution = params.get("entidad_resolution") or {}
    assert not str(resolution.get("method", "")).startswith("fuzzy")


def test_router_blocked_set_does_not_grow(router: QueryRouter) -> None:
    """La deuda D6 no puede crecer en silencio ni convertirse en dueño ajeno.

    ``_is_unqualified_dept`` compara con ``endswith`` sin frontera de palabra,
    de modo que ``corpocaldas`` "termina en" el departamento *Caldas* y el scan
    lo descarta como topónimo desnudo. Las once corporaciones de abajo sólo son
    alcanzables por ``resolve_entidad``; arreglarlo es tocar D6, fuera de R2D.
    """
    blocked = set()
    for acronym, name, physical, admitted in CAR_CORE_33:
        forms = [name] + ([acronym] if admitted else [])
        resolved = {
            router.parse(f"contratos de {form} en 2025").params.get("entidad_resolved")
            for form in forms
        }
        assert resolved <= {physical, None}
        if physical not in resolved:
            blocked.add(acronym)
    assert blocked == ROUTER_BLOCKED_BY_D6


@pytest.mark.parametrize(("phrase", "expected"), TERRITORIAL_CANARIES)
def test_neighbouring_institutions_keep_their_identity(
    resolver: EntityResolver, phrase: str, expected: str
) -> None:
    assert resolver.resolve_entidad(phrase).value == expected


@pytest.mark.parametrize(("phrase", "_expected"), TERRITORIAL_CANARIES)
def test_no_cross_class_hijack_into_the_car_core(
    resolver: EntityResolver, phrase: str, _expected: str
) -> None:
    assert resolver.resolve_entidad(phrase).value not in CAR_PHYSICAL_NAMES


CORMAGDALENA = "CORPORACIÓN AUTÓNOMA DEL RÍO GRANDE DE LA MAGDALENA**"


@pytest.mark.parametrize(
    "phrase",
    ["Cormagdalena", "Corporación Autónoma Regional del Río Grande de la Magdalena"],
)
def test_cormagdalena_is_not_captured_by_the_car_core(
    resolver: EntityResolver, phrase: str
) -> None:
    """Cormagdalena queda fuera de las 33; ningún alias del lote puede tomarla.

    Entró al gazetteer como defensa, no como miembro: al registrar CORPAMAG,
    ``Corporación Autónoma Regional del Río Grande de la Magdalena`` empezó a
    caer en la CAR del Magdalena por fuzzy. Su identidad propia —NIT 829000127,
    ente corporativo especial de la Ley 161 de 1994— es la única defensa que no
    depende del azar léxico.
    """
    result = resolver.resolve_entidad(phrase)
    assert result.value not in CAR_PHYSICAL_NAMES
    assert result.value == CORMAGDALENA
    assert result.method == "exact"


@pytest.mark.parametrize(("alias", "previous_owner"), REMOVED_ALIASES)
def test_removed_alias_no_longer_belongs_to_the_wrong_owner(
    resolver: EntityResolver, alias: str, previous_owner: str
) -> None:
    assert previous_owner not in resolver.owners_by_alias.get(normalize_text(alias), set())


def test_every_car_core_alias_has_exactly_one_owner(resolver: EntityResolver) -> None:
    """Ninguna clave del lote puede quedar reclamada por dos compradores."""
    claimed = {normalize_text(name) for _, name, _, _ in CAR_CORE_33}
    claimed |= {normalize_text(acronym) for acronym, _ in ADMITTED_ACRONYMS}
    shared = {
        key: sorted(owners)
        for key, owners in resolver.owners_by_alias.items()
        if key in claimed and len(owners) > 1
    }
    assert shared == {}


def test_the_universe_is_thirty_three_and_excludes_cormagdalena() -> None:
    assert len(CAR_CORE_33) == 33
    assert len(CAR_PHYSICAL_NAMES) == 33
    assert all("RIO GRANDE" not in physical.upper() for physical in CAR_PHYSICAL_NAMES)
