"""R2B — cobertura nacional verificada de ministerios y departamentos administrativos.

Los canarios se escribieron contra el HEAD previo a la mutacion del gazetteer.
Cada frase aceptada debe quedar respaldada por ownership exacto y unico; los
compradores relacionados (fondos, unidades militares) conservan su identidad.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.utils.money import normalize_text

ALIASES_DB = Path("app/data/aliases_db.json")

RELACIONES = "MINISTERIO DE RELACIONES EXTERIORES"
DEFENSA = "MINISTERIO DE DEFENSA NACIONAL"
MINTIC = "MINISTERIO DE TECNOLOGÍAS DE LA INFORMACIÓN Y LAS COMUNICACIONES DE COLOMBIA."
MINCIT = "MINISTERIO DE COMERCIO INDUSTRIA Y TURISMO - MINCIT"
MINAMBIENTE = "MINISTERIO DE AMBIENTE Y DESARROLLO SOSTENIBLE"
MINVIVIENDA = "MINVIVIENDA"
CULTURA = "Ministerio de las Culturas, las Artes y los Saberes"
DAFP = "DEPARTAMENTO ADMINISTRATIVO DE LA FUNCIÓN PÚBLICA ENTIDAD"
DNI = "DIRECCION NACIONAL DE INTELIGENCIA"

ACCEPTED_CANARIES = [
    ("Ministerio de Relaciones Exteriores", RELACIONES),
    ("Cancillería", RELACIONES),
    ("MinDefensa", DEFENSA),
    ("Ministerio de Defensa Nacional", DEFENSA),
    ("MinTIC", MINTIC),
    ("Ministerio TIC", MINTIC),
    ("Mincomercio", MINCIT),
    ("Minambiente", MINAMBIENTE),
    ("Ministerio de Ambiente", MINAMBIENTE),
    ("Minvivienda", MINVIVIENDA),
    ("Ministerio de Vivienda", MINVIVIENDA),
    ("Ministerio de Cultura", CULTURA),
    ("Ministerio de las Culturas, las Artes y los Saberes", CULTURA),
    ("DAFP", DAFP),
    ("Función Pública", DAFP),
    ("Dirección Nacional de Inteligencia", DNI),
]

MINISTRIES_18 = [
    ("Ministerio del Interior", "MINISTERIO DEL INTERIOR"),
    ("Ministerio de Relaciones Exteriores", RELACIONES),
    ("Ministerio de Hacienda y Crédito Público", "MINISTERIO DE HACIENDA Y CREDITO PUBLICO"),
    ("Ministerio de Justicia y del Derecho", "MINJUSTICIA.."),
    ("Ministerio de Defensa Nacional", DEFENSA),
    (
        "Ministerio de Agricultura y Desarrollo Rural",
        "MINISTERIO DE AGRICULTURA Y DESARROLLO RURAL",
    ),
    ("Ministerio de Salud y Protección Social", "MINSALUD"),
    ("Ministerio del Trabajo", "MINISTERIO DEL TRABAJO NIVEL CENTRAL*/"),
    ("Ministerio de Minas y Energía", "MINISTERIO DE MINAS Y ENERGIA"),
    ("Ministerio de Comercio, Industria y Turismo", MINCIT),
    ("Ministerio de Educación Nacional", "MINISTERIO DE EDUCACION NACIONAL (MEN)"),
    ("Ministerio de Ambiente y Desarrollo Sostenible", MINAMBIENTE),
    ("Ministerio de Vivienda, Ciudad y Territorio", MINVIVIENDA),
    ("Ministerio de Tecnologías de la Información y las Comunicaciones", MINTIC),
    ("Ministerio de Transporte", "MINISTERIO DE TRANSPORTE*"),
    ("Ministerio de Cultura", CULTURA),
    ("Ministerio del Deporte", "MINISTERIO DEL DEPORTE"),
    ("Ministerio de Ciencia, Tecnología e Innovación", "MINCIENCIAS"),
]

ADMIN_DEPARTMENTS_6 = [
    (
        "Departamento Administrativo de la Presidencia de la República",
        "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA",
    ),
    ("Departamento Nacional de Planeación", "DEPARTAMENTO NACIONAL DE PLANEACIÓN"),
    ("Departamento Administrativo de la Función Pública", DAFP),
    ("Departamento Administrativo para la Prosperidad Social", "PROSPERIDAD SOCIAL"),
    ("Dirección Nacional de Inteligencia", DNI),
    (
        "Departamento Administrativo Nacional de Estadística",
        "DEPARTAMENTO ADMINISTRATIVO NACIONAL DE ESTADISTICA (DANE)",
    ),
]


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def router(resolver: EntityResolver) -> QueryRouter:
    return QueryRouter(entity_resolver=resolver)


@pytest.mark.parametrize(("phrase", "expected"), ACCEPTED_CANARIES)
def test_accepted_phrase_has_one_exact_owner(
    resolver: EntityResolver, phrase: str, expected: str
) -> None:
    key = normalize_text(phrase)
    assert resolver.owners_by_alias.get(key) == {expected}
    result = resolver.resolve_entidad(phrase)
    assert result.value == expected
    assert result.method == "exact"


@pytest.mark.parametrize(("phrase", "expected"), ACCEPTED_CANARIES)
def test_accepted_phrase_routes_to_buyer_without_fabricated_territory(
    router: QueryRouter, phrase: str, expected: str
) -> None:
    params = router.parse(f"contratos de {phrase} en 2025").params
    assert params.get("entidad_resolved") == expected
    assert not params.get("departamento_resolved")
    resolution = params.get("entidad_resolution") or {}
    assert not str(resolution.get("method", "")).startswith("fuzzy")


@pytest.mark.parametrize(("phrase", "expected"), MINISTRIES_18)
def test_active_ministries_matrix_is_18_of_18(
    router: QueryRouter, phrase: str, expected: str
) -> None:
    params = router.parse(f"contratos de {phrase} en 2025").params
    assert params.get("entidad_resolved") == expected
    assert (params.get("entidad_resolution") or {}).get("method") == "scan_exact"


@pytest.mark.parametrize(("phrase", "expected"), ADMIN_DEPARTMENTS_6)
def test_administrative_departments_matrix_is_6_of_6(
    router: QueryRouter, phrase: str, expected: str
) -> None:
    params = router.parse(f"contratos de {phrase} en 2025").params
    assert params.get("entidad_resolved") == expected
    assert (params.get("entidad_resolution") or {}).get("method") == "scan_exact"


@pytest.mark.parametrize(
    ("phrase", "expected"),
    [
        ("Fondo Rotatorio del Ministerio de Relaciones Exteriores", "FONDO ROTATORIO**"),
        (
            "Fondo Único de Tecnologías de la Información y las Comunicaciones",
            "FONDO UNICO DE TECNOLOGÍAS DE LA INFORMACIÓN Y LAS COMUNICACIONES",
        ),
        ("Hospital Militar Central", "HOSPITAL MILITAR CENTRAL"),
        (
            "Agencia Logística de las Fuerzas Militares",
            "AGENCIA LOGISTICA DE LAS FUERZAS MILITARES",
        ),
        ("Fuerza Aeroespacial Colombiana", "FUERZA AEROESPACIAL COLOMBIANA"),
    ],
)
def test_related_buyers_keep_their_own_identity(
    resolver: EntityResolver, phrase: str, expected: str
) -> None:
    result = resolver.resolve_entidad(phrase)
    assert result.value == expected
    assert result.method == "exact"


def test_bare_dni_is_not_a_buyer_alias(resolver: EntityResolver) -> None:
    assert resolver.owners_by_alias.get("dni", set()) == set()
    assert resolver.resolve_entidad("DNI").value is None


@pytest.mark.parametrize(
    "query",
    [
        "contratos que exigen DNI en el documento de identidad",
        "servicios para tramitar el DNI de una persona",
        "validación de documento nacional de identidad DNI",
    ],
)
def test_bare_dni_does_not_hijack_non_institutional_intent(router: QueryRouter, query: str) -> None:
    params = router.parse(query).params
    assert params.get("entidad_resolved") != DNI


def test_historical_equality_ministry_stays_outside_active_matrix() -> None:
    assert all("Igualdad y Equidad" not in phrase for phrase, _ in MINISTRIES_18)
