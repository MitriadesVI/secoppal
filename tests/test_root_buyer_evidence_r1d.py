"""R1D — una peticion departamental raiz exige evidencia de ownership raiz.

El nombre fisico que SECOP asigna a un comprador no demuestra por si solo su
identidad institucional.  Estas pruebas fijan las tres consecuencias:

* una subentidad cuyo nombre contiene el del padre no puede suplantarlo;
* un comprador raiz puede estar respaldado por aliases aunque su nombre fisico
  no contenga el territorio (Tolima);
* la ausencia de evidencia falla cerrada, sin degradarse a territorio.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


ALIASES_DB = Path("app/data/aliases_db.json")

CUNDINAMARCA_SALUD = "DEPARTAMENTO DE CUNDINAMARCA- SECRETARIA DE SALUD"
VALLE_INFRAESTRUCTURA = "SECRETARIA DE INFRAESTRUCTURA - GOBERNACION DEL VALLE"
TOLIMA = "GOBIERNO DEPARTAMENTAL"
CAUCA = "GOBERNACIÓN DEL CAUCA"
ATLANTICO = "DEPARTAMENTO DEL ATLANTICO**"
BOYACA = "GOBERNACIÓN DE BOYACÁ"

MISSING_GOVERNORSHIPS = ("Guainía", "Guaviare", "Vaupés", "Vichada")


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def router(resolver: EntityResolver) -> QueryRouter:
    return QueryRouter(entity_resolver=resolver)


def _assert_router_fails_closed(router: QueryRouter, buyer: str) -> None:
    params = router.parse(f"contratos de {buyer} en 2025").params
    resolution = params.get("entidad_resolution") or {}
    assert params.get("entidad")
    assert not params.get("entidad_resolved")
    assert resolution.get("clarification_needed") is True
    assert not params.get("departamento_resolved")


@pytest.mark.parametrize(
    ("buyer", "forbidden_child"),
    [
        ("gobernación de Cundinamarca", CUNDINAMARCA_SALUD),
        ("gobernación del Valle", VALLE_INFRAESTRUCTURA),
    ],
)
def test_root_request_cannot_resolve_to_child(
    resolver: EntityResolver,
    router: QueryRouter,
    buyer: str,
    forbidden_child: str,
) -> None:
    direct = resolver.resolve_entidad(buyer)
    assert direct.value is None
    assert direct.clarification_needed is True
    assert direct.value != forbidden_child
    _assert_router_fails_closed(router, buyer)


@pytest.mark.parametrize(
    "buyer",
    [
        "gobernación del Tolima",
        "gobernación de Tolima",
        "gobierno departamental del Tolima",
    ],
)
def test_tolima_root_alias_evidence_overrides_opaque_physical_name(
    resolver: EntityResolver,
    router: QueryRouter,
    buyer: str,
) -> None:
    assert resolver.resolve_entidad(buyer).value == TOLIMA
    params = router.parse(f"contratos de {buyer} en 2025").params
    assert params.get("entidad_resolved") == TOLIMA
    assert not params.get("departamento_resolved")


def test_gobierno_departamental_cauca_uses_same_target_root_evidence(
    resolver: EntityResolver,
    router: QueryRouter,
) -> None:
    assert resolver.resolve_entidad("gobierno departamental del Cauca").value == CAUCA
    params = router.parse(
        "contratos del gobierno departamental del Cauca en 2025"
    ).params
    assert params.get("entidad_resolved") == CAUCA
    assert params.get("entidad_resolved") != TOLIMA
    assert not params.get("departamento_resolved")


@pytest.mark.parametrize(
    ("buyer", "expected"),
    [
        ("gobernación del Cauca", CAUCA),
        ("gobernación del Atlántico", ATLANTICO),
        ("gobernación de Boyacá", BOYACA),
    ],
)
def test_known_roots_keep_resolving_from_unique_alias_ownership(
    resolver: EntityResolver,
    buyer: str,
    expected: str,
) -> None:
    result = resolver.resolve_entidad(buyer)
    assert result.value == expected
    assert result.method == "exact"


@pytest.mark.parametrize("department", MISSING_GOVERNORSHIPS)
def test_missing_roots_continue_to_fail_closed(
    resolver: EntityResolver,
    router: QueryRouter,
    department: str,
) -> None:
    buyer = f"gobernación del {department}"
    direct = resolver.resolve_entidad(buyer)
    assert direct.value is None
    assert direct.clarification_needed is True
    _assert_router_fails_closed(router, buyer)


def test_san_andres_without_natural_root_ownership_never_becomes_santander(
    resolver: EntityResolver,
    router: QueryRouter,
) -> None:
    buyer = "gobernación de San Andrés"
    result = resolver.resolve_entidad(buyer)
    assert result.value is None
    assert result.clarification_needed is True
    _assert_router_fails_closed(router, buyer)


@pytest.mark.parametrize(
    "official_name",
    [
        "HOSPITAL DEPARTAMENTAL DEL CAUCA",
        "DEPARTAMENTO DE CUNDINAMARCA - SECRETARIA DE SALUD",
        "INSTITUTO DEPARTAMENTAL DE SALUD DE NARIÑO",
    ],
)
def test_physical_child_name_alone_is_not_root_evidence(
    tmp_path: Path,
    official_name: str,
) -> None:
    alias_path = tmp_path / "aliases.json"
    alias_path.write_text(
        json.dumps([
            {
                "official_name": official_name,
                "aliases": [official_name.lower()],
            }
        ]),
        encoding="utf-8",
    )
    isolated = EntityResolver(alias_path)

    target = (
        "Cauca" if "CAUCA" in official_name
        else "Cundinamarca" if "CUNDINAMARCA" in official_name
        else "Nariño"
    )
    result = isolated.resolve_entidad(f"gobernación de {target}")
    assert result.value is None
    assert result.clarification_needed is True


@pytest.mark.parametrize(
    ("alias", "expected"),
    [
        ("secretaría de salud de Cundinamarca", CUNDINAMARCA_SALUD),
        ("secretaría de infraestructura del Valle", VALLE_INFRAESTRUCTURA),
    ],
)
def test_explicit_child_aliases_remain_resolvable(
    resolver: EntityResolver,
    alias: str,
    expected: str,
) -> None:
    result = resolver.resolve_entidad(alias)
    assert result.value == expected
    assert result.method == "exact"


@pytest.mark.parametrize("reverse_records", [False, True])
def test_multi_owner_root_alias_fails_closed_independent_of_load_order(
    tmp_path: Path,
    reverse_records: bool,
) -> None:
    records = [
        {
            "official_name": "ROOT BUYER A",
            "aliases": ["gobernación del Cauca"],
        },
        {
            "official_name": "ROOT BUYER B",
            "aliases": ["gobernación del Cauca"],
        },
    ]
    if reverse_records:
        records.reverse()
    alias_path = tmp_path / "aliases.json"
    alias_path.write_text(json.dumps(records), encoding="utf-8")

    isolated = EntityResolver(alias_path)
    result = isolated.resolve_entidad("gobernación del Cauca")
    assert result.value is None
    assert result.clarification_needed is True
    assert result.method == "root_alias_ambiguous"
    assert isolated.scan_entity("gobernacion del cauca") is None


def test_generic_physical_name_is_not_a_national_scan_alias(tmp_path: Path) -> None:
    alias_path = tmp_path / "aliases.json"
    alias_path.write_text(
        json.dumps([
            {
                "official_name": TOLIMA,
                "aliases": [
                    "gobernación del Tolima",
                    "gobierno departamental del Tolima",
                ],
            }
        ]),
        encoding="utf-8",
    )
    isolated = EntityResolver(alias_path)

    assert isolated.scan_entity("gobierno departamental del cauca") is None
    tolima = isolated.scan_entity("gobierno departamental del tolima")
    assert tolima is not None
    assert tolima.official_name == TOLIMA
    assert tolima.matched_alias == "gobierno departamental del tolima"
