"""R2A — cobertura de compradores raíz departamentales verificada en registro.

R1D dejó 25/32 gobernaciones resueltas y 7 fallando cerrado. R2A no busca
32/32: busca que cada raíz que resuelva tenga identidad **demostrada** en los
datasets de SECOP II, y que las que no la tienen sigan fallando cerrado.

Las dos mitades deben leerse juntas:

* ``TestVerifiedRootsResolve``  — las cuatro identidades verificadas en registro.
* ``TestUnverifiedRootsStayClosed`` — las tres que NO pueden inventarse.

La segunda mitad es la que protege el invariante: preferimos 29 correctas y 3
honestamente desconocidas antes que 32 con identidades fabricadas.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter

ALIASES_DB = Path("app/data/aliases_db.json")

SAN_ANDRES = (
    "GOBERNACIÓN DEL DEPARTAMENTO ARCHIPIELAGO DE SAN ANDRES "
    "PROVIDENCIA Y SANTA CATALINA"
)
GUAVIARE = "GOBERNACION DEL GUAVIARE"
VAUPES = "GOBERNACION DEL VAUPES"
VICHADA = "DEPARTAMENTO DE VICHADA"

# (consulta natural, official_name esperado, departamento canónico)
VERIFIED_ROOTS = [
    ("gobernación de San Andrés", SAN_ANDRES, "San Andrés, Providencia y Santa Catalina"),
    ("gobernación del Guaviare", GUAVIARE, "Guaviare"),
    ("gobernación del Vaupés", VAUPES, "Vaupés"),
    ("gobernación del Vichada", VICHADA, "Vichada"),
]

# Sin comprador raíz en SECOP II: la ausencia se reporta, no se rellena.
UNVERIFIED_ROOTS = [
    ("gobernación de Cundinamarca", "Cundinamarca"),
    ("gobernación del Valle del Cauca", "Valle del Cauca"),
    ("gobernación del Guainía", "Guainía"),
]

CUNDINAMARCA_SALUD = "DEPARTAMENTO DE CUNDINAMARCA- SECRETARIA DE SALUD"
VALLE_INFRAESTRUCTURA = "SECRETARIA DE INFRAESTRUCTURA - GOBERNACION DEL VALLE"


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def router(resolver: EntityResolver) -> QueryRouter:
    return QueryRouter(entity_resolver=resolver)


class TestVerifiedRootsResolve:
    """Cada identidad aceptada trae evidencia de registro, no parecido léxico."""

    @pytest.mark.parametrize(("query", "expected", "department"), VERIFIED_ROOTS)
    def test_resolves_to_the_verified_root(
        self, resolver: EntityResolver, query: str, expected: str, department: str
    ) -> None:
        result = resolver.resolve_entidad(query)
        assert result.value == expected
        assert result.clarification_needed is False
        # INV: nunca por fuzzy. La identidad raíz sale del ownership del alias.
        assert not result.method.startswith("fuzzy")
        assert result.method in {"exact", "root_alias_evidence"}

    @pytest.mark.parametrize(("query", "expected", "department"), VERIFIED_ROOTS)
    def test_root_evidence_is_unique_for_the_department(
        self, resolver: EntityResolver, query: str, expected: str, department: str
    ) -> None:
        evidence = resolver._root_evidence_by_department.get(department, {})
        assert list(evidence) == [expected], (
            f"{department} debe tener exactamente un comprador raíz: {list(evidence)}"
        )
        assert evidence[expected], "el owner raíz necesita al menos un alias de soporte"

    @pytest.mark.parametrize(("query", "expected", "department"), VERIFIED_ROOTS)
    def test_alias_ownership_is_unambiguous(
        self, resolver: EntityResolver, query: str, expected: str, department: str
    ) -> None:
        for alias in resolver._root_evidence_by_department[department][expected]:
            assert resolver.owners_by_alias[alias] == {expected}

    @pytest.mark.parametrize(("query", "expected", "department"), VERIFIED_ROOTS)
    def test_router_preserves_buyer_intent_without_territorial_leak(
        self, router: QueryRouter, query: str, expected: str, department: str
    ) -> None:
        params = router.parse(f"contratos de {query} en 2025").params
        assert params.get("entidad_resolved") == expected
        # Un comprador no se convierte además en un filtro de territorio.
        assert not params.get("departamento_resolved")

    @pytest.mark.parametrize(("query", "expected", "department"), VERIFIED_ROOTS)
    def test_no_wrong_child_can_answer_for_the_root(
        self, resolver: EntityResolver, query: str, expected: str, department: str
    ) -> None:
        value = resolver.resolve_entidad(query).value
        assert value is not None
        # Una subentidad lleva su unidad en el nombre; la raíz no.
        assert " - " not in value and "- SECRETARIA" not in value


class TestTerritoryStaysTerritory:
    """La gramática territorial no se convierte en comprador por el lote."""

    @pytest.mark.parametrize(
        "department",
        ["San Andrés", "Guaviare", "Vaupés", "Vichada"],
    )
    def test_en_el_departamento_de_x_is_territory(
        self, router: QueryRouter, department: str
    ) -> None:
        params = router.parse(f"contratos en el departamento de {department}").params
        assert params.get("departamento_resolved")
        assert not params.get("entidad_resolved")


class TestUnverifiedRootsStayClosed:
    """Sin comprador raíz en SECOP II no hay respuesta que inventar."""

    @pytest.mark.parametrize(("query", "department"), UNVERIFIED_ROOTS)
    def test_direct_resolution_fails_closed(
        self, resolver: EntityResolver, query: str, department: str
    ) -> None:
        result = resolver.resolve_entidad(query)
        assert result.value is None
        assert result.clarification_needed is True

    @pytest.mark.parametrize(("query", "department"), UNVERIFIED_ROOTS)
    def test_router_fails_closed_without_degrading_to_territory(
        self, router: QueryRouter, query: str, department: str
    ) -> None:
        params = router.parse(f"contratos de {query} en 2025").params
        resolution = params.get("entidad_resolution") or {}
        assert params.get("entidad")
        assert not params.get("entidad_resolved")
        assert resolution.get("clarification_needed") is True
        assert not params.get("departamento_resolved")

    @pytest.mark.parametrize(("query", "department"), UNVERIFIED_ROOTS)
    def test_no_root_evidence_is_manufactured(
        self, resolver: EntityResolver, query: str, department: str
    ) -> None:
        assert resolver._root_evidence_by_department.get(department, {}) == {}

    def test_cundinamarca_root_never_becomes_a_secretaria(
        self, resolver: EntityResolver
    ) -> None:
        assert resolver.resolve_entidad("gobernación de Cundinamarca").value != CUNDINAMARCA_SALUD

    def test_valle_root_never_becomes_a_secretaria(self, resolver: EntityResolver) -> None:
        assert resolver.resolve_entidad("gobernación del Valle").value != VALLE_INFRAESTRUCTURA


class TestSanAndresReconciliation:
    """San Andrés se reconcilia: no se crea una entidad nueva."""

    def test_the_archipelago_root_is_a_single_record(self, resolver: EntityResolver) -> None:
        """La raíz territorial del archipiélago tiene un solo dueño.

        El oráculo medía esto por substring ``ARCHIPIELAGO`` en el nombre
        oficial, y R2D demostró que ese proxy era más ancho que el invariante:
        CORALINA lleva el archipiélago en su nombre físico SECOP y sin embargo
        es otro comprador, con NIT propio, que no compite por la raíz. Lo que
        no puede duplicarse es la evidencia raíz y el ownership de las frases
        raíz; eso es lo que se afirma aquí.
        """
        department = "San Andrés, Providencia y Santa Catalina"
        assert list(resolver._root_evidence_by_department.get(department, {})) == [SAN_ANDRES]
        for phrase in ("gobernacion de san andres", "departamento de san andres"):
            assert resolver.owners_by_alias.get(phrase) == {SAN_ANDRES}

    def test_the_archipelago_corporation_is_not_the_territorial_root(
        self, resolver: EntityResolver
    ) -> None:
        """CORALINA es autoridad ambiental, no gobierno del archipiélago."""
        coralina = resolver.resolve_entidad("CORALINA").value
        assert coralina is not None
        assert coralina != SAN_ANDRES
        assert coralina not in resolver._root_evidence_by_department.get(
            "San Andrés, Providencia y Santa Catalina", {}
        )
        assert resolver.resolve_entidad("gobernación de San Andrés").value == SAN_ANDRES

    def test_bare_toponym_is_not_a_buyer_alias(self, resolver: EntityResolver) -> None:
        """'san andrés' desnudo pertenece también a la autoridad geográfica."""
        assert "san andres" not in resolver.aliases_by_official.get(SAN_ANDRES, set())

    def test_the_hospital_keeps_its_own_identity(self, resolver: EntityResolver) -> None:
        assert resolver.resolve_entidad("hospital san andrés").value == "HOSPITAL SAN ANDRES"
