"""GAZETTEER-ALIAS-OWNERSHIP-R1C — canarios de propiedad de aliases.

Un alias no significa "esta frase podría tener alguna relación con esta
entidad". Significa "cuando el usuario usa esta frase como comprador, esta
entidad es una interpretación institucionalmente válida". La cobertura no
justifica un ownership incorrecto: es preferible que la Gobernación de
Cundinamarca quede sin resolver a que sea un hospital de Sopó.

Los tests se dividen en dos mitades que deben leerse juntas:

* ``TestHijacksRemoved``  — lo que R1C corrige; ROJOS antes de la limpieza.
* ``TestLegitimateOwnershipKept`` — lo que R1C NO puede romper al limpiar.

Se prueba sobre ``alias_index`` (la resolución exacta) además de sobre
``scan_entity``/``resolve_entidad`` porque el ownership es una propiedad del
dato, y el gate ``_strict_rewrite_target`` puede enmascarar un hijack: hoy
"Gobernación del Atlántico" acierta por fuzzy pese a que su alias exacto
pertenece a una ESE.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.utils.money import normalize_text

ALIASES_DB = Path("app/data/aliases_db.json")

HOSPITAL_SOPO = "E.S.E. HOSPITAL DIVINO SALVADOR DE SOPO"
DADI = "GOBERNACION DEL VALLE DEL CAUCA - DADI"
HOSPITAL_SAN_ANDRES = "HOSPITAL SAN ANDRES"
GOBIERNO_DEPARTAMENTAL = "GOBIERNO DEPARTAMENTAL"
MINMINAS = "MINISTERIO DE MINAS Y ENERGIA"
ANM = "ANM"


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


def _owner(resolver: EntityResolver, phrase: str) -> str | None:
    """Dueño del alias en la resolución exacta."""
    return resolver.alias_index.get(normalize_text(phrase))


def _scanned(resolver: EntityResolver, phrase: str) -> str | None:
    result = resolver.scan_entity(normalize_text(phrase))
    return result.official_name if result else None


class TestHijacksRemoved:
    """Aliases cuyo dueño actual es institucionalmente inválido."""

    # A — Cundinamarca / Sopó
    @pytest.mark.parametrize(
        "phrase",
        ["gobernación de cundinamarca", "gobierno de cundinamarca",
         "departamento de cundinamarca", "alcaldía de sopó", "municipio de sopó"],
    )
    def test_sopo_hospital_does_not_own_government_phrases(self, resolver, phrase):
        assert _owner(resolver, phrase) != HOSPITAL_SOPO
        assert _scanned(resolver, phrase) != HOSPITAL_SOPO

    # B — Valle / DADI
    @pytest.mark.parametrize(
        "phrase",
        ["gobernación del valle", "gobernación del valle del cauca",
         "gobierno del valle del cauca", "departamento del valle del cauca"],
    )
    def test_valle_parent_phrase_owned_by_no_subunit(self, resolver, phrase):
        """La frase del comprador padre no puede pertenecer a una subentidad.

        Si la raíz no existe en el gazetteer el resultado correcto es
        FAIL_CLOSED, no una secretaría elegida por accidente de longitud.
        """
        for owner in (_owner(resolver, phrase), _scanned(resolver, phrase)):
            assert owner is None or "-" not in owner, (
                f"'{phrase}' sigue perteneciendo a la subentidad {owner!r}"
            )

    def test_valle_cross_territory_alias_removed(self, resolver):
        """DADI (Valle) no puede poseer la frase de la Gobernación del Cauca."""
        assert _owner(resolver, "gobernación del cauca") != DADI

    # C — San Andrés
    @pytest.mark.parametrize("phrase", ["san andres", "san andrés"])
    def test_bare_territory_is_not_the_hospital(self, resolver, phrase):
        """El archipiélago no es un hospital de Tumaco (Nariño)."""
        assert _owner(resolver, phrase) != HOSPITAL_SAN_ANDRES
        assert resolver.resolve_entidad(phrase).value != HOSPITAL_SAN_ANDRES

    # D — Tolima
    def test_unqualified_gobierno_departamental_has_no_owner(self, resolver):
        """"gobierno departamental" sin territorio no identifica a nadie.

        Capturaría "gobierno departamental del Cauca" y produciría un WHERE
        contradictorio: comprador del Tolima ∧ territorio del Cauca.
        """
        assert _owner(resolver, "gobierno departamental") is None
        assert _owner(resolver, "gobernación departamental") is None

    # E — ANM / MinMinas
    @pytest.mark.parametrize(
        "phrase",
        ["ministerio de minas", "ministerio de minas y energía",
         "ministerio de minas y energia"],
    )
    def test_ministry_phrases_belong_to_the_ministry(self, resolver, phrase):
        assert _owner(resolver, phrase) == MINMINAS
        assert resolver.resolve_entidad(phrase).value == MINMINAS

    # Hijacks que el gate strict_rewrite venía enmascarando
    def test_atlantico_governor_phrase_is_not_a_hospital(self, resolver):
        """Canario ADR-040: hoy acierta por fuzzy, no por ownership."""
        owner = _owner(resolver, "gobernación del atlántico")
        assert owner is None or "SOCIAL DEL ESTADO" not in owner.upper()

    def test_boyaca_governor_phrase_is_not_a_university(self, resolver):
        assert _owner(resolver, "gobernación de boyacá") != "UPTC"


class TestLegitimateOwnershipKept:
    """Lo que la limpieza NO puede romper."""

    @pytest.mark.parametrize(
        "phrase",
        ["hospital divino salvador de sopó", "hospital de sopó", "hospital sopó",
         "ese hospital divino salvador de sopo"],
    )
    def test_sopo_hospital_still_resolves(self, resolver, phrase):
        assert resolver.resolve_entidad(phrase).value == HOSPITAL_SOPO

    @pytest.mark.parametrize("phrase", ["dadi", "dadi valle", "dadi valle del cauca"])
    def test_dadi_still_resolves(self, resolver, phrase):
        assert resolver.resolve_entidad(phrase).value == DADI

    @pytest.mark.parametrize(
        "phrase",
        ["gobernación del valle del cauca dadi", "dadi valle del cauca",
         "valle del cauca dadi"],
    )
    def test_dadi_keeps_its_qualified_aliases(self, resolver, phrase):
        """Un alias que nombra la unidad sigue siendo suyo.

        Se prueba el ownership y no ``resolve_entidad``: para las frases que
        empiezan por "gobernación de…" el gate ``_strict_rewrite_target``
        devuelve ``rewrite_unresolved`` porque el nombre oficial no contiene el
        territorio. Es un defecto preexistente del resolver, ajeno a R1C.
        """
        assert _owner(resolver, phrase) == DADI
        assert _scanned(resolver, phrase) == DADI

    @pytest.mark.parametrize(
        "phrase",
        ["hospital san andres", "hospital san andrés", "hospital san andres de tumaco",
         "ips san andres", "hsa"],
    )
    def test_san_andres_hospital_still_resolves(self, resolver, phrase):
        assert resolver.resolve_entidad(phrase).value == HOSPITAL_SAN_ANDRES

    @pytest.mark.parametrize(
        "phrase",
        ["gobierno departamental del tolima", "gobierno departamental de tolima",
         "gobierno del tolima"],
    )
    def test_tolima_qualified_phrases_still_resolve(self, resolver, phrase):
        assert resolver.resolve_entidad(phrase).value == GOBIERNO_DEPARTAMENTAL

    @pytest.mark.parametrize(
        "phrase",
        ["gobernación del tolima", "departamento del tolima", "gobernación de tolima",
         "gobierno departamental del tolima"],
    )
    def test_tolima_territorially_qualified_aliases_kept(self, resolver, phrase):
        """Retirar el genérico no puede llevarse los aliases con territorio.

        Igual que con DADI, se mide el ownership: ``resolve_entidad`` devuelve
        ``rewrite_unresolved`` para "gobernación del Tolima" porque el nombre
        oficial SECOP es "GOBIERNO DEPARTAMENTAL" y no contiene "tolima".
        """
        assert _owner(resolver, phrase) == GOBIERNO_DEPARTAMENTAL
        assert _scanned(resolver, phrase) == GOBIERNO_DEPARTAMENTAL

    @pytest.mark.parametrize(
        "phrase",
        ["anm", "agencia nacional de minería", "agencia nacional de mineria",
         "autoridad minera"],
    )
    def test_anm_still_resolves(self, resolver, phrase):
        assert resolver.resolve_entidad(phrase).value == ANM
