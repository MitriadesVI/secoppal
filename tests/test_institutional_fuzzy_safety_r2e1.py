"""R2E.1 — seguridad del fuzzy institucional.

R2E dejó dos dueños ajenos medidos y sin cerrar:

* `resolve_entidad("Presidencia de la República")` → `BANCO DE LA REPUBLICA`;
* `resolve_entidad("Contraloría Departamental de Bolívar")` → `CGR-NIVEL CENTRAL`.

Parecen el mismo defecto y no lo son. El primero es un **alias sobre-genérico**:
`la república` no contiene cabeza institucional y aparece en el nombre de
dieciséis NIT distintos del directorio SECOP II.  El segundo es **fail-open del
fuzzy sobre una identidad ausente**: la contraloría territorial existe, tiene
NIT propio y actividad, pero no está en el gazetteer, y Tier 3 entrega el
vecino léxico que quede —la CGR, una gobernación, un municipio o una secretaría
distrital, según la frase—.

Los oráculos se anclan al NIT y al nombre físico publicado por SECOP II
(`p6dx-8zbt` / `jbjy-vk9h`), no al gazetteer: el gazetteer es lo que se audita.

ADR-072 gobierna la separación exacto/scan.  La puerta del fuzzy no la deroga:
sólo actúa en los tiers inferenciales, nunca sobre ownership exacto.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from app.core.entity_resolver import TERRITORY_PHRASES, EntityResolver
from app.utils.money import normalize_text

sys.path.insert(0, str(Path("scripts").resolve()))

from gazetteer_ownership_baseline import (  # noqa: E402
    load_baseline,
    new_multi_owner_keys,
)

ALIASES_DB = Path("app/data/aliases_db.json")

BANREP = "BANCO DE LA REPUBLICA"
DAPRE = "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA"
CGR = "CGR-NIVEL CENTRAL"
AGR = "AUDITORIA GENERAL DE LA REPUBLICA"
PGN = "PROCURADURIA GENERAL DE LA NACION"
DP = "DEFENSORÍA DEL PUEBLO"
FBS = "FONDO DE BIENESTAR SOCIAL DE LA CONTRALORIA GENERAL DE LA REPUBLICA"
IEMP = "INSTITUTO DE ESTUDIOS DEL MINISTERIO PUBLICO."
CONGRESO = "CONGRESO DE LA REPÚBLICA - HONORABLE SENADO DE LA REPÚBLICA"
CAMARA = "CAMARA DE REPRESENTANTES"

OVERGENERIC_ALIAS = "la república"

# Instituciones nacionales reales cuyo nombre contiene "de la República".
# Ninguna puede terminar en el Banco por ese fragmento.
REPUBLIC_ADVERSARIAL = (
    "Presidencia de la República",
    "Vicepresidencia de la República",
    "Auditoría General de la República",
    "Contraloría General de la República",
    "Senado de la República",
    "Congreso de la República",
    "Cámara de Representantes",
    "Departamento Administrativo de la Presidencia de la República",
    "Fondo de Bienestar Social de la Contraloría General de la República",
)

# Formas que sí nombran al Banco y deben seguir resolviendo a él.
BANREP_POSITIVE_CANARIES = (
    "Banco de la República",
    "Banrep",
    "Banco Central de Colombia",
    "Banco emisor",
    "BR",
    "banrepública",
    "el emisor",
    "Banco de la República de Colombia",
)

# Contralorías territoriales verificadas en SECOP II por NIT y nombre físico.
# ``physical_name`` es el nombre publicado; ``nit`` la identidad base.  Ninguna
# está en el gazetteer: R2E.1 no añade cobertura territorial, sólo prohíbe que
# el fuzzy invente un dueño.
TERRITORIAL_CONTROL = (
    ("Contraloría Departamental de Bolívar", "890480306", "CONTRALORIA DEPARTAMENTAL DE BOLIVAR"),
    ("Contraloría General de Caldas", "800077897", "CONTRALORIA GENERAL DE CALDAS"),
    ("Contraloría General de Santander", "890201705", "CONTRALORIA GENERAL DE SANTANDER"),
    ("Contraloría Municipal de Ibagué", "890706867", "CONTRALORIA MUNICIPAL DE IBAGUE"),
    ("Contraloría General de Medellín", "811026988", "CONTRALORÍA DISTRITAL DE MEDELLÍN"),
    ("Contraloría Departamental del Huila", "800169790", "CONTRALORIA DEPARTAMENTAL DEL HUILA"),
    ("Contraloría Municipal de Pasto", "800156682", "CONTRALORÍA MUNICIPAL DE PASTO"),
    ("Contraloría Departamental del Atlántico", "890103037", "CONTRALORIA DEPARTAMENTAL DEL ATLANTICO"),
    ("Contraloría Distrital de Cartagena", "800194000", "CONTRALORIA DISTRITAL DE CARTAGENA DE INDIAS"),
    ("Contraloría Municipal de Bucaramanga", "890211142", "CONTRALORIA MUNICIPAL DE BUCARAMANGA"),
    ("Contraloría Departamental del Valle del Cauca", "800090735", "CONTRALORIA DEPARTAMENTAL DEL VALLE DEL CAUCA"),
    ("Contraloría Distrital de Barranquilla", "800022773", "CONTRALORIA DISTRITAL DE BARRANQUILLA"),
    ("Contraloría Departamental de Nariño", "800157830", "CONTRALORIA DEPARTAMENTAL DE NARIÑO"),
    ("Contraloría Municipal de Neiva", "800095147", "CONTRALORIA MUNICIPAL DE NEIVA."),
)

# Raíces nacionales y dueños territoriales ajenos que el fuzzy entregaba.
FORBIDDEN_TERRITORIAL_OWNERS = frozenset({
    CGR,
    "CONTRALORIA DE BOGOTA",
    "DEPARTAMENTO DE SANTANDER",
    "DEPARTAMENTO DEL ATLANTICO**",
    "GOBERNACION DEL HUILA*",
    "GOBERNACION DE NARIÑO",
    "GOBERNACION DEL VALLE DEL CAUCA - SECRETARIA DE EDUCACION",
    "MUNICIPIO DE BUCARAMANGA",
    "MUNICIPIO DE NEIVA",
    "MUNICIPIO DE CALDAS ANTIOQUIA",
    "(Secretaría Distrital de Integración Social)",
    "SUBRED INTEGRADA DE SERVICIOS DE SALUD NORTE E.S.E. (OFICIAL)",
})

# R2E: universo nacional que no puede moverse.
R2E_NATIONAL_SIX = {
    "Contraloría General de la República": CGR,
    "Auditoría General de la República": AGR,
    "Procuraduría General de la Nación": PGN,
    "Defensoría del Pueblo": DP,
    "Fondo de Bienestar Social de la Contraloría General de la República": FBS,
    "Instituto de Estudios del Ministerio Público": IEMP,
}

# R2E §4.5/§4.6: padre e hijo no se confunden.
PARENT_RELATED_CANARIES = (
    ("contratos del fondo de bienestar social de la contraloria general de la republica", FBS),
    ("licitaciones de la contraloria general de la republica", CGR),
    ("contratos del instituto de estudios del ministerio publico", IEMP),
    ("contratos de la procuraduria general de la nacion", PGN),
)

# R2E §5: la raíz nacional no muerde territorio por scan.
TERRITORIAL_SCAN_TEXTS = (
    "contratos de la contraloria departamental de bolivar",
    "contratos de la contraloria general de santander",
    "contratos de la contraloria general de caldas",
    "contratos de la contraloria general de medellin",
    "contratos de la contraloria municipal de ibague",
    "contratos de la defensoria civil de nobsa",
    "contratos de la personeria municipal de soledad",
    "contratos del fondo de bienestar social de la contraloria departamental de bolivar",
)


@pytest.fixture(scope="module")
def gazetteer() -> list[dict]:
    return json.loads(ALIASES_DB.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def banrep_record(gazetteer: list[dict]) -> dict:
    return next(r for r in gazetteer if r["official_name"] == BANREP)


class TestOvergenericRepublicAlias:
    """`la república` sola no acredita al Banco: le falta cabeza institucional."""

    def test_alias_removed_from_ownership(self, banrep_record: dict) -> None:
        normalized = {normalize_text(a) for a in banrep_record["aliases"]}
        assert normalize_text(OVERGENERIC_ALIAS) not in normalized

    def test_alias_declared_as_negative_evidence(self, banrep_record: dict) -> None:
        blocked = {
            normalize_text(a)
            for a in banrep_record.get("resolution_blocked_aliases", [])
        }
        assert normalize_text(OVERGENERIC_ALIAS) in blocked

    def test_blocked_phrase_asks_for_precision(self, resolver: EntityResolver) -> None:
        result = resolver.resolve_entidad("la República")
        assert result.value is None
        assert result.method == "alias_resolution_blocked"
        assert result.clarification_needed is True

    def test_no_other_owner_claims_the_phrase(self, gazetteer: list[dict]) -> None:
        owners = {
            record["official_name"]
            for record in gazetteer
            if normalize_text(OVERGENERIC_ALIAS)
            in {normalize_text(a) for a in record["aliases"]}
        }
        assert owners == set()


class TestBanrepCrossOwner:
    """BANREP_CROSS_OWNER = 0 — ninguna otra institución cae en el Banco."""

    @pytest.mark.parametrize("phrase", REPUBLIC_ADVERSARIAL)
    def test_resolve_does_not_reach_banrep(
        self, resolver: EntityResolver, phrase: str
    ) -> None:
        assert resolver.resolve_entidad(phrase).value != BANREP

    @pytest.mark.parametrize("phrase", REPUBLIC_ADVERSARIAL)
    def test_scan_does_not_reach_banrep(
        self, resolver: EntityResolver, phrase: str
    ) -> None:
        hit = resolver.scan_entity(normalize_text(f"contratos de {phrase}"))
        assert hit is None or hit.official_name != BANREP

    def test_presidencia_lands_on_its_own_identity(
        self, resolver: EntityResolver
    ) -> None:
        # NIT 899999083: "PRESIDENCIA DE LA REPÚBLICA" y DAPRE son el mismo
        # comprador.  Resolver a la ficha del DAPRE es la identidad correcta.
        assert resolver.resolve_entidad("Presidencia de la República").value == DAPRE

    @pytest.mark.parametrize(
        "phrase,expected",
        (
            ("Senado de la República", CONGRESO),
            ("Congreso de la República", CONGRESO),
            ("Cámara de Representantes", CAMARA),
            ("Auditoría General de la República", AGR),
            ("Contraloría General de la República", CGR),
        ),
    )
    def test_other_republic_institutions_keep_their_owner(
        self, resolver: EntityResolver, phrase: str, expected: str
    ) -> None:
        assert resolver.resolve_entidad(phrase).value == expected


class TestBanrepPositiveCanaries:
    """Retirar el fragmento no puede costar las formas que sí nombran al Banco."""

    @pytest.mark.parametrize("phrase", BANREP_POSITIVE_CANARIES)
    def test_resolve_reaches_banrep(
        self, resolver: EntityResolver, phrase: str
    ) -> None:
        result = resolver.resolve_entidad(phrase)
        assert result.value == BANREP
        assert result.method == "exact"

    @pytest.mark.parametrize(
        "phrase",
        ("Banco de la República", "Banrep", "Banco Central de Colombia", "Banco emisor"),
    )
    def test_scan_reaches_banrep(
        self, resolver: EntityResolver, phrase: str
    ) -> None:
        hit = resolver.scan_entity(normalize_text(f"contratos del {phrase}"))
        assert hit is not None
        assert hit.official_name == BANREP

    def test_two_letter_acronym_stays_out_of_scan(
        self, resolver: EntityResolver
    ) -> None:
        # `br` conserva ownership exacto y nunca entra al índice de scan:
        # dos caracteres dentro de texto libre no acreditan a nadie.
        assert resolver.scan_entity(normalize_text("contratos del br")) is None


class TestTerritorialControlFuzzy:
    """CONTROL_TERRITORIAL_WRONG_NATIONAL_ROOT = 0 y ningún otro dueño ajeno."""

    @pytest.mark.parametrize(
        "phrase,nit,physical", TERRITORIAL_CONTROL, ids=[c[0] for c in TERRITORIAL_CONTROL]
    )
    def test_no_foreign_owner(
        self, resolver: EntityResolver, phrase: str, nit: str, physical: str
    ) -> None:
        result = resolver.resolve_entidad(phrase)
        assert result.value not in FORBIDDEN_TERRITORIAL_OWNERS

    @pytest.mark.parametrize(
        "phrase,nit,physical", TERRITORIAL_CONTROL, ids=[c[0] for c in TERRITORIAL_CONTROL]
    )
    def test_falls_closed_to_the_user_own_words(
        self, resolver: EntityResolver, phrase: str, nit: str, physical: str
    ) -> None:
        # Ninguna está en el gazetteer.  Sin dueño acreditado, el resolver no
        # puede afirmar identidad: cae al filtro literal con la frase del
        # usuario, que no atribuye contratos a nadie más.
        result = resolver.resolve_entidad(phrase)
        assert result.value is None
        assert result.method == "like"
        assert result.like_value == normalize_text(phrase).upper()

    @pytest.mark.parametrize("text", TERRITORIAL_SCAN_TEXTS)
    def test_scan_still_does_not_hijack(
        self, resolver: EntityResolver, text: str
    ) -> None:
        assert resolver.scan_entity(text) is None

    def test_bogota_keeps_its_own_identity(self, resolver: EntityResolver) -> None:
        result = resolver.resolve_entidad("Contraloría de Bogotá")
        assert result.value == "CONTRALORIA DE BOGOTA"
        assert result.method == "exact"


class TestFuzzyIdentityGuard:
    """La puerta es una función pura sobre tokens; no conoce instituciones."""

    def test_head_token_must_survive(self) -> None:
        from app.core.fuzzy_identity_guard import candidate_keeps_requested_identity

        territories = TERRITORY_PHRASES
        assert not candidate_keeps_requested_identity(
            "presidencia de la republica", "la republica", BANREP, territories
        )
        assert candidate_keeps_requested_identity(
            "presidencia de la republica",
            "departamento administrativo de la presidencia de la republica",
            DAPRE,
            territories,
        )

    def test_requested_territory_must_survive(self) -> None:
        from app.core.fuzzy_identity_guard import candidate_keeps_requested_identity

        territories = TERRITORY_PHRASES
        assert not candidate_keeps_requested_identity(
            "contraloria departamental de bolivar", "contraloria", CGR, territories
        )
        assert not candidate_keeps_requested_identity(
            "contraloria municipal de bucaramanga",
            "bucaramanga",
            "MUNICIPIO DE BUCARAMANGA",
            territories,
        )

    def test_phrase_without_extra_content_is_not_penalized(self) -> None:
        from app.core.fuzzy_identity_guard import candidate_keeps_requested_identity

        territories = TERRITORY_PHRASES
        assert candidate_keeps_requested_identity(
            "contraloria general de la republica de colombia",
            "la contraloria general de la republica",
            CGR,
            territories,
        )

    def test_exact_ownership_is_not_subject_to_the_guard(
        self, resolver: EntityResolver
    ) -> None:
        # `br` no comparte un solo token con "BANCO DE LA REPUBLICA".  Si la
        # puerta actuara sobre Tier 1, las siglas dejarían de resolver.
        assert resolver.resolve_entidad("BR").method == "exact"


class TestR2ERegressionPreserved:
    """R2E 6/6, padre-relacionado 4/4 y scan territorial sin secuestros."""

    @pytest.mark.parametrize("phrase,expected", sorted(R2E_NATIONAL_SIX.items()))
    def test_national_six(
        self, resolver: EntityResolver, phrase: str, expected: str
    ) -> None:
        result = resolver.resolve_entidad(phrase)
        assert result.value == expected
        assert result.method == "exact"

    @pytest.mark.parametrize("text,expected", PARENT_RELATED_CANARIES)
    def test_parent_related(
        self, resolver: EntityResolver, text: str, expected: str
    ) -> None:
        hit = resolver.scan_entity(text)
        assert hit is not None
        assert hit.official_name == expected


class TestCollisionGates:
    """NEW_MULTI_OWNER_KEYS = 0 sobre el techo de deuda aceptada."""

    def test_no_new_multi_owner_keys(self, gazetteer: list[dict]) -> None:
        assert new_multi_owner_keys(gazetteer, load_baseline()) == []
