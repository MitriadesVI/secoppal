"""R2E — cobertura segura del universo de control nacional.

Seis compradores físicos SECOP II, no seis entidades equivalentes: la
Contraloría, la Auditoría, la Procuraduría y la Defensoría son raíces
constitucionales; el Fondo de Bienestar Social de la CGR y el Instituto de
Estudios del Ministerio Público son compradores relacionados con NIT propio y
no se fusionan con su entidad padre.

Cada owner esperado está anclado al NIT y al nombre físico publicado por SECOP
II, no al gazetteer: el gazetteer es lo que se audita, no el oráculo.

ADR-072 gobierna la separación exacto/scan que aplican estos casos.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.scan_alias_policy import record_alias_policy
from app.utils.money import normalize_text

sys.path.insert(0, str(Path("scripts").resolve()))

from gazetteer_ownership_baseline import (  # noqa: E402
    load_baseline,
    multi_owner_groups,
    new_multi_owner_keys,
)

ALIASES_DB = Path("app/data/aliases_db.json")

CGR = "CGR-NIVEL CENTRAL"
AGR = "AUDITORIA GENERAL DE LA REPUBLICA"
PGN = "PROCURADURIA GENERAL DE LA NACION"
DP = "DEFENSORÍA DEL PUEBLO"
FBS = "FONDO DE BIENESTAR SOCIAL DE LA CONTRALORIA GENERAL DE LA REPUBLICA"
IEMP = "INSTITUTO DE ESTUDIOS DEL MINISTERIO PUBLICO."
DAEP = "DEPARTAMENTO ADMINISTRATIVO DE LA DEFENSORIA DEL ESPACIO PUBLICO"

# NIT base publicado por SECOP II (procesos p6dx-8zbt / contratos jbjy-vk9h).
# El IEMP se publica con dígito de verificación: 8300157281 = 830015728-1.
SECOP_IDENTITY = {
    CGR: "899999067",
    AGR: "830065741",
    PGN: "899999119",
    DP: "800186061",
    FBS: "800252683",
    IEMP: "830015728",
}

CONTROL_ROOTS = (CGR, AGR, PGN, DP)
RELATED_BUYERS = (FBS, IEMP)

INSTITUTIONAL_NAMES = {
    "Contraloría General de la República": CGR,
    "Auditoría General de la República": AGR,
    "Procuraduría General de la Nación": PGN,
    "Defensoría del Pueblo": DP,
    "Fondo de Bienestar Social de la Contraloría General de la República": FBS,
    "Instituto de Estudios del Ministerio Público": IEMP,
}

HUMAN_FORMS = {
    "la Contraloría General de la República": CGR,
    "Contraloría General": CGR,
    "la Contraloría": CGR,
    "la Auditoría General de la República": AGR,
    "Auditoría General": AGR,
    "la Auditoría General": AGR,
    "la Procuraduría General de la Nación": PGN,
    "Procuraduría General": PGN,
    "la Procuraduría": PGN,
    "Procuraduría": PGN,
    "la Defensoría del Pueblo": DP,
    "la Defensoría": DP,
    "Defensoría": DP,
    "el Fondo de Bienestar Social de la Contraloría General de la República": FBS,
    "Fondo de Bienestar Social de la CGR": FBS,
    "el Instituto de Estudios del Ministerio Público": IEMP,
}

ACRONYMS = {"CGR": CGR, "AGR": AGR, "PGN": PGN, "DP": DP, "IEMP": IEMP}

# Aliases que conservan ownership exacto y pierden elegibilidad de scan.
SCAN_DISABLED = {
    "contraloría": CGR,
    "la contraloría": CGR,
    "contraloría general": CGR,
    "la contraloría general": CGR,
    "defensoría": DP,
    "la defensoría": DP,
}

# Frases sin comprador único: fallan cerradas antes del fuzzy.
BLOCKED_PHRASES = (
    "Ministerio Público",
    "fondo de bienestar social",
    "Fondo de Bienestar Social de la Contraloría",
)

# Compradores físicos reales de control territorial. Ninguno puede quedar
# capturado por un alias de una raíz nacional.
TERRITORIAL_CONTROL_TEXTS = (
    "contratos de la contraloria departamental de bolivar",
    "procesos de la contraloria general de santander",
    "contratos de la contraloria general de caldas",
    "contratos de la contraloria general de medellin",
    "contratos de la defensoria civil de nobsa",
    "contratos del fondo de bienestar social de la contraloria departamental de bolivar",
)

NATIONAL_OWNERS = frozenset({CGR, AGR, PGN, DP, FBS, IEMP})


@pytest.fixture(scope="module")
def gazetteer() -> list[dict]:
    return json.loads(ALIASES_DB.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def records(gazetteer: list[dict]) -> dict[str, dict]:
    return {record["official_name"]: record for record in gazetteer}


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def alias_owners(gazetteer: list[dict]) -> dict[str, set[str]]:
    owners: dict[str, set[str]] = {}
    for record in gazetteer:
        for alias in record.get("aliases", []):
            owners.setdefault(normalize_text(alias), set()).add(record["official_name"])
    return owners


# ── Identidad ───────────────────────────────────────────────────────────


class TestVerifiedIdentity:
    def test_universe_is_four_roots_and_two_related_buyers(self):
        assert len(CONTROL_ROOTS) == 4
        assert len(RELATED_BUYERS) == 2
        assert len(set(CONTROL_ROOTS) | set(RELATED_BUYERS)) == 6

    @pytest.mark.parametrize("official", sorted(SECOP_IDENTITY))
    def test_record_exists_with_published_nit(self, records: dict[str, dict], official: str):
        record = records[official]
        assert record["nit"] == SECOP_IDENTITY[official]

    def test_related_buyers_are_not_merged_into_parents(self, records: dict[str, dict]):
        """El Fondo y el IEMP tienen NIT propio: son compradores separados."""
        assert records[FBS]["nit"] != records[CGR]["nit"]
        assert records[IEMP]["nit"] != records[PGN]["nit"]


# ── Matriz de resolución ────────────────────────────────────────────────


class TestInstitutionalNames:
    @pytest.mark.parametrize("query,expected", sorted(INSTITUTIONAL_NAMES.items()))
    def test_pass_exact(
        self,
        resolver: EntityResolver,
        alias_owners: dict[str, set[str]],
        query: str,
        expected: str,
    ):
        result = resolver.resolve_entidad(query)
        assert result.value == expected
        assert result.method == "exact"
        assert expected in alias_owners[normalize_text(query)]

    def test_no_wrong_owner_and_no_fuzzy_only(self, resolver: EntityResolver):
        methods = {
            query: resolver.resolve_entidad(query) for query in INSTITUTIONAL_NAMES
        }
        wrong = {
            query: result.value
            for query, result in methods.items()
            if result.value != INSTITUTIONAL_NAMES[query]
        }
        fuzzy = {
            query: result.method
            for query, result in methods.items()
            if str(result.method).startswith("fuzzy")
        }
        assert wrong == {}
        assert fuzzy == {}


class TestHumanFormsAndAcronyms:
    @pytest.mark.parametrize("query,expected", sorted(HUMAN_FORMS.items()))
    def test_human_form_resolves_to_verified_buyer(
        self, resolver: EntityResolver, query: str, expected: str
    ):
        assert resolver.resolve_entidad(query).value == expected

    @pytest.mark.parametrize("acronym,expected", sorted(ACRONYMS.items()))
    def test_acronym_resolves_exact(
        self, resolver: EntityResolver, acronym: str, expected: str
    ):
        result = resolver.resolve_entidad(acronym)
        assert (result.value, result.method) == (expected, "exact")


# ── Separación raíz / comprador relacionado ─────────────────────────────


class TestParentRelatedSeparation:
    def test_fondo_phrase_never_resolves_to_cgr(self, resolver: EntityResolver):
        for query in (
            "Fondo de Bienestar Social de la Contraloría General de la República",
            "Fondo de Bienestar Social de la CGR",
        ):
            assert resolver.resolve_entidad(query).value == FBS

    def test_fondo_phrase_never_scans_to_cgr(self, resolver: EntityResolver):
        text = normalize_text(
            "contratos del fondo de bienestar social de la contraloria general de la republica"
        )
        scan = resolver.scan_entity(text)
        assert scan is not None
        assert scan.official_name == FBS

    def test_cgr_phrase_never_resolves_to_fondo(self, resolver: EntityResolver):
        for query in ("Contraloría General de la República", "CGR"):
            assert resolver.resolve_entidad(query).value == CGR

    def test_cgr_phrase_never_scans_to_fondo(self, resolver: EntityResolver):
        scan = resolver.scan_entity(
            normalize_text("licitaciones de la contraloria general de la republica")
        )
        assert scan is not None
        assert scan.official_name == CGR

    def test_iemp_phrase_never_resolves_to_procuraduria(self, resolver: EntityResolver):
        for query in ("Instituto de Estudios del Ministerio Público", "IEMP"):
            assert resolver.resolve_entidad(query).value == IEMP

    def test_iemp_phrase_never_scans_to_procuraduria(self, resolver: EntityResolver):
        scan = resolver.scan_entity(
            normalize_text("contratos del instituto de estudios del ministerio publico")
        )
        assert scan is not None
        assert scan.official_name == IEMP

    def test_procuraduria_phrase_never_resolves_to_iemp(self, resolver: EntityResolver):
        assert resolver.resolve_entidad("Procuraduría General de la Nación").value == PGN


# ── Control territorial ─────────────────────────────────────────────────


class TestTerritorialControlNonHijack:
    @pytest.mark.parametrize("text", TERRITORIAL_CONTROL_TEXTS)
    def test_national_root_does_not_capture_territorial_buyer(
        self, resolver: EntityResolver, text: str
    ):
        scan = resolver.scan_entity(normalize_text(text))
        owner = scan.official_name if scan else None
        assert owner not in NATIONAL_OWNERS

    def test_bogota_comptroller_keeps_its_own_identity(self, resolver: EntityResolver):
        scan = resolver.scan_entity(normalize_text("contratos de la contraloria de bogota"))
        assert scan is not None
        assert scan.official_name == "CONTRALORIA DE BOGOTA"
        assert resolver.resolve_entidad("Contraloría de Bogotá").value == "CONTRALORIA DE BOGOTA"


# ── ADR-072: exacto y scan son autoridades distintas ────────────────────


class TestExactVersusScanPolicy:
    @pytest.mark.parametrize("alias,owner", sorted(SCAN_DISABLED.items()))
    def test_alias_keeps_exact_ownership(
        self, resolver: EntityResolver, alias: str, owner: str
    ):
        result = resolver.resolve_entidad(alias)
        assert (result.value, result.method) == (owner, "exact")

    @pytest.mark.parametrize("alias,owner", sorted(SCAN_DISABLED.items()))
    def test_alias_is_declared_scan_disabled(
        self, records: dict[str, dict], alias: str, owner: str
    ):
        scan_disabled, _ = record_alias_policy(records[owner])
        assert normalize_text(alias) in scan_disabled

    def test_scan_index_ignores_disabled_aliases(self, resolver: EntityResolver):
        indexed = {
            alias
            for entries in resolver._scan_index.values()
            for alias, _ in entries
        }
        assert indexed.isdisjoint({normalize_text(a) for a in SCAN_DISABLED})

    @pytest.mark.parametrize("phrase", BLOCKED_PHRASES)
    def test_ambiguous_phrase_fails_closed(self, resolver: EntityResolver, phrase: str):
        result = resolver.resolve_entidad(phrase)
        assert result.value is None
        assert result.method == "alias_resolution_blocked"
        assert result.clarification_needed is True


# ── Colisión 'la defensoría' ────────────────────────────────────────────


class TestDefensoriaCollision:
    def test_la_defensoria_has_a_single_owner(self, alias_owners: dict[str, set[str]]):
        assert alias_owners[normalize_text("la defensoría")] == {DP}

    def test_espacio_publico_keeps_its_specific_forms(self, resolver: EntityResolver):
        for query in (
            "Defensoría del Espacio Público",
            "Departamento Administrativo de la Defensoría del Espacio Público",
        ):
            assert resolver.resolve_entidad(query).value == DAEP

    def test_removed_alias_does_not_resurrect_by_fuzzy(self, resolver: EntityResolver):
        assert resolver.resolve_entidad("la Defensoría").value != DAEP


# ── Puertas de colisión ─────────────────────────────────────────────────


class TestCollisionGates:
    def test_no_new_multi_owner_keys(self, gazetteer: list[dict]):
        assert new_multi_owner_keys(gazetteer, load_baseline()) == []

    def test_multi_owner_debt_does_not_grow(self, gazetteer: list[dict]):
        assert len(multi_owner_groups(gazetteer)) <= 173

    def test_declared_policy_is_consistent(self, gazetteer: list[dict]):
        owned = {
            normalize_text(alias)
            for record in gazetteer
            for alias in record.get("aliases", [])
        }
        for record in gazetteer:
            scan_disabled, blocked = record_alias_policy(record)
            record_aliases = {
                normalize_text(alias) for alias in record.get("aliases", [])
            }
            # ADR-072: exclusion governs scan surfaces, including an owned
            # physical canonical. It does not remove its exact ownership.
            assert scan_disabled <= record_aliases | {normalize_text(record['official_name'])}
            assert blocked.isdisjoint(owned)
