"""R2F-B — cierre de las ocho universidades publicas que R2F-A dejo cerradas.

Los oraculos vienen de NIT + nombre fisico byte-for-byte + actividad SECOP II
revalidada en vivo el 2026-09-02, no del gazetteer: el gazetteer es el objeto
bajo prueba y nunca la autoridad que decide quien deberia ser el owner.

ADR-072 separa ownership exacto de elegibilidad de scan.
ADR-073 separa identidad canonica de nombre fisico proyectado por dataset.
"""

from __future__ import annotations

from tests.sql_oracle_properties import assert_identity, exact_identity

import json
import sys
from pathlib import Path

import pytest

from app.core.buyer_source_names import project_buyer_names
from app.core.entity_resolver import EntityResolver
from app.core.scan_alias_policy import record_alias_policy
from app.core.soql_builder import SoQLBuilder
from app.utils.money import normalize_text

sys.path.insert(0, str(Path("scripts").resolve()))

from gazetteer_ownership_baseline import (  # noqa: E402
    load_baseline,
    multi_owner_groups,
    new_multi_owner_keys,
)

ALIASES_DB = Path("app/data/aliases_db.json")
SOURCE_NAMES = Path("app/data/buyer_source_names.json")
PROCESSES = SoQLBuilder.PROCESOS_DATASET
CONTRACTS = SoQLBuilder.CONTRATOS_DATASET

R2FB_ARTIFACT = "artifacts/audits/public_university_completion_r2fb.json"
R2FA_ARTIFACT = "artifacts/audits/public_university_pilot_r2fa.json"
V1_ARTIFACT = "artifacts/audits/source_name_projection_v1.json"

COLEGIO_MAYOR = "UNIVERSIDAD COLEGIO MAYOR DE CUNDINAMARCA"
CUNDINAMARCA = "UNIVERSIDAD DE CUNDINAMARCA"
GUAJIRA = "UNIVERSIDAD DE LA GUAJIRA"
PACIFICO = "UNIVERSIDAD DEL PACÍFICO"
POPULAR_CESAR = "UNIVERSIDAD POPULAR DEL CESAR"
SUCRE = "UNIVERSIDAD DE SUCRE"
CHOCO = "UNIVERSIDAD TECNOLÓGICA DEL CHOCÓ DIEGO LUIS CÓRDOBA"
TOLIMA = "UNIVERSIDAD DEL TOLIMA"

# Bindings de lotes anteriores. Comparten sidecar, no procedencia.
DAPRE = "DEPARTAMENTO ADMINISTRATIVO DE LA PRESIDENCIA DE LA REPÚBLICA"
R2FA_CANONICALS = {
    "UNIATLANTICO": R2FA_ARTIFACT,
    "UNIVERSIDAD DE LA AMAZONIA": R2FA_ARTIFACT,
    "UNIVERSIDAD DE CÓRDOBA": R2FA_ARTIFACT,
    "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA": R2FA_ARTIFACT,
    DAPRE: V1_ARTIFACT,
}

# PRE congelado en HEAD e33a97a antes de mutar datos, revalidado en vivo: las
# ocho resuelven a None por el guard de identidad fuzzy. Ninguna tiene registro
# propio en el gazetteer y ninguna publica bajo mas de un nombre fisico.
TARGET_PRE = {
    "Universidad Colegio Mayor de Cundinamarca": "FAIL_CLOSED",
    "Universidad de Cundinamarca": "FAIL_CLOSED",
    "Universidad de La Guajira": "FAIL_CLOSED",
    "Universidad del Pacífico": "FAIL_CLOSED",
    "Universidad Popular del Cesar": "FAIL_CLOSED",
    "Universidad de Sucre": "FAIL_CLOSED",
    "Universidad Tecnológica del Chocó Diego Luis Córdoba": "FAIL_CLOSED",
    "Universidad del Tolima": "FAIL_CLOSED",
}

# NIT + canonical acreditado. El NIT es la autoridad de identidad; la similitud
# de nombres nunca lo es.
VERIFIED_IDENTITIES = {
    "Universidad Colegio Mayor de Cundinamarca": ("800144829", COLEGIO_MAYOR),
    "Universidad de Cundinamarca": ("890680062", CUNDINAMARCA),
    "Universidad de La Guajira": ("892115029", GUAJIRA),
    "Universidad del Pacífico": ("835000300", PACIFICO),
    "Universidad Popular del Cesar": ("8923002856", POPULAR_CESAR),
    "Universidad de Sucre": ("892200323", SUCRE),
    "Universidad Tecnológica del Chocó Diego Luis Córdoba": ("891680089", CHOCO),
    "Universidad del Tolima": ("890700640", TOLIMA),
}

# Nombres fisicos byte-for-byte observados el 2026-09-02. Solo entran al
# sidecar las identidades cuyo canonico NO es el nombre fisico exacto.
SOURCE_BINDINGS = {
    COLEGIO_MAYOR: {PROCESSES: ("Universidad Colegio Mayor de Cundinamarca",)},
    PACIFICO: {
        PROCESSES: ("UNIVERSIDAD DEL PACIFICO//",),
        CONTRACTS: ("UNIVERSIDAD DEL PACIFICO//",),
    },
    POPULAR_CESAR: {PROCESSES: ("UNIVERSIDAD POPULAR DEL CESAR.",)},
    CHOCO: {
        PROCESSES: ("UNIVERSIDAD TECNOLOGICA DEL CHOCO",),
        CONTRACTS: ("UNIVERSIDAD TECNOLOGICA DEL CHOCO",),
    },
}

# Identidades cuyo canonico ya es el nombre fisico byte-for-byte: proyectarlas
# no cambiaria un solo caracter del filtro, y un binding que no separa nada es
# ruido de auditoria, no evidencia.
NO_BINDING_REQUIRED = (CUNDINAMARCA, GUAJIRA, SUCRE, TOLIMA)

# Fragmentos que YA pertenecen a otro comprador. Los aliases nuevos los
# contienen pero son estrictamente mas largos: el scan devuelve el match mas
# largo, y esa es toda la separacion que los mantiene fuera de un hijack.
CONTESTED_FRAGMENTS = {
    "colegio mayor": "INSTITUCIÓN UNIVERSITARIA COLEGIO MAYOR DE ANTIOQUIA",
    "universidad tecnologica": "UPTC",
}


@pytest.fixture(scope="module")
def gazetteer() -> list[dict]:
    return json.loads(ALIASES_DB.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def records(gazetteer: list[dict]) -> dict[str, dict]:
    return {record["official_name"]: record for record in gazetteer}


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


def test_pre_matrix_is_frozen_at_eight_fail_closed_cases() -> None:
    assert len(TARGET_PRE) == 8
    assert set(TARGET_PRE.values()) == {"FAIL_CLOSED"}


@pytest.mark.parametrize("query,identity", sorted(VERIFIED_IDENTITIES.items()))
def test_accredited_university_resolves_exact(
    resolver: EntityResolver, query: str, identity: tuple[str, str],
) -> None:
    _nit, expected_owner = identity
    result = resolver.resolve_entidad(query)
    assert (result.value, result.method) == (expected_owner, "exact")


@pytest.mark.parametrize("query,identity", sorted(VERIFIED_IDENTITIES.items()))
def test_nit_is_the_identity_authority(
    records: dict[str, dict], query: str, identity: tuple[str, str],
) -> None:
    nit, canonical = identity
    assert records[canonical]["nit"] == nit


def test_every_target_nit_is_unique_in_the_gazetteer(gazetteer: list[dict]) -> None:
    """Dos canonicos con el mismo NIT serian una identidad partida en dos."""
    targets = {nit for nit, _canonical in VERIFIED_IDENTITIES.values()}
    for target in targets:
        owners = [
            record["official_name"]
            for record in gazetteer
            if record.get("nit") == target
        ]
        assert len(owners) == 1, f"{target}: {owners}"


@pytest.mark.parametrize("canonical,by_dataset", sorted(SOURCE_BINDINGS.items()))
def test_source_projection_is_exact_equality_never_like(
    canonical: str, by_dataset: dict[str, tuple[str, ...]],
) -> None:
    builder = SoQLBuilder()
    for dataset, physical_names in by_dataset.items():
        params = {"entidad_resolved": canonical}
        snapshot = dict(params)
        projected = project_buyer_names(canonical, dataset)
        soql = builder.build_count(dataset, params)

        assert projected == physical_names
        assert params == snapshot
        field = "entidad" if dataset == PROCESSES else "nombre_entidad"
        assert_identity(soql, dataset, exact_identity(field, physical_names))


def test_zero_contract_rows_do_not_fabricate_a_contract_name() -> None:
    """Ausencia de contratos no es SOURCE_NAME_SPLIT ni licencia para inventar.

    Colegio Mayor y Popular del Cesar no publican un solo contrato bajo su NIT.
    Declarar ahi el nombre de procesos afirmaria una observacion que nadie hizo,
    asi que el binding solo habla de procesos y contratos conserva su camino.
    """
    for canonical in (COLEGIO_MAYOR, POPULAR_CESAR):
        assert project_buyer_names(canonical, PROCESSES) is not None
        assert project_buyer_names(canonical, CONTRACTS) is None


@pytest.mark.parametrize("canonical", NO_BINDING_REQUIRED)
def test_canonical_equal_to_physical_name_needs_no_binding(canonical: str) -> None:
    assert project_buyer_names(canonical, PROCESSES) is None
    assert project_buyer_names(canonical, CONTRACTS) is None


@pytest.mark.parametrize(
    "text,expected",
    [
        ("procesos de la universidad colegio mayor de cundinamarca", COLEGIO_MAYOR),
        ("contratos de la universidad tecnológica del chocó", CHOCO),
    ],
)
def test_longer_alias_wins_over_contested_fragment(
    resolver: EntityResolver, text: str, expected: str,
) -> None:
    hit = resolver.scan_entity(normalize_text(text))
    assert hit is not None
    assert hit.official_name == expected


def test_contested_fragments_keep_their_previous_owner(
    resolver: EntityResolver,
) -> None:
    """El lote agrega dueños; no se los quita a nadie."""
    for fragment, owner in CONTESTED_FRAGMENTS.items():
        assert resolver.resolve_entidad(fragment).value == owner


@pytest.mark.parametrize("query,identity", sorted(VERIFIED_IDENTITIES.items()))
def test_target_scan_attributes_to_its_own_owner(
    resolver: EntityResolver, query: str, identity: tuple[str, str],
) -> None:
    _nit, canonical = identity
    hit = resolver.scan_entity(normalize_text(f"contratos de la {query}"))
    assert hit is not None
    assert hit.official_name == canonical


# Grafias que SECOP publica y que un usuario copiaria y pegaria. Sin ellas el
# comprador llega por fuzzy: el owner sale bien, pero el nombre fisico propio de
# la identidad no entra por la via exacta, que es justo lo que este lote curo.
COPY_PASTE_ALIASES = {
    "UNIVERSIDAD DEL PACIFICO//": PACIFICO,
    "UNIVERSIDAD POPULAR DEL CESAR.": POPULAR_CESAR,
    "UNIVERSIDA DE LA GUAJIRA": GUAJIRA,
}

# El typo del directorio acredita ownership exacto y nada mas. Un error de
# digitacion no puede reclamar compradores dentro de texto libre: lo que lo
# acredita es una fila registral, no una forma que alguien use para nombrarlos.
TYPO_ALIAS = "universida de la guajira"

# Aportada externamente, NO reproducida en p6dx-8zbt, jbjy-vk9h ni el
# directorio: el punto no aparece en ninguna fuente que el runtime consulte.
# Queda sin alias a proposito. Sigue llegando al owner correcto por fuzzy, y
# ese es el comportamiento congelado mientras la variante no se acredite.
UNREPRODUCED_SURFACE_VARIANT = "UNIVERSIDAD TECNOLOGICA DEL CHOCO."


@pytest.mark.parametrize("surface,owner", sorted(COPY_PASTE_ALIASES.items()))
def test_published_surface_form_resolves_exact(
    resolver: EntityResolver, surface: str, owner: str,
) -> None:
    result = resolver.resolve_entidad(surface)
    assert (result.value, result.method) == (owner, "exact")


def test_registry_typo_keeps_exact_but_loses_scan(
    records: dict[str, dict], resolver: EntityResolver,
) -> None:
    record = records[GUAJIRA]
    disabled, _blocked = record_alias_policy(record)
    assert TYPO_ALIAS in {normalize_text(alias) for alias in record["aliases"]}
    assert TYPO_ALIAS in disabled
    assert resolver.resolve_entidad(TYPO_ALIAS).value == GUAJIRA

    scanned = resolver.scan_entity(normalize_text(f"contratos de la {TYPO_ALIAS}"))
    assert scanned is None or scanned.matched_alias != TYPO_ALIAS


@pytest.mark.parametrize(
    "surface", ["UNIVERSIDAD DEL PACIFICO//", "UNIVERSIDAD POPULAR DEL CESAR."],
)
def test_physical_surface_forms_keep_scan(
    resolver: EntityResolver, surface: str,
) -> None:
    hit = resolver.scan_entity(normalize_text(f"contratos de la {surface}"))
    assert hit is not None
    assert hit.official_name == COPY_PASTE_ALIASES[surface]


def test_unreproduced_variant_is_not_promoted_to_an_alias(
    records: dict[str, dict], resolver: EntityResolver,
) -> None:
    """Evidencia externa no reproducida no compra ownership exacto.

    La forma con punto la aporto una vista de SECOP que no es fuente de
    proyeccion. Acreditarla igual seria tratar un dato no verificado como si lo
    estuviera; fallar cerrado tampoco corresponde, porque la identidad SI esta
    acreditada. El fuzzy es la respuesta correcta para una grafia que nadie
    pudo observar en las fuentes del runtime.
    """
    variant = normalize_text(UNREPRODUCED_SURFACE_VARIANT)
    assert variant not in {
        normalize_text(alias) for alias in records[CHOCO]["aliases"]
    }
    result = resolver.resolve_entidad(UNREPRODUCED_SURFACE_VARIANT)
    assert result.value == CHOCO
    assert result.method.startswith("fuzzy")


def test_no_new_exact_multi_owner_keys(gazetteer: list[dict]) -> None:
    assert new_multi_owner_keys(gazetteer, load_baseline()) == []


def test_exact_multi_owner_does_not_increase(gazetteer: list[dict]) -> None:
    assert len(multi_owner_groups(gazetteer)) <= len(load_baseline())


def test_sidecar_grows_without_touching_previous_provenance() -> None:
    """El sidecar es acumulativo; la procedencia nunca lo es.

    Este es el oraculo que impide repetir el defecto de R2F-A.1: un lote nuevo
    no puede readjudicarse los bindings de los lotes anteriores, ni siquiera
    por descuido al reescribir el archivo.
    """
    payload = json.loads(SOURCE_NAMES.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 2
    assert "evidence" not in payload

    by_canonical = {row["canonical_name"]: row for row in payload["bindings"]}
    historical = set(R2FA_CANONICALS) | set(SOURCE_BINDINGS)
    assert historical <= set(by_canonical)
    assert len(by_canonical) == len(payload["bindings"])

    for canonical, evidence in R2FA_CANONICALS.items():
        assert by_canonical[canonical]["evidence"] == evidence
    for canonical in SOURCE_BINDINGS:
        assert by_canonical[canonical]["evidence"] == R2FB_ARTIFACT


@pytest.mark.parametrize("canonical", sorted(SOURCE_BINDINGS))
def test_new_bindings_carry_complete_schema_2_evidence(canonical: str) -> None:
    payload = json.loads(SOURCE_NAMES.read_text(encoding="utf-8"))
    binding = {row["canonical_name"]: row for row in payload["bindings"]}[canonical]

    assert binding["identity_key"]["type"] == "NIT"
    assert binding["observed_at"] == "2026-09-02"
    assert binding["evidence"] == R2FB_ARTIFACT
    assert set(binding["observed_rows"]) <= set(binding["sources"])
    assert all(rows > 0 for rows in binding["observed_rows"].values())
