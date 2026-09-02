"""R2F-A — piloto supervisado de ocho universidades publicas.

Los oraculos de identidad de este archivo vienen de NIT + nombre fisico +
actividad SECOP II observada el 2026-09-02.  El gazetteer es el objeto bajo
prueba: nunca se usa para decidir cual deberia ser el owner.

ADR-072 gobierna ownership exacto y elegibilidad de scan por separado.
ADR-073 gobierna la proyeccion exacta de nombres fisicos por dataset.
"""

from __future__ import annotations

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

AMAZONIA = "UNIVERSIDAD DE LA AMAZONIA"
ANTIOQUIA = "UNIVERSIDAD DE ANTIOQUIA"
ATLANTICO = "UNIATLANTICO"
CORDOBA = "UNIVERSIDAD DE CÓRDOBA"
UFPS_CUCUTA = "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER"
UFPS_OCANA = "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA"
UNITROPICO = "UNIVERSIDAD INTERNACIONAL DEL TRÓPICO AMERICANO UNITRÓPICO"
UAIIN = "Universidad Autónoma Indígena Intercultural"

# Congelado antes de cualquier mutacion de datos en HEAD 8849f0d. Ocaña
# corrige la identidad historica R2C: es un comprador separado, NIT 8001631300.
PILOT_PRE = {
    "Universidad de la Amazonia": "FAIL_CLOSED",
    "Universidad de Antioquia": "PASS_EXACT",
    "Universidad del Atlántico": "WRONG_OWNER",
    "Universidad de Córdoba": "FAIL_CLOSED",
    "Universidad Francisco de Paula Santander - Cúcuta": "FUZZY_ONLY",
    "Universidad Francisco de Paula Santander - Ocaña": "WRONG_OWNER",
    "Universidad Internacional del Trópico Americano": "WRONG_OWNER",
    UAIIN: "WRONG_OWNER",
}

VERIFIED_IDENTITIES = {
    "Universidad de la Amazonia": ("891190346", AMAZONIA),
    "Universidad de Antioquia": ("890980040", ANTIOQUIA),
    "Universidad del Atlántico": ("890102257", ATLANTICO),
    "Universidad de Córdoba": ("891080031", CORDOBA),
    "Universidad Francisco de Paula Santander - Cúcuta": (
        "890500622",
        UFPS_CUCUTA,
    ),
    "Universidad Francisco de Paula Santander - Ocaña": (
        "8001631300",
        UFPS_OCANA,
    ),
    "Universidad Internacional del Trópico Americano": (
        "8440020714",
        UNITROPICO,
    ),
}

SOURCE_BINDINGS = {
    AMAZONIA: {
        PROCESSES: ("UNIVERSIDAD DE LA AMAZONIA (Entidad)",),
        CONTRACTS: ("UNIVERSIDAD DE LA AMAZONIA (Entidad)",),
    },
    ATLANTICO: {
        PROCESSES: ("Universidad del Atlántico",),
        CONTRACTS: ("Universidad del Atlántico",),
    },
    CORDOBA: {
        PROCESSES: ("UNIVERSIDAD DE CORDOBA",),
        CONTRACTS: ("UNIVERSIDAD DE CORDOBA",),
    },
    UFPS_OCANA: {
        PROCESSES: (
            "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA (ENTIDAD)",
        ),
        CONTRACTS: (
            "UNIVERSIDAD FRANCISCO DE PAULA SANTANDER SECCIONAL OCAÑA (ENTIDAD)",
        ),
    },
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


def test_pre_matrix_is_frozen_at_eight_cases() -> None:
    assert len(PILOT_PRE) == 8
    assert list(PILOT_PRE.values()).count("WRONG_OWNER") == 4
    assert list(PILOT_PRE.values()).count("FUZZY_ONLY") == 1
    assert list(PILOT_PRE.values()).count("FAIL_CLOSED") == 2
    assert list(PILOT_PRE.values()).count("PASS_EXACT") == 1


@pytest.mark.parametrize("query,identity", sorted(VERIFIED_IDENTITIES.items()))
def test_verified_institutional_name_passes_exact(
    resolver: EntityResolver,
    query: str,
    identity: tuple[str, str],
) -> None:
    _nit, expected_owner = identity
    result = resolver.resolve_entidad(query)
    assert (result.value, result.method) == (expected_owner, "exact")


def test_no_evidence_case_fails_closed_before_fuzzy(resolver: EntityResolver) -> None:
    result = resolver.resolve_entidad(UAIIN)
    assert result.value is None
    assert result.method == "alias_resolution_blocked"
    assert result.clarification_needed is True


def test_ufps_cucuta_and_ocana_are_separate_buyers(records: dict[str, dict]) -> None:
    assert records[UFPS_CUCUTA]["nit"] == "890500622"
    assert records[UFPS_OCANA]["nit"] == "8001631300"
    assert records[UFPS_CUCUTA]["nit"] != records[UFPS_OCANA]["nit"]


@pytest.mark.parametrize("canonical,by_dataset", sorted(SOURCE_BINDINGS.items()))
def test_source_projection_is_exact_and_preserves_canonical(
    canonical: str,
    by_dataset: dict[str, tuple[str, ...]],
) -> None:
    builder = SoQLBuilder()
    for dataset, physical_names in by_dataset.items():
        params = {"entidad_resolved": canonical}
        snapshot = dict(params)
        projected = project_buyer_names(canonical, dataset)
        soql = builder.build_count(dataset, params)

        assert projected == physical_names
        assert params == snapshot
        assert " LIKE " not in soql
        assert "UPPER" not in soql
        assert "unaccent" not in soql
        field = "entidad" if dataset == PROCESSES else "nombre_entidad"
        if len(physical_names) == 1:
            assert soql.endswith(f"WHERE {field} = '{physical_names[0]}'")


def test_antioquia_control_requires_no_mutation(records: dict[str, dict]) -> None:
    record = records[ANTIOQUIA]
    assert record["aliases"] == ["universidad de antioquia"]
    assert project_buyer_names(ANTIOQUIA, PROCESSES) is None
    assert project_buyer_names(ANTIOQUIA, CONTRACTS) is None


def test_ufps_unqualified_physical_name_keeps_exact_but_loses_scan(
    records: dict[str, dict], resolver: EntityResolver
) -> None:
    record = records[UFPS_CUCUTA]
    disabled, _blocked = record_alias_policy(record)
    alias = normalize_text("universidad francisco de paula santander")
    assert alias in {normalize_text(value) for value in record["aliases"]}
    assert alias in disabled
    assert resolver.resolve_entidad(alias).value == UFPS_CUCUTA


def test_ocana_scan_wins_over_cucuta_fragment(resolver: EntityResolver) -> None:
    text = normalize_text(
        "contratos de la universidad francisco de paula santander seccional ocaña"
    )
    hit = resolver.scan_entity(text)
    assert hit is not None
    assert hit.official_name == UFPS_OCANA


def test_no_new_exact_multi_owner_keys(gazetteer: list[dict]) -> None:
    assert new_multi_owner_keys(gazetteer, load_baseline()) == []


def test_exact_multi_owner_does_not_increase(gazetteer: list[dict]) -> None:
    baseline = load_baseline()
    assert len(multi_owner_groups(gazetteer)) <= len(baseline)


def test_sidecar_provenance_points_to_accumulated_audit() -> None:
    payload = json.loads(SOURCE_NAMES.read_text(encoding="utf-8"))
    assert payload["evidence"] == "artifacts/audits/public_university_pilot_r2fa.json"
    assert payload["authority"] == "ADR-073"
    assert len(payload["bindings"]) == 5
