"""R2D.3 — exact alias ownership is not text-scan authority.

The 17 hijacks are read from the immutable R2D.2 input artifact.  Decisions,
owners, exact behavior and policy mutations are frozen explicitly here.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
from app.core.scan_alias_policy import (
    is_scan_eligible_alias,
    normalize_alias_policy,
    record_alias_policy,
)
from app.utils.money import normalize_text

ALIASES_DB = Path("app/data/aliases_db.json")
R2D2_ARTIFACT = Path("artifacts/audits/freed_alias_safety_r2d2.json")

KEEP_EXACT_DISABLE_SCAN = {
    "candelaria valle": "ALCALDÍA MUNICIPAL DE CANDELARIA VALLE",
    "puerto boyaca": "ALCALDIA MUNICIPAL DE PUERTO BOYACA",
    "soledad atlantico": "MUNICIPIO DE SOLEDAD",
}

REMOVE_ALIAS_COMPLETELY = {
    "ciudad bolivar": "ALCALDIA LOCAL DE CIUDAD BOLIVAR //",
    "corporacion quindio": "CORPORACION AUTONOMA REGIONAL DEL QUINDIO",
    "francisco jose de caldas": "UNIVERSIDAD DISTRITAL FRANCISCO JOSE DE CALDAS",
    "salud arauca": "UNIDAD ADMINISTRATIVA ESPECIAL DE SALUD DE ARAUCA",
    "salud narino": "INSTITUTO DEPARTAMENTAL DE SALUD DE NARIÑO",
    "santa marta magdalena": "ALCALDÍA DISTRITAL DE SANTA MARTA",
}

MUTATION_CANARIES = (
    (
        "puerto boyaca",
        "ALCALDIA MUNICIPAL DE PUERTO BOYACA",
        "CONCEJO MUNICIPAL DE PUERTO BOYACA",
    ),
    (
        "ciudad bolivar",
        "ALCALDIA LOCAL DE CIUDAD BOLIVAR //",
        "COLEGIO CEDID CIUDAD BOLIVAR INSTITUCION EDUCATIVA DISTRITAL",
    ),
    (
        "santa marta magdalena",
        "ALCALDÍA DISTRITAL DE SANTA MARTA",
        "Terminal de Transportes de Santa Marta Magdalena",
    ),
)


def _load_gazetteer() -> list[dict]:
    return json.loads(ALIASES_DB.read_text(encoding="utf-8"))


def _frozen_new_hijacks() -> list[dict]:
    artifact = json.loads(R2D2_ARTIFACT.read_text(encoding="utf-8"))
    return [
        record
        for record in artifact["adversarial_sample"]["records"]
        if record["new_wrong_owner_confirmed"]
    ]


@pytest.fixture(scope="module")
def resolver() -> EntityResolver:
    return EntityResolver(ALIASES_DB)


@pytest.fixture(scope="module")
def router(resolver: EntityResolver) -> QueryRouter:
    return QueryRouter(entity_resolver=resolver)


@pytest.mark.parametrize(
    ("alias", "owner"),
    KEEP_EXACT_DISABLE_SCAN.items(),
)
def test_exact_owner_survives_when_scan_is_disabled(
    resolver: EntityResolver, alias: str, owner: str
) -> None:
    assert resolver.owners_by_alias[alias] == {owner}

    exact = resolver.resolve_entidad(alias)
    assert exact.value == owner
    assert exact.method == "exact"

    assert resolver.scan_entity(normalize_text(alias)) is None


@pytest.mark.parametrize(
    ("alias", "former_owner"),
    REMOVE_ALIAS_COMPLETELY.items(),
)
def test_removed_alias_has_no_owner_and_cannot_reopen_through_fuzzy(
    resolver: EntityResolver, alias: str, former_owner: str
) -> None:
    assert resolver.owners_by_alias.get(alias, set()) == set()

    result = resolver.resolve_entidad(alias)
    assert result.value is None
    assert result.method == "alias_resolution_blocked"
    assert result.clarification_needed is True
    assert result.metadata["blocked_alias"] == alias
    assert former_owner not in resolver.owners_by_alias.get(alias, set())


def test_data_schema_expresses_the_nine_decisions_without_code_denylist() -> None:
    data = _load_gazetteer()
    by_official = {record["official_name"]: record for record in data}

    for alias, owner in KEEP_EXACT_DISABLE_SCAN.items():
        record = by_official[owner]
        owned = {normalize_text(value) for value in record["aliases"]}
        scan_disabled, resolution_blocked = record_alias_policy(record)
        assert alias in owned
        assert alias in scan_disabled
        assert alias not in resolution_blocked

    for alias, owner in REMOVE_ALIAS_COMPLETELY.items():
        record = by_official[owner]
        owned = {normalize_text(value) for value in record["aliases"]}
        scan_disabled, resolution_blocked = record_alias_policy(record)
        assert alias not in owned
        assert alias not in scan_disabled
        assert alias in resolution_blocked


def test_r2d2_frozen_universe_is_exactly_nine_aliases_and_seventeen_buyers() -> None:
    records = _frozen_new_hijacks()
    assert len(records) == 17
    assert {str(record["alias"]) for record in records} == {
        *KEEP_EXACT_DISABLE_SCAN,
        *REMOVE_ALIAS_COMPLETELY,
    }


@pytest.mark.parametrize(
    "record",
    _frozen_new_hijacks(),
    ids=lambda record: (
        f"{record['alias']}-"
        f"{record['expected_secop_buyer']['code']}"
    ),
)
def test_none_of_the_seventeen_physical_buyers_is_scanned_as_the_former_owner(
    resolver: EntityResolver, record: dict
) -> None:
    former_owner = record["current_owners"][0]
    phrase = record["probe"]["phrase"]
    result = resolver.scan_entity(normalize_text(phrase))
    assert result is None or result.official_name != former_owner


@pytest.mark.parametrize(
    "record",
    _frozen_new_hijacks(),
    ids=lambda record: (
        f"router-{record['alias']}-"
        f"{record['expected_secop_buyer']['code']}"
    ),
)
def test_product_route_never_returns_the_former_owner_for_the_seventeen_buyers(
    router: QueryRouter, record: dict
) -> None:
    former_owner = record["current_owners"][0]
    params = router.parse(record["probe"]["query"]).params
    assert params.get("entidad_resolved") != former_owner


def test_policy_is_pure_and_normalized() -> None:
    disabled = normalize_alias_policy(["Puerto Boyacá", "SOLEDAD ATLÁNTICO"])
    assert disabled == frozenset({"puerto boyaca", "soledad atlantico"})
    assert is_scan_eligible_alias("Candelaria Valle", disabled) is True
    assert is_scan_eligible_alias("Puerto Boyaca", disabled) is False


@pytest.mark.parametrize("invalid", ["puerto boyaca", ["valid", 7]])
def test_policy_schema_rejects_non_list_or_non_string_values(invalid: object) -> None:
    with pytest.raises(TypeError, match="lists of strings|only strings"):
        normalize_alias_policy(invalid)  # type: ignore[arg-type]


def test_resolution_block_cannot_coexist_with_exact_ownership(tmp_path: Path) -> None:
    path = tmp_path / "conflicting_aliases.json"
    path.write_text(
        json.dumps(
            [
                {
                    "official_name": "TEST BUYER",
                    "aliases": ["test buyer"],
                    "resolution_blocked_aliases": ["test buyer"],
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="still have owners"):
        EntityResolver(path)


def test_scan_disabled_alias_must_still_be_exactly_owned(tmp_path: Path) -> None:
    path = tmp_path / "orphan_scan_policy.json"
    path.write_text(
        json.dumps(
            [
                {
                    "official_name": "TEST BUYER",
                    "aliases": ["test buyer"],
                    "scan_disabled_aliases": ["missing alias"],
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="disabled aliases are not owned"):
        EntityResolver(path)


def test_disabling_r2d3_reopens_puerto_ciudad_bolivar_and_santa_marta(
    tmp_path: Path,
) -> None:
    """Mutation: restore the three old scan authorities and all hijacks return.

    The future specific Santa Marta transport record is removed from this
    mutant on purpose: this mutation tests defense A (generic alias policy),
    independently of defense B (specific identity and longest match).
    """
    mutated = deepcopy(_load_gazetteer())
    mutation_by_owner = {
        owner: alias for alias, owner, _phrase in MUTATION_CANARIES
    }
    mutated = [
        record
        for record in mutated
        if record["official_name"] != "CENTRAL DE TRANSPORTE DE SANTA MARTA LTDA."
    ]
    for record in mutated:
        alias = mutation_by_owner.get(record["official_name"])
        if not alias:
            continue
        record.pop("scan_disabled_aliases", None)
        record.pop("resolution_blocked_aliases", None)
        normalized = {normalize_text(value) for value in record["aliases"]}
        if alias not in normalized:
            record["aliases"].append(alias)

    path = tmp_path / "mutated_aliases.json"
    path.write_text(json.dumps(mutated, ensure_ascii=False), encoding="utf-8")
    mutant = EntityResolver(path)

    for _alias, owner, phrase in MUTATION_CANARIES:
        result = mutant.scan_entity(normalize_text(phrase))
        assert result is not None
        assert result.official_name == owner
