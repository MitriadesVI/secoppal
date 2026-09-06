"""Contract tests for the stable M1A -> M2B.1 territorial knowledge shadow."""
from __future__ import annotations

import ast
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pytest

from app.core.territorial_knowledge import (
    DATA_PATH,
    TerritorialKnowledgeError,
    get_institution,
    get_territory,
    institutions_for_family,
    institutions_for_territory,
    load,
    validate,
)
from scripts.build_territorial_knowledge_v0 import (
    AUTHORIZED_TERRITORIAL_RUNTIME_CONSUMERS,
    _build_payload,
    _runtime_imports,
    _unauthorized_runtime_imports,
    render,
)

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def payload() -> dict:
    return json.loads(DATA_PATH.read_text(encoding="utf-8"))


def _read(relative: str) -> dict:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def test_frozen_counts_and_required_gates(payload):
    assert payload["state"] == "SHADOW_NO_RUNTIME"
    assert payload["authority"] == "ADR-080_TERRITORIAL_KNOWLEDGE_SHADOW_V0"
    assert payload["next"] == "ENTITY_ANALYSIS_V1"
    assert payload["counts"] == {
        "territories": 1103,
        "institutions": 1099,
        "m2a_logical_units": 994,
        "m2b_offline_candidates": 55,
        "m2b1_new_offline_candidates": 43,
        "m2b1_existing_product_links": 7,
        "identity_pending_profile_refs": 826,
        "institutions_by_status": {
            "PRODUCT_RECOGNIZED": 632,
            "VERIFIED_LOGICAL_UNRECOGNIZED": 369,
            "ADJUDICATED_OFFLINE_CANDIDATE": 98,
        },
    }
    assert payload["gates"] == {
        "TERRITORIES": 1103,
        "DUPLICATE_INSTITUTION_IDS": 0,
        "MULTI_TERRITORY_INSTITUTION": 0,
        "IDENTITY_PENDING_AS_INSTITUTION": 0,
        "UNRESOLVED_AFFILIATION_AS_MEMBERSHIP": 0,
        "M2B1_EXISTING_LINKS_DUPLICATED": 0,
        "RUNTIME_IMPORTS": 0,
        "BUYER_GROUP_MEMBERSHIP_REDEFINED": 0,
    }


def test_m2a_units_enter_once_and_nit_never_is_an_id(payload):
    source_ids = {
        row["logical_unit_id"]
        for row in _read("artifacts/audits/municipal_m2a/logical_units.json")["units"]
    }
    ids = [row["institution_id"] for row in payload["institutions"]]
    assert len(source_ids) == 994
    assert all(ids.count(institution_id) == 1 for institution_id in source_ids)
    assert all(not institution_id.isdigit() and "NIT" not in institution_id for institution_id in ids)


def test_m2b_and_m2b1_candidate_sets_are_exact(payload):
    candidates = {
        row["institution_id"]
        for row in payload["institutions"]
        if row["knowledge_status"] == "ADJUDICATED_OFFLINE_CANDIDATE"
    }
    m2b = _read("artifacts/audits/municipal_m2b/adjudicated_candidates.json")["candidates"]
    m2b1 = _read("artifacts/audits/municipal_m2b_1/adjudicated_delta.json")["candidates"]
    expected = {row["candidate_logical_unit_id"] for row in m2b}
    expected.update(
        row["candidate_logical_unit_id"] for row in m2b1 if row["creates_new_logical_unit"]
    )
    assert candidates == expected
    assert sum(value.startswith("m2b-logical:") for value in candidates) == 55
    assert sum(value.startswith("m2b1-logical:") for value in candidates) == 43


def test_m2b1_existing_links_enrich_product_institutions_once(payload):
    source = _read("artifacts/audits/municipal_m2b_1/adjudicated_delta.json")["candidates"]
    links = [row for row in source if not row["creates_new_logical_unit"]]
    institutions = {row["institution_id"]: row for row in payload["institutions"]}
    assert len(links) == 7
    for link in links:
        institution = institutions[link["candidate_logical_unit_id"]]
        assert institution["knowledge_status"] == "PRODUCT_RECOGNIZED"
        assert institution["product_buyer_unit"] == link["target_existing_canonical"]
        assert institution["profile_refs"] == [link["profile_id"]]


def test_identity_pending_is_debt_only_and_affiliation_is_accredited(payload):
    remaining = set(_read("artifacts/audits/municipal_m2b_1/impact.json")["remaining"]["profile_ids"])
    pending_rows = _read("artifacts/audits/municipal_m2b/pending_identity_profiles.json")["profiles"]
    pending = {row["profile_id"]: row for row in pending_rows}
    territory_refs = {
        profile_id
        for territory in payload["territories"]
        for profile_id in territory["identity_pending_profile_refs"]
    }
    institution_refs = {
        profile_id
        for institution in payload["institutions"]
        for profile_id in institution["profile_refs"]
    }
    assert territory_refs == remaining
    assert not territory_refs & institution_refs
    assert all(pending[profile_id]["affiliation"] == "VERIFIED" for profile_id in remaining)
    assert all(pending[profile_id]["territory_divipola"] for profile_id in remaining)


def test_bogota_references_declared_groups_without_membership_copy(payload):
    bogota = next(row for row in payload["territories"] if row["divipola"] == "11001")
    assert bogota["buyer_group_refs"] == [
        "bogota_central", "bogota_executive", "bogota_local_mayoralties",
    ]
    assert all(
        not row["buyer_group_refs"]
        for row in payload["territories"]
        if row["divipola"] != "11001"
    )
    assert all("members" not in row for row in payload["territories"])
    declared = {row["id"] for row in _read("app/data/buyer_groups.json")["groups"]}
    assert set(bogota["buyer_group_refs"]) <= declared


def test_baranoa_keeps_territory_and_buyer_unit_separate(payload):
    territory = next(row for row in payload["territories"] if row["divipola"] == "08078")
    institutions = {row["institution_id"]: row for row in payload["institutions"]}
    matches = [
        institutions[ref]
        for ref in territory["institution_refs"]
        if institutions[ref]["display_name"] == "ALCALDIA MUNICIPAL DE BARANOA"
    ]
    assert territory["municipality"] == "BARANOA"
    assert territory["macro_region"] == "Caribe"
    assert len(matches) == 1
    assert matches[0]["knowledge_status"] == "PRODUCT_RECOGNIZED"
    assert matches[0]["product_buyer_unit"] == "ALCALDIA MUNICIPAL DE BARANOA"


def test_loader_exposes_only_exact_read_only_indexes():
    registry = load()
    baranoa = get_territory("08078")
    assert baranoa is not None and baranoa.municipality == "BARANOA"
    items = institutions_for_territory("08078")
    assert items and all(row.territory_divipola == "08078" for row in items)
    assert all(get_institution(row.institution_id) == row for row in items)
    assert institutions_for_territory("00000") == ()
    assert institutions_for_family("NOT_A_FAMILY") == ()
    executive = institutions_for_family("MUNICIPAL_EXECUTIVE")
    assert executive and all(row.family == "MUNICIPAL_EXECUTIVE" for row in executive)
    with pytest.raises(TypeError):
        registry.territory_index["00000"] = baranoa
    with pytest.raises(Exception):
        baranoa.municipality = "OTHER"


def test_loader_fails_closed_on_boundary_violations(payload):
    duplicate = deepcopy(payload)
    duplicate["institutions"].append(deepcopy(duplicate["institutions"][0]))
    with pytest.raises(TerritorialKnowledgeError, match="sorted unique"):
        validate(duplicate)
    pending = deepcopy(payload)
    pending["territories"][0]["identity_pending_profile_refs"][0:1] = [
        pending["institutions"][0]["profile_refs"][0]
    ]
    pending["territories"][0]["identity_pending_profile_refs"].sort()
    with pytest.raises(TerritorialKnowledgeError, match="identity-pending"):
        validate(pending)
    groups = deepcopy(payload)
    groups["territories"][0]["buyer_group_refs"] = ["bogota_central"]
    with pytest.raises(TerritorialKnowledgeError, match="BuyerGroup"):
        validate(groups)
    root = deepcopy(payload)
    root["buyer_group_members"] = []
    with pytest.raises(TerritorialKnowledgeError, match="root fields"):
        validate(root)
    digest = deepcopy(payload)
    digest["source_digests"]["app/data/aliases_db.json"] = "not-a-digest"
    with pytest.raises(TerritorialKnowledgeError, match="source digests"):
        validate(digest)


def test_build_is_byte_reproducible_and_matches_committed_registry(tmp_path):
    first = render(_build_payload())
    second = render(_build_payload())
    assert first == second == DATA_PATH.read_bytes()
    assert hashlib.sha256(first).hexdigest() == "c0d6933182a7ccbdecaf465b6380ecca9507c816719607d3d64fe630ddbc9c63"
    assert hashlib.sha256(first).hexdigest() == hashlib.sha256(second).hexdigest()
    replay = tmp_path / "territorial_knowledge_v0.json"
    replay.write_bytes(second)
    assert replay.read_bytes() == DATA_PATH.read_bytes()


def test_registry_has_exactly_one_authorized_runtime_consumer_and_no_prohibited_surfaces():
    module = ROOT / "app/core/territorial_knowledge.py"
    assert AUTHORIZED_TERRITORIAL_RUNTIME_CONSUMERS == frozenset({
        "app/core/entity_profile/scope.py",
    })
    assert _runtime_imports() == ["app/core/entity_profile/scope.py"]
    assert _unauthorized_runtime_imports() == []
    tree = ast.parse(module.read_text(encoding="utf-8"))
    imports = {
        alias.name.lower()
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    imports.update(
        node.module.lower()
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    for prohibited in (
        "query_router", "soql", "radar", "analytics", "formatter", "followup",
        "aliases", "bindings", "buyer_groups", "result_identity",
    ):
        assert all(prohibited not in imported for imported in imports)
