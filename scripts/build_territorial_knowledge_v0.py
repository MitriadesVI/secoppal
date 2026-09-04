"""Build the deterministic M1A -> M2B.1 territorial knowledge shadow registry."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.data.colombia_geography import REGION_BY_DEPARTMENT

OUTPUT = ROOT / "app/data/territorial_knowledge_v0.json"
M2A = ROOT / "artifacts/audits/municipal_m2a"
M2B = ROOT / "artifacts/audits/municipal_m2b"
M2B1 = ROOT / "artifacts/audits/municipal_m2b_1"
BOGOTA = "11001"
BOGOTA_GROUPS = (
    "bogota_central",
    "bogota_executive",
    "bogota_local_mayoralties",
)
KNOWLEDGE_STATUSES = (
    "PRODUCT_RECOGNIZED",
    "VERIFIED_LOGICAL_UNRECOGNIZED",
    "ADJUDICATED_OFFLINE_CANDIDATE",
)
SOURCE_PATHS = (
    "app/data/aliases_db.json",
    "app/data/buyer_groups.json",
    "app/data/colombia_geography.py",
    "app/data/municipal_territories_v1.json",
    "artifacts/audits/municipal_m2a/logical_units.json",
    "artifacts/audits/municipal_m2a/physical_profiles.json",
    "artifacts/audits/municipal_m2b/adjudicated_candidates.json",
    "artifacts/audits/municipal_m2b/pending_identity_profiles.json",
    "artifacts/audits/municipal_m2b_1/adjudicated_delta.json",
    "artifacts/audits/municipal_m2b_1/gates.json",
    "artifacts/audits/municipal_m2b_1/impact.json",
)


def _read(relative: str) -> object:
    return json.loads((ROOT / relative).read_text(encoding="utf-8"))


def _digest(relative: str) -> str:
    return hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()


def _ref(relative: str, fragment: str) -> str:
    return f"{relative}#{fragment}"


def _normalized_department(value: str) -> str:
    value = "".join(
        char
        for char in unicodedata.normalize("NFD", value.upper())
        if unicodedata.category(char) != "Mn"
    )
    if value.startswith("ARCHIPIELAGO DE SAN ANDRES"):
        return "SAN ANDRES"
    if value.startswith("BOGOTA"):
        return "BOGOTA"
    return value


def _runtime_imports() -> list[str]:
    pattern = re.compile(r"(?:from\s+app\.core\s+import\s+territorial_knowledge|"
                         r"(?:from|import)\s+app\.core\.territorial_knowledge)")
    own_module = ROOT / "app/core/territorial_knowledge.py"
    return [
        str(path.relative_to(ROOT))
        for path in sorted((ROOT / "app").rglob("*.py"))
        if path != own_module and pattern.search(path.read_text(encoding="utf-8"))
    ]


def _m2a_institutions(logical: dict, profiles: dict[str, dict]) -> list[dict]:
    decisions = {row["profile_id"]: row for row in logical["decisions"]}
    institutions = []
    for index, unit in enumerate(logical["units"]):
        profile_refs = tuple(sorted(unit["profile_ids"]))
        names = sorted(profiles[profile_id]["physical_source_name"] for profile_id in profile_refs)
        claims = sorted({
            claim
            for profile_id in profile_refs
            for claim in decisions[profile_id]["existing_canonical_claims"]
        })
        if unit["recognized"] and len(claims) != 1:
            raise ValueError(f"recognized M2A unit lacks one product canonical: {unit}")
        if not unit["recognized"] and claims:
            raise ValueError(f"unrecognized M2A unit claims a product canonical: {unit}")
        product = claims[0] if claims else None
        institutions.append({
            "institution_id": unit["logical_unit_id"],
            "territory_divipola": unit["territory_divipola"],
            "family": unit["institutional_family"],
            "knowledge_status": (
                "PRODUCT_RECOGNIZED"
                if unit["recognized"]
                else "VERIFIED_LOGICAL_UNRECOGNIZED"
            ),
            "display_name": product or names[0],
            "product_buyer_unit": product,
            "profile_refs": list(profile_refs),
            "identity_authorities": sorted(set(unit["identity_authorities"])),
            "provenance_refs": [
                _ref("artifacts/audits/municipal_m2a/logical_units.json", f"/units/{index}"),
                *[
                    _ref(
                        "artifacts/audits/municipal_m2a/physical_profiles.json",
                        f"profile_id={profile_id}",
                    )
                    for profile_id in profile_refs
                ],
            ],
        })
    return institutions


def _m2b_institutions(candidates: list[dict]) -> list[dict]:
    institutions = []
    for index, candidate in enumerate(candidates):
        institutions.append({
            "institution_id": candidate["candidate_logical_unit_id"],
            "territory_divipola": candidate["territory_divipola"],
            "family": candidate["institutional_family"],
            "knowledge_status": "ADJUDICATED_OFFLINE_CANDIDATE",
            "display_name": candidate["physical_source_name"],
            "product_buyer_unit": None,
            "profile_refs": [candidate["profile_id"]],
            "identity_authorities": [candidate["identity_authority"]],
            "provenance_refs": sorted(set([
                _ref(
                    "artifacts/audits/municipal_m2b/adjudicated_candidates.json",
                    f"/candidates/{index}",
                ),
                candidate["profile_reference"],
                candidate["territorial_authority_reference"],
                *candidate["evidence_sources"],
            ])),
        })
    return institutions


def _m2b1_institutions(candidates: list[dict], product_names: set[str]) -> list[dict]:
    institutions = []
    for index, candidate in enumerate(candidates):
        product = candidate["target_existing_canonical"]
        if candidate["creates_new_logical_unit"] == bool(product):
            raise ValueError(f"inconsistent M2B.1 link/new flag: {candidate['profile_id']}")
        if product and product not in product_names:
            raise ValueError(f"M2B.1 target is not a declared product BuyerUnit: {product}")
        institutions.append({
            "institution_id": candidate["candidate_logical_unit_id"],
            "territory_divipola": candidate["territory_divipola"],
            "family": candidate["family"],
            "knowledge_status": (
                "ADJUDICATED_OFFLINE_CANDIDATE" if not product else "PRODUCT_RECOGNIZED"
            ),
            "display_name": product or candidate["physical_source_name"],
            "product_buyer_unit": product,
            "profile_refs": [candidate["profile_id"]],
            "identity_authorities": sorted(set([
                candidate["identity_authority"],
                *(["DECLARED_PRODUCT_BUYER_UNIT"] if product else []),
            ])),
            "provenance_refs": sorted(set([
                _ref(
                    "artifacts/audits/municipal_m2b_1/adjudicated_delta.json",
                    f"/candidates/{index}",
                ),
                *candidate["evidence_sources"],
                *(
                    [_ref("app/data/aliases_db.json", f"official_name={product}")]
                    if product
                    else []
                ),
            ])),
        })
    return institutions


def _build_payload() -> dict:
    geography = _read("app/data/municipal_territories_v1.json")
    logical = _read("artifacts/audits/municipal_m2a/logical_units.json")
    physical_rows = _read("artifacts/audits/municipal_m2a/physical_profiles.json")["profiles"]
    physical = {row["profile_id"]: row for row in physical_rows}
    m2b_payload = _read("artifacts/audits/municipal_m2b/adjudicated_candidates.json")
    pending_rows = _read("artifacts/audits/municipal_m2b/pending_identity_profiles.json")["profiles"]
    pending = {row["profile_id"]: row for row in pending_rows}
    m2b1_payload = _read("artifacts/audits/municipal_m2b_1/adjudicated_delta.json")
    m2b1_gates = _read("artifacts/audits/municipal_m2b_1/gates.json")
    impact = _read("artifacts/audits/municipal_m2b_1/impact.json")
    product_names = {row["official_name"] for row in _read("app/data/aliases_db.json")}
    group_ids = {row["id"] for row in _read("app/data/buyer_groups.json")["groups"]}

    m2b = m2b_payload["candidates"]
    m2b1 = m2b1_payload["candidates"]
    if m2b_payload["production_apply"] or m2b1_payload["production_apply"]:
        raise ValueError("candidate audit unexpectedly applied to production")
    if m2b1_gates["audit_integrity"] != "PASS":
        raise ValueError("M2B.1 frozen audit integrity is not PASS")
    if set(BOGOTA_GROUPS) - group_ids:
        raise ValueError("a declared Bogotá BuyerGroup is missing")

    institutions = [
        *_m2a_institutions(logical, physical),
        *_m2b_institutions(m2b),
        *_m2b1_institutions(m2b1, product_names),
    ]
    institutions.sort(key=lambda row: row["institution_id"])
    missing_products = {
        row["product_buyer_unit"]
        for row in institutions
        if row["product_buyer_unit"] and row["product_buyer_unit"] not in product_names
    }
    if missing_products:
        raise ValueError(f"unknown product BuyerUnits: {sorted(missing_products)}")
    for unit in logical["units"]:
        for profile_id in unit["profile_ids"]:
            profile = physical[profile_id]
            if (profile["affiliation"] != "VERIFIED" or
                    profile["territory_divipola"] != unit["territory_divipola"]):
                raise ValueError(f"M2A institution lacks one accredited territory: {profile_id}")
    by_territory: dict[str, list[str]] = {}
    for institution in institutions:
        by_territory.setdefault(institution["territory_divipola"], []).append(
            institution["institution_id"]
        )

    remaining_ids = impact["remaining"]["profile_ids"]
    pending_by_territory: dict[str, list[str]] = {}
    for profile_id in remaining_ids:
        profile = pending[profile_id]
        if profile["affiliation"] != "VERIFIED" or not profile["territory_divipola"]:
            raise ValueError(f"identity debt lacks accredited territory: {profile_id}")
        pending_by_territory.setdefault(profile["territory_divipola"], []).append(profile_id)

    territories = []
    for source in geography["territories"]:
        code = source["divipola"]
        region_key = _normalized_department(source["department"])
        if region_key not in REGION_BY_DEPARTMENT:
            raise ValueError(f"department has no macro-region: {source['department']}")
        territories.append({
            "divipola": code,
            "municipality": source["municipality"],
            "department": source["department"],
            "macro_region": REGION_BY_DEPARTMENT[region_key],
            "institution_refs": sorted(by_territory.get(code, [])),
            "identity_pending_profile_refs": sorted(pending_by_territory.get(code, [])),
            "buyer_group_refs": list(BOGOTA_GROUPS) if code == BOGOTA else [],
        })
    territories.sort(key=lambda row: row["divipola"])

    institution_ids = [row["institution_id"] for row in institutions]
    institution_profile_ids = {
        profile_id for row in institutions for profile_id in row["profile_refs"]
    }
    linked = [row for row in m2b1 if not row["creates_new_logical_unit"]]
    unresolved_memberships = [
        profile_id
        for territory in territories
        for profile_id in territory["identity_pending_profile_refs"]
        if pending[profile_id]["affiliation"] != "VERIFIED"
    ]
    runtime_imports = _runtime_imports()
    gates = {
        "TERRITORIES": len(territories),
        "DUPLICATE_INSTITUTION_IDS": len(institution_ids) - len(set(institution_ids)),
        "MULTI_TERRITORY_INSTITUTION": sum(
            len(codes) > 1
            for institution_id in set(institution_ids)
            if len(codes := {
                row["territory_divipola"]
                for row in institutions
                if row["institution_id"] == institution_id
            }) > 1
        ),
        "IDENTITY_PENDING_AS_INSTITUTION": len(set(remaining_ids) & institution_profile_ids),
        "UNRESOLVED_AFFILIATION_AS_MEMBERSHIP": len(unresolved_memberships),
        "M2B1_EXISTING_LINKS_DUPLICATED": len(linked) - len({
            row["candidate_logical_unit_id"] for row in linked
        }),
        "RUNTIME_IMPORTS": len(runtime_imports),
        "BUYER_GROUP_MEMBERSHIP_REDEFINED": sum(
            "members" in territory for territory in territories
        ),
    }
    required_gates = {
        "TERRITORIES": 1103,
        "DUPLICATE_INSTITUTION_IDS": 0,
        "MULTI_TERRITORY_INSTITUTION": 0,
        "IDENTITY_PENDING_AS_INSTITUTION": 0,
        "UNRESOLVED_AFFILIATION_AS_MEMBERSHIP": 0,
        "M2B1_EXISTING_LINKS_DUPLICATED": 0,
        "RUNTIME_IMPORTS": 0,
        "BUYER_GROUP_MEMBERSHIP_REDEFINED": 0,
    }
    if gates != required_gates:
        raise ValueError(f"territorial knowledge gates failed: {gates}")

    status_counts = {
        status: sum(row["knowledge_status"] == status for row in institutions)
        for status in KNOWLEDGE_STATUSES
    }
    counts = {
        "territories": len(territories),
        "institutions": len(institutions),
        "m2a_logical_units": len(logical["units"]),
        "m2b_offline_candidates": len(m2b),
        "m2b1_new_offline_candidates": sum(row["creates_new_logical_unit"] for row in m2b1),
        "m2b1_existing_product_links": len(linked),
        "identity_pending_profile_refs": len(remaining_ids),
        "institutions_by_status": status_counts,
    }
    expected_counts = {
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
    if counts != expected_counts:
        raise ValueError(f"territorial knowledge counts drifted: {counts}")
    if not all(re.fullmatch(r"(?:logical|m2b-logical|m2b1-logical):[0-9a-f]{20}", value)
               for value in institution_ids):
        raise ValueError("institution IDs must be logical IDs, never fiscal identities")

    baranoa = next(row for row in territories if row["divipola"] == "08078")
    baranoa_institutions = {
        row["display_name"]: row for row in institutions
        if row["institution_id"] in baranoa["institution_refs"]
    }
    canary = baranoa_institutions.get("ALCALDIA MUNICIPAL DE BARANOA")
    if not canary or canary["knowledge_status"] != "PRODUCT_RECOGNIZED":
        raise ValueError("Baranoa product institution canary is missing")

    return {
        "schema_version": 0,
        "version": "TERRITORIAL_KNOWLEDGE_V0",
        "state": "SHADOW_NO_RUNTIME",
        "authority": "ADR-080_TERRITORIAL_KNOWLEDGE_SHADOW_V0",
        "next": "ENTITY_ANALYSIS_V1",
        "source_digests": {relative: _digest(relative) for relative in SOURCE_PATHS},
        "counts": counts,
        "gates": gates,
        "territories": territories,
        "institutions": institutions,
    }


def render(payload: dict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def build(output: Path = OUTPUT) -> bytes:
    raw = render(_build_payload())
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(raw)
    return raw


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    raw = render(_build_payload())
    if args.check:
        if not args.output.exists() or args.output.read_bytes() != raw:
            raise SystemExit(f"STALE: {args.output}")
        print(f"PASS byte-reproducible {hashlib.sha256(raw).hexdigest()}")
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(raw)
    print(f"WROTE {args.output} {len(raw)} bytes {hashlib.sha256(raw).hexdigest()}")


if __name__ == "__main__":
    main()
