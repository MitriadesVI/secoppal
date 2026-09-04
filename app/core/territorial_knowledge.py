"""Read-only loader and exact indexes for the territorial knowledge shadow registry."""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, Mapping

DATA_PATH = Path(__file__).resolve().parents[1] / "data/territorial_knowledge_v0.json"
KNOWLEDGE_STATUSES = frozenset({
    "PRODUCT_RECOGNIZED",
    "VERIFIED_LOGICAL_UNRECOGNIZED",
    "ADJUDICATED_OFFLINE_CANDIDATE",
})
EXPECTED_GATES = {
    "TERRITORIES": 1103,
    "DUPLICATE_INSTITUTION_IDS": 0,
    "MULTI_TERRITORY_INSTITUTION": 0,
    "IDENTITY_PENDING_AS_INSTITUTION": 0,
    "UNRESOLVED_AFFILIATION_AS_MEMBERSHIP": 0,
    "M2B1_EXISTING_LINKS_DUPLICATED": 0,
    "RUNTIME_IMPORTS": 0,
    "BUYER_GROUP_MEMBERSHIP_REDEFINED": 0,
}
EXPECTED_COUNTS = {
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
EXPECTED_SOURCES = frozenset({
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
})
_ID = re.compile(r"(?:logical|m2b-logical|m2b1-logical):[0-9a-f]{20}")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class TerritorialKnowledgeError(ValueError):
    """The shadow registry is malformed or violates an authority boundary."""


@dataclass(frozen=True, slots=True)
class Territory:
    divipola: str
    municipality: str
    department: str
    macro_region: str
    institution_refs: tuple[str, ...]
    identity_pending_profile_refs: tuple[str, ...]
    buyer_group_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Institution:
    institution_id: str
    territory_divipola: str
    family: str
    knowledge_status: str
    display_name: str
    product_buyer_unit: str | None
    profile_refs: tuple[str, ...]
    identity_authorities: tuple[str, ...]
    provenance_refs: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TerritorialKnowledgeRegistry:
    territories: tuple[Territory, ...]
    institutions: tuple[Institution, ...]
    territory_index: Mapping[str, Territory]
    institution_index: Mapping[str, Institution]
    territory_institution_index: Mapping[str, tuple[Institution, ...]]
    family_index: Mapping[str, tuple[Institution, ...]]


def _nonempty(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TerritorialKnowledgeError(f"{field} must be a non-empty string")
    return value


def _string_tuple(value: Any, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise TerritorialKnowledgeError(f"{field} must be a string list")
    result = tuple(value)
    if tuple(sorted(set(result))) != result:
        raise TerritorialKnowledgeError(f"{field} must be sorted and unique")
    return result


def _parse_territory(raw: Any) -> Territory:
    if not isinstance(raw, dict) or set(raw) != {
        "divipola", "municipality", "department", "macro_region", "institution_refs",
        "identity_pending_profile_refs", "buyer_group_refs",
    }:
        raise TerritorialKnowledgeError("invalid Territory fields")
    code = _nonempty(raw["divipola"], "Territory.divipola")
    if not re.fullmatch(r"\d{5}", code):
        raise TerritorialKnowledgeError(f"invalid DIVIPOLA: {code}")
    return Territory(
        divipola=code,
        municipality=_nonempty(raw["municipality"], "Territory.municipality"),
        department=_nonempty(raw["department"], "Territory.department"),
        macro_region=_nonempty(raw["macro_region"], "Territory.macro_region"),
        institution_refs=_string_tuple(raw["institution_refs"], "Territory.institution_refs"),
        identity_pending_profile_refs=_string_tuple(
            raw["identity_pending_profile_refs"], "Territory.identity_pending_profile_refs"
        ),
        buyer_group_refs=_string_tuple(raw["buyer_group_refs"], "Territory.buyer_group_refs"),
    )


def _parse_institution(raw: Any) -> Institution:
    if not isinstance(raw, dict) or set(raw) != {
        "institution_id", "territory_divipola", "family", "knowledge_status", "display_name",
        "product_buyer_unit", "profile_refs", "identity_authorities", "provenance_refs",
    }:
        raise TerritorialKnowledgeError("invalid Institution fields")
    institution_id = _nonempty(raw["institution_id"], "Institution.institution_id")
    if not _ID.fullmatch(institution_id):
        raise TerritorialKnowledgeError(f"invalid logical institution ID: {institution_id}")
    status = _nonempty(raw["knowledge_status"], "Institution.knowledge_status")
    if status not in KNOWLEDGE_STATUSES:
        raise TerritorialKnowledgeError(f"unknown knowledge status: {status}")
    product = raw["product_buyer_unit"]
    if product is not None:
        product = _nonempty(product, "Institution.product_buyer_unit")
    if (status == "PRODUCT_RECOGNIZED") != bool(product):
        raise TerritorialKnowledgeError(f"product/status mismatch: {institution_id}")
    return Institution(
        institution_id=institution_id,
        territory_divipola=_nonempty(raw["territory_divipola"], "Institution.territory"),
        family=_nonempty(raw["family"], "Institution.family"),
        knowledge_status=status,
        display_name=_nonempty(raw["display_name"], "Institution.display_name"),
        product_buyer_unit=product,
        profile_refs=_string_tuple(raw["profile_refs"], "Institution.profile_refs"),
        identity_authorities=_string_tuple(
            raw["identity_authorities"], "Institution.identity_authorities"
        ),
        provenance_refs=_string_tuple(raw["provenance_refs"], "Institution.provenance_refs"),
    )


def validate(payload: Any) -> tuple[tuple[Territory, ...], tuple[Institution, ...]]:
    if not isinstance(payload, dict):
        raise TerritorialKnowledgeError("registry root must be an object")
    if set(payload) != {
        "schema_version", "version", "state", "authority", "next", "source_digests",
        "counts", "gates", "territories", "institutions",
    }:
        raise TerritorialKnowledgeError("invalid territorial knowledge root fields")
    if payload.get("schema_version") != 0 or payload.get("version") != "TERRITORIAL_KNOWLEDGE_V0":
        raise TerritorialKnowledgeError("unsupported territorial knowledge version")
    if payload.get("state") != "SHADOW_NO_RUNTIME":
        raise TerritorialKnowledgeError("registry is not shadow-only")
    if payload.get("authority") != "ADR-080_TERRITORIAL_KNOWLEDGE_SHADOW_V0":
        raise TerritorialKnowledgeError("unknown shadow authority")
    if payload.get("next") != "ENTITY_ANALYSIS_V1":
        raise TerritorialKnowledgeError("unexpected next phase")
    if payload.get("gates") != EXPECTED_GATES:
        raise TerritorialKnowledgeError("registry gates are not closed")
    if payload.get("counts") != EXPECTED_COUNTS:
        raise TerritorialKnowledgeError("registry counts drifted")
    digests = payload.get("source_digests")
    if (not isinstance(digests, dict) or set(digests) != EXPECTED_SOURCES or
            any(not isinstance(value, str) or not _SHA256.fullmatch(value)
                for value in digests.values())):
        raise TerritorialKnowledgeError("invalid frozen source digests")
    territories = tuple(_parse_territory(row) for row in payload.get("territories", []))
    institutions = tuple(_parse_institution(row) for row in payload.get("institutions", []))
    territory_codes = [row.divipola for row in territories]
    institution_ids = [row.institution_id for row in institutions]
    if territory_codes != sorted(set(territory_codes)) or len(territories) != 1103:
        raise TerritorialKnowledgeError("territories must be 1,103 sorted unique DIVIPOLA records")
    if institution_ids != sorted(set(institution_ids)):
        raise TerritorialKnowledgeError("institutions must have sorted unique logical IDs")
    territory_set = set(territory_codes)
    if any(row.territory_divipola not in territory_set for row in institutions):
        raise TerritorialKnowledgeError("institution references an unknown territory")
    referenced = [ref for territory in territories for ref in territory.institution_refs]
    if sorted(referenced) != institution_ids:
        raise TerritorialKnowledgeError("territory institution refs must partition institutions once")
    profile_refs = [ref for row in institutions for ref in row.profile_refs]
    if len(profile_refs) != len(set(profile_refs)):
        raise TerritorialKnowledgeError("institution profile refs must be unique")
    institution_profiles = set(profile_refs)
    pending = [ref for territory in territories for ref in territory.identity_pending_profile_refs]
    if len(pending) != 826 or len(pending) != len(set(pending)):
        raise TerritorialKnowledgeError("identity-pending refs must contain 826 unique profiles")
    if institution_profiles & set(pending):
        raise TerritorialKnowledgeError("identity-pending profile cannot be an institution")
    bogota_groups = next(row.buyer_group_refs for row in territories if row.divipola == "11001")
    if bogota_groups != (
        "bogota_central", "bogota_executive", "bogota_local_mayoralties",
    ) or any(row.buyer_group_refs for row in territories if row.divipola != "11001"):
        raise TerritorialKnowledgeError("BuyerGroup refs must preserve the Bogotá declaration only")
    if len(institutions) != EXPECTED_COUNTS["institutions"]:
        raise TerritorialKnowledgeError("institution count metadata drifted")
    status_counts = {
        status: sum(row.knowledge_status == status for row in institutions)
        for status in KNOWLEDGE_STATUSES
    }
    if status_counts != EXPECTED_COUNTS["institutions_by_status"]:
        raise TerritorialKnowledgeError("institution status counts drifted")
    return territories, institutions


def load(path: Path = DATA_PATH) -> TerritorialKnowledgeRegistry:
    payload = json.loads(path.read_text(encoding="utf-8"))
    territories, institutions = validate(payload)
    by_territory = {row.divipola: row for row in territories}
    by_institution = {row.institution_id: row for row in institutions}
    territory_institutions = {
        code: tuple(by_institution[ref] for ref in territory.institution_refs)
        for code, territory in by_territory.items()
    }
    families: dict[str, list[Institution]] = {}
    for institution in institutions:
        families.setdefault(institution.family, []).append(institution)
    return TerritorialKnowledgeRegistry(
        territories=territories,
        institutions=institutions,
        territory_index=MappingProxyType(by_territory),
        institution_index=MappingProxyType(by_institution),
        territory_institution_index=MappingProxyType(territory_institutions),
        family_index=MappingProxyType({key: tuple(value) for key, value in families.items()}),
    )


@lru_cache(maxsize=1)
def _default() -> TerritorialKnowledgeRegistry:
    return load()


def get_territory(divipola: str) -> Territory | None:
    return _default().territory_index.get(divipola)


def get_institution(institution_id: str) -> Institution | None:
    return _default().institution_index.get(institution_id)


def institutions_for_territory(divipola: str) -> tuple[Institution, ...]:
    return _default().territory_institution_index.get(divipola, ())


def institutions_for_family(family: str) -> tuple[Institution, ...]:
    return _default().family_index.get(family, ())
