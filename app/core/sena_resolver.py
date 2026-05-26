"""
SENA Entity Ecosystem Resolver — pure resolver for SENA procurement entities.

SENA is structurally more complex than ICBF:
- National: Secretaría General + Dirección General
- Regional: parent + admin groups + centers + intercentros per department
- No 1:1 mapping — each department has multiple official SENA names

Resolution modes:
1. sena_national: global SENA (national entities + ALL regionales)
2. sena_regional_ecosystem: all SENA entities in a department
3. sena_macro_region: SENA across a macro-region
4. sena_unit_exact: specific SENA unit by exact name

No SECOP calls. No SoQL. No side effects.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from app.data.departamentos import DEPARTAMENTOS
from app.utils.money import normalize_text

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_SENA_CATALOG_PATH = Path(__file__).parent.parent / "data" / "sena_units.json"


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

ResolutionType = Literal[
    "sena_national",
    "sena_regional_ecosystem",
    "sena_macro_region",
    "sena_unit_exact",
    "none",
]


@dataclass(frozen=True)
class SENAResolution:
    resolution_type: ResolutionType
    label: str
    department: str | None = None
    region_macro: str | None = None
    official_names: list[str] = field(default_factory=list)
    contains_all: list[str] = field(default_factory=list)
    aliases_matched: list[str] = field(default_factory=list)
    confidence: Literal["high", "medium", "low"] = "low"
    warning: str | None = None


# ---------------------------------------------------------------------------
# Load catalog
# ---------------------------------------------------------------------------

_SENA_CATALOG: dict | None = None


def _load_catalog() -> dict:
    global _SENA_CATALOG
    if _SENA_CATALOG is None:
        _SENA_CATALOG = json.loads(_SENA_CATALOG_PATH.read_text(encoding="utf-8"))
    assert _SENA_CATALOG is not None
    return _SENA_CATALOG


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_sena_mention(normalized: str) -> bool:
    """Does the query mention SENA or its synonyms?

    Must match as whole word or at word boundary to avoid matching
    'senado', 'senador', 'presentar', etc.
    """
    # G7C.1: reject false positives first
    false_positives = ["senado", "senador"]
    for fp in false_positives:
        if fp in normalized:
            return False

    triggers = [
        " sena ", " sena", "sena ", " sena.",
        "servicio nacional de aprendizaje",
    ]
    for t in triggers:
        if t in normalized:
            return True
    # Also check at start of string
    if normalized.startswith("sena ") or normalized == "sena":
        return True
    if normalized.startswith("servicio nacional de aprendizaje"):
        return True
    return False


def _build_trigger_index(catalog: dict) -> dict[str, str]:
    """Build normalized alias → regional group lookup."""
    index: dict[str, str] = {}
    for group in catalog.get("regional_groups", []):
        for alias in group.get("aliases", []):
            key = normalize_text(alias)
            if key not in index:
                index[key] = group["department"]
    return index


def _find_department_in_normalized(normalized: str) -> str | None:
    """Find a department mention in the normalized query text."""
    # Check DEPARTAMENTOS first (broader coverage)
    for key in sorted(DEPARTAMENTOS.keys(), key=len, reverse=True):
        if key in normalized:
            return DEPARTAMENTOS[key]
    return None


def _find_macro_region(normalized: str, catalog: dict) -> tuple[str | None, list[str] | None]:
    """Check if query has macro region keywords."""
    for region_name, depts in catalog.get("macro_regions", {}).items():
        norm_region = normalize_text(region_name.replace("_", " "))
        if norm_region in normalized:
            return region_name, depts
    return None, None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_sena_reference(raw_query: str) -> SENAResolution | None:
    """
    Resolve a SENA reference in a user query.

    Returns a SENAResolution if SENA is mentioned, None otherwise.
    Pure resolver — no side effects.
    """
    normalized = normalize_text(raw_query)
    if not _is_sena_mention(normalized):
        return None

    catalog = _load_catalog()
    trigger_index = _build_trigger_index(catalog)

    # Collect matched aliases
    matched_aliases: list[str] = []
    # Check national triggers first
    for alias in catalog["national"]["trigger_aliases"]:
        norm_alias = normalize_text(alias)
        if norm_alias in normalized:
            matched_aliases.append(alias)

    # 1. Check for macro region FIRST (before department — macro might contain dept names)
    macro_region, macro_depts = _find_macro_region(normalized, catalog)
    if macro_region and macro_depts:
        official_names: list[str] = []
        contains_all_list: list[list[str]] = []
        for dept in macro_depts:
            for group in catalog.get("regional_groups", []):
                if group["department"] == dept:
                    contains_all_list.append(group.get("member_name_contains_all", []))
                    # Collect official names if available (from gazetteer or audit)
        return SENAResolution(
            resolution_type="sena_macro_region",
            label=f"SENA en {macro_region}",
            region_macro=macro_region,
            official_names=official_names,
            contains_all=[item for sublist in contains_all_list for item in sublist],
            aliases_matched=matched_aliases,
            confidence="high",
        )

    # 2. Check alias index for specific regional match
    for alias_key in sorted(trigger_index.keys(), key=len, reverse=True):
        if alias_key in normalized:
            dept = trigger_index[alias_key]
            # Find the matching group
            for group in catalog.get("regional_groups", []):
                if group["department"] == dept:
                    return SENAResolution(
                        resolution_type="sena_regional_ecosystem",
                        label=f"SENA {dept}",
                        department=dept,
                        contains_all=group.get("member_name_contains_all", []),
                        official_names=[],  # Filled at query time via SECOP
                        aliases_matched=matched_aliases + [alias_key],
                        confidence="high",
                    )

    # 3. Check department mention (for "sena en {dept}" patterns)
    department = _find_department_in_normalized(normalized)
    if department:
        # Try to find matching regional group
        for group in catalog.get("regional_groups", []):
            if group["department"] == department:
                return SENAResolution(
                    resolution_type="sena_regional_ecosystem",
                    label=f"SENA {department}",
                    department=department,
                    contains_all=group.get("member_name_contains_all", []),
                    official_names=[],
                    aliases_matched=matched_aliases,
                    confidence="high",
                )
        # Department not in catalog — still return regional with lower confidence
        return SENAResolution(
            resolution_type="sena_regional_ecosystem",
            label=f"SENA {department}",
            department=department,
            contains_all=["SENA"],
            official_names=[],
            aliases_matched=matched_aliases,
            confidence="medium",
        )

    # 4. No department/macro — national/global
    official_names = list(catalog["national"].get("official_names", []))
    return SENAResolution(
        resolution_type="sena_national",
        label="SENA Nacional",
        official_names=official_names,
        contains_all=[],
        aliases_matched=matched_aliases,
        confidence="high",
        warning=(
            "SENA se interpretó como ecosistema nacional: "
            "incluye Secretaría General, Dirección General y todas las regionales. "
            "Puedes filtrar por departamento o macro-región."
        ),
    )


def build_sena_filter_metadata(resolution: SENAResolution) -> dict:
    """
    Build filter metadata from a SENA resolution.

    Returns a dict that can be merged into query params.
    No SoQL — metadata only.
    """
    metadata: dict = {
        "entity_family": "SENA",
        "resolution_type": resolution.resolution_type,
        "confidence": resolution.confidence,
    }

    if resolution.resolution_type == "sena_national":
        metadata["official_names"] = resolution.official_names
        if resolution.warning:
            metadata["warning"] = resolution.warning

    elif resolution.resolution_type == "sena_regional_ecosystem":
        metadata["department"] = resolution.department
        metadata["contains_all"] = resolution.contains_all
        metadata["entidad_like_candidates"] = resolution.contains_all

    elif resolution.resolution_type == "sena_macro_region":
        metadata["region_macro"] = resolution.region_macro
        metadata["contains_all"] = resolution.contains_all
        metadata["entidad_like_candidates"] = resolution.contains_all

    elif resolution.resolution_type == "sena_unit_exact":
        metadata["official_names"] = resolution.official_names

    return metadata
