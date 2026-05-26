"""
ICBF Regional Resolver — pure entity ecosystem for ICBF.

Three resolution modes:
1. icbf_national: global ICBF (sede nacional + all regionales)
2. icbf_regional: specific department/regional
3. icbf_macro_region: ICBF across a macro-region (e.g. costa atlántica)

No SECOP calls. No SoQL. No side effects.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from app.data.departamentos import DEPARTAMENTOS
from app.utils.money import normalize_text

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_ICBF_CATALOG_PATH = Path(__file__).parent.parent / "data" / "icbf_regionals.json"


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

ResolutionType = Literal["icbf_national", "icbf_regional", "icbf_macro_region", "none"]


@dataclass(frozen=True)
class ICBFResolution:
    resolution_type: ResolutionType
    label: str
    department: str | None = None
    region_macro: str | None = None
    official_names: list[str] = field(default_factory=list)
    aliases_matched: list[str] = field(default_factory=list)
    confidence: Literal["high", "medium", "low"] = "low"


# ---------------------------------------------------------------------------
# Load catalog
# ---------------------------------------------------------------------------

_ICBF_CATALOG: dict | None = None


def _load_catalog() -> dict:
    global _ICBF_CATALOG
    if _ICBF_CATALOG is None:
        _ICBF_CATALOG = json.loads(_ICBF_CATALOG_PATH.read_text(encoding="utf-8"))
    assert _ICBF_CATALOG is not None
    return _ICBF_CATALOG


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_alias_index(catalog: dict) -> dict[str, str]:
    """Build normalized alias → official_name index for all ICBF regionals."""
    index: dict[str, str] = {}
    # Sede nacional
    for alias in catalog["sede_nacional"]["aliases"]:
        key = normalize_text(alias)
        if key not in index:  # first wins (sede nacional priority)
            index[key] = catalog["sede_nacional"]["official_name"]
    # Regionales
    for regional in catalog["regionales"]:
        for alias in regional["aliases"]:
            key = normalize_text(alias)
            if key not in index:
                index[key] = regional["official_name"]
    return index


def _build_dept_index(catalog: dict) -> dict[str, list[dict]]:
    """Build normalized department → [regional entries] index."""
    index: dict[str, list[dict]] = {}
    for regional in catalog["regionales"]:
        dept_key = normalize_text(regional["departamento"])
        if dept_key not in index:
            index[dept_key] = []
        index[dept_key].append(regional)
    return index


def _is_icbf_mention(normalized: str) -> bool:
    """Does the query mention ICBF or its synonyms?"""
    triggers = ("icbf", "bienestar familiar", "instituto colombiano de bienestar familiar")
    return any(t in normalized for t in triggers)


def _is_macro_region_phrase(normalized: str, region_keys: dict[str, str]) -> str | None:
    """Detect macro region phrases (e.g. 'costa atlantica', 'caribe')."""
    for key, canonical in region_keys.items():
        if key in normalized:
            return canonical
    return None


def _find_department(normalized: str, catalog: dict) -> str | None:
    """Find a department mention in the query from our regional list."""
    dept_index = _build_dept_index(catalog)
    for dept_normalized in sorted(dept_index.keys(), key=len, reverse=True):
        if dept_normalized in normalized:
            return dept_index[dept_normalized][0]["departamento"]
    # Try broader DEPARTAMENTOS lookup
    for key in sorted(DEPARTAMENTOS.keys(), key=len, reverse=True):
        if key in normalized:
            return DEPARTAMENTOS[key]
    return None


def _find_macro_region(normalized: str, catalog: dict) -> tuple[str | None, list[str] | None]:
    """Check if query has macro region keywords."""
    region_keys: dict[str, str] = {}
    for region_name, depts in catalog.get("_macro_regions", {}).items():
        key = normalize_text(region_name.replace("_", " "))
        region_keys[key] = region_name

    # Also add normalized versions of common synonyms
    for raw_name in catalog.get("_macro_regions", {}):
        # e.g. "costa atlantica" should also trigger for "caribe"
        region_keys[normalize_text(raw_name.replace("_", " "))] = raw_name

    macro_region = _is_macro_region_phrase(normalized, region_keys)
    if macro_region:
        departments = catalog["_macro_regions"].get(macro_region)
        return macro_region, departments
    return None, None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_icbf_reference(raw_query: str) -> ICBFResolution | None:
    """
    Resolve an ICBF reference in a user query.

    Returns an ICBFResolution if ICBF is mentioned, None otherwise.
    Pure resolver — no side effects.
    """
    normalized = normalize_text(raw_query)
    if not _is_icbf_mention(normalized):
        return None

    catalog = _load_catalog()
    alias_index = _build_alias_index(catalog)
    matched_aliases: list[str] = []

    # Check for specific known aliases first (longest match priority)
    for alias_key in sorted(alias_index.keys(), key=len, reverse=True):
        if alias_key in normalized:
            matched_aliases.append(alias_key)

    # 1. Check for macro region
    macro_region, macro_depts = _find_macro_region(normalized, catalog)
    if macro_region and macro_depts:
        dept_index = _build_dept_index(catalog)
        official_names = []
        for dept in macro_depts:
            dept_key = normalize_text(dept)
            if dept_key in dept_index:
                for regional in dept_index[dept_key]:
                    official_names.append(regional["official_name"])
        return ICBFResolution(
            resolution_type="icbf_macro_region",
            label=f"ICBF en {macro_region}",
            region_macro=macro_region,
            official_names=official_names,
            aliases_matched=matched_aliases,
            confidence="high",
        )

    # 2. Check for specific department
    department = _find_department(normalized, catalog)
    if department:
        dept_key = normalize_text(department)
        dept_index = _build_dept_index(catalog)
        if dept_key in dept_index:
            regionals = dept_index[dept_key]
            official_names = [r["official_name"] for r in regionals]
            return ICBFResolution(
                resolution_type="icbf_regional",
                label=f"I.C.B.F. Regional {department}",
                department=department,
                official_names=official_names,
                aliases_matched=matched_aliases,
                confidence="high",
            )
        # Department might not be in ICBF catalog but was found in DEPARTAMENTOS
        return ICBFResolution(
            resolution_type="icbf_regional",
            label=f"I.C.B.F. Regional {department}",
            department=department,
            official_names=[],
            aliases_matched=matched_aliases,
            confidence="medium",
        )

    # 3. No department/macro — national/global
    # Collect sede nacional + all regionales
    official_names = [catalog["sede_nacional"]["official_name"]]
    for regional in catalog["regionales"]:
        official_names.append(regional["official_name"])

    return ICBFResolution(
        resolution_type="icbf_national",
        label="I.C.B.F. Nacional (sede + 32 regionales)",
        official_names=official_names,
        aliases_matched=matched_aliases,
        confidence="high",
    )


def build_icbf_filter_metadata(resolution: ICBFResolution) -> dict:
    """
    Build filter metadata from an ICBF resolution.

    Returns a dict that can be merged into query params.
    No SoQL — metadata only.
    """
    metadata: dict = {
        "entity_family": "ICBF",
        "resolution_type": resolution.resolution_type,
        "confidence": resolution.confidence,
    }

    if resolution.resolution_type == "icbf_national":
        metadata["official_names"] = resolution.official_names
        metadata["warning"] = (
            "ICBF se interpretó como ecosistema nacional: "
            "sede nacional + 32 regionales. "
            "Puedes filtrar por regional o macro-región."
        )

    elif resolution.resolution_type == "icbf_regional":
        metadata["official_names"] = resolution.official_names
        metadata["department"] = resolution.department
        metadata["entidad_like"] = resolution.official_names[0] if resolution.official_names else None

    elif resolution.resolution_type == "icbf_macro_region":
        metadata["official_names"] = resolution.official_names
        metadata["region_macro"] = resolution.region_macro

    return metadata
