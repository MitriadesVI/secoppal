"""
Entity Ecosystem Router — unifies ICBF + SENA resolution for SECOP query pipeline.

Pure router. No SECOP calls. No SoQL. No mutation.
Operates on raw user query strings.

Priority: ICBF first, then SENA (they don't overlap).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from app.core.icbf_resolver import resolve_icbf_reference, build_icbf_filter_metadata
from app.core.sena_resolver import resolve_sena_reference, build_sena_filter_metadata


@dataclass(frozen=True)
class EntityEcosystemMatch:
    family: Literal["ICBF", "SENA"]
    resolution_type: str
    label: str
    department: str | None = None
    region_macro: str | None = None
    official_names: list[str] = field(default_factory=list)
    contains_all: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    warning: str | None = None
    confidence: Literal["high", "medium", "low"] = "high"


def resolve_entity_ecosystem(raw_query: str) -> EntityEcosystemMatch | None:
    """
    Detect ICBF or SENA entity ecosystems in a user query.

    Returns None if no ecosystem entity is mentioned.
    Priority: ICBF first, then SENA.
    """
    # 1. Try ICBF
    icbf_result = resolve_icbf_reference(raw_query)
    if icbf_result is not None:
        metadata = build_icbf_filter_metadata(icbf_result)
        return EntityEcosystemMatch(
            family="ICBF",
            resolution_type=icbf_result.resolution_type,
            label=icbf_result.label,
            department=icbf_result.department,
            region_macro=icbf_result.region_macro,
            official_names=icbf_result.official_names,
            contains_all=[],
            metadata=metadata,
            warning=metadata.get("warning"),
            confidence=icbf_result.confidence,
        )

    # 2. Try SENA
    sena_result = resolve_sena_reference(raw_query)
    if sena_result is not None:
        metadata = build_sena_filter_metadata(sena_result)
        return EntityEcosystemMatch(
            family="SENA",
            resolution_type=sena_result.resolution_type,
            label=sena_result.label,
            department=sena_result.department,
            region_macro=sena_result.region_macro,
            official_names=sena_result.official_names,
            contains_all=sena_result.contains_all,
            metadata=metadata,
            warning=sena_result.warning,
            confidence=sena_result.confidence,
        )

    return None


def build_entity_ecosystem_filter(match: EntityEcosystemMatch) -> dict:
    """
    Build normalized filter metadata from an ecosystem match.

    Returns a dict that can be merged into query params for downstream SoQLBuilder.
    """
    filter_meta: dict = {
        "entity_ecosystem": match.family,
        "entity_ecosystem_label": match.label,
        "entity_ecosystem_resolution_type": match.resolution_type,
        "entity_ecosystem_confidence": match.confidence,
    }

    if match.department is not None:
        filter_meta["entity_ecosystem_department"] = match.department

    if match.region_macro is not None:
        filter_meta["entity_ecosystem_region_macro"] = match.region_macro

    if match.contains_all:
        filter_meta["entidad_contains_all"] = match.contains_all

    if match.official_names:
        filter_meta["entidad_in_candidates"] = match.official_names

    if match.warning:
        filter_meta["entity_ecosystem_warning"] = match.warning

    return filter_meta
