"""
Entity Ecosystem Router — unifies ICBF + SENA resolution for SECOP query pipeline.

Pure router. No SECOP calls. No SoQL. No mutation.
Operates on raw user query strings.

Priority: ICBF first, then SENA (they don't overlap).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Literal

from app.core.icbf_resolver import resolve_icbf_reference, build_icbf_filter_metadata
from app.core.sena_resolver import resolve_sena_reference, build_sena_filter_metadata


# G7C.1: institutional acronym typo fixes
# Maps common misspellings/typos to canonical forms.
# Only exact word-level matches (case-insensitive) to avoid false positives.
INSTITUTIONAL_ACRONYM_FIXES: dict[str, str] = {
    "icfb": "icbf",
    "i c b f": "icbf",
}

# Tokens to scrub from params["objeto"] when an ecosystem is detected.
# These are institutional vocabulary, not contractual topics.
_ICBF_OBJETO_SCRUB: frozenset[str] = frozenset({
    "icbf", "icfb", "bienestar", "familiar",
    "instituto", "colombiano", "de",
})

_SENA_OBJETO_SCRUB: frozenset[str] = frozenset({
    "sena", "servicio", "nacional", "aprendizaje",
})

_OBJETO_SCRUB_BY_FAMILY: dict[str, frozenset[str]] = {
    "ICBF": _ICBF_OBJETO_SCRUB,
    "SENA": _SENA_OBJETO_SCRUB,
}


def normalize_institutional_acronyms(raw_query: str) -> str:
    """Replace known institutional typos with canonical forms.

    Only replaces whole-word matches (case-insensitive) to avoid
    corrupting unrelated words (e.g. "icfb" inside "bicfbs" stays).
    """
    result = raw_query
    for typo, canonical in INSTITUTIONAL_ACRONYM_FIXES.items():
        # Word boundary replacement: match as standalone token
        pattern = re.compile(re.escape(typo), re.IGNORECASE)
        result = pattern.sub(canonical, result)
    return result


def scrub_institutional_tokens_from_objeto(objeto_terms: list, family: str) -> list:
    """Remove institutional vocabulary tokens from objeto list.

    When an ecosystem entity (ICBF, SENA) is detected, tokens like
    "icbf", "sena", "bienestar" should not be treated as contractual topics.
    """
    if not objeto_terms:
        return objeto_terms
    tokens_to_remove = _OBJETO_SCRUB_BY_FAMILY.get(family, frozenset())
    scrubbed = []
    for term in objeto_terms:
        if isinstance(term, str):
            if term.lower() not in tokens_to_remove:
                scrubbed.append(term)
        elif isinstance(term, list):
            # OR group: filter inside
            filtered_group = [
                t for t in term
                if isinstance(t, str) and t.lower() not in tokens_to_remove
            ]
            if filtered_group:
                scrubbed.append(filtered_group)
        else:
            scrubbed.append(term)
    return scrubbed


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
    # G7C.1: normalize institutional typos before resolution
    normalized_query = normalize_institutional_acronyms(raw_query)

    # 1. Try ICBF
    icbf_result = resolve_icbf_reference(normalized_query)
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
    sena_result = resolve_sena_reference(normalized_query)
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
