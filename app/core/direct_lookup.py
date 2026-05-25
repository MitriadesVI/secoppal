"""direct_lookup.py — Detección y resolución de lookups directos por ID, URL o referencia de entidad.

Feature A1: Búsqueda por número/URL de proceso + referencias internas de entidad.
Bypass determinístico del pipeline normal cuando el input del usuario contiene
exactamente un identificador reconocible de SECOP II.

Dos formatos, ambos ciudadanos de primera clase:
  1. Número electrónico: CO1.NTC.X, CO1.REQ.X, CO1.PCCNTR.X → equality
  2. Referencia interna: CD-08-2026-5534, SAMC-JCT-006, LP-2026-001 → LIKE
"""
from __future__ import annotations

import re
import logging
from typing import Literal
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Mapping prefijo → dataset para single round-trip.
PREFIX_DATASET: dict[str, str] = {
    "NTC": "p6dx-8zbt",
    "REQ": "p6dx-8zbt",
    "BDOS": "p6dx-8zbt",
    "CONS": "p6dx-8zbt",
    "PPROC": "p6dx-8zbt",
    "AAPPM": "p6dx-8zbt",
    "PCCNTR": "jbjy-vk9h",
}

# Regex para ID de SECOP II: CO1.PREFIJO.NUMERO
_SECOP_ID_RE = re.compile(r"CO1\.([A-Z]{2,7})\.(\d+)")

# Dominios válidos para extraer noticeUID
_SECOP_URL_DOMAINS = {"community.secop.gov.co", "www.secop.gov.co"}

# Prefijos válidos para referencias internas de entidad.
# Cubre patrones del CORPUS_BACKLOG: LP, LIC, SAMC, MC, CMC, CD, IP, etc.
_ENTITY_REF_PREFIXES = (
    "LP", "LIC", "SAMC", "SA", "MC", "CMC", "MIC", "CD", "CDA",
    "IP", "IPC", "JCT", "MP", "MIP", "SI", "CM", "SDC", "SDA",
)

# Regex para referencias internas de entidad.
# Cubre: CD-08-2026-5534, SAMC-JCT-006, LP-2026-001, MC 005 2024, CMC-2026-019, IP-017-2026
_ENTITY_REF_RE = re.compile(
    r"\b(?P<prefix>"
    + "|".join(_ENTITY_REF_PREFIXES)
    + r")[-\s]?(?P<num1>\d{1,4}|[A-Z]{2,6})"
    r"(?:[-\s](?P<num2>(?:\d{1,4}|[A-Z]{2,6})))?"
    r"(?:[-\s](?P<year>\d{1,5}))?",
    re.IGNORECASE,
)

LookupKind = Literal["id", "url", "entity_ref"]


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def detect_direct_lookup(raw_query: str) -> tuple[str | None, LookupKind | None]:
    """Detect if raw_query contains exactly one SECOP identifier.

    Returns:
        (identifier, kind) where kind is "id", "url", or "entity_ref".
        Returns (None, None) if no match or multiple matches.

    Priority order when multiple formats detected:
        URL > SECOP ID > entity_ref
    """
    # 1. Find all SECOP IDs (CO1.XXX.YYY)
    id_matches = _SECOP_ID_RE.findall(raw_query)
    raw_ids = {f"CO1.{prefix}.{num}" for prefix, num in id_matches}

    # 2. Find all SECOP URLs and extract noticeUIDs
    url_ids = set(_extract_url_ids(raw_query))

    # 3. Find all entity references
    entity_refs = set(_extract_entity_refs(raw_query))

    # Deduplicate across categories
    url_only = url_ids - raw_ids
    raw_only = raw_ids - url_ids

    total_unique = len(raw_only) + len(url_ids) + len(entity_refs)

    if total_unique == 0:
        return None, None

    if total_unique >= 2:
        logger.info(
            "direct_lookup: %d unique identifiers found — deferring to normal pipeline",
            total_unique,
        )
        return None, None

    # Exactly one — priority: URL > ID > entity_ref
    if url_ids:
        return list(url_ids)[0], "url"
    if raw_only:
        return list(raw_only)[0], "id"
    return list(entity_refs)[0], "entity_ref"


# ---------------------------------------------------------------------------
# Dataset routing
# ---------------------------------------------------------------------------

def dataset_for_id(process_id: str) -> str | None:
    """Return the dataset_id for a given process_id based on its prefix."""
    match = _SECOP_ID_RE.match(process_id)
    if not match:
        return None
    prefix = match.group(1)
    return PREFIX_DATASET.get(prefix)


# ---------------------------------------------------------------------------
# Reference normalization
# ---------------------------------------------------------------------------

def normalize_entity_ref(ref: str) -> str:
    """Normalize an entity reference for LIKE search.

    CD 08 2026 5534 → CD-08-2026-5534
    SAMC JCT 006      → SAMC-JCT-006
    lp-2026-001        → LP-2026-001
    """
    ref = ref.strip().upper()
    # Collapse whitespace to single hyphens
    ref = re.sub(r"\s+", "-", ref)
    # Remove leading/trailing hyphens
    ref = ref.strip("-")
    return ref


# ---------------------------------------------------------------------------
# SoQL for entity references
# ---------------------------------------------------------------------------

def build_entity_ref_soql(dataset_id: str, normalized_ref: str, limit: int = 5) -> str:
    """Build a SoQL query searching by entity reference using LIKE.

    Searches both the normalized form AND the original (with spaces) form
    in case the dataset stores references in a different format.
    """
    from app.core.soql_builder import SoQLBuilder

    spec = SoQLBuilder.SPECS[dataset_id]
    id_field = "id_del_proceso" if dataset_id == "p6dx-8zbt" else "id_contrato"
    ref_field = spec.reference or "referencia_del_proceso"
    fields = ", ".join(spec.select_fields)

    esc = SoQLBuilder._escape(normalized_ref)

    return (
        f"SELECT {fields} "
        f"WHERE UPPER({ref_field}) LIKE UPPER('%{esc}%') "
        f"ORDER BY {spec.date} DESC "
        f"LIMIT {limit}"
    )


# ---------------------------------------------------------------------------
# URL parsing (internal)
# ---------------------------------------------------------------------------

def _extract_url_ids(text: str) -> list[str]:
    """Extract noticeUID values from all SECOP II URLs found in text."""
    url_pattern = re.compile(r"https?://\S+")
    urls = url_pattern.findall(text)

    ids: list[str] = []
    for url in urls:
        extracted = _parse_notice_uid(url)
        if extracted:
            ids.append(extracted)

    return ids


def _parse_notice_uid(url: str) -> str | None:
    """Extract noticeUID from a SECOP II URL."""
    try:
        parsed = urlparse(url)
    except Exception:
        return None

    if parsed.hostname not in _SECOP_URL_DOMAINS:
        return None

    params = parse_qs(parsed.query)
    uid_values = params.get("noticeUID", [])
    if not uid_values:
        return None

    uid = uid_values[0]
    if _SECOP_ID_RE.match(uid):
        return uid

    return None


# ---------------------------------------------------------------------------
# Entity reference extraction (internal)
# ---------------------------------------------------------------------------

def _extract_entity_refs(text: str) -> list[str]:
    """Extract entity reference patterns from text.

    Returns list of normalized references. A reference is only valid if:
    - It has at least a prefix AND a number (two segments minimum).
    - It's not just a standalone number.
    - It doesn't look like normal text (e.g., "MC" alone is too ambiguous).
    """
    matches = _ENTITY_REF_RE.finditer(text)
    refs: list[str] = []
    for m in matches:
        prefix = m.group("prefix")
        num1 = m.group("num1")
        num2 = m.group("num2")
        year = m.group("year")

        # Must have at least 2 components (prefix + num1)
        if not prefix or not num1:
            continue

        # Build the full match string
        raw = m.group(0)
        normalized = normalize_entity_ref(raw)

        # Validate: must have meaningful parts (not just "MC" or "LP" alone)
        parts = normalized.split("-")
        meaningful_parts = [p for p in parts if p]
        if len(meaningful_parts) < 2:
            continue

        refs.append(normalized)

    return refs
