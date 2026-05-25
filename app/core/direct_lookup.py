"""direct_lookup.py — Detección y resolución de lookups directos por ID o URL de SECOP II.

Feature A1: Búsqueda por número/URL de proceso.
Bypass determinístico del pipeline normal (query_router → entity_resolver → soql_builder)
cuando el input del usuario contiene exactamente un ID de SECOP II o una URL de proceso.
"""
from __future__ import annotations

import re
import logging
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Mapping prefijo → dataset para single round-trip.
# Si el prefijo no está acá, se intenta fallback secuencial.
PREFIX_DATASET: dict[str, str] = {
    "NTC": "p6dx-8zbt",       # Notificaciones → procesos
    "REQ": "p6dx-8zbt",       # Requisiciones → procesos
    "BDOS": "p6dx-8zbt",      # Borradores → procesos
    "CONS": "p6dx-8zbt",      # Consultas → procesos
    "PPROC": "p6dx-8zbt",     # Pre-procesos → procesos
    "AAPPM": "p6dx-8zbt",     # Adendas → procesos
    "PCCNTR": "jbjy-vk9h",    # Contratos → contratos
}

# Regex para ID de SECOP II: CO1.PREFIJO.NUMERO
_SECOP_ID_RE = re.compile(r"CO1\.([A-Z]{2,7})\.(\d+)")

# Dominios válidos para extraer noticeUID
_SECOP_URL_DOMAINS = {"community.secop.gov.co", "www.secop.gov.co"}


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def detect_direct_lookup(raw_query: str) -> tuple[str | None, str | None]:
    """Detect if raw_query contains exactly one SECOP II ID or one SECOP II URL.

    Returns:
        (process_id, source) where source is "id" or "url".
        Returns (None, None) if no match or multiple matches.

    Detection is non-strict (re.search, not re.match) — the ID/URL can be
    embedded in natural text like "muéstrame CO1.NTC.123".

    Edge cases:
        - 0 IDs/URLs → (None, None) → pipeline normal
        - 2+ IDs/URLs → (None, None) → pipeline normal + mensaje
        - 1 ID   → (id, "id")
        - 1 URL  → (id, "url")
    """
    # Find all SECOP IDs in the text
    id_matches = _SECOP_ID_RE.findall(raw_query)
    raw_ids = {f"CO1.{prefix}.{num}" for prefix, num in id_matches}

    # Find all SECOP URLs in the text and extract their noticeUIDs
    url_ids = set(_extract_url_ids(raw_query))

    # Deduplicate: if a URL's noticeUID matches a raw ID, treat as URL
    raw_only_ids = raw_ids - url_ids  # IDs found in text but not in URL

    total_unique = len(raw_only_ids) + len(url_ids)

    if total_unique == 0:
        return None, None

    if total_unique >= 2:
        logger.info(
            "direct_lookup: %d unique IDs/URLs found — deferring to normal pipeline",
            total_unique,
        )
        return None, None

    # Exactly one unique ID — prefer URL extraction (URLs carry more intent)
    if url_ids:
        return list(url_ids)[0], "url"

    return list(raw_only_ids)[0], "id"


# ---------------------------------------------------------------------------
# Dataset routing
# ---------------------------------------------------------------------------

def dataset_for_id(process_id: str) -> str | None:
    """Return the dataset_id for a given process_id based on its prefix.

    Returns None if the prefix is unknown (caller should try sequential fallback).
    """
    match = _SECOP_ID_RE.match(process_id)
    if not match:
        return None
    prefix = match.group(1)
    return PREFIX_DATASET.get(prefix)


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------

def _extract_url_ids(text: str) -> list[str]:
    """Extract noticeUID values from all SECOP II URLs found in text."""
    # Find all URLs in the text
    url_pattern = re.compile(r"https?://\S+")
    urls = url_pattern.findall(text)

    ids: list[str] = []
    for url in urls:
        extracted = _parse_notice_uid(url)
        if extracted:
            ids.append(extracted)

    return ids


def _parse_notice_uid(url: str) -> str | None:
    """Extract noticeUID from a SECOP II URL. Returns None if not a valid SECOP URL."""
    try:
        parsed = urlparse(url)
    except Exception:
        return None

    # Validate domain
    if parsed.hostname not in _SECOP_URL_DOMAINS:
        return None

    # Extract noticeUID from query params
    params = parse_qs(parsed.query)
    uid_values = params.get("noticeUID", [])
    if not uid_values:
        return None

    uid = uid_values[0]
    # Validate it looks like a SECOP II ID
    if _SECOP_ID_RE.match(uid):
        return uid

    return None
