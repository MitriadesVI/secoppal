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
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qs, urlparse

from app.core.retrieval_text import normalize_retrieval_text

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


@dataclass(frozen=True)
class DirectLookupRequest:
    """Lookup ya extraído; el handler no vuelve a analizar el texto."""

    reference_raw: str
    reference_normalized: str
    entity_qualifier_raw: str | None
    uid: str | None
    url: str | None
    match_mode: str

    @property
    def lookup_kind(self) -> LookupKind:
        if self.url:
            return "url"
        if self.uid:
            return "id"
        return "entity_ref"

    @property
    def lookup_value(self) -> str:
        return self.uid or self.reference_normalized


@dataclass(frozen=True)
class DirectLookupResolution:
    outcome: str
    outcome_reason: str
    candidates: list[dict]


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------

def detect_direct_lookup(
    raw_query: str,
) -> DirectLookupRequest | None:
    """Detect if raw_query contains exactly one SECOP identifier.

    Returns a ``DirectLookupRequest`` or ``None`` when no unique lookup exists.

    Priority order when multiple formats detected:
        URL > SECOP ID > entity_ref
    """
    request = parse_direct_lookup_request(raw_query)
    return request


def parse_direct_lookup_request(raw_query: str) -> DirectLookupRequest | None:
    """Extrae un único ID/URL/referencia y conserva su qualifier crudo."""
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
        return None

    if total_unique >= 2:
        logger.info(
            "direct_lookup: %d unique identifiers found — deferring to normal pipeline",
            total_unique,
        )
        return None

    # Exactly one — priority: URL > ID > entity_ref
    if url_ids:
        uid = next(iter(url_ids))
        url = next((u for u in re.findall(r"https?://\S+", raw_query) if _parse_notice_uid(u) == uid), None)
        return DirectLookupRequest(uid, uid, None, uid, url, "url")
    if raw_only:
        uid = next(iter(raw_only))
        return DirectLookupRequest(uid, uid, None, uid, None, "id")

    reference = next(iter(entity_refs))
    qualifier = _extract_entity_qualifier(raw_query, reference)
    mode = "exact_reference_entity" if qualifier else "exact_reference"
    return DirectLookupRequest(
        reference_raw=reference,
        reference_normalized=normalize_entity_ref(reference),
        entity_qualifier_raw=qualifier,
        uid=None,
        url=None,
        match_mode=mode,
    )


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


_LEADING_LOOKUP_FRAMING = re.compile(
    r"^(?:(?:busca(?:me)?|quiero\s+ver|muestrame|muéstrame|info(?:rmacion)?\s+de)"
    r"(?:\s+(?:el|la|un))?(?:\s+(?:proceso|contrato))?"
    r"|(?:(?:el|la|un)\s+)?(?:proceso|contrato))\s*$",
    re.IGNORECASE,
)
_TRAILING_LOOKUP_FRAMING = re.compile(
    r"^(?:que\s+pediste|por\s+favor|ahora)$",
    re.IGNORECASE,
)


def _extract_entity_qualifier(raw_query: str, normalized_ref: str) -> str | None:
    """Devuelve el texto fuera de la referencia, sin framing conversacional."""
    ref_match = next(
        (
            match
            for match in _ENTITY_REF_RE.finditer(raw_query)
            if normalize_entity_ref(match.group(0)) == normalized_ref
        ),
        None,
    )
    if ref_match is None:
        return None

    before = raw_query[: ref_match.start()].strip(" ,.;:-")
    after = raw_query[ref_match.end() :].strip(" ,.;:-")
    if _LEADING_LOOKUP_FRAMING.match(before):
        before = ""
    if _TRAILING_LOOKUP_FRAMING.match(after):
        after = ""
    after = re.sub(r"^de\s+", "", after, flags=re.IGNORECASE)
    after = re.sub(
        r"^(?:la|el)\s+(?=(?:alcaldia|alcaldía|gobernacion|gobernación|municipio|distrito)\b)",
        "",
        after,
        flags=re.IGNORECASE,
    )
    remaining = " ".join(part for part in (before, after) if part).strip(" ,.;:-")
    return remaining or None


def _reference_value(candidate: dict) -> str:
    return str(
        candidate.get("referencia_del_proceso")
        or candidate.get("referencia_del_contrato")
        or ""
    )


def _tolerant_reference(ref: str) -> tuple[str, ...]:
    parts = re.findall(r"[A-Z]+|\d+", normalize_entity_ref(ref))
    return tuple(str(int(part)) if part.isdigit() else part for part in parts)


def _candidate_uid(candidate: dict) -> str | None:
    url_uid = _parse_notice_uid(str(candidate.get("urlproceso") or ""))
    if url_uid:
        return url_uid
    for field in ("noticeUID", "notice_uid", "id_del_proceso", "id_contrato"):
        value = str(candidate.get(field) or "").strip()
        if value.upper().startswith("CO1."):
            return value.upper()
    return None


def _dedup_key(candidate: dict) -> tuple[str, ...]:
    uid = _candidate_uid(candidate)
    if uid:
        return ("uid", uid)
    dataset = str(candidate.get("_dataset_id") or candidate.get("dataset_id") or "")
    identifier = str(candidate.get("id_del_proceso") or candidate.get("id_contrato") or "")
    return ("dataset_id", dataset, identifier)


def deduplicate_lookup_candidates(candidates: list[dict]) -> list[dict]:
    """Conserva orden y elimina versiones repetidas por identidad canónica."""
    unique: dict[tuple[str, ...], dict] = {}
    for candidate in candidates:
        unique.setdefault(_dedup_key(candidate), candidate)
    return list(unique.values())


_QUALIFIER_NOISE = {
    "de", "del", "la", "el", "los", "las",
    "alcaldia", "municipio", "gobernacion", "departamento", "distrito",
}


def _qualifier_matches(candidate: dict, qualifier: str) -> bool:
    normalized = normalize_retrieval_text(qualifier)
    significant = set(normalized.split()) - _QUALIFIER_NOISE
    if not significant:
        significant = set(normalized.split())

    aliases = candidate.get("entity_aliases") or candidate.get("aliases") or []
    if isinstance(aliases, str):
        aliases = [aliases]
    fields = [
        candidate.get("entidad"),
        candidate.get("nombre_entidad"),
        candidate.get("ciudad_entidad"),
        candidate.get("ciudad"),
        candidate.get("departamento_entidad"),
        candidate.get("departamento"),
        *aliases,
    ]
    for value in fields:
        field = normalize_retrieval_text(str(value or ""))
        if not field:
            continue
        if normalized in field or field in normalized:
            return True
        if significant and significant.issubset(set(field.split())):
            return True
    return False


def resolve_reference_candidates(
    request: DirectLookupRequest,
    candidates: list[dict],
) -> DirectLookupResolution:
    """Deduplica y decide exact/ambiguous/approximate/not_found localmente."""
    exact: list[dict] = []
    tolerant: list[dict] = []
    wanted = request.reference_normalized
    wanted_tolerant = _tolerant_reference(wanted)
    for candidate in deduplicate_lookup_candidates(candidates):
        candidate_ref = normalize_entity_ref(_reference_value(candidate))
        if candidate_ref == wanted:
            exact.append(candidate)
        elif _tolerant_reference(candidate_ref) == wanted_tolerant:
            tolerant.append(candidate)

    if request.entity_qualifier_raw:
        exact_matches = [
            candidate
            for candidate in exact
            if _qualifier_matches(candidate, request.entity_qualifier_raw)
        ]
        tolerant_matches = [
            candidate
            for candidate in tolerant
            if _qualifier_matches(candidate, request.entity_qualifier_raw)
        ]
    else:
        exact_matches = exact
        tolerant_matches = tolerant

    reference_match = "exact" if exact_matches else "tolerant"
    matches = exact_matches or tolerant_matches

    if not matches:
        reason = "entity_qualifier_not_found" if (exact or tolerant) else "reference_not_found"
        return DirectLookupResolution("not_found", reason, [])

    annotated: list[dict] = []
    for candidate in matches:
        item = dict(candidate)
        item.update({
            "match_type": (
                "exact_reference_entity"
                if request.entity_qualifier_raw and reference_match == "exact"
                else "exact_reference"
                if reference_match == "exact"
                else "approximate_reference_entity"
                if request.entity_qualifier_raw
                else "approximate_reference"
            ),
            "reference_match": reference_match,
            "entity_match": "normalized_qualifier" if request.entity_qualifier_raw else "not_requested",
            "confidence": "high" if reference_match == "exact" else "medium",
        })
        annotated.append(item)

    if len(annotated) > 1:
        return DirectLookupResolution("ambiguous", "multiple_distinct_uids", annotated)
    if reference_match == "tolerant":
        return DirectLookupResolution("approximate", "tolerant_reference_match", annotated)
    return DirectLookupResolution("exact", "unique_reference_match", annotated)


# ---------------------------------------------------------------------------
# SoQL for entity references
# ---------------------------------------------------------------------------

def build_entity_ref_soql(
    dataset_id: str,
    normalized_ref: str,
    limit: int = 5,
    *,
    exact: bool = False,
) -> str:
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
    if exact:
        where = f"UPPER({ref_field}) = UPPER('{esc}')"
    else:
        tokens = re.findall(r"[A-Z]+|\d+", normalized_ref.upper())
        stripped = [str(int(token)) if token.isdigit() else token for token in tokens]
        variants = {
            normalized_ref,
            " ".join(tokens),
            "-".join(stripped),
            " ".join(stripped),
        }
        exact_like = f"UPPER({ref_field}) LIKE UPPER('%{esc}%')"
        prefix_checks = [
            f"UPPER({ref_field}) LIKE UPPER('{SoQLBuilder._escape(value)}%')"
            for value in sorted(variants)
            if value
        ]
        where = f"({exact_like} OR {' OR '.join(prefix_checks)})"

    return (
        f"SELECT {fields} "
        f"WHERE {where} "
        f"ORDER BY {spec.date} DESC, {id_field} ASC "
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
