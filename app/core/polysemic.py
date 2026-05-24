"""Polysemic topic resolution (POLYSEMIC-TOPIC-001).

When a query contains an ambiguous term (like "alojamiento"), this module
detects the term and checks for context markers to auto-resolve the sense.
If no context resolves, returns a clarification question for the user.

Catalog: app/data/polysemic_topics.json — curated by hand, not auto-generated.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_db_path = Path(__file__).parent.parent / "data" / "polysemic_topics.json"
_catalog: dict[str, Any] = {}

if _db_path.exists():
    with _db_path.open("r", encoding="utf-8") as f:
        _catalog = json.load(f)


@dataclass
class PolysemicResolution:
    """Result of resolving a polysemic term."""
    term: str
    resolved: bool = False
    auto_resolved: bool = False
    sense_id: str | None = None
    sense_label: str | None = None
    object_variants: list[str] | None = None
    clarification_hint: str | None = None
    matched_markers: list[str] = field(default_factory=list)


def detect_polysemic_terms(objeto_terms: list[str]) -> set[str]:
    """Return which terms in objeto are polysemic (present in catalog)."""
    if not _catalog:
        return set()
    return {t.lower() for t in objeto_terms if t.lower() in _catalog}


def resolve_polysemic(
    term: str,
    query_text: str = "",
    departamento: str = "",
) -> PolysemicResolution:
    """Try to resolve a polysemic term using context markers.

    Args:
        term: The polysemic term (e.g., "alojamiento")
        query_text: Full original query for context marker detection
        departamento: Resolved department for additional context

    Returns:
        PolysemicResolution with auto_resolved=True if context disambiguated,
        or clarification_hint if manual clarification needed.
    """
    term_lower = term.lower()
    if term_lower not in _catalog:
        return PolysemicResolution(term=term)

    entry = _catalog[term_lower]
    query_lower = query_text.lower()

    # Try auto-resolution by context markers
    best_sense: dict | None = None
    best_matches: list[str] = []
    best_count = 0

    for sense_id, sense in entry["senses"].items():
        markers = sense.get("context_markers", [])
        matches = [m for m in markers if m.lower() in query_lower]
        if len(matches) > best_count:
            best_count = len(matches)
            best_sense = sense
            best_matches = matches
            best_sense["_id"] = sense_id

    if best_sense and best_count >= 2:
        # Auto-resolved: at least 2 context markers matched
        sense_data = best_sense  # type narrowing
        return PolysemicResolution(
            term=term,
            resolved=True,
            auto_resolved=True,
            sense_id=best_sense["_id"],
            sense_label=best_sense["label"],
            object_variants=list(best_sense.get("object_variants", [term])),
            matched_markers=best_matches,
        )

    # Not auto-resolved — return clarification hint
    return PolysemicResolution(
        term=term,
        resolved=False,
        auto_resolved=False,
        clarification_hint=entry.get("hint", f"El término '{term}' tiene múltiples sentidos. ¿Cuál buscás?"),
    )


def build_clarification(resolution: PolysemicResolution) -> str | None:
    """Build user-facing clarification message for unresolved polysemic term."""
    if resolution.resolved:
        return None
    if not resolution.clarification_hint:
        return None
    return resolution.clarification_hint
