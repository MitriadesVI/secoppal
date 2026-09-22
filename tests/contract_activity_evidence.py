"""Shared parser/resolver witness for Contract Activity integration tests."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from app.core.entity_resolver import EntityResolver
from app.core.query_router import ParsedQuery, QueryRouter


_ROUTER = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
_TODAY = date(2026, 9, 20)


def parsed_turn(raw: str, *, today: date = _TODAY) -> ParsedQuery:
    parsed = _ROUTER.parse(raw, today=today)
    assert parsed.semantic_evidence is not None
    assert parsed.semantic_evidence.valid_for(raw)
    return parsed


def accredited_turn(raw: str, *, today: date = _TODAY) -> ParsedQuery:
    parsed = parsed_turn(raw, today=today)
    assert parsed.params.get("entidad_resolved"), raw
    return parsed
