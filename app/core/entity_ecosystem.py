"""Entity ecosystem resolution (ENTITY-ECOSYSTEM-001).

When a user searches for "contratos de Bogotá" without specifying an entity,
the ecosystem resolver expands the search to include all dependent entities
in that jurisdiction (Alcaldía, Secretarías, Institutos, Empresas).

Catalog: app/data/entity_ecosystem.json
"""
from __future__ import annotations

import json
from pathlib import Path

_db_path = Path(__file__).parent.parent / "data" / "entity_ecosystem.json"
_ecosystem: dict[str, list[str]] = {}

if _db_path.exists():
    with _db_path.open("r", encoding="utf-8") as f:
        _ecosystem = json.load(f)


def get_ecosystem_entities(ciudad: str = "", departamento: str = "") -> list[str] | None:
    """Return the list of ecosystem entities for a city or department.

    Returns None if the city/department is not in the ecosystem catalog.
    """
    # Try exact match on city first
    for key, entities in _ecosystem.items():
        if key.lower() == ciudad.lower() or key.lower() == departamento.lower():
            return list(entities)

    # Try substring match (e.g., "Cundinamarca" in "Cundinamarca")
    search = (ciudad or departamento).lower()
    if search:
        for key, entities in _ecosystem.items():
            if search in key.lower() or key.lower() in search:
                return list(entities)

    return None


def should_expand_ecosystem(params: dict) -> bool:
    """Check if ecosystem expansion should be applied.

    Conditions: ciudad or departamento is set, AND no explicit entity was specified.
    """
    has_place = bool(params.get("ciudad") or params.get("departamento_resolved"))
    has_entity = bool(params.get("entidad_resolved") or params.get("entidad_like") or params.get("entidad"))
    return has_place and not has_entity
