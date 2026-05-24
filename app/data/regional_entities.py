"""Regional entity resolution for multi-regional entities (ENTITY-MULTI-REGIONAL-001).

Entities like SENA, ICBF, DNP have regional offices across Colombia. When a user
searches for "SENA Bolívar", the generic alias may resolve to "SENA" (Bogotá),
but the correct match should be "SENA REGIONAL BOLÍVAR".

This module loads a curated catalog of regional entities and provides
resolve_regional() to map (entity, departamento) → specific regional entity.
"""
from __future__ import annotations

import json
from pathlib import Path

_regional_db: dict[str, dict[str, str]] = {}

_db_path = Path(__file__).parent / "regional_entities.json"
if _db_path.exists():
    with _db_path.open("r", encoding="utf-8") as f:
        _regional_db = json.load(f)


def resolve_regional(entidad_resolved: str, departamento_resolved: str) -> str | None:
    """Resolve a multi-regional entity to its department-specific office.

    Args:
        entidad_resolved: Official entity name from gazetteer (e.g., "SENA")
        departamento_resolved: Official department name (e.g., "Bolívar")

    Returns:
        Regional entity name if found, or None if not a multi-regional entity
        or no specific regional exists for this department.
    """
    # Try exact match on the central entity name
    # The catalog keys are the canonical central entity names
    for central_name, regionals in _regional_db.items():
        # Check if the resolved entity contains the central name
        # (e.g., "SENA" matches "SERVICIO NACIONAL DE APRENDIZAJE SENA")
        if central_name.upper() in entidad_resolved.upper():
            # Look for department match
            for dept_name, regional in regionals.items():
                if dept_name.upper() in departamento_resolved.upper():
                    return regional
            # If no specific department match, return None (keep central entity)
            return None

    return None
