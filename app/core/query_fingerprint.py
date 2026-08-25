"""Huella estable de una consulta — Etapa 4 (y reutilizable por Etapa 8).

`sha256(canonical_json)`, NO `hash()`: el hash de Python varía entre procesos
(`PYTHONHASHSEED`), así que no sirve para comparar páginas/turnos de una conversación
que cruza procesos. Determinista: mismas claves WHERE-relevantes ⇒ misma huella.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

# Claves transitorias que NO definen la consulta (cursor/paginación/límites display).
_TRANSIENT = frozenset({
    "offset", "limit", "_display_offset", "_shown_total", "_page_fingerprint",
    "_temporal_replace_scope", "_temporal_error", "_temporal_clarification",
})

_EMPTY = (None, [], "", {}, False)


def _canonical(value: Any) -> Any:
    """Forma canónica: listas ordenadas por su repr, dicts por clave, recursivo."""
    if isinstance(value, dict):
        return {k: _canonical(value[k]) for k in sorted(value)}
    if isinstance(value, (list, tuple, set)):
        items = [_canonical(v) for v in value]
        # Orden estable sin depender del orden de inserción.
        return sorted(items, key=lambda x: json.dumps(x, sort_keys=True, default=str))
    return value


def query_fingerprint(params: dict, *, extra: dict | None = None) -> str:
    """Huella sha256 de los campos que definen la consulta.

    ``extra`` permite a la Etapa 8 inyectar versión de PhraseSpec / ranking / validador
    sin que este módulo conozca esas capas.
    """
    relevant = {
        k: v for k, v in (params or {}).items()
        if k not in _TRANSIENT and v not in _EMPTY
    }
    if extra:
        relevant["__extra__"] = {k: v for k, v in extra.items() if v not in _EMPTY}
    blob = json.dumps(_canonical(relevant), sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()
