"""builder.py — construcción y guardado de un radar desde params resueltos.

Camino único compartido por el comando de chat ("guardar esta búsqueda como
Radar X") y el formulario "➕ Crear radar" de Streamlit. Centraliza:

- ``sanitize_for_save`` — quita campos derivados/contextuales que no replayean
  fielmente la búsqueda (sobre todo ``entidad_ecosystem``, que normal_flow
  inyecta pero nunca aplica a los resultados, los ``*_resolution`` de debug y
  órdenes conversacionales incompatibles con la ventana por recencia del radar).
- ``save_radar_from_params`` — serializa el QueryFrame limpio y crea el radar.
"""
from __future__ import annotations

import dataclasses
import json
from typing import TYPE_CHECKING

from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.query_frame import frame_from_params

if TYPE_CHECKING:
    from app.core.radar.store import RadarStore

# Campos que NO deben persistirse en el QueryFrame de un radar porque son
# derivados/contextuales y, al replayearse, divergen de la búsqueda original.
# Caso crítico: ``entidad_ecosystem`` (normal_flow lo inyecta en
# resolved_params y reconstruye el SoQL, pero NO re-ejecuta la query, así que
# los resultados mostrados nunca lo aplicaron → guardarlo daría ≈0 resultados).
RADAR_SAVE_DENY_KEYS = frozenset({
    "entidad_ecosystem",
    "ordering_signal",
    "offset", "_display_offset", "_shown_total",
})


def sanitize_for_save(params: dict) -> dict:
    """Limpia los params antes de serializarlos como QueryFrame del radar."""
    return {
        k: v for k, v in params.items()
        if k not in RADAR_SAVE_DENY_KEYS and not k.endswith("_resolution")
    }


def has_scope_or_topic(params: dict) -> bool:
    """True si los params tienen alcance o tema (guard anti-WHERE 1=1)."""
    return any(
        params.get(k) and params.get(k) not in (None, [], "", {}, False)
        for k in SCOPE_TOPIC_KEYS
    )


def save_radar_from_params(
    store: "RadarStore",
    *,
    owner: str,
    name: str,
    params: dict,
    channel: str = "web",
    frequency: str = "manual",
    created_from_trace_id: str | None = None,
) -> str:
    """Sanitiza, serializa el frame y crea el radar. Devuelve el radar_id.

    ``frequency`` (manual | diaria | semanal | cada_6h) queda sembrada para el
    scheduler push; en v1.0 (pull) no cambia el comportamiento.
    Puede lanzar ``RadarExistsError`` si ya existe ``(owner, name)``.
    """
    from app.core.contract_universe import stamp
    clean = stamp(sanitize_for_save(params))
    frame = frame_from_params(clean)
    frame_json = json.dumps(dataclasses.asdict(frame), ensure_ascii=False)
    return store.create_radar(
        name=name, owner=owner, query_frame_json=frame_json,
        dataset=clean.get("dataset"), channel=channel, frequency=frequency,
        created_from_trace_id=created_from_trace_id,
    )
