"""workspace.py — puente puro Radar → Workspace (R2).

Traduce una oportunidad detectada (``OpportunityDetail``, R1) a las tres cosas
que el escritorio necesita para guardarla: qué tipo de item es, qué identidad
tiene y qué snapshot se conserva. NO escribe SQLite y no sabe que SQLite existe:
la escritura tiene dueño (``app.core.workspace_store``) y meter lógica de radar
allí acoplaría el contenedor transversal del producto a una sola de sus fuentes.

Dos reglas que definen lo que puede afirmar:

1. **La identidad sale del dataset y la clave del resultado, nunca del título.**
   Dos procesos distintos pueden llamarse igual y el mismo proceso puede cambiar
   de nombre entre corridas; ``dataset_id`` + ``result_key`` es lo que R1 ya
   estableció como identidad estable. Si falta cualquiera de los dos, la
   oportunidad NO es guardable: es preferible no ofrecer el botón que guardar
   algo que después no se puede volver a reconocer.

2. **El snapshot solo copia hechos ya observados.** Es una copia de lo que el
   evento congeló en su ``ts``, más el instante en que el usuario decidió
   conservarlo. No se consulta SECOP, no se deriva un estado operativo, no se
   inventa una fecha de publicación ni una hora de cierre. Un campo que el
   evento no trae simplemente no aparece en el snapshot.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.core.radar.detail import OpportunityDetail
from app.core.soql_builder import SoQLBuilder

__all__ = [
    "WORKSPACE_SNAPSHOT_SCHEMA_VERSION",
    "build_workspace_snapshot",
    "is_saveable",
    "workspace_item_type",
    "workspace_reference_id",
    "workspace_title",
]

# Versión del formato de ``metadata_json`` que escribe este puente. Sube cuando
# el significado de una clave cambie, no cuando se añada una nueva: un lector
# viejo sobre una clave nueva simplemente no la ve.
WORKSPACE_SNAPSHOT_SCHEMA_VERSION = 1

# Los dos datasets del producto, cada uno con el ``item_type`` del schema real
# de ``workspace_items``. Los strings son los desplegados (español); el plan
# histórico proponía nombres en inglés que la tabla nunca llevó.
_DATASET_ITEM_TYPES = {
    SoQLBuilder.PROCESOS_DATASET: "proceso",
    SoQLBuilder.CONTRATOS_DATASET: "contrato",
}


def workspace_item_type(detail: OpportunityDetail | None) -> str | None:
    """``"proceso"`` / ``"contrato"``, o ``None`` si el dataset no se reconoce.

    Sin default a procesos: clasificar una fila ajena como proceso le afirmaría
    al escritorio un cierre de ofertas que nadie observó.
    """
    if detail is None:
        return None
    return _DATASET_ITEM_TYPES.get(detail.dataset_id or "")


def workspace_reference_id(detail: OpportunityDetail | None) -> str | None:
    """Identidad natural namespaced: ``"<dataset_id>/<result_key>"``.

    El dataset va delante porque las claves de procesos y contratos viven en
    espacios distintos y podrían coincidir por casualidad.
    """
    if detail is None:
        return None
    dataset_id = detail.dataset_id
    result_key = (detail.result_key or "").strip()
    if not dataset_id or dataset_id not in _DATASET_ITEM_TYPES or not result_key:
        return None
    return f"{dataset_id}/{result_key}"


def is_saveable(detail: OpportunityDetail | None) -> bool:
    """¿Se le puede ofrecer Guardar a esta oportunidad? Falla cerrado."""
    return (
        workspace_item_type(detail) is not None
        and workspace_reference_id(detail) is not None
    )


def workspace_title(detail: OpportunityDetail) -> str:
    """Texto para la columna ``title``, que es NOT NULL.

    Es display, nunca identidad. Un evento sin nombre cae a la referencia
    observada y, si tampoco la hay, a la clave del resultado: el usuario tiene
    que poder reconocer la tarjeta, y esos tres son hechos del evento.
    """
    for candidate in (detail.title, detail.reference, detail.result_key):
        text = (candidate or "").strip()
        if text:
            return text
    return "(sin nombre en los datos observados)"


def build_workspace_snapshot(
    event: Mapping[str, Any] | None,
    detail: OpportunityDetail,
    saved_at: str,
) -> dict[str, Any]:
    """Snapshot self-contained del item guardado.

    Self-contained significa que "Mi escritorio" puede renderizar la tarjeta sin
    releer ``radar_events``, ``radar_seen_results`` ni SECOP. Por eso se copia y
    no se referencia: si el radar se borra, lo guardado sigue siendo legible.

    ``event`` solo aporta el ``radar_id`` —el único dato de procedencia que la
    ficha de R1 no proyecta—; todo lo demás sale de ``detail``, que ya aplicó las
    reglas de lectura de R1 (día civil sin hora, dinero formateado, URL solo si
    SECOP la entregó). Las claves descriptivas ausentes se omiten en vez de
    escribirse en ``null``: quien lea el snapshot no tiene que distinguir entre
    "no observado" y "observado vacío", porque son el mismo hecho.
    """
    snapshot: dict[str, Any] = {
        "schema_version": WORKSPACE_SNAPSHOT_SCHEMA_VERSION,
        "source": "radar",
        "dataset_id": detail.dataset_id,
        "result_key": detail.result_key,
        "radar_event_id": detail.event_id,
        "saved_at": saved_at,
    }
    radar_id = _text(event.get("radar_id")) if isinstance(event, Mapping) else None
    observed = {
        "radar_id": radar_id,
        "radar_name": detail.radar_name,
        "observed_at": detail.observed_at,
        "title": detail.title,
        "object_excerpt": detail.object_excerpt,
        "entity": detail.entity,
        "location": detail.location,
        "value_display": detail.value_display,
        "modality": detail.modality,
        "source_status": detail.source_status,
        # El día civil que R1 leyó de la columna calendar_date. NO se guarda la
        # relación con hoy (``faltan N días``): eso caduca al día siguiente y
        # convertiría el snapshot en una afirmación falsa sobre el futuro.
        "closing_date": detail.closing.display,
        "reference": detail.reference,
        "secop_url": detail.secop_url,
        "contractor": detail.contractor,
        "signature_date": detail.signature_date,
    }
    snapshot.update({k: v for k, v in observed.items() if v})
    return snapshot


def _text(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None
