"""runner.py — ejecuta un radar y registra la corrida.

``run_radar`` reconstruye los params desde el QueryFrame guardado, fuerza la
ventana de seguimiento (los 100 resultados más recientes), ejecuta la búsqueda
con el núcleo **puro** ``execute_resolved_params`` (sin contaminar conversación),
deduplica, clasifica contra la última revisión, persiste lo visto, registra la
corrida con auditoría y devuelve el digest asesor.
"""
from __future__ import annotations

import dataclasses
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.formatter import rank_and_dedupe
from app.core.query_frame import QueryFrame, params_from_frame
from app.core.radar.diff import RadarDiff, classify
from app.core.radar.digest import build_digest
from app.core.radar.store import Radar, RadarStore
from app.core.workflow.helpers import execute_resolved_params

if TYPE_CHECKING:
    from app.core.workflow.deps import WorkflowDeps

# Ventana de seguimiento: cuántos resultados recientes revisa cada corrida.
WINDOW_LIMIT = 100


@dataclass
class RadarRunResult:
    run_id: str
    radar_id: str
    status: str  # "ok" | "error"
    diff: RadarDiff | None
    digest: dict
    total: int
    execution_ms: int
    error: str | None = None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def params_from_radar(radar: Radar) -> dict:
    """Reconstruye los params de búsqueda desde el QueryFrame serializado.

    Filtra a los campos válidos de ``QueryFrame`` por robustez ante esquemas
    futuros, luego delega en ``params_from_frame`` (la fuente de verdad de la
    reconstrucción sin pérdida).
    """
    frame_dict = radar.frame_dict()
    valid = {f.name for f in dataclasses.fields(QueryFrame)}
    frame = QueryFrame(**{k: v for k, v in frame_dict.items() if k in valid})
    return params_from_frame(frame)


def _emit_events(store, radar, run_id, dataset_name, diff, deduped, total,
                 is_first_run) -> None:
    """Construye y persiste los eventos del feed para esta corrida."""
    ts = _now()
    base = {
        "radar_id": radar.id, "owner": radar.owner, "radar_name": radar.name,
        "run_id": run_id, "ts": ts, "dataset": dataset_name,
    }
    events: list[dict] = []
    if is_first_run:
        events.append({
            **base, "kind": "baseline",
            "title": f"Empezó a vigilar · {len(deduped)} resultados en seguimiento",
            "value": str(total),
        })
    else:
        for it in diff.new:
            events.append({**base, "kind": "new", "result_key": it.key,
                           "title": it.fields.get("title"),
                           "entity": it.fields.get("entity"),
                           "value": it.fields.get("value"),
                           "status": it.fields.get("status"),
                           "urlproceso": it.fields.get("urlproceso"),
                           "referencia": it.fields.get("referencia")})
        for it in diff.changed_state:
            events.append({**base, "kind": "changed_state", "result_key": it.key,
                           "title": it.fields.get("title"),
                           "entity": it.fields.get("entity"),
                           "value": it.fields.get("value"),
                           "status": it.fields.get("status"),
                           "prev_status": it.prev_status,
                           "urlproceso": it.fields.get("urlproceso"),
                           "referencia": it.fields.get("referencia")})
        for it in diff.changed_value:
            events.append({**base, "kind": "changed_value", "result_key": it.key,
                           "title": it.fields.get("title"),
                           "entity": it.fields.get("entity"),
                           "value": it.fields.get("value"),
                           "status": it.fields.get("status"),
                           "prev_value": it.prev_value,
                           "urlproceso": it.fields.get("urlproceso"),
                           "referencia": it.fields.get("referencia")})
    store.record_events(events)


def run_radar(
    deps: "WorkflowDeps",
    store: RadarStore,
    radar: Radar,
) -> RadarRunResult:
    """Ejecuta un radar: búsqueda → diff → persistencia → digest."""
    started_at = _now()
    params = params_from_radar(radar)
    # Ventana de seguimiento: forzar limit; el ordering por defecto del
    # SoQLBuilder ya es fecha DESC (recencia primero), así que basta el límite.
    params["limit"] = WINDOW_LIMIT

    t0 = time.perf_counter()
    try:
        execution = execute_resolved_params(deps, params, channel="web")
    except Exception as exc:  # defensivo: el helper ya captura, esto es cinturón
        execution = None
        exec_error = str(exc)
    else:
        exec_error = None
    execution_ms = int(round((time.perf_counter() - t0) * 1000))

    if execution is None:
        message = exec_error or (
            "No se pudo ejecutar el radar (sin alcance/tema o error de consulta)."
        )
        run_id = store.record_run(
            radar_id=radar.id, started_at=started_at, status="error",
            error=message, params_json=json.dumps(params, ensure_ascii=False),
            execution_ms=execution_ms,
        )
        store.record_error(radar.id, message)
        return RadarRunResult(
            run_id=run_id, radar_id=radar.id, status="error", diff=None,
            digest={"text": f"🔔 Radar {radar.name}\n\n⚠️ {message}",
                    "has_novelty": False, "risk_flags": [], "new_count": 0,
                    "changed_count": 0, "disappeared_count": 0, "atypical_keys": []},
            total=0, execution_ms=execution_ms, error=message,
        )

    ds_id = execution.dataset_id
    deduped = rank_and_dedupe(execution.results, ds_id, params)

    seen_map = store.get_seen(radar.id)  # estado PREVIO (antes del upsert)
    diff = classify(deduped, seen_map, ds_id)

    run_meta = {
        "total": execution.total,
        "approximate": execution.approximate,
        "window_limit": WINDOW_LIMIT,
    }
    digest = build_digest(radar.name, diff, run_meta, seen_map)

    approx_total = execution.total if execution.approximate else None
    run_id = store.record_run(
        radar_id=radar.id, started_at=started_at, status="ok",
        total_count=execution.total, new_count=diff.new_count,
        changed_count=diff.changed_count,
        digest_json=json.dumps(digest, ensure_ascii=False),
        soql=execution.soql, params_json=json.dumps(params, ensure_ascii=False),
        dataset_id=ds_id, approx_total=approx_total, execution_ms=execution_ms,
    )

    # Persistir lo visto en esta corrida (todo lo presente: nuevos + cambios + iguales).
    dataset_name = params.get("dataset") or radar.dataset
    is_first_run = not seen_map  # seen_map es el estado PREVIO al upsert
    for item in (diff.new + diff.changed_state + diff.changed_value + diff.unchanged):
        store.upsert_seen(
            radar_id=radar.id, result_key=item.key, dataset=dataset_name,
            fields=item.fields, snapshot_hash=item.snapshot_hash, run_id=run_id,
            last_snapshot_json=json.dumps(item.row, ensure_ascii=False),
        )

    # Emitir eventos al feed. En la PRIMERA corrida se establece la línea base
    # silenciosa (un solo evento "empezó a vigilar") para no inundar el feed con
    # N "nuevos" de backfill; a partir de ahí, un evento por movimiento real.
    _emit_events(store, radar, run_id, dataset_name, diff, deduped,
                 execution.total, is_first_run)

    store.update_last_run(radar.id)

    return RadarRunResult(
        run_id=run_id, radar_id=radar.id, status="ok", diff=diff,
        digest=digest, total=execution.total, execution_ms=execution_ms,
    )
