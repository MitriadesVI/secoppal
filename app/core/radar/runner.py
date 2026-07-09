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
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from app.core.phrase_ranking import validate_rank_and_dedupe
from app.core.query_frame import QueryFrame, params_from_frame
from app.core.radar.diff import RadarDiff, classify
from app.core.radar.digest import build_digest
from app.core.radar.follows import fetch_current_rows_by_keys
from app.core.radar.keys import radar_result_key
from app.core.radar.store import Radar, RadarStore
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import execute_resolved_params

if TYPE_CHECKING:
    from app.core.workflow.deps import WorkflowDeps

# Ventana de seguimiento: cuántos resultados recientes revisa cada corrida.
WINDOW_LIMIT = 100
# Barrido por clave: máximo de vistos fuera de ventana que se refrescan por corrida.
MAX_SEEN_REFRESH = 500

logger = logging.getLogger(__name__)


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


def _apply_window_policy(params: dict) -> dict:
    """Fuerza la ventana de radar: 100 resultados por recencia."""
    clean = dict(params)
    clean.pop("ordering_signal", None)
    clean["limit"] = WINDOW_LIMIT
    return clean


def _dataset_id_from_seen(raw_dataset: str | None, fallback_dataset_id: str) -> str:
    """Normaliza dataset guardado como nombre conversacional o id Socrata."""
    if raw_dataset in SoQLBuilder.SPECS:
        return str(raw_dataset)
    if raw_dataset == "contratos":
        return SoQLBuilder.CONTRATOS_DATASET
    if raw_dataset == "procesos":
        return SoQLBuilder.PROCESOS_DATASET
    return fallback_dataset_id


def _current_window_keys(rows: list[dict], dataset_id: str) -> set[str]:
    return {
        key for row in rows
        if (key := radar_result_key(row, dataset_id)) is not None
    }


def _validated_rank_and_dedupe(rows: list[dict], params: dict, dataset_id: str) -> list[dict]:
    """Aplica la ruta canónica y excluye candidatos en cuarentena para Radar."""
    ranked = validate_rank_and_dedupe(rows, params, dataset_id)
    unvalidated_ids = {id(row) for row in ranked.unvalidated_candidates}
    if not unvalidated_ids:
        return ranked.ranked_results
    return [row for row in ranked.ranked_results if id(row) not in unvalidated_ids]


def _refresh_seen_rows(
    deps: "WorkflowDeps",
    seen_map: dict[str, dict],
    current_window_keys: set[str],
    fallback_dataset_id: str,
) -> tuple[list[dict], dict]:
    """Refresca por clave vistos fuera de la ventana reciente.

    Devuelve filas encontradas y metadatos de auditoría. Los errores de lookup
    son transitorios para el radar: se registran en meta y no confirman
    desapariciones.
    """
    candidates = [
        (key, rec) for key, rec in seen_map.items()
        if key not in current_window_keys
    ]
    candidates.sort(
        key=lambda item: (
            item[1].get("last_seen_at")
            or item[1].get("first_seen_at")
            or ""
        ),
        reverse=True,
    )
    limited = len(candidates) > MAX_SEEN_REFRESH
    selected = candidates[:MAX_SEEN_REFRESH]

    by_dataset: dict[str, list[str]] = {}
    for key, rec in selected:
        dataset_id = _dataset_id_from_seen(rec.get("dataset"), fallback_dataset_id)
        if dataset_id != fallback_dataset_id:
            continue
        by_dataset.setdefault(dataset_id, []).append(key)

    rows: list[dict] = []
    errors = 0
    requested_keys = {key for keys in by_dataset.values() for key in keys}
    for dataset_id, keys in by_dataset.items():
        try:
            fetched = fetch_current_rows_by_keys(deps, dataset_id, keys)
        except Exception:
            errors += len(keys)
            logger.exception(
                "run_radar: fallo refresh por clave dataset=%s keys=%s",
                dataset_id, len(keys),
            )
            continue
        allowed = set(keys)
        seen_fetched: set[str] = set()
        for row in fetched:
            key = radar_result_key(row, dataset_id)
            if key in allowed and key not in seen_fetched:
                rows.append(row)
                seen_fetched.add(key)

    meta = {
        "seen_refresh_candidates": len(candidates),
        "seen_refresh_checked": len(requested_keys),
        "seen_refresh_found": len(_current_window_keys(rows, fallback_dataset_id)),
        "seen_refresh_limited": limited,
        "seen_refresh_limit": MAX_SEEN_REFRESH,
        "seen_refresh_errors": errors,
    }
    return rows, meta


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
    params = _apply_window_policy(params_from_radar(radar))

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
                    "changed_count": 0, "disappeared_count": 0,
                    "window_churn_count": 0, "seen_refresh_checked": 0,
                    "atypical_keys": []},
            total=0, execution_ms=execution_ms, error=message,
        )

    ds_id = execution.dataset_id
    deduped = _validated_rank_and_dedupe(execution.results, params, ds_id)

    seen_map = store.get_seen(radar.id)  # estado PREVIO (antes del upsert)
    current_keys = _current_window_keys(deduped, ds_id)
    refreshed_rows, refresh_meta = _refresh_seen_rows(deps, seen_map, current_keys, ds_id)
    refreshed = _validated_rank_and_dedupe(refreshed_rows, params, ds_id)
    current_for_diff = deduped + refreshed
    diff = classify(current_for_diff, seen_map, ds_id, current_window_keys=current_keys)

    run_meta = {
        "total": execution.total,
        "approximate": execution.approximate,
        "window_limit": WINDOW_LIMIT,
        **refresh_meta,
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
