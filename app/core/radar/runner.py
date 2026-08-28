"""runner.py — ejecuta un radar y registra la corrida.

``run_radar`` reconstruye los params desde el QueryFrame guardado, fuerza la
ventana de seguimiento (los 100 resultados más recientes), ejecuta la búsqueda
sin contaminar conversación, valida/rankea por la ruta canónica, clasifica
contra la última revisión, persiste lo visto, registra la corrida con auditoría
de outcome/reason y devuelve el digest asesor.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.phrase_ranking import validate_rank_and_dedupe
from app.core.query_frame import QueryFrame, params_from_frame
from app.core.radar import lease
from app.core.radar.diff import RadarDiff, classify
from app.core.radar.digest import build_digest
from app.core.radar.follows import fetch_current_rows_by_keys
from app.core.radar.keys import extract_event_metadata, radar_result_key
from app.core.radar.store import (
    EventRecord,
    FailureRecord,
    Radar,
    RadarStore,
    RunRecord,
    SeenRecord,
    new_run_id,
)
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.helpers import _is_timeout_exception, _query_first_then_count

if TYPE_CHECKING:
    from app.core.workflow.deps import WorkflowDeps

# Ventana de seguimiento: cuántos resultados recientes revisa cada corrida.
WINDOW_LIMIT = 100
# Barrido por clave: máximo de vistos fuera de ventana que se refrescan por corrida.
MAX_SEEN_REFRESH = 500
TRANSIENT_BACKOFF_MINUTES = 30
PERMANENT_BACKOFF_HOURS = 24
PERMANENT_FAILURE_PAUSE_THRESHOLD = 3

RADAR_OK = "radar_ok"
RADAR_EMPTY = "radar_empty"
RADAR_INVALID_FRAME = "radar_invalid_frame"
RADAR_GUARD_BLOCKED = "radar_guard_blocked"
RADAR_TIMEOUT = "radar_timeout"
RADAR_TRANSIENT_ERROR = "radar_transient_error"
RADAR_PERMANENT_ERROR = "radar_permanent_error"

RUN_KIND_BASELINE = "baseline"
RUN_KIND_REVIEW = "review"
RUN_TRIGGERS = frozenset({"create", "manual", "scheduler"})

COVERAGE_COMPLETE = "COMPLETE"
COVERAGE_PARTIAL_DISCOVERY = "PARTIAL_DISCOVERY"
COVERAGE_PARTIAL_MAINTENANCE = "PARTIAL_MAINTENANCE"
COVERAGE_UNKNOWN = "UNKNOWN"

# Estados de control: no son corridas. No se registran en radar_runs, no tocan
# failure_count/backoff/seen/eventos y no consultan Socrata. Quedan fuera de la
# taxonomía de outcomes de corrida (P0D) a propósito.
RADAR_ALREADY_RUNNING = "radar_already_running"
RADAR_LEASE_LOST = "radar_lease_lost"
STATUS_SKIPPED = "skipped"

# El radar fue borrado entre que el llamador lo resolvió y que se intentó
# tomar el lease (carrera legítima con delete_radar). Es un estado propio,
# no un caso de "already_running": no hay ningún dueño con quien competir.
RADAR_NOT_FOUND = "radar_not_found"
STATUS_NOT_FOUND = "not_found"

logger = logging.getLogger(__name__)


@dataclass
class RadarRunResult:
    run_id: str
    radar_id: str
    status: str  # "ok" | "error" | "skipped" | "not_found"
    diff: RadarDiff | None
    digest: dict
    total: int
    execution_ms: int
    outcome: str
    reason: str | None = None
    message: str | None = None
    error: str | None = None
    run_kind: str | None = None
    baseline_count: int | None = None
    run_trigger: str | None = None
    coverage_status: str | None = None
    coverage_reason: str | None = None


@dataclass(frozen=True)
class _RadarExecution:
    results: list[dict]
    total: int
    soql: str
    dataset_id: str
    approximate: bool


@dataclass(frozen=True)
class _RadarFailure:
    outcome: str
    reason: str
    message: str
    permanent: bool
    next_run_after: str | None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _after(**delta) -> str:
    return (datetime.now(timezone.utc) + timedelta(**delta)).isoformat()


def _error_digest(
    radar_name: str,
    message: str,
    outcome: str,
    reason: str,
    icon: str = "⚠️",
    *,
    run_kind: str | None = None,
    run_trigger: str | None = None,
) -> dict:
    return {
        "text": (
            f"🔔 Radar {radar_name}\n\n"
            "No puedo confirmar el estado completo de este Radar en esta revisión.\n\n"
            f"{icon} {message}"
        ),
        "has_novelty": False,
        "risk_flags": [],
        "new_count": 0,
        "changed_count": 0,
        "disappeared_count": 0,
        "window_churn_count": 0,
        "seen_refresh_checked": 0,
        "atypical_keys": [],
        "outcome": outcome,
        "reason": reason,
        "run_kind": run_kind,
        "baseline_count": 0,
        "run_trigger": run_trigger,
        "coverage_status": COVERAGE_UNKNOWN,
        "coverage_reason": "run_did_not_complete",
    }


def _normalize_run_trigger(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("run_trigger debe ser create, manual o scheduler")
    normalized = value.strip().lower()
    if normalized not in RUN_TRIGGERS:
        raise ValueError(f"run_trigger inválido: {value!r}")
    return normalized


def _coverage_for_success(
    execution: _RadarExecution, refresh_meta: dict,
) -> tuple[str, str]:
    """Deriva solo la cobertura demostrable por el runtime TOP-N actual."""
    if execution.approximate:
        return COVERAGE_PARTIAL_DISCOVERY, "approximate_total"
    if execution.total > WINDOW_LIMIT:
        return COVERAGE_PARTIAL_DISCOVERY, "universe_exceeds_window"
    if refresh_meta.get("seen_refresh_limited"):
        return COVERAGE_PARTIAL_MAINTENANCE, "seen_refresh_limited"
    if int(refresh_meta.get("seen_refresh_errors", 0) or 0) > 0:
        return COVERAGE_PARTIAL_MAINTENANCE, "seen_refresh_errors"
    return COVERAGE_COMPLETE, "bounded_run_complete"


def _validate_frame_json(radar: Radar) -> _RadarFailure | None:
    try:
        frame = json.loads(radar.query_frame_json)
    except (json.JSONDecodeError, TypeError):
        return _RadarFailure(
            outcome=RADAR_INVALID_FRAME,
            reason="invalid_query_frame_json",
            message="El radar tiene un QueryFrame inválido; requiere revisión.",
            permanent=True,
            next_run_after=_after(hours=PERMANENT_BACKOFF_HOURS),
        )
    if not isinstance(frame, dict):
        return _RadarFailure(
            outcome=RADAR_INVALID_FRAME,
            reason="invalid_query_frame_shape",
            message="El radar tiene un QueryFrame inválido; requiere revisión.",
            permanent=True,
            next_run_after=_after(hours=PERMANENT_BACKOFF_HOURS),
        )
    return None


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


def _execute_radar_window(deps: "WorkflowDeps", params: dict) -> _RadarExecution | _RadarFailure:
    has_scope_or_topic = any(
        params.get(k) and params.get(k) not in (None, [], "", {}, False)
        for k in SCOPE_TOPIC_KEYS
    )
    if not has_scope_or_topic:
        return _RadarFailure(
            outcome=RADAR_GUARD_BLOCKED,
            reason="guard_no_scope_or_topic",
            message="El radar no tiene alcance o tema suficiente para ejecutarse.",
            permanent=True,
            next_run_after=_after(hours=PERMANENT_BACKOFF_HOURS),
        )

    ds_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
    try:
        soql = deps.soql_builder.build(ds_id, params)
        count_soql = deps.soql_builder.build_count(ds_id, params)
    except Exception as exc:
        return _RadarFailure(
            outcome=RADAR_PERMANENT_ERROR,
            reason="soql_build_error",
            message=f"No se pudo construir la consulta del radar: {str(exc)[:160]}",
            permanent=True,
            next_run_after=_after(hours=PERMANENT_BACKOFF_HOURS),
        )

    try:
        total, results, _timings, approximate = _query_first_then_count(
            deps.secop_client,
            ds_id,
            count_soql,
            soql,
            limit=int(params.get("limit", WINDOW_LIMIT) or WINDOW_LIMIT),
        )
    except Exception as exc:
        if _is_timeout_exception(exc):
            return _RadarFailure(
                outcome=RADAR_TIMEOUT,
                reason="socrata_timeout",
                message="La consulta agotó tiempo; se reintentará después.",
                permanent=False,
                next_run_after=_after(minutes=TRANSIENT_BACKOFF_MINUTES),
            )
        return _RadarFailure(
            outcome=RADAR_TRANSIENT_ERROR,
            reason="socrata_query_error",
            message=f"No se pudo consultar SECOP temporalmente: {str(exc)[:160]}",
            permanent=False,
            next_run_after=_after(minutes=TRANSIENT_BACKOFF_MINUTES),
        )

    return _RadarExecution(
        results=results,
        total=total,
        soql=soql,
        dataset_id=ds_id,
        approximate=approximate,
    )


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


def _build_events(dataset_name, dataset_id, diff, deduped, total, is_first_run) -> list[EventRecord]:
    """Construye los eventos del feed de esta corrida. Función pura.

    No recibe store, conexión, radar ni run_id: ``EventRecord`` no lleva
    identidad de radar/corrida/dueño a propósito (la deriva la transacción de
    materialización, no este caller). Aporta solo lo propio del movimiento.
    Se ejecuta antes de abrir la transacción, para que dentro de ella solo
    queden escrituras.

    ``dataset_id`` llega como argumento y no se re-deriva de ``dataset_name``:
    quien ya lo resolvió fue la ejecución, y volver a resolverlo aquí crearía una
    segunda respuesta posible a "¿qué dataset es este?".
    """
    ts = _now()
    events: list[EventRecord] = []
    if is_first_run:
        events.append(EventRecord(
            kind="baseline", ts=ts, dataset=dataset_name,
            title=f"Empezó a vigilar · {len(deduped)} resultados en seguimiento",
            value=str(total),
        ))
    else:
        for it in diff.new:
            events.append(EventRecord(
                kind="new", ts=ts, dataset=dataset_name, result_key=it.key,
                title=it.fields.get("title"), entity=it.fields.get("entity"),
                value=it.fields.get("value"), status=it.fields.get("status"),
                urlproceso=it.fields.get("urlproceso"),
                referencia=it.fields.get("referencia"),
                **extract_event_metadata(it.row, dataset_id),
            ))
        for it in diff.changed_state:
            events.append(EventRecord(
                kind="changed_state", ts=ts, dataset=dataset_name, result_key=it.key,
                title=it.fields.get("title"), entity=it.fields.get("entity"),
                value=it.fields.get("value"), status=it.fields.get("status"),
                prev_status=it.prev_status,
                urlproceso=it.fields.get("urlproceso"),
                referencia=it.fields.get("referencia"),
                **extract_event_metadata(it.row, dataset_id),
            ))
        for it in diff.changed_value:
            events.append(EventRecord(
                kind="changed_value", ts=ts, dataset=dataset_name, result_key=it.key,
                title=it.fields.get("title"), entity=it.fields.get("entity"),
                value=it.fields.get("value"), status=it.fields.get("status"),
                prev_value=it.prev_value,
                urlproceso=it.fields.get("urlproceso"),
                referencia=it.fields.get("referencia"),
                **extract_event_metadata(it.row, dataset_id),
            ))
    return events


def _lease_lost_result(radar: Radar) -> RadarRunResult:
    """El lease venció durante la corrida y otro ejecutor lo tomó.

    Abortamos sin persistir: los datos que traemos ya no nos autorizan a
    escribir, y el dueño actual está produciendo los suyos.
    """
    logger.warning("run_radar: lease perdido durante la corrida radar id=%s", radar.id)
    return _control_result(
        radar, status=STATUS_SKIPPED, outcome=RADAR_LEASE_LOST, reason="lease_lost",
        message="La revisión perdió su turno de ejecución; no se guardaron cambios.",
    )


def _already_running_result(radar: Radar) -> RadarRunResult:
    logger.info("run_radar: radar id=%s ya está ejecutándose; se omite", radar.id)
    return _control_result(
        radar, status=STATUS_SKIPPED, outcome=RADAR_ALREADY_RUNNING, reason="already_running",
        message="El radar ya se está ejecutando; se omite esta revisión.",
    )


def _not_found_result(radar: Radar) -> RadarRunResult:
    """El radar fue borrado entre la resolución del llamador y el intento de lease.

    Distinto de already_running: aquí no hay ningún dueño con quien competir,
    el registro simplemente ya no existe.
    """
    return _control_result(
        radar, status=STATUS_NOT_FOUND, outcome=RADAR_NOT_FOUND, reason=RADAR_NOT_FOUND,
        message="El radar ya no existe.",
    )


def _materialize_failure(
    store: RadarStore,
    radar: Radar,
    token: str,
    failure: _RadarFailure,
    *,
    started_at: str,
    execution_ms: int,
    params_json: str | None,
    run_kind: str,
    run_trigger: str,
) -> RadarRunResult:
    """Registra un fallo real: corrida y backoff en una sola transacción.

    Un run sin su backoff (o al revés) deja el radar mintiendo sobre su propio
    estado, así que ambos comparten transacción y fence de lease.
    """
    digest = _error_digest(
        radar.name, failure.message, failure.outcome, failure.reason,
        run_kind=run_kind, run_trigger=run_trigger,
    )
    run = RunRecord(
        id=new_run_id(), started_at=started_at, status="error",
        error=failure.message, digest_json=json.dumps(digest, ensure_ascii=False),
        params_json=params_json, execution_ms=execution_ms,
        outcome=failure.outcome, reason=failure.reason,
        run_kind=run_kind, baseline_count=0, run_trigger=run_trigger,
        coverage_status=COVERAGE_UNKNOWN,
        coverage_reason="run_did_not_complete",
    )
    materialized = store.materialize_failed_run(
        radar_id=radar.id, token=token, run=run,
        failure=FailureRecord(
            reason=failure.reason, message=failure.message,
            next_run_after=failure.next_run_after, permanent=failure.permanent,
            pause_after=PERMANENT_FAILURE_PAUSE_THRESHOLD,
        ),
    )
    if materialized.lease_lost:
        return _lease_lost_result(radar)
    return RadarRunResult(
        run_id=run.id, radar_id=radar.id, status="error", diff=None,
        digest=digest, total=0, execution_ms=execution_ms,
        outcome=failure.outcome, reason=failure.reason,
        message=failure.message, error=failure.message,
        run_kind=run_kind, baseline_count=0, run_trigger=run_trigger,
        coverage_status=COVERAGE_UNKNOWN,
        coverage_reason="run_did_not_complete",
    )


def _control_result(
    radar: Radar, *, status: str, outcome: str, reason: str, message: str,
) -> RadarRunResult:
    """Resultado de control: no hubo corrida, no hay nada que auditar."""
    digest = _error_digest(radar.name, message, outcome, reason, icon="⏳")
    return RadarRunResult(
        run_id="", radar_id=radar.id, status=status, diff=None,
        digest=digest, total=0, execution_ms=0, outcome=outcome,
        reason=reason, message=message,
    )


def run_radar(
    deps: "WorkflowDeps",
    store: RadarStore,
    radar: Radar,
    *,
    run_trigger: str = "manual",
) -> RadarRunResult:
    """Ejecuta un radar bajo exclusión mutua interproceso.

    Es la única puerta de entrada (scheduler, chat y UI pasan por aquí), así
    que el lease se toma aquí: ningún ejecutor —ni uno manual con force— llega
    a Socrata sin ser el dueño. El lease se libera pase lo que pase.
    """
    run_trigger = _normalize_run_trigger(run_trigger)
    claim = store.try_acquire_run_lease(radar.id)
    if not claim.acquired:
        if claim.status == lease.LEASE_RADAR_NOT_FOUND:
            return _not_found_result(radar)
        return _already_running_result(radar)
    try:
        return _run_radar_locked(
            deps, store, radar, claim.token, run_trigger=run_trigger,
        )
    finally:
        store.release_run_lease(radar.id, claim.token)


def _run_radar_locked(
    deps: "WorkflowDeps",
    store: RadarStore,
    radar: Radar,
    token: str,
    *,
    run_trigger: str,
) -> RadarRunResult:
    """Ejecuta un radar: búsqueda → diff → persistencia → digest."""
    started_at = _now()
    is_first_run = not store.has_successful_run(radar.id)
    run_kind = RUN_KIND_BASELINE if is_first_run else RUN_KIND_REVIEW
    frame_failure = _validate_frame_json(radar)
    if frame_failure is not None:
        return _materialize_failure(
            store, radar, token, frame_failure,
            started_at=started_at, execution_ms=0,
            params_json=radar.query_frame_json,
            run_kind=run_kind, run_trigger=run_trigger,
        )

    params = _apply_window_policy(params_from_radar(radar))

    t0 = time.perf_counter()
    execution = _execute_radar_window(deps, params)
    execution_ms = int(round((time.perf_counter() - t0) * 1000))

    if isinstance(execution, _RadarFailure):
        # Un fallo tras la red también persiste (run + failure/backoff): si
        # perdimos el lease, ese registro le corresponde al dueño actual.
        return _materialize_failure(
            store, radar, token, execution,
            started_at=started_at, execution_ms=execution_ms,
            params_json=json.dumps(params, ensure_ascii=False),
            run_kind=run_kind, run_trigger=run_trigger,
        )

    ds_id = execution.dataset_id
    deduped = _validated_rank_and_dedupe(execution.results, params, ds_id)

    seen_map = store.get_seen(radar.id)  # estado PREVIO (antes del upsert)
    current_keys = _current_window_keys(deduped, ds_id)
    refreshed_rows, refresh_meta = _refresh_seen_rows(deps, seen_map, current_keys, ds_id)
    refreshed = _validated_rank_and_dedupe(refreshed_rows, params, ds_id)

    # Última llamada remota hecha. A partir de aquí no hay red: solo se arma en
    # memoria lo que se va a escribir, y la transacción final lo vuelca.
    current_for_diff = deduped + refreshed
    diff = classify(current_for_diff, seen_map, ds_id, current_window_keys=current_keys)
    outcome = RADAR_EMPTY if not execution.results and not refreshed and not diff.has_novelty else RADAR_OK
    reason = "no_results" if outcome == RADAR_EMPTY else "ok"
    coverage_status, coverage_reason = _coverage_for_success(execution, refresh_meta)
    baseline_count = len(deduped) if is_first_run else 0

    run_meta = {
        "total": execution.total,
        "approximate": execution.approximate,
        "window_limit": WINDOW_LIMIT,
        "run_kind": run_kind,
        "baseline_count": baseline_count,
        "run_trigger": run_trigger,
        "coverage_status": coverage_status,
        "coverage_reason": coverage_reason,
        **refresh_meta,
    }
    digest = build_digest(radar.name, diff, run_meta, seen_map)
    digest["outcome"] = outcome
    digest["reason"] = reason

    approx_total = execution.total if execution.approximate else None
    run_id = new_run_id()
    run = RunRecord(
        id=run_id, started_at=started_at, status="ok",
        total_count=execution.total, new_count=diff.new_count,
        changed_count=diff.changed_count,
        digest_json=json.dumps(digest, ensure_ascii=False),
        soql=execution.soql, params_json=json.dumps(params, ensure_ascii=False),
        dataset_id=ds_id, approx_total=approx_total, execution_ms=execution_ms,
        outcome=outcome, reason=reason,
        run_kind=run_kind, baseline_count=baseline_count,
        run_trigger=run_trigger, coverage_status=coverage_status,
        coverage_reason=coverage_reason,
    )

    # Lo visto en esta corrida (todo lo presente: nuevos + cambios + iguales).
    dataset_name = params.get("dataset") or radar.dataset
    seen_records = [
        SeenRecord(
            result_key=item.key, dataset=dataset_name, fields=item.fields,
            snapshot_hash=item.snapshot_hash,
            last_snapshot_json=json.dumps(item.row, ensure_ascii=False),
        )
        for item in (diff.new + diff.changed_state + diff.changed_value + diff.unchanged)
    ]

    # Eventos del feed. En la PRIMERA corrida se establece la línea base
    # silenciosa (un solo evento "empezó a vigilar") para no inundar el feed con
    # N "nuevos" de backfill; a partir de ahí, un evento por movimiento real.
    events = _build_events(dataset_name, ds_id, diff, deduped, execution.total, is_first_run)

    # Todo lo anterior fue construcción en memoria. Recién aquí se escribe, y
    # se escribe entero o nada: vistos sin sus eventos perderían la novedad.
    materialized = store.materialize_successful_run(
        radar_id=radar.id, token=token, run=run, seen=seen_records, events=events,
    )
    if materialized.lease_lost:
        return _lease_lost_result(radar)

    return RadarRunResult(
        run_id=run_id, radar_id=radar.id, status="ok", diff=diff,
        digest=digest, total=execution.total, execution_ms=execution_ms,
        outcome=outcome, reason=reason, message=None,
        run_kind=run_kind, baseline_count=baseline_count,
        run_trigger=run_trigger, coverage_status=coverage_status,
        coverage_reason=coverage_reason,
    )
