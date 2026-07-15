"""store.py — persistencia SQLite del Radar.

Cuatro tablas sobre la misma base que feedback (``data/secoppal.db``), creadas
de forma idempotente:

- ``radars``              — la definición del radar (QueryFrame serializado + metadatos).
- ``radar_runs``          — una fila por corrida, con auditoría (soql/params/execution_ms).
- ``radar_events``        — el feed de movimientos (nuevo/cambio de estado/valor).
- ``radar_seen_results``  — memoria de lo visto, para diffear corridas sucesivas.

El ``RadarStore`` abre conexiones cortas por operación vía ``connect_sqlite``
(WAL + FK + busy_timeout ya aplicados).

La *materialización* de una corrida es la excepción: ``run``, ``seen``,
``events`` y el estado del radar describen un mismo hecho y se escriben en una
sola transacción (ver ``materialize_successful_run``). Escribirlos por separado
—como hacía cada método autocommit— permite que un crash entre `seen` y
`events` marque resultados como vistos sin haber anunciado su novedad, que se
pierde para siempre. Los métodos sueltos siguen existiendo para escrituras que
sí son hechos independientes; todos comparten el mismo SQL vía los helpers
connection-aware de este módulo.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.radar import lease
from app.core.radar.lease import LeaseClaim


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


def new_run_id() -> str:
    """Id de corrida generable antes de persistir.

    Permite construir eventos y vistos —que referencian el run— en memoria y
    fuera de la transacción, sin que la construcción dependa del store.
    """
    return _new_id()


class RadarExistsError(ValueError):
    """Ya existe un radar con ese (owner, name)."""


# ---------------------------------------------------------------------------
# Dataclasses de lectura
# ---------------------------------------------------------------------------

@dataclass
class Radar:
    id: str
    name: str
    description: str
    query_frame_json: str
    dataset: str | None
    frequency: str
    channel: str
    enabled: int
    owner: str
    created_at: str
    last_run_at: str | None = None
    created_from_trace_id: str | None = None
    last_error_at: str | None = None
    last_error_message: str | None = None
    failure_count: int = 0
    last_failure_reason: str | None = None
    next_run_after: str | None = None
    needs_attention: int = 0
    permanent_failure_streak: int = 0
    run_lease_token: str | None = None
    run_lease_acquired_at: str | None = None
    run_lease_expires_at: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Radar":
        return cls(**{k: row[k] for k in row.keys()})

    def frame_dict(self) -> dict:
        try:
            return json.loads(self.query_frame_json) or {}
        except (json.JSONDecodeError, TypeError):
            return {}

    @property
    def is_enabled(self) -> bool:
        return bool(self.enabled)


@dataclass
class RadarRun:
    id: str
    radar_id: str
    started_at: str
    finished_at: str | None
    status: str
    total_count: int
    new_count: int
    changed_count: int
    digest_json: str | None
    error: str | None
    soql: str | None
    params_json: str | None
    dataset_id: str | None
    approx_total: int | None
    execution_ms: int | None
    outcome: str | None = None
    reason: str | None = None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "RadarRun":
        return cls(**{k: row[k] for k in row.keys()})

    def digest(self) -> dict:
        try:
            return json.loads(self.digest_json) if self.digest_json else {}
        except (json.JSONDecodeError, TypeError):
            return {}


# ---------------------------------------------------------------------------
# Dataclasses de materialización
#
# Son datos planos: describen *qué* escribir, no *cómo*. El caller los arma en
# memoria mientras todavía puede hacer red; la transacción solo los vuelca.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class RunRecord:
    """Una fila de ``radar_runs`` sin ``radar_id`` (lo aporta la operación)."""

    id: str
    started_at: str
    status: str
    total_count: int = 0
    new_count: int = 0
    changed_count: int = 0
    digest_json: str | None = None
    error: str | None = None
    soql: str | None = None
    params_json: str | None = None
    dataset_id: str | None = None
    approx_total: int | None = None
    execution_ms: int | None = None
    outcome: str | None = None
    reason: str | None = None
    finished_at: str | None = None


@dataclass(frozen=True)
class SeenRecord:
    """Un resultado a recordar en ``radar_seen_results``."""

    result_key: str
    dataset: str | None
    fields: dict = field(default_factory=dict)
    snapshot_hash: str = ""
    last_snapshot_json: str | None = None
    relevance_label: str | None = None


@dataclass(frozen=True)
class EventRecord:
    """Un movimiento del feed, sin identidad de radar/corrida/dueño.

    A propósito no lleva ``radar_id``, ``run_id``, ``owner`` ni ``radar_name``:
    esos cuatro campos los deriva la operación transaccional del lease ya
    validado y de la fila de ``radars`` leída tras el fence, nunca del caller.
    Si el caller pudiera declararlos, una llamada autorizada sobre el radar A
    podría insertar un evento que se hace pasar por B o por otro dueño.
    """

    kind: str
    ts: str | None = None
    dataset: str | None = None
    result_key: str | None = None
    title: str | None = None
    entity: str | None = None
    value: str | None = None
    status: str | None = None
    prev_status: str | None = None
    prev_value: str | None = None
    urlproceso: str | None = None
    referencia: str | None = None


@dataclass(frozen=True)
class FailureRecord:
    """El fallo operativo a imputar al radar junto con su corrida."""

    reason: str
    message: str
    next_run_after: str | None = None
    permanent: bool = False
    pause_after: int = 3


@dataclass(frozen=True)
class MaterializedRun:
    """Resultado de materializar. ``lease_lost`` no es error: es un estado."""

    run_id: str | None = None
    lease_lost: bool = False


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS radars (
    id                     TEXT PRIMARY KEY,
    name                   TEXT NOT NULL,
    description            TEXT NOT NULL DEFAULT '',
    query_frame_json       TEXT NOT NULL,
    dataset                TEXT,
    frequency              TEXT NOT NULL DEFAULT 'manual',
    channel                TEXT NOT NULL DEFAULT 'web',
    enabled                INTEGER NOT NULL DEFAULT 1,
    owner                  TEXT NOT NULL,
    created_at             TEXT NOT NULL,
    last_run_at            TEXT,
    created_from_trace_id  TEXT,
    last_error_at          TEXT,
    last_error_message     TEXT,
    failure_count          INTEGER NOT NULL DEFAULT 0,
    last_failure_reason    TEXT,
    next_run_after         TEXT,
    needs_attention        INTEGER NOT NULL DEFAULT 0,
    permanent_failure_streak INTEGER NOT NULL DEFAULT 0,
    run_lease_token        TEXT,
    run_lease_acquired_at  TEXT,
    run_lease_expires_at   TEXT,
    UNIQUE (owner, name)
);

CREATE TABLE IF NOT EXISTS radar_runs (
    id            TEXT PRIMARY KEY,
    radar_id      TEXT NOT NULL REFERENCES radars(id) ON DELETE CASCADE,
    started_at    TEXT NOT NULL,
    finished_at   TEXT,
    status        TEXT NOT NULL,
    total_count   INTEGER NOT NULL DEFAULT 0,
    new_count     INTEGER NOT NULL DEFAULT 0,
    changed_count INTEGER NOT NULL DEFAULT 0,
    digest_json   TEXT,
    error         TEXT,
    soql          TEXT,
    params_json   TEXT,
    dataset_id    TEXT,
    approx_total  INTEGER,
    execution_ms  INTEGER,
    outcome       TEXT,
    reason        TEXT
);
CREATE INDEX IF NOT EXISTS idx_radar_runs_radar ON radar_runs(radar_id, started_at DESC);

CREATE TABLE IF NOT EXISTS radar_events (
    id           TEXT PRIMARY KEY,
    radar_id     TEXT NOT NULL REFERENCES radars(id) ON DELETE CASCADE,
    owner        TEXT NOT NULL,
    radar_name   TEXT,
    run_id       TEXT,
    ts           TEXT NOT NULL,
    kind         TEXT NOT NULL,
    result_key   TEXT,
    dataset      TEXT,
    title        TEXT,
    entity       TEXT,
    value        TEXT,
    status       TEXT,
    prev_status  TEXT,
    prev_value   TEXT,
    urlproceso   TEXT,
    referencia   TEXT
);
CREATE INDEX IF NOT EXISTS idx_radar_events_owner ON radar_events(owner, ts DESC);

CREATE TABLE IF NOT EXISTS radar_seen_results (
    radar_id           TEXT NOT NULL REFERENCES radars(id) ON DELETE CASCADE,
    result_key         TEXT NOT NULL,
    dataset            TEXT,
    urlproceso         TEXT,
    referencia         TEXT,
    title              TEXT,
    entity             TEXT,
    value              TEXT,
    status             TEXT,
    contractor         TEXT,
    first_seen_at      TEXT NOT NULL,
    last_seen_at       TEXT NOT NULL,
    snapshot_hash      TEXT,
    last_snapshot_json TEXT,
    first_run_id       TEXT,
    last_run_id        TEXT,
    relevance_label    TEXT,
    PRIMARY KEY (radar_id, result_key)
);
"""


# ---------------------------------------------------------------------------
# SQL de escritura + helpers connection-aware
#
# Cada sentencia vive una sola vez. Los métodos autocommit y la materialización
# transaccional la comparten, así que no pueden divergir en silencio.
# ---------------------------------------------------------------------------

_INSERT_RUN_SQL = """
INSERT INTO radar_runs
   (id, radar_id, started_at, finished_at, status, total_count,
    new_count, changed_count, digest_json, error, soql, params_json,
    dataset_id, approx_total, execution_ms, outcome, reason)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

_INSERT_EVENT_SQL = """
INSERT INTO radar_events
   (id, radar_id, owner, radar_name, run_id, ts, kind, result_key,
    dataset, title, entity, value, status, prev_status, prev_value,
    urlproceso, referencia)
   VALUES (:id, :radar_id, :owner, :radar_name, :run_id, :ts, :kind,
    :result_key, :dataset, :title, :entity, :value, :status,
    :prev_status, :prev_value, :urlproceso, :referencia)
"""

_UPSERT_SEEN_SQL = """
INSERT INTO radar_seen_results
   (radar_id, result_key, dataset, urlproceso, referencia, title,
    entity, value, status, contractor, first_seen_at, last_seen_at,
    snapshot_hash, last_snapshot_json, first_run_id, last_run_id,
    relevance_label)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
   ON CONFLICT(radar_id, result_key) DO UPDATE SET
       last_seen_at       = excluded.last_seen_at,
       last_run_id        = excluded.last_run_id,
       snapshot_hash      = excluded.snapshot_hash,
       last_snapshot_json = excluded.last_snapshot_json,
       status             = excluded.status,
       value              = excluded.value,
       title              = excluded.title,
       entity             = excluded.entity,
       urlproceso         = excluded.urlproceso,
       referencia         = excluded.referencia,
       contractor         = excluded.contractor
"""

# Éxito: limpia error y ambos contadores. No toca enabled ni resucita un radar
# pausado a mano. needs_attention = 0 es la semántica *actual*; Preflight 3 la
# cambiará (una corrida OK no debe borrar la necesidad de revisión humana), y
# hay un test de caracterización que la congela hasta entonces.
_MARK_SUCCESS_SQL = """
UPDATE radars
   SET last_run_at = ?, last_error_at = NULL, last_error_message = NULL,
       failure_count = 0, last_failure_reason = NULL, next_run_after = NULL,
       needs_attention = 0, permanent_failure_streak = 0
 WHERE id = ?
"""

# Toda la decisión de pausa ocurre aquí dentro: sin SELECT previo, la racha se
# lee y se decide en la misma sentencia, atómica frente a corridas paralelas.
_RECORD_FAILURE_SQL = """
UPDATE radars
   SET last_error_at = ?, last_error_message = ?,
       failure_count = failure_count + 1,
       permanent_failure_streak = CASE
           WHEN ? THEN permanent_failure_streak + 1
           ELSE 0
       END,
       last_failure_reason = ?,
       next_run_after = ?,
       needs_attention = CASE
           WHEN ? AND permanent_failure_streak + 1 >= ?
           THEN 1 ELSE needs_attention
       END,
       enabled = CASE
           WHEN ? AND permanent_failure_streak + 1 >= ?
           THEN 0 ELSE enabled
       END
 WHERE id = ?
"""


def _insert_run(conn: sqlite3.Connection, radar_id: str, run: RunRecord) -> str:
    conn.execute(
        _INSERT_RUN_SQL,
        (run.id, radar_id, run.started_at, run.finished_at or _now(), run.status,
         run.total_count, run.new_count, run.changed_count, run.digest_json,
         run.error, run.soql, run.params_json, run.dataset_id, run.approx_total,
         run.execution_ms, run.outcome, run.reason),
    )
    return run.id


def _insert_event_dicts(conn: sqlite3.Connection, events: list[dict]) -> None:
    """Inserta eventos ya armados como dicts completos (todas las columnas).

    Usado por el registro genérico (``record_events``, consumido hoy por
    ``follows.py``): ahí el caller es dueño legítimo de todos los campos,
    incluida la identidad, porque no hay una transacción de por medio que
    pueda validarla. La materialización transaccional NO llama a esta
    función directamente — ver ``_insert_events`` abajo.
    """
    if not events:
        return
    conn.executemany(
        _INSERT_EVENT_SQL,
        [
            {
                "id": _new_id(),
                "radar_id": e.get("radar_id"),
                "owner": e.get("owner"),
                "radar_name": e.get("radar_name"),
                "run_id": e.get("run_id"),
                "ts": e.get("ts") or _now(),
                "kind": e.get("kind"),
                "result_key": e.get("result_key"),
                "dataset": e.get("dataset"),
                "title": e.get("title"),
                "entity": e.get("entity"),
                "value": e.get("value"),
                "status": e.get("status"),
                "prev_status": e.get("prev_status"),
                "prev_value": e.get("prev_value"),
                "urlproceso": e.get("urlproceso"),
                "referencia": e.get("referencia"),
            }
            for e in events
        ],
    )


def _insert_events(
    conn: sqlite3.Connection,
    *,
    radar_id: str,
    owner: str,
    radar_name: str | None,
    run_id: str,
    events: list[EventRecord],
) -> None:
    """Inserta los eventos de una corrida materializada.

    La identidad —``radar_id``, ``owner``, ``radar_name``, ``run_id``— viene
    siempre de estos argumentos, nunca de ``events``: son los únicos datos que
    la transacción ya verificó (``radar_id`` contra el lease renovado,
    ``owner``/``radar_name`` contra la fila de ``radars`` leída después del
    fence, ``run_id`` contra el run recién insertado en esta misma
    transacción). ``EventRecord`` ni siquiera expone esos campos.
    """
    if not events:
        return
    _insert_event_dicts(conn, [
        {
            "radar_id": radar_id, "owner": owner, "radar_name": radar_name,
            "run_id": run_id, "ts": e.ts, "kind": e.kind,
            "result_key": e.result_key, "dataset": e.dataset, "title": e.title,
            "entity": e.entity, "value": e.value, "status": e.status,
            "prev_status": e.prev_status, "prev_value": e.prev_value,
            "urlproceso": e.urlproceso, "referencia": e.referencia,
        }
        for e in events
    ])


def _upsert_seen(
    conn: sqlite3.Connection, radar_id: str, rec: SeenRecord, run_id: str, ts: str,
) -> None:
    f = rec.fields
    conn.execute(
        _UPSERT_SEEN_SQL,
        (radar_id, rec.result_key, rec.dataset, f.get("urlproceso", ""),
         f.get("referencia", ""), f.get("title", ""), f.get("entity", ""),
         f.get("value", ""), f.get("status", ""), f.get("contractor", ""),
         ts, ts, rec.snapshot_hash, rec.last_snapshot_json, run_id, run_id,
         rec.relevance_label),
    )


def _mark_success(conn: sqlite3.Connection, radar_id: str, ts: str) -> None:
    conn.execute(_MARK_SUCCESS_SQL, (ts, radar_id))


def _apply_failure(
    conn: sqlite3.Connection, radar_id: str, failure: FailureRecord, ts: str,
) -> None:
    permanent_flag = 1 if failure.permanent else 0
    conn.execute(
        _RECORD_FAILURE_SQL,
        (ts, failure.message,
         permanent_flag,
         failure.reason, failure.next_run_after,
         permanent_flag, failure.pause_after,
         permanent_flag, failure.pause_after,
         radar_id),
    )


# Puntos de inyección de fallo. Existen para probar que cada paso de la
# transacción revierte por completo; en runtime el hook siempre es None.
PHASE_AFTER_BEGIN = "after_begin"
PHASE_AFTER_LEASE_FENCE = "after_lease_fence"
PHASE_AFTER_RUN_INSERT = "after_run_insert"
PHASE_AFTER_SEEN_UPSERTS = "after_seen_upserts"
PHASE_AFTER_EVENT_INSERTS = "after_event_inserts"
PHASE_AFTER_RADAR_UPDATE = "after_radar_update"
PHASE_AFTER_FAILURE_UPDATE = "after_failure_update"
PHASE_BEFORE_COMMIT = "before_commit"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


class RadarStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def _ensure_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            self._ensure_columns(conn)
        finally:
            conn.close()

    def _ensure_columns(self, conn: sqlite3.Connection) -> None:
        """Migraciones aditivas para bases ya creadas."""
        radar_cols = {row["name"] for row in conn.execute("PRAGMA table_info(radars)")}
        for name, ddl in {
            "failure_count": "INTEGER NOT NULL DEFAULT 0",
            "last_failure_reason": "TEXT",
            "next_run_after": "TEXT",
            "needs_attention": "INTEGER NOT NULL DEFAULT 0",
            "permanent_failure_streak": "INTEGER NOT NULL DEFAULT 0",
            "run_lease_token": "TEXT",
            "run_lease_acquired_at": "TEXT",
            "run_lease_expires_at": "TEXT",
        }.items():
            if name not in radar_cols:
                conn.execute(f"ALTER TABLE radars ADD COLUMN {name} {ddl}")

        run_cols = {row["name"] for row in conn.execute("PRAGMA table_info(radar_runs)")}
        for name, ddl in {"outcome": "TEXT", "reason": "TEXT"}.items():
            if name not in run_cols:
                conn.execute(f"ALTER TABLE radar_runs ADD COLUMN {name} {ddl}")

    # -- radars ------------------------------------------------------------

    def create_radar(
        self,
        *,
        name: str,
        owner: str,
        query_frame_json: str,
        dataset: str | None = None,
        description: str = "",
        frequency: str = "manual",
        channel: str = "web",
        created_from_trace_id: str | None = None,
    ) -> str:
        radar_id = _new_id()
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO radars
                   (id, name, description, query_frame_json, dataset, frequency,
                    channel, enabled, owner, created_at, created_from_trace_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                (radar_id, name, description, query_frame_json, dataset, frequency,
                 channel, owner, _now(), created_from_trace_id),
            )
        except sqlite3.IntegrityError as exc:
            raise RadarExistsError(
                f"Ya existe un radar llamado '{name}'."
            ) from exc
        finally:
            conn.close()
        return radar_id

    def get_radar(self, radar_id: str) -> Radar | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radars WHERE id = ?", (radar_id,)
            ).fetchone()
        finally:
            conn.close()
        return Radar.from_row(row) if row else None

    def get_radar_by_name(self, owner: str, name: str) -> Radar | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radars WHERE owner = ? AND name = ? COLLATE NOCASE",
                (owner, name),
            ).fetchone()
        finally:
            conn.close()
        return Radar.from_row(row) if row else None

    def list_radars(self, owner: str) -> list[Radar]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radars WHERE owner = ? ORDER BY created_at DESC",
                (owner,),
            ).fetchall()
        finally:
            conn.close()
        return [Radar.from_row(r) for r in rows]

    def list_all_enabled_radars(self) -> list[Radar]:
        """Todos los radares activos de todos los dueños (para el scheduler)."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radars WHERE enabled = 1 "
                "AND (next_run_after IS NULL OR next_run_after <= ?) "
                "ORDER BY owner, created_at",
                (_now(),),
            ).fetchall()
        finally:
            conn.close()
        return [Radar.from_row(r) for r in rows]

    def set_enabled(self, radar_id: str, enabled: bool) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radars SET enabled = ? WHERE id = ?",
                (1 if enabled else 0, radar_id),
            )
        finally:
            conn.close()

    def update_last_run(self, radar_id: str, ts: str | None = None) -> None:
        conn = self._connect()
        try:
            _mark_success(conn, radar_id, ts or _now())
        finally:
            conn.close()

    def record_error(self, radar_id: str, message: str, ts: str | None = None) -> None:
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE radars SET last_error_at = ?, last_error_message = ? WHERE id = ?",
                (ts or _now(), message, radar_id),
            )
        finally:
            conn.close()

    def record_failure(
        self,
        radar_id: str,
        *,
        reason: str,
        message: str,
        next_run_after: str | None = None,
        permanent: bool = False,
        pause_after: int = 3,
        ts: str | None = None,
    ) -> int:
        """Registra fallo operativo y devuelve el nuevo ``failure_count``.

        ``failure_count`` cuenta fallos consecutivos de cualquier clase desde
        el último éxito; ``permanent_failure_streak`` cuenta solo la racha de
        fallos *permanentes* consecutivos (un transitorio la reinicia a 0). La
        pausa automática (``needs_attention``/``enabled=0``) depende
        exclusivamente de esa racha, nunca de ``failure_count`` general — de
        lo contrario, transitorio+transitorio+permanente pausaría el radar
        con un solo fallo permanente real.

        Toda la decisión ocurre en un único UPDATE (sin SELECT previo) para
        que sea atómica respecto de corridas concurrentes.
        """
        failure = FailureRecord(
            reason=reason, message=message, next_run_after=next_run_after,
            permanent=permanent, pause_after=pause_after,
        )
        conn = self._connect()
        try:
            _apply_failure(conn, radar_id, failure, ts or _now())
            row = conn.execute(
                "SELECT failure_count FROM radars WHERE id = ?", (radar_id,)
            ).fetchone()
        finally:
            conn.close()
        return int(row["failure_count"]) if row else 0

    # -- run lease (exclusión mutua interproceso) --------------------------
    #
    # Política aparte de enabled/needs_attention/next_run_after: esos deciden
    # *si toca* correr; el lease decide *quién* corre. Ver lease.py.

    def try_acquire_run_lease(
        self,
        radar_id: str,
        *,
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
    ) -> LeaseClaim:
        conn = self._connect()
        try:
            return lease.try_acquire(
                conn, radar_id, now=now, lease_duration=lease_duration
            )
        finally:
            conn.close()

    def renew_run_lease(
        self,
        radar_id: str,
        token: str,
        *,
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
    ) -> bool:
        conn = self._connect()
        try:
            return lease.renew(
                conn, radar_id, token, now=now, lease_duration=lease_duration
            )
        finally:
            conn.close()

    def release_run_lease(self, radar_id: str, token: str) -> bool:
        conn = self._connect()
        try:
            return lease.release(conn, radar_id, token)
        finally:
            conn.close()

    def is_run_lease_owned(
        self, radar_id: str, token: str, *, now: datetime | None = None
    ) -> bool:
        conn = self._connect()
        try:
            return lease.is_owned(conn, radar_id, token, now=now)
        finally:
            conn.close()

    def delete_radar(self, radar_id: str) -> None:
        conn = self._connect()
        try:
            conn.execute("DELETE FROM radars WHERE id = ?", (radar_id,))
        finally:
            conn.close()

    # -- runs --------------------------------------------------------------

    def record_run(
        self,
        *,
        radar_id: str,
        started_at: str,
        status: str,
        total_count: int = 0,
        new_count: int = 0,
        changed_count: int = 0,
        digest_json: str | None = None,
        error: str | None = None,
        soql: str | None = None,
        params_json: str | None = None,
        dataset_id: str | None = None,
        approx_total: int | None = None,
        execution_ms: int | None = None,
        outcome: str | None = None,
        reason: str | None = None,
        finished_at: str | None = None,
    ) -> str:
        run = RunRecord(
            id=_new_id(), started_at=started_at, status=status,
            total_count=total_count, new_count=new_count, changed_count=changed_count,
            digest_json=digest_json, error=error, soql=soql, params_json=params_json,
            dataset_id=dataset_id, approx_total=approx_total,
            execution_ms=execution_ms, outcome=outcome, reason=reason,
            finished_at=finished_at,
        )
        conn = self._connect()
        try:
            return _insert_run(conn, radar_id, run)
        finally:
            conn.close()

    def get_runs(self, radar_id: str, limit: int = 50) -> list[RadarRun]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_runs WHERE radar_id = ? "
                "ORDER BY started_at DESC LIMIT ?",
                (radar_id, limit),
            ).fetchall()
        finally:
            conn.close()
        return [RadarRun.from_row(r) for r in rows]

    # -- events (feed) -----------------------------------------------------

    def record_events(self, events: list[dict]) -> None:
        """Inserta movimientos en el feed. Cada dict trae las columnas de
        ``radar_events`` (sin ``id``, que se genera). No-op si la lista vacía.

        Genérico y de un solo hecho, sin transacción: lo usa ``follows.py``,
        donde el caller sí es dueño de la identidad de cada evento. La
        materialización de una corrida no pasa por aquí — ver
        ``materialize_successful_run``.
        """
        if not events:
            return
        conn = self._connect()
        try:
            _insert_event_dicts(conn, events)
        finally:
            conn.close()

    def get_feed(self, owner: str, limit: int = 60) -> list[dict]:
        """Feed cronológico de movimientos de todos los radares del owner."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_events WHERE owner = ? "
                "ORDER BY ts DESC, rowid DESC LIMIT ?",
                (owner, limit),
            ).fetchall()
        finally:
            conn.close()
        return [dict(r) for r in rows]

    # -- seen results ------------------------------------------------------

    def delete_seen(self, radar_id: str, result_key: str) -> None:
        """Borra un resultado seguido (para 'dejar de seguir')."""
        conn = self._connect()
        try:
            conn.execute(
                "DELETE FROM radar_seen_results WHERE radar_id = ? AND result_key = ?",
                (radar_id, result_key),
            )
        finally:
            conn.close()

    def get_seen(self, radar_id: str) -> dict[str, dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_seen_results WHERE radar_id = ?",
                (radar_id,),
            ).fetchall()
        finally:
            conn.close()
        return {r["result_key"]: dict(r) for r in rows}

    def upsert_seen(
        self,
        *,
        radar_id: str,
        result_key: str,
        dataset: str | None,
        fields: dict,
        snapshot_hash: str,
        run_id: str,
        last_snapshot_json: str | None = None,
        seen_at: str | None = None,
        relevance_label: str | None = None,
    ) -> None:
        """Inserta el resultado o actualiza su last_seen/snapshot.

        En INSERT fija first_seen_at/first_run_id; en UPDATE solo toca
        last_seen_at/last_run_id/snapshot y campos mutables (status/value).
        """
        rec = SeenRecord(
            result_key=result_key, dataset=dataset, fields=fields,
            snapshot_hash=snapshot_hash, last_snapshot_json=last_snapshot_json,
            relevance_label=relevance_label,
        )
        conn = self._connect()
        try:
            _upsert_seen(conn, radar_id, rec, run_id, seen_at or _now())
        finally:
            conn.close()

    # -- materialización transaccional de una corrida ----------------------
    #
    # El lease se revalida como primera sentencia DML de la transacción, no
    # antes de abrirla: entre una comprobación externa y las escrituras habría
    # una ventana en la que el lease vence y otro ejecutor toma el radar.
    # Dentro de BEGIN IMMEDIATE ya tenemos el lock de escritura, así que si el
    # fence pasa, nadie más puede escribir hasta el COMMIT.

    def materialize_successful_run(
        self,
        *,
        radar_id: str,
        token: str,
        run: RunRecord,
        seen: list[SeenRecord],
        events: list[EventRecord],
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> MaterializedRun:
        """Persiste corrida, vistos, eventos y estado del radar, o nada.

        ``seen`` y ``events`` describen el mismo hecho: si los vistos avanzan
        sin que los eventos lleguen al feed, la novedad se pierde para siempre
        (la próxima corrida ya no la ve como nueva). Por eso van juntos.

        ``events`` no trae ``radar_id``/``owner``/``radar_name``/``run_id``:
        ``EventRecord`` no los expone. Esta operación los deriva del lease ya
        renovado y de la fila de ``radars`` leída justo después, así que un
        caller autorizado sobre ``radar_id`` no puede declarar un evento como
        perteneciente a otro radar u otro dueño.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                if not lease.renew(
                    conn, radar_id, token, now=now, lease_duration=lease_duration
                ):
                    conn.execute("ROLLBACK")
                    return MaterializedRun(lease_lost=True)
                _fire(_fault_hook, PHASE_AFTER_LEASE_FENCE)

                radar_row = conn.execute(
                    "SELECT owner, name FROM radars WHERE id = ?", (radar_id,)
                ).fetchone()
                owner, radar_name = radar_row["owner"], radar_row["name"]

                _insert_run(conn, radar_id, run)
                _fire(_fault_hook, PHASE_AFTER_RUN_INSERT)

                ts = _now()
                for rec in seen:
                    _upsert_seen(conn, radar_id, rec, run.id, ts)
                _fire(_fault_hook, PHASE_AFTER_SEEN_UPSERTS)

                _insert_events(
                    conn, radar_id=radar_id, owner=owner, radar_name=radar_name,
                    run_id=run.id, events=events,
                )
                _fire(_fault_hook, PHASE_AFTER_EVENT_INSERTS)

                _mark_success(conn, radar_id, ts)
                _fire(_fault_hook, PHASE_AFTER_RADAR_UPDATE)

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return MaterializedRun(run_id=run.id)

    def materialize_failed_run(
        self,
        *,
        radar_id: str,
        token: str,
        run: RunRecord,
        failure: FailureRecord,
        now: datetime | None = None,
        lease_duration: timedelta | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> MaterializedRun:
        """Persiste la corrida fallida junto con su backoff y contadores.

        Media transacción aquí significa un radar que registró el fallo sin la
        corrida que lo explica, o al revés: un backoff sin auditoría.
        """
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                if not lease.renew(
                    conn, radar_id, token, now=now, lease_duration=lease_duration
                ):
                    conn.execute("ROLLBACK")
                    return MaterializedRun(lease_lost=True)
                _fire(_fault_hook, PHASE_AFTER_LEASE_FENCE)

                _insert_run(conn, radar_id, run)
                _fire(_fault_hook, PHASE_AFTER_RUN_INSERT)

                _apply_failure(conn, radar_id, failure, _now())
                _fire(_fault_hook, PHASE_AFTER_FAILURE_UPDATE)

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return MaterializedRun(run_id=run.id)
