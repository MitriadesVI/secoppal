"""store.py — persistencia SQLite del Radar.

Tres tablas sobre la misma base que feedback (``data/secoppal.db``), creadas
de forma idempotente:

- ``radars``              — la definición del radar (QueryFrame serializado + metadatos).
- ``radar_runs``          — una fila por corrida, con auditoría (soql/params/execution_ms).
- ``radar_seen_results``  — memoria de lo visto, para diffear corridas sucesivas.

El ``RadarStore`` abre conexiones cortas por operación vía ``connect_sqlite``
(WAL + FK + busy_timeout ya aplicados).
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.radar import lease
from app.core.radar.lease import LeaseClaim


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


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
            conn.execute(
                "UPDATE radars SET last_run_at = ?, last_error_at = NULL, "
                "last_error_message = NULL, failure_count = 0, "
                "last_failure_reason = NULL, next_run_after = NULL, "
                "needs_attention = 0, permanent_failure_streak = 0 WHERE id = ?",
                (ts or _now(), radar_id),
            )
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
        permanent_flag = 1 if permanent else 0
        conn = self._connect()
        try:
            conn.execute(
                """UPDATE radars
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
                   WHERE id = ?""",
                (
                    ts or _now(), message,
                    permanent_flag,
                    reason, next_run_after,
                    permanent_flag, pause_after,
                    permanent_flag, pause_after,
                    radar_id,
                ),
            )
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
        run_id = _new_id()
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO radar_runs
                   (id, radar_id, started_at, finished_at, status, total_count,
                    new_count, changed_count, digest_json, error, soql, params_json,
                    dataset_id, approx_total, execution_ms, outcome, reason)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (run_id, radar_id, started_at, finished_at or _now(), status,
                 total_count, new_count, changed_count, digest_json, error, soql,
                 params_json, dataset_id, approx_total, execution_ms, outcome, reason),
            )
        finally:
            conn.close()
        return run_id

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
        ``radar_events`` (sin ``id``, que se genera). No-op si la lista vacía."""
        if not events:
            return
        conn = self._connect()
        try:
            conn.executemany(
                """INSERT INTO radar_events
                   (id, radar_id, owner, radar_name, run_id, ts, kind, result_key,
                    dataset, title, entity, value, status, prev_status, prev_value,
                    urlproceso, referencia)
                   VALUES (:id, :radar_id, :owner, :radar_name, :run_id, :ts, :kind,
                    :result_key, :dataset, :title, :entity, :value, :status,
                    :prev_status, :prev_value, :urlproceso, :referencia)""",
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
        ts = seen_at or _now()
        conn = self._connect()
        try:
            conn.execute(
                """INSERT INTO radar_seen_results
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
                       contractor         = excluded.contractor""",
                (radar_id, result_key, dataset, fields.get("urlproceso", ""),
                 fields.get("referencia", ""), fields.get("title", ""),
                 fields.get("entity", ""), fields.get("value", ""),
                 fields.get("status", ""), fields.get("contractor", ""),
                 ts, ts, snapshot_hash, last_snapshot_json, run_id, run_id,
                 relevance_label),
            )
        finally:
            conn.close()
