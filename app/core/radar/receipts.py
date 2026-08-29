"""Receipts append-only de interacciones humanas con Radar (R5A).

``radar_events`` conserva lo que Radar observó. Esta autoridad separada conserva
lo que una persona hizo con ese hecho: abrirlo, descartarlo, marcarlo como no
relevante o restaurarlo. La separación es deliberada: ninguna interacción muta
ni borra el fact log del Radar.

Las disposiciones se resuelven por ``owner + radar_id + result_key``. Así, una
decisión sobre un proceso sigue aplicando a movimientos futuros del mismo
resultado dentro del mismo Radar, sin filtrarse a otro Radar ni a otro owner.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.db import connect_sqlite

VIEWED = "viewed"
DISCARDED = "discarded"
NOT_RELEVANT = "not_relevant"
RESTORED = "restored"
ACTIVE = "active"

DISPOSITION_ACTIONS = (DISCARDED, NOT_RELEVANT, RESTORED)
SUPPRESSED_DISPOSITIONS = (DISCARDED, NOT_RELEVANT)
EVIDENCE_SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS radar_event_receipts (
    receipt_seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    receipt_id   TEXT NOT NULL UNIQUE,
    owner        TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    radar_id     TEXT NOT NULL,
    result_key   TEXT,
    action       TEXT NOT NULL CHECK (
        action IN ('viewed', 'discarded', 'not_relevant', 'restored')
    ),
    event_kind   TEXT,
    occurred_at  TEXT NOT NULL,
    evidence_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_radar_receipts_owner_event
    ON radar_event_receipts(owner, event_id);
CREATE INDEX IF NOT EXISTS idx_radar_receipts_result_order
    ON radar_event_receipts(owner, radar_id, result_key, receipt_seq);
CREATE INDEX IF NOT EXISTS idx_radar_receipts_action_order
    ON radar_event_receipts(owner, action, receipt_seq);
CREATE UNIQUE INDEX IF NOT EXISTS idx_radar_receipts_one_view
    ON radar_event_receipts(owner, event_id)
    WHERE action = 'viewed';
"""

_LATEST_DISPOSITIONS_SQL = """
WITH ranked AS (
    SELECT receipt_seq, receipt_id, owner, event_id, radar_id, result_key,
           action, event_kind, occurred_at, evidence_json,
           ROW_NUMBER() OVER (
               PARTITION BY owner, radar_id, result_key
               ORDER BY receipt_seq DESC
           ) AS position
      FROM radar_event_receipts
     WHERE owner = ?
       AND action IN ('discarded', 'not_relevant', 'restored')
       AND result_key IS NOT NULL
)
SELECT * FROM ranked WHERE position = 1
"""

_SECRET_KEYS = (
    "authorization", "cookie", "password", "secret", "token", "api_key",
    "apikey", "app_token",
)


class RadarReceiptError(ValueError):
    """La interacción no puede aplicarse sin inventar identidad o ownership."""


class ReceiptTargetNotFoundError(RadarReceiptError):
    """El evento/resultado no existe para ese owner."""


class ReceiptDispositionError(RadarReceiptError):
    """El evento no admite una disposición estable."""


@dataclass(frozen=True, slots=True)
class ReceiptResult:
    """Resultado explícito de una escritura idempotente."""

    action: str
    created: bool
    noop: bool
    receipt_id: str | None
    previous_disposition: str
    current_disposition: str


@dataclass(frozen=True, slots=True)
class SuppressedResult:
    """Proyección de la última disposición de un resultado oculto."""

    radar_id: str
    result_key: str
    disposition: str
    receipt_seq: int
    event_id: str
    title: str | None
    radar_name: str | None


@dataclass(frozen=True, slots=True)
class NotRelevantFeedback:
    """Señal histórica y su vigencia actual para análisis posterior."""

    receipt_id: str
    receipt_seq: int
    owner: str
    radar_id: str
    result_key: str
    occurred_at: str
    evidence: dict[str, Any]
    current_disposition: str
    retracted: bool
    currently_restored: bool


def ensure_receipt_schema(conn: sqlite3.Connection) -> None:
    """Crea la autoridad de receipts de forma aditiva e idempotente."""
    conn.executescript(_SCHEMA)


class RadarReceiptStore:
    """API mínima de escritura y proyección sobre ``radar_event_receipts``."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        conn = self._connect()
        try:
            ensure_receipt_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def mark_viewed(
        self, owner: str, event_id: str, *, now: datetime | None = None,
    ) -> ReceiptResult:
        """Registra una sola vista real por ``owner + event_id``."""
        owner = _required(owner, "owner")
        event_id = _required(event_id, "event_id")
        stamp = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                event = _owned_event(conn, owner, event_id)
                if event is None:
                    raise ReceiptTargetNotFoundError(
                        "El evento no existe o no pertenece al owner."
                    )
                receipt_id = uuid.uuid4().hex
                inserted = conn.execute(
                    """
                    INSERT OR IGNORE INTO radar_event_receipts (
                        receipt_id, owner, event_id, radar_id, result_key,
                        action, event_kind, occurred_at, evidence_json
                    ) VALUES (?, ?, ?, ?, ?, 'viewed', ?, ?, ?)
                    """,
                    (
                        receipt_id, owner, event_id, event["radar_id"],
                        _optional_text(event["result_key"]), event["kind"], stamp,
                        _evidence_json(event, VIEWED, stamp),
                    ),
                ).rowcount == 1
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return ReceiptResult(
            action=VIEWED, created=inserted, noop=not inserted,
            receipt_id=receipt_id if inserted else None,
            previous_disposition=ACTIVE, current_disposition=ACTIVE,
        )

    def discard(
        self, owner: str, event_id: str, *, now: datetime | None = None,
    ) -> ReceiptResult:
        return self._set_disposition(owner, event_id, DISCARDED, now=now)

    def mark_not_relevant(
        self, owner: str, event_id: str, *, now: datetime | None = None,
    ) -> ReceiptResult:
        return self._set_disposition(owner, event_id, NOT_RELEVANT, now=now)

    def _set_disposition(
        self,
        owner: str,
        event_id: str,
        action: str,
        *,
        now: datetime | None,
    ) -> ReceiptResult:
        owner = _required(owner, "owner")
        event_id = _required(event_id, "event_id")
        stamp = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                event = _owned_event(conn, owner, event_id)
                if event is None:
                    raise ReceiptTargetNotFoundError(
                        "El evento no existe o no pertenece al owner."
                    )
                _validate_disposable(event)
                radar_id = str(event["radar_id"])
                result_key = str(event["result_key"]).strip()
                previous = _current_disposition(
                    conn, owner=owner, radar_id=radar_id, result_key=result_key
                )
                if previous == action:
                    conn.execute("COMMIT")
                    return ReceiptResult(
                        action=action, created=False, noop=True, receipt_id=None,
                        previous_disposition=previous,
                        current_disposition=previous,
                    )
                receipt_id = _insert_receipt(
                    conn, owner=owner, event=event, action=action, occurred_at=stamp
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return ReceiptResult(
            action=action, created=True, noop=False, receipt_id=receipt_id,
            previous_disposition=previous, current_disposition=action,
        )

    def restore(
        self,
        owner: str,
        radar_id: str,
        result_key: str,
        *,
        now: datetime | None = None,
    ) -> ReceiptResult:
        """Restaura sin borrar el receipt que originó la supresión."""
        owner = _required(owner, "owner")
        radar_id = _required(radar_id, "radar_id")
        result_key = _required(result_key, "result_key")
        stamp = _iso(now)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                event = _latest_owned_result_event(
                    conn, owner=owner, radar_id=radar_id, result_key=result_key
                )
                if event is None:
                    raise ReceiptTargetNotFoundError(
                        "El resultado no existe o no pertenece al owner y Radar."
                    )
                previous = _current_disposition(
                    conn, owner=owner, radar_id=radar_id, result_key=result_key
                )
                if previous == ACTIVE:
                    conn.execute("COMMIT")
                    return ReceiptResult(
                        action=RESTORED, created=False, noop=True, receipt_id=None,
                        previous_disposition=ACTIVE, current_disposition=ACTIVE,
                    )
                receipt_id = _insert_receipt(
                    conn, owner=owner, event=event, action=RESTORED,
                    occurred_at=stamp,
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return ReceiptResult(
            action=RESTORED, created=True, noop=False, receipt_id=receipt_id,
            previous_disposition=previous, current_disposition=ACTIVE,
        )

    def get_viewed_event_ids(
        self, owner: str, event_ids: list[str] | tuple[str, ...] | set[str],
    ) -> set[str]:
        owner = _required(owner, "owner")
        ids = list(dict.fromkeys(str(value) for value in event_ids if value))
        if not ids:
            return set()
        viewed: set[str] = set()
        conn = self._connect()
        try:
            for start in range(0, len(ids), 900):
                chunk = ids[start:start + 900]
                marks = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT event_id FROM radar_event_receipts "
                    f"WHERE owner = ? AND action = 'viewed' AND event_id IN ({marks})",
                    (owner, *chunk),
                ).fetchall()
                viewed.update(str(row["event_id"]) for row in rows)
        finally:
            conn.close()
        return viewed

    def get_current_dispositions(
        self, owner: str, *, radar_id: str | None = None,
    ) -> dict[tuple[str, str], str]:
        owner = _required(owner, "owner")
        conn = self._connect()
        try:
            rows = _latest_disposition_rows(conn, owner, radar_id=radar_id)
        finally:
            conn.close()
        return {
            (str(row["radar_id"]), str(row["result_key"])): (
                ACTIVE if row["action"] == RESTORED else str(row["action"])
            )
            for row in rows
        }

    def get_suppressed_result_keys(
        self,
        owner: str,
        radar_id: str | None = None,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> set[str] | set[tuple[str, str]]:
        """Claves suprimidas; puede leer dentro de una transacción del caller.

        NotificationSelector pasa su propia conexión para ordenar esta lectura
        con la materialización del batch bajo el mismo ``BEGIN IMMEDIATE``.
        """
        owner = _required(owner, "owner")
        conn = connection or self._connect()
        try:
            rows = _latest_disposition_rows(conn, owner, radar_id=radar_id)
        finally:
            if connection is None:
                conn.close()
        suppressed = [row for row in rows if row["action"] in SUPPRESSED_DISPOSITIONS]
        if radar_id is not None:
            return {str(row["result_key"]) for row in suppressed}
        return {
            (str(row["radar_id"]), str(row["result_key"])) for row in suppressed
        }

    def list_suppressed(self, owner: str) -> list[SuppressedResult]:
        owner = _required(owner, "owner")
        conn = self._connect()
        try:
            rows = _latest_disposition_rows(conn, owner)
        finally:
            conn.close()
        results: list[SuppressedResult] = []
        for row in rows:
            if row["action"] not in SUPPRESSED_DISPOSITIONS:
                continue
            evidence = _loads_dict(row["evidence_json"])
            results.append(SuppressedResult(
                radar_id=str(row["radar_id"]),
                result_key=str(row["result_key"]),
                disposition=str(row["action"]),
                receipt_seq=int(row["receipt_seq"]),
                event_id=str(row["event_id"]),
                title=_optional_text(evidence.get("title")),
                radar_name=_optional_text(evidence.get("radar_name")),
            ))
        return results

    def list_not_relevant_feedback(
        self, *, owner: str | None = None,
    ) -> list[NotRelevantFeedback]:
        """Histórico de señales, indicando cuáles fueron retractadas/restauradas."""
        params: list[Any] = []
        owner_filter = ""
        if owner is not None:
            owner_filter = "AND nr.owner = ?"
            params.append(_required(owner, "owner"))
        conn = self._connect()
        try:
            rows = conn.execute(
                f"""
                SELECT nr.*,
                       COALESCE((
                           SELECT latest.action
                             FROM radar_event_receipts latest
                            WHERE latest.owner = nr.owner
                              AND latest.radar_id = nr.radar_id
                              AND latest.result_key = nr.result_key
                              AND latest.action IN (
                                  'discarded', 'not_relevant', 'restored'
                              )
                            ORDER BY latest.receipt_seq DESC LIMIT 1
                       ), 'restored') AS latest_action,
                       EXISTS(
                           SELECT 1 FROM radar_event_receipts retracted
                            WHERE retracted.owner = nr.owner
                              AND retracted.radar_id = nr.radar_id
                              AND retracted.result_key = nr.result_key
                              AND retracted.action = 'restored'
                              AND retracted.receipt_seq > nr.receipt_seq
                       ) AS was_retracted
                  FROM radar_event_receipts nr
                 WHERE nr.action = 'not_relevant' {owner_filter}
                 ORDER BY nr.receipt_seq DESC
                """,
                tuple(params),
            ).fetchall()
        finally:
            conn.close()
        return [
            NotRelevantFeedback(
                receipt_id=str(row["receipt_id"]),
                receipt_seq=int(row["receipt_seq"]),
                owner=str(row["owner"]),
                radar_id=str(row["radar_id"]),
                result_key=str(row["result_key"]),
                occurred_at=str(row["occurred_at"]),
                evidence=_loads_dict(row["evidence_json"]),
                current_disposition=(
                    ACTIVE if row["latest_action"] == RESTORED
                    else str(row["latest_action"])
                ),
                retracted=bool(row["was_retracted"]),
                currently_restored=row["latest_action"] == RESTORED,
            )
            for row in rows
        ]


def _owned_event(
    conn: sqlite3.Connection, owner: str, event_id: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT e.*, r.query_frame_json, r.name AS current_radar_name
          FROM radar_events e
          JOIN radars r ON r.id = e.radar_id AND r.owner = e.owner
         WHERE e.id = ? AND e.owner = ?
        """,
        (event_id, owner),
    ).fetchone()


def _latest_owned_result_event(
    conn: sqlite3.Connection, *, owner: str, radar_id: str, result_key: str,
) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT e.*, r.query_frame_json, r.name AS current_radar_name
          FROM radar_events e
          JOIN radars r ON r.id = e.radar_id AND r.owner = e.owner
         WHERE e.owner = ? AND e.radar_id = ? AND e.result_key = ?
         ORDER BY e.event_seq DESC, e.rowid DESC
         LIMIT 1
        """,
        (owner, radar_id, result_key),
    ).fetchone()


def _validate_disposable(event: sqlite3.Row) -> None:
    if event["kind"] == "baseline":
        raise ReceiptDispositionError("La línea base no es una oportunidad descartable.")
    if _optional_text(event["result_key"]) is None:
        raise ReceiptDispositionError(
            "El evento no tiene una identidad estable para aplicar la decisión."
        )


def _current_disposition(
    conn: sqlite3.Connection, *, owner: str, radar_id: str, result_key: str,
) -> str:
    row = conn.execute(
        """
        SELECT action FROM radar_event_receipts
         WHERE owner = ? AND radar_id = ? AND result_key = ?
           AND action IN ('discarded', 'not_relevant', 'restored')
         ORDER BY receipt_seq DESC LIMIT 1
        """,
        (owner, radar_id, result_key),
    ).fetchone()
    if row is None or row["action"] == RESTORED:
        return ACTIVE
    return str(row["action"])


def _insert_receipt(
    conn: sqlite3.Connection,
    *,
    owner: str,
    event: sqlite3.Row,
    action: str,
    occurred_at: str,
) -> str:
    receipt_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO radar_event_receipts (
            receipt_id, owner, event_id, radar_id, result_key, action,
            event_kind, occurred_at, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            receipt_id, owner, event["id"], event["radar_id"],
            _optional_text(event["result_key"]), action, event["kind"],
            occurred_at, _evidence_json(event, action, occurred_at),
        ),
    )
    return receipt_id


def _latest_disposition_rows(
    conn: sqlite3.Connection, owner: str, *, radar_id: str | None = None,
) -> list[sqlite3.Row]:
    sql = _LATEST_DISPOSITIONS_SQL
    params: list[Any] = [owner]
    if radar_id is not None:
        sql += " AND radar_id = ?"
        params.append(_required(radar_id, "radar_id"))
    sql += " ORDER BY receipt_seq DESC"
    return conn.execute(sql, tuple(params)).fetchall()


def _evidence_json(event: sqlite3.Row, action: str, occurred_at: str) -> str:
    query_frame = _loads_dict(event["query_frame_json"])
    evidence: dict[str, Any] = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "action": action,
        "event_id": event["id"],
        "event_kind": event["kind"],
        "event_ts": event["ts"],
        "radar_id": event["radar_id"],
        "radar_name": event["radar_name"] or event["current_radar_name"],
        "dataset": event["dataset"],
        "result_key": event["result_key"],
        "title": event["title"],
        "object_excerpt": event["object_excerpt"],
        "entity": event["entity"],
        "department": event["department"],
        "city": event["city"],
        "value": event["value"],
        "source_status": event["status"],
        "modality": event["modality"],
        "closing_date": event["closing_date"],
        "reference": event["referencia"],
        "secop_url": event["urlproceso"],
        "feedback_at": occurred_at,
    }
    if action == NOT_RELEVANT:
        evidence["feedback"] = NOT_RELEVANT
        evidence["query_frame"] = _sanitize(query_frame)
    return json.dumps(evidence, ensure_ascii=False, sort_keys=True)


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _sanitize(item)
            for key, item in value.items()
            if not any(secret in str(key).casefold() for secret in _SECRET_KEYS)
        }
    if isinstance(value, list):
        return [_sanitize(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _loads_dict(raw: object) -> dict[str, Any]:
    try:
        value = json.loads(str(raw or "{}"))
    except (json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def _required(value: object, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} no puede estar vacío")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _iso(now: datetime | None) -> str:
    moment = now or datetime.now(timezone.utc)
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("now debe incluir zona horaria")
    return moment.astimezone(timezone.utc).isoformat()


__all__ = [
    "ACTIVE",
    "DISCARDED",
    "NOT_RELEVANT",
    "RESTORED",
    "RadarReceiptError",
    "RadarReceiptStore",
    "ReceiptDispositionError",
    "ReceiptResult",
    "ReceiptTargetNotFoundError",
    "SuppressedResult",
    "NotRelevantFeedback",
    "ensure_receipt_schema",
]
