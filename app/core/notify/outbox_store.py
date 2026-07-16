"""outbox_store.py — la intención determinista de entrega (P1B-2).

``create_batch`` es la única escritura de producto: da de alta un batch y sus
asociaciones con sus eventos, o no da de alta nada. No consulta preferencias,
no consulta ``radar_events``, no selecciona qué notificar — eso es P1B-3. Este
módulo solo sabe materializar una intención ya decidida por el caller, de
forma atómica e idempotente.

La identidad del batch (``idempotency_key``) la calcula ``idempotency.py``;
este módulo no la recalcula con otra fórmula ni la deja en manos del caller.
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.notify.idempotency import NOTIFICATION_POLICY_VERSION
from app.core.notify.idempotency import batch_key as _compute_batch_key
from app.core.notify.idempotency import idempotency_key as _compute_idempotency_key
from app.core.notify.schema import ensure_schema

# Puntos de inyección de fallo, igual que radar/store.py y notify/ledger_store.py:
# existen para probar que cada paso revierte por completo; en runtime el hook
# siempre es None.
PHASE_AFTER_BATCH_INSERT = "after_batch_insert"
PHASE_AFTER_ASSOC_INSERT = "after_assoc_insert"
PHASE_BEFORE_COMMIT = "before_commit"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


class OutboxError(Exception):
    """Error de negocio del outbox."""


class DuplicateEventAssociationError(OutboxError):
    """Un event_id se repite dentro del batch, o ya pertenece a otro batch del
    mismo endpoint. En ambos casos, el batch completo se revierte: no hay
    forma de "arreglar" solo la asociación en conflicto sin dejar la
    intención a medias."""


@dataclass(frozen=True)
class BatchEventRef:
    """Un evento que entra a un batch, con su ``event_seq`` ya asignado."""

    event_id: str
    event_seq: int


@dataclass(frozen=True)
class NotificationBatch:
    notification_id: str
    principal_id: str
    endpoint_id: str
    radar_id: str
    batch_key: str
    idempotency_key: str
    external_idempotency_key: str | None
    payload_json: str
    status: str
    dead_reason: str | None
    not_before: str | None
    claim_token: str | None
    claim_until: str | None
    attempt_count: int
    created_at: str
    expires_at: str

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "NotificationBatch":
        return cls(**{k: row[k] for k in row.keys()})


@dataclass(frozen=True)
class BatchMaterialization:
    """Resultado de ``create_batch``. ``created=False`` = ya existía el mismo
    batch lógico (misma ``idempotency_key``): se devuelve la fila existente,
    sin tocarla ni crear nuevas asociaciones."""

    notification_id: str
    created: bool = False


_INSERT_BATCH_SQL = """
INSERT INTO radar_notification_batches
   (notification_id, principal_id, endpoint_id, radar_id, batch_key,
    idempotency_key, external_idempotency_key, payload_json, status,
    dead_reason, not_before, claim_token, claim_until, attempt_count,
    created_at, expires_at)
   VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'pending', NULL, ?, NULL, NULL, 0, ?, ?)
"""

_INSERT_ASSOC_SQL = """
INSERT INTO radar_notification_batch_events
   (notification_id, endpoint_id, event_id, event_seq)
   VALUES (?, ?, ?, ?)
"""

_FETCH_BY_IDEMPOTENCY_SQL = """
SELECT * FROM radar_notification_batches
 WHERE endpoint_id = ? AND idempotency_key = ?
"""


class OutboxStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        conn = self._connect()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def create_batch(
        self,
        *,
        principal_id: str,
        endpoint_id: str,
        radar_id: str,
        payload_json,
        events: list[BatchEventRef],
        created_at: str,
        expires_at: str,
        not_before: str | None = None,
        policy_version: str = NOTIFICATION_POLICY_VERSION,
        _fault_hook: _FaultHook | None = None,
    ) -> BatchMaterialization:
        """Da de alta un batch y todas sus asociaciones, o ninguna.

        ``payload_json`` acepta el texto ya serializado o un dict/list —en
        ese caso se serializa aquí, una sola vez, con claves ordenadas para
        que el mismo payload lógico produzca siempre el mismo texto.
        """
        if not events:
            raise OutboxError(
                "Un batch necesita al menos un evento: sin eventos no hay "
                "intención de entrega que materializar."
            )
        seen_ids: set[str] = set()
        for ref in events:
            if ref.event_id in seen_ids:
                raise DuplicateEventAssociationError(
                    f"El evento '{ref.event_id}' aparece dos veces en el "
                    f"mismo batch."
                )
            seen_ids.add(ref.event_id)

        if isinstance(payload_json, (dict, list)):
            payload_json = json.dumps(
                payload_json, ensure_ascii=False, sort_keys=True
            )

        key = _compute_idempotency_key(
            endpoint_id=endpoint_id,
            radar_id=radar_id,
            event_seqs=[ref.event_seq for ref in events],
            policy_version=policy_version,
        )
        bkey = _compute_batch_key(radar_id=radar_id, endpoint_id=endpoint_id)
        notification_id = uuid.uuid4().hex

        conn = self._connect()
        result: BatchMaterialization | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                try:
                    conn.execute(
                        _INSERT_BATCH_SQL,
                        (notification_id, principal_id, endpoint_id, radar_id,
                         bkey, key, payload_json, not_before, created_at,
                         expires_at),
                    )
                except sqlite3.IntegrityError:
                    existing = self._fetch_by_idempotency(conn, endpoint_id, key)
                    if existing is None:
                        raise OutboxError(
                            "Conflicto de idempotencia sin fila existente "
                            "localizable: estado inconsistente."
                        )
                    if existing.radar_id != radar_id or existing.endpoint_id != endpoint_id:
                        raise OutboxError(
                            f"El batch existente con idempotency_key='{key}' "
                            f"no corresponde al radar/endpoint esperados."
                        )
                    result = BatchMaterialization(
                        notification_id=existing.notification_id, created=False
                    )
                else:
                    _fire(_fault_hook, PHASE_AFTER_BATCH_INSERT)
                    for ref in events:
                        conn.execute(
                            _INSERT_ASSOC_SQL,
                            (notification_id, endpoint_id, ref.event_id, ref.event_seq),
                        )
                    _fire(_fault_hook, PHASE_AFTER_ASSOC_INSERT)
                    result = BatchMaterialization(
                        notification_id=notification_id, created=True
                    )
                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                conn.execute("ROLLBACK")
                raise DuplicateEventAssociationError(
                    f"No se pudo asociar el batch a sus eventos: {exc}"
                ) from exc
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        assert result is not None
        return result

    def _fetch_by_idempotency(
        self, conn: sqlite3.Connection, endpoint_id: str, key: str
    ) -> NotificationBatch | None:
        row = conn.execute(_FETCH_BY_IDEMPOTENCY_SQL, (endpoint_id, key)).fetchone()
        return NotificationBatch.from_row(row) if row else None

    def get_batch(self, notification_id: str) -> NotificationBatch | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radar_notification_batches WHERE notification_id = ?",
                (notification_id,),
            ).fetchone()
        finally:
            conn.close()
        return NotificationBatch.from_row(row) if row else None

    def list_batch_events(self, notification_id: str) -> list[BatchEventRef]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT event_id, event_seq FROM radar_notification_batch_events "
                "WHERE notification_id = ? ORDER BY event_seq",
                (notification_id,),
            ).fetchall()
        finally:
            conn.close()
        return [
            BatchEventRef(event_id=r["event_id"], event_seq=r["event_seq"])
            for r in rows
        ]

    def list_batches_by_key(self, batch_key: str) -> list[NotificationBatch]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_notification_batches WHERE batch_key = ? "
                "ORDER BY created_at",
                (batch_key,),
            ).fetchall()
        finally:
            conn.close()
        return [NotificationBatch.from_row(r) for r in rows]

    _COUNT_FILTERABLE = frozenset({"radar_id", "endpoint_id", "status"})

    def count_batches(self, **filters) -> int:
        """Cuenta batches por los filtros dados (whitelist: radar_id,
        endpoint_id, status). Solo para inspección/tests, no hay caller de
        producto todavía."""
        unknown = set(filters) - self._COUNT_FILTERABLE
        if unknown:
            raise OutboxError(
                f"Filtros no soportados: {', '.join(sorted(unknown))}. "
                f"Permitidos: {', '.join(sorted(self._COUNT_FILTERABLE))}."
            )
        sql = "SELECT COUNT(*) AS n FROM radar_notification_batches"
        if filters:
            sql += " WHERE " + " AND ".join(f"{k} = ?" for k in filters)
        conn = self._connect()
        try:
            row = conn.execute(sql, tuple(filters.values())).fetchone()
        finally:
            conn.close()
        return int(row["n"])
