"""Selector transaccional de intenciones de entrega (P1B-3, shadow-only).

La unidad central es ``select_pair(endpoint_id, radar_id)``. Cursor, lectura,
batch, asociaciones y watermark comparten una conexión y un ``BEGIN IMMEDIATE``.
No reclama batches, no crea intentos y no llama proveedores.

El SQL del par y la matriz de consentimiento viven en ``authorization.py`` y se
importan: el worker (P1B-4) revalida con exactamente los mismos, así que no puede
enviar lo que este módulo no habría creado.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

from app.core.db import connect_sqlite
from app.core.notify.authorization import TARGET_SQL as _TARGET_SQL
from app.core.notify.authorization import skip_reason as _target_skip_reason
from app.core.notify.grouping import SelectableRadarEvent, group_events
from app.core.notify.quiet_hours import QuietHoursError
from app.core.notify.quiet_hours import quiet_hours_not_before as shared_quiet_hours_not_before
from app.core.notify.outbox_store import (
    BatchEventRef,
    DuplicateEventAssociationError,
    PHASE_AFTER_ASSOC_INSERT as OUTBOX_AFTER_ASSOC_INSERT,
    PHASE_AFTER_BATCH_INSERT as OUTBOX_AFTER_BATCH_INSERT,
    _create_batch_on_connection,
)
from app.core.notify.render import build_payload, render_payload
from app.core.notify.schema import FREQUENCIES, ensure_schema
from app.core.radar.receipts import RadarReceiptStore


PHASE_AFTER_WINDOW_READ = "after_window_read"
PHASE_AFTER_BATCH_INSERT = "after_batch_insert"
PHASE_AFTER_ASSOCIATIONS = "after_associations"
PHASE_BEFORE_CURSOR_UPDATE = "before_cursor_update"
PHASE_AFTER_CURSOR_UPDATE = "after_cursor_update"
PHASE_BEFORE_COMMIT = "before_commit"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


class SelectorError(Exception):
    """Configuración o estado inconsistente durante la selección."""


@dataclass(frozen=True)
class SelectionResult:
    endpoint_id: str
    radar_id: str
    examined: int
    eligible: int
    associated: int
    notification_id: str | None
    cursor_before: int
    cursor_after: int
    created: bool
    skipped_reason: str | None


@dataclass(frozen=True)
class SelectionTarget:
    endpoint_id: str
    radar_id: str


@dataclass(frozen=True)
class EligibilityFlags:
    include_new: bool = True
    include_state_changes: bool = True
    include_value_changes: bool = True
    include_disappeared: bool = False


_ELIGIBLE_KIND_FLAG = {
    "new": "include_new",
    "changed_state": "include_state_changes",
    "changed_value": "include_value_changes",
    "disappeared": "include_disappeared",
}


def is_event_eligible(event: SelectableRadarEvent, flags: EligibilityFlags) -> bool:
    flag_name = _ELIGIBLE_KIND_FLAG.get(event.kind)
    if flag_name is None or not getattr(flags, flag_name):
        return False
    if event.kind == "disappeared" and not event.disappeared_confirmed:
        return False
    return True


def quiet_hours_not_before(
    now: datetime,
    *,
    start: str | None,
    end: str | None,
    timezone_name: str,
) -> str | None:
    """Wrapper de compatibilidad sobre la política compartida.

    El algoritmo vive en ``quiet_hours.py`` y es el mismo que revalida el worker
    al despachar: dos copias podrían separarse y el silencio dejaría de ser una
    propiedad del sistema para ser una coincidencia entre dos módulos. Aquí solo
    se traduce el error al vocabulario que los callers del selector ya manejan.
    """
    try:
        return shared_quiet_hours_not_before(
            now, start=start, end=end, timezone_name=timezone_name
        )
    except QuietHoursError as exc:
        raise SelectorError(str(exc)) from exc


# Solo ``per_run``. Las entradas ``daily``/``weekly`` que vivían aquí eran el único
# efecto que la frecuencia tenía en todo el sistema: elegir "diario" no cambiaba
# cuándo se entregaba, solo estiraba el TTL de 12 h a 36 h. Con una sola frecuencia
# soportada, la expiración vuelve a ser lo que debe ser —cuánto tiempo el mensaje
# sigue valiendo la pena— y deja de poder disfrazarse de cadencia.
_EXPIRATION = {
    "per_run": timedelta(hours=12),
}

# Razón estable de skip. No es consentimiento —la persona sí dijo que sí—, así que no
# entra en la matriz compartida: es una función que el runtime todavía no sabe cumplir.
SKIP_INCLUDE_DISAPPEARED_UNSUPPORTED = "include_disappeared_unsupported"


def expires_at_for_frequency(now: datetime, frequency: str) -> str:
    if now.tzinfo is None or now.utcoffset() is None:
        raise SelectorError("now debe incluir zona horaria")
    try:
        ttl = _EXPIRATION[frequency]
    except KeyError as exc:
        raise SelectorError(f"Frecuencia inválida: {frequency!r}") from exc
    return (now.astimezone(timezone.utc) + ttl).isoformat()


_WINDOW_SQL = """
SELECT e.*
  FROM radar_events e
 WHERE e.radar_id = ?
   AND e.event_seq IS NOT NULL
   AND e.event_seq > ?
   AND NOT EXISTS (
       SELECT 1 FROM radar_notification_batch_events be
        WHERE be.endpoint_id = ? AND be.event_id = e.id
   )
 ORDER BY e.event_seq ASC
 LIMIT ?
"""


def _event_from_row(row: Mapping) -> SelectableRadarEvent:
    keys = set(row.keys())

    def optional(name: str):
        # Filas heredadas (o sintéticas en pruebas) pueden no traer la columna.
        # Ausencia es ``None``, no un valor inventado.
        return row[name] if name in keys else None

    return SelectableRadarEvent(
        event_id=row["id"], event_seq=row["event_seq"], kind=row["kind"],
        result_key=row["result_key"], dataset=optional("dataset"), title=row["title"],
        entity=row["entity"], value=row["value"], status=row["status"],
        prev_status=row["prev_status"], prev_value=row["prev_value"],
        urlproceso=row["urlproceso"], referencia=row["referencia"],
        # ``ts`` es el instante en que el Radar observó el evento: es nuestro
        # reloj, no el de la fuente. El resto lo congeló R2B-1B1 en su instante.
        observed_at=optional("ts"),
        modality=optional("modality"),
        closing_date=optional("closing_date"),
        contractor=optional("contractor"),
        signature_date=optional("signature_date"),
        # Columna aditiva (TG-P2C): en bases migradas trae el dato real; en
        # filas heredadas de antes de migrar, ausencia es ``None``, igual que
        # el resto de metadata opcional de arriba.
        object_excerpt=optional("object_excerpt"),
        disappeared_confirmed=bool(row["disappeared_confirmed"])
        if "disappeared_confirmed" in keys else False,
    )


class NotificationSelector:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        # Misma SQLite, autoridad separada. El store crea su tabla de forma
        # idempotente; select_pair lee suppressions sobre SU propia conexión y
        # transacción para ordenar decisión humana ↔ batch.
        self.receipt_store = RadarReceiptStore(self.db_path)
        conn = self._connect()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def list_targets(self) -> list[SelectionTarget]:
        """Pares configurados; ``select_pair`` revalida su elegibilidad."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT endpoint_id, radar_id FROM radar_notification_preferences "
                "ORDER BY endpoint_id, radar_id"
            ).fetchall()
        finally:
            conn.close()
        return [SelectionTarget(r["endpoint_id"], r["radar_id"]) for r in rows]

    def select_pair(
        self,
        endpoint_id: str,
        radar_id: str,
        *,
        now: datetime,
        scan_limit: int = 500,
        _fault_hook: _FaultHook | None = None,
    ) -> SelectionResult:
        if isinstance(scan_limit, bool) or not isinstance(scan_limit, int) or scan_limit <= 0:
            raise ValueError("scan_limit debe ser un entero positivo")
        if now.tzinfo is None or now.utcoffset() is None:
            raise SelectorError("now debe incluir zona horaria")

        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                target = conn.execute(_TARGET_SQL, (endpoint_id, radar_id)).fetchone()
                cursor_row = conn.execute(
                    "SELECT last_examined_seq FROM radar_notification_cursors "
                    "WHERE endpoint_id = ? AND radar_id = ?",
                    (endpoint_id, radar_id),
                ).fetchone()
                cursor_before = int(cursor_row["last_examined_seq"]) if cursor_row else 0
                reason = _target_skip_reason(target)
                if reason is None and bool(target["include_disappeared"]):
                    # Fila heredada, o escrita por SQL directo: el store la rechaza
                    # desde R2B-1A, pero el schema todavía admite la columna en 1 y
                    # este módulo no puede confiar en que todo el mundo pasó por el
                    # store. ``is_event_eligible`` ya descartaría cada evento
                    # ``disappeared`` uno a uno —``disappeared_confirmed`` no existe
                    # como columna, así que siempre lee False—, pero eso entregaría un
                    # batch con el resto de los eventos y avanzaría el cursor como si
                    # la preferencia se hubiera respetado. Callar la diferencia entre
                    # "no había desapariciones" y "no sabemos verlas" es la mentira
                    # que este skip impide.
                    reason = SKIP_INCLUDE_DISAPPEARED_UNSUPPORTED
                if reason is not None:
                    conn.execute("ROLLBACK")
                    return SelectionResult(
                        endpoint_id, radar_id, 0, 0, 0, None,
                        cursor_before, cursor_before, False, reason,
                    )
                # Fail closed antes de leer la ventana: una frecuencia que este código
                # no sabe cumplir no puede materializar nada. Solo llega aquí por SQL
                # directo sobre una base heredada —el CHECK vigente y el store la
                # rechazan—, y en ese caso el estado es imposible, no inesperado.
                if target["frequency"] not in FREQUENCIES:
                    raise SelectorError(
                        f"La preferencia ({endpoint_id}, {radar_id}) tiene frecuencia "
                        f"{target['frequency']!r}, que este código no implementa. No se "
                        f"materializa nada: entregar por corrida bajo una etiqueta que "
                        f"promete otra cadencia sería incumplir lo que la persona eligió."
                    )

                rows = conn.execute(
                    _WINDOW_SQL, (radar_id, cursor_before, endpoint_id, scan_limit)
                ).fetchall()
                _fire(_fault_hook, PHASE_AFTER_WINDOW_READ)
                if not rows:
                    conn.execute("COMMIT")
                    return SelectionResult(
                        endpoint_id, radar_id, 0, 0, 0, None,
                        cursor_before, cursor_before, False, "empty_window",
                    )

                cursor_after = int(rows[-1]["event_seq"])
                flags = EligibilityFlags(
                    include_new=bool(target["include_new"]),
                    include_state_changes=bool(target["include_state_changes"]),
                    include_value_changes=bool(target["include_value_changes"]),
                    include_disappeared=bool(target["include_disappeared"]),
                )
                window = [_event_from_row(row) for row in rows]
                try:
                    suppressed = self.receipt_store.get_suppressed_result_keys(
                        str(target["owner"]), radar_id, connection=conn,
                    )
                except Exception as exc:
                    # Una decisión humana explícita pesa más que la puntualidad
                    # del push: sin poder resolverla de forma confiable no se
                    # materializa batch ni se mueve el cursor.
                    raise SelectorError(
                        "No se pudieron resolver las supresiones del Radar; "
                        "la selección falla cerrada."
                    ) from exc
                eligible = [
                    event for event in window
                    if event.result_key not in suppressed
                    and is_event_eligible(event, flags)
                ]
                notification_id: str | None = None
                created = False
                associated = 0

                if eligible:
                    created_at = now.astimezone(timezone.utc).isoformat()
                    # Antes de construir nada: ``quiet_hours_not_before`` valida la
                    # zona horaria de la preferencia y falla con ``SelectorError``.
                    # Es config inconsistente, no un payload roto, y así se sigue
                    # reportando como lo que es.
                    #
                    # Este ``not_before`` es una optimización, **no** la autorización
                    # final de entrega: evita que el worker reclame un batch que va a
                    # tener que devolver. La ventana puede cambiar entre esta corrida
                    # y el dispatch —o el batch puede envejecer hasta entrar en
                    # silencio—, así que quien decide de verdad es
                    # ``authorize_and_begin_dispatch``, que la reevalúa bajo su lock.
                    not_before = quiet_hours_not_before(
                        now, start=target["quiet_hours_start"],
                        end=target["quiet_hours_end"],
                        timezone_name=target["timezone"],
                    )
                    groups = group_events(eligible)
                    payload = build_payload(
                        groups,
                        principal_id=target["principal_id"], endpoint_id=endpoint_id,
                        radar_id=radar_id, radar_name=target["radar_name"],
                        channel=target["channel"], timezone=target["timezone"],
                        created_at=created_at,
                        max_items=int(target["max_items"]),
                    )
                    # Ejecutar el render puro aquí prueba que el payload congelado
                    # representa exactamente un mensaje lógico antes de persistir.
                    render_payload(payload, channel=target["channel"])
                    refs = [BatchEventRef(e.event_id, e.event_seq) for e in eligible]

                    def outbox_fault_bridge(phase: str) -> None:
                        if phase == OUTBOX_AFTER_BATCH_INSERT:
                            _fire(_fault_hook, PHASE_AFTER_BATCH_INSERT)
                        elif phase == OUTBOX_AFTER_ASSOC_INSERT:
                            _fire(_fault_hook, PHASE_AFTER_ASSOCIATIONS)

                    materialized = _create_batch_on_connection(
                        conn,
                        principal_id=target["principal_id"], endpoint_id=endpoint_id,
                        radar_id=radar_id, payload_json=payload, events=refs,
                        created_at=created_at,
                        expires_at=expires_at_for_frequency(now, target["frequency"]),
                        not_before=not_before,
                        _fault_hook=outbox_fault_bridge,
                    )
                    notification_id, created = materialized.notification_id, materialized.created
                    represented = {
                        (row["event_id"], row["event_seq"])
                        for row in conn.execute(
                            "SELECT event_id, event_seq "
                            "FROM radar_notification_batch_events "
                            "WHERE notification_id = ? AND endpoint_id = ?",
                            (notification_id, endpoint_id),
                        ).fetchall()
                    }
                    expected = {
                        (event.event_id, event.event_seq) for event in eligible
                    }
                    if represented != expected:
                        raise SelectorError(
                            "El batch idempotente no representa todos los eventos elegibles"
                        )
                    associated = len(expected)

                _fire(_fault_hook, PHASE_BEFORE_CURSOR_UPDATE)
                conn.execute(
                    "INSERT INTO radar_notification_cursors "
                    "(endpoint_id, radar_id, last_examined_seq, updated_at) "
                    "VALUES (?, ?, ?, ?) "
                    "ON CONFLICT(endpoint_id, radar_id) DO UPDATE SET "
                    "last_examined_seq=excluded.last_examined_seq, "
                    "updated_at=excluded.updated_at",
                    (endpoint_id, radar_id, cursor_after,
                     now.astimezone(timezone.utc).isoformat()),
                )
                _fire(_fault_hook, PHASE_AFTER_CURSOR_UPDATE)
                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                conn.execute("ROLLBACK")
                raise DuplicateEventAssociationError(
                    f"No se pudo materializar la selección: {exc}"
                ) from exc
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

        return SelectionResult(
            endpoint_id, radar_id, len(window), len(eligible), associated,
            notification_id, cursor_before, cursor_after, created, None,
        )


__all__ = [
    "EligibilityFlags",
    "NotificationSelector",
    "PHASE_AFTER_ASSOCIATIONS",
    "PHASE_AFTER_BATCH_INSERT",
    "PHASE_AFTER_CURSOR_UPDATE",
    "PHASE_AFTER_WINDOW_READ",
    "PHASE_BEFORE_COMMIT",
    "PHASE_BEFORE_CURSOR_UPDATE",
    "SKIP_INCLUDE_DISAPPEARED_UNSUPPORTED",
    "SelectionResult",
    "SelectionTarget",
    "SelectorError",
    "expires_at_for_frequency",
    "is_event_eligible",
    "quiet_hours_not_before",
]
