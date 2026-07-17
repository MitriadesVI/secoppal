"""ledger_store.py — write-ahead de intentos de entrega (P1B-2).

Un intento se registra **antes** de llamar al proveedor (``dispatching``) y se
cierra **después** (``finished``), nunca solo al terminar: si el proceso muere
entre ambos, ``dispatching`` huérfano es una señal legible —"se intentó, no se
sabe qué pasó"— en vez de silencio. La reconciliación de ese huérfano es
P1B-4; aquí solo se puede consultar (``get_open_dispatching_attempt``).

Toda escritura exige el ``claim_token`` vigente del batch: sin él, un worker
que ya perdió el claim no puede abrir ni cerrar un intento sobre el trabajo de
su sucesor. No existe un atajo público que marque ``sent`` sin pasar por un
intento — la única puerta es ``finish_attempt`` sobre un ``attempt_id`` real.
"""
from __future__ import annotations

import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.notify.schema import DEAD_REASONS, ensure_schema

_VALID_OUTCOMES = frozenset({"sent", "retry", "ambiguous", "dead"})
# Se deriva del vocabulario que el CHECK de la base impone, en vez de repetirlo: dos
# listas del mismo vocabulario divergen, y la que está en Python es la que produce el
# mensaje de error legible —si se quedara corta, el caller recibiría un IntegrityError
# de SQLite en vez de una explicación—.
_VALID_DEAD_REASONS = frozenset(DEAD_REASONS)


class LedgerError(Exception):
    """Error de negocio del ledger de intentos."""


class ClaimLostError(LedgerError):
    """El batch no está reclamado por este ``claim_token`` en este momento."""


class AttemptNotDispatchingError(LedgerError):
    """El intento no existe, no pertenece a este batch, o ya no está en
    ``dispatching``."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


# Puntos de inyección de fallo, igual que ``radar/store.py``: existen para
# probar que cada paso revierte por completo; en runtime el hook es None.
PHASE_AFTER_BEGIN = "after_begin"
PHASE_AFTER_ATTEMPT_COUNT_UPDATE = "after_attempt_count_update"
PHASE_AFTER_ATTEMPT_INSERT = "after_attempt_insert"
PHASE_AFTER_ATTEMPT_UPDATE = "after_attempt_update"
PHASE_AFTER_BATCH_UPDATE = "after_batch_update"
PHASE_BEFORE_COMMIT = "before_commit"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


def _insert_dispatching_attempt_on_connection(
    conn: sqlite3.Connection,
    *,
    notification_id: str,
    started_at: str,
    _fault_hook: _FaultHook | None = None,
) -> str:
    """Abre un intento ``dispatching`` sobre una transacción que es del caller.

    Mismo patrón que ``outbox_store._create_batch_on_connection``: no abre conexión,
    no ejecuta BEGIN, COMMIT ni ROLLBACK, y no revierte nada — si falla, revierte el
    caller, que es quien tiene la transacción. Existe porque ``begin_attempt`` (P1B-2)
    y ``worker_store.authorize_and_begin_dispatch`` (P1B-4) tienen que producir
    exactamente el mismo intento: dos INSERT distintos del mismo hecho acabarían
    numerando distinto, y ``attempt_count`` dejaría de contar llamadas al proveedor.

    **No valida autorización ni claim**, a propósito: el caller ya decidió que puede
    escribir, y meter aquí esa validación daría dos reglas de propiedad —la del ledger
    y la del worker— que podrían discrepar. Por eso tampoco es público: no debe existir
    una puerta que inserte intentos sin que alguien haya comprobado el claim antes.
    """
    row = conn.execute(
        "SELECT attempt_count FROM radar_notification_batches "
        "WHERE notification_id = ?",
        (notification_id,),
    ).fetchone()
    if row is None:
        raise LedgerError(
            f"El batch '{notification_id}' no existe: no se puede abrir un intento "
            f"sobre una intención de entrega que no está registrada."
        )
    attempt_number = row["attempt_count"] + 1
    conn.execute(
        "UPDATE radar_notification_batches SET attempt_count = ? "
        "WHERE notification_id = ?",
        (attempt_number, notification_id),
    )
    _fire(_fault_hook, PHASE_AFTER_ATTEMPT_COUNT_UPDATE)
    attempt_id = _new_id()
    conn.execute(
        "INSERT INTO radar_delivery_attempts "
        "(attempt_id, notification_id, attempt_number, phase, started_at) "
        "VALUES (?, ?, ?, 'dispatching', ?)",
        (attempt_id, notification_id, attempt_number, started_at),
    )
    _fire(_fault_hook, PHASE_AFTER_ATTEMPT_INSERT)
    return attempt_id


@dataclass(frozen=True)
class DeliveryAttempt:
    attempt_id: str
    notification_id: str
    attempt_number: int
    phase: str
    started_at: str
    finished_at: str | None
    provider_message_id: str | None
    provider_status: str | None
    error_class: str | None
    error_message_safe: str | None
    retry_after: int | None
    outcome: str | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "DeliveryAttempt":
        return cls(**{k: row[k] for k in row.keys()})


class LedgerStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        conn = self._connect()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    def begin_attempt(
        self,
        notification_id: str,
        claim_token: str,
        started_at: str | None = None,
        *,
        now: datetime | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> str:
        """Abre un intento en ``dispatching``, o nada si el claim no es vigente.

        "Vigente" exige lo mismo que ``claim.is_owned``: token exacto **y**
        ``claim_until`` todavía no vencido. Comprobar solo el token dejaría
        que un worker cuyo claim ya expiró siguiera escribiendo mientras nadie
        más lo hubiera reclamado todavía — pero ``claim_until`` es la promesa
        de que el trabajo termina para esa fecha, no una cortesía que solo
        aplica si alguien más la exige primero.

        ``attempt_count`` se incrementa y se lee en la misma transacción que
        el INSERT del intento: es lo que da ``attempt_number`` sin duplicados
        frente a dos ``begin_attempt`` concurrentes (solo uno puede tener el
        claim vigente a la vez, así que no hay carrera real, pero el
        incremento igual vive dentro del BEGIN IMMEDIATE por consistencia).

        ``now`` es inyectable para pruebas deterministas de expiración; por
        defecto usa el reloj real.
        """
        conn = self._connect()
        now_iso = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                row = conn.execute(
                    "SELECT status, claim_token, claim_until FROM "
                    "radar_notification_batches WHERE notification_id = ?",
                    (notification_id,),
                ).fetchone()
                if (row is None or row["status"] != "claimed"
                        or row["claim_token"] != claim_token
                        or row["claim_until"] is None
                        or row["claim_until"] <= now_iso):
                    raise ClaimLostError(
                        f"El batch '{notification_id}' no está reclamado "
                        f"(vigente) por este token: no se puede abrir un "
                        f"intento sin claim vigente."
                    )
                attempt_id = _insert_dispatching_attempt_on_connection(
                    conn,
                    notification_id=notification_id,
                    started_at=started_at or _now_iso(),
                    _fault_hook=_fault_hook,
                )
                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return attempt_id

    def finish_attempt(
        self,
        attempt_id: str,
        notification_id: str,
        claim_token: str,
        outcome: str,
        finished_at: str | None = None,
        provider_message_id: str | None = None,
        provider_status: str | None = None,
        error_class: str | None = None,
        error_message_safe: str | None = None,
        retry_after: int | None = None,
        next_not_before: str | None = None,
        dead_reason: str | None = None,
        *,
        now: datetime | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> None:
        """Cierra el intento y transiciona el batch, atómicamente o nada.

        Mapeo de ``outcome`` → estado del batch:

        - ``sent``      → batch ``sent``, claim limpio, ``dead_reason`` NULL.
        - ``ambiguous``  → batch ``ambiguous``, claim limpio. **No** vuelve a
          ``pending``: reintentar un envío que pudo haber llegado sería
          arriesgar un duplicado sin que nada lo distinga de un reintento
          seguro.
        - ``retry``      → batch ``pending``, claim limpio, ``not_before`` =
          ``next_not_before`` tal cual lo aporta el caller (P1B-2 no calcula
          backoff).
        - ``dead``       → batch ``dead``, claim limpio, exige ``dead_reason``
          válido.

        Si el claim ya no es del caller —vencido, liberado, o reclamado por
        otro worker— no se toca ni el intento ni el batch: todo o nada. Igual
        que en ``begin_attempt``, "vencido" se decide contra ``claim_until``,
        no solo contra el token: terminar un envío con un claim ya expirado
        dejaría escribir a un worker que la política ya considera reemplazable.

        ``now`` es inyectable para pruebas deterministas de expiración; por
        defecto usa el reloj real.
        """
        if outcome not in _VALID_OUTCOMES:
            raise LedgerError(
                f"outcome inválido '{outcome}'. Válidos: "
                f"{', '.join(sorted(_VALID_OUTCOMES))}."
            )
        if outcome == "dead" and dead_reason not in _VALID_DEAD_REASONS:
            raise LedgerError(
                f"dead_reason inválido o ausente para outcome='dead': "
                f"'{dead_reason}'. Válidos: {', '.join(sorted(_VALID_DEAD_REASONS))}."
            )

        now_iso = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                batch_row = conn.execute(
                    "SELECT status, claim_token, claim_until FROM "
                    "radar_notification_batches WHERE notification_id = ?",
                    (notification_id,),
                ).fetchone()
                if (batch_row is None or batch_row["status"] != "claimed"
                        or batch_row["claim_token"] != claim_token
                        or batch_row["claim_until"] is None
                        or batch_row["claim_until"] <= now_iso):
                    raise ClaimLostError(
                        f"El batch '{notification_id}' ya no está reclamado "
                        f"(vigente) por este token: no se puede cerrar el "
                        f"intento."
                    )
                attempt_row = conn.execute(
                    "SELECT notification_id, phase FROM radar_delivery_attempts "
                    "WHERE attempt_id = ?",
                    (attempt_id,),
                ).fetchone()
                if (attempt_row is None
                        or attempt_row["notification_id"] != notification_id
                        or attempt_row["phase"] != "dispatching"):
                    raise AttemptNotDispatchingError(
                        f"El intento '{attempt_id}' no existe, no pertenece "
                        f"al batch '{notification_id}', o ya no está en "
                        f"dispatching."
                    )

                conn.execute(
                    "UPDATE radar_delivery_attempts SET phase = 'finished', "
                    "finished_at = ?, provider_message_id = ?, "
                    "provider_status = ?, error_class = ?, "
                    "error_message_safe = ?, retry_after = ?, outcome = ? "
                    "WHERE attempt_id = ?",
                    (finished_at or _now_iso(), provider_message_id,
                     provider_status, error_class, error_message_safe,
                     retry_after, outcome, attempt_id),
                )
                _fire(_fault_hook, PHASE_AFTER_ATTEMPT_UPDATE)

                if outcome == "sent":
                    conn.execute(
                        "UPDATE radar_notification_batches SET status = 'sent', "
                        "claim_token = NULL, claim_until = NULL, "
                        "dead_reason = NULL WHERE notification_id = ?",
                        (notification_id,),
                    )
                elif outcome == "ambiguous":
                    conn.execute(
                        "UPDATE radar_notification_batches "
                        "SET status = 'ambiguous', claim_token = NULL, "
                        "claim_until = NULL WHERE notification_id = ?",
                        (notification_id,),
                    )
                elif outcome == "retry":
                    conn.execute(
                        "UPDATE radar_notification_batches SET status = 'pending', "
                        "claim_token = NULL, claim_until = NULL, "
                        "not_before = ? WHERE notification_id = ?",
                        (next_not_before, notification_id),
                    )
                else:  # dead
                    conn.execute(
                        "UPDATE radar_notification_batches SET status = 'dead', "
                        "claim_token = NULL, claim_until = NULL, "
                        "dead_reason = ? WHERE notification_id = ?",
                        (dead_reason, notification_id),
                    )
                _fire(_fault_hook, PHASE_AFTER_BATCH_UPDATE)

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()

    def get_attempt(self, attempt_id: str) -> DeliveryAttempt | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radar_delivery_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone()
        finally:
            conn.close()
        return DeliveryAttempt.from_row(row) if row else None

    def list_attempts(self, notification_id: str) -> list[DeliveryAttempt]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM radar_delivery_attempts WHERE notification_id = ? "
                "ORDER BY attempt_number",
                (notification_id,),
            ).fetchall()
        finally:
            conn.close()
        return [DeliveryAttempt.from_row(r) for r in rows]

    def get_open_dispatching_attempt(
        self, notification_id: str
    ) -> DeliveryAttempt | None:
        """El intento en ``dispatching`` más reciente, si quedó huérfano.

        Solo lectura: la reconciliación (reintentar, marcar ``ambiguous``,
        etc.) es del worker P1B-4, no de este módulo.
        """
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM radar_delivery_attempts WHERE notification_id = ? "
                "AND phase = 'dispatching' ORDER BY attempt_number DESC LIMIT 1",
                (notification_id,),
            ).fetchone()
        finally:
            conn.close()
        return DeliveryAttempt.from_row(row) if row else None
