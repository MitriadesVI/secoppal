"""worker_store.py — las primitivas transaccionales del worker (P1B-4, R2A).

Dueño exclusivo del SQL nuevo del worker. **No hay worker todavía**: aquí no se
llama a ningún proveedor, no se renderiza nada y no se toca la red. Este módulo
solo garantiza que, cuando exista, el worker pueda reclamar, autorizar, reconciliar
y barrer sin poder mentir.

Cuatro decisiones gobiernan el módulo:

1. **El reloj se captura después del lock.** ``BEGIN IMMEDIATE`` puede esperar hasta
   ``busy_timeout`` por el lock de escritor. Si ``now`` se capturara antes, un claim
   de 120 s que esperó 25 s por el lock daría 95 s reales de propiedad: la ventana se
   come a sí misma justo cuando hay contención, que es cuando importa.
2. **Autorizar y abrir el intento son una sola transacción.** Su COMMIT es el punto
   de linearización entre revocar y autorizar: SQLite en WAL tiene un solo escritor,
   así que revocación y autorización tienen orden total. Del lado "revocó primero"
   no hay intento; del lado "autorizamos primero", el intento ya está en vuelo y
   fingir que lo detuvimos sería mentir.
3. **Un ``dispatching`` abierto es evidencia, no basura.** Significa "alguien iba a
   llamar al proveedor y no sabemos cómo acabó". Por eso el sweep no puede tocar un
   batch que tenga uno: marcarlo ``expired`` afirmaría que nada salió sobre un mensaje
   que puede estar en el teléfono de alguien.
4. **Ante dos ``dispatching`` abiertos, fallar cerrado.** Escoger "el último" sería
   inventar una respuesta a una pregunta que el estado no contesta.

Ninguna función libera el claim tras abrir un intento: las únicas salidas de un
intento abierto son cerrarlo o abandonarlo para que la reconciliación lo tome.
"""
from __future__ import annotations

import hashlib
import sqlite3
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.db import connect_sqlite
from app.core.notify import authorization
from app.core.notify.ledger_store import (
    PHASE_AFTER_ATTEMPT_COUNT_UPDATE as _LEDGER_AFTER_ATTEMPT_COUNT,
    PHASE_AFTER_ATTEMPT_INSERT as _LEDGER_AFTER_ATTEMPT_INSERT,
    _insert_dispatching_attempt_on_connection,
)
from app.core.notify.quiet_hours import QuietHoursError
from app.core.notify.quiet_hours import quiet_hours_not_before as _quiet_hours_not_before
from app.core.notify.schema import ensure_schema

EXTERNAL_IDEMPOTENCY_VERSION = "p1b-extid-v1"

DEFAULT_HOLD_DURATION = timedelta(minutes=15)

# Razón estable de un aplazamiento por silencio. **No** es una categoría de la matriz
# de consentimiento: la persona sí dijo que sí, solo que no a estas horas. Meterla en
# ``AuthorizationDecision`` mezclaría "no quiere esto" con "ahora no".
REASON_QUIET_HOURS = "quiet_hours"

# La expiración es terminal, pero **no** vive en la matriz compartida: es una regla
# temporal del batch, no del consentimiento, y el selector no tiene nada que
# revalidar sobre ella (él fija el TTL al crear). Se expresa con el mismo tipo que la
# matriz para que el dispatch de salidas sea uno solo.
_EXPIRED = authorization.AuthorizationDecision(
    authorization.TERMINAL, "expired", "expired"
)

# Salidas de ``authorize_and_begin_dispatch``. Cerradas: el caller hace dispatch sobre
# ellas sin rama por defecto.
OUTCOME_AUTHORIZED = "authorized"
OUTCOME_CLAIM_LOST = "claim_lost"
OUTCOME_DEAD = "dead"
OUTCOME_HOLD = "hold"
OUTCOME_SECURITY_BLOCKED = "security_blocked"

# Fases de inyección de fallo. En runtime el hook es siempre None; existen para probar
# que cada paso revierte por completo, y están puestas donde la evidencia empieza a
# existir, no donde sería cómodo.
PHASE_AFTER_BEGIN = "after_begin"
PHASE_AFTER_CLAIM_VALIDATION = "after_claim_validation"
PHASE_AFTER_AUTHORIZATION_READ = "after_authorization_read"
PHASE_AFTER_EXTERNAL_KEY = "after_external_key"
PHASE_AFTER_ATTEMPT_COUNT = "after_attempt_count"
PHASE_AFTER_ATTEMPT_INSERT = "after_attempt_insert"
PHASE_BEFORE_COMMIT = "before_commit"
# Fuera de la transacción, por definición: sirve para orquestar tests que necesitan
# actuar entre el COMMIT y el retorno. Dispararlo dentro haría de la fase una mentira.
PHASE_AFTER_COMMIT = "after_commit"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


class WorkerStoreError(Exception):
    """Error de negocio de las primitivas del worker."""


class WorkerStoreConsistencyError(WorkerStoreError):
    """El estado persistido no admite una respuesta correcta.

    No es un fallo transitorio ni un caso a reintentar: es una señal de que algo
    escribió un estado que el diseño considera imposible (dos intentos abiertos sobre
    el mismo batch, una clave externa que cambió bajo nuestros pies). Se lanza sin
    modificar nada, para que quede exactamente como estaba y un humano pueda mirarlo.
    """


def external_idempotency_key(notification_id: str) -> str:
    """``sha256("p1b-extid-v1|" + notification_id)``. Pura, estable, sin PII.

    Depende **solo** del ``notification_id``: ni del texto, ni del destinatario, ni
    del intento. Meter el recipient la volvería un pseudónimo estable de una persona
    viajando hacia un tercero; que dependiera del texto o del intento haría que dos
    reintentos del mismo mensaje lógico llevaran claves distintas, que es exactamente
    lo contrario de para lo que existe.

    Vive aquí —y no en un ``provider.py`` que R2A no crea— porque este módulo es su
    único escritor.
    """
    payload = f"{EXTERNAL_IDEMPOTENCY_VERSION}|{notification_id}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _stamp(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat()


def _require_aware(moment: datetime, label: str) -> datetime:
    if not isinstance(moment, datetime):
        raise WorkerStoreError(
            f"{label} debe devolver datetime, no {type(moment).__name__}."
        )
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise WorkerStoreError(
            f"{label} debe incluir zona horaria: un instante naive comparado contra "
            f"marcas UTC ordena mal y el claim vencería cuando no debe."
        )
    return moment


def _require_duration(duration: timedelta, label: str) -> timedelta:
    if not isinstance(duration, timedelta):
        raise WorkerStoreError(
            f"{label} debe ser timedelta, no {type(duration).__name__}."
        )
    if duration <= timedelta(0):
        raise WorkerStoreError(
            f"{label} debe ser positiva: un claim de duración {duration!r} vence en "
            f"el instante en que se toma."
        )
    return duration


def _require_limit(limit: int, label: str) -> int:
    # ``isinstance(True, int)`` es True en Python, así que sin este chequeo
    # ``limit=True`` pasaría como 1 y barrería un batch en silencio.
    if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
        raise WorkerStoreError(f"{label} debe ser un entero positivo, no {limit!r}.")
    return limit


@dataclass(frozen=True)
class ClaimedCandidate:
    """``claim_token`` va con ``repr=False``: es la prueba de propiedad del batch.
    Quien lo tenga puede escribir sobre el trabajo de otro, así que está en la lista
    de lo que nunca se loguea — y estos objetos son justo lo que alguien vuelca entero
    al depurar."""

    notification_id: str
    claim_token: str = field(repr=False)
    claimed_at: str
    claim_until: str


@dataclass(frozen=True)
class DispatchAuthorization:
    """Resultado de ``authorize_and_begin_dispatch``.

    Tres campos van con ``repr=False``, y son exactamente los que la política prohíbe
    loguear: ``external_recipient_id`` identifica a una persona real, ``claim_token``
    es la prueba de propiedad del batch, y ``payload_json`` es el contenido íntegro
    del mensaje. Este objeto es justo lo que alguien vuelca entero al depurar el
    worker; lo que quede en el repr acaba en un log.

    Lo que sí aparece es vocabulario cerrado e ids opacos —``outcome``, ``reason``,
    ``dead_reason``, ``channel``, ``attempt_id``—: suficiente para explicar qué pasó
    sin decir a quién ni con qué texto.
    """

    outcome: str
    notification_id: str
    reason: str | None = None
    dead_reason: str | None = None
    attempt_id: str | None = None
    channel: str | None = None
    external_idempotency_key: str | None = None
    claim_token: str | None = field(default=None, repr=False)
    payload_json: str | None = field(default=None, repr=False)
    external_recipient_id: str | None = field(default=None, repr=False)
    # Cuándo vuelve a ser evaluable. **Al final de todo**, y no junto a
    # ``dead_reason`` donde encajaría mejor semánticamente: insertarlo en medio
    # desplaza cada argumento posicional siguiente, así que un caller que
    # construyera esto por posición pasaría a poner su ``attempt_id`` aquí. El
    # default no protege de eso —solo de omitirlo—.
    not_before: str | None = None


@dataclass(frozen=True)
class ReconciliationClaim:
    notification_id: str
    attempt_id: str
    claim_token: str = field(repr=False)
    claim_until: str


# ── Predicados ────────────────────────────────────────────────────────────────────
#
# El fragmento de dispatching abierto se escribe una vez y se compone: es la cláusula
# que separa "de quién es este batch" en las tres operaciones, y tenerla en tres
# literales distintos sería tener tres definiciones de huérfano.

_NO_OPEN_DISPATCHING = """
   AND NOT EXISTS (SELECT 1 FROM radar_delivery_attempts a
                    WHERE a.notification_id = b.notification_id
                      AND a.phase = 'dispatching')
"""

_HAS_OPEN_DISPATCHING = """
   AND EXISTS (SELECT 1 FROM radar_delivery_attempts a
                WHERE a.notification_id = b.notification_id
                  AND a.phase = 'dispatching')
"""

# Reclamable por el camino normal: vivo, fuera de cuarentena, y en 'pending' o en
# 'claimed' con el claim ya vencido (el dueño anterior murió sin liberar).
_CLAIMABLE = """
   AND b.expires_at > :now
   AND (b.not_before IS NULL OR b.not_before <= :now)
   AND ( b.status = 'pending'
      OR (b.status = 'claimed' AND b.claim_until IS NOT NULL
          AND b.claim_until <= :now) )
"""

# Recuperable por reconciliación: 'claimed' con claim vencido, o 'pending' heredado.
# Sin predicado sobre expires_at — es la única operación que puede ignorarlo, porque
# un huérfano expirado sigue siendo un huérfano y ``expired`` sería una mentira.
_RECONCILABLE = """
   AND ( b.status = 'pending'
      OR (b.status = 'claimed' AND b.claim_until IS NOT NULL
          AND b.claim_until <= :now) )
"""

# Barrible: vencido, sin nadie trabajando en él (fencing temporal) y sin intento
# abierto que contradiga el "nada salió" (fencing por evidencia). Los dos son
# ortogonales: un batch con claim vencido Y un intento abierto pasa el primero y muere
# en el segundo — no se barre, va a reconciliación.
_SWEEPABLE = """
   AND b.expires_at <= :now
   AND ( b.status = 'pending'
      OR (b.status = 'claimed' AND (b.claim_until IS NULL
                                    OR b.claim_until <= :now)) )
"""

_ORDER_CANDIDATES = """
 ORDER BY COALESCE(b.not_before, b.created_at), b.created_at, b.notification_id
"""

_ORDER_SWEEP = """
 ORDER BY b.expires_at, b.created_at, b.notification_id
"""

_SELECT_CANDIDATES_SQL = (
    "SELECT b.notification_id FROM radar_notification_batches b WHERE 1=1"
    + _CLAIMABLE + _NO_OPEN_DISPATCHING + _ORDER_CANDIDATES + " LIMIT :limit"
)

_SELECT_ORPHANS_SQL = (
    "SELECT b.notification_id FROM radar_notification_batches b WHERE 1=1"
    + _RECONCILABLE + _HAS_OPEN_DISPATCHING
    + " ORDER BY b.created_at, b.notification_id LIMIT :limit"
)

_SELECT_SWEEPABLE_SQL = (
    "SELECT b.notification_id FROM radar_notification_batches b WHERE 1=1"
    + _SWEEPABLE + _NO_OPEN_DISPATCHING + _ORDER_SWEEP + " LIMIT :limit"
)

# La exclusión mutua vive aquí: un UPDATE condicional cuyo rowcount decide el ganador.
# Un SELECT previo no decide nada —solo propone a quién intentar—; si el predicado
# viviera en el SELECT y el UPDATE fuera incondicional, dos claimers que leyeran el
# mismo estado escribirían los dos.
_CLAIM_SQL = (
    "UPDATE radar_notification_batches AS b "
    "   SET status = 'claimed', claim_token = :token, claim_until = :until "
    " WHERE b.notification_id = :nid"
    + _CLAIMABLE + _NO_OPEN_DISPATCHING
)

_RECONCILE_CLAIM_SQL = (
    "UPDATE radar_notification_batches AS b "
    "   SET status = 'claimed', claim_token = :token, claim_until = :until "
    " WHERE b.notification_id = :nid"
    + _RECONCILABLE + _HAS_OPEN_DISPATCHING
)

_SWEEP_SQL = (
    "UPDATE radar_notification_batches AS b "
    "   SET status = 'dead', dead_reason = 'expired', "
    "       claim_token = NULL, claim_until = NULL "
    " WHERE b.notification_id = :nid"
    + _SWEEPABLE + _NO_OPEN_DISPATCHING
)

# Toda escritura post-claim va fenced por token y vigencia: un worker cuyo claim ya
# venció no puede escribir sobre el trabajo de su sucesor aunque conserve el token.
_FENCE = (
    " WHERE notification_id = :nid AND status = 'claimed' "
    "   AND claim_token = :token AND claim_until > :now"
)

_MARK_DEAD_SQL = (
    "UPDATE radar_notification_batches "
    "   SET status = 'dead', dead_reason = :dead_reason, "
    "       claim_token = NULL, claim_until = NULL" + _FENCE
)

_MARK_PENDING_SQL = (
    "UPDATE radar_notification_batches "
    "   SET status = 'pending', claim_token = NULL, claim_until = NULL, "
    "       not_before = :not_before" + _FENCE
)

_SET_EXTERNAL_KEY_SQL = (
    "UPDATE radar_notification_batches "
    "   SET external_idempotency_key = COALESCE(external_idempotency_key, :key)"
    + _FENCE
)


class WorkerStore:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        conn = self._connect()
        try:
            ensure_schema(conn)
        finally:
            conn.close()

    def _connect(self) -> sqlite3.Connection:
        return connect_sqlite(self.db_path)

    # ── 5A. Reclamar ──────────────────────────────────────────────────────────────

    def claim_candidate(
        self,
        *,
        clock: Callable[[], datetime],
        duration: timedelta,
        notification_id: str | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> ClaimedCandidate | None:
        """Reclama un batch due, o ``None`` si no hay ninguno reclamable.

        El orden de la transacción es normativo: ``BEGIN IMMEDIATE`` primero, y solo
        entonces ``clock()``. ``claim_until`` mide desde el instante en que el worker
        *es* dueño, no desde antes de pedirlo. ``after_begin`` marca justo el punto en
        que el lock ya es nuestro, y por eso hace el orden observable sin depender de
        ganar una carrera.

        Con ``notification_id`` se apunta a un batch concreto, pero se aplican
        exactamente los mismos predicados: un claim dirigido que se saltara la
        expiración, la cuarentena o el intento abierto sería una puerta trasera al
        camino que el resto del módulo protege.
        """
        _require_duration(duration, "duration")
        token = uuid.uuid4().hex
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                now = _require_aware(clock(), "clock()")
                now_s = _stamp(now)
                until_s = _stamp(now + duration)
                target = notification_id
                if target is None:
                    row = conn.execute(
                        _SELECT_CANDIDATES_SQL, {"now": now_s, "limit": 1}
                    ).fetchone()
                    if row is None:
                        conn.execute("ROLLBACK")
                        return None
                    target = row["notification_id"]
                cur = conn.execute(
                    _CLAIM_SQL,
                    {"token": token, "until": until_s, "nid": target, "now": now_s},
                )
                if cur.rowcount != 1:
                    conn.execute("ROLLBACK")
                    return None
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return ClaimedCandidate(
            notification_id=target, claim_token=token,
            claimed_at=now_s, claim_until=until_s,
        )

    # ── 5B. Autorizar y abrir el intento ──────────────────────────────────────────

    def authorize_and_begin_dispatch(
        self,
        notification_id: str,
        claim_token: str,
        *,
        clock: Callable[[], datetime],
        hold_duration: timedelta = DEFAULT_HOLD_DURATION,
        _fault_hook: _FaultHook | None = None,
    ) -> DispatchAuthorization:
        """Revalida el consentimiento y abre el intento, todo en una transacción.

        Las cinco salidas son exhaustivas y ninguna llama a nadie:

        - ``claim_lost``: el claim ya no es nuestro. Cero cambios.
        - ``dead``: causa terminal — del consentimiento (matriz compartida) o del
          reloj (``expires_at`` vencido, §INV-W9). Sin intento, sin clave externa.
        - ``hold``: causa reversible. Vuelve a ``pending`` con cuarentena. Cubre
          tanto un hold de la matriz como un aplazamiento por quiet hours
          (``reason='quiet_hours'``): los dos son "ahora no", vuelven al mismo
          estado y se combinan por la espera más larga.
        - ``security_blocked``: el endpoint es de otra persona que la dueña del radar.
        - ``authorized``: clave externa write-once + intento ``dispatching``.

        Precedencia real (ADR-052):

            claim válido → intento ``dispatching`` abierto → matriz → expiración
            → quiet hours

        El claim va primero a propósito: un intento abierto con el claim ya vencido
        es un **huérfano**, el caso normal que ``claim_for_reconciliation`` existe
        para resolver, y responder ``claim_lost`` es lo correcto. El estado imposible
        —y por tanto el ``WorkerStoreConsistencyError``— es tener el claim *vigente*
        y aun así encontrar un intento abierto.

        La matriz va antes que el reloj porque ``dead_reason`` es el registro de por
        qué no se entregó: ``opted_out``, ``endpoint_revoked`` y ``radar_archived``
        ganan sobre ``expired`` y quedan persistidos.

        ``security_blocked`` **no** es un ``dead_reason``: es el resultado de esta
        autorización, pero el batch queda ``pending`` hasta su TTL y el sweep acaba
        registrándolo como ``expired``. Ese registro final es hoy menos verdadero de
        lo que nos gustaría; arreglarlo pide un estado persistente nuevo y migración
        de schema, y no se hace aquí.

        El COMMIT es el punto de linearización. Si una revocación commitea antes, esta
        transacción la lee y sale por ``dead``; si commitea después, el intento ya está
        autorizado. No hay tercera posibilidad, y por eso la revalidación no puede
        vivir en otra transacción.
        """
        _require_duration(hold_duration, "hold_duration")
        conn = self._connect()
        result: DispatchAuthorization | None = None
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                now = _require_aware(clock(), "clock()")
                now_s = _stamp(now)
                batch = conn.execute(
                    "SELECT endpoint_id, radar_id, status, claim_token, claim_until, "
                    "       expires_at, payload_json, external_idempotency_key "
                    "  FROM radar_notification_batches WHERE notification_id = ?",
                    (notification_id,),
                ).fetchone()
                if (batch is None or batch["status"] != "claimed"
                        or batch["claim_token"] != claim_token
                        or batch["claim_until"] is None
                        or batch["claim_until"] <= now_s):
                    conn.execute("ROLLBACK")
                    return DispatchAuthorization(
                        outcome=OUTCOME_CLAIM_LOST,
                        notification_id=notification_id,
                        reason="claim_lost",
                    )
                _fire(_fault_hook, PHASE_AFTER_CLAIM_VALIDATION)

                # Esta es la puerta que crea intentos, así que es aquí donde hay que
                # impedir el segundo. Tener el claim vigente no basta: el mismo dueño
                # puede llamar dos veces (un reintento de orquestación, un bug), y
                # ``UNIQUE(notification_id, attempt_number)`` no lo detiene porque el
                # número simplemente avanza. ``claim_candidate`` excluye los batches
                # con intento abierto, pero nadie revalidaba al abrirlo.
                #
                # Va **antes** del chequeo de expiración a propósito: si el batch
                # venció y además tiene un intento abierto, marcarlo ``expired``
                # afirmaría "nada salió" sobre un mensaje que pudo entregarse y
                # dejaría el intento huérfano sobre un batch terminal, donde la
                # reconciliación ya no lo alcanza (INV-W16). Falla cerrado y el batch
                # va a reconciliación cuando su claim venza.
                open_dispatching = conn.execute(
                    "SELECT COUNT(*) AS n FROM radar_delivery_attempts "
                    "WHERE notification_id = ? AND phase = 'dispatching'",
                    (notification_id,),
                ).fetchone()["n"]
                if open_dispatching:
                    raise WorkerStoreConsistencyError(
                        f"El batch '{notification_id}' ya tiene {open_dispatching} "
                        f"intento(s) 'dispatching' abierto(s): abrir otro sería una "
                        f"segunda llamada al proveedor por el mismo mensaje."
                    )

                fence = {"nid": notification_id, "token": claim_token, "now": now_s}
                # **ADR-052.** La matriz primero, el reloj después. Ambas causas son
                # terminales y ninguna envía, así que la precedencia solo elige la
                # etiqueta —pero la etiqueta *es* el registro: ``dead_reason`` es lo
                # único que queda explicando por qué no se entregó. Marcar ``expired``
                # el batch de alguien que se dio de baja afirma que el mensaje caducó,
                # cuando la verdad es que esa persona ya no lo quería. Cuesta un join
                # dentro del lock en la rama de vencidos; el registro honesto lo vale.
                row = authorization.fetch_target_row(
                    conn, batch["endpoint_id"], batch["radar_id"]
                )
                decision = authorization.authorize_row(
                    row, notification_id=notification_id
                )
                if (decision.category == authorization.AUTHORIZED
                        and batch["expires_at"] <= now_s):
                    # **INV-W9.** Solo aquí: el consentimiento está intacto y lo único
                    # que falló fue el reloj. La ventana es estrecha
                    # —``claim_candidate`` exige ``expires_at > now``— pero existe, y
                    # es justo la que un proveedor lento o un lock disputado abren.
                    decision = _EXPIRED
                _fire(_fault_hook, PHASE_AFTER_AUTHORIZATION_READ)

                if decision.category == authorization.TERMINAL:
                    self._fenced(
                        conn, _MARK_DEAD_SQL,
                        {**fence, "dead_reason": decision.dead_reason},
                        notification_id,
                    )
                    result = DispatchAuthorization(
                        outcome=OUTCOME_DEAD, notification_id=notification_id,
                        reason=decision.reason, dead_reason=decision.dead_reason,
                    )
                elif decision.category == authorization.HOLD:
                    # Dos esperas simultáneas se combinan con la **más larga**: la
                    # corta no puede liberar a la otra. Reevaluar un principal
                    # suspendido a las 22:25 porque el hold dura 15 minutos volvería a
                    # chocar con el silencio; y salir del silencio a las 07:00 no
                    # resucita el consentimiento. Se espera a que ambas terminen.
                    #
                    # Nunca prometer una espera posterior a la expiración: sería
                    # programar una reevaluación que jamás va a ocurrir y dejar el
                    # batch pareciendo "en espera" cuando en realidad ya murió. Si la
                    # espera desborda el TTL, se duerme hasta el TTL y decide el sweep.
                    hold_until = _stamp(now + hold_duration)
                    quiet_end = self._quiet_end(row, now, notification_id)
                    candidate = max(hold_until, quiet_end) if quiet_end else hold_until
                    not_before = min(candidate, batch["expires_at"])
                    self._fenced(
                        conn, _MARK_PENDING_SQL, {**fence, "not_before": not_before},
                        notification_id,
                    )
                    result = DispatchAuthorization(
                        outcome=OUTCOME_HOLD, notification_id=notification_id,
                        reason=decision.reason, not_before=not_before,
                    )
                elif decision.category == authorization.SECURITY_BLOCKED:
                    # La condición no se resuelve sola: reevaluarla cada quince minutos
                    # sería un hot loop sobre un fallo de aislamiento. Una evaluación,
                    # y que muera por TTL.
                    self._fenced(
                        conn, _MARK_PENDING_SQL,
                        {**fence, "not_before": batch["expires_at"]},
                        notification_id,
                    )
                    result = DispatchAuthorization(
                        outcome=OUTCOME_SECURITY_BLOCKED,
                        notification_id=notification_id, reason=decision.reason,
                        not_before=batch["expires_at"],
                    )
                elif decision.category == authorization.AUTHORIZED:
                    # Lo último antes de abrir el intento, y la única razón por la que
                    # este bloque existe: el batch pudo crearse a las 21:00, fuera de
                    # silencio, y llegar aquí a las 22:30. El ``not_before`` que congeló
                    # el selector era una foto de entonces; quien decide es este
                    # instante, ya bajo el lock.
                    quiet_end = self._quiet_end(row, now, notification_id)
                    if quiet_end is not None:
                        not_before = min(quiet_end, batch["expires_at"])
                        self._fenced(
                            conn, _MARK_PENDING_SQL,
                            {**fence, "not_before": not_before}, notification_id,
                        )
                        result = DispatchAuthorization(
                            outcome=OUTCOME_HOLD, notification_id=notification_id,
                            reason=REASON_QUIET_HOURS, not_before=not_before,
                        )
                    else:
                        result = self._authorize(
                            conn, batch, row, fence,
                            notification_id=notification_id, now_s=now_s,
                            _fault_hook=_fault_hook,
                        )
                else:
                    # Autorizar es la única rama que llama al proveedor, así que no
                    # puede ser el caso por defecto. Con un `else` genérico, una
                    # categoría nueva —o un typo— se convertiría en permiso de envío:
                    # el fallo silencioso más caro posible. Se pide explícitamente.
                    raise WorkerStoreConsistencyError(
                        f"Categoría de autorización desconocida: "
                        f"{decision.category!r}. No se autoriza nada que la matriz no "
                        f"haya aprobado con {authorization.AUTHORIZED!r}."
                    )

                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        _fire(_fault_hook, PHASE_AFTER_COMMIT)
        assert result is not None
        return result

    def _quiet_end(
        self, row: sqlite3.Row | None, now: datetime, notification_id: str
    ) -> str | None:
        """Fin del silencio en UTC, o ``None`` si se puede entregar ahora.

        La ventana sale de la preferencia de la persona —nunca del reloj del sistema
        operativo del servidor, que puede estar en otro continente—. Una configuración
        ilegible falla cerrado: caer a UTC o a "sin silencio" convertiría un error de
        datos en una llamada a las tres de la mañana.
        """
        if row is None:
            return None
        try:
            return _quiet_hours_not_before(
                now,
                start=row["quiet_hours_start"],
                end=row["quiet_hours_end"],
                timezone_name=row["timezone"],
            )
        except QuietHoursError as exc:
            raise WorkerStoreError(
                f"El batch '{notification_id}' tiene quiet hours que no se pueden "
                f"interpretar: {exc}. No se abre intento: entregar bajo una ventana "
                f"desconocida es despertar a quien pidió silencio."
            ) from exc

    def _fenced(
        self, conn: sqlite3.Connection, sql: str, params: dict, notification_id: str,
    ) -> None:
        cur = conn.execute(sql, params)
        if cur.rowcount != 1:
            # El claim se validó al principio de esta misma transacción y nadie más
            # puede escribir mientras la tenemos: llegar aquí significa que el estado
            # cambió bajo un lock exclusivo, que es imposible.
            raise WorkerStoreConsistencyError(
                f"La escritura fenced sobre '{notification_id}' no afectó "
                f"exactamente una fila dentro de su propia transacción."
            )

    def _authorize(
        self,
        conn: sqlite3.Connection,
        batch: sqlite3.Row,
        row: sqlite3.Row,
        fence: dict,
        *,
        notification_id: str,
        now_s: str,
        _fault_hook: _FaultHook | None,
    ) -> DispatchAuthorization:
        key = external_idempotency_key(notification_id)
        self._fenced(conn, _SET_EXTERNAL_KEY_SQL, {**fence, "key": key}, notification_id)
        stored = conn.execute(
            "SELECT external_idempotency_key FROM radar_notification_batches "
            "WHERE notification_id = ?",
            (notification_id,),
        ).fetchone()["external_idempotency_key"]
        if stored != key:
            # El COALESCE conservó una clave anterior distinta de la que la fórmula
            # produce hoy. O la fórmula cambió sin versionarse, o alguien la editó a
            # mano: en ambos casos enviar con una clave que no es la nuestra es peor
            # que no enviar.
            raise WorkerStoreConsistencyError(
                f"La external_idempotency_key almacenada de '{notification_id}' no "
                f"coincide con la calculada: la clave es write-once y no puede "
                f"haber cambiado."
            )
        _fire(_fault_hook, PHASE_AFTER_EXTERNAL_KEY)

        def bridge(phase: str) -> None:
            if phase == _LEDGER_AFTER_ATTEMPT_COUNT:
                _fire(_fault_hook, PHASE_AFTER_ATTEMPT_COUNT)
            elif phase == _LEDGER_AFTER_ATTEMPT_INSERT:
                _fire(_fault_hook, PHASE_AFTER_ATTEMPT_INSERT)

        attempt_id = _insert_dispatching_attempt_on_connection(
            conn, notification_id=notification_id, started_at=now_s,
            _fault_hook=bridge,
        )
        snapshot = authorization.snapshot_from_row(row, notification_id=notification_id)
        return DispatchAuthorization(
            outcome=OUTCOME_AUTHORIZED,
            notification_id=notification_id,
            claim_token=fence["token"],
            attempt_id=attempt_id,
            channel=snapshot.channel,
            external_recipient_id=snapshot.external_recipient_id,
            external_idempotency_key=key,
            payload_json=batch["payload_json"],
        )

    # ── 5C. Reconciliar huérfanos ─────────────────────────────────────────────────

    def claim_for_reconciliation(
        self,
        *,
        clock: Callable[[], datetime],
        duration: timedelta,
        notification_id: str | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> ReconciliationClaim | None:
        """Toma el claim de un huérfano para poder cerrarlo, o ``None``.

        Un huérfano es un batch con un intento ``dispatching`` abierto cuyo claim no
        está vigente, en dos formas: ``claimed`` vencido, o ``pending`` (heredado de
        un ``release`` indebido, una versión anterior o una edición manual). Ignora
        ``expires_at`` a propósito: es la única operación que puede, porque un
        huérfano expirado sigue mereciendo un veredicto honesto en vez de ``expired``.

        Renueva el claim en vez de escribir ``ambiguous`` directamente para que quien
        cierre sea ``LedgerStore.finish_attempt``, que cierra intento **y** batch en
        una transacción ya probada. Un UPDATE directo dejaría el ``dispatching``
        abierto para siempre.
        """
        _require_duration(duration, "duration")
        token = uuid.uuid4().hex
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                _fire(_fault_hook, PHASE_AFTER_BEGIN)
                now = _require_aware(clock(), "clock()")
                now_s = _stamp(now)
                until_s = _stamp(now + duration)
                target = notification_id
                if target is None:
                    row = conn.execute(
                        _SELECT_ORPHANS_SQL, {"now": now_s, "limit": 1}
                    ).fetchone()
                    if row is None:
                        conn.execute("ROLLBACK")
                        return None
                    target = row["notification_id"]

                open_attempts = conn.execute(
                    "SELECT attempt_id FROM radar_delivery_attempts "
                    "WHERE notification_id = ? AND phase = 'dispatching' "
                    "ORDER BY attempt_number",
                    (target,),
                ).fetchall()
                if len(open_attempts) > 1:
                    # Escoger "el último" inventaría una respuesta: cada intento
                    # abierto puede haber llegado al proveedor, y cerrar uno dejaría
                    # el otro afirmando en silencio que sigue en vuelo. El ROLLBACK lo
                    # hace el manejador de abajo: hacerlo también aquí dejaría la
                    # transacción ya cerrada y el segundo intento reventaría con un
                    # OperationalError que taparía esta señal.
                    raise WorkerStoreConsistencyError(
                        f"El batch '{target}' tiene {len(open_attempts)} intentos "
                        f"'dispatching' abiertos. No se reconcilia ninguno: el estado "
                        f"no dice cuál representa la llamada real."
                    )
                if not open_attempts:
                    conn.execute("ROLLBACK")
                    return None

                cur = conn.execute(
                    _RECONCILE_CLAIM_SQL,
                    {"token": token, "until": until_s, "nid": target, "now": now_s},
                )
                if cur.rowcount != 1:
                    conn.execute("ROLLBACK")
                    return None
                attempt_id = open_attempts[0]["attempt_id"]
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return ReconciliationClaim(
            notification_id=target, attempt_id=attempt_id,
            claim_token=token, claim_until=until_s,
        )

    # ── 5D. Barrer expirados ──────────────────────────────────────────────────────

    def sweep_expired(
        self,
        *,
        clock: Callable[[], datetime],
        limit: int = 100,
        _fault_hook: _FaultHook | None = None,
    ) -> list[str]:
        """Marca ``dead``/``expired`` lo vencido que **puede** decir "nada salió".

        Nunca toca un batch con claim vigente (alguien trabaja en él) ni con un
        intento ``dispatching`` abierto (alguien ya trabajó y no sabemos cómo acabó).
        La segunda cláusula es la que impide la peor mentira del sistema: afirmar con
        certeza que un mensaje no se envió cuando puede estar entregado.

        Devuelve los ids afectados, en orden determinista y acotado por ``limit``.
        """
        _require_limit(limit, "limit")
        conn = self._connect()
        swept: list[str] = []
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                now = _require_aware(clock(), "clock()")
                now_s = _stamp(now)
                rows = conn.execute(
                    _SELECT_SWEEPABLE_SQL, {"now": now_s, "limit": limit}
                ).fetchall()
                for row in rows:
                    nid = row["notification_id"]
                    cur = conn.execute(_SWEEP_SQL, {"nid": nid, "now": now_s})
                    if cur.rowcount != 1:
                        raise WorkerStoreConsistencyError(
                            f"El batch '{nid}' dejó de ser barrible dentro de la "
                            f"transacción que lo seleccionó."
                        )
                    swept.append(nid)
                _fire(_fault_hook, PHASE_BEFORE_COMMIT)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
        return swept

    # ── 5E. Lecturas ──────────────────────────────────────────────────────────────

    def list_candidates(self, *, now: datetime, limit: int = 100) -> list[str]:
        """Batches que ``claim_candidate`` aceptaría, en su mismo orden.

        Reproduce el predicado del claim: proponer un candidato que el claim rechaza
        gastaría un intento de claim en un batch imposible. **No es exclusión mutua**
        —decide el UPDATE—, así que una foto ligeramente vieja de ``now`` solo hace el
        filtro más estricto.
        """
        return self._list(_SELECT_CANDIDATES_SQL, now=now, limit=limit)

    def list_orphans(self, *, now: datetime, limit: int = 100) -> list[str]:
        """Batches con intento abierto y claim no vigente: ``pending`` o ``claimed``
        vencido, expirados o no. Nunca uno con claim vigente: ese no es huérfano, es
        trabajo en curso."""
        return self._list(_SELECT_ORPHANS_SQL, now=now, limit=limit)

    def list_sweepable(self, *, now: datetime, limit: int = 100) -> list[str]:
        """Lo que ``sweep_expired`` marcaría. Lectura pura: no escribe."""
        return self._list(_SELECT_SWEEPABLE_SQL, now=now, limit=limit)

    def _list(self, sql: str, *, now: datetime, limit: int) -> list[str]:
        _require_aware(now, "now")
        _require_limit(limit, "limit")
        conn = self._connect()
        try:
            rows = conn.execute(sql, {"now": _stamp(now), "limit": limit}).fetchall()
        finally:
            conn.close()
        return [row["notification_id"] for row in rows]

    def count_open_dispatching(self, notification_id: str) -> int:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM radar_delivery_attempts "
                "WHERE notification_id = ? AND phase = 'dispatching'",
                (notification_id,),
            ).fetchone()
        finally:
            conn.close()
        return int(row["n"])


__all__ = [
    "DEFAULT_HOLD_DURATION",
    "EXTERNAL_IDEMPOTENCY_VERSION",
    "OUTCOME_AUTHORIZED",
    "OUTCOME_CLAIM_LOST",
    "OUTCOME_DEAD",
    "OUTCOME_HOLD",
    "OUTCOME_SECURITY_BLOCKED",
    "PHASE_AFTER_ATTEMPT_COUNT",
    "PHASE_AFTER_ATTEMPT_INSERT",
    "PHASE_AFTER_AUTHORIZATION_READ",
    "PHASE_AFTER_BEGIN",
    "PHASE_AFTER_CLAIM_VALIDATION",
    "PHASE_AFTER_COMMIT",
    "PHASE_AFTER_EXTERNAL_KEY",
    "PHASE_BEFORE_COMMIT",
    "ClaimedCandidate",
    "DispatchAuthorization",
    "ReconciliationClaim",
    "WorkerStore",
    "WorkerStoreConsistencyError",
    "WorkerStoreError",
    "external_idempotency_key",
]
