"""worker.py — el ciclo de entrega, de principio a fin (P1B-4, R2C).

Orquesta lo que otros módulos ya garantizan y **no reimplementa ninguno**: el SQL
transaccional es de ``worker_store``, el cierre atómico de intento+batch es de
``ledger_store``, el backoff es de ``retry``, el texto es de ``render`` y la red es
del proveedor. Lo único que este módulo aporta es el **orden**, y el orden es la
política.

Tres decisiones lo gobiernan:

1. **Reconciliar antes de barrer.** Un huérfano expirado cumple las dos condiciones a
   la vez. Si el sweep fuera primero, lo marcaría ``expired`` —"nada salió"— sobre un
   mensaje que pudo estar entregado. ``sweep_expired`` ya se niega a tocar batches con
   un intento abierto, así que el orden no es la única defensa; es la primera.
2. **Un intento abierto nunca se reenvía.** La reconciliación lo cierra como
   ``ambiguous`` sin preguntarle nada al proveedor. No sabemos si el mensaje salió, y
   la única acción que no puede duplicarlo es no volver a llamar.
3. **Lo desconocido pesa más que lo cómodo.** Un timeout y una excepción inesperada
   se tratan como posible efecto externo, que es la respuesta cara: el batch queda
   ``ambiguous`` y nadie lo reintenta solo. Tratarlos como reintentables abarataría el
   caso normal a costa de duplicar mensajes en el caso malo.

El worker no abre intentos por ``LedgerStore.begin_attempt``: la única puerta es
``WorkerStore.authorize_and_begin_dispatch``, porque es la que revalida el
consentimiento bajo el mismo lock que abre el intento. Autorizar en una transacción y
abrir en otra dejaría una ventana donde una revocación se pierde.
"""
from __future__ import annotations

import json
import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.notify import retry as retry_policy_module
from app.core.notify.ledger_store import LedgerStore
from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    NO_EXTERNAL_EFFECT,
    POSSIBLE_EXTERNAL_EFFECT,
    NotificationProvider,
    ProviderRequest,
    ProviderResult,
)
from app.core.notify.render import render_payload
from app.core.notify.retry import (
    DEFAULT_CLAIM_DURATION,
    DEFAULT_RECONCILIATION_CLAIM_DURATION,
    DEFAULT_RETRY_POLICY,
    RetryPolicy,
)
from app.core.notify.worker_store import (
    OUTCOME_AUTHORIZED,
    OUTCOME_CLAIM_LOST,
    OUTCOME_DEAD,
    OUTCOME_HOLD,
    OUTCOME_SECURITY_BLOCKED,
    DispatchAuthorization,
    WorkerStore,
)

DEFAULT_DISPATCH_LIMIT = 50
DEFAULT_RECONCILE_LIMIT = 50

# Categorías estables y sin PII. Son lo único que se persiste sobre un fallo: el
# mensaje crudo de una excepción de un SDK cita la URL con el token, el número de
# teléfono y a veces el cuerpo del mensaje.
SAFE_UNCLASSIFIED_EXCEPTION = "unclassified_provider_exception"
SAFE_RENDER_ERROR = "payload_render_failed"
SAFE_PROVIDER_UNAVAILABLE = "no_provider_registered_for_channel"
SAFE_ORPHAN_RECONCILED = "orphaned_attempt_unknown_outcome"

ERROR_CLASS_RENDER = "render_error"
ERROR_CLASS_PROVIDER_UNAVAILABLE = "provider_unavailable"
ERROR_CLASS_PROVIDER_ERROR = "provider_error"

DEAD_REASON_RENDER_ERROR = "render_error"
DEAD_REASON_RETRIES_EXHAUSTED = "retries_exhausted"
DEAD_REASON_PERMANENT = "permanent_provider_error"

# Fases de inyección de fallo. La primera es el caso central del bloque: si el proceso
# muere aquí, el proveedor ya pudo haber entregado y el ledger todavía no lo sabe.
PHASE_BEFORE_PROVIDER_SEND = "before_provider_send"
PHASE_AFTER_PROVIDER_SEND = "after_provider_send"

_FaultHook = Callable[[str], None]


def _fire(hook: _FaultHook | None, phase: str) -> None:
    if hook is not None:
        hook(phase)


@dataclass(frozen=True)
class WorkerRunReport:
    """Qué hizo una corrida. Contadores y nada más.

    No lleva ``recipient``, ni ``claim_token``, ni payload, ni el texto renderizado, y
    no es un descuido: este objeto se loguea entero al final de cada corrida. Un
    campo con el id del destinatario aquí sería el sitio más fácil del sistema por el
    que filtrar a quién se le escribe.

    ``sent + retried + dead + ambiguous`` no tiene por qué igualar ``authorized``: una
    corrida puede morir a medias y el reporte cuenta lo que ocurrió, no lo que debía
    ocurrir.
    """

    reconciled: int = 0
    swept: int = 0
    claimed: int = 0
    authorized: int = 0
    sent: int = 0
    retried: int = 0
    dead: int = 0
    ambiguous: int = 0
    held: int = 0
    security_blocked: int = 0
    claim_lost: int = 0
    provider_calls: int = 0


class _Tally:
    """Acumulador mutable. El reporte que sale es inmutable; contar sobre un frozen
    dataclass obligaría a reconstruirlo doce veces por corrida."""

    def __init__(self) -> None:
        self._counts: dict[str, int] = {}

    def bump(self, name: str, amount: int = 1) -> None:
        self._counts[name] = self._counts.get(name, 0) + amount

    def freeze(self) -> WorkerRunReport:
        return WorkerRunReport(**self._counts)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class NotificationWorker:
    """Una corrida completa: reconciliar → barrer → despachar.

    ``providers`` es un mapping de canal a proveedor. Un canal ausente **no** es un
    error de programación: es configuración que falta, y su respuesta es un reintento
    seguro (``provider_unavailable``), no una excepción que tumbe la corrida entera y
    deje sin entregar los canales que sí están.

    ``clock`` y ``rng`` se inyectan para que las pruebas sean deterministas sin dormir
    ni parchear módulos. El reloj lo consume ``worker_store`` **dentro** de su lock;
    aquí solo se pasa.
    """

    def __init__(
        self,
        db_path: str | Path,
        providers: Mapping[str, NotificationProvider],
        *,
        clock: Callable[[], datetime] = _utc_now,
        rng: Callable[[], float] = random.random,
        policy: RetryPolicy = DEFAULT_RETRY_POLICY,
        claim_duration: timedelta = DEFAULT_CLAIM_DURATION,
        reconciliation_duration: timedelta = DEFAULT_RECONCILIATION_CLAIM_DURATION,
    ) -> None:
        self.db_path = Path(db_path)
        self.providers = dict(providers)
        self.clock = clock
        self.rng = rng
        self.policy = policy
        self.claim_duration = claim_duration
        self.reconciliation_duration = reconciliation_duration
        self.store = WorkerStore(db_path)
        self.ledger = LedgerStore(db_path)

    # ── Corrida ───────────────────────────────────────────────────────────────────

    def run_once(
        self,
        *,
        dispatch_limit: int = DEFAULT_DISPATCH_LIMIT,
        reconcile_limit: int = DEFAULT_RECONCILE_LIMIT,
        sweep_limit: int = 100,
        notification_id: str | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> WorkerRunReport:
        """Reconcilia, barre y despacha. En ese orden, que es normativo.

        No lanza por no encontrar trabajo: una corrida sobre una base vacía devuelve
        un reporte en cero. El worker se ejecuta en bucle, y una excepción por "nada
        que hacer" convertiría el estado normal en una alarma.

        El ``int`` ``0`` en cualquiera de los tres límites significa "omití esta
        fase por completo" (así lo usa el piloto manual de Telegram). En
        reconciliación y dispatch eso ya ocurre solo -``range(0)`` no itera-, pero
        el barrido llama a ``sweep_expired`` una sola vez fuera de todo bucle, así
        que necesita el guard explícito. Se trata como omisión **solo** el int 0
        exacto: ``-1``, ``0.0`` y ``False`` siguen llegando a ``sweep_expired``
        para que ``WorkerStore`` los rechace como el error de programación que son,
        sin debilitar esa validación (su guard es la única defensa contra
        ``limit=True`` barriendo un batch en silencio).

        ``notification_id`` convierte la corrida en un **dispatch dirigido**: solo se
        intenta reclamar ese batch (vía ``claim_candidate(notification_id=...)``, con
        los mismos predicados de siempre), y si no es reclamable no se cae a ningún
        otro. Un dispatch dirigido exige ``dispatch_limit == 1``: apuntar a un ID y a
        la vez pedir varios claims es una contradicción —el segundo claim ya no
        tendría objetivo—, así que se rechaza en vez de adivinar.
        """
        if notification_id is not None and not (
            type(dispatch_limit) is int and dispatch_limit == 1
        ):
            # Igualdad estricta de tipo: en Python ``True == 1`` y ``1.0 == 1``, así
            # que un ``!=`` simple dejaría pasar un ``bool``/``float`` disfrazado de
            # "exactamente uno". El mismo criterio que ``sweep_limit`` ya aplica más
            # arriba para el mismo motivo.
            raise ValueError(
                "un dispatch dirigido (notification_id) exige dispatch_limit == 1"
            )
        tally = _Tally()
        self._reconcile(tally, limit=reconcile_limit)
        if not (type(sweep_limit) is int and sweep_limit == 0):
            tally.bump("swept", len(self.store.sweep_expired(
                clock=self.clock, limit=sweep_limit
            )))
        self._dispatch(
            tally, limit=dispatch_limit, notification_id=notification_id,
            _fault_hook=_fault_hook,
        )
        return tally.freeze()

    # ── 1. Reconciliación ─────────────────────────────────────────────────────────

    def _reconcile(self, tally: _Tally, *, limit: int) -> None:
        """Cierra como ``ambiguous`` todo intento huérfano. Sin llamar a nadie.

        Un intento abierto significa "alguien iba a llamar al proveedor y no sabemos
        cómo acabó". Preguntarle al proveedor si llegó sería lo natural, y es
        exactamente lo que no se puede hacer: no todos los canales tienen esa consulta,
        y ``send`` no es idempotente por sí solo. La única acción que no puede duplicar
        un mensaje es no volver a llamar.
        """
        for _ in range(limit):
            claim = self.store.claim_for_reconciliation(
                clock=self.clock, duration=self.reconciliation_duration
            )
            if claim is None:
                return
            self.ledger.finish_attempt(
                claim.attempt_id,
                claim.notification_id,
                claim.claim_token,
                outcome="ambiguous",
                error_message_safe=SAFE_ORPHAN_RECONCILED,
                now=self.clock(),
            )
            tally.bump("reconciled")
            tally.bump("ambiguous")

    # ── 3. Dispatch ───────────────────────────────────────────────────────────────

    def _dispatch(
        self, tally: _Tally, *, limit: int,
        notification_id: str | None = None,
        _fault_hook: _FaultHook | None = None,
    ) -> None:
        for _ in range(limit):
            candidate = self.store.claim_candidate(
                clock=self.clock, duration=self.claim_duration,
                notification_id=notification_id,
            )
            if candidate is None:
                # Dirigido o no, un claim vacío termina la fase: el dispatch
                # dirigido no cae a otro batch porque ``claim_candidate`` solo
                # devuelve el objetivo (o nada), nunca un sustituto.
                return
            tally.bump("claimed")
            authorization = self.store.authorize_and_begin_dispatch(
                candidate.notification_id, candidate.claim_token, clock=self.clock
            )
            self._on_authorization(tally, authorization, _fault_hook=_fault_hook)

    def _on_authorization(
        self,
        tally: _Tally,
        authorization: DispatchAuthorization,
        *,
        _fault_hook: _FaultHook | None,
    ) -> None:
        """Las cinco salidas son exhaustivas y solo una llama al proveedor.

        Sin rama por defecto a propósito: si ``worker_store`` añadiera una salida
        nueva, un ``else`` genérico la convertiría en silencio en "no pasó nada" o
        —peor— en permiso de envío. Se pide explícitamente.
        """
        outcome = authorization.outcome
        if outcome == OUTCOME_CLAIM_LOST:
            tally.bump("claim_lost")
        elif outcome == OUTCOME_DEAD:
            tally.bump("dead")
        elif outcome == OUTCOME_HOLD:
            tally.bump("held")
        elif outcome == OUTCOME_SECURITY_BLOCKED:
            tally.bump("security_blocked")
        elif outcome == OUTCOME_AUTHORIZED:
            tally.bump("authorized")
            self._deliver(tally, authorization, _fault_hook=_fault_hook)
        else:
            raise AssertionError(
                f"Salida de autorización desconocida: {outcome!r}. El worker no "
                f"inventa qué hacer con ella."
            )

    # ── 4-7. Entrega ──────────────────────────────────────────────────────────────

    def _deliver(
        self,
        tally: _Tally,
        authorization: DispatchAuthorization,
        *,
        _fault_hook: _FaultHook | None,
    ) -> None:
        rendered = self._render(authorization)
        if rendered is None:
            # El payload congelado no se pudo leer. El fallo es nuestro y ocurrió antes
            # de cualquier llamada: cero efecto externo, y ningún reintento lo va a
            # arreglar porque el payload no va a cambiar solo.
            self._finish(
                tally, authorization, outcome="dead",
                dead_reason=DEAD_REASON_RENDER_ERROR,
                error_class=ERROR_CLASS_RENDER,
                error_message_safe=SAFE_RENDER_ERROR,
            )
            return
        text, payload_version = rendered

        provider = self.providers.get(authorization.channel)
        if provider is None:
            # Configuración que falta, no dato corrupto: registrar el proveedor lo
            # resuelve, así que es reintentable y no mata el batch.
            self._on_result(
                tally, authorization,
                ProviderResult(
                    effect=NO_EXTERNAL_EFFECT, retryable=True,
                    error_class=ERROR_CLASS_PROVIDER_UNAVAILABLE,
                    error_message_safe=SAFE_PROVIDER_UNAVAILABLE,
                ),
            )
            return

        request = ProviderRequest(
            notification_id=authorization.notification_id,
            attempt_id=authorization.attempt_id,
            channel=authorization.channel,
            external_idempotency_key=authorization.external_idempotency_key,
            external_recipient_id=authorization.external_recipient_id,
            text=text,
            payload_version=payload_version,
        )

        _fire(_fault_hook, PHASE_BEFORE_PROVIDER_SEND)
        tally.bump("provider_calls")
        try:
            result = provider.send(request)
        except Exception:
            # **El caso caro.** La excepción no dice si el mensaje salió antes o
            # después de reventar, y no hay forma de averiguarlo desde aquí. Se asume
            # lo peor: posible efecto externo, batch ``ambiguous``, sin reintento
            # automático. Del mensaje crudo no se persiste nada — trae la dirección y
            # el texto—; solo una categoría estable.
            self._on_result(
                tally, authorization,
                ProviderResult(
                    effect=POSSIBLE_EXTERNAL_EFFECT, retryable=False,
                    error_class=ERROR_CLASS_PROVIDER_ERROR,
                    error_message_safe=SAFE_UNCLASSIFIED_EXCEPTION,
                ),
            )
            return
        # Punto crítico: el proveedor ya respondió y el ledger todavía no lo sabe. Si
        # el proceso muere aquí, el intento queda ``dispatching`` y la siguiente
        # corrida lo reconcilia como ``ambiguous`` sin volver a llamar.
        _fire(_fault_hook, PHASE_AFTER_PROVIDER_SEND)
        self._on_result(tally, authorization, result)

    def _render(
        self, authorization: DispatchAuthorization
    ) -> tuple[str, str] | None:
        """``(texto, versión)`` o ``None`` si el payload congelado no se deja leer.

        El canal se **confirma**, no se sustituye: ``render_payload`` recibe el canal
        del endpoint y revienta si no coincide con el congelado en el payload. Pasar el
        del endpoint como si fuera el bueno dejaría que un payload armado para email
        —12.000 bytes de presupuesto— saliera por Telegram.
        """
        try:
            payload = json.loads(authorization.payload_json)
            if not isinstance(payload, dict):
                return None
            message = render_payload(payload, channel=authorization.channel)
            version = payload.get("version")
            if not isinstance(version, str):
                return None
            return message.text, version
        except (json.JSONDecodeError, ValueError, TypeError, KeyError):
            # Se capturan las clases, no ``Exception``: un ``MemoryError`` o un
            # ``KeyboardInterrupt`` no son "el payload está corrupto", y tratarlos como
            # tal mataría un batch sano por un problema del proceso.
            return None

    def _on_result(
        self,
        tally: _Tally,
        authorization: DispatchAuthorization,
        result: ProviderResult,
    ) -> None:
        """La matriz efecto → ledger. Es el corazón del bloque.

        No hay rama por defecto ni ``retryable`` consultado fuera de
        ``no_external_effect``: el contrato ya garantiza que los otros dos efectos no
        son reintentables, y volver a mirarlo aquí sugeriría que podría no serlo.
        """
        if result.effect == CONFIRMED_EXTERNAL_EFFECT:
            self._finish(
                tally, authorization, outcome="sent",
                provider_message_id=result.provider_message_id,
                provider_status="sent",
                error_message_safe=result.error_message_safe,
            )
        elif result.effect == POSSIBLE_EXTERNAL_EFFECT:
            self._finish(
                tally, authorization, outcome="ambiguous",
                provider_message_id=result.provider_message_id,
                error_class=result.error_class,
                error_message_safe=result.error_message_safe,
            )
        elif result.effect == NO_EXTERNAL_EFFECT and result.retryable:
            self._retry_or_exhaust(tally, authorization, result)
        elif result.effect == NO_EXTERNAL_EFFECT:
            self._finish(
                tally, authorization, outcome="dead",
                dead_reason=DEAD_REASON_PERMANENT,
                error_class=result.error_class,
                error_message_safe=result.error_message_safe,
            )
        else:
            raise AssertionError(
                f"Efecto desconocido: {result.effect!r}. No se decide nada sobre un "
                f"efecto que el contrato no define."
            )

    def _retry_or_exhaust(
        self,
        tally: _Tally,
        authorization: DispatchAuthorization,
        result: ProviderResult,
    ) -> None:
        """Reintenta con el backoff de ``retry.py``, o muere si se agotó la política.

        ``attempt_number`` se lee del ledger, que es quien lo cuenta: derivarlo aquí
        de un contador propio produciría dos verdades sobre cuántas veces se ha
        llamado al proveedor, y la del ledger es la que sobrevive a un reinicio.
        """
        now = self.clock()
        attempt = self.ledger.get_attempt(authorization.attempt_id)
        attempt_number = attempt.attempt_number if attempt else 1
        if not retry_policy_module.should_retry(
            attempt_number=attempt_number, policy=self.policy
        ):
            self._finish(
                tally, authorization, outcome="dead",
                dead_reason=DEAD_REASON_RETRIES_EXHAUSTED,
                error_class=result.error_class,
                error_message_safe=result.error_message_safe,
            )
            return
        when = retry_policy_module.next_not_before(
            attempt_number=attempt_number,
            now=now,
            policy=self.policy,
            retry_after=result.retry_after,
            jitter=self.rng(),
        )
        # ``retry_after`` se persiste en segundos —así lo define la columna, y así lo
        # expresa el header del que sale—, medidos contra el instante de esta
        # decisión. Guardar el instante absoluto en una columna INTEGER lo convertiría
        # en un epoch que nadie más interpreta igual.
        delta = result.retry_after - now if result.retry_after else None
        self._finish(
            tally, authorization, outcome="retry",
            next_not_before=when.astimezone(timezone.utc).isoformat(),
            error_class=result.error_class,
            error_message_safe=result.error_message_safe,
            retry_after=int(delta.total_seconds()) if delta else None,
        )

    def _finish(
        self,
        tally: _Tally,
        authorization: DispatchAuthorization,
        *,
        outcome: str,
        **fields,
    ) -> None:
        self.ledger.finish_attempt(
            authorization.attempt_id,
            authorization.notification_id,
            authorization.claim_token,
            outcome=outcome,
            now=self.clock(),
            **fields,
        )
        tally.bump({"retry": "retried"}.get(outcome, outcome))


__all__ = [
    "DEFAULT_DISPATCH_LIMIT",
    "DEFAULT_RECONCILE_LIMIT",
    "PHASE_AFTER_PROVIDER_SEND",
    "PHASE_BEFORE_PROVIDER_SEND",
    "SAFE_ORPHAN_RECONCILED",
    "SAFE_PROVIDER_UNAVAILABLE",
    "SAFE_RENDER_ERROR",
    "SAFE_UNCLASSIFIED_EXCEPTION",
    "NotificationWorker",
    "WorkerRunReport",
]
