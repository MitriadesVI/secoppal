"""fake_provider.py — un proveedor de mentira que no miente sobre lo que hizo.

Existe para que el ciclo completo del worker se pueda ejercer sin red: reclamar,
autorizar, renderizar, "enviar", cerrar. Su valor no está en simular a Telegram, sino
en **contar** — ``external_effect_count`` es la única forma de comprobar la propiedad
que da sentido a todo el bloque: que un mensaje no se entrega dos veces.

Vive en ``app/`` y no en ``tests/`` a propósito: es la implementación de referencia
del Protocol, y el sitio donde se lee qué significa cada efecto. Un adaptador real que
no sepa qué devolver ante un timeout mira aquí. Nada en producción lo instancia.

**Idempotencia.** Modela la propiedad que un proveedor real ofrece a cambio de la
clave: repetir una clave ya aceptada devuelve el mismo ``provider_message_id`` y **no**
produce un segundo efecto. Sin eso, el fake no podría distinguir un worker que
reintenta bien de uno que duplica, que es justo lo que hay que distinguir.
"""
from __future__ import annotations

import threading
from collections import Counter
from dataclasses import dataclass

from app.core.notify.provider import (
    CONFIRMED_EXTERNAL_EFFECT,
    NO_EXTERNAL_EFFECT,
    POSSIBLE_EXTERNAL_EFFECT,
    ProviderRequest,
    ProviderResult,
)

# Modos. El nombre dice **dónde** cae el fallo respecto del efecto externo, que es lo
# único que el worker necesita saber: ``_before_effect`` garantiza que no salió nada,
# ``_after_effect`` que pudo salir.
MODE_ACCEPT = "accept"
MODE_RETRY_BEFORE_EFFECT = "retry_before_effect"
MODE_PERMANENT_BEFORE_EFFECT = "permanent_before_effect"
MODE_TIMEOUT_AFTER_EFFECT = "timeout_after_effect"
MODE_RAISE_UNCLASSIFIED = "raise_unclassified"

MODES = (
    MODE_ACCEPT,
    MODE_RETRY_BEFORE_EFFECT,
    MODE_PERMANENT_BEFORE_EFFECT,
    MODE_TIMEOUT_AFTER_EFFECT,
    MODE_RAISE_UNCLASSIFIED,
)


class FakeProviderError(Exception):
    """Uso incorrecto del fake (un modo que no existe). No es un fallo de entrega."""


class UnclassifiedProviderError(Exception):
    """Lo que revienta en ``raise_unclassified``: un fallo que el proveedor no supo
    clasificar. Su mensaje trae PII a propósito —así son los errores reales de un SDK,
    que citan la URL y el cuerpo— para que los tests puedan probar que el worker
    **no** lo persiste."""


@dataclass(frozen=True)
class RecordedCall:
    """Lo que el fake recuerda de una llamada. ``text`` y ``external_recipient_id``
    quedan fuera: los tests no los necesitan para verificar el ciclo, y un registro de
    pruebas que los guarda acaba impreso en la salida de un fallo."""

    notification_id: str
    attempt_id: str
    channel: str
    external_idempotency_key: str
    payload_version: str
    mode: str


class FakeProvider:
    """Determinista y configurable por secuencia o por clave idempotente.

    - ``modes``: secuencia consumida en orden. El último se repite indefinidamente,
      para que un test que no sabe cuántas llamadas habrá no reviente por agotarla.
    - ``mode_by_key``: fija el modo por ``external_idempotency_key``. Gana sobre la
      secuencia. Es lo que permite un escenario con dos batches donde uno acepta y el
      otro falla, sin depender del orden en que el worker los tome.

    Es thread-safe: las pruebas de concurrencia lanzan varios workers contra el mismo
    fake, y un contador que se pierde incrementos no probaría nada.
    """

    def __init__(
        self,
        modes: list[str] | tuple[str, ...] | None = None,
        *,
        mode_by_key: dict[str, str] | None = None,
        default_mode: str = MODE_ACCEPT,
    ) -> None:
        for mode in list(modes or ()) + list((mode_by_key or {}).values()) + [default_mode]:
            if mode not in MODES:
                raise FakeProviderError(
                    f"Modo desconocido: {mode!r}. Válidos: {', '.join(MODES)}."
                )
        self._modes = list(modes or ())
        self._mode_by_key = dict(mode_by_key or {})
        self._default_mode = default_mode
        self._lock = threading.Lock()
        self._calls: list[RecordedCall] = []
        self._accepted: dict[str, str] = {}
        self._effects: Counter[str] = Counter()
        self._index = 0

    # ── Estado observable ─────────────────────────────────────────────────────────

    @property
    def calls(self) -> list[RecordedCall]:
        with self._lock:
            return list(self._calls)

    @property
    def call_count(self) -> int:
        with self._lock:
            return len(self._calls)

    @property
    def external_effect_count(self) -> int:
        """Cuántos efectos externos **reales** se produjeron.

        Un reintento sobre una clave ya aceptada no incrementa: el proveedor lo
        deduplica y no hay un segundo mensaje. Este número es el que tiene que valer
        1 al final de cualquier escenario, por muchos workers y reintentos que haya.
        """
        with self._lock:
            return sum(self._effects.values())

    def effects_for(self, key: str) -> int:
        with self._lock:
            return self._effects[key]

    @property
    def seen_keys(self) -> list[str]:
        with self._lock:
            return [call.external_idempotency_key for call in self._calls]

    def __repr__(self) -> str:
        # Sin destinatarios, sin texto y sin claves: un fake que se imprime entero en
        # el fallo de un test es un fake que filtra a la consola de CI.
        return (
            f"FakeProvider(calls={self.call_count}, "
            f"external_effects={self.external_effect_count})"
        )

    # ── Protocol ──────────────────────────────────────────────────────────────────

    def send(self, request: ProviderRequest) -> ProviderResult:
        with self._lock:
            mode = self._resolve_mode(request.external_idempotency_key)
            self._calls.append(
                RecordedCall(
                    notification_id=request.notification_id,
                    attempt_id=request.attempt_id,
                    channel=request.channel,
                    external_idempotency_key=request.external_idempotency_key,
                    payload_version=request.payload_version,
                    mode=mode,
                )
            )
            key = request.external_idempotency_key
            # La deduplicación va **antes** que el modo: una clave ya aceptada devuelve
            # el mismo id sin volver a producir efecto, aunque el modo diga otra cosa.
            # Es la promesa que hace un proveedor idempotente de verdad, y sin ella el
            # escenario "reintento tras timeout" no podría distinguirse de un duplicado.
            if key in self._accepted:
                return ProviderResult(
                    effect=CONFIRMED_EXTERNAL_EFFECT,
                    retryable=False,
                    provider_message_id=self._accepted[key],
                    error_message_safe="deduplicated_by_idempotency_key",
                )
            if mode == MODE_ACCEPT:
                message_id = f"fake-msg-{len(self._accepted) + 1}"
                self._accepted[key] = message_id
                self._effects[key] += 1
                return ProviderResult(
                    effect=CONFIRMED_EXTERNAL_EFFECT,
                    retryable=False,
                    provider_message_id=message_id,
                )
            if mode == MODE_RETRY_BEFORE_EFFECT:
                return ProviderResult(
                    effect=NO_EXTERNAL_EFFECT,
                    retryable=True,
                    error_class="network_error",
                    error_message_safe="transient_network_failure",
                )
            if mode == MODE_PERMANENT_BEFORE_EFFECT:
                return ProviderResult(
                    effect=NO_EXTERNAL_EFFECT,
                    retryable=False,
                    error_class="invalid_address",
                    error_message_safe="recipient_rejected",
                )
            if mode == MODE_TIMEOUT_AFTER_EFFECT:
                # El efecto **sí** cuenta: el mensaje salió y la respuesta se perdió.
                # Es el caso que hace honesto el contador — si no contara, un worker
                # que reintentara tras un timeout parecería correcto.
                self._effects[key] += 1
                return ProviderResult(
                    effect=POSSIBLE_EXTERNAL_EFFECT,
                    retryable=False,
                    error_class="timeout_after_send",
                    error_message_safe="timeout_waiting_for_ack",
                )
            # raise_unclassified. El mensaje lleva PII adrede: es la trampa que prueba
            # que el worker no persiste el texto crudo de una excepción.
            raise UnclassifiedProviderError(
                f"boom al enviar a {request.external_recipient_id!r}: "
                f"{request.text[:40]!r}"
            )

    def _resolve_mode(self, key: str) -> str:
        if key in self._mode_by_key:
            return self._mode_by_key[key]
        if not self._modes:
            return self._default_mode
        # El último modo se repite en vez de agotarse: un test que fija
        # ``['retry_before_effect']`` quiere "siempre falla", no "falla una vez y luego
        # revienta con IndexError".
        mode = self._modes[min(self._index, len(self._modes) - 1)]
        self._index += 1
        return mode


__all__ = [
    "MODES",
    "MODE_ACCEPT",
    "MODE_PERMANENT_BEFORE_EFFECT",
    "MODE_RAISE_UNCLASSIFIED",
    "MODE_RETRY_BEFORE_EFFECT",
    "MODE_TIMEOUT_AFTER_EFFECT",
    "FakeProvider",
    "FakeProviderError",
    "RecordedCall",
    "UnclassifiedProviderError",
]
