"""retry.py — aritmética pura del tiempo del worker (P1B-4, R2A/R2B).

Solo funciones puras: sin base de datos, sin reloj, sin configuración global. Que la
duración del claim sea una función de sus tres inputs —y no una constante escondida
en el worker— es lo que permite verificarla sin montar una base y recalibrarla con el
p99 medido sin tocar el motor.

Responde dos preguntas: "¿cuánto puede tardar como máximo el trabajo que protege el
claim?" (R2A) y "¿cuándo —y si— se vuelve a intentar?" (R2B). El worker no tiene
permiso para responder la segunda por su cuenta: una segunda copia del backoff que
alguien tocara dejaría el sistema con dos políticas de reintento y ninguna auditable.

El jitter entra como **parámetro**, no como llamada a ``random`` aquí dentro: es lo
que mantiene el módulo puro y hace que su aritmética se pueda verificar sin parchear
el generador. Quien decide de dónde sale la aleatoriedad es el worker.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

# Defaults operacionales, explícitos y a la vista. No los aplica ninguna función de
# este módulo: existen para que el caller los pase, y para que su derivación sea
# auditable en vez de folclore.
#
# - PROVIDER_TIMEOUT: el timeout de Telegram que fijará P1B-5.
# - SQLITE_BUSY_TIMEOUT: refleja ``PRAGMA busy_timeout=30000`` de ``db.py``. Es una
#   copia —no hay constante que importar—, así que ``test_notify_retry.py`` la compara
#   contra el PRAGMA real de una conexión: si alguien cambia db.py, el test cae.
# - SAFETY_MARGIN: pausas de GC y el hecho de que httpx aplica su timeout *por fase*,
#   no al total de la petición.
DEFAULT_PROVIDER_TIMEOUT = timedelta(seconds=10)
DEFAULT_SQLITE_BUSY_TIMEOUT = timedelta(seconds=30)
DEFAULT_SAFETY_MARGIN = timedelta(seconds=50)
DEFAULT_RECONCILIATION_SAFETY_MARGIN = timedelta(seconds=10)


def _check(value: timedelta, name: str) -> None:
    # ``isinstance(True, timedelta)`` es False, así que el chequeo de tipo ya rechaza
    # bool; el aviso explícito existe porque ``claim_duration(provider_timeout=True)``
    # es el error que alguien comete al copiar una firma con keywords.
    if not isinstance(value, timedelta):
        raise TypeError(
            f"{name} debe ser timedelta, no {type(value).__name__}: la duración del "
            f"claim es tiempo, no un número sin unidad."
        )
    if value < timedelta(0):
        raise ValueError(f"{name} no puede ser negativo: {value!r}")


def _positive(total: timedelta, label: str) -> timedelta:
    if total <= timedelta(0):
        raise ValueError(
            f"{label} debe ser positivo; con estos inputs da {total!r}. Un claim de "
            f"duración cero vence en el instante en que se toma."
        )
    return total


def claim_duration(
    *,
    provider_timeout: timedelta,
    sqlite_busy_timeout: timedelta,
    safety_margin: timedelta,
) -> timedelta:
    """``provider_timeout + 2×sqlite_busy_timeout + safety_margin``.

    El ``2×`` no es holgura: durante la vida efectiva del claim hay exactamente dos
    escrituras que pueden quedarse esperando el lock de escritor de SQLite —abrir el
    intento y cerrarlo—, y entre ellas cabe una llamada al proveedor. La adquisición
    del propio claim no cuenta porque su reloj se captura *después* del lock, así que
    su espera es previa a la propiedad.

    Default operacional: 10 s + 2×30 s + 50 s = **120 s**.
    """
    _check(provider_timeout, "provider_timeout")
    _check(sqlite_busy_timeout, "sqlite_busy_timeout")
    _check(safety_margin, "safety_margin")
    return _positive(
        provider_timeout + 2 * sqlite_busy_timeout + safety_margin, "claim_duration"
    )


def reconciliation_claim_duration(
    *,
    sqlite_busy_timeout: timedelta,
    safety_margin: timedelta,
) -> timedelta:
    """``2×sqlite_busy_timeout + safety_margin``.

    Sin término de proveedor: la reconciliación no envía nada. Sus dos escrituras
    bloqueables son tomar el claim del huérfano y cerrarlo como ``ambiguous``.

    Default operacional: 2×30 s + 10 s = **70 s**.
    """
    _check(sqlite_busy_timeout, "sqlite_busy_timeout")
    _check(safety_margin, "safety_margin")
    return _positive(
        2 * sqlite_busy_timeout + safety_margin, "reconciliation_claim_duration"
    )


DEFAULT_CLAIM_DURATION = claim_duration(
    provider_timeout=DEFAULT_PROVIDER_TIMEOUT,
    sqlite_busy_timeout=DEFAULT_SQLITE_BUSY_TIMEOUT,
    safety_margin=DEFAULT_SAFETY_MARGIN,
)

DEFAULT_RECONCILIATION_CLAIM_DURATION = reconciliation_claim_duration(
    sqlite_busy_timeout=DEFAULT_SQLITE_BUSY_TIMEOUT,
    safety_margin=DEFAULT_RECONCILIATION_SAFETY_MARGIN,
)


# ── R2B. Política de reintentos ───────────────────────────────────────────────────

DEFAULT_MAX_ATTEMPTS = 5
DEFAULT_BACKOFF_BASE = timedelta(seconds=30)
DEFAULT_BACKOFF_FACTOR = 2.0
DEFAULT_BACKOFF_CAP = timedelta(minutes=30)
# El jitter **resta** hasta un 20 %, nunca suma: sumar podría empujar el reintento
# más allá del TTL del batch y convertir una espera en una muerte silenciosa. Su
# trabajo es separar en el tiempo a los reintentos que fallaron juntos —un proveedor
# caído los sincroniza a todos—, y para eso basta con desalinearlos hacia abajo.
DEFAULT_JITTER_RATIO = 0.2


@dataclass(frozen=True)
class RetryPolicy:
    """Cuántas veces y cada cuánto. Inmutable y sin estado: el número de intento lo
    aporta el ledger, que es quien lo cuenta de verdad."""

    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    base: timedelta = DEFAULT_BACKOFF_BASE
    factor: float = DEFAULT_BACKOFF_FACTOR
    cap: timedelta = DEFAULT_BACKOFF_CAP
    jitter_ratio: float = DEFAULT_JITTER_RATIO

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int):
            raise TypeError(
                f"max_attempts debe ser int, no {type(self.max_attempts).__name__}."
            )
        if self.max_attempts < 1:
            raise ValueError(
                f"max_attempts debe ser al menos 1: {self.max_attempts!r}. Una política "
                f"de cero intentos no es una política de reintentos, es un apagado."
            )
        _check(self.base, "base")
        _check(self.cap, "cap")
        if self.base <= timedelta(0):
            raise ValueError("base debe ser positiva: un backoff de cero no espera.")
        if self.cap < self.base:
            raise ValueError(
                f"cap ({self.cap!r}) no puede ser menor que base ({self.base!r}): el "
                f"techo estaría por debajo del primer reintento."
            )
        if self.factor < 1.0:
            raise ValueError(
                f"factor debe ser >= 1.0: {self.factor!r}. Un factor menor haría los "
                f"reintentos cada vez más agresivos contra un proveedor que ya falla."
            )
        if not 0.0 <= self.jitter_ratio < 1.0:
            raise ValueError(
                f"jitter_ratio debe estar en [0.0, 1.0): {self.jitter_ratio!r}. Con 1.0 "
                f"el reintento podría programarse para ahora mismo."
            )


DEFAULT_RETRY_POLICY = RetryPolicy()


def _check_attempt_number(attempt_number: int) -> int:
    if isinstance(attempt_number, bool) or not isinstance(attempt_number, int):
        raise TypeError(
            f"attempt_number debe ser int, no {type(attempt_number).__name__}."
        )
    if attempt_number < 1:
        raise ValueError(
            f"attempt_number debe ser al menos 1: {attempt_number!r}. El ledger numera "
            f"desde 1 y un cero significaría un fallo que nunca se intentó."
        )
    return attempt_number


def should_retry(*, attempt_number: int, policy: RetryPolicy = DEFAULT_RETRY_POLICY) -> bool:
    """¿Queda presupuesto tras fallar el intento ``attempt_number``?

    ``attempt_number`` es el del intento que **acaba** de fallar, tal y como lo numeró
    el ledger. Con ``max_attempts=5``, el quinto fallo agota: no hay sexto.
    """
    return _check_attempt_number(attempt_number) < policy.max_attempts


def backoff_delay(
    *,
    attempt_number: int,
    policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    jitter: float = 0.0,
) -> timedelta:
    """``min(base × factor^(n-1), cap)``, menos hasta ``jitter_ratio`` de jitter.

    ``jitter`` ∈ [0, 1) lo aporta el caller —típicamente ``random.random()``—; con 0
    devuelve el backoff íntegro, que es lo que hace la aritmética verificable sin
    tocar el generador. El resultado nunca es cero ni negativo: el jitter escala, no
    resta un absoluto.
    """
    _check_attempt_number(attempt_number)
    if not isinstance(jitter, (int, float)) or isinstance(jitter, bool):
        raise TypeError(f"jitter debe ser float, no {type(jitter).__name__}.")
    if not 0.0 <= jitter < 1.0:
        raise ValueError(f"jitter debe estar en [0.0, 1.0): {jitter!r}")
    raw_seconds = policy.base.total_seconds() * (policy.factor ** (attempt_number - 1))
    capped = min(raw_seconds, policy.cap.total_seconds())
    return timedelta(seconds=capped * (1.0 - policy.jitter_ratio * jitter))


def next_not_before(
    *,
    attempt_number: int,
    now: datetime,
    policy: RetryPolicy = DEFAULT_RETRY_POLICY,
    retry_after: datetime | None = None,
    jitter: float = 0.0,
) -> datetime:
    """Cuándo puede volver a intentarse: **el más tardío** entre nuestro backoff y el
    ``retry_after`` del proveedor.

    Se toma el máximo, nunca el que aporta el proveedor a secas ni el nuestro a
    secas. Obedecer solo al proveedor dejaría que un ``retry_after: 0`` nos hiciera
    martillearlo; obedecer solo al nuestro ignoraría un ``429`` que dice
    explícitamente "no antes de las 10:05" y nos ganaría un bloqueo. El que manda es
    el que espera más.
    """
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError(
            f"now debe incluir zona horaria: {now!r}. Un instante naive comparado "
            f"contra marcas UTC ordena mal y el reintento saldría cuando no debe."
        )
    own = now + backoff_delay(
        attempt_number=attempt_number, policy=policy, jitter=jitter
    )
    if retry_after is None:
        return own
    if retry_after.tzinfo is None or retry_after.utcoffset() is None:
        raise ValueError(
            f"retry_after debe incluir zona horaria: {retry_after!r}."
        )
    return max(own, retry_after)


__all__ = [
    "DEFAULT_BACKOFF_BASE",
    "DEFAULT_BACKOFF_CAP",
    "DEFAULT_BACKOFF_FACTOR",
    "DEFAULT_CLAIM_DURATION",
    "DEFAULT_JITTER_RATIO",
    "DEFAULT_MAX_ATTEMPTS",
    "DEFAULT_PROVIDER_TIMEOUT",
    "DEFAULT_RECONCILIATION_CLAIM_DURATION",
    "DEFAULT_RECONCILIATION_SAFETY_MARGIN",
    "DEFAULT_RETRY_POLICY",
    "DEFAULT_SAFETY_MARGIN",
    "DEFAULT_SQLITE_BUSY_TIMEOUT",
    "RetryPolicy",
    "backoff_delay",
    "claim_duration",
    "next_not_before",
    "reconciliation_claim_duration",
    "should_retry",
]
