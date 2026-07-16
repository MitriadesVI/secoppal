"""retry.py — aritmética pura del tiempo del worker (P1B-4, R2A).

Solo funciones puras: sin base de datos, sin reloj, sin configuración global. Que la
duración del claim sea una función de sus tres inputs —y no una constante escondida
en el worker— es lo que permite verificarla sin montar una base y recalibrarla con el
p99 medido sin tocar el motor.

**Backoff y jitter no están aquí todavía**: son R2B. Este módulo solo responde
"¿cuánto tiempo puede tardar como máximo el trabajo que protege el claim?".
"""
from __future__ import annotations

from datetime import timedelta

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


__all__ = [
    "DEFAULT_CLAIM_DURATION",
    "DEFAULT_PROVIDER_TIMEOUT",
    "DEFAULT_RECONCILIATION_CLAIM_DURATION",
    "DEFAULT_RECONCILIATION_SAFETY_MARGIN",
    "DEFAULT_SAFETY_MARGIN",
    "DEFAULT_SQLITE_BUSY_TIMEOUT",
    "claim_duration",
    "reconciliation_claim_duration",
]
