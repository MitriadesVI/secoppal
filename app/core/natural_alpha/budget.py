"""La fase lingüística no le cobra su latencia a la ejecución.

Un turno tiene UN deadline monotónico de 15 s (`app/core/deadline.py`). Cuando
Natural Alpha observa, su latencia —y la del verificador de residual, que corre
después, dentro de la admisión— se descuenta de ese mismo presupuesto, así que
la ejecución recibe lo que sobre. Con `fetch_budget_s = restante − reserva`, dos
segundos de fase lingüística se convierten en dos segundos menos de socket para
SECOP.

El carril legacy ya tenía la protección: `legacy_budget` abre una ventana nueva
y completa antes de `handle_normal_flow`. La política no era de legacy, era de
la ejecución; el carril PROFILE del Frame se quedó fuera por construcción, no
por decisión, y el incidente de Soledad (DOGFOOD-001) lo mostró — 6 s de
timeout por llamada donde legacy tenía 8.

Aquí la política vive UNA vez y los dos carriles la piden por su nombre. No se
aumenta el deadline del turno: se le devuelve a la ejecución el presupuesto que
la fase lingüística ya había consumido, que es exactamente lo que legacy hacía.
"""
from contextlib import contextmanager

from app.core.deadline import (
    TURN_DEADLINE_SECONDS,
    begin_deadline,
    end_deadline,
    now,
    record_deadline,
)

# El presupuesto completo del turno, el mismo de la spec. No es un timeout nuevo
# ni un caso especial: es el valor que `begin_deadline` ya usa por defecto.
FULL_BUDGET_MS = int(TURN_DEADLINE_SECONDS * 1000)


@contextmanager
def _fresh_execution_budget(observed: bool, lane: str):
    """Ventana nueva y completa para la ejecución; restaura el turno al salir.

    `observed` es la misma condición en los dos carriles: sin observación no se
    consumió latencia lingüística y no hay nada que devolver. La telemetría del
    tramo interno se funde en el buffer del turno ANTES de anotar la del carril,
    para que `end_deadline` no se lleve la evidencia consigo.
    """
    if not observed:
        yield
        return
    # Alpha has its own provider timeout. Execution gets its full original 15s.
    # Keep HandlerContext.run_started_at unchanged for end-to-end timings.
    started = now()
    tokens = begin_deadline(started)
    try:
        yield
    finally:
        telemetry = end_deadline(tokens)
        record_deadline(**telemetry)
        record_deadline(**{
            f"natural_alpha_{lane}_budget_ms": FULL_BUDGET_MS,
            f"natural_alpha_{lane}_elapsed_ms": round((now() - started) * 1000, 3),
        })


@contextmanager
def legacy_budget(observed: bool):
    """El carril legacy. Comportamiento y telemetría idénticos a los previos."""
    with _fresh_execution_budget(observed, "legacy"):
        yield


@contextmanager
def profile_budget(observed: bool):
    """El carril PROFILE del Frame — credenciado o no — con la MISMA política."""
    with _fresh_execution_budget(observed, "profile"):
        yield


__all__ = ["FULL_BUDGET_MS", "legacy_budget", "profile_budget"]
