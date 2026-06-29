"""Deadline total por turno — Etapa 6 H2.

Un turno tiene un presupuesto monotónico compartido (15s). Ninguna llamada individual
a Socrata puede usar un timeout mayor que el tiempo restante menos una reserva de cierre
(clasificación, count, formato, narración). Esto acota la cola de fetch de 60s sin tocar
H1 (count no bloqueante) ni introducir concurrencia (H3).

Diseño:
- Reloj **monotónico** (`perf_counter`), arranca UNA vez en `run_query`.
- Propagación por `ContextVar` (igual patrón que la telemetría de Etapa 5) → las capas
  bajas leen el deadline sin cambiar firmas.
- `fetch_budget_s = min(timeout_fuente, restante − reserva)`. Si < mínimo útil, NO se
  inicia la llamada (no se deja una petición que no puede terminar).
- Sin threads/tareas huérfanas: se usa el timeout de socket (`with_timeout`), no hilos.
"""
from __future__ import annotations

import contextvars
import time
from dataclasses import dataclass

# Valores de la spec (aprobados). Configurables vía begin_deadline.
TURN_DEADLINE_SECONDS = 15.0
CLOSING_RESERVE_SECONDS = 4.0       # margen post-fetch: count (≤3s) + formato/cierre
MIN_USEFUL_FETCH_SECONDS = 1.0      # por debajo, una llamada no alcanzaría a terminar

# H4 — reparto interno del deadline (8 + 3 + 4 = 15, aprobado por el asesor). La query
# PRINCIPAL se acota a 8s (no a `restante − reserva` ≈ 11s) para DEJAR sitio a UN fallback
# sin ORDER BY de ≤3s dentro de los mismos 15s. Sin este tope, un timeout de la principal
# consume todo y el fallback nunca corre (lo que mostró H4.0: 8 turnos "skipped").
PRIMARY_FETCH_SECONDS = 8.0
FALLBACK_FETCH_SECONDS = 3.0

# Reloj monotónico inyectable (perf_counter en prod; reloj falso en tests, sin sleep).
_clock = {"now": time.perf_counter}


def set_clock(fn) -> None:
    _clock["now"] = fn


def reset_clock() -> None:
    _clock["now"] = time.perf_counter


def now() -> float:
    return _clock["now"]()


@dataclass(frozen=True)
class Deadline:
    started_at: float                       # marca monotónica del inicio del turno
    budget_s: float = TURN_DEADLINE_SECONDS
    reserve_s: float = CLOSING_RESERVE_SECONDS

    def elapsed_s(self) -> float:
        return now() - self.started_at

    def remaining_s(self) -> float:
        return self.budget_s - self.elapsed_s()

    def fetch_budget_s(self, source_timeout_s: float, cap_s: float | None = None) -> float:
        """Presupuesto para una llamada: min(timeout fuente, restante − reserva[, cap]).

        ``cap_s`` aplica el tope de la fase (H4): ``PRIMARY_FETCH_SECONDS`` para la query
        principal, ``FALLBACK_FETCH_SECONDS`` para el fallback sin ORDER BY.
        """
        budget = min(float(source_timeout_s), self.remaining_s() - self.reserve_s)
        if cap_s is not None:
            budget = min(budget, float(cap_s))
        return budget

    def can_afford(self, min_s: float = MIN_USEFUL_FETCH_SECONDS) -> bool:
        return self.fetch_budget_s(float("inf")) >= min_s


_deadline: contextvars.ContextVar[Deadline | None] = contextvars.ContextVar(
    "secoppal_deadline", default=None,
)
_dl_tel: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "secoppal_deadline_tel", default=None,
)


def begin_deadline(
    started_at: float,
    *,
    budget_s: float = TURN_DEADLINE_SECONDS,
    reserve_s: float = CLOSING_RESERVE_SECONDS,
) -> tuple[contextvars.Token, contextvars.Token]:
    """Inicia el deadline del turno; devuelve tokens para restaurar (anidación segura)."""
    t1 = _deadline.set(Deadline(started_at, budget_s, reserve_s))
    t2 = _dl_tel.set({})
    return (t1, t2)


def end_deadline(tokens: tuple[contextvars.Token, contextvars.Token]) -> dict:
    """Cierra el deadline; devuelve la telemetría acumulada y restaura el estado previo."""
    tel = collected_deadline()
    t1, t2 = tokens
    _dl_tel.reset(t2)
    _deadline.reset(t1)
    return tel


def current_deadline() -> Deadline | None:
    return _deadline.get()


def record_deadline(**fields) -> None:
    """Registra telemetría de deadline del turno. No-op si no hay deadline activo."""
    buf = _dl_tel.get()
    if buf is not None:
        buf.update(fields)


def collected_deadline() -> dict:
    return dict(_dl_tel.get() or {})


def attach_deadline_telemetry(result, tel: dict, started_at: float,
                              budget_s: float = TURN_DEADLINE_SECONDS) -> None:
    """Adjunta al result la telemetría del deadline (centralizado en run_query)."""
    if not isinstance(result, dict):
        return
    result["turn_deadline_ms"] = round(budget_s * 1000, 1)
    result["turn_elapsed_ms"] = round((now() - started_at) * 1000, 1)
    for k, v in tel.items():
        result[k] = v


