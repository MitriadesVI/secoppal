"""Telemetría observacional — Etapa 5.

Mide; NO cambia comportamiento. Regla (gate del asesor): la misma consulta con o
sin recolección activa debe producir los mismos params, SoQL, filas, outcome y
respuesta. La recolección es **pasiva**: si un turno no llama ``begin_collection``,
``record_http`` es no-op y el costo es nulo.

- Reloj **monotónico** inyectable (``perf_counter``; reloj falso en tests, nunca
  ``sleep``).
- Captura en ``finally``: un timeout es justo donde más se necesita el dato.
- Telemetría operativa guarda ``query_hash`` (no el SoQL completo, que vive en el
  trace de desarrollo).
"""
from __future__ import annotations

import contextvars
import hashlib
import time
from typing import Any

# Buffer de métricas HTTP por turno (aislado por contexto). default None = inactivo.
_http_metrics: contextvars.ContextVar[list[dict] | None] = contextvars.ContextVar(
    "secoppal_http_metrics", default=None,
)

# Reloj inyectable (holder mutable para no recrear el ContextVar en tests).
_clock = {"now": time.perf_counter}


def set_clock(fn) -> None:
    """Inyecta un reloj (tests). Debe devolver segundos monotónicos crecientes."""
    _clock["now"] = fn


def reset_clock() -> None:
    _clock["now"] = time.perf_counter


def _now_s() -> float:
    return _clock["now"]()


# ── Recolección por turno ────────────────────────────────────────────────────

def begin_collection() -> contextvars.Token:
    """Inicia la recolección HTTP del turno. Devuelve el token para restaurar después.

    El token permite anidar (un `run_query` dentro de otro): ``end_collection(token)``
    restaura el buffer previo en vez de dejarlo inactivo, así el collector externo
    conserva sus métricas. También protege workers concurrentes.
    """
    return _http_metrics.set([])


def record_http(metric: dict) -> None:
    """Registra una métrica HTTP. No-op si la recolección no está activa."""
    buf = _http_metrics.get()
    if buf is not None:
        buf.append(metric)


def collected_http() -> list[dict]:
    """Métricas HTTP acumuladas en el turno actual (copia)."""
    return list(_http_metrics.get() or [])


def end_collection(token: contextvars.Token | None = None) -> list[dict]:
    """Cierra la recolección: devuelve las métricas y restaura el estado previo.

    Con ``token`` (de ``begin_collection``) restaura el buffer anterior — anidación
    segura. Sin token, deja el estado inactivo (``record_http`` vuelve a ser no-op).
    """
    buf = collected_http()
    if token is not None:
        _http_metrics.reset(token)
    else:
        _http_metrics.set(None)
    return buf


# ── Adjuntar al resultado (centralizado en run_query) ────────────────────────

def attach_telemetry(result, metrics: list[dict]) -> None:
    """Adjunta http_metrics y consolida timings agregados en el result (in-place).

    Centralizado en run_query para que TODOS los handlers (normal, lookup, paginación,
    dual, analítico…) expongan telemetría — cerrar el collector no basta, hay que
    propagar lo capturado al result que el handler ya construyó.
    """
    if not isinstance(result, dict) or not metrics:
        return
    # Copia defensiva: no exponer el buffer mutable del collector (importa con
    # anidación y reutilización de workers).
    result["http_metrics"] = [dict(m) for m in metrics]
    tm = dict(result.get("timings_ms") or {})
    fetch = sum(m.get("duration_ms", 0.0) for m in metrics if m["operation"] in ("query", "aggregate"))
    count = sum(m.get("duration_ms", 0.0) for m in metrics if m["operation"] == "count")
    retry = sum(m.get("duration_ms", 0.0) for m in metrics if m["operation"] == "retry_delay")
    http = sum(m.get("duration_ms", 0.0) for m in metrics)
    tm["fetch_results_ms"] = round(fetch, 1)
    tm["count_ms"] = round(count, 1)
    tm["retry_delay_ms"] = round(retry, 1)
    tm["http_wall_ms"] = round(http, 1)
    tm["http_attempts"] = len(metrics)
    total = tm.get("total_ms")
    if total:
        tm["total_wall_ms"] = round(total, 1)
        tm["pipeline_self_ms"] = round(max(total - http, 0.0), 1)
    result["timings_ms"] = tm


# ── Utilidades ───────────────────────────────────────────────────────────────

def query_hash(soql: str) -> str:
    """Huella corta de un SoQL para telemetría (no se guarda el SoQL completo)."""
    return hashlib.sha256((soql or "").encode("utf-8")).hexdigest()[:16]


def is_timeout_exception(exc: BaseException) -> bool:
    """True si la excepción (o su cadena causal) es un timeout."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            return True
        detail = f"{type(current).__name__} {current}".lower()
        if "timeout" in detail or "timed out" in detail:
            return True
        current = current.__cause__ or current.__context__
    return False


class http_span:
    """Context manager que cronometra una operación HTTP y la registra en `finally`.

    Uso::

        with http_span("query", dataset_id, soql, attempt=1) as span:
            rows = client.get(...)
            span.rows_received = len(rows)

    Registra siempre — éxito o excepción — para no perder el dato en timeouts.
    """

    def __init__(self, operation: str, dataset: str, soql: str, *, attempt: int = 1):
        self.metric: dict[str, Any] = {
            "operation": operation,
            "dataset": dataset,
            "attempt": attempt,
            "query_hash": query_hash(soql),
            "status": "completed",
            "exception_type": None,
            "timeout": False,
            "timeout_stage": None,
            "rows_received": 0,
            "duration_ms": 0.0,
        }
        self._start = 0.0

    def __enter__(self) -> dict:
        self._start = _now_s()
        return self.metric

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.metric["duration_ms"] = (_now_s() - self._start) * 1000
        if exc is not None:
            self.metric["status"] = "failed"
            self.metric["exception_type"] = exc_type.__name__ if exc_type else None
            if is_timeout_exception(exc):
                self.metric["timeout"] = True
                self.metric["timeout_stage"] = "read"  # los timeouts de SECOP son de lectura
        record_http(self.metric)
        return False  # nunca suprime la excepción
