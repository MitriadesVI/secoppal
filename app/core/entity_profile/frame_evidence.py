"""Lo realmente OBSERVADO, atado al plan que lo autorizó.

No se introduce una clase `Evidence` genérica que compita con Truth Layer: se
extiende el contrato que ya existe —una llamada, su SQL, su transporte— con lo
que faltaba, que es la BINDING al plan y el estado de cada métrica por separado.

Dos hechos que V1.1 confundía y aquí no pueden confundirse:

* Un transporte SUCCESS no acredita un agregado. Cero filas no es cero
  contratos: es una respuesta sin la cifra pedida.
* Una suma ilegible no invalida su conteo. ``count(*)`` no depende de la
  columna de valor, así que un universo con montos nulos tiene un conteo cierto
  y una suma que no existe. Invalidar el conteo perdía un hecho; convertir la
  suma en 0 inventaba otro.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum

from app.core.entity_profile.aspects.common import as_optional_int


_COUNT_METRICS = ("count", "row_count")


class MetricStatus(str, Enum):
    VALID = "VALID"
    MISSING = "MISSING"
    INVALID = "INVALID"
    CONTRADICTORY = "CONTRADICTORY"
    UNAVAILABLE = "UNAVAILABLE"
    # Nadie la pidió. Se mantiene aparte de todo fallo: una sección no
    # solicitada no es una sección que falló.
    NOT_REQUESTED = "NOT_REQUESTED"


@dataclass(frozen=True, slots=True)
class AccreditedMetric:
    name: str
    value: object
    status: MetricStatus


def accredit_metric(name: str, raw: object) -> AccreditedMetric:
    """Una cifra sólo vale si la fuente la dijo de forma legible."""
    if _is_count(name):
        parsed = as_optional_int(raw)
        if parsed is None:
            status = MetricStatus.MISSING if raw is None else MetricStatus.INVALID
            return AccreditedMetric(name, None, status)
        return AccreditedMetric(name, parsed, MetricStatus.VALID)

    if raw is None or raw == "":
        return AccreditedMetric(name, None, MetricStatus.UNAVAILABLE)
    try:
        amount = Decimal(str(raw))
    except (ArithmeticError, InvalidOperation, TypeError, ValueError):
        return AccreditedMetric(name, None, MetricStatus.INVALID)
    if not amount.is_finite():
        # NaN, +Infinity y -Infinity nunca acreditan un monto, y tampoco se
        # dejan llegar al render, donde desbordarían al formatear.
        return AccreditedMetric(name, None, MetricStatus.INVALID)
    return AccreditedMetric(name, amount, MetricStatus.VALID)


@dataclass(frozen=True, slots=True)
class ObservedCall:
    """Una llamada observada, con el plan y los predicados que la autorizaron."""

    plan_id: str
    job_id: str
    predicate_fingerprint: str
    dataset_id: str
    sql: str
    status: str
    attempted: bool
    metrics: tuple[AccreditedMetric, ...]

    def metric(self, name: str) -> AccreditedMetric | None:
        for item in self.metrics:
            if item.name == name:
                return item
        return None

    def metric_status(self, name: str) -> MetricStatus:
        if not self.attempted:
            return MetricStatus.NOT_REQUESTED
        found = self.metric(name)
        return found.status if found is not None else MetricStatus.MISSING


def bind_observation(
    *,
    plan_id: str,
    job_id: str,
    fingerprint: str,
    dataset_id: str,
    sql: str,
    status: str,
    rows: tuple[dict, ...],
    attempted: bool = True,
) -> ObservedCall:
    """Ata lo observado al job que lo pidió; sin esa atadura no es evidencia."""
    metrics: list[AccreditedMetric] = []
    if attempted and status == "SUCCESS":
        for row in rows:
            for name, raw in row.items():
                metrics.append(accredit_metric(name, raw))
    return ObservedCall(
        plan_id=plan_id, job_id=job_id, predicate_fingerprint=fingerprint,
        dataset_id=dataset_id, sql=sql, status=status, attempted=attempted,
        metrics=tuple(metrics),
    )


def _is_count(name: str) -> bool:
    return any(name.endswith(suffix) for suffix in _COUNT_METRICS)
