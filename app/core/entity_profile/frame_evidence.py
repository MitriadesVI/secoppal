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
from typing import TYPE_CHECKING

from app.core.entity_profile.aspects.common import as_optional_int

if TYPE_CHECKING:
    from app.core.entity_profile.frame_compiler import CompiledJob, ProfilePlan


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
    plan: ProfilePlan | None = None,
    plan_id: str,
    job_id: str,
    fingerprint: str,
    dataset_id: str,
    sql: str,
    status: str,
    rows: tuple[dict, ...],
    attempted: bool = True,
) -> ObservedCall:
    """Validate legacy call metadata against the authorizing plan.

    Metadata alone is only a claim.  A caller that cannot supply the plan does
    not have enough authority to create accredited evidence.
    """
    if plan is None:
        raise ValueError("the authorizing plan is required")
    job = _authorized_job(plan, job_id)
    source_plan = plan.source_plans.get(job.source)
    if source_plan is None:
        raise ValueError("job source is absent from the authorizing plan")
    expected = {
        "plan_id": plan.plan_id,
        "fingerprint": source_plan.fingerprint,
        "dataset_id": job.dataset_id,
        "sql": job.sql,
    }
    supplied = {
        "plan_id": plan_id,
        "fingerprint": fingerprint,
        "dataset_id": dataset_id,
        "sql": sql,
    }
    mismatches = tuple(
        name for name, value in supplied.items() if value != expected[name]
    )
    if mismatches:
        raise ValueError(
            "observation is foreign to the authorizing plan: "
            + ", ".join(mismatches),
        )
    return observe_call(
        plan, job_id, sql, status=status, rows=rows, attempted=attempted,
    )


def observe_call(
    plan: ProfilePlan,
    job_id: str,
    actual_sql: str,
    *,
    status: str,
    rows: tuple[dict, ...],
    attempted: bool = True,
) -> ObservedCall:
    """Create evidence from one real job and the exact SQL that was sent."""
    job = _authorized_job(plan, job_id)
    source_plan = plan.source_plans.get(job.source)
    if source_plan is None or job.dataset_id != source_plan.dataset_id:
        raise ValueError("job is not owned by the named source plan")
    if actual_sql != job.sql:
        raise ValueError("actual SQL differs from the authorized SQL")
    metrics: list[AccreditedMetric] = []
    if attempted and status == "SUCCESS":
        for row in rows:
            for name, raw in row.items():
                metrics.append(accredit_metric(name, raw))
    return ObservedCall(
        plan_id=plan.plan_id, job_id=job.job_id,
        predicate_fingerprint=source_plan.fingerprint,
        dataset_id=job.dataset_id, sql=actual_sql, status=status, attempted=attempted,
        metrics=tuple(metrics),
    )


def _authorized_job(plan: ProfilePlan, job_id: str) -> CompiledJob:
    jobs = tuple(job for job in plan.jobs if job.job_id == job_id)
    if len(jobs) != 1:
        raise ValueError("job_id does not identify one job in the authorizing plan")
    return jobs[0]


def _is_count(name: str) -> bool:
    return any(name.endswith(suffix) for suffix in _COUNT_METRICS)
