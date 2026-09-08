"""Ejecutar EXACTAMENTE los jobs de un plan READY. Nada más.

El runner de V1.1 recibía jobs con `params` y volvía a construir el SQL en el
momento de llamar. Eso dejaba abierta una puerta: entre «lo que el plan
autorizó» y «lo que se envió» había un constructor que podía decidir otra cosa.

Aquí el SQL ya viene congelado en `CompiledJob`. Este módulo no construye
predicados, no amplía fuentes, no añade defaults y no reintenta con filtros
relajados: sólo emite, mide y ACREDITA cada llamada contra el plan que la
autorizó, mediante `observe_call`.

Presupuestos: los mismos que ya estaban configurados (8 s por llamada, 9 s
globales, 6 llamadas). No se aumentan para que pase un escenario.
"""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from dataclasses import dataclass, field
import time
from typing import Any, Mapping

from app.core.entity_profile.frame_compiler import CompiledJob, ProfilePlan
from app.core.entity_profile.frame_evidence import ObservedCall, observe_call
from app.core.entity_profile.query_runner import (
    ENTITY_PROFILE_TIMEOUT_GLOBAL,
    ENTITY_PROFILE_TIMEOUT_PER_CALL,
    _is_timeout,
)


@dataclass(frozen=True, slots=True)
class ProfileEvidence:
    """Lo intentado, lo observado y lo acreditado, atado a UN plan."""

    plan_id: str
    calls: tuple[ObservedCall, ...]
    rows: Mapping[str, tuple[dict, ...]] = field(default_factory=dict)
    latency_ms: Mapping[str, float] = field(default_factory=dict)
    aspect_by_job: Mapping[str, str] = field(default_factory=dict)
    total_latency_ms: float = 0.0

    def call(self, job_id: str) -> ObservedCall | None:
        for item in self.calls:
            if item.job_id == job_id:
                return item
        return None

    def rows_for(self, job_id: str) -> tuple[dict, ...]:
        return tuple(self.rows.get(job_id) or ())

    @property
    def attempted_calls(self) -> int:
        """Llamadas INTENTADAS. Un job planificado no es una llamada hecha."""
        return sum(call.attempted for call in self.calls)

    @property
    def succeeded_jobs(self) -> frozenset[str]:
        return frozenset(
            call.job_id for call in self.calls
            if call.attempted and call.status == "SUCCESS"
        )

    @property
    def total_failure(self) -> bool:
        """Ninguna de las llamadas planificadas devolvió una respuesta."""
        return bool(self.calls) and not self.succeeded_jobs

    def calls_by_aspect(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for call in self.calls:
            if not call.attempted:
                continue
            aspect = self.aspect_by_job.get(call.job_id, "")
            counts[aspect] = counts.get(aspect, 0) + 1
        return counts

    def latency_by_aspect(self) -> dict[str, float]:
        totals: dict[str, float] = {}
        for call in self.calls:
            if not call.attempted:
                continue
            aspect = self.aspect_by_job.get(call.job_id, "")
            totals[aspect] = totals.get(aspect, 0.0) + self.latency_ms.get(
                call.job_id, 0.0,
            )
        return totals


@dataclass(frozen=True, slots=True)
class _JobOutcome:
    job_id: str
    status: str
    rows: tuple[dict, ...]
    latency_ms: float
    attempted: bool = True


def execute_plan(plan: ProfilePlan, secop_client: Any) -> ProfileEvidence:
    """Emite el SQL congelado de cada job y acredita lo observado."""
    started = time.perf_counter()
    budget = _fetch_budget()
    if budget is None:
        outcomes = tuple(
            _JobOutcome(job.job_id, "BUDGET_EXHAUSTED", (), 0.0, attempted=False)
            for job in plan.jobs
        )
        return _evidence(plan, outcomes, started)

    timeout_s = max(1, int(budget))
    executor = ThreadPoolExecutor(max_workers=max(1, len(plan.jobs)))
    futures: dict[Future[_JobOutcome], CompiledJob] = {
        executor.submit(_run_job, job, secop_client, timeout_s): job
        for job in plan.jobs
    }
    collected: dict[str, _JobOutcome] = {}
    pending = set(futures)
    fetch_started = time.perf_counter()
    try:
        for future in as_completed(futures, timeout=ENTITY_PROFILE_TIMEOUT_GLOBAL):
            pending.discard(future)
            outcome = future.result()
            collected[outcome.job_id] = outcome
    except FuturesTimeout:
        pass
    finally:
        elapsed = (time.perf_counter() - fetch_started) * 1000
        for future in pending:
            job = futures[future]
            future.cancel()
            collected[job.job_id] = _JobOutcome(
                job.job_id, "TIMEOUT", (), elapsed,
            )
        executor.shutdown(wait=False, cancel_futures=True)
    outcomes = tuple(
        collected.get(job.job_id)
        # Un job del plan que no dejó rastro no se convierte en un cero: se
        # declara no disponible por falta de respuesta.
        or _JobOutcome(job.job_id, "UNAVAILABLE", (), 0.0)
        for job in plan.jobs
    )
    return _evidence(plan, outcomes, started)


def _fetch_budget() -> float | None:
    from app.core.deadline import MIN_USEFUL_FETCH_SECONDS, current_deadline

    deadline = current_deadline()
    budget = (
        deadline.fetch_budget_s(ENTITY_PROFILE_TIMEOUT_PER_CALL)
        if deadline is not None
        else float(ENTITY_PROFILE_TIMEOUT_PER_CALL)
    )
    return None if budget < MIN_USEFUL_FETCH_SECONDS else budget


def _run_job(job: CompiledJob, secop_client: Any, timeout_s: int) -> _JobOutcome:
    start = time.perf_counter()
    try:
        client = secop_client.derived_for_timeout(timeout_s)
        rows = tuple(getattr(client, job.operation)(job.dataset_id, job.sql))
        status = "SUCCESS"
    except Exception as exc:  # noqa: BLE001 — el transporte no clasifica solo
        rows, status = (), ("TIMEOUT" if _is_timeout(exc) else "ERROR")
    return _JobOutcome(
        job.job_id, status, rows, (time.perf_counter() - start) * 1000,
    )


def _evidence(
    plan: ProfilePlan, outcomes: tuple[_JobOutcome, ...], started: float,
) -> ProfileEvidence:
    calls = tuple(
        observe_call(
            plan, outcome.job_id,
            _job(plan, outcome.job_id).sql,
            status=outcome.status, rows=outcome.rows, attempted=outcome.attempted,
        )
        for outcome in outcomes
    )
    return ProfileEvidence(
        plan_id=plan.plan_id,
        calls=calls,
        rows={o.job_id: o.rows for o in outcomes},
        latency_ms={o.job_id: o.latency_ms for o in outcomes},
        aspect_by_job={job.job_id: job.aspect for job in plan.jobs},
        total_latency_ms=(time.perf_counter() - started) * 1000,
    )


def _job(plan: ProfilePlan, job_id: str) -> CompiledJob:
    for job in plan.jobs:
        if job.job_id == job_id:
            return job
    raise ValueError("outcome references a job outside its plan")


__all__ = ["ProfileEvidence", "execute_plan"]
