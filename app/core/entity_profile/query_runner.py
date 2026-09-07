"""Create and execute the fixed, bounded Entity Profile source-call plan."""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from dataclasses import dataclass
import time
from typing import Any, Callable

from app.core.entity_profile.aspects import (
    build_contract_values_query,
    build_process_activity_query,
    build_recent_query,
    build_supplier_query,
    build_temporal_query,
)
from app.core.entity_profile.models import CONTRACT_DATASET, PROCESS_DATASET, SourceCall


ENTITY_PROFILE_QUERY_BUDGET = 6
ENTITY_PROFILE_TIMEOUT_PER_CALL = 8
ENTITY_PROFILE_TIMEOUT_GLOBAL = 9


@dataclass(frozen=True, slots=True)
class QueryJob:
    """Un job del catálogo no tiene params; los recibe al vincularse a un plan.

    ``params`` es el frame YA proyectado para ``dataset_id``. Que viaje con el
    job y no como argumento común de la ejecución es lo que impide volver a
    pasar un mismo diccionario a los dos esquemas.
    """

    call_id: str
    aspect: str
    dataset_id: str
    operation: str
    build: Callable[[Any, dict], str]
    params: dict | None = None


@dataclass(frozen=True, slots=True)
class JobResult:
    rows: tuple[dict, ...]
    call: SourceCall


def jobs_for_focus(focus: str) -> tuple[QueryJob, ...]:
    process_activity = QueryJob(
        "process_activity", "activity_modalities", PROCESS_DATASET, "aggregate",
        build_process_activity_query,
    )
    contract_values = QueryJob(
        "contract_values", "activity_values", CONTRACT_DATASET, "aggregate",
        build_contract_values_query,
    )
    supplier_count = QueryJob(
        "supplier_count", "suppliers", CONTRACT_DATASET, "aggregate",
        lambda builder, params: build_supplier_query(builder, params, "count"),
    )
    supplier_value = QueryJob(
        "supplier_value", "suppliers", CONTRACT_DATASET, "aggregate",
        lambda builder, params: build_supplier_query(builder, params, "value"),
    )
    recent_processes = QueryJob(
        "recent_processes", "recent", PROCESS_DATASET, "query",
        lambda builder, params: build_recent_query(builder, PROCESS_DATASET, params),
    )
    recent_contracts = QueryJob(
        "recent_contracts", "recent", CONTRACT_DATASET, "query",
        lambda builder, params: build_recent_query(builder, CONTRACT_DATASET, params),
    )
    plans = {
        "overview": (
            process_activity, contract_values, supplier_count, supplier_value,
            recent_processes, recent_contracts,
        ),
        "suppliers": (supplier_count, supplier_value),
        "modalities": (process_activity,),
        "values": (contract_values,),
        "recent": (recent_processes, recent_contracts),
        "temporal": (
            QueryJob(
                "temporal_processes", "temporal", PROCESS_DATASET, "aggregate",
                lambda builder, params: build_temporal_query(
                    builder, PROCESS_DATASET, params,
                ),
            ),
            QueryJob(
                "temporal_contracts", "temporal", CONTRACT_DATASET, "aggregate",
                lambda builder, params: build_temporal_query(
                    builder, CONTRACT_DATASET, params,
                ),
            ),
        ),
    }
    try:
        jobs = plans[focus]
    except KeyError as exc:
        raise ValueError(f"unknown Entity Profile focus: {focus}") from exc
    if len(jobs) > ENTITY_PROFILE_QUERY_BUDGET:
        raise ValueError("Entity Profile query budget exceeded before execution")
    return jobs


def _is_timeout(exc: Exception) -> bool:
    from app.core.workflow.helpers import _is_timeout_exception

    return _is_timeout_exception(exc)


def _run_job(
    job: QueryJob, soql: str, secop_client: Any, timeout_s: int,
) -> JobResult:
    start = time.perf_counter()
    try:
        client = secop_client.derived_for_timeout(timeout_s)
        rows = list(getattr(client, job.operation)(job.dataset_id, soql))
        status, error_type = "SUCCESS", None
    except Exception as exc:
        rows = []
        status = "TIMEOUT" if _is_timeout(exc) else "ERROR"
        error_type = type(exc).__name__
    return JobResult(
        tuple(rows),
        SourceCall(
            call_id=job.call_id,
            aspect=job.aspect,
            dataset_id=job.dataset_id,
            operation=job.operation,
            status=status,
            rows_received=len(rows),
            latency_ms=(time.perf_counter() - start) * 1000,
            soql=soql,
            error_type=error_type,
        ),
    )


def execute_jobs(
    jobs: tuple[QueryJob, ...], secop_client: Any, soql_builder: Any,
) -> tuple[dict[str, list[dict]], dict[str, SourceCall]]:
    """Execute only successfully built calls; every submitted call is recorded."""
    built: dict[str, str] = {}
    calls: dict[str, SourceCall] = {}
    for job in jobs:
        if job.params is None:
            raise ValueError(f"unbound Entity Profile job: {job.call_id}")
        try:
            built[job.call_id] = job.build(soql_builder, job.params)
        except Exception as exc:
            calls[job.call_id] = SourceCall(
                call_id=job.call_id,
                aspect=job.aspect,
                dataset_id=job.dataset_id,
                operation=job.operation,
                status="BUILD_ERROR",
                rows_received=0,
                latency_ms=0.0,
                soql="",
                error_type=type(exc).__name__,
                attempted=False,
            )

    from app.core.deadline import MIN_USEFUL_FETCH_SECONDS, current_deadline

    deadline = current_deadline()
    budget = (
        deadline.fetch_budget_s(ENTITY_PROFILE_TIMEOUT_PER_CALL)
        if deadline is not None
        else float(ENTITY_PROFILE_TIMEOUT_PER_CALL)
    )
    rows: dict[str, list[dict]] = {}
    if budget < MIN_USEFUL_FETCH_SECONDS:
        for job in jobs:
            calls.setdefault(job.call_id, SourceCall(
                call_id=job.call_id,
                aspect=job.aspect,
                dataset_id=job.dataset_id,
                operation=job.operation,
                status="BUDGET_EXHAUSTED",
                rows_received=0,
                latency_ms=0.0,
                soql=built.get(job.call_id, ""),
                error_type="TurnDeadline",
                attempted=False,
            ))
        return rows, calls
    timeout_s = max(1, int(budget))
    executor = ThreadPoolExecutor(max_workers=max(1, len(built)))
    futures: dict[Future[JobResult], QueryJob] = {
        executor.submit(
            _run_job, job, built[job.call_id], secop_client, timeout_s,
        ): job
        for job in jobs
        if job.call_id in built
    }
    started = time.perf_counter()
    pending = set(futures)
    try:
        for future in as_completed(futures, timeout=ENTITY_PROFILE_TIMEOUT_GLOBAL):
            pending.discard(future)
            result = future.result()
            rows[result.call.call_id] = list(result.rows)
            calls[result.call.call_id] = result.call
    except FuturesTimeout:
        pass
    finally:
        elapsed = (time.perf_counter() - started) * 1000
        for future in pending:
            job = futures[future]
            future.cancel()
            calls[job.call_id] = SourceCall(
                call_id=job.call_id,
                aspect=job.aspect,
                dataset_id=job.dataset_id,
                operation=job.operation,
                status="TIMEOUT",
                rows_received=0,
                latency_ms=elapsed,
                soql=built[job.call_id],
                error_type="GlobalProfileTimeout",
            )
        executor.shutdown(wait=False, cancel_futures=True)
    return rows, calls
