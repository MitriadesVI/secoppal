"""Ensamblado del perfil: fan-out acotado y resultado parcial honesto.

Reusa el patrón que ``observer.py`` ya corre en producción —presupuesto
por tarea, techo global, ensamblaje con lo que llegó— en vez de inventar
uno. Lo que aquí se añade es la disciplina de estado: cada aspecto que
falla queda registrado con el vocabulario de ``outcome.py``, y esa
distinción viaja hasta el render.

La regla que gobierna el módulo
-------------------------------
Un agregado secundario que falla **no** destruye el perfil, y un perfil
incompleto **no** se presenta como completo. Las dos mitades importan por
igual: la primera evita perder la respuesta entera por una distribución
por año; la segunda evita que un timeout se lea como "este actor no tiene
contratos en Antioquia".

``queried_at`` se toma UNA vez, al empezar, y es el mismo para todo el
perfil. Los agregados corren en paralelo contra una fuente que cambia, así
que fechar cada uno por separado sugeriría una precisión que no existe:
lo honesto es decir cuándo se hizo la consulta.
"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime, timezone
from typing import Any

from app.core.actor_profile.aspects import ASPECTS, ProfileAspect
from app.core.actor_profile.models import ActorProfileResult, AspectStatus
from app.core.deadline import current_deadline
from app.core.outcome import Outcome, OutcomeReason

logger = logging.getLogger(__name__)

__all__ = ["ACTOR_PROFILE_MAX_WORKERS", "build_actor_profile"]

# Espeja el fan-out del observer: presupuesto por tarea y techo global.
ACTOR_PROFILE_TIMEOUT_PER_TASK = 5
ACTOR_PROFILE_TIMEOUT_GLOBAL = 8
ACTOR_PROFILE_MAX_WORKERS = 4

# Hora de Colombia. El perfil se consulta y se lee en Colombia, y una marca
# en UTC obligaría a cada lector a convertirla — con DEPLOY-TZ-001 abierto,
# fijar la zona explícitamente es lo que impide que la respuesta dependa de
# dónde esté corriendo el proceso.
PROFILE_TIMEZONE = "America/Bogota"


def _now() -> datetime:
    """Instante de la consulta, con zona explícita (INV-CUTOFF)."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo(PROFILE_TIMEZONE))
    except Exception:  # pragma: no cover - sin tzdata en el sistema
        return datetime.now(timezone.utc)


def _budget_for_task() -> int:
    """Presupuesto por agregado, acotado por lo que queda del turno.

    Sin deadline activo (tests, scripts) usa el presupuesto nominal. Con
    deadline, nunca inicia una llamada que no alcanzaría a terminar dentro
    del turno: dejar peticiones colgando es cómo se agota el presupuesto sin
    obtener nada.
    """
    deadline = current_deadline()
    if deadline is None:
        return ACTOR_PROFILE_TIMEOUT_PER_TASK
    budget = deadline.fetch_budget_s(ACTOR_PROFILE_TIMEOUT_PER_TASK)
    return max(1, int(budget))


def _run_aspect(
    aspect: ProfileAspect,
    secop_client: Any,
    soql_builder: Any,
    dataset_id: str,
    params: dict,
    timeout_s: int,
) -> list[dict]:
    soql = aspect.build(soql_builder, dataset_id, params)
    if not soql:
        # El dataset no puede servir este aspecto (p. ej. no publica al
        # proveedor). No es un error de transporte: es una capacidad ausente.
        raise NotImplementedError(f"aspect {aspect.name} unsupported by {dataset_id}")
    client = secop_client.with_timeout(timeout_s)
    if aspect.transport == "query":
        return list(client.query(dataset_id, soql))
    return list(client.aggregate(dataset_id, soql))


def build_actor_profile(
    params: dict,
    dataset_id: str,
    secop_client: Any,
    soql_builder: Any,
) -> ActorProfileResult:
    """Consulta la huella contractual de un actor y la ensambla.

    Nunca lanza por un aspecto caído: el fallo se registra y el perfil se
    entrega con lo que sí se obtuvo.
    """
    queried_at = _now()
    spec = soql_builder.SPECS.get(dataset_id)
    timeout_s = _budget_for_task()

    payload: dict[str, Any] = {}
    statuses: list[AspectStatus] = []
    pending = {aspect.name: aspect for aspect in ASPECTS}

    with ThreadPoolExecutor(max_workers=ACTOR_PROFILE_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _run_aspect, aspect, secop_client, soql_builder,
                dataset_id, params, timeout_s,
            ): aspect
            for aspect in ASPECTS
        }
        try:
            for future in as_completed(futures, timeout=ACTOR_PROFILE_TIMEOUT_GLOBAL):
                aspect = futures[future]
                pending.pop(aspect.name, None)
                try:
                    rows = future.result()
                except NotImplementedError:
                    statuses.append(AspectStatus(
                        aspect.name, Outcome.EMPTY, OutcomeReason.ZERO_RESULTS,
                    ))
                    continue
                except Exception as exc:
                    logger.warning("actor profile aspect %s failed: %s", aspect.name, exc)
                    statuses.append(_failure_status(aspect.name, exc))
                    continue
                statuses.append(_absorb(payload, aspect, rows, spec))
        except FuturesTimeout:
            logger.warning(
                "build_actor_profile: techo global de %ss alcanzado — perfil parcial",
                ACTOR_PROFILE_TIMEOUT_GLOBAL,
            )

    # Lo que nunca llegó a completarse es tan desconocido como lo que falló.
    for name in pending:
        statuses.append(AspectStatus(name, Outcome.ERROR, OutcomeReason.FETCH_TIMEOUT))

    return ActorProfileResult(
        queried_at=queried_at,
        identity=dict(params.get("actor_scope") or {}),
        dataset_id=dataset_id,
        contract_count=payload.get("contract_count"),
        total_reported_value=payload.get("total_reported_value"),
        first_observed_date=payload.get("first_observed_date"),
        last_observed_date=payload.get("last_observed_date"),
        buyers=payload.get("buyers", ()),
        years=payload.get("years", ()),
        modalities=payload.get("modalities", ()),
        territories=payload.get("territories", ()),
        observed_names=payload.get("observed_names", ()),
        recent_contracts=payload.get("recent_contracts", ()),
        aspects=tuple(sorted(statuses, key=lambda s: s.name)),
    )


def _absorb(
    payload: dict, aspect: ProfileAspect, rows: list[dict], spec: Any,
) -> AspectStatus:
    """Lee la respuesta del aspecto y decide su estado.

    Un parser que revienta cuenta como error de ese aspecto, no del perfil:
    una respuesta con forma inesperada en una distribución no puede costar
    el conteo de contratos.
    """
    try:
        value = aspect.parse(rows, spec)
    except Exception as exc:  # pragma: no cover - forma inesperada de la fuente
        logger.warning("actor profile aspect %s unparseable: %s", aspect.name, exc)
        return AspectStatus(aspect.name, Outcome.ERROR, OutcomeReason.QUERY_ERROR)

    if isinstance(value, dict):
        payload.update(value)
    else:
        payload[aspect.name] = value

    if not rows:
        # Cero filas CONFIRMADAS por la fuente. Distinto de no haber podido
        # preguntar, y esa diferencia es la doctrina de outcome.py.
        return AspectStatus(aspect.name, Outcome.EMPTY, OutcomeReason.ZERO_RESULTS)
    return AspectStatus(aspect.name, Outcome.SUCCESS, OutcomeReason.RESULTS_FOUND)


def _failure_status(name: str, exc: Exception) -> AspectStatus:
    """Traduce una excepción de transporte al vocabulario de ``outcome.py``."""
    from app.core.workflow.helpers import _is_timeout_exception

    reason = (
        OutcomeReason.FETCH_TIMEOUT if _is_timeout_exception(exc)
        else OutcomeReason.QUERY_ERROR
    )
    return AspectStatus(name, Outcome.ERROR, reason)
