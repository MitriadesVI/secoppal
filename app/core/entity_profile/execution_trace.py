"""Lo que un turno del carril PROFILE deja ESCRITO sobre su ejecución.

El incidente DOGFOOD-001 dejó una traza en la que todo lo que se necesitaba
saber estaba ausente: la traza decía que el turno planeó dos jobs, que la
latencia de proveedores fue de 12,8 s y que los resultados fueron cero. No decía
si los jobs se intentaron, con qué estado terminaron ni cuántas filas volvieron.
Con eso, «SECOP expiró» y «no hay actividad» se leen igual.

Aquí cada job del Frame deja un recibo de seis campos y el turno credenciado
deja sus autoridades. Dos reglas, las mismas que gobiernan el recibo A2:

* Valores enumerados o numéricos. Nunca el SoQL, nunca una clave, nunca la
  respuesta de la fuente. Un recibo no puede convertirse en una segunda copia
  no auditada de la consulta.
* Un campo que el runtime no fijó está AUSENTE, no puesto por defecto. Un
  ``False`` fabricado en un contador de seguridad es peor que un hueco.
"""
from __future__ import annotations

import math

# Los que emite `frame_runner.execute_plan` más los que la acreditación de
# métricas puede declarar sobre una respuesta que sí llegó.
JOB_STATUSES = frozenset({
    "SUCCESS", "TIMEOUT", "ERROR", "BUDGET_EXHAUSTED", "UNAVAILABLE",
    "INVALID_RESPONSE", "INCONSISTENT_RESPONSE",
})

# El plan nunca autoriza más (`PROFILE_QUERY_BUDGET`); el tope es del recibo.
MAX_JOBS = 6

# Campos planos que la admisión credenciada ya adjunta al result y que hasta
# ahora sólo vivían en memoria: `log_trace` los descartaba por no estar en su
# lista. Se persisten con su tipo declarado o no se persisten.
_TEXT_FIELDS = (
    "credential_capability", "credential_source", "admission_owner",
    "admission_reason", "authority_status", "authority_reason",
    "residual_verifier_status", "residual_verifier_contract",
)
_BOOL_FIELDS = (
    "legacy_bypassed", "residual_verifier_called", "weak_actor_guard_called",
)
_INT_FIELDS = ("query_router_calls",)
_FLOAT_FIELDS = ("residual_verifier_latency_ms",)

_MAX_TEXT = 64
_MAX_UNIT_TEXT = 120
_MAX_UNITS = 12


def job_receipts(evidence) -> tuple[dict, ...]:
    """Un recibo por job del plan: intentado, estado, filas y latencia.

    `attempted` y `status` son cosas distintas y se guardan por separado: un job
    con `attempted=False` no llegó a emitirse (el turno se quedó sin
    presupuesto), y confundirlo con uno que salió y expiró es exactamente el
    error que hace ilegible un incidente.
    """
    if evidence is None:
        return ()
    receipts = []
    for call in evidence.calls[:MAX_JOBS]:
        latency = evidence.latency_ms.get(call.job_id, 0.0)
        if type(latency) not in (int, float) or not math.isfinite(latency) or latency < 0:
            latency = 0.0
        receipts.append({
            "job_id": str(call.job_id)[:_MAX_TEXT],
            "dataset_id": str(call.dataset_id)[:_MAX_TEXT],
            "attempted": bool(call.attempted),
            "status": (call.status if call.status in JOB_STATUSES else "UNAVAILABLE"),
            "rows_received": len(evidence.rows_for(call.job_id)),
            "latency_ms": round(float(latency), 3),
        })
    return tuple(receipts)


def profile_trace_metadata(result: dict) -> dict:
    """Valida el recibo del turno. Lo que no se reconoce se descarta, no se acuña."""
    if type(result) is not dict:
        return {}
    clean: dict = {}
    jobs = _clean_jobs(result.get("profile_job_receipts"))
    if jobs:
        clean["profile_job_receipts"] = jobs
    for key in _TEXT_FIELDS:
        value = result.get(key)
        if type(value) is str and value:
            clean[key] = value[:_MAX_TEXT]
    for key in _BOOL_FIELDS:
        if type(result.get(key)) is bool:
            clean[key] = result[key]
    for key in _INT_FIELDS:
        value = result.get(key)
        if type(value) is int and not isinstance(value, bool) and value >= 0:
            clean[key] = value
    for key in _FLOAT_FIELDS:
        value = result.get(key)
        if type(value) in (int, float) and math.isfinite(value) and value >= 0:
            clean[key] = float(value)
    units = _clean_units(result.get("residual_units"))
    if units:
        clean["residual_units"] = units
    labels = _clean_labels(result.get("residual_labels"))
    if labels:
        clean["residual_labels"] = labels
    return clean


def _clean_jobs(raw) -> list[dict]:
    if type(raw) not in (list, tuple):
        return []
    jobs = []
    for item in tuple(raw)[:MAX_JOBS]:
        if type(item) is not dict or item.get("status") not in JOB_STATUSES:
            continue
        job_id, dataset = item.get("job_id"), item.get("dataset_id")
        rows, latency = item.get("rows_received"), item.get("latency_ms")
        if type(job_id) is not str or not job_id:
            continue
        if type(item.get("attempted")) is not bool:
            continue
        if type(rows) is not int or isinstance(rows, bool) or rows < 0:
            continue
        if type(latency) not in (int, float) or not math.isfinite(latency) or latency < 0:
            continue
        jobs.append({
            "job_id": job_id[:_MAX_TEXT],
            "dataset_id": dataset[:_MAX_TEXT] if type(dataset) is str else "",
            "attempted": item["attempted"],
            "status": item["status"],
            "rows_received": rows,
            "latency_ms": float(latency),
        })
    return jobs


def _clean_units(raw) -> list[dict]:
    """Los fragmentos que B vio. Son trozos de la consulta, ya persistida; se
    acotan igual para que un recibo no crezca hacia texto libre."""
    if type(raw) not in (list, tuple):
        return []
    units = []
    for item in tuple(raw)[:_MAX_UNITS]:
        if type(item) is not dict:
            continue
        unit_id, text = item.get("id"), item.get("text")
        if type(unit_id) is not str or type(text) is not str:
            continue
        units.append({"id": unit_id[:_MAX_TEXT], "text": text[:_MAX_UNIT_TEXT]})
    return units


def _clean_labels(raw) -> dict:
    if type(raw) is not dict:
        return {}
    return {
        str(key)[:_MAX_TEXT]: str(value)[:_MAX_TEXT]
        for key, value in tuple(raw.items())[:_MAX_UNITS]
        if type(key) is str and type(value) is str
    }


__all__ = ["JOB_STATUSES", "MAX_JOBS", "job_receipts", "profile_trace_metadata"]
