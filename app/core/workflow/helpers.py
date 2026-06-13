"""Pure helpers extracted from orchestrator.py (Paso 1 of workflow refactor).

These functions have **no domain logic** — they are utilities used by
multiple actions/handlers and by the SecopalWorkflow facade:

- ``_elapsed_ms``: stopwatch in ms.
- ``_with_timing`` / ``_with_timings``: Burr State helpers that merge
  timing measurements into ``state.timings_ms``.
- ``_query_first_then_count``: SECOP query orchestration that skips the
  count call when the result set is short.
- ``_is_timeout_exception``: walks exception chains to detect timeouts.
- ``_relax_params``: drops the most restrictive filter for degrade retry.
- ``_build_timeout_suggestions``: per-intent suggestion list for timeouts.

Moved here verbatim. **No behavior changes.** Verified by the golden
contract suite (``tests/test_orchestrator_golden.py``).
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from burr.core import State

from app.core._followup_constants import SCOPE_TOPIC_KEYS
from app.core.secop_client import SecopClient
from app.core.soql_builder import SoQLBuilder

if TYPE_CHECKING:
    from app.core.workflow.deps import WorkflowDeps


def _elapsed_ms(start: float) -> float:
    return round((time.perf_counter() - start) * 1000, 2)


# ---------------------------------------------------------------------------
# execute_resolved_params — núcleo de búsqueda CONVERSACIONALMENTE PURO
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ResolvedExecution:
    """Resultado de ejecutar params ya resueltos contra SECOP.

    Es la salida del núcleo compartido por ``handle_refinement`` y el
    Radar (y, a futuro, export Excel / jobs async). No contiene ningún
    efecto conversacional: ni trace de feedback, ni turno de historial.
    """

    results: list[dict]
    rows: list[dict]
    response: str
    total: int
    soql: str
    dataset_id: str
    approximate: bool


def execute_resolved_params(
    deps: "WorkflowDeps",
    params: dict,
    *,
    channel: str = "whatsapp",
) -> ResolvedExecution | None:
    """Ejecuta params ya resueltos: params → SoQL → guard → query → format.

    **Núcleo puro en términos conversacionales.** Hace exactamente:
    build SoQL + count → guard anti-WHERE-1=1 → ``_query_first_then_count``
    → ``format_for_channel``. **No** hace ``append_turn``, **no** registra
    feedback/trace, **no** muta contexto ni escribe historial. Esa es la
    regla que permite que el Radar reutilice la búsqueda sin contaminar la
    conversación.

    Retorna ``None`` cuando el guard anti-1=1 falla (sin scope/topic) o la
    consulta lanza excepción, señalando al caller que caiga a otro flujo.
    Nunca muta ``params`` recibido (trabaja sobre una copia).
    """
    params = dict(params)
    # offset/limit deben empezar de cero: es una búsqueda nueva, no paginación.
    params.pop("offset", None)
    params.setdefault("limit", 50)

    # Guard anti-WHERE 1=1: sin scope ni topic no se ejecuta.
    has_scope_or_topic = any(
        params.get(k) and params.get(k) not in (None, [], "", {}, False)
        for k in SCOPE_TOPIC_KEYS
    )
    if not has_scope_or_topic:
        return None

    ds_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
    try:
        soql = deps.soql_builder.build(ds_id, params)
        count_soql = deps.soql_builder.build_count(ds_id, params)
        total, results, _t, approx = _query_first_then_count(
            deps.secop_client, ds_id, count_soql, soql,
            limit=int(params.get("limit", 50) or 50),
        )
    except Exception:
        return None

    response, rows = deps.formatter.format_for_channel(
        results, ds_id, channel,
        params=params, total_count=total,
        total_count_approximate=approx,
    )
    return ResolvedExecution(
        results=results,
        rows=rows,
        response=response,
        total=total,
        soql=soql,
        dataset_id=ds_id,
        approximate=approx,
    )


def _with_timing(state: State, key: str, start: float, **updates) -> State:
    timings = dict(state.get("timings_ms", {}) or {})
    timings[key] = _elapsed_ms(start)
    return state.update(**updates, timings_ms=timings)


def _with_timings(state: State, timings_update: dict[str, float], **updates) -> State:
    timings = dict(state.get("timings_ms", {}) or {})
    timings.update(timings_update)
    return state.update(**updates, timings_ms=timings)


def _query_first_then_count(
    secop_client: SecopClient,
    dataset_id: str,
    count_soql: str,
    soql_query: str,
    limit: int = 50,
) -> tuple[int, list[dict], dict[str, float], bool]:
    """Run query first; skip count if <limit rows. If ==limit, count with short timeout.

    Returns (total, results, timings, approximate).
    approximate=True when count failed but query returned limit rows.
    """
    start = time.perf_counter()

    # Step 1: query first
    query_start = time.perf_counter()
    results = secop_client.query(dataset_id, soql_query)
    query_ms = _elapsed_ms(query_start)

    shown = len(results)
    if shown < limit:
        # Exact count known; no need to hit SECOP count
        return shown, results, {
            "secop_count_ms": 0,
            "secop_query_ms": query_ms,
        }, False

    # Step 2: count — if it fails we show "X+"
    count_start = time.perf_counter()
    try:
        total = secop_client.count(dataset_id, count_soql)
        count_ms = _elapsed_ms(count_start)
        return total, results, {
            "secop_count_ms": count_ms,
            "secop_query_ms": query_ms,
        }, False
    except Exception:
        count_ms = _elapsed_ms(count_start)
        return shown, results, {
            "secop_count_ms": count_ms,
            "secop_query_ms": query_ms,
        }, True


def _is_timeout_exception(exc: BaseException) -> bool:
    seen: set[int] = set()
    current: BaseException | None = exc
    while current and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, TimeoutError):
            return True
        message = str(current).lower()
        if "timeout" in message or "timed out" in message:
            return True
        current = current.__cause__ or current.__context__
    return False


def _relax_params(params: dict) -> tuple[dict | None, str]:
    """Remove the single most restrictive filter and return (relaxed_params, hint).
    Priority (most → least restrictive to drop): estado, fecha, valor, departamento.
    Returns (None, "") if nothing left to relax.
    """
    p = dict(params)

    if "estado" in p:
        del p["estado"]
        return p, "sin filtro de estado"

    if "fecha_desde" in p or "fecha_hasta" in p:
        original_from = p.pop("fecha_desde", None) or ""
        original_to = p.pop("fecha_hasta", None) or ""
        # AUTO-FALLBACK-WITHOUT-QUALITY: avisa que el año cambió
        if original_from and original_to:
            hint = f"sin filtro de fechas (pediste {original_from[:4]}-{original_to[:4]}, ampliado a todos los años)"
        elif original_from:
            hint = f"sin filtro de fechas (pediste desde {original_from[:4]}, ampliado a todos los años)"
        else:
            hint = "sin filtro de fechas"
        return p, hint

    if "valor_min" in p or "valor_max" in p:
        p.pop("valor_min", None)
        p.pop("valor_max", None)
        return p, "sin filtro de valor"

    if "departamento_resolved" in p or "departamento" in p:
        p.pop("departamento_resolved", None)
        p.pop("departamento", None)
        p.pop("departamento_resolution", None)
        return p, "en todo el pais"

    return None, ""


def _build_timeout_suggestions(params: dict, total_count: int) -> list[dict]:
    """Generate suggestions for timeout scenarios.
    For opportunity_search: no quitar fecha if none existed.
    Suggest acotar por lugar/frescura/subtipo instead.
    """
    suggestions = []
    is_opportunity = params.get("intent_type") == "opportunity_search"
    has_fecha = bool(params.get("fecha_desde"))

    if not has_fecha and not is_opportunity:
        suggestions.append({
            "label": "Solo 2026",
            "modified_params": {**params, "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31"},
            "reason": "Filtrar solo contratos de 2026 para reducir el universo",
        })
        suggestions.append({
            "label": "Ultimos 3 anos",
            "modified_params": {**params, "fecha_desde": "2023-01-01", "fecha_hasta": "2026-12-31"},
            "reason": "Contratos de los ultimos 3 anos",
        })

    if is_opportunity:
        suggestions.append({
            "label": "Acotar por lugar o entidad",
            "modified_params": params,
            "reason": "Agregar ciudad o entidad para reducir resultados",
        })
        suggestions.append({
            "label": "Ultimos 45 dias",
            "modified_params": {**params, "fecha_desde": "2026-04-01", "fecha_hasta": "2026-05-17"},
            "reason": "Ventana reciente para oportunidades activas",
        })
    else:
        suggestions.append({
            "label": "Sin ordenar por valor",
            "modified_params": {k: v for k, v in params.items() if k != "ordering_signal"},
            "reason": "Quitar el orden por valor para que la busqueda sea mas rapida",
        })

    return suggestions[:3]
