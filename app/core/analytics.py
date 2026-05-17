"""analytics.py — AQ-001A: capa de producto para aggregate_sum.

No debe usar observer.py como API pública.
"""

from __future__ import annotations

import re
from typing import Any

from app.core._followup_constants import SCOPE_TOPIC_KEYS


_AGGREGATE_SUM_PATTERNS = re.compile(
    r"\b("
    r"cuánto se contrató|cuanto se contrato|"
    r"valor total contratado|"
    r"suma de contratos|"
    r"cuánto suma|cuanto suma|"
    r"total contratado"
    r")\b",
    re.IGNORECASE,
)


def detect_analytical_intent(query: str, params: dict | None = None) -> str | None:
    """Detecta si la query pide aggregate_sum."""
    if _AGGREGATE_SUM_PATTERNS.search(query):
        return "aggregate_sum"
    return None


def has_analytics_scope(params: dict) -> bool:
    """Verifica que haya al menos un scope/topic real."""
    for key in SCOPE_TOPIC_KEYS:
        if params.get(key):
            return True
    return False


def build_aggregate_sum_query(soql_builder: Any, dataset_id: str, params: dict) -> str:
    """Construye SoQL de suma reutilizando la lógica de filtros existente."""
    spec = soql_builder.SPECS.get(dataset_id)
    if not spec:
        raise ValueError(f"Dataset desconocido: {dataset_id}")

    where_clause = soql_builder._build_where(spec, params)

    if "1=1" in where_clause or not where_clause.strip():
        raise ValueError("Query analítica sin filtros suficientes")

    value_field = spec.value
    return f"SELECT SUM({value_field}) as total_value, COUNT(*) as total_count WHERE {where_clause}"


def _format_currency(total: float | int) -> str:
    """Formatea valores grandes con singular/plural correcto."""
    if total >= 1_000_000_000:
        val = total / 1_000_000_000
        return f"{val:.1f} mil millones"
    elif total >= 1_000_000:
        val = total / 1_000_000
        if val == 1:
            return "1 millón"
        elif val == int(val):
            return f"{int(val)} millones"
        else:
            return f"{val:.1f} millones"
    else:
        return f"{int(total):,}"


def format_aggregate_sum_response(rows: list[dict], params: dict, dataset_id: str) -> str:
    """Formatea respuesta analítica breve y asesora."""
    if not rows or not rows[0].get("total_value"):
        return "No encontré registros con valor para ese universo."

    total = rows[0].get("total_value", 0)
    count = rows[0].get("total_count", 0)

    valor_str = _format_currency(total)

    scope = params.get("ciudad") or params.get("departamento_resolved") or "el universo consultado"

    return (
        f"Para ese universo encontré un valor total contratado aproximado de ${valor_str} "
        f"en {count:,} registros.\n\n"
        f"Universo analizado: {scope}."
    )


def maybe_handle_analytical_query(
    user_query: str,
    params: dict,
    dataset_id: str,
    secop_client: Any,
    soql_builder: Any,
    channel: str = "whatsapp",
) -> dict | None:
    """Punto de entrada controlado para analytical queries.

    No infla run_query. Retorna None si no aplica.
    """
    intent = detect_analytical_intent(user_query)
    if intent != "aggregate_sum":
        return None

    if not has_analytics_scope(params):
        return {
            "needs_clarification": True,
            "clarification_reason": "Necesito al menos un filtro (objeto, entidad, ciudad o departamento) para calcular el total.",
            "analytical_intent": "aggregate_sum",
            "route_reason": "analytics_aggregate_sum_no_scope",
        }

    try:
        soql = build_aggregate_sum_query(soql_builder, dataset_id, params)
        rows = secop_client.aggregate(dataset_id, soql, timeout=10)
        response = format_aggregate_sum_response(rows, params, dataset_id)

        return {
            "response": response,
            "analytical_intent": "aggregate_sum",
            "route_reason": "analytics_aggregate_sum",
            "dataset_id": dataset_id,
            "results": [],
            "total_count": int(rows[0].get("total_count", 0)) if rows else 0,
            "needs_clarification": False,
        }
    except Exception:
        return {
            "needs_clarification": True,
            "clarification_reason": "No pude calcular el total con los filtros actuales.",
            "analytical_intent": "aggregate_sum",
            "route_reason": "analytics_aggregate_sum_error",
        }