"""aggregate_sum.py — análisis "valor total contratado" (AQ-001A).

Extraído sin cambio de comportamiento del antiguo app/core/analytics.py.
Las funciones build_aggregate_sum_query / format_aggregate_sum_response se conservan
como API pública (las importan tests y scripts); el analyzer las reusa.
"""

from __future__ import annotations

import re
from typing import Any

from app.core.analytics.base import BaseAnalyzer, format_currency

_AGGREGATE_SUM_PATTERNS = re.compile(
    r"\b("
    r"cu[áa]nto\s+se\s+contrat[óo]|"
    r"valor\s+total\s+contratado|"
    r"suma\s+de\s+contratos|"
    # CORPUS-AN003: aceptar plural ("cuánto suman") y pretérito ("sumaron").
    r"cu[áa]nto\s+suma[n]?|cu[áa]nto\s+sumaron|"
    r"total\s+contratado"
    r")\b",
    re.IGNORECASE,
)


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


def format_aggregate_sum_response(rows: list[dict], params: dict, dataset_id: str) -> str:
    """Formatea respuesta analítica breve y asesora."""
    if not rows or not rows[0].get("total_value"):
        return "No encontré registros con valor para ese universo."

    total = rows[0].get("total_value", 0)
    count = rows[0].get("total_count", 0)

    valor_str = format_currency(total)
    scope = params.get("ciudad") or params.get("departamento_resolved") or "el universo consultado"

    return (
        f"Para ese universo encontré un valor total contratado aproximado de ${valor_str} "
        f"en {count:,} registros.\n\n"
        f"Universo analizado: {scope}."
    )


class AggregateSumAnalyzer(BaseAnalyzer):
    intent = "aggregate_sum"
    requires_scope = True
    scope_hint = (
        "Necesito al menos un filtro (objeto, entidad, ciudad o departamento) "
        "para calcular el total."
    )
    error_hint = "No pude calcular el total con los filtros actuales."

    def detect(self, query: str, params: dict) -> bool:
        return bool(_AGGREGATE_SUM_PATTERNS.search(query))

    def build(self, soql_builder: Any, dataset_id: str, params: dict) -> str:
        return build_aggregate_sum_query(soql_builder, dataset_id, params)

    def format(self, rows: list[dict], params: dict, dataset_id: str) -> str:
        return format_aggregate_sum_response(rows, params, dataset_id)

    def total_count(self, rows: list[dict]) -> int:
        return int(rows[0].get("total_count", 0)) if rows else 0
