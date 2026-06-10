"""value_stats.py — estadísticas de valor (promedio, mínimo, máximo).

Para entes de control: ¿cuál es el valor típico (y los extremos) de los contratos de
un universo? Útil para detectar contratos atípicamente grandes. Reusa
soql_builder.build_value_stats (SELECT avg/min/max ... WHERE ... AND v IS NOT NULL).
"""

from __future__ import annotations

import re
from typing import Any

from app.core.analytics.base import BaseAnalyzer, format_currency

_VALUE_STATS_PATTERNS = re.compile(
    r"\b("
    r"valor\s+promedio|"
    r"promedio\s+(de\s+)?(contrat|valor)|"
    r"monto\s+promedio|"
    r"ticket\s+promedio|"
    r"cu[áa]nto\s+vale\s+en\s+promedio|"
    r"valor\s+(m[íi]nimo|m[áa]ximo)|"
    r"contrato\s+m[áa]s\s+(grande|caro|costoso)"
    r")\b",
    re.IGNORECASE,
)


class ValueStatsAnalyzer(BaseAnalyzer):
    intent = "value_stats"
    requires_scope = True
    scope_hint = (
        "Necesito un filtro (objeto, entidad, ciudad o departamento) para calcular "
        "las estadísticas de valor."
    )
    error_hint = "No pude calcular las estadísticas de valor con los filtros actuales."

    def detect(self, query: str, params: dict) -> bool:
        return bool(_VALUE_STATS_PATTERNS.search(query))

    def build(self, soql_builder: Any, dataset_id: str, params: dict) -> str:
        return soql_builder.build_value_stats(dataset_id, params)

    def format(self, rows: list[dict], params: dict, dataset_id: str) -> str:
        if not rows or rows[0].get("mean") is None:
            return "No encontré registros con valor para ese universo."
        r = rows[0]
        mean = float(r.get("mean") or 0)
        min_val = float(r.get("min_val") or 0)
        max_val = float(r.get("max_val") or 0)
        scope = params.get("ciudad") or params.get("departamento_resolved") or "el universo consultado"
        return (
            f"Valor de los contratos en {scope}: "
            f"promedio ${format_currency(mean)}, "
            f"mínimo ${format_currency(min_val)}, "
            f"máximo ${format_currency(max_val)}."
        )
