"""temporal.py — evolución de la contratación por año.

Para entes de control: ¿cómo evolucionó la contratación de un universo en el tiempo?
Reusa soql_builder.build_temporal_dist (SELECT date_trunc_y(...) AS yr, count(*) AS cnt).
"""

from __future__ import annotations

import re
from typing import Any

from app.core.analytics.base import BaseAnalyzer

_TEMPORAL_PATTERNS = re.compile(
    r"\b("
    r"evoluci[óo]n|"
    r"c[óo]mo\s+ha\s+evolucionado|"
    r"por\s+a[ñn]o|"
    r"tendencia|"
    r"hist[óo]rico|"
    r"distribuci[óo]n\s+(temporal|por\s+a[ñn]o)|"
    r"contrataci[óo]n\s+anual|"
    r"a[ñn]o\s+por\s+a[ñn]o"
    r")\b",
    re.IGNORECASE,
)


class TemporalAnalyzer(BaseAnalyzer):
    intent = "temporal"
    requires_scope = True
    scope_hint = (
        "Necesito un filtro (objeto, entidad, ciudad o departamento) para mostrar "
        "la evolución de ese universo."
    )
    error_hint = "No pude calcular la evolución temporal con los filtros actuales."

    def detect(self, query: str, params: dict) -> bool:
        return bool(_TEMPORAL_PATTERNS.search(query))

    def build(self, soql_builder: Any, dataset_id: str, params: dict) -> str:
        return soql_builder.build_temporal_dist(dataset_id, params)

    def format(self, rows: list[dict], params: dict, dataset_id: str) -> str:
        if not rows:
            return "No encontré registros con fecha para ese universo."
        parts = []
        for row in rows:
            yr = str(row.get("yr", ""))[:4]  # "2026-01-01T..." → "2026"
            cnt = int(row.get("cnt", 0) or 0)
            if yr:
                parts.append(f"{yr}: {cnt:,}")
        scope = params.get("ciudad") or params.get("departamento_resolved") or "el universo consultado"
        return "Evolución por año en {}:\n{}".format(scope, "  |  ".join(parts))

    def total_count(self, rows: list[dict]) -> int:
        return sum(int(r.get("cnt", 0) or 0) for r in rows)
