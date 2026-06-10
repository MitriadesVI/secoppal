"""ranking.py — top entidades por número de contratos/procesos.

Para entes de control: ¿qué entidades concentran más contratación en un universo?
Reusa soql_builder.build_top_entities (ya existente, validado en auditoría Fase 0).
"""

from __future__ import annotations

import re
from typing import Any

from app.core.analytics.base import BaseAnalyzer

_RANKING_PATTERNS = re.compile(
    r"\b("
    r"top\s+(de\s+)?entidades|"
    r"ranking\s+de\s+entidades|"
    r"qu[ée]\s+entidades\s+(contratan|m[áa]s)|"
    r"entidades\s+que\s+m[áa]s\s+contratan|"
    r"principales\s+entidades|"
    r"qui[ée]n(es)?\s+contrata(n)?\s+m[áa]s"
    r")\b",
    re.IGNORECASE,
)


def _dim_key(row: dict) -> str | None:
    """Nombre de la columna-dimensión = la clave que no es 'cnt'."""
    for k in row:
        if k != "cnt":
            return k
    return None


class RankingAnalyzer(BaseAnalyzer):
    intent = "ranking"
    requires_scope = True
    scope_hint = (
        "Necesito un filtro (objeto, ciudad o departamento) para rankear "
        "las entidades de ese universo."
    )
    error_hint = "No pude calcular el ranking de entidades con los filtros actuales."

    def detect(self, query: str, params: dict) -> bool:
        return bool(_RANKING_PATTERNS.search(query))

    def build(self, soql_builder: Any, dataset_id: str, params: dict) -> str:
        return soql_builder.build_top_entities(dataset_id, params)

    def format(self, rows: list[dict], params: dict, dataset_id: str) -> str:
        if not rows:
            return "No encontré entidades para ese universo."
        lines = []
        for i, row in enumerate(rows, 1):
            key = _dim_key(row)
            nombre = str(row.get(key, "—"))[:50] if key else "—"
            cnt = int(row.get("cnt", 0) or 0)
            lines.append(f"{i}. {nombre} ({cnt:,} contratos)")
        scope = params.get("ciudad") or params.get("departamento_resolved") or "el universo consultado"
        return "Entidades que más contratan en {}:\n{}".format(scope, "\n".join(lines))

    def total_count(self, rows: list[dict]) -> int:
        return sum(int(r.get("cnt", 0) or 0) for r in rows)
