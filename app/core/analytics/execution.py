"""execution.py — análisis de EJECUCIÓN: contratado vs pagado.

Para entes de control: ¿de lo contratado, cuánto se ha pagado realmente? El gap es
la ejecución pendiente. Solo aplica a contratos (procesos no tiene valor_pagado).

DECISIÓN DE DATOS (auditoría Fase 0, 2026-06-09): el campo crudo
`valor_pendiente_de_ejecucion` está CONTAMINADO (suma 10× el contratado; outliers de
~$10^15 en un solo contrato). Por eso el pendiente se DERIVA aquí (contratado − pagado),
nunca se lee de ese campo. `valor_del_contrato` y `valor_pagado` sí tienen cobertura
100% y magnitudes coherentes ($222B vs $35.6B en el universo Antioquia).
"""

from __future__ import annotations

import re
from typing import Any

from app.core.analytics.base import BaseAnalyzer, format_currency

# Patrones de EJECUCIÓN. Deliberadamente NO capturan "en ejecución" (estado de
# contrato, no análisis) — "ejecución" exige un sufijo presupuestal explícito.
_EXECUTION_PATTERNS = re.compile(
    r"\b("
    r"cu[áa]nto\s+se\s+(ha\s+)?pag[óo]|"
    r"cu[áa]nto\s+(se\s+)?ha\s+pagado|"
    r"cu[áa]nto\s+(se\s+)?ha\s+ejecutado|"
    r"ejecuci[óo]n\s+(presupuestal|del\s+contrato|de\s+los\s+contratos|de\s+contratos)|"
    r"nivel\s+de\s+ejecuci[óo]n|"
    r"contratado\s+vs\.?\s+pagado|"
    r"cu[áa]nto\s+falta\s+por\s+(pagar|ejecutar)"
    r")\b",
    re.IGNORECASE,
)


class ExecutionAnalyzer(BaseAnalyzer):
    intent = "execution_status"
    requires_scope = True
    scope_hint = (
        "Necesito un filtro (objeto, entidad, ciudad o departamento) "
        "para calcular la ejecución (contratado vs pagado)."
    )
    error_hint = (
        "No pude calcular la ejecución: la diferencia contratado vs pagado "
        "aplica a contratos con al menos un filtro."
    )

    def detect(self, query: str, params: dict) -> bool:
        return bool(_EXECUTION_PATTERNS.search(query))

    def build(self, soql_builder: Any, dataset_id: str, params: dict) -> str:
        # Solo contratos: procesos (precio_base) no tiene valor pagado.
        if dataset_id != soql_builder.CONTRATOS_DATASET:
            raise ValueError("La ejecución contratado/pagado solo aplica a contratos")
        spec = soql_builder.SPECS.get(dataset_id)
        if not spec:
            raise ValueError(f"Dataset desconocido: {dataset_id}")
        where = soql_builder._build_where(spec, params)
        if "1=1" in where or not where.strip():
            raise ValueError("Query de ejecución sin filtros suficientes")
        return (
            "SELECT sum(valor_del_contrato) as contratado, "
            "sum(valor_pagado) as pagado, count(*) as total_count "
            f"WHERE {where}"
        )

    def format(self, rows: list[dict], params: dict, dataset_id: str) -> str:
        if not rows or not rows[0].get("contratado"):
            return "No encontré contratos con valor para ese universo."
        contratado = float(rows[0].get("contratado") or 0)
        pagado = float(rows[0].get("pagado") or 0)
        count = int(rows[0].get("total_count") or 0)
        # Pendiente DERIVADO — nunca el campo crudo (contaminado, ver docstring).
        pendiente = max(contratado - pagado, 0)
        pct = (pagado / contratado * 100) if contratado else 0
        scope = params.get("ciudad") or params.get("departamento_resolved") or "el universo consultado"
        count_label = "filas físicas" if params.get("buyer_group") else "contratos"
        return (
            f"De ${format_currency(contratado)} contratados, se han pagado "
            f"${format_currency(pagado)} ({pct:.0f}% ejecutado). "
            f"Pendiente por ejecutar: ${format_currency(pendiente)}.\n\n"
            f"Universo analizado: {scope} ({count:,} {count_label})."
        )

    def total_count(self, rows: list[dict]) -> int:
        return int(rows[0].get("total_count", 0)) if rows else 0
