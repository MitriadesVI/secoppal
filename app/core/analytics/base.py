"""base.py — contrato común de la capa analítica modular.

Cada tipo de análisis (aggregate_sum, ranking, temporal…) es un Analyzer que vive
en su propio módulo y se registra en registry.py. La lógica de orquestación
(scope-guard, ejecución, manejo de error/clarification) NO se repite por analyzer:
vive una sola vez en registry._run. Añadir un análisis = 1 archivo + 1 línea en REGISTRY.
"""

from __future__ import annotations

from typing import Any

from app.core._followup_constants import SCOPE_TOPIC_KEYS


def has_analytics_scope(params: dict) -> bool:
    """Verifica que haya al menos un scope/topic real (filtro) en los params.

    Sin esto, una query analítica agregaría sobre TODO el universo (peligroso).
    """
    for key in SCOPE_TOPIC_KEYS:
        if params.get(key):
            return True
    return False


def format_currency(total: float | int) -> str:
    """Formatea valores grandes en pesos con singular/plural correcto.

    Compartido por todos los analyzers que reportan montos.
    """
    if total >= 1_000_000_000_000:  # billón colombiano = 10^12 (millón de millones)
        val = total / 1_000_000_000_000
        if val == 1:
            return "1 billón"
        return f"{int(val)} billones" if val == int(val) else f"{val:.1f} billones"
    elif total >= 1_000_000_000:
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


class BaseAnalyzer:
    """Clase base de un analizador. Subclasear y definir detect/build/format.

    Atributos de clase que la subclase puede sobrescribir:
      - intent: identificador del análisis (entra en route_reason="analytics_{intent}").
      - requires_scope: si True, registry exige has_analytics_scope antes de ejecutar.
      - scope_hint / error_hint: mensajes de clarificación específicos del análisis.
    """

    intent: str = "analytical"
    requires_scope: bool = True
    scope_hint: str = "Necesito al menos un filtro (objeto, entidad, ciudad o departamento)."
    error_hint: str = "No pude calcular el resultado con los filtros actuales."

    def detect(self, query: str, params: dict) -> bool:
        """¿Esta query pide este tipo de análisis?"""
        raise NotImplementedError

    def build(self, soql_builder: Any, dataset_id: str, params: dict) -> str:
        """SoQL del análisis. Reusa soql_builder.* y su lógica de filtros."""
        raise NotImplementedError

    def format(self, rows: list[dict], params: dict, dataset_id: str) -> str:
        """Respuesta conversacional grounded en las filas agregadas."""
        raise NotImplementedError

    def total_count(self, rows: list[dict]) -> int:
        """Nº de registros del universo analizado (para el state). Default 0."""
        return 0
