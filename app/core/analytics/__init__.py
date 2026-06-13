"""Capa analítica modular de SECOPPAL (rankings, sumas, evolución, comparativas).

Paquete (antes módulo único analytics.py). API pública estable — re-exportada aquí
para no romper importadores (workflow/actions/execution.py, tests, scripts):

  - maybe_handle_analytical_query : punto de entrada (itera REGISTRY)
  - detect_analytical_intent      : intent del primer analyzer que reconoce la query
  - has_analytics_scope           : guard de scope/topic
  - build_aggregate_sum_query     : SoQL de suma (aggregate_sum)
  - format_aggregate_sum_response : respuesta formateada (aggregate_sum)

Para añadir un análisis: crear analytics/<nombre>.py con un BaseAnalyzer y registrarlo
en analytics/registry.py REGISTRY. Cero cambios en los importadores.
"""

from __future__ import annotations

from app.core.analytics.aggregate_sum import (
    build_aggregate_sum_query,
    format_aggregate_sum_response,
)
from app.core.analytics.base import has_analytics_scope
from app.core.analytics.registry import (
    REGISTRY,
    detect_analytical_intent,
    maybe_handle_analytical_query,
)

__all__ = [
    "REGISTRY",
    "build_aggregate_sum_query",
    "detect_analytical_intent",
    "format_aggregate_sum_response",
    "has_analytics_scope",
    "maybe_handle_analytical_query",
]
