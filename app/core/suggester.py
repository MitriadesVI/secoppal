"""suggester.py — Tarea 2.8: Motor de sugerencias accionables para SECOPPAL.

Genera 2-3 Suggestion objects basados en params actuales, universe_insights,
y total_count. Cada suggestion es una query pre-armada que el usuario puede
ejecutar con un clic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.core.observer import UniverseInsights


@dataclass
class Suggestion:
    label: str                              # Texto mostrado al usuario
    modified_params: dict                   # Cambios sobre params actuales
    expected_count: int | None = None       # Total esperado (opcional, v1.3)
    reason: str = ""                        # Por qué esta sugerencia es útil


def generate_suggestions(
    params: dict,
    universe_insights: "UniverseInsights | None",
    total_count: int,
    rows: list[dict],
    dataset_id: str,
) -> list[Suggestion]:
    """Genera 2-3 sugerencias basadas en el estado actual de la consulta."""
    _ESTADO_FAMILIES = [
        ("Ver contratos activos (en ejecución)", ["En ejecución", "Modificado", "Prorrogado"], "Contratos actualmente activos"),
        ("Ver contratos cerrados", ["Cerrado"], "Contratos con gestión cerrada"),
        ("Ver suspendidos", ["Suspendido"], "Contratos suspendidos"),
        ("Ver terminados", ["terminado"], "Contratos que finalizaron"),
    ]
    suggestions: list[Suggestion] = []
    params = params or {}

    # 1. Familias de estado_contrato reales (contratos dataset)
    if dataset_id == "jbjy-vk9h" and "estado" not in params:
        # Solo la primera familia (firmados) compite por los 3 slots.
        # Las otras dos se agregan si hay espacio después de ordering/contratista/dataset.
        suggestions.append(Suggestion(
            label=_ESTADO_FAMILIES[0][0],
            modified_params={**params, "estado": _ESTADO_FAMILIES[0][1]},
            reason=_ESTADO_FAMILIES[0][2],
        ))

    # 2. Sugerencia de orden por valor
    if params.get("ordering_signal") != "valor_desc" and total_count > 1:
        suggestions.append(Suggestion(
            label="Ordenar por mayor valor",
            modified_params={**params, "ordering_signal": "valor_desc"},
            reason="Ver los contratos de mayor cuantía primero",
        ))

    # 3. Sugerencia de dataset switch
    if total_count > 0:
        other = "procesos" if dataset_id == "jbjy-vk9h" else "contratos"
        suggestions.append(Suggestion(
            label=f"Buscar en {other}",
            modified_params={**params, "dataset": other},
            reason=f"Explorar resultados similares en {other}",
        ))

    # Las otras 3 familias de estado se agregan si hay espacio
    if dataset_id == "jbjy-vk9h" and "estado" not in params and len(suggestions) < 3:
        for label, values, reason in _ESTADO_FAMILIES[1:]:
            if len(suggestions) >= 3:
                break
            suggestions.append(Suggestion(
                label=label,
                modified_params={**params, "estado": values},
                reason=reason,
            ))

    return suggestions[:3]  # máximo 3 sugerencias
