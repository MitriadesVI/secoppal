"""suggester.py — Tarea 2.8: Motor de sugerencias accionables para SECOPPAL.

Genera 2-3 Suggestion objects basados en params actuales, universe_insights,
y total_count. Cada suggestion es una query pre-armada que el usuario puede
ejecutar con un clic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.core.modality_dataset_projection import procedure_id_for_raw_value

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
    intent_type: str = "",
) -> list[Suggestion]:
    """Genera 2-3 sugerencias basadas en el estado actual de la consulta.

    Ahora contextual por intent_type para orientar al usuario según su perfil.
    """
    _ESTADO_FAMILIES = [
        ("Ver contratos activos (en ejecución)", ["En ejecución", "Modificado", "Prorrogado"], "Contratos actualmente activos"),
        ("Ver contratos cerrados", ["Cerrado"], "Contratos con gestión cerrada"),
        ("Ver suspendidos", ["Suspendido"], "Contratos suspendidos"),
        ("Ver terminados", ["terminado"], "Contratos que finalizaron"),
    ]
    suggestions: list[Suggestion] = []
    params = params or {}

    # ── Contextual por intent_type (Sprint 3 fase 2) ────────────────────

    if intent_type == "opportunity_search":
        if params.get("valor_min") is None:
            suggestions.append(Suggestion(
                label="Restringir a >100 millones",
                modified_params={**params, "valor_min": 100_000_000},
                reason="Filtrar oportunidades de alta cuantía",
            ))
        if (
            not params.get("modalidad_include_procedure_ids")
            and universe_insights
            and universe_insights.modality_dominance_name
        ):
            dataset = (
                "contratos" if dataset_id == "jbjy-vk9h" else "procesos"
            )
            raw_value = universe_insights.modality_dominance_name
            procedure_id = procedure_id_for_raw_value(dataset, raw_value)
            if procedure_id:
                modified = {
                    **params,
                    "modalidad_include_procedure_ids": [procedure_id],
                }
                modified.pop("modalidad_exclude_procedure_ids", None)
                suggestions.append(Suggestion(
                    label=f"Filtrar por {raw_value}",
                    modified_params=modified,
                    reason="Concentrar en la modalidad más frecuente",
                ))
        if dataset_id == "p6dx-8zbt" and len(suggestions) < 3:
            suggestions.append(Suggestion(
                label="Ver contratos firmados",
                modified_params={**params, "dataset": "contratos"},
                reason="Explorar contratos ya ejecutados",
            ))

    elif intent_type == "contract_search":
        if "estado" not in params and dataset_id == "jbjy-vk9h":
            suggestions.append(Suggestion(
                label="Ver solo adjudicados",
                modified_params={**params, "estado": ["Adjudicado"]},
                reason="Filtrar contratos ya adjudicados",
            ))
        if params.get("ordering_signal") != "valor_desc" and total_count > 1:
            suggestions.append(Suggestion(
                label="Ordenar por mayor valor",
                modified_params={**params, "ordering_signal": "valor_desc"},
                reason="Ver los contratos de mayor cuantía primero",
            ))
        if len(suggestions) < 3:
            suggestions.append(Suggestion(
                label="Buscar en procesos",
                modified_params={**params, "dataset": "procesos"},
                reason="Explorar procesos de selección similares",
            ))

    elif intent_type == "entity_query":
        if dataset_id != "p6dx-8zbt":
            suggestions.append(Suggestion(
                label="Ver procesos abiertos",
                modified_params={**params, "dataset": "procesos", "intent_type": "opportunity_search"},
                reason="Ver oportunidades de esta entidad",
            ))
        if params.get("ordering_signal") != "valor_desc" and total_count > 1:
            suggestions.append(Suggestion(
                label="Ordenar por mayor valor",
                modified_params={**params, "ordering_signal": "valor_desc"},
                reason="Ver los de mayor cuantía primero",
            ))
        if len(suggestions) < 3:
            suggestions.append(Suggestion(
                label="Ver contratos",
                modified_params={**params, "dataset": "contratos"},
                reason="Explorar contratos de esta entidad",
            ))

    # ── Genéricas (fallback cuando hay espacio) ──────────────────────────

    # Familias de estado_contrato (contratos dataset)
    if dataset_id == "jbjy-vk9h" and "estado" not in params and len(suggestions) < 3:
        suggestions.append(Suggestion(
            label=_ESTADO_FAMILIES[0][0],
            modified_params={**params, "estado": _ESTADO_FAMILIES[0][1]},
            reason=_ESTADO_FAMILIES[0][2],
        ))

    # Orden por valor
    if params.get("ordering_signal") != "valor_desc" and total_count > 1 and len(suggestions) < 3:
        suggestions.append(Suggestion(
            label="Ordenar por mayor valor",
            modified_params={**params, "ordering_signal": "valor_desc"},
            reason="Ver los contratos de mayor cuantía primero",
        ))

    # Dataset switch
    if total_count > 0 and len(suggestions) < 3:
        other = "procesos" if dataset_id == "jbjy-vk9h" else "contratos"
        suggestions.append(Suggestion(
            label=f"Buscar en {other}",
            modified_params={**params, "dataset": other},
            reason=f"Explorar resultados similares en {other}",
        ))

    # Otras familias de estado si hay espacio
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
