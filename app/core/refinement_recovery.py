"""Recuperación de refinamientos vacíos — Etapa 4.

Cuando un refinamiento deja 0 resultados pero la búsqueda anterior SÍ tenía
(``zero_results_after_refinement``), no fingimos que el usuario fue ambiguo: le
decimos qué filtro vació la búsqueda y le ofrecemos deshacerlo o ampliar.

El ``failed_refinement_delta`` se CALCULA como diff entre el frame del último turno
exitoso y el intento actual — no se guarda una copia paralela de params.
"""
from __future__ import annotations

from typing import Any

_EMPTY = (None, [], "", {}, False)

# Etiquetas legibles por campo para el mensaje de recuperación.
_FIELD_LABELS = {
    "ciudad": "ciudad", "ciudad_resolved": "ciudad",
    "departamento_resolved": "departamento", "departamentos_in": "departamento",
    "entidad_resolved": "entidad", "entidad_like": "entidad",
    "valor_min": "valor mínimo", "valor_max": "valor máximo",
    "fecha_desde": "fecha desde", "fecha_hasta": "fecha hasta",
    "estado_family": "estado", "estado": "estado", "modalidad": "modalidad",
    "objeto": "tema", "objeto_or": "tema", "objeto_phrases": "tema",
    "objeto_exclude": "exclusión",
}


def refinement_delta(previous_params: dict, current_params: dict) -> dict[str, Any]:
    """Campos que el turno actual AÑADIÓ o cambió respecto al último exitoso."""
    prev = previous_params or {}
    curr = current_params or {}
    delta: dict[str, Any] = {}
    for key, value in curr.items():
        if value in _EMPTY:
            continue
        if prev.get(key) != value:
            delta[key] = value
    return delta


def describe_delta(delta: dict[str, Any]) -> str:
    """Frase humana de los filtros añadidos: 'ciudad y valor mínimo'."""
    labels: list[str] = []
    for key in delta:
        label = _FIELD_LABELS.get(key)
        if label and label not in labels:
            labels.append(label)
    if not labels:
        return "ese filtro"
    if len(labels) == 1:
        return labels[0]
    return ", ".join(labels[:-1]) + " y " + labels[-1]


def build_recovery_message(delta: dict[str, Any]) -> str:
    """Mensaje honesto de zero-after-refinement con opciones de recuperación."""
    what = describe_delta(delta)
    return (
        f"🔍 No encontré resultados al aplicar {what}. "
        "La búsqueda anterior sí tenía resultados.\n\n"
        "¿Qué prefieres?\n"
        f"1. Deshacer {what} y volver a la búsqueda anterior\n"
        "2. Ampliar la búsqueda (quitar algunos filtros)\n"
        "3. Cambiar el tema"
    )
