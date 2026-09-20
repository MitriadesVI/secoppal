"""Quién es dueño de la EJECUCIÓN y de la PRESENTACIÓN de un turno.

La revisión independiente (F5) demostró que el `EntityProfileExecutionPlan` no
era autoridad única: después de ejecutarlo, `handle_normal_flow` llamaba
incondicionalmente a `dual_dataset.build_dataset_groups`, que volvía a decidir
desde `dataset_scope` y el dataset escalar. Medido sobre 991a02f:

    petición                                    plan   observado
    procesos y contratos publicados en agosto   2      2 + 1 contrato
    proveedores de procesos y contratos         2      3 contratos
    montos de procesos y contratos              1      2 contratos

y la respuesta del perfil se reemplazaba por `Resultados en Procesos y
Contratos`, que llegaba a afirmar «Procesos: No encontré coincidencias» sin
ningún conteo de procesos que sostuviera esa ausencia.

El problema no era una palabra en `params` —borrar `dataset_scope` habría sido
un parche incidental— sino que nadie declaraba de quién es el resultado.

Este módulo no introduce una segunda fuente de verdad: **lee tipadamente la que
ya existe**. `analytical_intent` lo escribe `entity_profile_state` en los tres
finales posibles del perfil (respuesta, aclaración y error honesto), así que ya
identifica sin ambigüedad los turnos que el perfil posee. Aquí se le pone
nombre y se centraliza la decisión, en vez de repartir comparaciones de cadenas
por los handlers.
"""
from __future__ import annotations

from enum import Enum
from typing import Any

from app.core.workflow.actions.entity_profile import ENTITY_PROFILE_INTENT


class ResultOwner(str, Enum):
    """Quién produjo el resultado del turno y responde por él hasta el final."""

    TABULAR = "TABULAR"
    ENTITY_PROFILE = "ENTITY_PROFILE"
    CONTRACT_ACTIVITY = "CONTRACT_ACTIVITY"


# Intents cuya ejecución y presentación son TERMINALES: ningún postprocesador
# puede consultar la fuente ni reemplazar su respuesta después de ellos.
_TERMINAL_INTENTS = {
    ENTITY_PROFILE_INTENT: ResultOwner.ENTITY_PROFILE,
    "contract_activity": ResultOwner.CONTRACT_ACTIVITY,
}


def _intent(source: Any) -> str:
    if source is None:
        return ""
    try:
        value = source.get("analytical_intent", "")
    except (AttributeError, TypeError):
        return ""
    return value if isinstance(value, str) else ""


def result_owner(result: Any = None, state: Any = None) -> ResultOwner:
    """El dueño del turno; TABULAR mientras nadie lo reclame."""
    for source in (result, state):
        owner = _TERMINAL_INTENTS.get(_intent(source))
        if owner is not None:
            return owner
    return ResultOwner.TABULAR


def owns_execution_and_presentation(result: Any = None, state: Any = None) -> bool:
    """¿Este turno ya tiene dueño terminal?

    Se consulta ANTES de cualquier expansión, formateo alternativo o arranque
    de paginación. Una aclaración y un error honesto del perfil cuentan igual
    que una respuesta: los tres son resultados suyos.
    """
    return result_owner(result, state) is not ResultOwner.TABULAR
