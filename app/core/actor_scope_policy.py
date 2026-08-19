"""Capacidad de dataset para consultas de actor — INV-NO-SILENT-DROP.

El defecto que este módulo cierra
---------------------------------
``soql_builder._build_where`` aplica el filtro de contratista así::

    if params.get("contratista") and spec.contractor:

En el dataset de procesos ``spec.contractor is None``, así que la condición
es falsa y **el filtro simplemente no se añade**. La consulta se ejecuta con
lo que quedó. AP0 lo verificó emitiendo el SoQL: ``"qué ha celebrado la
persona 1140835135 en 2025"`` producía una búsqueda de la palabra
``persona`` en 2025, y esa lista se presentaba como el historial de esa
persona.

Por qué el degradado silencioso es la peor de las opciones: no falla, no
avisa, y devuelve una respuesta con la forma correcta. Es indistinguible de
un acierto para quien pregunta, y lo que afirma es falso sobre un tercero
identificable.

La regla
--------
Cuando la consulta trae alcance de actor y el dataset objetivo no puede
proyectarlo, **no se ejecuta**: se pide la aclaración que corresponde. El
actor nunca se descarta para seguir adelante con los filtros restantes.

Antes de llegar aquí el router ya resolvió el caso barato: si el usuario no
pidió procesos explícitamente, un documento de actor promueve la consulta a
contratos, que sí publica quién contrata. Este módulo gobierna lo que queda
—el usuario pidió procesos a sabiendas— y ahí su intención manda: se le dice
que esa combinación no se puede servir, en vez de responderle otra cosa.

Capacidad, no lista
-------------------
Qué dataset soporta actor se pregunta al ``DatasetSpec``, no a una lista
escrita a mano. Si mañana la fuente de procesos publicara el documento del
proveedor, la política se entera sola; una lista habría que acordarse de
actualizarla.

Puro y determinista: sin red, sin reloj, sin ``Settings``.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.core.soql_builder import SoQLBuilder

__all__ = [
    "REASON_DATASET_UNSUPPORTED",
    "REASON_NAME_UNRESOLVED",
    "ActorScopeDecision",
    "dataset_supports_actor",
    "evaluate_actor_scope",
    "has_actor_scope",
]

REASON_DATASET_UNSUPPORTED = "actor_dataset_unsupported"
REASON_NAME_UNRESOLVED = "actor_name_requires_resolution"

_DATASET_LABELS = {"procesos": "procesos", "contratos": "contratos"}


@dataclass(frozen=True)
class ActorScopeDecision:
    """Resultado de ``evaluate_actor_scope``. Nunca muta los params."""

    should_execute: bool
    missing_slots: tuple[str, ...] = ()
    risk_flags: tuple[str, ...] = ()
    clarification_question: str | None = None
    reason: str = ""


def has_actor_scope(params: dict) -> bool:
    """¿La consulta está acotada a un actor?

    Cuenta tanto el documento resuelto como el nombre pendiente de
    resolver: en ambos casos el usuario preguntó por alguien concreto, y
    ejecutar sin ese alcance respondería otra pregunta.
    """
    return bool(params.get("actor_resolution") or params.get("actor_scope"))


def dataset_supports_actor(dataset: str | None) -> bool:
    """¿Ese dataset publica el documento del proveedor?

    Se responde desde el ``DatasetSpec``, que es lo que el builder usa de
    verdad para decidir si añade el filtro.
    """
    spec = SoQLBuilder.SPECS.get(SoQLBuilder.dataset_id_for(dataset))
    return bool(spec and spec.contractor_document)


def evaluate_actor_scope(params: dict) -> ActorScopeDecision:
    """Decide si una consulta con alcance de actor puede ejecutarse.

    Devuelve ``should_execute=True`` —sin más— cuando no hay actor: esta
    política no opina sobre el resto de las consultas.
    """
    if not has_actor_scope(params):
        return ActorScopeDecision(should_execute=True)

    if params.get("actor_resolution") == "name_unresolved":
        name = _observed_name(params)
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_document",),
            risk_flags=("actor_requires_resolution",),
            clarification_question=(
                f"Para darte la huella contractual de {name} necesito su NIT o "
                "su cédula. Con el nombre solo no puedo distinguirlo de otros "
                "proveedores de nombre parecido, y atribuirle contratos ajenos "
                "sería peor que no responder."
                if name else
                "Necesito el NIT o la cédula del proveedor para consultar su "
                "actividad contractual."
            ),
            reason=REASON_NAME_UNRESOLVED,
        )

    dataset = params.get("dataset")
    if not dataset_supports_actor(dataset):
        label = _DATASET_LABELS.get(str(dataset), str(dataset))
        return ActorScopeDecision(
            should_execute=False,
            missing_slots=("actor_supported_dataset",),
            risk_flags=("actor_dataset_unsupported",),
            clarification_question=(
                f"El conjunto de {label} no publica quién contrata, así que ahí "
                "no puedo filtrar por ese NIT o cédula. ¿Busco su actividad en "
                "los contratos?"
            ),
            reason=REASON_DATASET_UNSUPPORTED,
        )

    return ActorScopeDecision(should_execute=True)


def _observed_name(params: dict) -> str | None:
    scope = params.get("actor_scope")
    if isinstance(scope, dict):
        name = scope.get("observed_name")
        if isinstance(name, str) and name.strip():
            return name.strip().upper()
    return None
