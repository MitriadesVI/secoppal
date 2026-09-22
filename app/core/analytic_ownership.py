"""No silent downgrade of an analytic instruction.

INV-ANALYTIC-CONTROL-HAS-OWNER-OR-LIMITATION
    Consuming an analytic control keeps it out of the WHERE clause; it does not
    make it disappear.  Every turn whose controls derive an operation ends in
    exactly one of: an owner that executes it, an owner that clarifies it, or a
    response that states it was not performed.  A search listing presented as
    if it answered "¿cuántos…?" or "desglosa por modalidad" is the failure this
    module exists to prevent.

The only owner today is Contract Activity (COUNT / GROUP BY modality / GROUP
BY reported state, contracts of one buyer).  Nothing here executes anything:
it produces the limitation text the renderer must prepend when the turn was
served by a capability that does not own the operation.
"""
from __future__ import annotations

from app.core.analytic_control import AnalyticDimension
from app.core.analytic_operation import AnalyticOperation, AnalyticOperationKind

# Intents whose answer executes (or explicitly clarifies) the derived operation.
ANALYTIC_OWNER_INTENTS = frozenset({"contract_activity"})

_DIMENSION_LABEL = {
    AnalyticDimension.MODALITY: "modalidad",
    AnalyticDimension.REPORTED_STATE: "estado reportado",
}


def limitation_notice(operation: AnalyticOperation) -> str:
    """Text that must precede a non-owner answer, or ``""`` when none is due."""
    if not operation.requested:
        return ""
    if operation.kind is AnalyticOperationKind.COUNT:
        return (
            "ℹ️ No tengo un conteo certificado para esta consulta. Lo que sigue "
            "es el resultado de la búsqueda, no un conteo de contratos.\n\n"
        )
    if operation.kind is AnalyticOperationKind.GROUP and operation.dimension is not None:
        return (
            f"ℹ️ No puedo agrupar esta consulta por "
            f"{_DIMENSION_LABEL[operation.dimension]}. Lo que sigue son los "
            "resultados sin agrupar.\n\n"
        )
    return (
        "ℹ️ No pude conciliar la instrucción de conteo o agrupación de tu "
        "consulta, así que no la apliqué. Lo que sigue son los resultados sin "
        "ese análisis.\n\n"
    )


def owner_limitation(operation: AnalyticOperation, analytical_intent: str) -> str:
    """Limitation due after the turn was served by ``analytical_intent``."""
    if analytical_intent in ANALYTIC_OWNER_INTENTS:
        return ""
    return limitation_notice(operation)


__all__ = ["ANALYTIC_OWNER_INTENTS", "limitation_notice", "owner_limitation"]
