"""Compuerta temporal: decide si un scope de fecha bloquea la ejecución.

Vive fuera de query_plan porque es una sola responsabilidad — traducir la
resolución temporal (o su fallo) en un veredicto de ejecución — y query_plan ya
arbitra varias dimensiones ortogonales.
"""
from __future__ import annotations

from typing import Any

_DEFAULT_QUESTION = (
    "No pude resolver esa referencia temporal con seguridad. "
    "¿Qué intervalo querías usar?"
)


def temporal_plan_block(params: dict, signals: list[Any]) -> dict | None:
    """Consume las señales temporales privadas y dictamina si se puede ejecutar.

    Siempre saca de ``params`` las claves privadas del parser —
    ``_temporal_replace_scope`` solo vive hasta que el follow-up puede
    reemplazar el cluster completo, y jamás debe llegar al builder ni
    persistirse en el frame confirmado.

    Returns:
        kwargs de QueryPlan que describen el bloqueo, o None si el scope
        temporal no impide ejecutar.
    """
    error_code = params.pop("_temporal_error", None)
    question = params.pop("_temporal_clarification", None)
    params.pop("_temporal_replace_scope", None)

    if error_code:
        return _block(str(error_code), str(question) if question else None)

    for signal in signals:
        if getattr(signal, "kind", None) == "temporal_invalid":
            payload = getattr(signal, "payload", {}) or {}
            return _block(str(payload.get("code", "invalid")), payload.get("question"))

    return None


def _block(code: str, question: str | None) -> dict:
    return {
        "should_execute": False,
        "risk_flags": [f"TEMPORAL_{code.upper()}"],
        "clarification_question": str(question or _DEFAULT_QUESTION),
        "reason": "invalid_temporal_scope",
    }
