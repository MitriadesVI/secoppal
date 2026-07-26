"""Productor mínimo de telemetría de follow-ups de modalidad (M3C-CG-I).

Telemetría pasiva: observa, no decide; no muta el workflow; no consulta SECOP,
LLM, ConversationStore ni QueryRouter. Emite exactamente un evento JSONL por
turno de follow-up con señal real de modalidad, y solo proyecta metadatos
agregables: no almacena texto de usuario, chat_id, procedure_ids, resultados ni
valores contractuales.

Contrato V1::

    {
      "schema_version": 1,
      "event_id": "uuid",
      "recorded_at": "UTC ISO-8601",
      "source": "runtime",
      "is_followup": true,
      "had_previous_modality_filter": false,
      "has_final_modality_filter": false,
      "has_modality_operation": false,
      "operation": null,
      "instruction_status": null,
      "resolution_status": null,
      "clarification_code": null,
      "blocked": false,
      "no_op": false,
      "clear": false,
      "implicit_replace": false
    }

Un evento se emite solo cuando ``is_followup`` es true y al menos una de estas
señales es cierta: hay una operación de modalidad en el trace, había un filtro
canónico de modalidad en el contexto previo, hay un filtro canónico final, o el
turno quedó bloqueado por una razón ``modality_*``.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
SOURCE = "runtime"

_INCLUDE_KEY = "modalidad_include_procedure_ids"
_EXCLUDE_KEY = "modalidad_exclude_procedure_ids"
_MODALITY_KEYS = (_INCLUDE_KEY, _EXCLUDE_KEY)

_VALID_OPERATIONS = frozenset({
    "replace", "add_include", "add_exclude", "remove_member",
    "remove_exclude", "restore_include", "clear",
})
_VALID_INSTRUCTION_STATUS = frozenset({"resolved", "ambiguous", "conflict", "none"})
_VALID_RESOLUTION_STATUS = frozenset({"applied", "no_op", "blocked"})

_BLOCKED_REASONS = frozenset({
    "partial_target",
    "ambiguous_target",
    "conflicting_target",
    "non_searchable_target",
    "dataset_inapplicable_target",
    "target_missing",
    "control_conflict",
    "mixed_target_polarity",
    "unknown_procedure_id",
    "ambiguous_clear_or_null_value",
})


# ── Proyección pura ───────────────────────────────────────────────────────────

def _has_modality_filter(params: dict[str, Any]) -> bool:
    """True si ``params`` contiene alguna clave canónica de modalidad no vacía."""
    if not isinstance(params, dict):
        return False
    return any(bool(params.get(key)) for key in _MODALITY_KEYS)


def _operation_from_trace(trace: dict[str, Any]) -> str | None:
    op = trace.get("operation")
    return op if op in _VALID_OPERATIONS else None


def _instruction_status_from_trace(trace: dict[str, Any]) -> str | None:
    status = trace.get("instruction_status")
    return status if status in _VALID_INSTRUCTION_STATUS else None


def _resolution_status_from_trace(trace: dict[str, Any]) -> str | None:
    """Mapea el reason/no_op del trace a uno de applied/no_op/blocked."""
    if trace.get("no_op") is True:
        return "no_op"
    reason = trace.get("reason")
    if reason in ("mutation_applied", "clear_applied"):
        return "applied"
    if reason == "no_operation":
        return "no_op"
    if reason in _BLOCKED_REASONS:
        return "blocked"
    return None


def _blocked_reason(trace: dict[str, Any]) -> str | None:
    reason = trace.get("reason")
    return reason if reason in _BLOCKED_REASONS else None


def _clear(trace: dict[str, Any]) -> bool:
    return trace.get("operation") == "clear"


def _implicit_replace(trace: dict[str, Any]) -> bool:
    return (
        trace.get("operation") == "replace"
        and trace.get("control_code") == "implicit_target_replace"
    )


def _query_plan_blocked_reason(query_plan_reason: str) -> str | None:
    """Extrae el código de bloqueo de un query_plan.reason como ``modality_<code>``."""
    prefix = "modality_"
    if not isinstance(query_plan_reason, str) or not query_plan_reason.startswith(prefix):
        return None
    code = query_plan_reason[len(prefix):]
    return code if code in _BLOCKED_REASONS else None


def should_emit(
    followup: bool,
    context_params: dict[str, Any],
    parsed_params: dict[str, Any],
    followup_mutation: dict[str, Any],
    query_plan_reason: str,
    needs_clarification: bool,
) -> bool:
    """Decide si un turno debe emitir un evento de telemetría de modalidad.

    Requiere ``followup == True`` y al menos una señal real de modalidad:
    operación en el trace, filtro previo, filtro final o bloqueo modality_*.
    """
    if not followup:
        return False

    if not isinstance(context_params, dict):
        context_params = {}
    if not isinstance(parsed_params, dict):
        parsed_params = {}
    if not isinstance(followup_mutation, dict):
        followup_mutation = {}

    had_previous = _has_modality_filter(context_params)
    has_final = _has_modality_filter(parsed_params)
    trace = followup_mutation.get("modality") if isinstance(followup_mutation, dict) else {}
    if not isinstance(trace, dict):
        trace = {}
    has_operation = bool(trace.get("operation") in _VALID_OPERATIONS)

    blocked = False
    if needs_clarification and query_plan_reason:
        blocked = _query_plan_blocked_reason(query_plan_reason) is not None
    if not blocked:
        blocked = _blocked_reason(trace) is not None

    return has_operation or had_previous or has_final or blocked


def project_event(
    *,
    followup: bool,
    context_params: dict[str, Any],
    parsed_params: dict[str, Any],
    followup_mutation: dict[str, Any],
    query_plan_reason: str,
    needs_clarification: bool,
) -> dict[str, Any] | None:
    """Proyecta un evento V1 o ``None`` si este turno no debe emitir.

    Args:
        followup: El turno fue detectado como follow-up.
        context_params: ``parsed_params`` del turno anterior (para herencia).
        parsed_params: ``parsed_params`` finales del turno actual.
        followup_mutation: ``state["followup_mutation"]`` (contiene el bloque
            ``modality`` si hubo operación).
        query_plan_reason: ``query_plan.reason`` del resultado final.
        needs_clarification: El turno terminó en aclaración.
    """
    if not should_emit(
        followup,
        context_params,
        parsed_params,
        followup_mutation,
        query_plan_reason,
        needs_clarification,
    ):
        return None

    if not isinstance(context_params, dict):
        context_params = {}
    if not isinstance(parsed_params, dict):
        parsed_params = {}
    if not isinstance(followup_mutation, dict):
        followup_mutation = {}

    had_previous = _has_modality_filter(context_params)
    has_final = _has_modality_filter(parsed_params)
    trace = followup_mutation.get("modality") if isinstance(followup_mutation, dict) else {}
    if not isinstance(trace, dict):
        trace = {}
    has_operation = bool(trace.get("operation") in _VALID_OPERATIONS)

    blocked_reason = None
    blocked = False
    if needs_clarification and query_plan_reason:
        blocked_reason = _query_plan_blocked_reason(query_plan_reason)
        if blocked_reason:
            blocked = True
    if not blocked:
        blocked_reason = _blocked_reason(trace)
        blocked = blocked_reason is not None

    operation = _operation_from_trace(trace)
    instruction_status = _instruction_status_from_trace(trace)
    resolution_status = _resolution_status_from_trace(trace)

    # Si un bloqueo viene del query_plan y no hay trace de operación, la
    # instrucción era resuelta (la operación se intentó aplicar).
    if blocked and instruction_status is None:
        instruction_status = "resolved"
    if blocked and resolution_status is None:
        resolution_status = "blocked"

    # Herencia pura: filtro previo/final sin operación de este turno.
    if instruction_status is None and (had_previous or has_final):
        instruction_status = "none"

    clarification_code = blocked_reason

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "recorded_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": SOURCE,
        "is_followup": True,
        "had_previous_modality_filter": had_previous,
        "has_final_modality_filter": has_final,
        "has_modality_operation": has_operation,
        "operation": operation,
        "instruction_status": instruction_status,
        "resolution_status": resolution_status,
        "clarification_code": clarification_code,
        "blocked": blocked,
        "no_op": resolution_status == "no_op",
        "clear": _clear(trace),
        "implicit_replace": _implicit_replace(trace),
    }


# ── Escritura (único efecto secundario) ─────────────────────────────────────

def append_event(path: str | Path | None, event: dict[str, Any]) -> None:
    """Append JSONL en ``path``. No-op si path es ``None`` o directorio ausente.

    Un fallo de escritura se registra como warning sin datos sensibles, sin
    ruta completa y sin contenido del evento; nunca se propaga.
    """
    if path is None:
        return
    try:
        target = Path(path)
    except Exception:
        logger.warning("modality telemetry path invalid (ignored)")
        return

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        logger.warning("modality telemetry append failed: %s", "directory_error")
        return

    try:
        with target.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    except OSError as exc:
        logger.warning("modality telemetry append failed: %s", type(exc).__name__)
    except Exception as exc:
        logger.warning("modality telemetry append failed: %s", type(exc).__name__)
