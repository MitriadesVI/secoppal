"""Publication that only accepts a reconciled ContractActivityResult."""
from __future__ import annotations

from .models import ContractActivityRequest, ContractActivityResult, Operation


def render_result(
    request: ContractActivityRequest, result: ContractActivityResult,
) -> str:
    noun = "contrato distinto" if result.total == 1 else "contratos distintos"
    scope = _scope(request)
    if result.operation is Operation.TOTAL:
        return (
            f"Encontré {result.total:,} {noun} para {request.buyer_display}{scope}. "
            "El conteo usa id_contrato y, cuando hay periodo, fecha_de_firma."
        )
    dimension = (
        "modalidad reportada"
        if result.operation is Operation.BY_MODALITY
        else "estado actualmente reportado"
    )
    lines = [
        f"Encontré {result.total:,} {noun} para {request.buyer_display}{scope}, "
        f"desglosados por {dimension}:"
    ]
    lines.extend(
        f"- {group.label}: {group.contract_count:,} ({group.share:.1%})"
        for group in result.groups
    )
    lines.append("El detalle completo reconcilia exactamente con el total de id_contrato.")
    return "\n".join(lines)


def clarification(reason: str) -> str:
    unavailable = {
        "SOURCE_CALL_UNAVAILABLE", "MALFORMED_SOURCE_ROWS",
        "SCALAR_ROW_SHAPE_INVALID", "SCALAR_COLUMNS_INVALID",
    }
    if reason in unavailable or "RECONCILIATION" in reason or "MISMATCH" in reason:
        return (
            "No pude acreditar un conteo contractual completo con la fuente. "
            "No voy a presentar la ausencia de evidencia como cero."
        )
    return (
        "Puedo contar contratos distintos de un comprador acreditado, en total o "
        "desglosados por una sola dimensión (modalidad o estado actualmente reportado). "
        "Aclara la unidad, operación o filtro que quedó incompleto."
    )


def _scope(request: ContractActivityRequest) -> str:
    parts: list[str] = []
    if request.date_from:
        parts.append(f"firmados entre {request.date_from} y {request.date_to}")
    if request.modality_values:
        parts.append("modalidad " + ", ".join(request.modality_values))
    if request.reported_states:
        parts.append("estado actual " + ", ".join(request.reported_states))
    if request.money:
        parts.append(
            f"valor_del_contrato {request.money.operator} {request.money.amount_cop}"
        )
    return " (" + "; ".join(parts) + ")" if parts else ""
