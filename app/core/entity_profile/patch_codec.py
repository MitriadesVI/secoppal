"""Serializar un parche para que un PENDIENTE sobreviva a la recarga.

Un pendiente no es un Frame: es «lo que este turno pedía cambiar» sobre una
revisión concreta, todavía sin aceptar. Si sólo se persistiera el Frame
confirmado, al recargar la conversación la pregunta abierta desaparecería y la
selección posterior («solo contratos», «la primera opción») no tendría sobre
qué aplicarse — que es exactamente el defecto R5c.

El codec es ESTRICTO por la misma razón que el Frame: un valor de otro tipo,
una dimensión desconocida o una operación inventada invalidan el pendiente
entero. Un pendiente a medias reconstruido decidiría por la persona.
"""
from __future__ import annotations

from app.core.entity_profile.frame import (
    FrameSchemaError,
    SourceSelection,
    TemporalPredicate,
)
from app.core.entity_profile.frame_values import (
    Selection,
    SourceKind,
    StateExpression,
    TopicNode,
    ValueConstraint,
)
from app.core.entity_profile.patch import (
    EntityProfilePatch,
    PatchOp,
    PatchOperation,
    PatchTarget,
    TargetKind,
)


PATCH_SCHEMA_VERSION = 1

# Cada destino tiene UN tipo. La tabla es el contrato: un `topic` que llega
# como `Selection` no se «adapta», se rechaza.
_VALUE_CODEC = {
    TargetKind.FOCUS: (str, lambda v: v, lambda raw: str(raw)),
    TargetKind.CONTRACT_UNIVERSE: (str, lambda v: v, lambda raw: str(raw)),
    TargetKind.SOURCE_SELECTION: (
        SourceSelection, lambda v: v.to_dict(), SourceSelection.from_dict,
    ),
    TargetKind.TEMPORAL: (
        TemporalPredicate, lambda v: v.to_dict(), TemporalPredicate.from_dict,
    ),
    TargetKind.CURRENT_STATE: (
        StateExpression, lambda v: v.to_dict(), StateExpression.from_dict,
    ),
    TargetKind.TOPIC: (TopicNode, lambda v: v.to_dict(), TopicNode.from_dict),
    TargetKind.MODALITY: (Selection, lambda v: v.to_dict(), Selection.from_dict),
    TargetKind.CONTRACT_TYPE: (
        Selection, lambda v: v.to_dict(), Selection.from_dict,
    ),
    TargetKind.VALUE: (
        ValueConstraint, lambda v: v.to_dict(), ValueConstraint.from_dict,
    ),
}


def patch_to_dict(patch: EntityProfilePatch) -> dict:
    return {
        "schema_version": PATCH_SCHEMA_VERSION,
        "patch_id": patch.patch_id,
        "turn_id": patch.turn_id,
        "base_frame_ref": (
            list(patch.base_frame_ref) if patch.base_frame_ref else None
        ),
        "new_buyer_scope": (
            patch.new_buyer_scope.to_dict() if patch.new_buyer_scope else None
        ),
        "operations": [operation_to_dict(op) for op in patch.operations],
    }


def patch_from_dict(raw: object) -> EntityProfilePatch:
    if not isinstance(raw, dict):
        raise FrameSchemaError("patch must be an object")
    if raw.get("schema_version") != PATCH_SCHEMA_VERSION:
        raise FrameSchemaError(f"unknown patch schema: {raw.get('schema_version')!r}")
    from app.core.entity_profile.frame import BuyerScope

    try:
        base_ref = raw.get("base_frame_ref")
        buyer = raw.get("new_buyer_scope")
        return EntityProfilePatch(
            patch_id=str(raw["patch_id"]),
            turn_id=str(raw["turn_id"]),
            operations=tuple(operation_from_dict(item) for item in raw["operations"]),
            base_frame_ref=(
                (str(base_ref[0]), int(base_ref[1])) if base_ref else None
            ),
            new_buyer_scope=BuyerScope.from_dict(buyer) if buyer else None,
        )
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise FrameSchemaError(f"malformed patch payload: {exc}") from exc


def operation_to_dict(op: PatchOp) -> dict:
    kind = op.target.kind
    payload = {
        "target": kind.value,
        "source": op.target.source.value if op.target.source else None,
        "operation": op.operation.value,
        "evidence_refs": list(op.evidence_refs),
    }
    if op.operation is PatchOperation.SET:
        expected, encode, _ = _VALUE_CODEC[kind]
        if not isinstance(op.value, expected):
            raise FrameSchemaError(
                f"{kind.value} SET carries a {type(op.value).__name__}",
            )
        payload["value"] = encode(op.value)
    return payload


def operation_from_dict(raw: object) -> PatchOp:
    if not isinstance(raw, dict):
        raise FrameSchemaError("patch operation must be an object")
    kind = TargetKind(raw["target"])
    operation = PatchOperation(raw["operation"])
    source_raw = raw.get("source")
    value = None
    if operation is PatchOperation.SET:
        if "value" not in raw:
            raise FrameSchemaError(f"{kind.value} SET without a value")
        _, _, decode = _VALUE_CODEC[kind]
        value = decode(raw["value"])
    elif "value" in raw and raw["value"] is not None:
        # KEEP y CLEAR no llevan valor. Uno que llegue con carga no se ignora:
        # significa que el emisor y el lector no comparten contrato.
        raise FrameSchemaError(f"{operation.value} must not carry a value")
    return PatchOp(
        target=PatchTarget(kind, SourceKind(source_raw) if source_raw else None),
        operation=operation,
        value=value,
        evidence_refs=tuple(str(ref) for ref in raw.get("evidence_refs") or ()),
    )


__all__ = [
    "PATCH_SCHEMA_VERSION",
    "operation_from_dict",
    "operation_to_dict",
    "patch_from_dict",
    "patch_to_dict",
]
