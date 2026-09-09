"""Un turno sobre un perfil es un PARCHE TIPADO, no un merge de diccionarios.

V1.1 componía turnos fusionando params. Con eso, "proveedores" borraba el
periodo, "en 2025" borraba el evento, y una aclaración sustituía la lectura
confirmada. Ninguno de esos tres efectos estaba escrito en ninguna parte: eran
consecuencias de que un ``dict.update`` no distingue "no lo mencionó" de "lo
quitó".

Aquí hay tres operaciones y sólo tres:

    KEEP   la dimensión no se nombró; sobrevive intacta
    SET    la dimensión se nombró; el valor tipado la reemplaza
    CLEAR  la persona pidió quitarla

La ausencia de operación es KEEP. CLEAR existe porque "sin filtro de fecha" no
es lo mismo que no decir nada, y en V1.1 ambas cosas eran indistinguibles.

Dos SET contradictorios sobre el mismo destino producen un parche INVÁLIDO. No
gana el último: que una escritura silencie a otra es exactamente lo que hace
que un universo cambie sin que nadie lo pida.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any
import uuid

from app.core.entity_profile.frame import (
    BuyerScope,
    EntityProfileFrame,
    FrameProvenance,
    SourcePredicates,
    SourceSelection,
)
from app.core.entity_profile.frame_values import SourceKind


_NAMESPACE = uuid.UUID("5f1f3f4e-2a11-4c9d-9c0f-1d2b3a4c5d6e")


class PatchOperation(str, Enum):
    KEEP = "KEEP"
    SET = "SET"
    CLEAR = "CLEAR"


class TargetKind(str, Enum):
    FOCUS = "focus"
    SOURCE_SELECTION = "source_selection"
    TEMPORAL = "temporal"
    CURRENT_STATE = "current_state"
    CONTRACT_UNIVERSE = "contract_universe"
    TOPIC = "topic"
    MODALITY = "modality"
    CONTRACT_TYPE = "contract_type"
    VALUE = "value"


# Sólo los filtros OPCIONALES pueden limpiarse. El comprador no es un target
# en absoluto —no existe una operación capaz de borrarlo— y las fuentes no
# pueden quedar vacías: un perfil sin universo no es un perfil.
_PER_SOURCE = frozenset({
    TargetKind.TEMPORAL, TargetKind.CURRENT_STATE, TargetKind.TOPIC,
    TargetKind.MODALITY, TargetKind.CONTRACT_TYPE, TargetKind.VALUE, TargetKind.CONTRACT_UNIVERSE,
})
_CLEARABLE = _PER_SOURCE


@dataclass(frozen=True, slots=True)
class PatchTarget:
    kind: TargetKind
    source: SourceKind | None = None

    def __post_init__(self) -> None:
        if self.kind in _PER_SOURCE and self.source is None:
            raise ValueError(f"{self.kind.value} needs the source it belongs to")
        if self.kind not in _PER_SOURCE and self.source is not None:
            raise ValueError(f"{self.kind.value} is not a per-source target")


@dataclass(frozen=True, slots=True)
class PatchOp:
    target: PatchTarget
    operation: PatchOperation
    value: Any = None
    evidence_refs: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EntityProfilePatch:
    """Lo que este turno cambia del universo anterior, y nada más."""

    patch_id: str
    turn_id: str
    operations: tuple[PatchOp, ...] = ()
    base_frame_ref: tuple[str, int] | None = None
    new_buyer_scope: BuyerScope | None = None
    schema_version: int = 1

    @property
    def is_new(self) -> bool:
        return self.base_frame_ref is None

    def targets(self) -> tuple[PatchTarget, ...]:
        return tuple(op.target for op in self.operations)


@dataclass(frozen=True, slots=True)
class PatchOutcome:
    """El candidato, o el motivo por el que no hay candidato."""

    frame: EntityProfileFrame | None
    reason: str | None = None
    conflicts: tuple[PatchTarget, ...] = ()


def apply_patch(
    base: EntityProfileFrame | None, patch: EntityProfilePatch,
) -> PatchOutcome:
    """Aplica el parche SIN mutar el Frame base; devuelve un candidato nuevo."""
    invalid = _validate(base, patch)
    if invalid is not None:
        return invalid

    if base is None:
        return _new_frame(patch)

    if patch.base_frame_ref != (base.frame_id, base.revision):
        return PatchOutcome(None, "STALE_BASE_REVISION")

    focus = base.focus
    selection = base.source_selection
    predicates = dict(base.source_predicates)

    for op in patch.operations:
        if op.operation is PatchOperation.KEEP:
            continue
        if op.target.kind is TargetKind.FOCUS:
            focus = op.value
        elif op.target.kind is TargetKind.SOURCE_SELECTION:
            selection = op.value
            predicates = _reshape(predicates, selection)
        else:
            source = op.target.source
            if source not in predicates:
                # Un filtro dirigido a una fuente que el universo no tiene no
                # amplía el universo en silencio.
                return PatchOutcome(None, "TARGET_SOURCE_NOT_IN_FRAME")
            predicates[source] = _apply_dimension(predicates[source], op)

    candidate = base.with_revision(
        focus=focus, source_selection=selection, source_predicates=predicates,
        provenance=_merge_provenance(base.provenance, patch),
    )
    return PatchOutcome(candidate)


def _validate(
    base: EntityProfileFrame | None, patch: EntityProfilePatch,
) -> PatchOutcome | None:
    if base is None and patch.base_frame_ref is not None:
        return PatchOutcome(None, "MISSING_BASE_FRAME")
    seen: dict[PatchTarget, Any] = {}
    conflicts: list[PatchTarget] = []
    for op in patch.operations:
        if op.operation is PatchOperation.CLEAR and op.target.kind not in _CLEARABLE:
            return PatchOutcome(None, "CLEAR_ON_REQUIRED_DIMENSION", (op.target,))
        if op.operation is PatchOperation.KEEP:
            continue
        marker = op.value if op.operation is PatchOperation.SET else PatchOperation.CLEAR
        if op.target in seen and seen[op.target] != marker:
            conflicts.append(op.target)
        seen[op.target] = marker
    if conflicts:
        return PatchOutcome(None, "CONFLICTING_OPERATIONS", tuple(conflicts))
    for op in patch.operations:
        if (
            op.target.kind is TargetKind.SOURCE_SELECTION
            and op.operation is PatchOperation.SET
            and (not isinstance(op.value, SourceSelection) or not op.value.requested)
        ):
            return PatchOutcome(None, "EMPTY_SOURCE_SELECTION", (op.target,))
    return None


def _new_frame(patch: EntityProfilePatch) -> PatchOutcome:
    if patch.new_buyer_scope is None:
        return PatchOutcome(None, "BUYER_REQUIRED")
    focus = "overview"
    selection: SourceSelection | None = None
    for op in patch.operations:
        if op.operation is not PatchOperation.SET:
            continue
        if op.target.kind is TargetKind.FOCUS:
            focus = op.value
        elif op.target.kind is TargetKind.SOURCE_SELECTION:
            selection = op.value
    if selection is None:
        return PatchOutcome(None, "SOURCE_SELECTION_REQUIRED")
    predicates = {source: SourcePredicates(source) for source in selection.ordered}
    for op in patch.operations:
        if op.operation is PatchOperation.SET and op.target.kind in _PER_SOURCE:
            source = op.target.source
            if source not in predicates:
                return PatchOutcome(None, "TARGET_SOURCE_NOT_IN_FRAME")
            predicates[source] = _apply_dimension(predicates[source], op)
    frame_id = str(uuid.uuid5(_NAMESPACE, f"{patch.turn_id}:{patch.patch_id}"))
    return PatchOutcome(EntityProfileFrame(
        frame_id=frame_id, revision=1, buyer_scope=patch.new_buyer_scope,
        focus=focus, source_selection=selection, source_predicates=predicates,
        provenance=_merge_provenance(FrameProvenance(), patch),
    ))


def _apply_dimension(predicates: SourcePredicates, op: PatchOp) -> SourcePredicates:
    """Una dimensión cambia; las demás de esa fuente se conservan intactas."""
    if op.operation is PatchOperation.CLEAR:
        default = SourcePredicates(predicates.source)
        value = getattr(default, op.target.kind.value)
    else:
        value = op.value
    return replace(predicates, **{op.target.kind.value: value})


def _reshape(
    predicates: dict[SourceKind, SourcePredicates], selection: SourceSelection,
) -> dict[SourceKind, SourcePredicates]:
    """Las fuentes que siguen conservan sus predicados; las nuevas nacen limpias.

    Una fuente que entra al universo NO hereda el evento de otra: la fecha de
    firma de un contrato no describe la publicación de un proceso, y adivinarlo
    contestaría una pregunta distinta de la que se hizo.
    """
    return {
        source: predicates.get(source) or SourcePredicates(source)
        for source in selection.ordered
    }


def _merge_provenance(
    base: FrameProvenance, patch: EntityProfilePatch,
) -> FrameProvenance:
    refs = tuple(ref for op in patch.operations for ref in op.evidence_refs)
    return FrameProvenance(
        claim_ids=tuple(dict.fromkeys((*base.claim_ids, *refs))),
        turn_ids=tuple(dict.fromkeys((*base.turn_ids, patch.turn_id))),
        authorities=base.authorities,
    )
