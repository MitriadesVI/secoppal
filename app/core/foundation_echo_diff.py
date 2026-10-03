"""M2A-7: the typed difference between what a turn composed over and what it executed (contract §11, INV-M2A-12).

``composed_diff(base, effective, patch)`` reads three typed values (the composition base, the effective accepted
interpretation and the patch) and says, with no text and no authority, what the turn did to each condition and to the
universe: what it CARRIED untouched, what it PLACED, what DEPARTED and by which of the closed departures, and whether the
universe changed. ``fresh_diff(effective)`` is the diff of a fresh acceptance (a first turn or a new search): everything
it holds is placed. ``foundation_echo`` turns this diff, and the provenance of the conditions, into ``EchoItem``s.

It is the READER of ``compose``, not a second ``compose``: it re-derives nothing (no collision test of A8.1, no
uniqueness test of F16). It only refuses (``ContractError``) a triple whose difference it cannot explain with one of the
three departures of the contract, because an echo that hid an unexplained change would not be «exactly the execution»:

* CITED: a ``CLEAR`` covers the slot of the condition (a dimension-wide ``CLEAR`` covers each slot of its dimension,
  §6.2) or a ``SET`` / ``SET_ORDER`` writes it (the slot's content is replaced, §5.2);
* A8.1 (§6.5, §15.2): a VALUE bound that no operation addresses leaves while the patch sets the opposite bound;
* F16 (§6.5): a carried POSITIVE value leaves while the patch sets the same canonical value NEGATIVE.

``answer_diff`` is the same reading for the answer to a pending question (contract §11: «delta **o respuesta** ejecutados
con algo **del ancla** que sobrevive»; INV-M2A-12: eco = ejecución). The frame of a pending is what the asking turn
composed and never executed, so the answer's difference is read against the ANCHOR, in two steps that each keep the
reader's own checks: anchor to frame (by the operations the asking turn left in the frame) and frame to the effective
(by the answer). What the frame carried and the answer left stands as carried; what the frame placed and the answer left
stands as placed (announced now, once); what left the anchor, in either step, leaves with that step's departure; what the
frame placed and the answer replaced was never executed and is nobody's fact.

Pure over typed values (INV-M2A-24): no regex, no lexicon, no authority; values are read only through
``foundation_accepted`` (``canonical_value``).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, replace
from enum import Enum

from app.core.foundation_accepted import (
    AcceptedCondition, AcceptedInterpretation, PatchOp, PatchOpKind, SemanticPatch, Slot, canonical_value,
)
from app.core.interpretation_contract import POLARIZED_SLOT_DIMENSIONS, ContractError, Polarity, ValueSide
from app.core.semantic_dimensions import SemanticDimension

PLACERS = frozenset({PatchOpKind.SET, PatchOpKind.SET_ORDER})
_LOWER_SIDE = Slot(SemanticDimension.VALUE, side=ValueSide.LOWER)
_UPPER_SIDE = Slot(SemanticDimension.VALUE, side=ValueSide.UPPER)


class Departure(str, Enum):
    """Why a condition of the base is not in the effective: the closed list of the module docstring."""

    CITED = "CITED"
    A81_OPPOSITE_BOUND = "A81_OPPOSITE_BOUND"
    F16_POSITIVE_INVERTED = "F16_POSITIVE_INVERTED"


@dataclass(frozen=True, slots=True)
class Removal:
    condition: AcceptedCondition
    departure: Departure


@dataclass(frozen=True, slots=True)
class TurnDiff:
    """``carried`` and ``placed`` are in the effective's order, ``removed`` in the base's. ``wide_value_clear``: a
    ``CLEAR`` of the whole VALUE dimension is in the patch. ``set_order``: a ``SET_ORDER`` is in the patch.
    ``universe_replaced``: a ``REPLACE_UNIVERSE`` is in the patch; ``universe_changed``: the requests' (subject,
    universe kind) are not the base's (a dual anchor reduced to one request counts, §14)."""

    carried: tuple[AcceptedCondition, ...]
    placed: tuple[AcceptedCondition, ...]
    removed: tuple[Removal, ...]
    universe_replaced: bool
    universe_changed: bool
    wide_value_clear: bool
    set_order: bool


def fresh_diff(effective: AcceptedInterpretation) -> TurnDiff:
    """A fresh acceptance has no base: all of its conditions are the turn's own."""
    return TurnDiff((), effective.conditions, (), False, False, False, False)


def _covers(slot: Slot) -> frozenset[Slot]:
    """The slots an op's slot addresses (§6.2): a dimension-wide ``CLEAR`` covers each slot of its dimension."""
    if slot.is_universe or not slot.dimension_wide:
        return frozenset({slot})
    if slot.dimension is SemanticDimension.VALUE:
        return frozenset({_LOWER_SIDE, _UPPER_SIDE})
    return frozenset(Slot(slot.dimension, polarity) for polarity in (Polarity.POSITIVE, Polarity.NEGATIVE))


def _shape(condition: AcceptedCondition) -> AcceptedCondition:
    """A condition without the two fields ``compose`` assigns (its id and its requests): what a patch op and the
    condition it placed must share."""
    return replace(condition, condition_id="_", request_ids=("_",))


def _departure(old: AcceptedCondition, placers, written: frozenset[Slot], cleared: frozenset[Slot]) -> Departure:
    if old.slot in written or old.slot in cleared:
        return Departure.CITED
    slot = old.slot
    if slot.dimension is SemanticDimension.VALUE:
        opposite = _UPPER_SIDE if slot == _LOWER_SIDE else _LOWER_SIDE
        if any(op.slot == opposite for op in placers):
            return Departure.A81_OPPOSITE_BOUND
    elif slot.dimension in POLARIZED_SLOT_DIMENSIONS and slot.polarity is Polarity.POSITIVE:
        negative = Slot(slot.dimension, Polarity.NEGATIVE)
        if any(op.slot == negative and canonical_value(op.condition) == canonical_value(old) for op in placers):
            return Departure.F16_POSITIVE_INVERTED
    raise ContractError("ECHO_UNCITED_REMOVAL")


def composed_diff(base: AcceptedInterpretation, effective: AcceptedInterpretation, patch: SemanticPatch) -> TurnDiff:
    """The diff of ``effective`` against ``base`` under ``patch``; ``ContractError`` when the triple is inconsistent."""
    if type(patch) is not SemanticPatch:
        raise ContractError("ECHO_DIFF_NEEDS_A_PATCH")
    return ops_diff(base, effective, patch.ops)


def ops_diff(base: AcceptedInterpretation, effective: AcceptedInterpretation, ops: tuple[PatchOp, ...]) -> TurnDiff:
    """``composed_diff`` over a bare tuple of operations (a frame's operations are a subset of a turn's, not a patch;
    the tuple may be empty: then the effective is the base)."""
    if type(base) is not AcceptedInterpretation or type(effective) is not AcceptedInterpretation:
        raise ContractError("ECHO_DIFF_NEEDS_INTERPRETATIONS")
    if type(ops) is not tuple or any(type(op) is not PatchOp for op in ops):
        raise ContractError("ECHO_DIFF_NEEDS_OPERATIONS")
    before = {condition.condition_id: condition for condition in base.conditions}
    carried, placed = [], []
    for condition in effective.conditions:
        old = before.get(condition.condition_id)
        if old is None:
            placed.append(condition)
        elif replace(old, request_ids=condition.request_ids) == condition:  # §6.3: byte-identical, but P-1's request_ids
            carried.append(condition)
        else:
            raise ContractError("ECHO_CARRIED_CONDITION_CHANGED")
    placers = tuple(op for op in ops if op.op in PLACERS)
    if Counter(map(_shape, placed)) != Counter(_shape(op.condition) for op in placers):
        raise ContractError("ECHO_PLACED_WITHOUT_OPERATION")
    written = frozenset(op.slot for op in placers)
    cleared = frozenset(slot for op in ops if op.op is PatchOpKind.CLEAR for slot in _covers(op.slot))
    standing = {condition.condition_id for condition in effective.conditions}
    removed = tuple(Removal(old, _departure(old, placers, written, cleared))
                    for old in base.conditions if old.condition_id not in standing)
    redirects = tuple(op for op in ops if op.op is PatchOpKind.REPLACE_UNIVERSE)
    universe_before = tuple((request.subject, request.universe_kind) for request in base.requests)
    universe_after = tuple((request.subject, request.universe_kind) for request in effective.requests)
    if not redirects and effective.requests != base.requests:
        raise ContractError("ECHO_REQUESTS_CHANGED_WITHOUT_OPERATION")
    if redirects and universe_after != tuple((op.universe.subject, op.universe.universe_kind) for op in redirects):
        raise ContractError("ECHO_UNIVERSE_NOT_THE_REQUESTED")
    wide_value_clear = any(op.op is PatchOpKind.CLEAR and op.slot.dimension is SemanticDimension.VALUE
                           and op.slot.side is None for op in ops)
    return TurnDiff(tuple(carried), tuple(placed), removed, bool(redirects), universe_before != universe_after,
                    wide_value_clear, any(op.op is PatchOpKind.SET_ORDER for op in ops))


def answer_diff(anchor: AcceptedInterpretation, frame: AcceptedInterpretation, frame_ops: tuple[PatchOp, ...],
                effective: AcceptedInterpretation, answer: SemanticPatch) -> TurnDiff:
    """The diff of an executed answer against the ANCHOR: ``frame_ops`` turn the anchor into the frame (what the asking
    turn left in it), ``answer`` turns the frame into the effective. Each step is read (and refused) by ``ops_diff``."""
    first = ops_diff(anchor, frame, frame_ops)
    second = composed_diff(frame, effective, answer)
    anchor_by_id = {condition.condition_id: condition for condition in anchor.conditions}
    carried_by_frame = {condition.condition_id for condition in first.carried}
    placed_by_frame = {condition.condition_id for condition in first.placed}
    standing = {condition.condition_id for condition in second.carried}
    placed_by_answer = {condition.condition_id for condition in second.placed}
    carried = tuple(c for c in second.carried if c.condition_id in carried_by_frame)
    placed = tuple(c for c in effective.conditions
                   if c.condition_id in placed_by_answer or (c.condition_id in placed_by_frame and c.condition_id in standing))
    left_with_the_answer = [Removal(anchor_by_id[r.condition.condition_id], r.departure) for r in second.removed
                            if r.condition.condition_id in carried_by_frame]  # a frame's replacement is nobody's fact
    order = {condition.condition_id: rank for rank, condition in enumerate(anchor.conditions)}
    removed = tuple(sorted((*first.removed, *left_with_the_answer), key=lambda r: order[r.condition.condition_id]))
    universe_before = tuple((request.subject, request.universe_kind) for request in anchor.requests)
    universe_after = tuple((request.subject, request.universe_kind) for request in effective.requests)
    return TurnDiff(carried, placed, removed, first.universe_replaced or second.universe_replaced,
                    universe_before != universe_after, first.wide_value_clear or second.wide_value_clear,
                    first.set_order or second.set_order)
