"""M2A-3: the pure reducer ``compose(base, patch)`` (contract §4–§6, §15; annex A.3; §19).

``compose(base, patch, *, base_trace_id) -> Composed | DeltaRefusal``:

* §6 step 3b first: every refusal cause, in the closed precedence TARGET_NOT_ADDRESSABLE > DELTA_SLOT_CONFLICT >
  KEEP_ON_EMPTY_SLOT > OP_NOT_ACCREDITED:<DIM> > DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER, ties by the §4.2 slot order.
  Any cause refuses the whole patch (§6.1): the base never changes and nothing partial exists.
* otherwise CLEAR, then SET / SET_ORDER / REPLACE_UNIVERSE (§6.2); what no op touches passes byte-identical (§6.3, F1),
  but ``request_ids`` when REPLACE_UNIVERSE reduces a dual anchor (R3, P-1); the declared substitutions of §6.5 (A8.1,
  F16) and their effects (A8.1, F16, F3/F22). The result is built by ``canonical_interpretation``, the one constructor
  path (1b).

Pure over typed values: no text, no regex, no lexicon, no authority (INV-M2A-24). Values are decoded only through
``foundation_accepted`` (R1). The host rules (§6 step 4) and the law of the possible pending (§8.4) are
``foundation_delta_outcome``; the turn's scope (§7) is ``foundation_turn_scope``.

Fail-closed decisions with an owner (inventory §6.6, ``M2A3_PHASE1_CLOSE.md``; contract §19):

* REPLACE_UNIVERSE to ``opportunities`` re-decides O1B / ``opportunity-explicit-state``: no owner in M2a ⇒
  ``OP_NOT_ACCREDITED:UNIVERSE`` (§19 DEP-O1B-OPPORTUNITIES, ``FOUNDATION_O1B_EXPLICIT_TIME_PARITY`` + M2A-1b);
* a request policy carried across a change of universe, or a STATE edit over an opportunities request, would have to be
  re-decided on the composed effective (§6.6): a policy without owner ⇒ ``OP_NOT_ACCREDITED:<DIM of its condition>``;
* a policy goes with its condition: when every cited condition leaves by a cited op it leaves too (no dangling policy).
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from enum import Enum

from app.core.foundation_accepted import (
    CONDITION_SLOT_ORDER, UNIVERSE_SLOT, AcceptedCondition, AcceptedInterpretation, PatchOp, PatchOpKind,
    SemanticPatch, Slot, UniverseKind, canonical_interpretation, canonical_value, money_bound, unit_class,
)
from app.core.foundation_operations import Subject
from app.core.interpretation_contract import (
    POLARIZED_SLOT_DIMENSIONS, ConditionExpression, ContractError, LogicOperator, Polarity, ValueSide,
)
from app.core.semantic_dimensions import SemanticDimension

# The dataset each subject projects (contract §3.2): the host's closed table, ``SoQLBuilder.PROCESOS_DATASET`` and
# ``CONTRATOS_DATASET``, pinned by the tests (M2a never imports the builder).
SUBJECT_DATASET = {Subject.PROCESSES: "p6dx-8zbt", Subject.CONTRACTS: "jbjy-vk9h"}

WRITERS = frozenset({PatchOpKind.SET, PatchOpKind.SET_ORDER, PatchOpKind.REPLACE_UNIVERSE})
PLACERS = frozenset({PatchOpKind.SET, PatchOpKind.SET_ORDER})
UNIVERSE_KEY = UNIVERSE_SLOT.key
ORDER_KEY, CARDINALITY_KEY = Slot(SemanticDimension.ORDER).key, Slot(SemanticDimension.CARDINALITY).key
LOWER_KEY = Slot(SemanticDimension.VALUE, side=ValueSide.LOWER).key
UPPER_KEY = Slot(SemanticDimension.VALUE, side=ValueSide.UPPER).key
TOPIC_POSITIVE_KEY = Slot(SemanticDimension.TOPIC, Polarity.POSITIVE).key
SLOT_BY_KEY = {**{slot.key: slot for slot in CONDITION_SLOT_ORDER}, UNIVERSE_KEY: UNIVERSE_SLOT}
SLOT_RANK = {**{slot.key: rank for rank, slot in enumerate(CONDITION_SLOT_ORDER)},
             UNIVERSE_KEY: len(CONDITION_SLOT_ORDER)}


class Refusal(str, Enum):
    """The ``DeltaRefusal`` causes of §6 step 3b, in their closed precedence (highest first). ``BASE_INVALID`` is not
    one: §7.1 row 1 decides it before composing."""

    TARGET_NOT_ADDRESSABLE = "TARGET_NOT_ADDRESSABLE"
    DELTA_SLOT_CONFLICT = "DELTA_SLOT_CONFLICT"
    KEEP_ON_EMPTY_SLOT = "KEEP_ON_EMPTY_SLOT"
    OP_NOT_ACCREDITED = "OP_NOT_ACCREDITED"
    DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER = "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"


PRECEDENCE = tuple(Refusal)


class DeltaEffect(str, Enum):
    """The typed effects of the declared substitutions (§6.5) that ``compose`` reports for M2A-7 (no echo here)."""

    VALUE_RANGE_REPLACED = "VALUE_RANGE_REPLACED"
    TOPIC_REPLACED = "TOPIC_REPLACED"
    POLARITY_INVERTED = "POLARITY_INVERTED"


def covered(slot: Slot) -> tuple[Slot, ...]:
    """The slots an op's slot addresses: a dimension-wide CLEAR covers each slot of its dimension (§6.2)."""
    if slot.is_universe:
        return (slot,)
    if slot.dimension in POLARIZED_SLOT_DIMENSIONS and slot.polarity is None:
        return tuple(Slot(slot.dimension, polarity) for polarity in (Polarity.POSITIVE, Polarity.NEGATIVE))
    if slot.dimension is SemanticDimension.VALUE and slot.side is None:
        return tuple(Slot(slot.dimension, side=side) for side in (ValueSide.LOWER, ValueSide.UPPER))
    return (slot,)


def covered_keys(ops) -> frozenset[str]:
    return frozenset(s.key for op in ops for s in covered(op.slot))


def dimension_key(slot: Slot) -> str:
    """``<DIM>`` of annex A.3: the dimension of a slot, or UNIVERSE."""
    return UNIVERSE_KEY if slot.is_universe else slot.dimension.value


@dataclass(frozen=True, slots=True)
class Cause:
    """One cause of §6 step 3b: the refusal, the slot it is decided on, the operations that open it and the clause of
    the contract that makes it a cause."""

    refusal: Refusal
    slot: Slot
    ops: tuple[PatchOp, ...]
    clause: str

    @property
    def code(self) -> str:
        """The ``DeltaRefusal`` code (§6)."""
        if self.refusal is Refusal.OP_NOT_ACCREDITED:
            return f"{self.refusal.value}:{dimension_key(self.slot)}"
        return self.refusal.value

    @property
    def published(self) -> str:
        """The A.3 code the §6 table publishes for it (``OP_NOT_ACCREDITED:<DIM>`` ⇒ ``UNRESOLVED_VALUE:<DIM>``)."""
        if self.refusal is Refusal.OP_NOT_ACCREDITED:
            return f"UNRESOLVED_VALUE:{dimension_key(self.slot)}"
        return self.refusal.value

    @property
    def order(self) -> tuple[int, int]:
        return PRECEDENCE.index(self.refusal), SLOT_RANK[self.slot.key]


@dataclass(frozen=True, slots=True)
class DeltaRefusal:
    """Every cause, highest precedence first. ``code`` / ``published`` are the top cause's."""

    causes: tuple[Cause, ...]

    def __post_init__(self) -> None:
        if not self.causes or list(self.causes) != sorted(self.causes, key=lambda c: c.order):
            raise ContractError("INVALID_DELTA_REFUSAL")

    @property
    def code(self) -> str:
        return self.causes[0].code

    @property
    def published(self) -> str:
        return self.causes[0].published


@dataclass(frozen=True, slots=True)
class Composed:
    """A lawful composition: the effective, its §6.5 effects, which conditions the patch placed (the rest is carried)
    and whether the universe changed (§14: what H8 reads)."""

    effective: AcceptedInterpretation
    effects: frozenset[DeltaEffect]
    placed_ids: frozenset[str]
    universe_changed: bool


def _check(base, ops, base_trace_id) -> tuple[PatchOp, ...]:
    if type(base) is not AcceptedInterpretation:
        raise ContractError("INVALID_COMPOSE_BASE")
    ops = tuple(ops)
    if not ops or any(type(op) is not PatchOp for op in ops):
        raise ContractError("INVALID_COMPOSE_OPS")
    if type(base_trace_id) is not str or not base_trace_id.strip():
        raise ContractError("INVALID_COMPOSE_BASE_TRACE_ID")
    return ops


def compose(base: AcceptedInterpretation, patch: SemanticPatch, *, base_trace_id: str) -> Composed | DeltaRefusal:
    """Contract §6: the one constructor of an effective with a base. ``base_trace_id`` is the record whose effective is
    ``base`` (the anchor or the pending's frame): ``AcceptedInterpretation`` does not carry its own trace."""
    if type(patch) is not SemanticPatch:
        raise ContractError("INVALID_COMPOSE_PATCH")
    return compose_ops(base, patch.ops, base_trace_id=base_trace_id)


def compose_ops(base: AcceptedInterpretation, ops, *, base_trace_id: str) -> Composed | DeltaRefusal:
    """``compose`` over a bare tuple of operations (frames and candidates of §8.4 are op subsets, not patches)."""
    ops = _check(base, ops, base_trace_id)
    causes = refusal_causes(base, ops)
    if causes:
        return DeltaRefusal(causes)
    return _apply(base, ops, base_trace_id)


# ---- §6 step 3b ----------------------------------------------------------------------------------------------------
def grouped_keys(base: AcceptedInterpretation) -> frozenset[str]:
    """Slots holding an atom inside an OR/NOT group: not addressable (§4.2)."""
    if base.logic is None:
        return frozenset()
    return frozenset(c.slot.key for c in base.conditions if base.logic.has_nonconjunctive_atom(c.condition_id))


def _touched(ops) -> tuple[dict, dict, dict]:
    """Per slot key: the writers, the KEEPs and the CLEARs (a dimension-wide CLEAR under each slot it covers). Plain
    dicts: reading a key never creates it (a KEEP's slot is not «written»)."""
    writers, keeps, clears = defaultdict(list), defaultdict(list), defaultdict(list)
    for op in ops:
        if op.op in WRITERS:
            writers[op.slot.key].append(op)
        elif op.op is PatchOpKind.KEEP:
            keeps[op.slot.key].append(op)
        else:
            for slot in covered(op.slot):
                clears[slot.key].append(op)
    return dict(writers), dict(keeps), dict(clears)


def _redirect(ops) -> PatchOp | None:
    return next((op for op in ops if op.op is PatchOpKind.REPLACE_UNIVERSE), None)


def _touched_request(base: AcceptedInterpretation, redirect: PatchOp):
    """§5.2: with a dual anchor the named subject's request stays; a single anchor's request is redirected."""
    return next((r for r in base.requests if r.subject is redirect.universe.subject), base.requests[0])


def _negative_to_positive(slot: Slot) -> str | None:
    if slot.dimension in POLARIZED_SLOT_DIMENSIONS and slot.polarity is Polarity.NEGATIVE:
        return Slot(slot.dimension, Polarity.POSITIVE).key
    return None


def _opposite(key: str) -> str | None:
    return {LOWER_KEY: UPPER_KEY, UPPER_KEY: LOWER_KEY}.get(key)


def _a81_collides(new: AcceptedCondition, kept: AcceptedCondition) -> bool:
    """§15.2: the new bound and the carried opposite one leave an empty or degenerate interval (``lo >= hi``)."""
    new_amount, new_lower, _ = money_bound(new)
    kept_amount, _, _ = money_bound(kept)
    low, high = (new_amount, kept_amount) if new_lower else (kept_amount, new_amount)
    return low >= high


def refusal_causes(base: AcceptedInterpretation, ops) -> tuple[Cause, ...]:
    """Every cause of §6 step 3b in ``ops`` over ``base``, highest precedence first, ties by the §4.2 slot order. The
    result does not depend on the order of the ops (§6.2)."""
    present = {c.slot.key for c in base.conditions} | {UNIVERSE_KEY}
    grouped = grouped_keys(base)
    writers, keeps, clears = _touched(ops)
    found: list[Cause] = []

    def carried(key: str) -> bool:  # standing in the base and touched by no op (a KEEP touches nothing)
        return key in present and key not in writers and key not in clears

    standing = defaultdict(list)
    for c in base.conditions:
        standing[c.slot.key].append(c)
    for op in ops:  # 1: an op on an atom inside an OR/NOT group (§4.2)
        if op.op in (PatchOpKind.CLEAR, PatchOpKind.SET, PatchOpKind.KEEP):
            hit = [s for s in covered(op.slot) if s.key in grouped]
            if hit:
                found.append(Cause(Refusal.TARGET_NOT_ADDRESSABLE, min(hit, key=lambda s: SLOT_RANK[s.key]), (op,),
                                   "§4.2"))
        if op.op is PatchOpKind.SET:  # a declared substitution whose victim sits inside a group is not addressable
            positive = _negative_to_positive(op.slot)
            if positive in grouped and carried(positive) and len(standing[positive]) == 1 and (
                    canonical_value(standing[positive][0]) == canonical_value(op.condition)):
                found.append(Cause(Refusal.TARGET_NOT_ADDRESSABLE, SLOT_BY_KEY[positive], (op,), "§4.2 + §6.5 F16"))
            opposite = _opposite(op.slot.key)
            if opposite in grouped and carried(opposite) and _comparable(op.condition, standing[opposite][0]) and (
                    _a81_collides(op.condition, standing[opposite][0])):
                found.append(Cause(Refusal.TARGET_NOT_ADDRESSABLE, SLOT_BY_KEY[opposite], (op,), "§4.2 + §15.2"))
    for key in set(writers) | set(keeps):  # 2: two writers of one slot; KEEP with a writer or a CLEAR of it
        w, k, c = writers.get(key, []), keeps.get(key, []), clears.get(key, [])
        if len(w) >= 2 or (k and (w or c)):
            found.append(Cause(Refusal.DELTA_SLOT_CONFLICT, SLOT_BY_KEY[key], tuple(w + k + c), "§6.2"))
    for op in ops:  # 3: KEEP of a slot the base does not hold
        if op.op is PatchOpKind.KEEP and op.slot.key not in present:
            found.append(Cause(Refusal.KEEP_ON_EMPTY_SLOT, op.slot, (op,), "§5.2 KEEP"))
    found += _not_accredited(base, ops, carried, standing, writers, clears)
    found += _cardinality_without_order(base, ops, present, writers, clears)
    return tuple(sorted(found, key=lambda c: c.order))


def _comparable(a: AcceptedCondition, b: AcceptedCondition) -> bool:
    """§15.2 (A8): two bounds are compared unless their units are known and of different classes."""
    first, second = unit_class(a), unit_class(b)
    return first is None or second is None or first == second


def _not_accredited(base, ops, carried, standing, writers, clears) -> list[Cause]:
    """Row 4 of §6 step 3b: F16's fail-closed branch and P-3 (§6.5), A8.1 with incomparable units (§15.2), the O1B lens
    (§19) and a request policy without owner (§6.6)."""
    out = []
    for op in ops:
        if op.op is PatchOpKind.REPLACE_UNIVERSE and op.universe.universe_kind is UniverseKind.OPPORTUNITIES:
            out.append(Cause(Refusal.OP_NOT_ACCREDITED, UNIVERSE_SLOT, (op,), "§19 DEP-O1B-OPPORTUNITIES"))
        if op.op is not PatchOpKind.SET:
            continue
        positive = _negative_to_positive(op.slot)
        if positive is not None:
            value = canonical_value(op.condition)
            same_patch = tuple(o for o in ops if o.op is PatchOpKind.SET and o.slot.key == positive
                               and canonical_value(o.condition) == value)
            several = carried(positive) and len(standing[positive]) > 1 and any(
                canonical_value(c) == value for c in standing[positive])
            if same_patch or several:
                out.append(Cause(Refusal.OP_NOT_ACCREDITED, op.slot, (op, *same_patch),
                                 "§6.5 P-3" if same_patch else "§6.5 F16 (R3-4)"))
        opposite = _opposite(op.slot.key)
        if opposite is not None and carried(opposite) and not _comparable(op.condition, standing[opposite][0]):
            out.append(Cause(Refusal.OP_NOT_ACCREDITED, op.slot, (op,), "§15.2 A8"))
    redirect = _redirect(ops)
    if redirect is not None and len(writers.get(UNIVERSE_KEY, [])) == 1:
        touched = _touched_request(base, redirect)
        target = redirect.universe
        if (touched.subject, touched.universe_kind) != (target.subject, target.universe_kind):
            kept = {c.condition_id for c in base.conditions if carried(c.slot.key)}
            by_id = {c.condition_id: c for c in base.conditions}
            for policy in base.policies:  # re-decided on the composed effective with no owner in M2a (§6.6)
                if set(policy.condition_ids) <= kept:
                    out.append(Cause(Refusal.OP_NOT_ACCREDITED, by_id[policy.condition_ids[0]].slot, (redirect,),
                                     "§6.6 policy without owner"))
    if any(r.universe_kind is UniverseKind.OPPORTUNITIES for r in base.requests):  # O1B reads the STATE (§6.6, §19)
        present = {c.slot.key for c in base.conditions}
        for op in ops:  # only an op that changes the STATE re-decides the lens (a KEEP or a CLEAR of nothing does not)
            for slot in covered(op.slot):
                if slot.dimension is SemanticDimension.STATE and (
                        op.op is PatchOpKind.SET or (op.op is PatchOpKind.CLEAR and slot.key in present)):
                    out.append(Cause(Refusal.OP_NOT_ACCREDITED, slot, (op,), "§6.6 + §19 O1B lens"))
    return out


def _cardinality_without_order(base, ops, present, writers, clears) -> list[Cause]:
    """§6.7 (R3-1), decided on the composed STATE: some request keeps a CARDINALITY without ORDER."""
    placed = {op.slot.key for op in ops if op.op in PLACERS}
    after = defaultdict(set)
    for request in base.requests:
        after[request.request_id] |= placed
    for c in base.conditions:
        if c.slot.key not in writers and c.slot.key not in clears:
            for request_id in c.request_ids:
                after[request_id].add(c.slot.key)
    if not any(CARDINALITY_KEY in slots and ORDER_KEY not in slots for slots in after.values()):
        return []
    if ORDER_KEY in present:  # the citation that takes the ORDER away opens it
        opened = tuple(op for op in ops
                       if op.op is PatchOpKind.CLEAR and ORDER_KEY in {s.key for s in covered(op.slot)})
    else:
        opened = tuple(op for op in ops if op.op is PatchOpKind.SET and op.slot.key == CARDINALITY_KEY)
    return [Cause(Refusal.DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER, SLOT_BY_KEY[ORDER_KEY], opened, "§6.7 R3-1")]


# ---- §6 steps 2, 3 and 5 -------------------------------------------------------------------------------------------
def _apply(base: AcceptedInterpretation, ops: tuple[PatchOp, ...], base_trace_id: str) -> Composed:
    writers, _keeps, clears = _touched(ops)
    sets = sorted((op for op in ops if op.op in PLACERS), key=lambda op: SLOT_RANK[op.slot.key])
    effects: set[DeltaEffect] = set()
    carried = [c for c in base.conditions if c.slot.key not in writers and c.slot.key not in clears]
    removed: set[str] = set()
    for op in sets:
        positive = _negative_to_positive(op.slot)
        if positive is not None:  # F16: the single carried positive of the negated value is inverted
            same = [c for c in carried if c.slot.key == positive]
            if len(same) == 1 and canonical_value(same[0]) == canonical_value(op.condition):
                removed.add(same[0].condition_id)
                effects.add(DeltaEffect.POLARITY_INVERTED)
        opposite = _opposite(op.slot.key)
        kept = [c for c in carried if c.slot.key == opposite] if opposite else []
        if kept and _comparable(op.condition, kept[0]) and _a81_collides(op.condition, kept[0]):  # A8.1
            removed.add(kept[0].condition_id)
            effects.add(DeltaEffect.VALUE_RANGE_REPLACED)
        if op.slot.key == TOPIC_POSITIVE_KEY and any(  # F3/F22: a different positive TOPIC is substituted
                c.slot.key == TOPIC_POSITIVE_KEY and c.value != op.condition.value for c in base.conditions):
            effects.add(DeltaEffect.TOPIC_REPLACED)
    carried = [c for c in carried if c.condition_id not in removed]
    requests, universe_changed = _requests(base, ops)
    request_ids = tuple(sorted(r.request_id for r in requests))
    if len(requests) != len(base.requests):  # R3, P-1: the only change a carried condition admits
        carried = [replace(c, request_ids=request_ids) for c in carried]
    taken = {c.condition_id for c in base.conditions}  # never reuse an id of the base, even of one that left
    placed = []
    for op in sets:  # ids are the reducer's: a patch id may collide with a base one
        condition_id, n = op.condition.condition_id, 1
        while condition_id in taken:
            condition_id, n = f"{op.condition.condition_id}~{n}", n + 1
        taken.add(condition_id)
        placed.append(replace(op.condition, condition_id=condition_id, request_ids=request_ids))
    kept_ids = {c.condition_id for c in carried}
    policies = tuple(p for p in base.policies if set(p.condition_ids) <= kept_ids)  # a policy goes with its condition
    gone = {c.condition_id for c in base.conditions} - kept_ids
    logic = _logic(base.logic, gone, [c.condition_id for c in placed])
    effective = canonical_interpretation(requests, (*carried, *placed), logic, policies, base_trace_id,
                                         base.root_trace_id)
    return Composed(effective, frozenset(effects), frozenset(c.condition_id for c in placed), universe_changed)


def _requests(base: AcceptedInterpretation, ops) -> tuple[tuple, bool]:
    redirect = _redirect(ops)
    if redirect is None:
        return base.requests, False
    touched = _touched_request(base, redirect)
    target = redirect.universe
    dataset = touched.dataset if touched.subject is target.subject else SUBJECT_DATASET[target.subject]
    request = replace(touched, subject=target.subject, universe_kind=target.universe_kind,
                      universe_origin=redirect.origin, dataset=dataset)
    return (request,), (touched.subject, touched.universe_kind) != (target.subject, target.universe_kind)


def _logic(logic: ConditionExpression | None, gone: set[str], added: list[str]) -> ConditionExpression | None:
    """The base's expression without the atoms of the conditions that left (all of the first-level AND: anything
    inside an OR/NOT was refused) and with the new ones joined to that AND. OR/NOT subtrees are kept as given."""
    if logic is None:
        return None
    tree = _prune(logic, gone)
    atoms = tuple(ConditionExpression(LogicOperator.ATOM, cid) for cid in added)
    if not atoms:
        return tree
    if tree is None:
        return ConditionExpression(LogicOperator.AND, None, atoms) if len(atoms) > 1 else atoms[0]
    if tree.operator is LogicOperator.AND:
        return ConditionExpression(LogicOperator.AND, None, (*tree.children, *atoms))
    return ConditionExpression(LogicOperator.AND, None, (tree, *atoms))


def _prune(node: ConditionExpression, gone: set[str]) -> ConditionExpression | None:
    if node.operator is LogicOperator.ATOM:
        return None if node.condition_id in gone else node
    if node.operator is not LogicOperator.AND:
        if set(node.atoms()) & gone:
            raise ContractError("REMOVED_ATOM_INSIDE_GROUP")  # refused first as TARGET_NOT_ADDRESSABLE
        return node
    children = [kept for kept in (_prune(child, gone) for child in node.children) if kept is not None]
    if not children:
        return None
    return children[0] if len(children) == 1 else ConditionExpression(LogicOperator.AND, None, tuple(children))
