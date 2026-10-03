"""M2A-4: the answer to a vigente pending (contract §8.3 rows 1 and 4–8 and its «Selección», §8.4, INV-M2A-08).

An answer is composed on the pending's ``frame``, NEVER on the base the asking turn had (§8.4): ``compose_answer`` takes
the frame and nothing else, and the effective it builds names the asking turn as its base (the record that holds the
frame, §3.1 ``base_trace_id``). Nothing here reads text (INV-M2A-24): an answer is a tuple of typed ``PatchOp``s.

«Selección» (rows 4, 5, 7, 8) in the contract's order, over P's operations:

1. §6 step 2 on P as it stands (two writers of a slot; ``KEEP`` with a writer or a ``CLEAR`` of the same slot): row 7;
2. F24: P identical (same multiset of (op, slot, encoded value / destination)) to the patch of the turn that asked does
   not answer: row 7. The comparison is made BEFORE normalizing, as the contract writes it (U27);
3. with candidates, the ``KEEP``s of slots the frame holds that no other operation of P touches are the identity and go;
   an empty P is row 8. M2A4-D20 (HERA, on ZEUS P1-1): F24 is judged AGAIN on what is left once the identity KEEPs go, for
   EVERY pending: if it is the asking patch it answers nothing, row 7; and, with candidates, so does a PART of it
   (``adds_nothing``: the turn that asked may have carried other resolved operations). A KEEP of filler no longer reaches
   the option that is the asking patch;
4. P touching no slot of ``answer_slots`` is row 8; touching them and others, row 5 (F25);
5. with candidates, EXACTLY one must equal P by identity (its whole patch is composed, never P) or it is row 7; with none,
   P is composed on the frame.

Row 6 (``no_selection``, F24) is decided first, after row 5. A language selection on a ``CONTEXT_EXPIRED_CONFIRM`` is
row 7 (M2A4-D2): «continuar» is confirmed only by event (row 1). The composed answer must execute: a refusal of §6 step
3b or a host question the answer leaves is row 7 («sigue ambiguo», never a hybrid), a §19 COMPOSICION dependency is that
dependency (declared red, fail closed). The pending an answer is composed from is exempt from the host rule it answers
(``answered``): choosing H6's «buscar con ambos» does not raise H6 again (``foundation_delta_outcome.executes``).
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from enum import Enum

from app.core.foundation_accepted import (
    AcceptedInterpretation, PatchOp, PatchOpKind, PendingClarification, Slot,
)
from app.core.foundation_delta import (
    UNIVERSE_KEY, WRITERS, Composed, DeltaRefusal, compose_ops, covered, covered_keys,
)
from app.core.foundation_delta_outcome import HOST_CODES, Dependency, FindingKind, host_findings
from app.core.interpretation_contract import ContractError

EXPIRED_CONFIRM = "CONTEXT_EXPIRED_CONFIRM"
RULE_OF_CODE = {code: rule for rule, code in HOST_CODES.items()}  # TERRITORY_VS_CARRIED_BUYER -> H6, ...
# §19 DEP-M1-PENDING-FRAME (owner M2A-1b): a pending of M1's first turn has no ``frame`` M2a can compose on.
DEP_M1_PENDING_FRAME = Dependency("DEP-M1-PENDING-FRAME", ("M2A-1b",))


class AnswerKind(str, Enum):
    EXECUTE = "EXECUTE"  # PENDING_ANSWER: the answer composed on the frame and executes
    REASK = "REASK"  # §8.3 rows 5, 6, 7, 8: the pending stays (or the cap cancels it, the façade decides)
    DEPENDENCY = "DEPENDENCY"  # a declared §19 red: nothing executes, the pending stays intact


@dataclass(frozen=True, slots=True)
class AnswerResult:
    """``row`` is the §8.3 row that decided; ``patch`` the operations composed on the frame (the chosen candidate's, or
    P's) and ``candidate_id`` the candidate chosen, if any."""

    kind: AnswerKind
    row: int
    code: str | None = None
    composed: Composed | None = None
    dependency: Dependency | None = None
    patch: tuple[PatchOp, ...] = ()
    candidate_id: str | None = None


def _reask(row: int, code: str) -> AnswerResult:
    return AnswerResult(AnswerKind.REASK, row, code)


# ---- identities ----------------------------------------------------------------------------------------------------
def op_identity(op: PatchOp) -> tuple:
    """§8.3 «Selección»: (op, slot, canonical encoded value) or (``REPLACE_UNIVERSE``, destination). Never text, ids,
    ``display_key`` or ``candidate_id``."""
    if op.condition is not None:
        payload = tuple(op.condition.value)
    elif op.universe is not None:
        payload = (op.universe.subject.value, op.universe.universe_kind.value)
    else:
        payload = ()
    return op.op.value, op.slot.key, payload


def ops_identity(ops) -> tuple:
    """The multiset of identities of ``ops`` (a patch is unordered)."""
    return tuple(sorted(op_identity(op) for op in ops))


def answer_keys(pending: PendingClarification) -> frozenset[str]:
    """The slots an open answer may fix, a dimension-wide slot expanded into the slots it covers."""
    return frozenset(s.key for slot in pending.answer_slots for s in covered(slot))


def unresolved_keys(unresolved) -> frozenset[str]:
    """The slots of P's conditions whose value no authority resolved (§8.3 row 7: «autoridad no RESOLVED»). They are not a
    ``PatchOp`` (there is no typed value to carry), but they are still what P says: they touch their slot and write it."""
    if type(unresolved) is not frozenset or any(type(slot) is not Slot or slot.dimension_wide for slot in unresolved):
        raise ContractError("INVALID_UNRESOLVED_SLOTS")
    return frozenset(slot.key for slot in unresolved)


def slot_conflicts(ops, unresolved: frozenset[Slot] = frozenset()) -> bool:
    """§6 step 2 on ``ops`` as they stand: two writers of one slot, or a ``KEEP`` with a writer or a ``CLEAR`` of the
    same slot (a dimension-wide ``CLEAR`` covers each of its slots). An unresolved condition writes its slot. Independent
    of any base."""
    writers, keeps, clears = defaultdict(int), set(), set()
    for key in unresolved_keys(unresolved):
        writers[key] += 1
    for op in ops:
        if op.op in WRITERS:
            writers[op.slot.key] += 1
        elif op.op is PatchOpKind.KEEP:
            keeps.add(op.slot.key)
        else:
            clears |= {s.key for s in covered(op.slot)}
    return any(n >= 2 for n in writers.values()) or any(key in writers or key in clears for key in keeps)


def adds_nothing(ops, asked) -> bool:
    """M2A4-D20: the answer, once its identity ``KEEP``s are gone, is a non-empty SUBSET (as a multiset of identities) of
    the patch that asked: it repeats part of what the product itself proposed and adds nothing that tells one candidate from
    another. An answer identical to the asking patch is the whole of it (F24); the part of it is the same refusal to choose
    for the user (the turn that asked may have carried other resolved operations: «en Antioquia» alone repeats the open one)."""
    asked_left = Counter(op_identity(op) for op in asked)
    left = Counter(op_identity(op) for op in ops)
    return bool(left) and not (left - asked_left)


def without_identity_keeps(frame: AcceptedInterpretation | None, ops,
                           unresolved: frozenset[Slot] = frozenset()) -> tuple[PatchOp, ...]:
    """The ``KEEP``s of a slot the ``frame`` holds that no OTHER operation of P touches are the identity (§8.3)."""
    ops, extra = tuple(ops), unresolved_keys(unresolved)
    present = {UNIVERSE_KEY} | ({c.slot.key for c in frame.conditions} if frame is not None else set())
    kept = []
    for i, op in enumerate(ops):
        others = covered_keys(o for j, o in enumerate(ops) if j != i) | extra
        if op.op is PatchOpKind.KEEP and op.slot.key in present and op.slot.key not in others:
            continue
        kept.append(op)
    return tuple(kept)


# ---- the composition: the frame, never the base ---------------------------------------------------------------------
def compose_answer(frame: AcceptedInterpretation, ops, *, base_trace_id: str, answered: str | None) -> AnswerResult:
    """``compose(frame, ops)`` judged as an answer (§8.3 rows 4 and 7, §19): a refusal of §6 step 3b is row 7; the first
    host finding the answer leaves — except the rule it ANSWERS (``answered``) — is a question still open (row 7) or, when
    it is a §19 COMPOSICION dependency, that dependency; otherwise it executes."""
    ops = tuple(ops)
    composed = compose_ops(frame, ops, base_trace_id=base_trace_id)
    if isinstance(composed, DeltaRefusal):
        return _reask(7, "ANSWER_UNRESOLVED")
    first = next((f for f in host_findings(frame, ops, composed)
                  if f.kind is FindingKind.DEPENDENCY or f.rule != answered), None)
    if first is None:
        return AnswerResult(AnswerKind.EXECUTE, 4, None, composed, None, ops)
    if first.kind is FindingKind.DEPENDENCY:
        return AnswerResult(AnswerKind.DEPENDENCY, 4, None, None, first.dependency, ops)
    return _reask(7, "ANSWER_UNRESOLVED")


def _compose_on_frame(pending: PendingClarification, ops, answered: str | None, candidate_id: str | None) -> AnswerResult:
    """The pending's frame is what an answer composes on; one that has no frame cannot be composed (M2A-1b)."""
    if pending.frame is None:
        return AnswerResult(AnswerKind.DEPENDENCY, 4, None, None, DEP_M1_PENDING_FRAME, tuple(ops), candidate_id)
    made = compose_answer(pending.frame, ops, base_trace_id=pending.asked_trace_id, answered=answered)
    return AnswerResult(made.kind, made.row, made.code, made.composed, made.dependency, made.patch, candidate_id)


# ---- row 1: an option event -----------------------------------------------------------------------------------------
def answer_by_event(pending: PendingClarification, candidate_id: str) -> AnswerResult:
    """§8.3 row 1: the event names its candidate (no selection, no LLM): ``compose(frame, candidate.patch)``. The caller has
    checked that the pending is the event's and still vigente, and that ``candidate_id`` is one of its candidates."""
    candidate = next((c for c in pending.candidate_patches if c.candidate_id == candidate_id), None)
    if candidate is None:
        raise ContractError("UNKNOWN_CANDIDATE")
    answered = RULE_OF_CODE.get(pending.reason)
    made = _compose_on_frame(pending, candidate.patch.ops, answered, candidate_id)
    row = made.row if made.kind is AnswerKind.REASK else 1  # a candidate that no longer composes is row 7 (corrupt)
    return AnswerResult(made.kind, row, made.code, made.composed, made.dependency, made.patch, candidate_id)


# ---- rows 4–8: an answer in language --------------------------------------------------------------------------------
def answer_in_language(pending: PendingClarification, ops, *, no_selection: bool,
                       asked_ops: tuple[PatchOp, ...], unresolved: frozenset[Slot] = frozenset()) -> AnswerResult:
    """§8.3 «Selección» over P's ``ops`` (typed; the host reads no text). ``asked_ops`` are the operations of the turn that
    asked (F24): the contract compares with «el parche del registro ``asked_trace_id``», but a ``CLARIFY`` record holds no
    patch (``AcceptedTurnRecord``), so the caller supplies them (M2A4-D13). F24 is judged twice (M2A4-D20): on P as it
    stands (the sealed order) and, with candidates, on P without its identity ``KEEP``s."""
    if type(pending) is not PendingClarification:
        raise ContractError("INVALID_PENDING")
    ops, asked = tuple(ops), tuple(asked_ops)
    if any(type(op) is not PatchOp for op in (*ops, *asked)):
        raise ContractError("INVALID_ANSWER_OPS")
    inside, said = answer_keys(pending), unresolved_keys(unresolved)  # ``said``: touched by a value nobody resolved
    if no_selection:  # row 6 (the label), after row 5: an answer plus an edit of another slot is re-asked as such
        touched = covered_keys(ops) | said
        if touched & inside and touched - inside:
            return _reask(5, "ANSWER_EDITS_OTHER_SLOT")
        return _reask(6, "OPTION_NOT_CHOSEN")
    if slot_conflicts(ops, unresolved):  # step 1
        return _reask(7, "ANSWER_UNRESOLVED")
    if ops and not said and ops_identity(ops) == ops_identity(asked):  # step 2, F24: before normalizing (P is not
        # identical to the asking patch if it also says something no authority resolved)
        return _reask(7, "ANSWER_UNRESOLVED")
    # M2A4-D20 (HERA, on ZEUS P1-1): F24 holds for EVERY pending, not only the one whose candidate is the asking patch (§8.4:
    # «un candidato idéntico al parche que preguntó sólo se alcanza por evento»): once the identity KEEPs go, an answer that
    # is the asking patch answers nothing. Judged for every pending; whether the KEEPs also go from what is COMPOSED stays
    # with the candidates (U27, M2A4-D4). Never an empty answer: that is row 8 (F29).
    stripped = without_identity_keeps(pending.frame, ops, unresolved)
    if stripped and not said and ops_identity(stripped) == ops_identity(asked):
        return _reask(7, "ANSWER_UNRESOLVED")
    if pending.candidate_patches:  # step 3
        ops = stripped
        # and a PART of it tells no candidate from another (the turn that asked may carry other resolved operations)
        if not said and adds_nothing(ops, asked):
            return _reask(7, "ANSWER_UNRESOLVED")
    touched = covered_keys(ops) | said
    if not touched & inside:  # step 4 (an empty P included: F29, never a cancellation)
        return _reask(8, "PENDING_UNANSWERED")
    if touched - inside:
        return _reask(5, "ANSWER_EDITS_OTHER_SLOT")
    if said or pending.reason == EXPIRED_CONFIRM:  # a value no authority resolved selects nothing (row 7); M2A4-D2:
        return _reask(7, "ANSWER_UNRESOLVED")  # «continuar» is confirmed only by event
    if pending.candidate_patches:  # step 5: exactly one candidate equals P; its WHOLE patch is composed
        wanted = ops_identity(ops)
        chosen = [c for c in pending.candidate_patches if ops_identity(c.patch.ops) == wanted]
        if len(chosen) != 1:
            return _reask(7, "ANSWER_UNRESOLVED")
        return _compose_on_frame(pending, chosen[0].patch.ops, RULE_OF_CODE.get(pending.reason),
                                 chosen[0].candidate_id)
    return _compose_on_frame(pending, ops, None, None)
