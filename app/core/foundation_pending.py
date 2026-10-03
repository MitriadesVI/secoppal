"""M2A-4: the protocol of a turn that meets a pending (contract §8.3, §10.1, §17; M2A4-D1…D3, D7).

``respond(pending, turn, *, trace_id, now, asked_ops)`` is the one decision of a turn that arrives with a pending in the
store, over typed values, an injected instant and the pending itself. It never reads text (INV-M2A-24) and changes no
runtime: nothing outside M2a imports it. The turn is one of

* ``OptionEvent(pending_id, candidate_id)``: §8.3 row 1, ``compose(frame, candidate.patch)`` with no LLM; an option of a
  pending that no longer exists (cancelled, expired, replaced) is ``INFORM STALE_OPTION``, 0 calls, anchor intact;
* ``ResetEvent(pending_id)``: «reset» before a ``CONTEXT_EXPIRED_CONFIRM`` (§10.1, §17.2 c);
* ``PendingTurn(facts, ops, no_selection)``: P with its closed facts (``TurnFacts``, ``pending = True``) and its typed
  operations. ``classify_turn`` (M2A-3) decides the scope; this module decides rows 2 and 4–8 over it.

Row 2 (a cancel directive) and row 3 (a new search with a pending vigente) start a NEW_SEARCH under ``control =
CANCEL_PENDING`` (§17.2 a, b); otherwise the rest of P is a delta ON THE FRAME (A7, §17.3), asked or executed like any
delta (``delta_outcome``) and, if it asks, its pending REPLACES the cancelled one. With a ``CONTEXT_EXPIRED_CONFIRM`` (or a
pending without frame) there is no base to compose on, so the rest of P is a new search ONLY IF ¬Continuación(P) ∧
Admisible sola(P), and ``CLARIFY NO_ANCHOR`` otherwise (M2A4-D1, A9: a continuation never starts a search). A cancel with
nothing to apply (no operations, no new search) cancels and executes nothing: ``INFORM``, code ``None`` (M2A4-D12).

Rows 5–8 are ``REASK`` over the same pending with ``asks + 1`` (plazo, frame, question and candidates untouched); an answer
that fails with ``asks`` at the cap CANCELS the pending (§8.3, cause d: control ``CANCEL_PENDING``, the label of the failed
answer, nothing left; M2A4-D10). An expired pending (``now`` ≥ its deadline) is cancelled by cause e at THIS turn: a text
turn proceeds as if there were none; an event gets ``STALE_OPTION`` (M2A4-D11). ``Cancellation.eliminates_expired_anchor``
carries M2A4-D3: every cause that cancels a ``CONTEXT_EXPIRED_CONFIRM`` removes the expired anchor.

INV-M2A-23 is a property of the decision itself: ``control`` and ``cancellation`` exist together, and a pending that does
not remain is answered, cancelled, or replaced by the one this turn created.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum

from app.core.foundation_accepted import (
    MAX_ASKS, OriginKind, PatchOp, PatchOpKind, PatchOriginKind, PendingClarification, PendingControl, SemanticPatch,
    Slot, TurnResolution, TurnScope,
)
from app.core.foundation_delta import SLOT_RANK, covered_keys, dimension_key
from app.core.foundation_delta_outcome import (
    HOST_CODES, DeltaDisposition, DeltaOutcome, Dependency, delta_outcome,
)
from app.core.foundation_pending_answer import (
    AnswerKind, AnswerResult, answer_by_event, answer_in_language, answer_keys, unresolved_keys,
)
from app.core.foundation_pending_cancel import (
    CancelCause, Cancellation, cancel_pending, fragment_base, holds_expired_anchor,
)
from app.core.foundation_pending_clock import pending_expired
from app.core.foundation_pending_create import Creation, pending_from_question, pending_without_anchor
from app.core.foundation_turn_scope import AnchorState, InputKind, TurnFacts, classify_turn
from app.core.interpretation_contract import ContractError

STALE_OPTION = "STALE_OPTION"
NO_ANCHOR = "NO_ANCHOR"
_ANSWER_OPS = frozenset({PatchOpKind.SET, PatchOpKind.SET_ORDER, PatchOpKind.REPLACE_UNIVERSE})
_INFORM_ROWS = frozenset({TurnScope.OUT_OPERATION_CHANGE, TurnScope.OUT_UNSUPPORTED_PROPOSAL,
                          TurnScope.OUT_REFERENT_EXTENSIONAL, TurnScope.OUT_RELATIVE_TO_PREVIOUS})


@dataclass(frozen=True, slots=True)
class OptionEvent:
    pending_id: str
    candidate_id: str


@dataclass(frozen=True, slots=True)
class ResetEvent:
    pending_id: str


@dataclass(frozen=True, slots=True)
class PendingTurn:
    """P of a text turn that meets a pending: its closed facts, its typed operations (the ones the authorities
    resolved) and whether it declares not to choose (§8.3 row 6, the ``no_selection`` label)."""

    facts: TurnFacts
    ops: tuple[PatchOp, ...] = ()
    no_selection: bool = False
    unresolved: frozenset[Slot] = frozenset()  # slots of P's conditions no authority resolved (§8.3 row 7)

    def __post_init__(self) -> None:
        if type(self.facts) is not TurnFacts or type(self.no_selection) is not bool:
            raise ContractError("INVALID_PENDING_TURN")
        unresolved_keys(self.unresolved)
        if type(self.ops) is not tuple or any(type(op) is not PatchOp for op in self.ops):
            raise ContractError("INVALID_PENDING_TURN_OPS")


class DecisionKind(str, Enum):
    PENDING_ANSWER = "PENDING_ANSWER"  # rows 1 and 4: the answer composed on the frame and executes
    NEW_SEARCH = "NEW_SEARCH"  # rows 2 and 3: a new search under CANCEL_PENDING (its acceptance is M2A-1b's)
    DELTA = "DELTA"  # row 2: the rest of P, a delta on the frame, executed
    REASK = "REASK"  # rows 5–8 (or the cap: the pending is cancelled)
    INFORM = "INFORM"  # an OUT_* row, STALE_OPTION, or a cancel with nothing to apply
    CLARIFY = "CLARIFY"  # NO_ANCHOR after a cancel with no base, or the rest of P asks
    DEPENDENCY = "DEPENDENCY"  # a declared §19 red: nothing executes
    EXPIRED = "EXPIRED"  # the pending expired (cause e): the text turn proceeds without it


_RESOLUTION = {DecisionKind.PENDING_ANSWER: TurnResolution.PENDING_ANSWER,
               DecisionKind.NEW_SEARCH: TurnResolution.NEW_SEARCH, DecisionKind.DELTA: TurnResolution.DELTA,
               DecisionKind.REASK: TurnResolution.REASK, DecisionKind.INFORM: TurnResolution.INFORM,
               DecisionKind.CLARIFY: TurnResolution.CLARIFY}


@dataclass(frozen=True, slots=True)
class PendingDecision:
    """What a turn comes to. ``pending`` is what REMAINS after it (kept, re-asked with ``asks + 1``, or the one this turn
    created); ``cancellation`` ⇔ ``control = CANCEL_PENDING`` (INV-M2A-23); ``base_trace_id`` is the record the composition's
    base lives in (the asking turn, §3.1)."""

    kind: DecisionKind
    scope: TurnScope | None
    code: str | None = None
    cancellation: Cancellation | None = None
    answer: AnswerResult | None = None
    outcome: DeltaOutcome | None = None
    created: Creation | None = None
    pending: PendingClarification | None = None
    dependency: Dependency | None = None
    base_trace_id: str | None = None
    context_reset: bool = False

    def __post_init__(self) -> None:
        if self.cancellation is not None and self.pending is not None and (
                self.created is None or self.pending is not self.created.pending):
            raise ContractError("CANCELLED_PENDING_REMAINS")  # a cancelled pending is gone, or replaced by the new one

    @property
    def resolution(self) -> TurnResolution | None:
        return _RESOLUTION.get(self.kind)

    @property
    def control(self) -> PendingControl | None:
        return None if self.cancellation is None else self.cancellation.control

    @property
    def anchor_eliminated(self) -> bool:
        """M2A4-D3: the cancellation removed an expired anchor (writing ``anchor_trace_id = None`` is M2A-6's)."""
        return self.cancellation is not None and self.cancellation.eliminates_expired_anchor

    @property
    def effects(self) -> frozenset[str]:
        return frozenset() if self.cancellation is None else self.cancellation.effects


# ---- helpers --------------------------------------------------------------------------------------------------------
def _stale(pending: PendingClarification | None, cancellation: Cancellation | None = None) -> PendingDecision:
    """§8.3: an option whose pending no longer exists. The pending that does exist (a stale id) stays intact."""
    return PendingDecision(DecisionKind.INFORM, TurnScope.IN_PROTOCOL, STALE_OPTION, cancellation,
                           pending=pending if cancellation is None else None)


def _within(pending: PendingClarification, ops: tuple[PatchOp, ...]) -> bool:
    """Whether every typed condition of P (what it sets or redirects) falls in ``answer_slots``. It checks the fact
    ``within_answer_slots`` (§8.3 row 3), which the CALLER supplies: a new search carries its conditions outside ``ops``
    (its acceptance is not a patch), so the fact cannot be derived from them."""
    inside = answer_keys(pending)
    return all(covered_keys((op,)) <= inside for op in ops if op.op in _ANSWER_OPS)


def _origin_kind(ops: tuple[PatchOp, ...]) -> PatchOriginKind:
    return PatchOriginKind.TEXT if all(op.origin.kind is OriginKind.SPAN for op in ops) else PatchOriginKind.EVENT


def _answered(pending: PendingClarification, result: AnswerResult, scope: TurnScope) -> PendingDecision:
    """An answer's result as a decision: executed (the pending is answered, consumed), a declared red (the pending stays
    intact), or re-asked (rows 5–8: ``asks + 1``, or cancelled when the cap is reached, §8.3)."""
    if result.kind is AnswerKind.EXECUTE:
        return PendingDecision(DecisionKind.PENDING_ANSWER, scope, None, answer=result,
                               base_trace_id=pending.asked_trace_id)
    if result.kind is AnswerKind.DEPENDENCY:
        return PendingDecision(DecisionKind.DEPENDENCY, scope, None, answer=result, pending=pending,
                               dependency=result.dependency)
    if pending.asks >= MAX_ASKS:  # §8.3: «asks llega a 2 y vuelve a fallar ⇒ la pendiente se cancela»
        return PendingDecision(DecisionKind.REASK, scope, result.code, cancel_pending(pending, CancelCause.EXHAUSTED),
                               answer=result)
    return PendingDecision(DecisionKind.REASK, scope, result.code, answer=result,
                           pending=replace(pending, asks=pending.asks + 1))


def _new_search(pending: PendingClarification, cause: CancelCause, context_reset: bool) -> PendingDecision:
    return PendingDecision(DecisionKind.NEW_SEARCH, TurnScope.IN_NEW_SEARCH, None, cancel_pending(pending, cause),
                           context_reset=context_reset)


def _pure_cancel(pending: PendingClarification, cause: CancelCause, scope: TurnScope) -> PendingDecision:
    """A cancel with nothing to apply: nothing executes, the anchor is as it was (§17.4; M2A4-D12)."""
    return PendingDecision(DecisionKind.INFORM, scope, None, cancel_pending(pending, cause))


def _starts_a_search_alone(facts: TurnFacts) -> bool:
    """M2A4-D1: with no base to compose on, the rest of P starts a search only if ¬Continuación(P) ∧ Admisible sola(P)
    (a mark of continuation wins, A9). Three-valued like ``classify_turn``: a fact that decides nothing may stay ``None``."""
    if facts.admissible_alone is False or facts.continuation is True:
        return False
    for name in ("admissible_alone", "continuation"):
        if getattr(facts, name) is None:
            raise ContractError(f"UNDETERMINED_FACT:{name}")
    return True


# ---- events ---------------------------------------------------------------------------------------------------------
def _event(pending: PendingClarification | None, event: OptionEvent | ResetEvent, now: datetime) -> PendingDecision:
    if type(event.pending_id) is not str or not event.pending_id.strip():
        raise ContractError("INVALID_EVENT")
    if pending is None:
        return _stale(None)
    if pending_expired(pending, now):  # cause e is written at the next turn, this one
        return _stale(None, cancel_pending(pending, CancelCause.EXPIRED))
    if event.pending_id != pending.pending_id:
        return _stale(pending)
    if type(event) is ResetEvent:  # §10.1: only before a CONTEXT_EXPIRED_CONFIRM (cancel_pending refuses any other)
        return _pure_cancel(pending, CancelCause.RESET, TurnScope.IN_PROTOCOL)
    if type(event.candidate_id) is not str or all(c.candidate_id != event.candidate_id for c in pending.candidate_patches):
        return _stale(pending)  # an option this pending never offered: the host never wrote it
    return _answered(pending, answer_by_event(pending, event.candidate_id), TurnScope.IN_PROTOCOL)


# ---- text -----------------------------------------------------------------------------------------------------------
def _cancel_turn(pending: PendingClarification, turn: PendingTurn, facts: TurnFacts, scope: TurnScope,
                 context_reset: bool, trace_id: str, now: datetime) -> PendingDecision:
    """Row 2: P carries ``CANCEL_PENDING`` (§17.2 a); what is left of P is applied on the frame, or is a new search."""
    frame = fragment_base(pending)
    if frame is None:  # M2A4-D1: no base (CONTEXT_EXPIRED_CONFIRM, or no frame): a new search alone, or NO_ANCHOR
        if not turn.ops and facts.own_conditions == 0:  # no fragment at all: the cancel alone (§17.3: «el fragmento»)
            return _pure_cancel(pending, CancelCause.DIRECTIVE, TurnScope.IN)
        if _starts_a_search_alone(facts):
            reset = facts.anchor is AnchorState.FOUNDATION_SEARCH and not facts.anchor_expired and (
                not holds_expired_anchor(pending))
            return _new_search(pending, CancelCause.DIRECTIVE, reset)
        created = pending_without_anchor(NO_ANCHOR, asked_trace_id=trace_id, now=now)
        return PendingDecision(DecisionKind.CLARIFY, TurnScope.OUT_NO_ANCHOR, NO_ANCHOR,
                               cancel_pending(pending, CancelCause.DIRECTIVE), created=created, pending=created.pending)
    if scope is TurnScope.IN_NEW_SEARCH:
        return _new_search(pending, CancelCause.DIRECTIVE, context_reset)
    if turn.unresolved:  # the rest of P says a value no authority resolved: nothing to compose it as, and no span to ask on
        slot = min(turn.unresolved, key=lambda s: SLOT_RANK[s.key])
        return PendingDecision(DecisionKind.INFORM, TurnScope.IN, f"UNRESOLVED_VALUE:{dimension_key(slot)}",
                               cancel_pending(pending, CancelCause.DIRECTIVE))
    if not turn.ops:
        return _pure_cancel(pending, CancelCause.DIRECTIVE, TurnScope.IN)
    cancellation = cancel_pending(pending, CancelCause.DIRECTIVE)
    base_trace = pending.asked_trace_id  # §17.3: the frame, never a turn older than the one that asked (A7)
    outcome = delta_outcome(frame, SemanticPatch(turn.ops, _origin_kind(turn.ops)), base_trace_id=base_trace,
                            anchor_expired=False)
    if outcome.disposition is DeltaDisposition.EXECUTE:
        return PendingDecision(DecisionKind.DELTA, TurnScope.IN, None, cancellation, outcome=outcome,
                               base_trace_id=base_trace)
    if outcome.disposition is DeltaDisposition.DEPENDENCY:
        return PendingDecision(DecisionKind.DEPENDENCY, TurnScope.IN, None, cancellation, outcome=outcome,
                               dependency=outcome.dependency)
    if outcome.disposition is DeltaDisposition.INFORM:
        return PendingDecision(DecisionKind.INFORM, TurnScope.IN, outcome.code, cancellation, outcome=outcome)
    created = pending_from_question(outcome.pending, base=frame, base_trace_id=base_trace, asked_trace_id=trace_id,
                                    now=now)
    if created.disposition is DeltaDisposition.CLARIFY:
        return PendingDecision(DecisionKind.CLARIFY, TurnScope.IN, outcome.code, cancellation, outcome=outcome,
                               created=created, pending=created.pending)
    kind = DecisionKind.DEPENDENCY if created.disposition is DeltaDisposition.DEPENDENCY else DecisionKind.INFORM
    return PendingDecision(kind, TurnScope.IN, created.code, cancellation, outcome=outcome, created=created,
                           dependency=created.dependency)


def _text(pending: PendingClarification, turn: PendingTurn, trace_id: str, now: datetime,
          asked_ops: tuple[PatchOp, ...]) -> PendingDecision:
    facts = turn.facts
    if facts.input_kind is not InputKind.TEXT or facts.pending is not True:
        raise ContractError("INVALID_FACT:pending_turn")
    if facts.within_answer_slots is True and not _within(pending, turn.ops):  # a fact the typed operations contradict
        raise ContractError("INCONSISTENT_FACT:within_answer_slots")
    # Marco(P) is no part of the rule when there is no base to compose on (M2A4-D1): the probe only reaches the OUT_* rows
    probe = replace(facts, frame=True) if (facts.cancel_directive and facts.frame is None
                                           and fragment_base(pending) is None) else facts
    classification = classify_turn(probe)
    scope = classification.scope
    if scope in _INFORM_ROWS:  # §7.1 rows 4, 4b, 5, 6: INFORM, the pending intact (a pending vigente never gets another)
        code = HOST_CODES["H4"] if scope is TurnScope.OUT_RELATIVE_TO_PREVIOUS else classification.code
        return PendingDecision(DecisionKind.INFORM, scope, code, pending=pending)
    if scope is not TurnScope.IN and scope is not TurnScope.IN_NEW_SEARCH:
        raise ContractError("SCOPE_NOT_DEFINED_UNDER_PENDING")
    if facts.cancel_directive:
        return _cancel_turn(pending, turn, facts, scope, classification.context_reset, trace_id, now)
    if scope is TurnScope.IN_NEW_SEARCH:  # row 3: A7/A9, the host writes the cancellation even with no directive
        return _new_search(pending, CancelCause.NEW_SEARCH, classification.context_reset)
    result = answer_in_language(pending, turn.ops, no_selection=turn.no_selection, asked_ops=asked_ops,
                                unresolved=turn.unresolved)
    return _answered(pending, result, TurnScope.IN)


def respond(pending: PendingClarification | None, turn: OptionEvent | ResetEvent | PendingTurn, *, trace_id: str,
            now: datetime, asked_ops: tuple[PatchOp, ...] = ()) -> PendingDecision:
    """The decision of a turn that meets ``pending`` (the one the store holds, or ``None`` if none): see the module. ``now``
    is the injected instant, ``trace_id`` the turn's own trace (the id of the pending it may create), ``asked_ops`` the
    operations of the turn that asked (F24; ``Creation.asked_ops`` gives them when the pending is created)."""
    if type(trace_id) is not str or not trace_id.strip():
        raise ContractError("INVALID_TRACE_ID")
    asked_ops = tuple(asked_ops)
    if any(type(op) is not PatchOp for op in asked_ops):
        raise ContractError("INVALID_ASKED_OPS")
    if type(turn) in (OptionEvent, ResetEvent):
        return _event(pending, turn, now)
    if type(turn) is not PendingTurn:
        raise ContractError("INVALID_PENDING_TURN")
    if type(pending) is not PendingClarification:
        raise ContractError("NO_PENDING")
    if pending_expired(pending, now):  # §17.2 e: the turn proceeds as if there were none; the cancellation is written
        return PendingDecision(DecisionKind.EXPIRED, None, None, cancel_pending(pending, CancelCause.EXPIRED))
    return _text(pending, turn, trace_id, now, asked_ops)
