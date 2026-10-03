"""M2A-4: the creation of a pending (contract §8.1, §8.2, §8.4, INV-M2A-26; M2A4-D6).

A ``PendingClarification`` is built here and nowhere else, and only if the LAW of the possible pending holds (§8.4, U20):
its ``frame`` is ``None`` or a valid effective that holds nothing open and raises no §19 COMPOSICION dependency (read
from the base), every candidate composes on it with no cause of §6 step 3b, and some answer executes. If not, the
creation says so as an ``INFORM`` with the code of the rule that would have asked, 0 calls, and builds NOTHING; a frame
that raises a §19 dependency is that ``DEPENDENCY`` (U29). The candidates are the options resolved AT ASK TIME (§8.2);
``answer_slots`` ⊇ their slots; the plazo is 30 minutes from the injected ``now`` (``foundation_pending_clock``).

Entry points, one per source of §8.4's table:

* ``pending_from_question``: a ``PendingQuestion`` of M2A-3 (§6 step 3b with one cause, H1, H3, H6, H8, H11, §7.1 row 6);
  its law is judged again here, from the base (it never trusts the question);
* ``ask_authority`` (M2A4-D6): the rules whose firing is an authority's or the host's reading, which no patch of resolved
  values carries (H5, H7, H9, H10, ``UNRESOLVED_VALUE:<DIM>`` with candidates, ``VALUE_WITHOUT_COMPARATOR``). M2A-4 does
  not decide that the rule fires (INV-M2A-07, M2A-3b); it builds the pending of a rule that did;
* ``ask_fresh``: a new search that asks (§7.2 c), ``frame`` = what P resolved, ``base_trace_id = None``. The frame is
  built by ``from_acceptance`` (M2A-1b, not yet there): the caller supplies it, and without one the pending is the
  declared red ``DEP-M1-PENDING-FRAME``;
* ``pending_without_anchor``: §7.1 row 1 (``NO_ANCHOR``, ``ANCHOR_UNREADABLE``): no ``frame``, no candidate; always
  possible.

R4 persists ``frame_ops`` and ``asked_ops`` in the pending itself, from the original typed inputs of the asking turn.
F24 reads the latter after reloading; no caller reconstructs it. ``pending_id`` derives from the asking turn: one pending
per asking turn, stable. Contextual replay validation uses the accredited base and its historical reference supplied by
the store/publication owner; a successful strict decode alone does not accredit that context.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from app.core.foundation_accepted import (
    DIMENSION_KEYS, AcceptedInterpretation, CandidatePatch, ConditionOrigin, OriginKind, PatchOp, PatchOpKind,
    PatchOriginKind, PendingClarification, SemanticPatch, Slot, TurnResolution, digest, reason_code_allowed,
)
from app.core.foundation_delta import (
    LOWER_KEY, SLOT_BY_KEY, UPPER_KEY, Composed, DeltaRefusal, compose_ops, covered_keys, dimension_key,
)
from app.core.foundation_delta_outcome import (
    DeltaDisposition, Dependency, FindingKind, PendingQuestion, frame_obstacle, law_violations,
)
from app.core.foundation_pending_answer import DEP_M1_PENDING_FRAME, RULE_OF_CODE, op_identity, ops_identity
from app.core.foundation_pending_cancel import EXPIRED_CONFIRM
from app.core.foundation_pending_clock import expires_at
from app.core.interpretation_contract import ContractError, Polarity
from app.core.semantic_dimensions import SemanticDimension

NO_ANCHOR_REASONS = frozenset({"NO_ANCHOR", "ANCHOR_UNREADABLE"})
# Rules whose firing is an authority's or the host's reading (annex A.3; §6 step 4 H5/H7/H9/H10; §6 table).
AUTHORITY_REASONS = frozenset({
    "BUYER_OR_PLACE_AFTER_ACTOR", "REGION_NOT_IN_LIST", "UNIT_AMBIGUOUS", "MONTH_YEAR_AMBIGUOUS",
    "VALUE_WITHOUT_COMPARATOR",
})
UNRESOLVED_PREFIX = "UNRESOLVED_VALUE:"
NO_CANDIDATE_REASONS = frozenset({"REGION_NOT_IN_LIST", "UNIT_AMBIGUOUS", "MONTH_YEAR_AMBIGUOUS",
                                  "VALUE_WITHOUT_COMPARATOR"})  # §8.4 table: «las demás, ninguno»
H5_SLOTS = frozenset({Slot(SemanticDimension.BUYER, Polarity.POSITIVE),
                      Slot(SemanticDimension.GEOGRAPHY, Polarity.POSITIVE)})
VALUE_SIDES = frozenset({SLOT_BY_KEY[LOWER_KEY], SLOT_BY_KEY[UPPER_KEY]})  # §8.4 (3), R3c: VALUE_WITHOUT_COMPARATOR
_DECIDED = (DeltaDisposition.CLARIFY, DeltaDisposition.INFORM, DeltaDisposition.DEPENDENCY)


@dataclass(frozen=True, slots=True)
class Creation:
    """What asking comes to: ``CLARIFY`` with its ``pending``, ``INFORM`` (§8.4 forbids the pending: ``violations`` says
    which condition fails) or ``DEPENDENCY`` (a declared §19 red). The pending is the sole source of causal operations."""

    disposition: DeltaDisposition
    code: str | None
    pending: PendingClarification | None = None
    dependency: Dependency | None = None
    violations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.disposition not in _DECIDED or (self.pending is not None) != (
                self.disposition is DeltaDisposition.CLARIFY):
            raise ContractError("INVALID_CREATION")
        if (self.dependency is not None) != (self.disposition is DeltaDisposition.DEPENDENCY):
            raise ContractError("INVALID_CREATION")


@dataclass(frozen=True, slots=True)
class AuthorityQuestion:
    """A fired rule whose reading is the authority's or the host's (M2A4-D6). ``origin`` is the ambiguous span (kind SPAN)
    or the event it comes from; ``open_slots`` the slots the unsettled span can occupy (what an open answer may fix);
    ``candidates`` the options the authority resolved at ask time, each a tuple of ``PatchOp``s."""

    reason: str
    origin: ConditionOrigin
    open_slots: frozenset[Slot]
    candidates: tuple[tuple[PatchOp, ...], ...] = ()

    def __post_init__(self) -> None:
        if type(self.origin) is not ConditionOrigin or self.origin.kind is OriginKind.HOST_POLICY:
            raise ContractError("INVALID_QUESTION_ORIGIN")
        if type(self.open_slots) is not frozenset or any(type(slot) is not Slot for slot in self.open_slots):
            raise ContractError("INVALID_QUESTION_SLOTS")
        if type(self.candidates) is not tuple or any(
                type(c) is not tuple or not c or any(type(op) is not PatchOp for op in c) for c in self.candidates):
            raise ContractError("INVALID_QUESTION_CANDIDATES")
        if type(self.reason) is not str or not (
                self.reason in AUTHORITY_REASONS or (
                    self.reason.startswith(UNRESOLVED_PREFIX) and self.reason[len(UNRESOLVED_PREFIX):] in DIMENSION_KEYS)):
            raise ContractError("QUESTION_REASON_NOT_AN_AUTHORITY_RULE")
        if self.reason in NO_CANDIDATE_REASONS and self.candidates:
            raise ContractError("QUESTION_WITH_UNEXPECTED_CANDIDATES")
        if self.reason == "BUYER_OR_PLACE_AFTER_ACTOR" and not (
                len(self.candidates) == 2 and all(len(c) == 1 and c[0].op is PatchOpKind.SET for c in self.candidates)
                and {c[0].slot for c in self.candidates} == H5_SLOTS):
            raise ContractError("INVALID_H5_CANDIDATES")  # F12: SET BUYER = X / SET GEOGRAPHY = X
        if self.reason.startswith(UNRESOLVED_PREFIX):
            dimension = self.reason[len(UNRESOLVED_PREFIX):]
            if any(dimension_key(op.slot) != dimension for c in self.candidates for op in c):
                raise ContractError("CANDIDATE_OUTSIDE_DIMENSION")

    @property
    def answer_slots(self) -> frozenset[Slot]:
        if self.reason == "VALUE_WITHOUT_COMPARATOR":  # §8.4 (3), R3c: both sides, whatever the caller says
            return VALUE_SIDES
        slots = set(self.open_slots)
        for candidate in self.candidates:
            for op in candidate:
                slots.add(op.slot)
                slots |= {SLOT_BY_KEY[key] for key in covered_keys((op,))}
        return frozenset(slots)


# ---- the pieces of a pending ----------------------------------------------------------------------------------------
def _patch_of(ops: tuple[PatchOp, ...]) -> SemanticPatch:
    """A candidate is one complete patch; its origin kind is the kind of its operations (a span, or an event)."""
    kind = PatchOriginKind.TEXT if all(op.origin.kind is OriginKind.SPAN for op in ops) else PatchOriginKind.EVENT
    return SemanticPatch(tuple(ops), kind)


def display_key(ops: tuple[PatchOp, ...]) -> str:
    """A stable key of a candidate for the presentation: its operations by slot and a digest of their identity (the
    values themselves are the presentation's to render from the typed value, §3.6)."""
    structure = "+".join(f"{op.op.value}:{op.slot.key}" for op in sorted(ops, key=op_identity))
    return f"{structure}#{digest(list(ops_identity(ops)))[:8]}"


def candidate_patches(candidates: tuple[tuple[PatchOp, ...], ...]) -> tuple[CandidatePatch, ...]:
    """Ids by position (``c1``, ``c2``…): the event names a candidate by id; the answer in language never does."""
    return tuple(CandidatePatch(f"c{i}", display_key(ops), _patch_of(ops)) for i, ops in enumerate(candidates, 1))


def hull_origin(ops: tuple[PatchOp, ...]) -> ConditionOrigin | None:
    """The one span a pending is born with (§8.2): the hull of the open operations' citations, when they cite the same
    text of the same turn; ``None`` when the question does not come from a span (an event, no operation)."""
    origins = [op.origin for op in ops]
    if not origins or any(o.kind is not OriginKind.SPAN for o in origins):
        return None
    if len({(o.trace_id, o.raw_sha256) for o in origins}) != 1:
        return None
    first = origins[0]
    return ConditionOrigin(OriginKind.SPAN, first.trace_id, first.raw_sha256,
                           (min(o.span[0] for o in origins), max(o.span[1] for o in origins)))


def _build(reason: str, frame: AcceptedInterpretation | None, base_trace_id: str | None, span_origin: ConditionOrigin | None,
           candidates: tuple[tuple[PatchOp, ...], ...], answer_slots: frozenset[Slot], asked_trace_id: str,
           now: datetime, *, frame_ops: tuple[PatchOp, ...], asked_ops: tuple[PatchOp, ...]) -> PendingClarification:
    span = raw = None
    if span_origin is not None and span_origin.kind is OriginKind.SPAN and reason != EXPIRED_CONFIRM:
        span, raw = span_origin.span, span_origin.raw_sha256  # CONTEXT_EXPIRED_CONFIRM is not born of a span (§8.1)
    return PendingClarification(f"pending-{asked_trace_id}", asked_trace_id, now, expires_at(now), 1, frame,
                                base_trace_id, span, raw, reason, candidate_patches(candidates), answer_slots,
                                frame_ops=frame_ops, asked_ops=asked_ops)


def _inform_code(reason: str) -> str:
    """The code of an ``INFORM`` that stands for a pending the law forbids: only the codes A.3 allows with ``INFORM``;
    ``CONTEXT_EXPIRED_CONFIRM`` and ``NO_ANCHOR`` never (their pending is always possible, §8.4), so the caller has
    handed over something M2A-3 would never have asked."""
    if not reason_code_allowed(reason, TurnResolution.INFORM):
        raise ContractError(f"PENDING_ALWAYS_POSSIBLE:{reason}")
    return reason


def _frame_of(base: AcceptedInterpretation, frame_ops: tuple[PatchOp, ...], trace: str) -> Composed | DeltaRefusal:
    """``base`` with the resolved operations of the asking turn composed (§8.2): the base itself when there are none."""
    return compose_ops(base, frame_ops, base_trace_id=trace) if frame_ops else Composed(base, frozenset(), frozenset(), False)


def validate_pending_base(pending: PendingClarification, *, base: AcceptedInterpretation | None,
                          base_trace_id: str | None) -> None:
    """C2 before consuming a reread pending. The caller accredits store/chat/publication and supplies A by its
    persisted reference; this module checks the binding and exact full replay, never searches for a substitute base."""
    if type(pending) is not PendingClarification:
        raise ContractError("INVALID_PENDING")
    if pending.base_trace_id is None:
        return  # C1 is enforced by the persisted type; no previous chat anchor becomes a fresh frame's base.
    if type(base) is not AcceptedInterpretation:
        raise ContractError("INVALID_BASE")
    if type(base_trace_id) is not str or not base_trace_id.strip():
        raise ContractError("INVALID_BASE_TRACE_ID")
    if base_trace_id != pending.base_trace_id:
        raise ContractError("PENDING_BASE_TRACE_MISMATCH")
    again = _frame_of(base, pending.frame_ops, base_trace_id)
    if not isinstance(again, Composed) or again.effective != pending.frame:
        raise ContractError("PENDING_FRAME_MISMATCH")


def _judge(base: AcceptedInterpretation, reason: str, frame_ops: tuple[PatchOp, ...],
           candidates, answer_slots, trace: str,
           origin: ConditionOrigin) -> tuple[AcceptedInterpretation | None, Creation | None]:
    """§8.4 over ``base`` + ``frame_ops``: ``(frame, None)`` when the pending is possible, else ``(None, the Creation that
    says why not)``. A frame that raises a §19 COMPOSICION dependency is that dependency (as H1 does, U29)."""
    composed = _frame_of(base, frame_ops, trace)
    if isinstance(composed, Composed):
        obstacle = frame_obstacle(base, frame_ops, composed)
        if obstacle is not None and obstacle.kind is FindingKind.DEPENDENCY:
            return None, Creation(DeltaDisposition.DEPENDENCY, None, dependency=obstacle.dependency)
    found = law_violations(base, frame_ops, candidates, answer_slots, trace, origin, RULE_OF_CODE.get(reason))
    if found:
        return None, Creation(DeltaDisposition.INFORM, _inform_code(reason), violations=tuple(found))
    return composed.effective, None


def _checked(base, base_trace_id, asked_trace_id, now, frame_ops=()):
    if type(base) is not AcceptedInterpretation:
        raise ContractError("INVALID_BASE")
    for name, value in (("BASE_TRACE_ID", base_trace_id), ("ASKED_TRACE_ID", asked_trace_id)):
        if type(value) is not str or not value.strip():
            raise ContractError(f"INVALID_{name}")
    expires_at(now)  # validates the injected instant
    frame_ops = tuple(frame_ops)
    if any(type(op) is not PatchOp for op in frame_ops):
        raise ContractError("INVALID_FRAME_OPS")
    return frame_ops


# ---- the entry points -----------------------------------------------------------------------------------------------
def pending_from_question(question: PendingQuestion, *, base: AcceptedInterpretation, base_trace_id: str,
                          asked_trace_id: str, now: datetime, origin: ConditionOrigin | None = None) -> Creation:
    """The pending of a ``PendingQuestion`` (M2A-3), judged again from ``base`` (the anchor, or what the question was asked
    over): its ``frame`` must be ``base`` + ``frame_ops`` composed, or it is not the question's (``ContractError``).
    ``origin`` is the citation of an open span that is not a ``PatchOp`` (§7.1 row 6, ``relative_outcome``)."""
    if type(question) is not PendingQuestion:
        raise ContractError("INVALID_PENDING_QUESTION")
    frame_ops = _checked(base, base_trace_id, asked_trace_id, now, question.frame_ops)
    open_ops = tuple(question.open_ops)
    asked_ops = (*frame_ops, *open_ops)
    again = _frame_of(base, frame_ops, base_trace_id)
    if not isinstance(again, Composed) or again.effective != question.frame:
        raise ContractError("QUESTION_FRAME_MISMATCH")
    cited = origin if origin is not None else next((op.origin for op in (*open_ops, *frame_ops)), None)
    if cited is None:
        raise ContractError("QUESTION_WITHOUT_ORIGIN")
    frame, refused = _judge(base, question.reason, frame_ops, question.candidates, question.answer_slots,
                            base_trace_id, cited)
    if refused is not None:
        return refused
    span_origin = hull_origin(open_ops) if open_ops else origin
    pending = _build(question.reason, frame, base_trace_id, span_origin, question.candidates, question.answer_slots,
                     asked_trace_id, now, frame_ops=frame_ops, asked_ops=asked_ops)
    return Creation(DeltaDisposition.CLARIFY, question.reason, pending)


def ask_authority(question: AuthorityQuestion, *, base: AcceptedInterpretation, base_trace_id: str,
                  asked_trace_id: str, now: datetime, frame_ops, open_ops, anchor_expired: bool = False,
                  pending_present: bool = False) -> Creation:
    """The pending of a rule an authority or the host fired over ``base`` (M2A4-D6). ``frame_ops`` are the operations of P
    that were resolved (they compose into the ``frame``), ``open_ops`` the typed form of what the authority left open, if
    it has one (H5's provisional label): both are the asking turn's patch (F24). On an expired anchor only H1's pending may
    exist, and with a pending vigente no other is born (§8.4): then ``INFORM`` with the rule's code."""
    if type(question) is not AuthorityQuestion:
        raise ContractError("INVALID_AUTHORITY_QUESTION")
    frame_ops = _checked(base, base_trace_id, asked_trace_id, now, frame_ops)
    open_ops = tuple(open_ops)
    if any(type(op) is not PatchOp for op in open_ops) or type(anchor_expired) is not bool or type(
            pending_present) is not bool:
        raise ContractError("INVALID_AUTHORITY_QUESTION_INPUT")
    asked_ops = (*frame_ops, *open_ops)
    if anchor_expired or pending_present:
        return Creation(DeltaDisposition.INFORM, _inform_code(question.reason))
    frame, refused = _judge(base, question.reason, frame_ops, question.candidates, question.answer_slots,
                            base_trace_id, question.origin)
    if refused is not None:
        return refused
    pending = _build(question.reason, frame, base_trace_id, question.origin, question.candidates,
                     question.answer_slots, asked_trace_id, now, frame_ops=frame_ops, asked_ops=asked_ops)
    return Creation(DeltaDisposition.CLARIFY, question.reason, pending)


def ask_fresh(question: AuthorityQuestion, *, frame: AcceptedInterpretation | None, asked_trace_id: str,
              now: datetime, asked_ops) -> Creation:
    """The pending of a new search that asks (§7.2 c): ``frame`` is what P resolved, with no base
    (``base_trace_id = None``). It comes from ``from_acceptance`` (M2A-1b); ``None``, or a frame M2a cannot judge, is the
    declared red ``DEP-M1-PENDING-FRAME``. The previous anchor stays until the pending is resolved or cancelled (§10.2).
    ``asked_ops`` are the typed operations of the asking turn, with NO default (M2A4-D28): they are persisted and F24
    reads them exclusively from the pending. An explicit empty tuple is the complete declared input, never an omission."""
    if type(question) is not AuthorityQuestion:
        raise ContractError("INVALID_AUTHORITY_QUESTION")
    asked_ops = tuple(asked_ops)
    if type(asked_trace_id) is not str or not asked_trace_id.strip() or any(type(op) is not PatchOp for op in asked_ops):
        raise ContractError("INVALID_ASKED_TRACE_ID")
    expires_at(now)
    if frame is None:
        return Creation(DeltaDisposition.DEPENDENCY, None, dependency=DEP_M1_PENDING_FRAME)
    if type(frame) is not AcceptedInterpretation or frame.base_trace_id is not None:
        raise ContractError("INVALID_FRESH_FRAME")  # a new search has no base
    # the same judgement as every other pending: a frame that raises a §19 dependency is that DEPENDENCY (U29), then §8.4
    _, refused = _judge(frame, question.reason, (), question.candidates, question.answer_slots,
                        asked_trace_id, question.origin)
    if refused is not None:
        return refused
    pending = _build(question.reason, frame, None, question.origin, question.candidates, question.answer_slots,
                     asked_trace_id, now, frame_ops=(), asked_ops=asked_ops)
    return Creation(DeltaDisposition.CLARIFY, question.reason, pending)


def pending_without_anchor(reason: str, *, asked_trace_id: str, now: datetime) -> Creation:
    """§7.1 row 1: ``NO_ANCHOR`` / ``ANCHOR_UNREADABLE``, no ``frame``, no candidate, no slot an answer may fix (the
    answer is the complete search, a new search by §8.3 row 3; a fragment is row 8). §8.4: always possible."""
    if reason not in NO_ANCHOR_REASONS:
        raise ContractError("NOT_A_NO_ANCHOR_REASON")
    if type(asked_trace_id) is not str or not asked_trace_id.strip():
        raise ContractError("INVALID_ASKED_TRACE_ID")
    pending = _build(reason, None, None, None, (), frozenset(), asked_trace_id, now, frame_ops=(), asked_ops=())
    return Creation(DeltaDisposition.CLARIFY, reason, pending)
