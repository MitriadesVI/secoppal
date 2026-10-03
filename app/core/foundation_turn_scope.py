"""M2A-3: the scope of a turn (contract §7.1–§7.2, §8 rows 2–3 for the scope only; annex A.1/A.3).

``classify_turn(facts) -> TurnClassification``: exactly one ``TurnScope`` of A.1 (INV-M2A-20), from CLOSED structural
facts of P (the turn's own proposal and its wire 5.1 directives), the anchor and the pending (design R8,
``M2A3_PHASE1_CLOSE.md`` §4). It never reads text (INV-M2A-24) and adds no ``TurnScope``.

* Rows 1–6 of §7.1 in their order, then §7.2 (a)–(d) with an anchor and no pending, or §8 (A7) with a pending: only
  whether the turn is a NEW_SEARCH (rows 2–3) or stays ``IN`` for §6 / M2A-4 (rows 4–8).
* The extensional/intensional signal of a referent is tri-state and comes from M2A-5 (G-FU-14): ``ABSENT`` is never
  read as intensional (§7.2) — a turn with referent evidence and no signal is refused (``REFERENT_SIGNAL_ABSENT``), as
  M2A-6 never activates without the signal.
* A fact the rule needs and the caller did not determine (``None``) is refused (``UNDETERMINED_FACT:<name>``): the rule
  is lazy (a fact it does not read may stay ``None``), and it never defaults one.
* «Nuevo ≠ ∅ ∧ Marco ∧ ¬Admisible sola» has no row of §7.2 (only an ``unspecified`` universe without Product View
  makes P non-admissible: wave F), so §7.2 does not say NEW_SEARCH and §7.1 row 8 («lo demás: delta») closes it:
  ``IN`` / ``DELTA`` (Q-R8-3; INV-M2A-20: every turn gets exactly one scope).

``search_effects`` gives the effects of a NEW_SEARCH over a vigente anchor: CONTEXT_RESET (§7.2, §11) and, when its
positive TOPIC replaces a different one of the anchor, TOPIC_REPLACED (§6.5 F3/F22, as the Gold reads it).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from itertools import product

from app.core.foundation_accepted import (
    UNSUPPORTED_PROPOSAL_MOTIVES, AcceptedCondition, AcceptedInterpretation, PendingControl, TurnResolution,
    TurnScope,
)
from app.core.foundation_operations import Operation
from app.core.interpretation_contract import ContractError, Polarity
from app.core.semantic_dimensions import SemanticDimension


class InputKind(str, Enum):
    TEXT = "TEXT"
    EVENT = "EVENT"


class AnchorState(str, Enum):
    NONE = "NONE"
    UNREADABLE = "UNREADABLE"
    LEGACY = "LEGACY"
    FOUNDATION_SEARCH = "FOUNDATION_SEARCH"


class ReferentSignal(str, Enum):
    """M2A-5's structural signal (G-FU-14). ``NONE``: P has no referent evidence. ``ABSENT``: it has, without signal."""

    NONE = "NONE"
    EXTENSIONAL = "EXTENSIONAL"
    INTENSIONAL = "INTENSIONAL"
    ABSENT = "ABSENT"


@dataclass(frozen=True, slots=True)
class TurnFacts:
    """Closed facts, derivable from the types of ``main`` (R8). ``None`` = not determined by the caller."""

    input_kind: InputKind
    anchor: AnchorState
    anchor_expired: bool
    pending: bool
    legacy_operation: Operation | None = None  # LEGACY anchor: the legacy owner's operation (ANCHOR_NOT_SEARCH:<op>)
    foreign_operation: Operation | None = None  # a job whose operation is neither SEARCH nor FOLLOWUP_DELTA
    unsupported_motive: str | None = None  # §7.1 row 4b, the closed list
    referent: ReferentSignal = ReferentSignal.NONE
    relative_non_temporal: bool = False  # an own UNSUPPORTED condition labelled relative_to_previous (A.6, F15)
    continuation: bool | None = None
    new_content: bool | None = None  # Nuevo(P, A) ≠ ∅
    frame: bool | None = None  # Marco(P)
    admissible_alone: bool | None = None
    own_conditions: int | None = None
    cancel_directive: bool = False
    within_answer_slots: bool | None = None  # every condition of P falls in the pending's answer_slots (§8.3 row 3)

    def __post_init__(self) -> None:
        for name, kind in (("input_kind", InputKind), ("anchor", AnchorState), ("referent", ReferentSignal)):
            if type(getattr(self, name)) is not kind:
                raise ContractError(f"INVALID_FACT:{name}")
        for name in ("anchor_expired", "pending", "relative_non_temporal", "cancel_directive"):
            if type(getattr(self, name)) is not bool:
                raise ContractError(f"INVALID_FACT:{name}")
        for name in ("continuation", "new_content", "frame", "admissible_alone", "within_answer_slots"):
            if getattr(self, name) is not None and type(getattr(self, name)) is not bool:
                raise ContractError(f"INVALID_FACT:{name}")
        if self.own_conditions is not None and (type(self.own_conditions) is not int or self.own_conditions < 0):
            raise ContractError("INVALID_FACT:own_conditions")
        for name in ("legacy_operation", "foreign_operation"):
            if getattr(self, name) is not None and type(getattr(self, name)) is not Operation:
                raise ContractError(f"INVALID_FACT:{name}")
        if self.foreign_operation in (Operation.SEARCH, Operation.FOLLOWUP_DELTA):
            raise ContractError("INVALID_FACT:foreign_operation")
        if (self.anchor is AnchorState.LEGACY) != (self.legacy_operation is not None):
            raise ContractError("INVALID_FACT:legacy_operation")
        if self.unsupported_motive is not None and self.unsupported_motive not in UNSUPPORTED_PROPOSAL_MOTIVES:
            raise ContractError("INVALID_FACT:unsupported_motive")


@dataclass(frozen=True, slots=True)
class TurnClassification:
    scope: TurnScope
    resolution: TurnResolution | None  # None: decided later (§6 for IN, M2A-4 for the pending rows, M2A-6 for events)
    code: str | None = None
    control: PendingControl | None = None
    context_reset: bool = False


def _need(facts: TurnFacts, name: str):
    value = getattr(facts, name)
    if value is None:
        raise ContractError(f"UNDETERMINED_FACT:{name}")
    return value


def _new_search_allowed(facts: TurnFacts) -> bool:
    """¬Continuación(P) ∧ Marco(P) ∧ Admisible sola(P), with the host's floor (D7: an own condition)."""
    if _need(facts, "continuation") or _need(facts, "own_conditions") == 0:
        return False
    return _need(facts, "frame") and _need(facts, "admissible_alone")


def _new_search(facts: TurnFacts, control: PendingControl | None) -> TurnClassification:
    reset = facts.anchor is AnchorState.FOUNDATION_SEARCH and not facts.anchor_expired  # «con ancla vigente» (§7.2)
    return TurnClassification(TurnScope.IN_NEW_SEARCH, TurnResolution.NEW_SEARCH, control=control, context_reset=reset)


_OPEN_FACTS = {"continuation": (True, False), "new_content": (True, False), "frame": (True, False),
               "admissible_alone": (True, False), "within_answer_slots": (True, False), "own_conditions": (0, 1, 2)}


def classify_turn(facts: TurnFacts) -> TurnClassification:
    """Contract §7.1 (the first row that applies) and §7.2 / §8 for the rest. A fact left ``None`` is read three-valued:
    the classification is returned only if it is the same for every value of the undetermined facts, else
    ``UNDETERMINED_FACT:<names>``; nothing is ever defaulted."""
    if type(facts) is not TurnFacts:
        raise ContractError("INVALID_TURN_FACTS")
    open_names = [name for name in _OPEN_FACTS if getattr(facts, name) is None]
    if not open_names:
        return _classify(facts)
    outcomes = set()
    for values in product(*(_OPEN_FACTS[name] for name in open_names)):
        try:
            outcomes.add(_classify(replace(facts, **dict(zip(open_names, values)))))
        except ContractError as error:
            outcomes.add(str(error))
    if len(outcomes) == 1:  # the same answer, or the same refusal, for every value: determined
        only = next(iter(outcomes))
        if type(only) is TurnClassification:
            return only
        raise ContractError(only)
    raise ContractError(f"UNDETERMINED_FACT:{','.join(open_names)}")


def _classify(facts: TurnFacts) -> TurnClassification:
    anchored = facts.anchor is AnchorState.FOUNDATION_SEARCH
    if not anchored and facts.anchor is not AnchorState.LEGACY and not facts.pending:  # row 1 / first turn
        # an event forms no SEARCH (§7.1 row 1 precedes row 3; an option of a pending that no longer exists is
        # §8.3's STALE_OPTION, M2A-4)
        if facts.input_kind is InputKind.TEXT and _need(facts, "admissible_alone"):
            return _new_search(facts, None)  # first turn: the disposition is M1's (INV-M2A-06)
        code = "ANCHOR_UNREADABLE" if facts.anchor is AnchorState.UNREADABLE else "NO_ANCHOR"
        return TurnClassification(TurnScope.OUT_NO_ANCHOR, TurnResolution.CLARIFY, code)
    if facts.anchor is AnchorState.LEGACY:  # row 2
        return TurnClassification(TurnScope.OUT_ANCHOR_NOT_SEARCH, TurnResolution.HANDOFF_LEGACY,
                                  f"ANCHOR_NOT_SEARCH:{facts.legacy_operation.value}")
    if facts.input_kind is InputKind.EVENT:  # row 3: page, view/order/filter, option: M2A-6 / M2A-4
        return TurnClassification(TurnScope.IN_PROTOCOL, None)
    if facts.foreign_operation is not None:  # row 4
        return TurnClassification(TurnScope.OUT_OPERATION_CHANGE, TurnResolution.INFORM,
                                  f"OPERATION_CHANGE:{facts.foreign_operation.value}")
    if facts.unsupported_motive is not None and not (facts.pending and facts.unsupported_motive == "NO_ACTION"):
        return TurnClassification(TurnScope.OUT_UNSUPPORTED_PROPOSAL, TurnResolution.INFORM,  # row 4b (D-6)
                                  f"UNSUPPORTED_PROPOSAL:{facts.unsupported_motive}")
    if facts.referent is ReferentSignal.ABSENT:
        raise ContractError("REFERENT_SIGNAL_ABSENT")  # G-FU-14: never intensional by default (§7.2)
    if facts.referent is ReferentSignal.EXTENSIONAL:  # row 5
        return TurnClassification(TurnScope.OUT_REFERENT_EXTENSIONAL, TurnResolution.INFORM, "REFERENT_EXTENSIONAL")
    if facts.relative_non_temporal:  # row 6: CLARIFY, or INFORM when §8.4 forbids the pending (relative_outcome)
        return TurnClassification(TurnScope.OUT_RELATIVE_TO_PREVIOUS, TurnResolution.CLARIFY,
                                  "RELATIVE_TO_PREVIOUS_VALUE")
    if facts.pending:  # §8 (A7): A9.1 does not apply to the frame; a continuation never starts a search
        if facts.cancel_directive:  # row 2
            control = PendingControl.CANCEL_PENDING
            if _new_search_allowed(facts):
                return _new_search(facts, control)
            return TurnClassification(TurnScope.IN, TurnResolution.DELTA, control=control)
        if _new_search_allowed(facts) and not _need(facts, "within_answer_slots"):  # row 3
            return _new_search(facts, PendingControl.CANCEL_PENDING)
        return TurnClassification(TurnScope.IN, None)  # rows 4–8: M2A-4
    if _need(facts, "continuation"):  # §7.2 (a): the mark wins
        return TurnClassification(TurnScope.IN, TurnResolution.DELTA)
    if not _need(facts, "new_content"):  # (b) A9.1
        return TurnClassification(TurnScope.IN, TurnResolution.DELTA)
    if not _need(facts, "frame"):  # (d)
        return TurnClassification(TurnScope.IN, TurnResolution.DELTA)
    if _need(facts, "own_conditions") == 0:  # D7's floor: never a search without an own condition
        return TurnClassification(TurnScope.IN, TurnResolution.DELTA)
    if _need(facts, "admissible_alone"):  # (c)
        return _new_search(facts, None)
    return TurnClassification(TurnScope.IN, TurnResolution.DELTA)  # no row of §7.2 says NEW_SEARCH: §7.1 row 8


def search_effects(anchor: AcceptedInterpretation | None, conditions: tuple[AcceptedCondition, ...], *,
                   anchor_expired: bool, pending_cancelled: bool) -> frozenset[str]:
    """Effects of a NEW_SEARCH (A.4): CONTEXT_RESET over a vigente anchor (§7.2, §11), PENDING_CANCELLED with
    ``control = CANCEL_PENDING`` (§17.5), TOPIC_REPLACED when its positive TOPIC replaces a different one (§6.5)."""
    out = set()
    if anchor is not None and not anchor_expired:
        out.add("CONTEXT_RESET")
    if pending_cancelled:
        out.add("PENDING_CANCELLED")
    if anchor is not None:
        mine = [c.value for c in conditions
                if c.dimension is SemanticDimension.TOPIC and c.polarity is Polarity.POSITIVE]
        if mine and any(c.dimension is SemanticDimension.TOPIC and c.polarity is Polarity.POSITIVE
                        and c.value not in mine for c in anchor.conditions):
            out.add("TOPIC_REPLACED")
    return frozenset(out)
