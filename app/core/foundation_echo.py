"""M2A-7: the pure ``echo_items`` and the coverage rule of contract §11 (INV-M2A-12, INV-M2A-21).

``echo_items(base, effective, patch, record, *, anchor_expired)`` turns what is ALREADY typed (the composition base, the
effective accepted interpretation, the patch, the turn's record, and the provenance and receipt rules each condition
carries) into the ``EchoItem``s the record stores. It never reads the turn's text, resolves nothing and decides no
semantics (INV-M2A-24): what the types do not say, it does not invent. The echo is the account of the execution, so it
is exactly what the typed difference and the provenance say, no more, no less (the stored echo is compared with
``echo_items`` over the same inputs: same inputs, same tuple).

Inputs. ``record`` is the turn's record (``AcceptedTurnRecord``; its own ``echo`` is the field this computes and is not
read); ``patch`` and ``effective`` are the record's own (a mismatch is refused). ``base`` is the interpretation the turn
composed over (the anchor) for a turn with a patch, and the anchor the turn found standing (``None`` without one) for a
new search. ``anchor_expired`` is the injected fact of §10 (as in ``delta_outcome``): the anchor the turn found is
expired, sticky expiry included; deciding it is M2A-6's. It has NO default: a caller that forgets it would revalidate an
expired anchor (§10.1), so omitting it is a ``TypeError``, never an echo.

The answer to a pending question has its own entry, ``echo_items_for_answer(anchor, frame, frame_ops, effective,
record, *, anchor_expired)``: the frame is what the asking turn composed and never executed, so the answer's echo is read
against the ANCHOR (§11: «delta o respuesta ejecutados con algo del ancla»; INV-M2A-12), once, in the turn that executes.
``echo_items`` refuses a ``PENDING_ANSWER`` (a frame-relative echo would call «carried» what the asking turn changed).
A pending WITHOUT an anchor is refused too, until HERA decides it: the sealed Gold lists ``CARRIED`` for the answers to
first-turn pendings (SF056, SF058, SF059, SF060, SF188, and SF191), a new search's echo does not (``M2A7_CLOSE.md``).

Which turn echoes what (§11, Gold schema 2.1 §7 «sí y sólo sí», plus the two additions of the contract):

* the execution echo exists only for a turn that executes an effective (``NEW_SEARCH``, ``DELTA``, ``PENDING_ANSWER``,
  ``PROTOCOL_EVENT``); a page (``PROTOCOL_PAGE``) is the same plan again and a ``CLARIFY`` / ``REASK`` / ``INFORM`` /
  ``HANDOFF_LEGACY`` executes nothing: they echo nothing but ``CONTEXT_EXPIRED``;
* ``CONTEXT_EXPIRED``: the turn found the anchor expired and neither confirms nor replaces it, whatever its scope or
  disposition (§10.1, R3c/R3d): ``anchor_expired`` and the turn executes no effective;
* ``CONTEXT_RESET``: a new search that executes over a standing (not expired) anchor, and only when no pending was
  vigente (Gold §6: the previous record is ``EXECUTED``/``EXECUTED_EMPTY``/``NOT_EXECUTED``; a new search that cancels a
  pending carries ``control = CANCEL_PENDING`` and echoes no reset: SF047-SF057, SF190);
* by DIFFERENCE (turns with a patch; ``foundation_echo_diff``): ``CARRIED`` (a delta or an answer only, never a button
  event: each condition that survives untouched, and the said universe when no ``REPLACE_UNIVERSE`` touches it),
  ``VALUE_RANGE_REPLACED`` (A8.1, or a ``CLEAR`` of the whole
  VALUE dimension followed by a ``SET`` that leaves a side unreplaced), ``TOPIC_REPLACED`` (F3/F22),
  ``POLARITY_INVERTED`` (F16), ``UNIVERSE_CHANGED`` (with the carried TIME, which date field it now applies to),
  ``TOPN_CARRIED`` (a carried CARDINALITY and a ``SET_ORDER``, F10);
* by PROVENANCE of the conditions this turn placed (its operations, or the frame's for an answer; a carried condition's
  provenance was announced by the turn that EXECUTED it): ``UNIT_INHERITED``, ``UNIT_SHARED_IN_RANGE`` (the whole range),
  ``YEAR_INHERITED``, ``YEAR_CLOCK_INFERRED``, ``RELATIVE_WINDOW_RESOLVED`` (a clock-relative period, or a year inherited
  by the receipt rule ``time.relative_to_previous``);
* by the executed plan, on EVERY condition of the effective (INV-M2A-21: a cap and a default hold at each execution):
  ``TOPN_CAPPED`` (receipt rule ``topn.capped``) and ``ORDER_DIRECTION_DEFAULTED`` (``order_direction_source =
  DEFAULTED`` or receipt rule ``order.direction_defaulted``). A receipt that contradicts the provenance
  (``order.direction_defaulted`` over a ``STATED`` direction) is refused, never echoed. The same check for
  ``topn.capped`` against ``n_requested <= n`` needs the decoded value: ``foundation_accepted`` (protected) has no
  decoding fact for a cardinality and C1.x forbids decoding it elsewhere, so it is declared, not done.

Not representable today (declared, never dispensed): ``VIEW_DERIVED`` / ``VIEW_REDERIVED``. ``AcceptedRequest.view`` only
admits ``None`` and a derived view needs ``subject = unspecified``, which the type refuses until wave F lands
(``HostViewDecision``; §19 ``DEP-DERIVED-VIEW``). Items are ordered by class (the order of §11), then by slot (§4.2).
"""

from __future__ import annotations

from app.core.foundation_accepted import (
    CONDITION_SLOT_ORDER, UNIVERSE_SLOT, AcceptedInterpretation, AcceptedTurnRecord, EchoClass, EchoItem,
    OrderDirectionSource, PatchOp, PendingControl, SemanticPatch, Slot, TurnResolution, UnitSource, YearSource,
    canonical_value,
)
from app.core.foundation_echo_diff import Departure, TurnDiff, answer_diff, composed_diff, fresh_diff
from app.core.interpretation_contract import ContractError, Polarity, ValueSide
from app.core.semantic_dimensions import SemanticDimension

# What each source of provenance announces. ``STATED`` is the one source of each kind that the user said himself and
# announces nothing; every other member is in its table, and the tests enumerate the enums so a new member forces a
# decision (a source that is in neither a table nor ``SILENT_SOURCES`` is an unannounced provenance).
UNIT_ECHO = {UnitSource.PREVIOUS_ACCEPTED_VALUE: EchoClass.UNIT_INHERITED,
             UnitSource.SIBLING_IN_RANGE: EchoClass.UNIT_SHARED_IN_RANGE}
YEAR_ECHO = {YearSource.PREVIOUS_ACCEPTED_TIME: EchoClass.YEAR_INHERITED,
             YearSource.CLOCK_INFERRED: EchoClass.YEAR_CLOCK_INFERRED,
             YearSource.RELATIVE_TO_CLOCK: EchoClass.RELATIVE_WINDOW_RESOLVED}
ORDER_ECHO = {OrderDirectionSource.DEFAULTED: EchoClass.ORDER_DIRECTION_DEFAULTED}
SILENT_SOURCES = frozenset({UnitSource.STATED, YearSource.STATED, OrderDirectionSource.STATED})
RULE_RELATIVE_TO_PREVIOUS = "time.relative_to_previous"  # A.7: the year inherited as a relative window (§16.3, D7)
RULE_TOPN_CAPPED = "topn.capped"  # A.7: P8, the cap that ran is smaller than the figure asked
RULE_ORDER_DEFAULTED = "order.direction_defaulted"  # A.7: F21 (the provenance field says it too)
# The classes no typed state can require today (module docstring): the tests pin the premise, wave F removes this.
NOT_REPRESENTABLE = frozenset({EchoClass.VIEW_DERIVED, EchoClass.VIEW_REDERIVED})

PATCHED = frozenset({TurnResolution.DELTA, TurnResolution.PROTOCOL_EVENT})  # composed over the anchor itself
ANSWERED = TurnResolution.PENDING_ANSWER  # composed over a frame: ``echo_items_for_answer``
EXECUTING = PATCHED | {ANSWERED, TurnResolution.NEW_SEARCH, TurnResolution.PROTOCOL_PAGE}
CLASS_RANK = {echo_class: rank for rank, echo_class in enumerate(EchoClass)}
_VALUE, _TOPIC = SemanticDimension.VALUE, SemanticDimension.TOPIC
_VALUE_WIDE = Slot(_VALUE)
_SLOT_RANK = {slot: rank for rank, slot in enumerate(CONDITION_SLOT_ORDER)}
_CARDINALITY = Slot(SemanticDimension.CARDINALITY)
_TOPIC_POSITIVE = Slot(_TOPIC, Polarity.POSITIVE)
_LOWER, _UPPER = Slot(_VALUE, side=ValueSide.LOWER), Slot(_VALUE, side=ValueSide.UPPER)


def _slot_rank(slot: Slot | None) -> int:
    if slot is None:
        return -1
    if slot.is_universe:
        return len(_SLOT_RANK) + 1
    return _SLOT_RANK.get(slot, _SLOT_RANK[_LOWER])  # only the whole-VALUE slot of UNIT_SHARED_IN_RANGE is not a rank


def _item(echo_class: EchoClass, slot: Slot | None, condition_ids) -> EchoItem:
    """The display is the presentation's, from the typed value the ids name; the key only selects what to show."""
    key = echo_class.value if slot is None else f"{echo_class.value}:{slot.key}"
    return EchoItem(echo_class, slot, tuple(condition_ids), key)


def _check_record(effective, patch, record, anchor_expired) -> None:
    if type(record) is not AcceptedTurnRecord:
        raise ContractError("ECHO_NEEDS_A_RECORD")
    if type(anchor_expired) is not bool:
        raise ContractError("ECHO_INVALID_ANCHOR_EXPIRED")
    if patch != record.patch or effective != record.effective:
        raise ContractError("ECHO_ARGUMENTS_OUTSIDE_THE_RECORD")  # the record is the authority on what it recorded
    asks_expiry = record.pending is not None and record.pending.reason == "CONTEXT_EXPIRED_CONFIRM"
    if asks_expiry and not anchor_expired:
        raise ContractError("ECHO_EXPIRED_PENDING_WITHOUT_EXPIRY")  # the question exists only over an expired anchor


def _executes(record: AcceptedTurnRecord, effective) -> bool:
    return record.resolution in EXECUTING and (
        effective is not None or record.resolution is TurnResolution.PROTOCOL_PAGE)


def _ordered(items: list[EchoItem]) -> tuple[EchoItem, ...]:
    return tuple(sorted(items, key=lambda item: (CLASS_RANK[item.echo_class], _slot_rank(item.slot))))


def echo_items(base: AcceptedInterpretation | None, effective: AcceptedInterpretation | None,
               patch: SemanticPatch | None, record: AcceptedTurnRecord, *,
               anchor_expired: bool) -> tuple[EchoItem, ...]:
    """Contract §11: the echo of one turn, ordered by class (the order of §11) and slot (§4.2); ``ContractError`` when
    the arguments are not one coherent turn. ``anchor_expired`` is keyword-only and has no default (module docstring)."""
    _check_record(effective, patch, record, anchor_expired)
    if base is not None and type(base) is not AcceptedInterpretation:
        raise ContractError("ECHO_INVALID_BASE")
    resolution = record.resolution
    if resolution is ANSWERED:
        raise ContractError("ECHO_ANSWER_HAS_ITS_OWN_ENTRY")  # echo_items_for_answer: against the anchor, not the frame
    if resolution in PATCHED and effective is not None:
        if base is None:
            raise ContractError("ECHO_COMPOSITION_WITHOUT_BASE")
        if effective.base_trace_id != record.base_trace_id:
            raise ContractError("ECHO_EFFECTIVE_OF_ANOTHER_BASE")
    executes = _executes(record, effective)
    items: list[EchoItem] = []
    if executes and resolution is not TurnResolution.PROTOCOL_PAGE:
        diff = composed_diff(base, effective, patch) if resolution in PATCHED else fresh_diff(effective)
        if (resolution is TurnResolution.NEW_SEARCH and base is not None and not anchor_expired
                and record.control is not PendingControl.CANCEL_PENDING):
            items.append(_item(EchoClass.CONTEXT_RESET, None, ()))
        items += _provenance_items(diff)
        items += _plan_items(effective)
        if resolution in PATCHED:
            items += _difference_items(effective, diff, carried=resolution is not TurnResolution.PROTOCOL_EVENT)
    if anchor_expired and not executes:
        items.append(_item(EchoClass.CONTEXT_EXPIRED, None, ()))
    return _ordered(items)


def echo_items_for_answer(anchor: AcceptedInterpretation | None, frame: AcceptedInterpretation | None,
                          frame_ops: tuple[PatchOp, ...], effective: AcceptedInterpretation | None,
                          record: AcceptedTurnRecord, *, anchor_expired: bool) -> tuple[EchoItem, ...]:
    """The echo of the answer to a pending question (``PENDING_ANSWER``), read against the ANCHOR (module docstring):
    ``frame`` is the pending's frame, ``frame_ops`` the operations the asking turn left in it (``PendingQuestion.
    frame_ops`` of ``delta_outcome``; empty when the frame is the anchor), ``record.patch`` the answer (the chosen
    candidate or the answer's operations) and ``effective`` what composing it on the frame gave. Everything between the
    anchor and what ran is announced once, here, whichever of the two turns did it."""
    if type(record) is not AcceptedTurnRecord:
        raise ContractError("ECHO_NEEDS_A_RECORD")
    _check_record(effective, record.patch, record, anchor_expired)
    if record.resolution is not ANSWERED:
        raise ContractError("ECHO_NOT_AN_ANSWER")
    if anchor is None:
        raise ContractError("ECHO_ANSWER_WITHOUT_ANCHOR")  # undecided: M2A7_CLOSE.md (F-6)
    if type(anchor) is not AcceptedInterpretation:
        raise ContractError("ECHO_INVALID_BASE")
    if effective is None:
        return _ordered([_item(EchoClass.CONTEXT_EXPIRED, None, ())] if anchor_expired else [])
    if effective.base_trace_id != record.base_trace_id:
        raise ContractError("ECHO_EFFECTIVE_OF_ANOTHER_BASE")
    diff = answer_diff(anchor, frame, tuple(frame_ops), effective, record.patch)
    return _ordered([*_provenance_items(diff), *_plan_items(effective), *_difference_items(effective, diff, carried=True)])


# ---- by the provenance of what the turn placed ----------------------------------------------------------------------
def _provenance_items(diff: TurnDiff) -> list[EchoItem]:
    items: list[EchoItem] = []
    for condition in diff.placed:
        provenance = condition.provenance
        echoed = [UNIT_ECHO.get(provenance.unit_source), YEAR_ECHO.get(provenance.year_source)]
        if (provenance.year_source is YearSource.PREVIOUS_ACCEPTED_TIME
                and RULE_RELATIVE_TO_PREVIOUS in condition.receipt_rules):
            echoed.append(EchoClass.RELATIVE_WINDOW_RESOLVED)
        items += [_item(echo_class, condition.slot, (condition.condition_id,)) for echo_class in echoed
                  if echo_class is not None and echo_class is not EchoClass.UNIT_SHARED_IN_RANGE]
    shared = [c for c in diff.placed if UNIT_ECHO.get(c.provenance.unit_source) is EchoClass.UNIT_SHARED_IN_RANGE]
    if shared:  # the unit said once covers the range: the echo shows both bounds with it
        items.append(_item(EchoClass.UNIT_SHARED_IN_RANGE, _VALUE_WIDE,
                           [c.condition_id for c in diff.placed if c.dimension is _VALUE]))
    return items


# ---- by the executed plan: every condition of the effective ---------------------------------------------------------
def _plan_items(effective: AcceptedInterpretation) -> list[EchoItem]:
    items: list[EchoItem] = []
    for condition in effective.conditions:
        if condition.dimension is SemanticDimension.CARDINALITY and RULE_TOPN_CAPPED in condition.receipt_rules:
            items.append(_item(EchoClass.TOPN_CAPPED, condition.slot, (condition.condition_id,)))
        defaulted = (ORDER_ECHO.get(condition.provenance.order_direction_source) is not None
                     or RULE_ORDER_DEFAULTED in condition.receipt_rules)
        if (condition.dimension is SemanticDimension.ORDER and RULE_ORDER_DEFAULTED in condition.receipt_rules
                and condition.provenance.order_direction_source is OrderDirectionSource.STATED):
            raise ContractError("ECHO_RECEIPT_CONTRADICTS_PROVENANCE")  # a default the user's own words contradict
        if condition.dimension is SemanticDimension.ORDER and defaulted:
            items.append(_item(EchoClass.ORDER_DIRECTION_DEFAULTED, condition.slot, (condition.condition_id,)))
    return items


# ---- by the difference between the base and the effective -----------------------------------------------------------
def _difference_items(effective: AcceptedInterpretation, diff: TurnDiff, *, carried: bool) -> list[EchoItem]:
    """``carried``: the turn is a delta or an answer (§11 CARRIED, Gold §7); a button event (order, remove a filter) is
    neither, and the Gold gives it no CARRIED line (SF149-SF152), though its effects below still apply."""
    items = []
    if carried:
        items += [_item(EchoClass.CARRIED, condition.slot, (condition.condition_id,)) for condition in diff.carried]
        if not diff.universe_replaced:
            items.append(_item(EchoClass.CARRIED, UNIVERSE_SLOT, ()))  # the said universe survives untouched
    items += _value_range_items(effective, diff)
    items += _topic_items(diff)
    items += _polarity_items(diff)
    if diff.universe_changed:
        carried_time = [c.condition_id for c in diff.carried if c.dimension is SemanticDimension.TIME]
        items.append(_item(EchoClass.UNIVERSE_CHANGED, UNIVERSE_SLOT, carried_time))
    if diff.set_order:
        items += [_item(EchoClass.TOPN_CARRIED, condition.slot, (condition.condition_id,))
                  for condition in diff.carried if condition.slot == _CARDINALITY]
    return items


def _value_range_items(effective: AcceptedInterpretation, diff: TurnDiff) -> list[EchoItem]:
    """A VALUE side the base had and the effective lacks, replaced by A8.1 or by a whole-VALUE ``CLEAR`` followed by a
    ``SET`` (Gold §6 (b): ``CLEAR(VALUE)`` alone, or a ``CLEAR`` of the one side, is no replacement)."""
    standing = {c.slot for c in effective.conditions if c.dimension is _VALUE}
    placed = [c.condition_id for c in diff.placed if c.dimension is _VALUE]
    items = []
    for side in (_LOWER, _UPPER):
        gone = [r for r in diff.removed if r.condition.slot == side]
        replaced = any(r.departure is Departure.A81_OPPOSITE_BOUND for r in gone) or (
            diff.wide_value_clear and bool(placed) and bool(gone))
        if gone and side not in standing and replaced:
            items.append(_item(EchoClass.VALUE_RANGE_REPLACED, side, [r.condition.condition_id for r in gone] + placed))
    return items


def _topic_items(diff: TurnDiff) -> list[EchoItem]:
    """F3/F22: a positive TOPIC the patch put leaves a different positive TOPIC of the base without its value."""
    placed = [c for c in diff.placed if c.slot == _TOPIC_POSITIVE]
    gone = [r.condition for r in diff.removed if r.condition.slot == _TOPIC_POSITIVE]
    if not placed or not any(old.value not in {new.value for new in placed} for old in gone):
        return []
    return [_item(EchoClass.TOPIC_REPLACED, _TOPIC_POSITIVE, [old.condition_id for old in gone]
                  + [new.condition_id for new in placed])]


def _polarity_items(diff: TurnDiff) -> list[EchoItem]:
    """F16: the positive that left and the negative that took its place, per inverted value."""
    items = []
    for removal in diff.removed:
        if removal.departure is not Departure.F16_POSITIVE_INVERTED:
            continue
        old = removal.condition
        negative = Slot(old.slot.dimension, Polarity.NEGATIVE)
        taken = [c.condition_id for c in diff.placed
                 if c.slot == negative and canonical_value(c) == canonical_value(old)]
        items.append(_item(EchoClass.POLARITY_INVERTED, negative, [old.condition_id] + taken))
    return items

