"""M2A-3: what a delta turn answers (contract §6 steps 3b–4, §8.4, §10.1 H1 as an injected fact, §19).

``delta_outcome(base, patch, *, base_trace_id, anchor_expired) -> DeltaOutcome`` follows the contract in this order:

1. the causes of §6 step 3b (``foundation_delta.refusal_causes``): two or more, or an expired anchor ⇒ ``INFORM`` with
   the top code; one ⇒ ``CLARIFY`` with its A.3 code and the data of its pending, only if §8.4 allows that pending,
   else ``INFORM``;
2. the host rules of §6 step 4 that are pure over typed values, in their order: H1 (``anchor_expired`` is injected:
   §10 is M2A-6's), H3 (SMMLV over the sealed catalogue 2024–2026), H6 (the carried buyer's ``buyer_scope``), H8 (STATE
   literals of the resulting dataset), H11, H12; the first that fires decides;
3. otherwise ``EXECUTE`` with the composed effective and its §6.5 effects.

A rule that needs another lane fails closed as a declared dependency of §19 (``DEPENDENCY``, never a guess):
DEP-SMMLV-RECALC (a carried SMMLV bound whose year changes), DEP-MODALITY-UNIVERSE (a MODALITY after a change of
universe). H2 (pending, §8.3) is M2A-4's: this function answers a turn with no pending. H4 is §7.1 row 6
(``relative_outcome``). H5, H7, H9 and H10 are readings of an authority (a question raised when it resolves, F12/F13/
F5/F7): a resolved value in a ``m2a-patch/1`` already passed them, so ``compose`` never raises them.

The pending is returned as DATA (``PendingQuestion``: frame, open operations, candidates, answer slots); building the
``PendingClarification`` and its protocol is M2A-4's.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from app.core.foundation_accepted import (
    UNIVERSE_SLOT, AcceptedInterpretation, ConditionOrigin, PatchOp, PatchOpKind, SemanticPatch, Slot,
    UniverseTarget, geo_divipola, state_datasets, time_years, unit_class,
)
from app.core.foundation_delta import (
    CARDINALITY_KEY, ORDER_KEY, SLOT_BY_KEY, SLOT_RANK, Cause, Composed, DeltaEffect, DeltaRefusal, Refusal,
    compose_ops, grouped_keys, covered, covered_keys, refusal_causes,
)
from app.core.interpretation_contract import ContractError, Polarity
from app.core.semantic_dimensions import SemanticDimension

# §15.5 / ADR-103 D10: the closed SMMLV catalogue (``app/data/smmlv.py``; pinned by the tests, M2a never imports it).
SMMLV_CATALOGUE_YEARS = frozenset({2024, 2025, 2026})
HOST_RULES = ("H1", "H2", "H3", "H4", "H5", "H6", "H7", "H8", "H9", "H10", "H11", "H12")  # §6 step 4, in order
HOST_CODES = {"H1": "CONTEXT_EXPIRED_CONFIRM", "H3": "SMMLV_YEAR_AMBIGUOUS", "H4": "RELATIVE_TO_PREVIOUS_VALUE",
              "H6": "TERRITORY_VS_CARRIED_BUYER", "H8": "STATE_NOT_IN_UNIVERSE", "H11": "EMPTY_SEARCH_AFTER_DELTA",
              "H12": "CLEAR_ON_EMPTY_SLOT"}
GEO_POSITIVE = Slot(SemanticDimension.GEOGRAPHY, Polarity.POSITIVE)
BUYER_POSITIVE = Slot(SemanticDimension.BUYER, Polarity.POSITIVE)
TIME_POSITIVE = Slot(SemanticDimension.TIME, Polarity.POSITIVE)


class DeltaDisposition(str, Enum):
    EXECUTE = "EXECUTE"
    CLARIFY = "CLARIFY"
    INFORM = "INFORM"
    DEPENDENCY = "DEPENDENCY"  # a declared red of §19: fail closed, never executed (§20.2)


@dataclass(frozen=True, slots=True)
class Dependency:
    """A §19 row with ``impacto_m2a3 = COMPOSICION`` that decides the turn, with its owners."""

    dep_id: str
    owners: tuple[str, ...]


DEP_SMMLV_RECALC = Dependency("DEP-SMMLV-RECALC", ("D-MONEY", "E-VALUE"))
DEP_MODALITY_UNIVERSE = Dependency("DEP-MODALITY-UNIVERSE", ("E-MODALITY",))


class FindingKind(str, Enum):
    QUESTION = "QUESTION"
    INFORM = "INFORM"
    DEPENDENCY = "DEPENDENCY"


@dataclass(frozen=True, slots=True)
class Finding:
    rule: str
    kind: FindingKind
    slot_rank: int
    open_ops: tuple[PatchOp, ...] = ()
    candidates: tuple[tuple[PatchOp, ...], ...] = ()
    dependency: Dependency | None = None

    @property
    def order(self) -> tuple[int, int]:
        return HOST_RULES.index(self.rule), self.slot_rank

    @property
    def code(self) -> str:
        return HOST_CODES[self.rule]


@dataclass(frozen=True, slots=True)
class PendingQuestion:
    """The data of the pending §8.1 needs (M2A-4 builds the ``PendingClarification``): ``frame`` is the base with the
    resolved part of the turn composed (§8.2), ``open_ops`` the part left open, ``candidates`` the options resolved
    at ask time, ``answer_slots`` ⊇ the candidates' slots."""

    reason: str
    frame: AcceptedInterpretation
    frame_ops: tuple[PatchOp, ...]
    open_ops: tuple[PatchOp, ...]
    candidates: tuple[tuple[PatchOp, ...], ...]
    answer_slots: frozenset[Slot]


@dataclass(frozen=True, slots=True)
class DeltaOutcome:
    disposition: DeltaDisposition
    code: str | None
    effective: AcceptedInterpretation | None = None
    effects: frozenset[DeltaEffect] = frozenset()
    pending: PendingQuestion | None = None
    dependency: Dependency | None = None
    causes: tuple[Cause, ...] = ()


# ---- host rules (§6 step 4) ----------------------------------------------------------------------------------------
def _smmlv(conditions) -> list:
    return [c for c in conditions if c.dimension is SemanticDimension.VALUE and unit_class(c) == "SMMLV"]


def _positive_times(conditions) -> list:
    return [c for c in conditions if c.dimension is SemanticDimension.TIME and c.polarity is Polarity.POSITIVE]


def _smmlv_status(base: AcceptedInterpretation, conditions) -> str:
    """§15.5: ``ok``, ``ambiguous`` (H3: no single catalogued year) or ``recalc`` (a SMMLV bound whose year changes:
    E-VALUE recalculates it from ``source_amount``, DEP-SMMLV-RECALC)."""
    smmlv, times = _smmlv(conditions), _positive_times(conditions)
    if not smmlv:
        return "ok"
    if len(times) > 1:
        return "ambiguous"
    if times:
        years = time_years(times[0])
        if years is None or years[0] != years[1] or years[0] not in SMMLV_CATALOGUE_YEARS:
            return "ambiguous"
        return "recalc" if any(c.provenance.year != years[0] for c in smmlv) else "ok"
    return "recalc" if _positive_times(base.conditions) else "ok"  # the year falls back to the clock (year.inferred)


def _territorial_scopes(conditions, placed_ids) -> set[str]:
    return {c.provenance.buyer_scope for c in conditions
            if c.slot == BUYER_POSITIVE and c.condition_id not in placed_ids
            and c.provenance.buyer_scope not in (None, "NATIONAL")}


def host_findings(base: AcceptedInterpretation, ops: tuple[PatchOp, ...], composed: Composed) -> tuple[Finding, ...]:
    """§6 step 4, H3–H12 as far as they are pure over typed values, plus the §19 COMPOSICION rows they reach."""
    effective, out = composed.effective, []
    status = _smmlv_status(base, effective.conditions)
    if status != "ok":  # H3: what opens it is the turn's TIME or VALUE operation
        opened = tuple(op for op in ops if op.op is not PatchOpKind.KEEP  # a KEEP changes nothing
                       and op.slot.dimension in (SemanticDimension.TIME, SemanticDimension.VALUE))
        rank = min((SLOT_RANK[s.key] for op in opened for s in covered(op.slot)), default=0)
        out.append(Finding("H3", FindingKind.QUESTION, rank, opened) if status == "ambiguous"
                   else Finding("H3", FindingKind.DEPENDENCY, rank, dependency=DEP_SMMLV_RECALC))
    for op in ops:  # H6 (F30): a new territory next to a carried territorial buyer of ANOTHER territory
        if op.op is PatchOpKind.SET and op.slot == GEO_POSITIVE:
            scopes = _territorial_scopes(effective.conditions, composed.placed_ids)
            if any(scope != geo_divipola(op.condition) for scope in scopes):
                change_buyer = (PatchOp(PatchOpKind.CLEAR, BUYER_POSITIVE, None, None, op.origin), op)
                out.append(Finding("H6", FindingKind.QUESTION, SLOT_RANK[GEO_POSITIVE.key], (op,),
                                   (change_buyer, (op,))))
    if composed.universe_changed:  # H8 (F27, §14): STATE and MODALITY over the resulting dataset
        redirect = next(op for op in ops if op.op is PatchOpKind.REPLACE_UNIVERSE)
        dataset = effective.requests[0].dataset
        states = [c for c in effective.conditions if c.dimension is SemanticDimension.STATE
                  and dataset not in state_datasets(c)]
        if states:
            out.append(Finding("H8", FindingKind.QUESTION, SLOT_RANK[states[0].slot.key], (redirect,)))
        modalities = [c for c in effective.conditions if c.dimension is SemanticDimension.MODALITY]
        if modalities:  # modality@1 carries no dataset: E-MODALITY's catalogue decides (COMPOSICION)
            out.append(Finding("H8", FindingKind.DEPENDENCY, SLOT_RANK[modalities[0].slot.key],
                               dependency=DEP_MODALITY_UNIVERSE))
    if not effective.conditions:  # H11 (F19): the whole turn is the open span; frame = the base
        out.append(Finding("H11", FindingKind.QUESTION, 0, tuple(ops)))
    present = {c.slot.key for c in base.conditions}
    if len(ops) == 1 and ops[0].op is PatchOpKind.CLEAR and not covered_keys(ops) & present:  # H12 (F18)
        out.append(Finding("H12", FindingKind.INFORM, 0, tuple(ops)))
    return tuple(sorted(out, key=lambda f: f.order))


# ---- §8.4: the law of the possible pending -------------------------------------------------------------------------
def _without(ops, removed) -> tuple[PatchOp, ...]:
    left, out = list(removed), []
    for op in ops:
        if op in left:
            left.remove(op)
        else:
            out.append(op)
    return tuple(out)


def _compose(base, ops, trace) -> Composed | DeltaRefusal:
    return compose_ops(base, ops, base_trace_id=trace) if ops else Composed(base, frozenset(), frozenset(), False)


def _questions(base, ops, composed: Composed) -> tuple[Finding, ...]:
    """What cannot sit in a frame (§8.2, §8.4): a host question, whether an operation opens it or the composed state
    itself raises it (a state the frame cannot resolve). H11 (the whole turn) is the one question a frame may hold."""
    return tuple(f for f in host_findings(base, ops, composed) if f.kind is FindingKind.QUESTION and f.rule != "H11")


def executes(frame: AcceptedInterpretation, ops, trace: str) -> bool:
    """The answer composes on the frame with no cause and no further question (a §19 dependency is a declared red, not a
    dead end, as in the contract's model)."""
    composed = _compose(frame, tuple(ops), trace)
    return isinstance(composed, Composed) and all(
        f.kind is FindingKind.DEPENDENCY for f in host_findings(frame, tuple(ops), composed))


def _fresh_set_executes(frame: AcceptedInterpretation, slot: Slot) -> bool:
    """A one-operation answer that SETs a resolved value the user chooses in ``slot`` (§8.4 condition 3). The value is
    free, so every value-dependent cause or rule is avoided (another value than F16's, a comparable unit, a bound that
    keeps the interval, the buyer's territory, a catalogued year); what remains is what no value avoids."""
    if slot.key in grouped_keys(frame):
        return False
    if slot.key == CARDINALITY_KEY and not any(c.slot.key == ORDER_KEY for c in frame.conditions):
        return False
    if slot == GEO_POSITIVE and len(_territorial_scopes(frame.conditions, frozenset())) > 1:
        return False
    rest = [c for c in frame.conditions if c.slot != slot]
    smmlv = _smmlv(rest)
    if not smmlv:
        return True
    if slot == TIME_POSITIVE:  # the chosen year can be the SMMLV bounds' own
        years = {c.provenance.year for c in smmlv}
        return len(years) == 1 and next(iter(years)) in SMMLV_CATALOGUE_YEARS
    return _smmlv_status(frame, rest) != "ambiguous"


def _has_executing_answer(frame, answer_slots, candidates, trace: str, origin: ConditionOrigin) -> bool:
    """§8.4 condition 3 (R3c): a candidate executes or, without candidates, some one-operation answer on
    ``answer_slots`` does (CLEAR, KEEP, REPLACE_UNIVERSE to the frame's universe, or a SET of a chosen value)."""
    if candidates:
        return any(executes(frame, candidate, trace) for candidate in candidates)
    for slot in sorted(answer_slots, key=lambda s: (min(SLOT_RANK[c.key] for c in covered(s)), s.key)):
        if slot.is_universe:
            request = frame.requests[0]
            trials = (PatchOp(PatchOpKind.REPLACE_UNIVERSE, UNIVERSE_SLOT, None,
                              UniverseTarget(request.subject, request.universe_kind), origin),
                      PatchOp(PatchOpKind.KEEP, UNIVERSE_SLOT, None, None, origin))
        elif slot.dimension_wide:  # a dimension-wide slot is only CLEAR's (§5.1)
            trials = (PatchOp(PatchOpKind.CLEAR, slot, None, None, origin),)
        else:
            trials = (PatchOp(PatchOpKind.CLEAR, slot, None, None, origin),
                      PatchOp(PatchOpKind.KEEP, slot, None, None, origin))
        if any(executes(frame, (trial,), trace) for trial in trials):
            return True
        if not slot.is_universe and not slot.dimension_wide and _fresh_set_executes(frame, slot):
            return True
    return False


def law_violations(base, frame_ops, candidates, answer_slots, trace: str, origin: ConditionOrigin) -> list[str]:
    """§8.4: the frame is a valid effective with nothing open; every candidate composes on it without causes and is
    resolved; ``answer_slots`` ⊇ the candidates' slots; and at least one answer executes."""
    frame_ops = tuple(frame_ops)
    composed = _compose(base, frame_ops, trace)
    if isinstance(composed, DeltaRefusal):
        return ["frame has a refusal cause"]
    if _questions(base, frame_ops, composed):
        return ["frame holds an open question"]
    frame, out = composed.effective, []
    for candidate in candidates:
        made = _compose(frame, candidate, trace)
        if isinstance(made, DeltaRefusal):
            out.append("candidate does not compose on the frame")
        elif _questions(frame, candidate, made):
            out.append("candidate not resolved at ask time")
    if not covered_keys(op for candidate in candidates for op in candidate) <= {s.key for s in answer_slots}:
        out.append("answer_slots do not cover the candidates")
    if not out and not _has_executing_answer(frame, answer_slots, candidates, trace, origin):
        out.append("no answer executes")
    return out


def _slots(ops) -> frozenset[Slot]:
    """The slots an answer may fix: each operation's own slot (a dimension-wide CLEAR included, §8.1: ⊇ the
    candidates' slots) and every slot it covers."""
    ops = tuple(ops)
    return frozenset(op.slot for op in ops) | frozenset(SLOT_BY_KEY[key] for key in covered_keys(ops))


def _ask(base, code: str, frame_ops, open_ops, candidates, answer_slots, trace, origin) -> DeltaOutcome:
    """CLARIFY with its pending when §8.4 allows it; INFORM with the same code otherwise (never a pending that cannot
    exist)."""
    if law_violations(base, frame_ops, candidates, answer_slots, trace, origin):
        return DeltaOutcome(DeltaDisposition.INFORM, code)
    frame = _compose(base, tuple(frame_ops), trace).effective
    return DeltaOutcome(DeltaDisposition.CLARIFY, code, pending=PendingQuestion(
        code, frame, tuple(frame_ops), tuple(open_ops), tuple(candidates), frozenset(answer_slots)))


# ---- the outcome -----------------------------------------------------------------------------------------------------
def delta_outcome(base: AcceptedInterpretation, patch: SemanticPatch, *, base_trace_id: str,
                  anchor_expired: bool = False) -> DeltaOutcome:
    """The answer of a delta turn with no pending vigente (§6 steps 3b–4, §8.4). ``anchor_expired`` is the injected
    fact of §10.1 (30 minutes from the previous turn, measured by M2A-6 with the injected instant)."""
    if type(patch) is not SemanticPatch or type(anchor_expired) is not bool:
        raise ContractError("INVALID_DELTA_OUTCOME_INPUT")
    ops, trace = patch.ops, base_trace_id
    origin = ops[0].origin
    causes = refusal_causes(base, ops)
    if causes:
        top = causes[0]
        if len(causes) > 1 or anchor_expired:  # §6 step 3b: no pending; on an expired anchor only H1's may exist
            return DeltaOutcome(DeltaDisposition.INFORM, top.published, causes=causes)
        candidates, extra = (), frozenset()
        if top.refusal is Refusal.DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER:  # §6.7 (R3-1)
            extra = frozenset({SLOT_BY_KEY[ORDER_KEY], SLOT_BY_KEY[CARDINALITY_KEY]})
            if top.ops and all(op.op is PatchOpKind.CLEAR for op in top.ops):  # without E-ORDER: only C1 (§19)
                at = top.ops[0].origin
                candidates = ((PatchOp(PatchOpKind.CLEAR, SLOT_BY_KEY[ORDER_KEY], None, None, at),
                               PatchOp(PatchOpKind.CLEAR, SLOT_BY_KEY[CARDINALITY_KEY], None, None, at)),)
        slots = _slots(top.ops) | _slots(op for c in candidates for op in c) | extra
        outcome = _ask(base, top.published, _without(ops, top.ops), top.ops, candidates, slots, trace, origin)
        return DeltaOutcome(outcome.disposition, outcome.code, pending=outcome.pending, causes=causes)
    composed = compose_ops(base, ops, base_trace_id=trace)
    findings = host_findings(base, ops, composed)
    first = findings[0] if findings else None
    if first is not None and first.kind is FindingKind.DEPENDENCY:
        return DeltaOutcome(DeltaDisposition.DEPENDENCY, None, dependency=first.dependency)
    if anchor_expired:  # H1: «continuar» composes and executes, or the rule that would fire informs
        if first is not None:
            return DeltaOutcome(DeltaDisposition.INFORM, first.code)
        return DeltaOutcome(DeltaDisposition.CLARIFY, HOST_CODES["H1"], pending=PendingQuestion(
            HOST_CODES["H1"], base, (), tuple(ops), (tuple(ops),), _slots(ops)))
    if first is None:
        return DeltaOutcome(DeltaDisposition.EXECUTE, None, composed.effective, composed.effects)
    if first.kind is FindingKind.INFORM:
        return DeltaOutcome(DeltaDisposition.INFORM, first.code)
    frame_ops = () if first.rule == "H11" else _without(ops, first.open_ops)
    slots = _slots(first.open_ops) | _slots(op for c in first.candidates for op in c)
    return _ask(base, first.code, frame_ops, first.open_ops, first.candidates, slots, trace, origin)


def relative_outcome(base: AcceptedInterpretation, rest: tuple[PatchOp, ...], *, base_trace_id: str,
                     origin: ConditionOrigin, answer_slots: frozenset[Slot], anchor_expired: bool,
                     pending_present: bool) -> DeltaOutcome:
    """§7.1 row 6 (F15, H4) under §8.4: CLARIFY RELATIVE_TO_PREVIOUS_VALUE with the anchor and the rest of the turn
    composed as ``frame``; INFORM with a pending vigente, on an expired anchor, or when that pending cannot exist. The
    relative condition is not a ``PatchOp``: its ``origin`` and ``answer_slots`` come from the caller (M2A-5/M2A-6)."""
    code = HOST_CODES["H4"]
    if pending_present or anchor_expired:
        return DeltaOutcome(DeltaDisposition.INFORM, code)
    return _ask(base, code, tuple(rest), (), (), frozenset(answer_slots), base_trace_id, origin)
