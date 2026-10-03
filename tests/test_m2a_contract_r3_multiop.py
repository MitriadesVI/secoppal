"""INV-M2A-25 multi-op (R3b, U21): a patch of several operations has exactly ONE representable response.

This works at the CONTRACT level and is not a reducer. A SYMBOLIC slot-occupancy model of the amended contract answers
a turn (base occupancy plus patch). An independent checker then says whether that answer is representable:

* EXECUTE: the composed state satisfies the type's slot invariants.
* CLARIFY: an A.3 ``CLARIFY`` code and a pending that satisfies §8.4:
  - the frame has no §6 3b cause and no open operation, and its state is valid;
  - every candidate composes without causes into a valid state, and some answer executes (R3c);
  - ``answer_slots`` ⊇ the candidates' slots;
  - on an expired anchor only ``CONTEXT_EXPIRED_CONFIRM``, and its «continuar» executes;
  - there is never a second pending.
* INFORM: an A.3 ``INFORM`` code, or a ``CLARIFY`` code from the closed INFORM-capable list. No new pending; the old
  one stays intact.
* REASK: an A.3 ``REASK`` code over the pending that already existed, kept intact.
* DEPENDENCY: a §19 row whose ``impacto_m2a3`` is ``COMPOSICION``.

Generated, never listed by hand:
* slots and admissible (op, slot) pairs: every ``PatchOpKind`` × slot constructed as a ``PatchOp`` (the sweep module);
* the precedence of refusals (§6 3b), the host rules (§6 4), the A.3 codes, the INFORM-capable list and the 4b
  motives: read from the contract through the one parser;
* the wire's ORDER/CARDINALITY rules: by constructing IR v2 proposals;
* dependencies: the sealed §19 extraction.

What the model abstracts, declared:
* Values are equality tokens. An authority reading is a STATUS of the operation: resolved, unresolved, or a host
  question with its rule.
* Value arithmetic is outside: SMMLV recalculation, territory containment.
* Events, NEW_SEARCH and the pending-control labels are outside: ``CANCEL_PENDING``, ``no_selection``,
  ``value_without_comparator``. Every patch is a delta, Continuación(P).
* Three items are not a ``PatchOp``: the A.6 reserved labels. They are a relative-to-previous condition (§7.1 row
  6), a VALUE bound without comparator (§6: ``OP_NOT_ACCREDITED:VALUE`` ⇒ ``VALUE_WITHOUT_COMPARATOR``) and
  ``no_selection`` (§8.3 row 6 with a pending; without one, §7.1 row 4b ``UNSUPPORTED_CONDITION``).

The universal property on the REAL ``compose`` (permutations plus ``check_transition``) is a gate of M2A-3 phase 2
(§8.4). ``python -m tests.test_m2a_contract_r3_multiop`` prints the sweep.
"""
from __future__ import annotations

import itertools
import json
import re
from collections import Counter, defaultdict
from functools import lru_cache
from pathlib import Path
from typing import NamedTuple

from hypothesis import event, given, settings, strategies as st

from app.core.foundation_authority_values import OrderKey
from app.core.foundation_operations import Operation
from app.core.interpretation_contract import SLOT_DIMENSIONS, ContractError, Polarity
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_m2a_contract_r3_sweep import CT, all_slots, dim_domain, op_slot_matrix, proposal, wire

ROOT = Path(__file__).resolve().parents[1]
TEXT = CT.read()
EXTRACTION = json.loads((ROOT / "docs/audits/m2a_conversation/gold/CONTRACT_S19_DEPENDENCIES.json")
                        .read_text(encoding="utf-8"))

# ---- the contract, through the one parser ---------------------------------------------------------------------------
PRECEDENCE = {code: rank for rank, code in enumerate(CT.refusal_precedence(TEXT), 1)}
REFUSAL_A3 = CT.refusal_table(TEXT)
HOST = {rule: (disposition, code) for rule, disposition, code in CT.host_rules(TEXT)}
HOST_RANK = {rule: rank for rank, rule in enumerate(HOST)}
OPNA, LEAVES = "OP_NOT_ACCREDITED:<DIM>", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER"
WIRE_INVALID = "UNSUPPORTED_PROPOSAL:INVALID_PROPOSAL"  # §7.1 row 4b (D-6)
NO_COMPARATOR_CODE = next(code for code in REFUSAL_A3[OPNA] if "<DIM>" not in code)  # §6 table (U25)
NO_SELECTION_CODE = "UNSUPPORTED_PROPOSAL:" + next(  # §7.1 row 4b: the motive whose row names no_selection (U25)
    motive for motive in CT.row_4b_motives(TEXT) if "`no_selection`" in CT.section(TEXT, f"| `{motive}` |", "\n"))


def _expand(codes) -> set[str]:
    out = set()
    for code in codes:
        head, _, tail = code.partition(":")
        if tail == "<DIM>":
            out |= {f"{head}:{dim}" for dim in dim_domain()}
        elif tail == "<motivo>":
            out |= {f"{head}:{motive}" for motive in CT.row_4b_motives(TEXT)}
        elif tail == "<op>":
            out |= {f"{head}:{op.value}" for op in Operation}
        else:
            out.add(code)
    return out


CODES = {disposition: _expand(codes) for disposition, codes in CT.annex_a3(TEXT).items()}
INFORM_OK = CODES["INFORM"] | _expand(CT.inform_capable_codes(TEXT))
COMPOSICION = {d["dep_id"] for d in EXTRACTION["dependencies"] if d["impacto_m2a3"] == "COMPOSICION"}
EXPIRED_CODE = HOST["H1"][1]

# ---- slots (generated from the code) and their §4.2 order (read from the contract) ----------------------------------
SLOT = {s.key: s for s in all_slots()}
SINGLE = tuple(k for k, s in SLOT.items() if not s.dimension_wide)
CONDITION_SLOTS = tuple(k for k in SINGLE if not SLOT[k].is_universe)
IDX = {k: i for i, k in enumerate(CONDITION_SLOTS)}
COVER = {k: (tuple(j for j in CONDITION_SLOTS if SLOT[j].dimension is s.dimension) if s.dimension_wide else (k,))
         for k, s in SLOT.items()}
POS_OF = {k: f"{s.dimension.value}/POSITIVE" for k, s in SLOT.items() if s.polarity is Polarity.NEGATIVE}
OPPOSITE = {"VALUE/lower": "VALUE/upper", "VALUE/upper": "VALUE/lower"}


def dim_of(key: str) -> str:
    return "UNIVERSE" if SLOT[key].is_universe else SLOT[key].dimension.value


def _slot_rank() -> dict[str, int]:
    """§4.2: dimensions in the order the slot table names them; POSITIVE before NEGATIVE and lower before upper, as
    its rows write them."""
    rows = CT.table(CT.section(TEXT, "### 4.2 Ranuras", "### 4.3 Identidad"), "| Ranura | Contenido |")
    assert "`(TOPIC, POSITIVE)`, `(TOPIC, NEGATIVE)`" in rows[0][0] and rows[1][0] == "`VALUE.lower`"
    names, dims = {d.value for d in SLOT_DIMENSIONS} | {"UNIVERSE"}, []
    for row in rows:
        dims += [t for t in re.findall(r"[A-Z][A-Z_]+", row[0]) if t in names and t not in dims]
    inner = {"POSITIVE": 0, "NEGATIVE": 1, "lower": 0, "upper": 1}
    ordered = sorted(SINGLE, key=lambda k: (dims.index(dim_of(k)), inner.get(k.partition("/")[2], 0)))
    return {k: rank for rank, k in enumerate(ordered)}


RANK = _slot_rank()


# ---- symbolic values ------------------------------------------------------------------------------------------------
class Op(NamedTuple):
    kind: str  # PatchOpKind value, or an A.6 reserved label: RELATIVE, NO_COMPARATOR, NO_SELECTION
    slot: str
    value: str | None = None
    status: str = "ok"  # ok | unresolved | crosses | noncomparable | asks:<H> | asks2:<H> | state_missing | label


class State(NamedTuple):
    slots: tuple[tuple[str, ...], ...]  # values per CONDITION_SLOTS
    or_grouped: frozenset[str]
    universe: tuple[str, ...]


class Pending(NamedTuple):
    reason: str
    frame_ops: tuple[Op, ...]  # the frame is base + these operations (§8.4: never an open one)
    frame: State
    candidates: tuple[tuple[Op, ...], ...]
    answer_slots: frozenset[str]
    asked: tuple[Op, ...] = ()  # the patch of the turn that asked (record asked_trace_id): F24, R3d


class Response(NamedTuple):
    kind: str  # EXECUTE | CLARIFY | INFORM | REASK | DEPENDENCY
    code: str | None = None
    state: State | None = None
    pending: Pending | None = None  # CLARIFY: the new one; INFORM/REASK: the one kept
    dependency: str | None = None
    clause: str = ""


class Config(NamedTuple):
    profile: str
    base: State
    dual: bool
    expired: bool
    pending: Pending | None
    wire: str  # current (IR v2 today) | normal (§6.7: «si M2A-5 cambia el wire, aplican las reglas normales»)
    view_derived: bool = False
    e_order: bool = True


# Host questions an authority reading can raise on a SET (§6 paso 4, §15.4-§15.5, §16.1, F12/F30). Codes come from
# the parsed table; only the attachment rule -> slot is declared here.
QUESTIONS = {"H3": ("VALUE", "asks"), "H5": ("BUYER/POSITIVE", "asks2"), "H6": ("GEOGRAPHY/POSITIVE", "asks2"),
             "H7": ("GEOGRAPHY/POSITIVE", "asks"), "H9": ("VALUE", "asks"), "H10": ("TIME/POSITIVE", "asks")}
H5_CANDIDATES = ((Op("SET", "BUYER/POSITIVE", "x"),), (Op("SET", "GEOGRAPHY/POSITIVE", "x"),))
H6_CANDIDATES = tuple(sorted(((Op("CLEAR", "BUYER/POSITIVE"), Op("SET", "GEOGRAPHY/POSITIVE", "x")),
                              (Op("SET", "GEOGRAPHY/POSITIVE", "x"),))))
WRITERS = ("SET", "SET_ORDER", "REPLACE_UNIVERSE")


def op_variants() -> tuple[Op, ...]:
    """Every admissible (op, slot) of the constructed PatchOp matrix, with the readings that change the contract's
    answer for it."""
    out = []
    for kind, key in sorted(k for k, result in op_slot_matrix().items() if result == "OK"):
        slot = SLOT[key]
        if kind == "SET" and slot.dimension is D.VALUE:
            out += [Op(kind, key, "v", s) for s in ("ok", "crosses", "noncomparable", "unresolved")]
            out += [Op(kind, key, "v", f"{how}:{rule}") for rule, (at, how) in QUESTIONS.items() if at == "VALUE"]
        elif kind == "SET" and slot.dimension is D.CARDINALITY:
            out += [Op(kind, key, "n"), Op(kind, key, "n", "unresolved")]
        elif kind == "SET" and slot.polarity is Polarity.NEGATIVE:  # x: the carried positive (F16); y: unrelated
            out += [Op(kind, key, "x"), Op(kind, key, "y"), Op(kind, key, "x", "unresolved")]
        elif kind == "SET":  # x: the carried positive; w: another value
            out += [Op(kind, key, "x"), Op(kind, key, "w"), Op(kind, key, "x", "unresolved")]
            out += [Op(kind, key, "x", f"{how}:{rule}") for rule, (at, how) in QUESTIONS.items() if at == key]
        elif kind == "SET_ORDER":
            out += [Op(kind, key, k.value) for k in OrderKey] + [Op(kind, key, OrderKey.DATE.value, "unresolved")]
        elif kind == "REPLACE_UNIVERSE":
            out += [Op(kind, key, "processes"), Op(kind, key, "contracts"), Op(kind, key, "opportunities"),
                    Op(kind, key, "processes", "state_missing"), Op(kind, key, "contracts", "state_missing")]
        else:
            out.append(Op(kind, key))
    return (*out, *LABEL_ITEMS)


# A.6 reserved labels: not PatchOps. The slot only places the item in §4.2 order (none of them writes a slot).
LABEL_ITEMS = (Op("RELATIVE", "VALUE/lower", None, "label"), Op("NO_COMPARATOR", "VALUE", None, "label"),
               Op("NO_SELECTION", "VALUE/lower", None, "label"))


OPS = op_variants()


# ---- the model ------------------------------------------------------------------------------------------------------
def occupied(state: State, key: str) -> bool:
    return key == "UNIVERSE" or bool(state.slots[IDX[key]])


def touched(ops) -> tuple[set[str], set[str]]:
    written = {op.slot for op in ops if op.kind in WRITERS}
    cleared = {k for op in ops if op.kind == "CLEAR" for k in COVER[op.slot]}
    return written, cleared


def carried(base: State, ops, key: str) -> bool:
    """§6 paso 3: untouched by every operation (KEEP touches nothing)."""
    written, cleared = touched(ops)
    return occupied(base, key) and key not in written and key not in cleared


def covered(ops) -> frozenset[str]:
    return frozenset(k for op in ops for k in COVER[op.slot])


@lru_cache(maxsize=None)
def wire_error(orders: int, cardinalities: int) -> str | None:
    """IR v2 decides the ORDER/CARDINALITY shape of a FOLLOWUP_DELTA proposal: build one and ask it."""
    quotes = [f"orden{i}" for i in range(orders)] + [f"tope{i}" for i in range(cardinalities)]
    if not quotes:
        return None
    raw = " ".join(quotes)
    conditions = tuple(wire(f"o{i}", D.ORDER, "date_desc", f"orden{i}", raw) for i in range(orders)) + tuple(
        wire(f"n{i}", D.CARDINALITY, "top_n", f"tope{i}", raw) for i in range(cardinalities))
    try:
        proposal(raw, Operation.FOLLOWUP_DELTA, conditions)
    except ContractError as error:
        return str(error)
    return None


class Cause(NamedTuple):
    rank: int
    slot_rank: int
    open_ops: tuple[Op, ...]
    refusal: str
    slot: str
    authority_code: str = ""  # the authority's own code when the table names one (VALUE_WITHOUT_COMPARATOR)

    @property
    def code(self) -> str:
        if self.authority_code:
            return self.authority_code
        a3 = REFUSAL_A3[self.refusal][0]
        return a3.replace("<DIM>", dim_of(self.slot)) if self.refusal == OPNA else a3


def _cause(refusal: str, slot: str, ops, authority_code: str = "") -> Cause:
    return Cause(PRECEDENCE[refusal], min(RANK[k] for k in COVER[slot]), tuple(sorted(ops)), refusal, slot,
                 authority_code)


def causes(base: State, ops) -> list[Cause]:
    """§6 paso 3b: every cause, sorted by precedence and then §4.2 slot order."""
    out = []
    written, cleared = touched(ops)
    for op in ops:
        if op.kind in ("CLEAR", "SET", "KEEP"):
            hit = [k for k in COVER[op.slot] if k in base.or_grouped]
            if hit:
                out.append(_cause("TARGET_NOT_ADDRESSABLE", min(hit, key=RANK.get), (op,)))
    per_slot = defaultdict(lambda: ([], [], []))
    for op in ops:
        if op.kind in WRITERS:
            per_slot[op.slot][0].append(op)
        elif op.kind == "KEEP":
            per_slot[op.slot][1].append(op)
        elif op.kind == "CLEAR":
            for k in COVER[op.slot]:
                per_slot[k][2].append(op)
    for key, (writers, keeps, clears) in per_slot.items():
        if len(writers) >= 2 or (keeps and (writers or clears)):
            out.append(_cause("DELTA_SLOT_CONFLICT", key, (*writers, *keeps, *clears)))
    out += [_cause("KEEP_ON_EMPTY_SLOT", op.slot, (op,)) for op in ops if op.kind == "KEEP" and not occupied(base, op.slot)]
    for op in ops:
        if op.kind == "NO_COMPARATOR":  # §6 table: the VALUE authority's own code passes through (U25)
            out.append(_cause(OPNA, op.slot, (op,), NO_COMPARATOR_CODE))
        elif op.status == "unresolved" or (op.kind == "REPLACE_UNIVERSE" and op.value == "opportunities"):  # §19 O1B
            out.append(_cause(OPNA, op.slot, (op,)))
        elif op.kind == "SET" and op.status == "noncomparable" and carried(base, ops, OPPOSITE[op.slot]):  # §15.2
            out.append(_cause(OPNA, op.slot, (op,)))
        elif op.kind == "SET" and op.slot in POS_OF:
            pos = POS_OF[op.slot]
            same_patch = [o for o in ops if o.kind == "SET" and o.slot == pos and o.value == op.value]
            if same_patch:  # P-3
                out.append(_cause(OPNA, op.slot, (op, *same_patch)))
            elif carried(base, ops, pos) and len(base.slots[IDX[pos]]) > 1 and op.value in base.slots[IDX[pos]]:
                out.append(_cause(OPNA, op.slot, (op,)))  # F16 fail-closed (§6.5)
    order_after = (occupied(base, "ORDER") and "ORDER" not in cleared) or "ORDER" in written
    card_after = (occupied(base, "CARDINALITY") and "CARDINALITY" not in cleared) or "CARDINALITY" in written
    if card_after and not order_after:  # §6.7, decided on the state (R3-1)
        opened = ([op for op in ops if op.kind == "CLEAR" and "ORDER" in COVER[op.slot]] if occupied(base, "ORDER")
                  else [op for op in ops if op.kind == "SET" and op.slot == "CARDINALITY"])
        out.append(_cause(LEAVES, "ORDER", opened))
    return sorted(out)


def apply(base: State, ops) -> State:
    """``compose`` of a patch without causes: CLEAR, then SET/SET_ORDER/REPLACE_UNIVERSE (§6 paso 2), with the
    declared substitutions F16 and A8.1 (§6 paso 5)."""
    slots, grouped, universe = list(base.slots), set(base.or_grouped), base.universe
    for op in ops:
        if op.kind == "CLEAR":
            for k in COVER[op.slot]:
                slots[IDX[k]] = ()
                grouped.discard(k)
    for op in ops:
        if op.kind in ("SET", "SET_ORDER"):
            if op.slot in POS_OF and carried(base, ops, POS_OF[op.slot]) and base.slots[IDX[POS_OF[op.slot]]] == (
                    op.value,):
                slots[IDX[POS_OF[op.slot]]] = ()  # F16: POLARITY_INVERTED
            if op.status == "crosses" and carried(base, ops, OPPOSITE[op.slot]):
                slots[IDX[OPPOSITE[op.slot]]] = ()  # A8.1: VALUE_RANGE_REPLACED
            slots[IDX[op.slot]] = (op.value,)
            grouped.discard(op.slot)
        elif op.kind == "REPLACE_UNIVERSE":
            universe = (op.value,)
    return State(tuple(slots), frozenset(grouped), universe)


VALID_UNIVERSES = {("contracts",), ("processes",), ("contracts", "processes")}
EMPTY = State(tuple(() for _ in CONDITION_SLOTS), frozenset(), ("contracts",))


def state_violations(state: State) -> list[str]:
    """The type's slot invariants (TWO_CONDITIONS_IN_*, CARDINALITY_WITHOUT_ORDER, NONCONJUNCTIVE_ORDER, dual
    anchor) plus O1B: opportunities are not representable yet (§19)."""
    out = [f"TWO_CONDITIONS_IN_{k}" for k in ("ORDER", "CARDINALITY", "VALUE/lower", "VALUE/upper")
           if len(state.slots[IDX[k]]) > 1]
    if occupied(state, "CARDINALITY") and not occupied(state, "ORDER"):
        out.append("CARDINALITY_WITHOUT_ORDER")
    if "ORDER" in state.or_grouped:
        out.append("NONCONJUNCTIVE_ORDER")
    if any(len(state.slots[IDX[k]]) < 2 for k in state.or_grouped):
        out.append("OR_GROUP_WITHOUT_ATOMS")
    if tuple(sorted(state.universe)) not in VALID_UNIVERSES:
        out.append(f"UNIVERSE_NOT_REPRESENTABLE:{state.universe}")
    return out


class Finding(NamedTuple):
    rank: int
    slot_rank: int
    rule: str
    kind: str  # question | inform | dependency
    open_ops: tuple[Op, ...] = ()
    candidates: tuple[tuple[Op, ...], ...] = ()
    dependency: str = ""


def findings(base: State, ops, state: State, view_derived: bool = False) -> list[Finding]:
    """§6 paso 4, H3-H12 (H1 and H2 are decided by the caller), plus the §19 COMPOSICION rows the model reaches."""
    out = []
    for op in ops:
        how, _, rule = op.status.partition(":")
        if how == "asks":
            out.append(Finding(HOST_RANK[rule], RANK[op.slot], rule, "question", (op,)))
        elif op.status == "asks2:H5" and occupied(state, "ACTOR/POSITIVE"):
            out.append(Finding(HOST_RANK["H5"], RANK[op.slot], "H5", "question", (op,), H5_CANDIDATES))
        elif op.status == "asks2:H6" and carried(base, ops, "BUYER/POSITIVE"):
            out.append(Finding(HOST_RANK["H6"], RANK[op.slot], "H6", "question", (op,), H6_CANDIDATES))
        elif op.kind == "REPLACE_UNIVERSE" and state.universe != base.universe:
            if op.status == "state_missing" and (occupied(state, "STATE/POSITIVE") or occupied(state, "STATE/NEGATIVE")):
                out.append(Finding(HOST_RANK["H8"], RANK["STATE/POSITIVE"], "H8", "question", (op,)))
            elif occupied(state, "MODALITY/POSITIVE") or occupied(state, "MODALITY/NEGATIVE"):
                out.append(Finding(HOST_RANK["H8"], RANK["MODALITY/POSITIVE"], "H8", "dependency",
                                   dependency="DEP-MODALITY-UNIVERSE"))
    if not any(state.slots):
        out.append(Finding(HOST_RANK["H11"], 0, "H11", "question", tuple(sorted(ops))))
    if len(ops) == 1 and ops[0].kind == "CLEAR" and not any(occupied(base, k) for k in COVER[ops[0].slot]):
        out.append(Finding(HOST_RANK["H12"], 0, "H12", "inform", tuple(ops)))
    if not out and view_derived and state != base and not any(op.kind == "REPLACE_UNIVERSE" for op in ops):
        out.append(Finding(len(HOST), 0, "F9", "dependency", dependency="DEP-DERIVED-VIEW"))
    return sorted(out)


def open_operations(base: State, ops) -> list[Op]:
    """Operations that cannot sit in a frame: unresolved, a host question, a relative condition (§8.2, §8.4)."""
    state = apply(base, ops)
    asked = {op for f in findings(base, ops, state) if f.kind == "question" and f.rule != "H11" for op in f.open_ops}
    return sorted(op for op in ops if op in asked or op.status in ("unresolved", "label"))


def law_violations(base: State, frame_ops, candidates, answer_slots) -> list[str]:
    """§8.4: the frame is valid and has nothing open; every candidate composes on it without causes."""
    out = []
    if causes(base, frame_ops):
        out.append("frame has a refusal cause")
    if open_operations(base, frame_ops):
        out.append("frame holds an open operation")
    frame = apply(base, frame_ops)
    out += [f"frame invalid: {v}" for v in state_violations(frame)]
    for candidate in candidates:
        if open_operations(frame, candidate):
            out.append("candidate not resolved at ask time")
        elif causes(frame, candidate):
            out.append("candidate does not compose on the frame")
        else:
            out += [f"candidate composes into an invalid state: {v}" for v in state_violations(apply(frame, candidate))]
    if not covered(op for c in candidates for op in c) <= answer_slots:
        out.append("answer_slots do not cover the candidates")
    if not out and not has_executing_answer(frame, frozenset(answer_slots), tuple(candidates)):
        out.append("no answer executes")  # §8.4 condition 3 (R3c, U26)
    return out


def executes(frame: State, patch) -> bool:
    """The patch composes on the frame with no cause, nothing open and no further question (a §19 dependency is a
    declared red, not a dead end)."""
    return (not causes(frame, patch) and not open_operations(frame, patch)
            and all(f.kind == "dependency" for f in findings(frame, patch, apply(frame, patch))))


@lru_cache(maxsize=None)
def has_executing_answer(frame: State, answer_slots: frozenset, candidates: tuple) -> bool:
    """§8.4 (3): a candidate executes or, without candidates, some one-operation answer on ``answer_slots`` does. The
    model searches one-operation answers; every answer to every pending is a gate of M2A-4."""
    if candidates:
        return any(executes(frame, c) for c in candidates)
    return any(executes(frame, (op,)) for op in OPS if op not in LABEL_ITEMS and covered((op,)) <= answer_slots)


def r31_candidates(frame: State, e_order: bool) -> tuple[tuple[Op, ...], ...]:
    """§6.7 (R3-1, D-1 option 1): C1 plus one SET_ORDER per alternative order key; without E-ORDER only C1."""
    c1 = (Op("CLEAR", "CARDINALITY"), Op("CLEAR", "ORDER"))
    current = frame.slots[IDX["ORDER"]]
    alternatives = tuple((Op("SET_ORDER", "ORDER", k.value),) for k in OrderKey if k.value not in current)
    return (c1, *alternatives) if e_order else (c1,)


def _pending(base, reason, open_ops, frame_ops, candidates, extra_slots=frozenset(), asked=None) -> Pending:
    candidates = tuple(sorted(tuple(sorted(c)) for c in candidates))
    slots = covered(open_ops) | covered(op for c in candidates for op in c) | extra_slots
    asked = tuple(sorted((*open_ops, *frame_ops) if asked is None else asked))  # the asking turn = open + resolved
    return Pending(reason, tuple(sorted(frame_ops)), apply(base, frame_ops), candidates, frozenset(slots), asked)


def _without(ops, removed) -> tuple[Op, ...]:
    removed = list(removed)
    out = []
    for op in ops:
        if op in removed:
            removed.remove(op)
        else:
            out.append(op)
    return tuple(out)


def respond(cfg: Config, ops, *, law: bool = True) -> Response:
    """The contract's answer to ``ops`` on ``cfg``. ``law=False`` is the mutant without §8.4 and without H1's
    restriction: it must be caught by the checker."""
    ops, base = tuple(ops), cfg.base
    pending = None if cfg.expired else cfg.pending  # §10.1: no pending outlives the anchor (G-FU-12: F11 wins)
    orders = sum(op.kind == "SET_ORDER" for op in ops)
    cards = sum(op.kind == "SET" and op.slot == "CARDINALITY" for op in ops)
    if cfg.wire == "current" and wire_error(orders, cards):  # §7.1 row 4b, also on an expired anchor
        return Response("INFORM", WIRE_INVALID, pending=pending, clause="§7.1 4b")
    if pending is None and any(op.kind == "NO_SELECTION" for op in ops):  # §7.1 4b: nothing to choose from (U25)
        return Response("INFORM", NO_SELECTION_CODE, clause="§7.1 4b")
    relative = [op for op in ops if op.kind == "RELATIVE"]
    if relative:  # §7.1 row 6 (F15)
        code, rest = HOST["H4"][1], _without(ops, relative)
        made = _pending(base, code, relative, rest, ())
        if law and (pending is not None or cfg.expired or law_violations(base, rest, (), made.answer_slots)):
            return Response("INFORM", code, pending=pending, clause="§7.1 6 + §8.4")
        return Response("CLARIFY", code, pending=made, clause="§7.1 6")
    if pending is not None:
        return answer(cfg, pending, ops)
    found = causes(base, ops)
    if found:
        top = found[0]
        if law and (len(found) > 1 or cfg.expired):
            return Response("INFORM", top.code, clause="§6 3b")
        frame_ops = _without(ops, top.open_ops)
        frame = apply(base, frame_ops)
        r31 = top.refusal == LEAVES and all(op.kind == "CLEAR" for op in top.open_ops)
        candidates = r31_candidates(frame, cfg.e_order) if r31 else ()
        extra = frozenset({"ORDER", "CARDINALITY"}) if top.refusal == LEAVES else frozenset()
        made = _pending(base, top.code, top.open_ops, frame_ops, candidates, extra)
        if law and law_violations(base, frame_ops, made.candidates, made.answer_slots):
            return Response("INFORM", top.code, clause="§6 3b + §8.4")
        return Response("CLARIFY", top.code, pending=made, clause="§6 3b")
    state = apply(base, ops)
    first = next(iter(findings(base, ops, state, cfg.view_derived)), None)
    if cfg.expired:  # H1
        if first is not None and first.kind == "dependency":
            return Response("DEPENDENCY", dependency=first.dependency, clause="H1 + §19")
        if first is not None and law:
            return Response("INFORM", HOST[first.rule][1], clause="H1")
        return Response("CLARIFY", EXPIRED_CODE, pending=_pending(base, EXPIRED_CODE, ops, (), (ops,)), clause="H1")
    if first is None:
        return Response("EXECUTE", state=state, clause="§6")
    if first.kind == "dependency":
        return Response("DEPENDENCY", dependency=first.dependency, clause=first.rule)
    code = HOST[first.rule][1]
    if first.kind == "inform":
        return Response("INFORM", code, clause=first.rule)
    frame_ops = () if first.rule == "H11" else _without(ops, first.open_ops)
    made = _pending(base, code, first.open_ops, frame_ops, first.candidates)
    if law and law_violations(base, frame_ops, made.candidates, made.answer_slots):
        return Response("INFORM", code, clause=f"{first.rule} + §8.4")
    return Response("CLARIFY", code, pending=made, clause=first.rule)


def normalized(frame: State, ops) -> tuple[Op, ...]:
    """§8.3 «Selección» (R3d, U27): with candidates and AFTER the §6 step-2 check, a KEEP of a slot the frame has and
    no other operation of P touches is the identity and is dropped."""
    others = {op: covered(o for o in ops if o is not op) for op in ops}
    return tuple(op for op in ops if not (op.kind == "KEEP" and occupied(frame, op.slot)
                                          and op.slot not in others[op]))


def identity(ops) -> list[tuple]:
    """The multiset of (op, slot, canonical value / universe target): never text or ids."""
    return sorted((op.kind, op.slot, op.value) for op in ops)


def selects(candidate: tuple[Op, ...], ops) -> bool:
    """§8.3 «Selección» (R3c, U26): EXACT match of the multiset of resolved operations with the candidate's patch."""
    return not open_operations(EMPTY, ops) and identity(ops) == identity(candidate)


def answer(cfg: Config, pending: Pending, ops) -> Response:
    """§8.3: rows 6 (label) and then «Selección» (rows 4, 5, 7, 8; R3d). Row 1 is an event; rows 2 and 3 need control
    the model leaves out."""
    labels = [op for op in ops if op.kind == "NO_SELECTION"]
    ops = tuple(op for op in ops if op not in labels)
    if labels:  # §8.3 row 5 before row 6: an answer plus an edit of another slot is re-asked as such
        touched_slots = covered(ops)
        if touched_slots & pending.answer_slots and touched_slots - pending.answer_slots:
            return Response("REASK", "ANSWER_EDITS_OTHER_SLOT", pending=pending, clause="§8.3 5")
        return Response("REASK", "OPTION_NOT_CHOSEN", pending=pending, clause="§8.3 6")
    if any(c.refusal == "DELTA_SLOT_CONFLICT" for c in causes(pending.frame, ops)):  # §6 step 2 on P, before
        return Response("REASK", "ANSWER_UNRESOLVED", pending=pending, clause="§8.3 7 (§6 paso 2)")  # normalizing
    if ops and identity(ops) == identity(pending.asked):  # F24 (R3d): re-sending the asking turn does not answer
        return Response("REASK", "ANSWER_UNRESOLVED", pending=pending, clause="§8.3 7 (F24)")
    if pending.candidates:
        ops = normalized(pending.frame, ops)
    touched_slots = covered(ops)
    if not touched_slots & pending.answer_slots:
        return Response("REASK", "PENDING_UNANSWERED", pending=pending, clause="§8.3 8")
    if touched_slots - pending.answer_slots:
        return Response("REASK", "ANSWER_EDITS_OTHER_SLOT", pending=pending, clause="§8.3 5")
    patch = ops
    if pending.candidates:
        chosen = [c for c in pending.candidates if selects(c, ops)]
        if len(chosen) != 1:
            return Response("REASK", "ANSWER_UNRESOLVED", pending=pending, clause="§8.3 7")
        patch = chosen[0]
    frame = pending.frame
    if causes(frame, patch):
        return Response("REASK", "ANSWER_UNRESOLVED", pending=pending, clause="§6 3b (pendiente)")
    state = apply(frame, patch)
    first = next(iter(findings(frame, patch, state, cfg.view_derived)), None)
    if first is not None and first.kind == "dependency":
        return Response("DEPENDENCY", dependency=first.dependency, clause="§8.3 4 + §19")
    if first is not None:
        return Response("REASK", "ANSWER_UNRESOLVED", pending=pending, clause="§8.3 (sigue ambiguo)")
    return Response("EXECUTE", state=state, clause="§8.3 4")


# ---- the checker (independent of which rule answered) ---------------------------------------------------------------
def frame_defects(base: State, frame_ops) -> list[str]:
    """§8.2/§8.4 restated without the model's ``causes``/``findings``, so a defect there cannot hide here: a frame is
    the base plus resolved, non-conflicting operations on addressable, existing slots."""
    out, state = [], apply(base, frame_ops)
    writes = Counter(op.slot for op in frame_ops if op.kind in WRITERS)
    keeps = {op.slot for op in frame_ops if op.kind == "KEEP"}
    clears = {k for op in frame_ops if op.kind == "CLEAR" for k in COVER[op.slot]}
    if any(n > 1 for n in writes.values()) or keeps & (set(writes) | clears):
        out.append("frame composes a slot conflict")
    if any(k != "UNIVERSE" and not base.slots[IDX[k]] for k in keeps):
        out.append("frame keeps an empty slot")
    if any(set(COVER[op.slot]) & base.or_grouped for op in frame_ops if op.kind in ("CLEAR", "SET", "KEEP")):
        out.append("frame targets an atom inside OR/NOT")
    for op in frame_ops:
        how = op.status.partition(":")[0]
        if op.status in ("unresolved", "label") or how == "asks" or op.value == "opportunities":
            out.append(f"frame holds an unresolved or asking operation: {op}")
        elif (op.status == "asks2:H5" and state.slots[IDX["ACTOR/POSITIVE"]]) or (
                op.status == "asks2:H6" and base.slots[IDX["BUYER/POSITIVE"]]
                and not {"BUYER/POSITIVE"} & (set(writes) | clears)):
            out.append(f"frame holds an asking operation: {op}")
    return out


def violations(cfg: Config, ops, r: Response) -> list[str]:
    out = []
    prior = None if cfg.expired else cfg.pending
    if r.kind == "EXECUTE":
        out += state_violations(r.state)
        if prior is None:  # a delta executes only resolved, non-conflicting operations on addressable slots
            out += [f"executed: {d}" for d in frame_defects(cfg.base, ops)]
    elif r.kind == "CLARIFY":
        p = r.pending
        if r.code not in CODES["CLARIFY"]:
            out.append(f"CLARIFY code outside A.3: {r.code}")
        if p is None or p.reason != r.code:
            out.append("CLARIFY without its pending")
        elif prior is not None:
            out.append("a second pending")
        else:
            if cfg.expired and r.code != EXPIRED_CODE:
                out.append("a pending other than CONTEXT_EXPIRED_CONFIRM on an expired anchor")
            if p.frame != apply(cfg.base, p.frame_ops):
                out.append("frame is not base + frame_ops")
            out += law_violations(cfg.base, p.frame_ops, p.candidates, p.answer_slots)
            out += frame_defects(cfg.base, p.frame_ops)
            if r.code == EXPIRED_CODE:
                if len(p.candidates) != 1 or p.frame_ops:
                    out.append("CONTEXT_EXPIRED_CONFIRM is the anchor plus one «continuar»")
                else:
                    go = p.candidates[0]
                    if causes(p.frame, go) or findings(p.frame, go, apply(p.frame, go), cfg.view_derived):
                        out.append("«continuar» would not execute")
    elif r.kind == "INFORM":
        if r.code not in INFORM_OK:
            out.append(f"INFORM code not valid with INFORM: {r.code}")
        if r.pending != prior:
            out.append("INFORM changed the pending")
    elif r.kind == "REASK":
        if prior is None:
            out.append("REASK without a pending")
        if r.code not in CODES["REASK"]:
            out.append(f"REASK code outside A.3: {r.code}")
        if r.pending != prior:
            out.append("REASK changed the pending")
    elif r.kind == "DEPENDENCY":
        if r.dependency not in COMPOSICION:
            out.append(f"dependency not a COMPOSICION row of §19: {r.dependency}")
    else:
        out.append(f"unknown response {r.kind}")
    return out


# ---- base occupancy ---------------------------------------------------------------------------------------------------
POSITIVE = [k for k in CONDITION_SLOTS if SLOT[k].polarity is Polarity.POSITIVE]
NEGATIVE = [k for k in CONDITION_SLOTS if SLOT[k].polarity is Polarity.NEGATIVE]


def _state(values: dict[str, tuple[str, ...]], grouped=(), dual=False) -> State:
    return State(tuple(values.get(k, ()) for k in CONDITION_SLOTS), frozenset(grouped),
                 ("contracts", "processes") if dual else ("contracts",))


def _full(positive: tuple[str, ...]) -> dict:
    return {**{k: positive for k in POSITIVE}, **{k: ("y",) for k in NEGATIVE}, "VALUE/lower": ("v0",),
            "VALUE/upper": ("v1",), "ORDER": (OrderKey.VALUE.value,), "CARDINALITY": ("n0",)}


PROFILES = {  # name -> (slot values, OR-grouped slots)
    "MIN": ({"TOPIC/POSITIVE": ("x",)}, ()),
    "ORDER_ONLY": ({"TOPIC/POSITIVE": ("x",), "ORDER": (OrderKey.VALUE.value,)}, ()),
    "FULL": (_full(("x",)), ()),
    "MULTI": (_full(("x", "z")), ()),
    "OR": ({**_full(("x",)), **{k: ("x", "z") for k in ("TOPIC/POSITIVE", "BUYER/POSITIVE", "GEOGRAPHY/POSITIVE")}},
           ("TOPIC/POSITIVE", "BUYER/POSITIVE", "GEOGRAPHY/POSITIVE")),
}


def prior_pendings(base: State, e_order: bool) -> dict[str, Pending | None]:
    """The pendings a previous turn could have left on this base, each one built only if §8.4 allows it."""
    options = {"none": None}
    if occupied(base, "ORDER") and occupied(base, "CARDINALITY"):
        options["R3-1"] = _pending(base, REFUSAL_A3[LEAVES][0], (), (), r31_candidates(base, e_order),
                                   frozenset({"ORDER", "CARDINALITY"}), asked=(Op("CLEAR", "ORDER"),))
    options["ONE_SLOT"] = _pending(base, HOST["H10"][1], (Op("SET", "TIME/POSITIVE", "x", "asks:H10"),), (), ())
    two = _pending(base, HOST["H5"][1], (), (), H5_CANDIDATES, asked=(Op("SET", "BUYER/POSITIVE", "x", "asks2:H5"),))
    if not law_violations(base, (), two.candidates, two.answer_slots):
        options["TWO_CANDIDATES"] = two
    return options


def configs(*, wires=("current", "normal"), views=(False,), e_orders=(True,)) -> list[Config]:
    out = []
    for name, (values, grouped) in PROFILES.items():
        for dual, expired, wire_rule, view, e_order in itertools.product((False, True), (False, True), wires, views,
                                                                          e_orders):
            base = _state(values, grouped, dual)
            for prior in prior_pendings(base, e_order).values():
                out.append(Config(name, base, dual, expired, prior, wire_rule, view, e_order))
    return out


# ---- (a) the exhaustive pairwise sweep --------------------------------------------------------------------------------
class Sweep(NamedTuple):
    combinations: int
    outcomes: Counter
    invalid: list
    order_dependent: list
    rule_failures: list


@lru_cache(maxsize=None)
def pairwise() -> Sweep:
    """Every ordered pair of operation variants × every base configuration. Each pair is also evaluated in the other
    order (permutation invariance)."""
    outcomes, invalid, order_dependent, rule_failures, total = Counter(), [], [], [], 0
    for cfg in configs():
        for first, second in itertools.combinations_with_replacement(OPS, 2):
            r = respond(cfg, (first, second))
            total += 1 if first == second else 2
            outcomes[r.kind] += 1 if first == second else 2
            bad = violations(cfg, (first, second), r)
            if bad:
                invalid.append((cfg.profile, cfg.dual, cfg.expired, cfg.pending and cfg.pending.reason, cfg.wire,
                                first, second, r.kind, r.code, bad))
            if first != second and respond(cfg, (second, first)) != r:
                order_dependent.append((cfg.profile, first, second))
            rule_failures += rule_pins(cfg, (first, second), r)
    return Sweep(total, outcomes, invalid, order_dependent, rule_failures)


def rule_pins(cfg: Config, ops, r: Response) -> list[tuple]:
    """The R3b rules themselves (not only representability), on the turns where no earlier row decides."""
    plain = (not cfg.expired and cfg.pending is None and r.clause.startswith("§6")
             and not any(op.kind in ("RELATIVE", "NO_SELECTION") for op in ops))
    if not plain:
        if cfg.expired and r.kind == "CLARIFY" and r.code != EXPIRED_CODE:
            return [("H1", ops, r.code)]
        return []
    found = causes(cfg.base, ops)
    out = []
    if len(found) > 1 and (r.kind, r.code, r.pending) != ("INFORM", found[0].code, None):  # U20
        out.append(("two causes ⇒ INFORM with the top code", ops, r.kind, r.code))
    for keep in (op for op in ops if op.kind == "KEEP"):  # U22
        if any(op.kind in WRITERS and op.slot == keep.slot for op in ops) and \
                "DELTA_SLOT_CONFLICT" not in {c.refusal for c in found}:
            out.append(("KEEP + writer of the same slot ⇒ DELTA_SLOT_CONFLICT", ops))
    return out


def test_the_operation_inventory_is_the_constructed_matrix():
    kinds = Counter(op.kind for op in OPS)
    assert {(op.kind, op.slot) for op in OPS if op not in LABEL_ITEMS} == {
        k for k, result in op_slot_matrix().items() if result == "OK"}
    assert kinds == {"SET": 66, "CLEAR": 29, "KEEP": 21, "SET_ORDER": 3, "REPLACE_UNIVERSE": 5, "RELATIVE": 1,
                     "NO_COMPARATOR": 1, "NO_SELECTION": 1}
    assert NO_COMPARATOR_CODE == "VALUE_WITHOUT_COMPARATOR" and NO_SELECTION_CODE == (
        "UNSUPPORTED_PROPOSAL:UNSUPPORTED_CONDITION")
    assert len(SINGLE) == 21 and set(RANK) == set(SINGLE)
    assert [code for code in PRECEDENCE] == ["TARGET_NOT_ADDRESSABLE", "DELTA_SLOT_CONFLICT", "KEEP_ON_EMPTY_SLOT",
                                             OPNA, LEAVES]
    assert {rule: code for rule, (_, code) in HOST.items() if rule in QUESTIONS} == {
        "H3": "SMMLV_YEAR_AMBIGUOUS", "H5": "BUYER_OR_PLACE_AFTER_ACTOR", "H6": "TERRITORY_VS_CARRIED_BUYER",
        "H7": "REGION_NOT_IN_LIST", "H9": "UNIT_AMBIGUOUS", "H10": "MONTH_YEAR_AMBIGUOUS"}
    assert wire_error(0, 1) == "CARDINALITY_WITHOUT_ORDER" and wire_error(1, 1) is None
    assert wire_error(2, 0) == "MULTIPLE_ORDER" and wire_error(1, 2) == "MULTIPLE_CARDINALITY"


def test_every_prior_pending_satisfies_the_law():
    for cfg in configs(e_orders=(True, False)):
        if cfg.pending is not None:
            p = cfg.pending
            assert not law_violations(cfg.base, p.frame_ops, p.candidates, p.answer_slots), (cfg.profile, p.reason)


def test_pairwise_every_combination_has_one_representable_response():
    sweep = pairwise()
    assert sweep.combinations == len(configs()) * len(OPS) ** 2
    assert sweep.invalid == [], sweep.invalid[:5]
    assert set(sweep.outcomes) == {"EXECUTE", "CLARIFY", "INFORM", "REASK", "DEPENDENCY"}
    assert sum(sweep.outcomes.values()) == sweep.combinations


def test_pairwise_the_response_does_not_depend_on_the_order_of_the_operations():
    assert pairwise().order_dependent == []


def test_pairwise_the_r3b_rules_hold():
    assert pairwise().rule_failures == []


def test_without_the_law_the_checker_finds_impossible_pendings():
    """Mutant: §8.4 and H1's restriction removed. The same checker must catch it, with the ZEUS witness among the
    cases: CLEAR(ORDER) with the CARDINALITY carried plus an unresolved SET BUYER."""
    found = Counter()
    for cfg in configs(wires=("current",)):
        if cfg.pending is not None or cfg.profile not in ("FULL", "MIN"):
            continue
        for pair in itertools.combinations_with_replacement(OPS, 2):
            for problem in violations(cfg, pair, respond(cfg, pair, law=False)):
                found[problem.split(":")[0]] += 1
    assert found["frame has a refusal cause"] and found["frame holds an open operation"]
    assert found["a pending other than CONTEXT_EXPIRED_CONFIRM on an expired anchor"]  # a refusal asks on it
    assert found["«continuar» would not execute"]
    full = next(c for c in configs(wires=("current",)) if c.profile == "FULL" and not c.dual and not c.expired
                and c.pending is None)
    zeus = (Op("CLEAR", "ORDER"), Op("SET", "BUYER/POSITIVE", "x", "unresolved"))
    assert violations(full, zeus, respond(full, zeus, law=False))
    assert respond(full, zeus) == Response("INFORM", "UNRESOLVED_VALUE:BUYER", clause="§6 3b")


def test_the_r3b_controls_answer_as_the_contract_says():
    full = {(c.dual, c.expired, c.wire, c.pending and c.pending.reason): c for c in configs() if c.profile == "FULL"}
    fresh, expired = full[(False, False, "current", None)], full[(False, True, "current", None)]
    # two causes at once ⇒ INFORM with the top code, no pending (U20)
    assert respond(fresh, (Op("CLEAR", "ORDER"), Op("KEEP", "BUYER/POSITIVE"), Op("SET", "BUYER/POSITIVE", "w"))) == \
        Response("INFORM", "DELTA_SLOT_CONFLICT", clause="§6 3b")
    # KEEP + SET of the same slot ⇒ DELTA_SLOT_CONFLICT, even with the same value (U22)
    same = respond(fresh, (Op("KEEP", "BUYER/POSITIVE"), Op("SET", "BUYER/POSITIVE", "x")))
    assert (same.kind, same.code) == ("CLARIFY", "DELTA_SLOT_CONFLICT")
    # an expired anchor whose «continuar» cannot execute ⇒ INFORM, no loop (H1)
    minimal = next(c for c in configs() if c.profile == "MIN" and c.expired and not c.dual and c.wire == "current")
    assert respond(minimal, (Op("SET", "TIME/POSITIVE", "x"), Op("KEEP", "BUYER/POSITIVE"))) == \
        Response("INFORM", "KEEP_ON_EMPTY_SLOT", clause="§6 3b")
    assert respond(expired, (Op("SET", "GEOGRAPHY/POSITIVE", "x", "asks:H7"),)) == \
        Response("INFORM", "REGION_NOT_IN_LIST", clause="H1")
    go = respond(expired, (Op("SET", "TIME/POSITIVE", "w"),))
    assert (go.kind, go.code, go.pending.candidates) == ("CLARIFY", EXPIRED_CODE, ((Op("SET", "TIME/POSITIVE", "w"),),))
    # «dame 20» with the R3-1 pending: the wire refuses it, row 4b, the pending intact (P1-2)
    r31 = full[(False, False, "current", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER")]
    assert respond(r31, (Op("SET", "CARDINALITY", "n"),)) == Response("INFORM", WIRE_INVALID, pending=r31.pending,
                                                                         clause="§7.1 4b")
    # selection by EXACT semantic identity (U22, R3c U26): «por fecha» (with or without «mantén el límite») picks its
    # SET_ORDER; «quítales también el límite» is C1's whole patch; a part of a candidate selects nothing (row 7);
    # an edit of a slot outside answer_slots is row 5
    by_date = respond(r31, (Op("SET_ORDER", "ORDER", OrderKey.DATE.value),))
    assert by_date.kind == "EXECUTE" and by_date.state.slots[IDX["CARDINALITY"]] == ("n0",)
    assert respond(r31, (Op("SET_ORDER", "ORDER", OrderKey.DATE.value), Op("KEEP", "CARDINALITY"))) == by_date
    limit = respond(r31, (Op("CLEAR", "CARDINALITY"), Op("CLEAR", "ORDER")))
    assert limit.kind == "EXECUTE" and not occupied(limit.state, "ORDER") and not occupied(limit.state, "CARDINALITY")
    assert respond(r31, (Op("CLEAR", "CARDINALITY"),)).code == "ANSWER_UNRESOLVED"
    assert respond(r31, (Op("CLEAR", "CARDINALITY"), Op("SET", "TIME/POSITIVE", "w"))).code == "ANSWER_EDITS_OTHER_SLOT"
    assert respond(r31, (Op("SET_ORDER", "ORDER", OrderKey.VALUE.value),)).code == "ANSWER_UNRESOLVED"


def test_r3d_a_step2_conflict_in_an_answer_never_executes():
    """ZEUS on 109095fe (zeus_r3c/probe_keepset.py): KEEP + a writer of the same slot is a §6 step-2 conflict decided
    on P before normalizing; with a pending it is row 7, for every pending and every such two-operation answer."""
    seen = Counter()
    for cfg in configs():
        if cfg.pending is None or cfg.expired:
            continue
        for first, second in itertools.combinations(OPS, 2):
            pair = (first, second)
            if {op.kind for op in pair} & {"KEEP"} and len({op.slot for op in pair}) == 1 and first.kind != second.kind \
                    and not set(pair) & set(LABEL_ITEMS):
                r = respond(cfg, pair)
                seen[r.kind] += 1
                assert r.kind != "EXECUTE", (cfg.profile, cfg.pending.reason, pair)
    assert seen["REASK"] and not seen["EXECUTE"]


def test_each_fail_closed_rule_is_pinned_by_a_concrete_combination():
    """ZEUS model mutants (zeus_r3b/model_mut.py) that the sweep alone let live: each rule answers as §6 says."""
    pick = {(c.profile, c.dual, c.expired, c.wire, c.pending is None): c for c in configs()}
    minimal, full, multi = (pick[(name, False, False, "current", True)] for name in ("MIN", "FULL", "MULTI"))
    # F16 fail-closed (§6.5, R3-4): negating a value among several carried positives is not accredited
    f16 = (Op("SET", "TOPIC/NEGATIVE", "x"),)
    assert [c.code for c in causes(multi.base, f16)] == ["UNRESOLVED_VALUE:TOPIC"]
    assert (respond(multi, f16).kind, respond(multi, f16).code) == ("CLARIFY", "UNRESOLVED_VALUE:TOPIC")
    # P-3: +X and −X created by the same patch, nothing carried: never resolved by F16
    p3 = (Op("SET", "BUYER/POSITIVE", "x"), Op("SET", "BUYER/NEGATIVE", "x"))
    assert [c.code for c in causes(minimal.base, p3)] == ["UNRESOLVED_VALUE:BUYER"]
    assert (respond(minimal, p3).kind, respond(minimal, p3).code) == ("CLARIFY", "UNRESOLVED_VALUE:BUYER")
    # A8.1 with non-comparable units (§15.2): against a carried opposite bound, fail-closed
    a81 = (Op("SET", "VALUE/lower", "v", "noncomparable"),)
    assert [c.code for c in causes(full.base, a81)] == ["UNRESOLVED_VALUE:VALUE"]
    assert (respond(full, a81).kind, respond(full, a81).code) == ("CLARIFY", "UNRESOLVED_VALUE:VALUE")
    # two non-accredited operations are two causes (D.3 c): INFORM with the first in §4.2 slot order
    two = (Op("SET", "TIME/POSITIVE", "x", "unresolved"), Op("SET", "BUYER/POSITIVE", "x", "unresolved"))
    assert [c.code for c in causes(minimal.base, two)] == ["UNRESOLVED_VALUE:BUYER", "UNRESOLVED_VALUE:TIME"]
    assert respond(minimal, two) == Response("INFORM", "UNRESOLVED_VALUE:BUYER", clause="§6 3b")


def test_r3c_zeus_witnesses():
    """ZEUS on 7b30b221 (zeus_r3b/h6.py, tna.py, vwc.py): each answer reaches its candidate; no dead pending."""
    base = _state({"TOPIC/POSITIVE": ("x",), "BUYER/POSITIVE": ("x",), "TIME/POSITIVE": ("x",)})
    h6 = _pending(base, HOST["H6"][1], (Op("SET", "GEOGRAPHY/POSITIVE", "x", "asks2:H6"),), (), H6_CANDIDATES)
    cfg = Config("H6", base, False, False, h6, "current")
    change_buyer = (Op("CLEAR", "BUYER/POSITIVE"), Op("SET", "GEOGRAPHY/POSITIVE", "x"))
    both = apply(base, (Op("SET", "GEOGRAPHY/POSITIVE", "x"),))
    assert respond(cfg, change_buyer).state == apply(base, change_buyer)  # A: only by repeating its two ops (M2A-4)
    said = (Op("KEEP", "BUYER/POSITIVE"), Op("SET", "GEOGRAPHY/POSITIVE", "x"))  # «buscar con ambos», «mantén … y en …»
    assert respond(cfg, said) == Response("EXECUTE", state=both, clause="§8.3 4")
    resent = (Op("SET", "GEOGRAPHY/POSITIVE", "x"),)  # the asking turn again: F24, the product does not choose (R3d)
    assert respond(cfg, resent) == Response("REASK", "ANSWER_UNRESOLVED", pending=h6, clause="§8.3 7 (F24)")
    assert respond(cfg, (Op("KEEP", "BUYER/POSITIVE"),)).code == "PENDING_UNANSWERED"  # empty after normalizing: row 8
    assert respond(cfg, (Op("KEEP", "BUYER/POSITIVE"), Op("CLEAR", "BUYER/POSITIVE"),
                         Op("SET", "GEOGRAPHY/POSITIVE", "x"))).clause == "§8.3 7 (§6 paso 2)"  # conflict before
    orc = next(c for c in configs() if c.profile == "OR" and not c.dual and not c.expired and c.wire == "normal"
               and c.pending is None)
    assert respond(orc, (Op("SET", "TOPIC/POSITIVE", "w"),)) == Response(
        "INFORM", "TARGET_NOT_ADDRESSABLE", clause="§6 3b + §8.4")  # no answer could ever execute (§8.4, 3)
    fresh = next(c for c in configs() if c.profile == "MIN" and not c.dual and not c.expired and c.wire == "current"
                 and c.pending is None)
    vwc = respond(fresh, (LABEL_ITEMS[1],)).pending
    assert vwc.answer_slots == {"VALUE/lower", "VALUE/upper"}
    assert all(respond(fresh._replace(pending=vwc), (Op("SET", side, "v"),)).kind == "EXECUTE"
               for side in ("VALUE/lower", "VALUE/upper"))


def test_the_u25_closures_answer_as_the_contract_says():
    """U25: value_without_comparator passes through the refusal table; no_selection without a pending is row 4b."""
    full = {(c.dual, c.expired, c.wire, c.pending and c.pending.reason): c for c in configs() if c.profile == "FULL"}
    fresh, expired = full[(False, False, "current", None)], full[(False, True, "current", None)]
    r31 = full[(False, False, "current", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER")]
    no_comparator, no_selection = LABEL_ITEMS[1], LABEL_ITEMS[2]
    asked = respond(fresh, (no_comparator, Op("SET", "TIME/POSITIVE", "w")))
    assert (asked.kind, asked.code, asked.pending.frame_ops) == ("CLARIFY", "VALUE_WITHOUT_COMPARATOR",
                                                                  (Op("SET", "TIME/POSITIVE", "w"),))
    assert asked.pending.answer_slots == {"VALUE/lower", "VALUE/upper"}  # §8.4 (R3c)
    assert respond(expired, (no_comparator,)) == Response("INFORM", "VALUE_WITHOUT_COMPARATOR", clause="§6 3b")
    assert respond(fresh, (no_comparator, Op("CLEAR", "ORDER"))).kind == "INFORM"  # two causes
    assert respond(fresh, (no_selection,)) == Response("INFORM", NO_SELECTION_CODE, clause="§7.1 4b")
    assert respond(expired, (no_selection,)) == Response("INFORM", NO_SELECTION_CODE, clause="§7.1 4b")
    assert respond(r31, (no_selection,)) == Response("REASK", "OPTION_NOT_CHOSEN", pending=r31.pending,
                                                     clause="§8.3 6")
    assert respond(r31, (no_selection, Op("CLEAR", "CARDINALITY"), Op("SET", "TIME/POSITIVE", "w"))).code == (
        "ANSWER_EDITS_OTHER_SLOT")  # §8.3: row 5 comes before row 6


# ---- (b) Hypothesis: N-operation patches, same property, plus permutation invariance ---------------------------------
ALL_CONFIGS = configs(views=(False, True), e_orders=(True, False))


@settings(max_examples=3000, derandomize=True, deadline=None)
@given(cfg=st.sampled_from(ALL_CONFIGS), ops=st.lists(st.sampled_from(OPS), min_size=1, max_size=6), data=st.data())
def test_n_operation_patches_are_representable_and_order_free(cfg, ops, data):
    r = respond(cfg, ops)
    event(f"{r.kind} {r.clause}")  # --hypothesis-show-statistics prints the outcomes by class
    assert violations(cfg, ops, r) == []
    assert respond(cfg, data.draw(st.permutations(ops))) == r


def render() -> str:
    sweep = pairwise()
    lines = [f"configuraciones de base: {len(configs())}; variantes de operación: {len(OPS)}",
             f"combinaciones (pares ordenados × configuraciones): {sweep.combinations}",
             "desenlaces: " + ", ".join(f"{k} {sweep.outcomes[k]}" for k in sorted(sweep.outcomes)),
             f"inválidas: {len(sweep.invalid)}; dependientes del orden: {len(sweep.order_dependent)}; "
             f"reglas R3b violadas: {len(sweep.rule_failures)}"]
    return "\n".join(lines)


if __name__ == "__main__":
    print(render())
