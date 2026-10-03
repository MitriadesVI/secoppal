"""M2A-4 gates of §8.4: sweep the answers to ALL the pendings, and an answer composes on the frame, never the base.

The contract names two gates for this step (§8.4, U26, U27) and the symbolic model of the contract cannot discharge them:
its pendings have ``frame`` = the base, and it holds no typed values. This module discharges them on the TYPED modules.

1. **Differential against the contract's own model** (``tests/test_m2a_contract_r3_multiop``): every pending the model can
   make (its prior pendings and every ``CLARIFY`` its ``respond`` raises on a single operation or a pair) is lifted to a
   typed ``PendingClarification`` (tokens → typed values, the same canonical value for the same token) and answered with
   every single operation of the model's inventory, every candidate whole, with each KEEP of filler, plus the asking patch
   itself (F24) and its fillers, plus a pair sample. The model's ``answer`` and ``respond`` of M2A-4 must say the same
   thing: disposition, code and resulting slot values. ``CONTEXT_EXPIRED_CONFIRM`` is out of the model's population (it
   drops the pending with an expired anchor) and is swept on its own below (M2A4-D2).
2. **The pendings M2A-3 creates**, generated over rich bases (Hypothesis): the creation never degrades, every candidate is
   reachable by event, and the answers (each candidate whole, with each filler, a part of it, an edit of another slot, the
   asking patch) obey the invariants of §8.3: F24, F25, F29, ``KEEP`` identity, ``base_trace_id`` of the asking turn,
   the pending kept intact with ``asks + 1``, and a §19 dependency is never lost by passing through the frame
   (``laundering``: the answer on the frame against ``frame_ops`` + answer from the base; M2A4-D8).
3. **Every ``CONTEXT_EXPIRED_CONFIRM`` pending** M2A-3 creates × every answer in language, with ``KEEP`` of filler: 0
   ``EXECUTE`` over the expired anchor (M2A4-D2; the ZEUS probe of P1-2 made 55,562 of 73,418).
4. ``slot_conflicts`` is exactly the ``DELTA_SLOT_CONFLICT`` cause of §6 step 2.

The mutants that matter to these gates (``answer_on_base_not_frame``, F24, normalization, D2) are also judged HERE.
"""
from __future__ import annotations

import itertools
import sys
from collections import Counter
from dataclasses import replace
from functools import lru_cache

import pytest
from hypothesis import HealthCheck, find, given, settings
from hypothesis.errors import NoSuchExample
from hypothesis import strategies as st

import app.core.foundation_accepted as fa
import app.core.foundation_delta as fd
import app.core.foundation_delta_outcome as fo
import tests.test_foundation_pending as T
import tests.test_m2a_contract_r3_multiop as M
from app.core.foundation_authority_values import Comparator, MoneyValue, OrderDirection, OrderKey, OrderValue
from app.core.foundation_operations import Subject
from app.core.semantic_dimensions import SemanticDimension as D
from tests.test_foundation_delta_properties import st_case

PROPS = settings(max_examples=120, deadline=None, derandomize=True,
                 suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large])
MODEL_BASES = {}  # original synthetic cfg.base retained at generation; never reverse-engineered from frame
MODEL_SLOT = M.SLOT  # key -> typed Slot, generated from the code by the model's own module
NO_SELECTION = M.LABEL_ITEMS[2]  # the A.6 label ``no_selection`` (§8.3 row 6)


# ---- lifting the model's symbols to typed values -------------------------------------------------------------------
BOUNDS = {"v0": MoneyValue(100, Comparator.GTE), "v1": MoneyValue(900, Comparator.LT)}


def lift_value(key: str, token: str):
    """The typed value a model token stands for in slot ``key``: the same token is the same canonical value; a new bound ``v``
    is chosen so that no interval it makes is empty (value arithmetic is outside the model)."""
    slot = MODEL_SLOT[key]
    if slot.dimension is D.VALUE:
        if token == "v":
            return MoneyValue(500, Comparator.GTE) if key.endswith("lower") else MoneyValue(700, Comparator.LT)
        return BOUNDS[token]
    if slot.dimension is D.ORDER:
        return OrderValue(OrderKey(token), OrderDirection.DESC)
    if slot.dimension is D.CARDINALITY:
        return T.TOP_10 if token == "n0" else T.TOP_20
    return T.typed_value(slot.dimension, token, excluded=slot.polarity is T.NEG)


def lift_op(op: "M.Op", i: int = 0) -> fa.PatchOp:
    slot = MODEL_SLOT[op.slot]
    if op.kind == "CLEAR":
        return T.clear_op(slot, i)
    if op.kind == "KEEP":
        return T.keep_op(slot, i)
    if op.kind == "REPLACE_UNIVERSE":
        kind = fa.UniverseKind.OPPORTUNITIES if op.value == "opportunities" else fa.UniverseKind.RECORDS
        subject = Subject.CONTRACTS if op.value == "opportunities" else Subject(op.value)
        return fa.PatchOp(fa.PatchOpKind.REPLACE_UNIVERSE, fa.UNIVERSE_SLOT, None, fa.UniverseTarget(subject, kind),
                          T._org(i))
    cond = T.condition(slot.dimension, lift_value(op.slot, op.value), f"p{i + 1}", T._org(i))
    kind = fa.PatchOpKind.SET_ORDER if op.kind == "SET_ORDER" else fa.PatchOpKind.SET
    return fa.PatchOp(kind, cond.slot, cond, None, cond.origin)


LABELS = frozenset({"RELATIVE", "NO_COMPARATOR", "NO_SELECTION"})  # A.6 labels: not PatchOps, nothing typed to lift


def lift_ops(ops) -> tuple[fa.PatchOp, ...]:
    return tuple(lift_op(op, i) for i, op in enumerate(op for op in ops if op.kind not in LABELS))


def lift_state(state: "M.State") -> fa.AcceptedInterpretation:
    subjects = (Subject.PROCESSES, Subject.CONTRACTS) if len(state.universe) == 2 else (Subject(state.universe[0]),)
    requests = tuple(T.request(s, f"r{i}") for i, s in enumerate(subjects, 1))
    rids = tuple(r.request_id for r in requests)
    conditions, n = [], 0
    for key, tokens in zip(M.CONDITION_SLOTS, state.slots):
        for token in tokens:
            slot = MODEL_SLOT[key]
            n += 1
            conditions.append(T.condition(slot.dimension, lift_value(key, token), f"c{n}", T.origin("t0", 3 * n, T.RAW_BASE),
                                          rids))
    return fa.canonical_interpretation(requests, tuple(conditions), None, (), None, "t-root")


def typed_only(ops) -> tuple["M.Op", ...]:
    """The operations that exist as a typed ``PatchOp``: a resolved one, and the open operation of an H5/H6 question (the
    producer's provisional label: the authority resolved its value, only its reading is ambiguous: status ``asks2:…``).
    The model also carries the A.6 labels and the operations an authority could not resolve (status ``asks:…`` /
    ``unresolved``): there is nothing typed to lift for them, so they are no part of an answer nor of the asking patch F24
    compares with (§8.3: «las mismas operaciones»)."""
    return tuple(o._replace(status="ok") if o.status.startswith("asks2:") else o for o in ops
                 if (o.status == "ok" or o.status.startswith("asks2:")) and o.kind not in LABELS)


def repeats_asked_unresolved(mp: "M.Pending", ops) -> bool:
    """A model artifact, left out of the comparison: the model's identity ignores the STATUS of an operation, so an
    UNRESOLVED answer equal to an operation the asking turn RESOLVED counts as «the same patch» (F24). A deterministic
    authority never resolves a value at one turn and fails on it at the next, so the typed world has no such answer."""
    asked = {(a.kind, a.slot, a.value) for a in mp.asked}
    return any(o.status == "unresolved" and (o.kind, o.slot, o.value) in asked for o in ops)


def lift_turn(ops, labelled: bool = False):
    """The turn of an answer in the model's symbols: its resolved operations, the slots it says with a value nobody
    resolved (status ``unresolved``), and whether it carries the ``no_selection`` label."""
    typed = tuple(o for o in ops if o.status == "ok" and o.kind not in LABELS)
    said = frozenset(MODEL_SLOT[o.slot] for o in ops if o.status == "unresolved")
    return T.P.PendingTurn(T.facts(), lift_ops(typed), no_selection=labelled, unresolved=said)


def typed_pending(mp: "M.Pending") -> "M.Pending":
    typed = mp._replace(asked=typed_only(mp.asked))
    MODEL_BASES[typed] = MODEL_BASES[mp]
    return typed


def lift_pending(mp: "M.Pending") -> fa.PendingClarification:
    # These are NEW typed synthetic histories from the model's original generation
    # inputs. The independent model still owns expected answer disposition/code/values.
    base = lift_state(MODEL_BASES[mp])
    frame_ops, asked_ops = lift_ops(mp.frame_ops), lift_ops(mp.asked)
    if frame_ops:
        composed = fd.compose_ops(base, frame_ops, base_trace_id="t0")
        assert isinstance(composed, fd.Composed)
        frame = composed.effective
    else:
        frame = base
    assert values_by_slot(frame) == model_values(mp.frame)
    candidates = tuple(fa.CandidatePatch(f"c{i}", f"k{i}", fa.SemanticPatch(lift_ops(c), fa.PatchOriginKind.TEXT))
                       for i, c in enumerate(mp.candidates, 1))
    return fa.PendingClarification("pending-t1", "t1", T.T0, T.T0 + 30 * T.MINUTE, 1, frame, "t0", None, None,
                                   mp.reason, candidates, frozenset(MODEL_SLOT[k] for k in mp.answer_slots),
                                   frame_ops=frame_ops, asked_ops=asked_ops)


def values_by_slot(effective: fa.AcceptedInterpretation) -> dict:
    out: dict[str, list] = {}
    for c in effective.conditions:
        out.setdefault(c.slot.key, []).append(c.value)
    return {k: sorted(v) for k, v in out.items()}


def model_values(state: "M.State") -> dict:
    lifted = {}
    for key, tokens in zip(M.CONDITION_SLOTS, state.slots):
        if tokens:
            lifted[key] = sorted(lift_value(key, t).encode() for t in tokens)
    return lifted


# ---- the model's population of pendings ---------------------------------------------------------------------------
def _supported(state: "M.State") -> bool:
    return not state.or_grouped  # OR/NOT trees are swept by the properties of M2A-3 (TARGET_NOT_ADDRESSABLE)


@lru_cache(maxsize=None)
def model_pendings() -> tuple:
    """Distinct pendings of the model (frame, reason, candidates, answer slots), up to two asking patches each: the prior
    pendings of every configuration and every CLARIFY ``respond`` makes on a single operation or a pair of the inventory."""
    seen: dict[tuple, list] = {}
    plain = [c for c in M.configs(wires=("normal",)) if c.pending is None and not c.expired and _supported(c.base)]
    singles = [o for o in M.OPS if o not in M.LABEL_ITEMS and o.status in ("ok", "unresolved")]
    asking = [o for o in M.OPS if o.status.startswith("asks") or o in M.LABEL_ITEMS or o.status == "unresolved"
              or o.kind in ("CLEAR", "KEEP")]
    for cfg in M.configs(wires=("normal",)):
        if cfg.pending is not None and not cfg.expired and _supported(cfg.base) and not cfg.dual:
            seen.setdefault((cfg.pending.reason, cfg.pending.frame, cfg.pending.candidates, cfg.pending.answer_slots),
                            []).append(cfg.pending)
            MODEL_BASES.setdefault(cfg.pending, cfg.base)
    for cfg in plain:
        if cfg.dual or cfg.profile in ("MULTI", "ORDER_ONLY"):
            continue
        for ops in itertools.chain(((o,) for o in singles), itertools.product(asking, singles)):
            r = M.respond(cfg, ops)
            if r.kind == "CLARIFY" and r.pending is not None and _supported(r.pending.frame):
                key = (r.pending.reason, r.pending.frame, r.pending.candidates, r.pending.answer_slots)
                if len(seen.setdefault(key, [])) < 2 and r.pending not in seen[key]:
                    seen[key].append(r.pending)
                    MODEL_BASES.setdefault(r.pending, cfg.base)
    per_shape: dict[tuple, list] = {}  # a few pendings of each (reason, answer slots): frames and askers vary
    for (reason, frame, candidates, answer_slots), group in seen.items():
        bucket = per_shape.setdefault((reason, answer_slots, bool(candidates)), [])
        if len(bucket) < 3:
            bucket.append(group[0])
    return tuple(p for group in per_shape.values() for p in group)


def answer_pool(mp: "M.Pending") -> list[tuple["M.Op", ...]]:
    """The answers swept for a pending, in the model's own symbols: every single operation of the inventory, every
    candidate whole and with each KEEP of filler or an edit of another slot, the asking patch and its fillers."""
    ok = [o for o in M.OPS if o.status == "ok" and o not in M.LABEL_ITEMS]
    keeps = [o for o in ok if o.kind == "KEEP"]
    sets = [o for o in ok if o.kind == "SET"]
    pool: list[tuple] = [(o,) for o in ok] + [()]
    patches = [tuple(c) for c in mp.candidates] + ([tuple(typed_only(mp.asked))] if typed_only(mp.asked) else [])
    for patch in patches:
        pool += [patch, patch[:-1]] + [(*patch, k) for k in keeps] + [(*patch, s) for s in sets[:6]]
    inside = [o for o in ok if M.covered((o,)) & mp.answer_slots]
    pool += [pair for pair in itertools.product(inside[:5], ok[::4]) if pair[0] != pair[1]]
    said = [o for o in M.OPS if o.status == "unresolved" and o.kind in ("SET", "SET_ORDER")]  # a value nobody resolved
    pool += [(o,) for o in said] + [pair for pair in itertools.product(inside[:5], said[::3])]
    return [ops for ops in dict.fromkeys(pool) if all(o.status in ("ok", "unresolved") and o.kind not in LABELS
                                                      for o in ops)]


def compare(model_response, decision, kept_pending) -> list[str]:
    """The model's answer against the typed one: disposition, code, resulting slot values (and the pending kept)."""
    kind = {"PENDING_ANSWER": "EXECUTE", "REASK": "REASK", "DEPENDENCY": "DEPENDENCY"}.get(decision.kind.value,
                                                                                         decision.kind.value)
    if kind != model_response.kind:
        return [f"disposition {kind} != model {model_response.kind}"]
    if kind == "REASK":
        if decision.code != model_response.code:
            return [f"code {decision.code} != model {model_response.code}"]
        if decision.pending is None or replace(decision.pending, asks=1) != kept_pending:
            return ["the pending is not kept whole"]
    elif kind == "DEPENDENCY":
        if decision.dependency.dep_id != model_response.dependency:
            return [f"dependency {decision.dependency.dep_id} != model {model_response.dependency}"]
    else:
        effective = decision.answer.composed.effective
        if values_by_slot(effective) != model_values(model_response.state):
            return [f"state {values_by_slot(effective)} != model {model_values(model_response.state)}"]
        if tuple(sorted(r.subject.value for r in effective.requests)) != tuple(sorted(model_response.state.universe)):
            return ["universe differs"]
    return []


@lru_cache(maxsize=None)
def differential() -> tuple[int, Counter, list]:
    """``(answers swept, Counter of (reason, model disposition, model code), mismatches)``."""
    total, outcomes, problems = 0, Counter(), []
    for mp in map(typed_pending, model_pendings()):
        pending = lift_pending(mp)
        cfg = M.Config("sweep", mp.frame, len(mp.frame.universe) == 2, False, mp, "normal")
        asked = lift_ops(mp.asked)
        pool = [ops for ops in answer_pool(mp) if not repeats_asked_unresolved(mp, ops)]
        for ops in [*pool, (NO_SELECTION,), (NO_SELECTION, *pool[0]), (NO_SELECTION, *pool[len(pool) // 2])]:
            labelled = any(o.kind == "NO_SELECTION" for o in ops)
            plain = tuple(o for o in ops if o.kind != "NO_SELECTION")
            expected = M.answer(cfg, mp, ops)
            got = T.P.respond(pending, lift_turn(plain, labelled), trace_id="t2", now=T.T0 + T.MINUTE,
                              base=lift_state(MODEL_BASES[mp]), base_trace_id="t0")
            total += 1
            outcomes[(mp.reason, expected.kind, expected.code)] += 1
            bad = compare(expected, got, pending)
            if bad:
                problems.append((mp.reason, [(o.kind, o.slot, o.value) for o in ops], bad))
    return total, outcomes, problems


def test_the_model_population_is_what_the_sweep_claims_to_cover():
    pendings = model_pendings()
    reasons = Counter(p.reason for p in pendings)
    assert len(pendings) >= 40, reasons
    assert {"BUYER_OR_PLACE_AFTER_ACTOR", "MONTH_YEAR_AMBIGUOUS", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER",
            "TERRITORY_VS_CARRIED_BUYER", "EMPTY_SEARCH_AFTER_DELTA"} <= set(reasons), reasons
    assert any(p.candidates for p in pendings) and any(not p.candidates for p in pendings)


def test_the_typed_answers_are_the_models_answers_for_every_pending_of_the_model():
    total, outcomes, problems = differential()
    assert total >= 50000, total
    assert problems == [], problems[:5]


def test_the_differential_reaches_every_row_of_8_3_on_every_kind_of_pending():
    """Not vacuous: for the kinds of pending that matter, the model's answers include an execution, F24, F25, F29 and a
    selection that fails; so a typed module that is wrong on any of them disagrees with the model somewhere."""
    _, outcomes, _ = differential()
    for reason in ("BUYER_OR_PLACE_AFTER_ACTOR", "MONTH_YEAR_AMBIGUOUS", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER",
                   "TERRITORY_VS_CARRIED_BUYER", "EMPTY_SEARCH_AFTER_DELTA"):
        seen = {(kind, code) for (r, kind, code), n in outcomes.items() if r == reason}
        assert ("EXECUTE", None) in seen, (reason, seen)
        for code in ("ANSWER_UNRESOLVED", "ANSWER_EDITS_OTHER_SLOT", "PENDING_UNANSWERED", "OPTION_NOT_CHOSEN"):
            assert ("REASK", code) in seen, (reason, code, seen)
    assert any(kind == "DEPENDENCY" for (_, kind, _), n in outcomes.items())


# ---- the pendings M2A-3 creates, over rich bases --------------------------------------------------------------------
def _created(case):
    """``(base, outcome, creation)`` for a case whose delta asks (else ``None``)."""
    base, patch, expired = case
    outcome = fo.delta_outcome(base, patch, base_trace_id="t0", anchor_expired=expired)
    if outcome.disposition is not fo.DeltaDisposition.CLARIFY:
        return None
    return base, outcome, T.P.pending_from_question(outcome.pending, base=base, base_trace_id="t0", asked_trace_id="t1",
                                                    now=T.T0)


def _keeps(frame: fa.AcceptedInterpretation, start: int = 20) -> list[fa.PatchOp]:
    slots = [c.slot for c in frame.conditions] + [fa.UNIVERSE_SLOT]
    return [T.keep_op(s, start + i) for i, s in enumerate(slots)]


def _answers(pending, asked_ops, frame):
    """Answers in language: each candidate whole, with each KEEP of filler, a part of it, an edit of another slot, the
    asking patch and its fillers, and each single KEEP / CLEAR of a slot the frame holds."""
    keeps = _keeps(frame)
    out = [(), *((k,) for k in keeps), *((T.clear_op(c.slot, 30),) for c in frame.conditions)]
    patches = [tuple(c.patch.ops) for c in pending.candidate_patches] + ([tuple(asked_ops)] if asked_ops else [])
    for patch in patches:
        said = reissued(patch)  # what the user SAYS: the same operations, never the same objects (origin, ids differ)
        out += [patch, patch[:-1], *((*patch, k) for k in keeps), (*patch, T.set_op(D.TIME, "x", i=40)),
                said, *((*said, k) for k in keeps)]
    return out


def reissued(ops) -> tuple[fa.PatchOp, ...]:
    """The same operations as new ``PatchOp``s of another turn (same identity, other origin and condition ids)."""
    out = []
    for i, op in enumerate(ops):
        org = T.origin("t9", 50 + 3 * i, T.RAW_TURN)
        condition = None if op.condition is None else replace(op.condition, condition_id=f"s{i}", origin=org)
        out.append(fa.PatchOp(op.op, op.slot, condition, op.universe, org))
    return tuple(out)


def spec_row(pending, ops, asked_ops):
    """§8.3 «Selección», written from the contract's words with another shape than the module's (counting touches rather
    than comparing the others): ``(row, candidate_id)`` for an answer in language to ``pending`` (row 4 means: composed)."""
    touches = Counter(key for op in ops for key in fd.covered_keys((op,)))
    if _spec_conflict(ops):
        return 7, None
    if ops and sorted(_spec_identity(o) for o in ops) == sorted(_spec_identity(o) for o in asked_ops):
        return 7, None
    present = {c.slot.key for c in pending.frame.conditions} | {"UNIVERSE"} if pending.frame is not None else {"UNIVERSE"}
    dropped = [o for o in ops if not (o.op is fa.PatchOpKind.KEEP and o.slot.key in present and touches[o.slot.key] == 1)]
    kept = dropped if pending.candidate_patches else list(ops)  # what is COMPOSED loses them only with candidates (U27)
    # M2A4-D20 (HERA, ZEUS P1-1; narrowed by D26): with candidates, F24 on the answer once its identity KEEPs go, and a PART of
    # the asking patch tells no candidate apart. Without them the sealed codes of rows 5 and 8 stand.
    if pending.candidate_patches and kept and _is_part_of(kept, asked_ops):
        return 7, None
    touched = {key for o in kept for key in fd.covered_keys((o,))}
    inside = {s.key for slot in pending.answer_slots for s in fd.covered(slot)}
    if not touched & inside:
        return 8, None
    if touched - inside:
        return 5, None
    if pending.reason == "CONTEXT_EXPIRED_CONFIRM":
        return 7, None
    if pending.candidate_patches:
        chosen = [c for c in pending.candidate_patches
                  if sorted(_spec_identity(o) for o in c.patch.ops) == sorted(_spec_identity(o) for o in kept)]
        return (4, chosen[0].candidate_id) if len(chosen) == 1 else (7, None)
    return 4, None


def _is_part_of(ops, asked) -> bool:
    """Every operation of ``ops`` is one of ``asked``, each asked operation used once (a multiset, written by crossing off)."""
    remaining = [_spec_identity(o) for o in asked]
    for identity in (_spec_identity(o) for o in ops):
        if identity not in remaining:
            return False
        remaining.remove(identity)
    return True


def _spec_identity(op):
    value = op.condition.value if op.condition is not None else (
        (op.universe.subject.value, op.universe.universe_kind.value) if op.universe is not None else ())
    return op.op.value, op.slot.key, tuple(value)


def _spec_conflict(ops) -> bool:
    for key in {k for op in ops for k in fd.covered_keys((op,))}:
        writers = [o for o in ops if o.op in fd.WRITERS and o.slot.key == key]
        keeps = [o for o in ops if o.op is fa.PatchOpKind.KEEP and o.slot.key == key]
        clears = [o for o in ops if o.op is fa.PatchOpKind.CLEAR and key in fd.covered_keys((o,))]
        if len(writers) >= 2 or (keeps and (writers or clears)):
            return True
    return False


def laundering(base, outcome, made) -> list[str]:
    """§19 is fail-closed (M2A4-D8): the answer composed on the frame must see every dependency the SAME answer raises when
    ``frame_ops`` + the answer are composed from the base (when that joint patch composes)."""
    frame_ops = outcome.pending.frame_ops
    joint = fd.compose_ops(base, (*frame_ops, *made.patch), base_trace_id="t0")
    if not isinstance(joint, fd.Composed):
        return []
    whole = {f.dependency for f in fo.host_findings(base, (*frame_ops, *made.patch), joint) if f.dependency is not None}
    seen = {f.dependency for f in fo.host_findings(outcome.pending.frame, made.patch, made.composed)
            if f.dependency is not None}
    return [f"§19 an answer launders {sorted(d.dep_id for d in whole - seen)} through the frame"] if whole - seen else []


def answer_problems(P, pending, creation, base, outcome, answers) -> list[str]:
    out, asked = [], creation.pending.asked_ops
    for ops in answers:
        d = T.respond(pending, T.P.PendingTurn(T.facts(), tuple(ops)), base=base, base_trace_id="t0")
        row, chosen = spec_row(pending, ops, asked)
        got = (d.answer.row if d.answer is not None else None)
        if d.kind.value == "PENDING_ANSWER":
            made = d.answer
            if row != 4 or (pending.candidate_patches and made.candidate_id != chosen):
                out.append(f"executed an answer the contract sends to row {row}: {[(o.op.value, o.slot.key) for o in ops]}")
            if made.composed.effective.base_trace_id != pending.asked_trace_id:
                out.append("§3.1: the effective does not name the asking turn as its base")
            if pending.candidate_patches:
                whole = next(c.patch.ops for c in pending.candidate_patches if c.candidate_id == chosen)
                if tuple(made.patch) != tuple(whole):
                    out.append("§8.3 «Selección» 5: what is composed is not the candidate's WHOLE patch")
            again = fd.compose_ops(pending.frame, made.patch, base_trace_id=pending.asked_trace_id)
            if not isinstance(again, fd.Composed) or again.effective != made.composed.effective:
                out.append("§8.4: the effective is not the patch composed on the frame")
            out += laundering(base, outcome, made)
        elif d.kind.value == "REASK":
            if row == 4 or d.answer.row != row and not (row == 4):
                if d.answer.row != 7 or row not in (4, 7):
                    out.append(f"row {d.answer.row} but the contract says {row}: {[(o.op.value, o.slot.key) for o in ops]}")
            if replace(d.pending, asks=pending.asks) != pending:
                out.append("a REASK changed more than asks")
        elif d.kind.value == "DEPENDENCY":
            if row != 4:
                out.append(f"a dependency for an answer that never composes (row {row})")
        else:
            out.append(f"an answer in language produced {d.kind.value} (row {got})")
    return out


def _case_problems(P, case) -> list[str]:
    got = _created(case)
    if got is None:
        return []
    base, outcome, creation = got
    if creation.disposition.value != "CLARIFY":
        return [f"M2A-3 asked but the creation says {creation.disposition.value}"]
    pending, out = creation.pending, []
    if (pending.frame, pending.answer_slots, pending.reason) != (outcome.pending.frame, outcome.pending.answer_slots,
                                                                 outcome.pending.reason):
        out.append("the pending is not what M2A-3 described")
    if [tuple(c.patch.ops) for c in pending.candidate_patches] != [tuple(c) for c in outcome.pending.candidates]:
        out.append("the candidates are not the ones resolved at ask time")
    if pending.expires_at_utc - pending.created_at_utc != T.timedelta(minutes=30) or pending.asks != 1:
        out.append("§8.2: plazo or asks")
    for candidate in pending.candidate_patches:  # every candidate is reachable by event and composes on the frame
        d = T.respond(pending, T.P.OptionEvent(pending.pending_id, candidate.candidate_id))
        if d.kind.value not in ("PENDING_ANSWER", "DEPENDENCY"):
            out.append(f"candidate {candidate.candidate_id} is not reachable by event: {d.kind.value}")
        if d.kind.value == "PENDING_ANSWER":
            if d.answer.composed.effective.base_trace_id != "t1":
                out.append("an event answer names another base than the asking turn")
            out += laundering(base, outcome, d.answer)
    return out + answer_problems(P, pending, creation, base, outcome, _answers(pending, creation.pending.asked_ops, pending.frame))


def seed_cases():
    """The pendings the direct tests keep asking about, as cases of the sweep: H6 (a carried territorial buyer), R3-1, H1,
    H11 (the whole turn is the open span) and a one-cause refusal. Hypothesis reaches them only now and then."""
    h6_base, _ = T.h6()
    r_base, _ = T.r31()
    one_base = T.base_of((D.TOPIC, "x"), (D.BUYER, "x"))
    return [(h6_base, fa.SemanticPatch((T.set_op(D.GEOGRAPHY, "y"),), fa.PatchOriginKind.TEXT), False),
            (r_base, fa.SemanticPatch((T.clear_op(T.ORDER_SLOT),), fa.PatchOriginKind.TEXT), False),
            (h6_base, fa.SemanticPatch((T.set_op(D.TIME, "y"),), fa.PatchOriginKind.TEXT), True),
            (T.base_of((D.TOPIC, "x")), fa.SemanticPatch((T.clear_op(T.slot(D.TOPIC)),), fa.PatchOriginKind.TEXT), False),
            (one_base, fa.SemanticPatch((T.set_op(D.BUYER, "y"), T.set_op(D.BUYER, "z", i=1)), fa.PatchOriginKind.TEXT),
             False)]


def test_the_seed_cases_are_pendings_and_obey_the_rules_of_8_3():
    asked = [_created(case) for case in seed_cases()]
    assert [c[2].pending.reason if c else None for c in asked] == [
        "TERRITORY_VS_CARRIED_BUYER", "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER", "CONTEXT_EXPIRED_CONFIRM",
        "EMPTY_SEARCH_AFTER_DELTA", "DELTA_SLOT_CONFLICT"]
    for case in seed_cases():
        assert _case_problems(T.P, case) == []


@PROPS
@given(st_case())
def test_every_pending_m2a3_creates_is_built_reachable_and_answered_by_the_rules_of_8_3(case):
    assert _case_problems(T.P, case) == []


@PROPS
@given(st_case(rich=False))
def test_the_same_over_plain_bases(case):
    assert _case_problems(T.P, case) == []


def _expired(case):
    return case[0], case[1], True


def h1_problems(case) -> list[str]:
    """D2 on real creations and explicitly adversarial persisted witnesses; never a Gold history reconstruction."""
    got = _created(case)
    if got is None or got[2].pending is None or got[2].pending.reason != "CONTEXT_EXPIRED_CONFIRM":
        return []
    base, outcome, creation = got
    pending, out = creation.pending, []
    for ops in _answers(pending, pending.asked_ops, base):
        for asked in (pending.asked_ops, (), (T.clear_op(T.TIME_POS, 60),)):
            # Fault injection is explicitly negative state, not the original asking turn.
            negative = replace(pending, asked_ops=asked)
            negative = fa.PendingClarification.from_dict(negative.to_dict())
            d = T.P.respond(negative, T.P.PendingTurn(T.facts(anchor_expired=True), tuple(ops)), trace_id="t2",
                            now=T.T0 + T.MINUTE, base=base, base_trace_id="t0")
            if d.kind.value != "REASK":
                out.append(f"{[(o.op.value, o.slot.key) for o in ops]} -> {d.kind.value}")
    event = T.respond(pending, T.P.OptionEvent(pending.pending_id, "c1"), base=base, base_trace_id="t0")
    return out if event.kind.value in ("PENDING_ANSWER", "DEPENDENCY") else out + ["the event does not confirm"]


@PROPS
@given(st_case(rich=False).map(_expired))
def test_h1_no_language_answer_executes_over_the_expired_anchor(case):
    """M2A4-D2 (P1-2): every pending ``CONTEXT_EXPIRED_CONFIRM`` M2A-3 creates × every answer in language, with or without a
    KEEP of filler (the shape ZEUS used: the asking patch + one KEEP, which the contract's model executed), whether or not
    the caller has the asking patch: D2 stops it on its own, F24 is not what holds it."""
    assert h1_problems(case) == []


def _has_expired_confirm(case) -> bool:
    got = _created(_expired(case))
    return got is not None and got[2].pending is not None and got[2].pending.reason == "CONTEXT_EXPIRED_CONFIRM"


def test_the_h1_sweep_reaches_the_pendings_the_probe_of_zeus_used():
    """The sweep above is not vacuous: it finds expired-confirm pendings among the generated cases."""
    assert find(st_case(rich=False), _has_expired_confirm,
                settings=settings(max_examples=2000, database=None, derandomize=True, suppress_health_check=list(HealthCheck)))


@PROPS
@given(st_case())
def test_slot_conflicts_is_exactly_the_delta_slot_conflict_cause_of_step_2(case):
    base, patch, _ = case
    expected = any(c.refusal is fd.Refusal.DELTA_SLOT_CONFLICT for c in fd.refusal_causes(base, patch.ops))
    assert T.P.slot_conflicts(patch.ops) is expected
    assert _spec_conflict(patch.ops) is expected


# ---- the mutants that matter to these gates are judged HERE too -----------------------------------------------------
def _differential_problems(mutated) -> list:
    """The differential over a slice of the model's population (every kind of pending, every kind of answer)."""
    for mp in map(typed_pending, model_pendings()[::3]):
        pending = lift_pending(mp)
        cfg = M.Config("sweep", mp.frame, len(mp.frame.universe) == 2, False, mp, "normal")
        asked = lift_ops(mp.asked)
        for ops in [o for o in answer_pool(mp) if not repeats_asked_unresolved(mp, o)][::2]:
            turn = lift_turn(ops)
            got = mutated.respond(pending, mutated.PendingTurn(turn.facts, turn.ops, False, turn.unresolved),
                                  trace_id="t2", now=T.T0 + T.MINUTE,
                                  base=lift_state(MODEL_BASES[mp]), base_trace_id="t0")
            if compare(M.answer(cfg, mp, ops), got, pending):
                return [(mp.reason, ops)]
    return []


def _sweep_kills(name: str) -> bool:
    """Whether the sweeps above notice mutant ``name`` (the same loader and mutants as the direct tests)."""
    mutated = T.load(T.MUTANTS[name])
    bases = {}
    sys.modules["m2a4_mut_foundation_pending_answer"].BASE_OF = bases  # only ``answer_on_base_not_frame`` reads it
    saved = T.P
    T.P = mutated
    try:
        if name == "answer_on_base_not_frame":
            _, made = T.authority_month()
            bases[made.pending.pending_id] = T.base_of((D.TOPIC, "x"), (D.BUYER, "x"), (D.TIME, "x"))
        if (_differential_problems(mutated) or any(_case_problems(mutated, case) for case in seed_cases())
                or any(h1_problems(case) for case in seed_cases())):
            return True
        try:
            find(st.one_of(st_case(rich=False), st_case()), lambda case: bool(_case_problems(mutated, case)),
                 settings=settings(max_examples=1500, database=None, derandomize=True, suppress_health_check=list(HealthCheck)))
            return True
        except NoSuchExample:
            return False
    except (Exception, pytest.fail.Exception):  # noqa: BLE001 - a mutant that makes the sweep itself fail is noticed
        return True
    finally:
        T.P = saved
        for stem in T.STEMS:
            sys.modules.pop(f"m2a4_mut_{stem}", None)


SWEEP_MUTANTS = ("answer_on_base_not_frame", "answer_with_the_anchors_trace", "f24_off",
                 "f24_only_before_normalizing", "subset_of_the_asking_patch_accepted",
                 "f24_part_counts_what_is_unresolved", "f24_normalized_for_every_pending",
                 "f25_executes", "f29_executes", "normalization_without_candidates", "normalization_drops_every_keep",
                 "expired_confirm_selected_in_language", "p_composed_instead_of_the_candidate", "conflict_not_decided_on_p",
                 "still_asking_executes", "selection_by_the_slot_only", "answered_rule_not_exempt")


@pytest.mark.parametrize("name", SWEEP_MUTANTS)
def test_the_sweeps_kill_the_mutants_of_the_gate(name):
    assert _sweep_kills(name), f"mutant {name} survives the sweeps of §8.4"
