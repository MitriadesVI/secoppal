"""M2A-4: the pending protocol against the sealed ``M2A_GOLD`` (P01–P10 of ``M2A_TRACEABILITY_MATRIX``).

The oracle of M2A-3 marks the cases of M2A-4 ``DEPENDIENTE(M2A-4)`` and gives them no typed input (``PENDING_PRIOR``): it works
on dicts and the protocol of pendings was not its subject. This adapter types them with the oracle's OWN typing
(``m2a_oracle_typing``: hints become closed values, the same hint the same value) and runs ``respond`` of M2A-4:

* the **prior** of a case (``prior.ir`` + ``prior.pending``) becomes a typed ``PendingClarification``: the frame is the typed
  prior IR, the candidates are its ``options`` resolved at ask time (a SET of the option's slot and hint; for the pending of
  §6.7 R3-1, C1 «quitar orden y límite» = ``CLEAR(ORDER)`` + ``CLEAR(CARDINALITY)`` and the ``SET_ORDER`` of the option), its
  ``answer_slots`` are the Gold's ``candidate_slots``;
* the **turn** of a case is typed from its ops: an ``ANSWER`` is a SET (the producer of the wire does the typing), an event
  names the option its value equals, ``CANCEL_PENDING`` is the directive, ``gap_codes`` ``ANY_OPTION`` is the label
  ``no_selection`` and an ``unresolved`` value is a slot nobody resolved; «selecciona oN» is the producer emitting the whole
  patch of option N (§6.7: «el productor emite las operaciones completas de C1»);
* its facts are the closed facts of P: a new search is ¬Continuación ∧ Marco ∧ Admisible sola.

Partition discipline (as in M2A-3): pytest MEASURES DEV only. TEST is measured ONCE, at the freeze, with
``python -m tests.test_foundation_pending_gold TEST`` (the suite only TYPES the TEST cases, to prove the adapter reaches
them). Declared red (§19, never dispensed): SF063 (``DEP-DERIVED-VIEW``, wave F) and SF192/SF199/SF200/SF201 (E-ORDER
enumerates the ``SET_ORDER`` candidates; SF199 also M2A-5). They are RUN here too, only to show what M2A-4 does with the
pending the Gold describes: the red is the dependency, not the protocol.
"""
from __future__ import annotations

import json
import re
import sys
from dataclasses import replace
from pathlib import Path

import pytest

import app.core.foundation_accepted as fa
import tests.test_foundation_pending as T
import tests.test_m2a3_oracle_link as LINK
from app.core.foundation_delta_outcome import delta_outcome
from app.core.interpretation_contract import ContractError
from scripts.audits.m2a_conversation import m2a_oracle_typing as TY

ROOT = Path(__file__).resolve().parents[1]
GOLD = {rec["gold_id"]: rec for rec in (json.loads(line) for line in (
    ROOT / "docs/audits/m2a_conversation/gold/M2A_GOLD_synthetic.jsonl").read_text("utf-8").splitlines())}
RED_DECLARED = {"SF063": "DEP-DERIVED-VIEW (F)", "SF192": "DEP-E-ORDER-CANDIDATES", "SF199": "DEP-E-ORDER-CANDIDATES + M2A-5",
                "SF200": "DEP-E-ORDER-CANDIDATES", "SF201": "DEP-E-ORDER-CANDIDATES"}
UNITS = {"millones": "MILLONES", "mil millones": "MIL_MILLONES", "billones": "BILLONES", "miles": "MILES"}
TRACE_ASKED, TRACE_ANSWER = "tA", "t1"


def owned_ids(partition: str | None = None) -> list[str]:
    """The cases the oracle hands to M2A-4 (``PENDING_PRIOR``), plus the ones of M2A-3 the creation also covers."""
    cases = LINK.cases()
    return sorted(c["gold_id"] for c in cases if "M2A-4" in c["owners"] and (partition is None or c["partition"] == partition))


def partition_of(gold_id: str) -> str:
    return next(c["partition"] for c in LINK.cases() if c["gold_id"] == gold_id)


# ---- typing ---------------------------------------------------------------------------------------------------------
def _slot(dimension: str) -> fa.Slot:
    if dimension in ("ORDER", "CARDINALITY"):
        return T.slot(T.D[dimension])
    if dimension.startswith("VALUE."):
        return T.slot(T.D.VALUE, side=T.ValueSide(dimension.partition(".")[2]))
    return fa.Slot.from_dict(f"{dimension}/POSITIVE")


def _condition(op: dict, org: fa.ConditionOrigin, cid: str) -> fa.AcceptedCondition:
    """The typed condition of a Gold op, by the oracle's own typing (never by a rule of M2a)."""
    dimension = "VALUE" if op["slot"].startswith("VALUE") else op["slot"]
    hint, inherited = op["value_hint"], op.get("inherited") or {}
    if dimension == "VALUE":
        unit = next(u for word, u in sorted(UNITS.items(), key=lambda kv: -len(kv[0])) if word in hint)
        typed = TY.tcond("VALUE", op["operator"], op["polarity"], None, unit, hint, unit_source=inherited.get("unit_source"))
    elif dimension == "GEOGRAPHY":
        typed = TY.tcond("GEOGRAPHY", "is", "POSITIVE", "MUNICIPALITY", None, hint)
    elif dimension == "TIME":
        typed = TY.tcond("TIME", op["operator"], "POSITIVE", None, None, hint, year_source=inherited.get("year_source"))
    else:
        typed = TY.tcond(dimension, op["operator"] or "is", "POSITIVE", None, None, hint)
    return fa.AcceptedCondition.from_dict(TY.condition_dict(typed, cid, ["r1"], org.to_dict()))


def _set(op: dict, org: fa.ConditionOrigin, cid: str) -> fa.PatchOp:
    cond = _condition(op, org, cid)
    return fa.PatchOp(fa.PatchOpKind.SET, cond.slot, cond, None, cond.origin)


def _org(trace: str, text: str, i: int = 0) -> fa.ConditionOrigin:
    return fa.ConditionOrigin.from_dict(TY.origin(trace, TY.sha(text), i))


def _r31_option(index: int, org: fa.ConditionOrigin) -> tuple[fa.PatchOp, ...]:
    """§6.7 / D-1 option 1: o1 = C1 «quitar orden y límite»; o2 = ``SET_ORDER`` by date (the limit is kept, F10)."""
    if index == 1:
        return (fa.PatchOp(fa.PatchOpKind.CLEAR, T.ORDER_SLOT, None, None, org),
                fa.PatchOp(fa.PatchOpKind.CLEAR, T.CARD_SLOT, None, None, org))
    typed = TY.tcond("ORDER", "date_desc", "POSITIVE", None, None, None, ods="STATED")
    cond = fa.AcceptedCondition.from_dict(TY.condition_dict(typed, "o2", ["r1"], org.to_dict()))
    return (fa.PatchOp(fa.PatchOpKind.SET_ORDER, cond.slot, cond, None, org),)


def typed_pending(gid: str, label: dict) -> tuple[fa.PendingClarification, dict]:
    prior, pend = label["prior"], label["prior"]["pending"]
    frame = LINK.base_of(TY.accepted_dict(gid, prior["ir"]))
    org = _org(TRACE_ASKED, pend["span"])
    candidates = []
    for i, option in enumerate(pend["options"], 1):
        if pend["reason"] == "DELTA_LEAVES_CARDINALITY_WITHOUT_ORDER":
            ops = _r31_option(i, org)
        else:
            ops = (_set({"slot": option["slot"], "operator": "is", "polarity": "POSITIVE", "value_hint": option["value_hint"]},
                        org, f"o{i}"),)
        candidates.append(fa.CandidatePatch(f"c{i}", option["option_id"], fa.SemanticPatch(ops, fa.PatchOriginKind.TEXT)))
    slots = {_slot(d) for d in pend["candidate_slots"]} | {s for c in candidates for s in c.patch.slots}
    pending = fa.PendingClarification(
        f"pending-{TRACE_ASKED}", TRACE_ASKED, T.T0, T.T0 + 30 * T.MINUTE, pend["asks"], frame, "t0",
        (0, len(pend["span"])), TY.sha(pend["span"]), pend["reason"], tuple(candidates), frozenset(slots))
    return pending, {"options": {o["option_id"]: f"c{i}" for i, o in enumerate(pend["options"], 1)}}


def typed_turn(gid: str, label: dict, pending: fa.PendingClarification, options: dict):
    """``(turn, event)``: the typed turn P (a ``PendingTurn``), or the option event for an event turn."""
    listed = label["prior"]["pending"]["options"]
    directive = any(op["op"] == "CANCEL_PENDING" for op in label["ops"])
    if label["input_kind"] == "EVENT":  # the event names the option the answer equals (O11)
        answer = next(op for op in label["ops"] if op["op"] == "ANSWER")
        chosen = next(o for o in listed if o["value_hint"] == answer["value_hint"])
        return None, T.P.OptionEvent(pending.pending_id, options["options"][chosen["option_id"]])
    no_selection = "ANY_OPTION" in label["gap_codes"]
    ops, unresolved = [], set()
    for i, op in enumerate(label["ops"]):
        if op["op"] == "CANCEL_PENDING" or no_selection:  # the label «no_selection» is all the producer says
            continue
        selected = re.search(r"selecciona (o\d)", op["value_hint"] or "") if op["op"] == "ANSWER" else None
        if selected:  # the producer emits the WHOLE patch of the option (§6.7)
            ops += list(_r31_option(int(selected.group(1)[1:]), _org(TRACE_ANSWER, gid, i)))
        elif op["unresolved"]:
            unresolved.add(_slot(op["slot"]))
        else:
            ops.append(_set(op, _org(TRACE_ANSWER, gid, i), f"p{i + 1}"))
    expected = label["expected_disposition"]
    if expected == "NEW_SEARCH":
        facts = T.new_search_facts(cancel_directive=directive)
    elif label["turn_kind"] == "DELTA":
        facts = T.facts(cancel_directive=directive, continuation=True)
    elif expected == "INFORM":
        facts = T.facts(unsupported_motive=label["disposition_reason"].partition(":")[2])
    else:
        facts = T.facts()
    return T.P.PendingTurn(facts, tuple(ops), no_selection, frozenset(unresolved)), None


def effective_values(effective: fa.AcceptedInterpretation) -> set:
    return {(c.slot.key, tuple(c.value)) for c in effective.conditions}


def expected_values(ir: dict) -> set:
    return {(t["slot"], tuple(t["value"])) for t in TY.ir_conditions(ir)}


def run_case(gid: str, which: str = "label_A") -> tuple[bool, str]:
    """Run one case; ``(agrees with the sealed label, what happened)``. Never reads another case."""
    label = GOLD[gid][which]
    pending, options = typed_pending(gid, label)
    turn, event = typed_turn(gid, label, pending, options)
    d = T.P.respond(pending, event or turn, trace_id="t2", now=T.T0 + T.MINUTE)
    want, reason = label["expected_disposition"], label["disposition_reason"]
    got = {"PENDING_ANSWER": "EXECUTE", "DELTA": "EXECUTE"}.get(d.kind.value, d.kind.value)
    seen = f"{got} {d.code}"
    if got != want:
        return False, seen
    if want in ("REASK", "INFORM") and d.code != reason:
        return False, seen
    if want == "NEW_SEARCH" and (d.control is None or d.control.value != "CANCEL_PENDING" or d.pending is not None):
        return False, f"{seen} without its cancellation"
    if want == "EXECUTE":
        effective = d.answer.composed.effective if d.kind.value == "PENDING_ANSWER" else d.outcome.effective
        if effective_values(effective) != expected_values(label["resulting_ir"]):
            return False, f"{seen}: the effective is not the resulting IR of the Gold"
        if effective.base_trace_id != TRACE_ASKED:
            return False, f"{seen}: not composed on the asking turn's frame"
    if want in ("REASK", "INFORM") and d.pending != (replace(pending, asks=pending.asks + 1) if want == "REASK" else pending):
        return False, f"{seen}: the pending is not kept as the contract says"
    return True, seen


def measure(partition: str) -> dict[str, tuple[bool, str]]:
    return {gid: run_case(gid) for gid in owned_ids(partition)}


# ---- the adapter reaches every case (TYPING only for TEST) ----------------------------------------------------------
@pytest.mark.parametrize("gid", owned_ids())
def test_every_case_of_m2a_4_is_typable_by_the_adapter_in_both_partitions(gid):
    for which in ("label_A", "label_B"):
        label = GOLD[gid][which]
        pending, options = typed_pending(gid, label)
        turn, event = typed_turn(gid, label, pending, options)
        assert (turn is None) != (event is None)


def test_the_cases_owned_by_m2a_4_are_the_ones_the_traceability_matrix_names():
    ids = set(owned_ids())
    assert {f"SF{n:03d}" for n in (47, 48, 49, 50, 51, 52, 53, 55, 56, 57, 58, 59, 60, 61, 63, 188, 189, 190, 191, 199, 200,
                                   201)} == ids
    assert {g for g in ids if partition_of(g) == "DEV"} == {"SF047", "SF048", "SF050", "SF059", "SF061", "SF189", "SF191",
                                                              "SF199"}
    assert set(RED_DECLARED) - {"SF192"} <= ids


# ---- DEV: measured by the suite ---------------------------------------------------------------------------------------
DEV = owned_ids("DEV")


@pytest.mark.parametrize("gid", DEV)
def test_dev_the_pending_protocol_agrees_with_the_sealed_label(gid):
    for which in ("label_A", "label_B"):  # the two blind labellers agree on the disposition and the code
        ok, seen = run_case(gid, which)
        assert ok, f"{gid} {which}: {seen}"


# ---- creation: the Gold's CLARIFY cases of M2A-3 become pendings ---------------------------------------------------------
def applicable_clarify(partition: str) -> list[str]:
    return sorted(c["gold_id"] for c in LINK.cases() if c["partition"] == partition
                  and c["applicability"] == "APLICABLE_M2A3" and c["expected"]["disposition"] == "CLARIFY")


def creation_problems(gid: str) -> list[str]:
    case = next(c for c in LINK.cases() if c["gold_id"] == gid)
    inputs = case["inputs"]
    facts = LINK.facts_of(inputs)
    base = LINK.base_of(inputs["base"])
    patch = fa.SemanticPatch.from_dict(inputs["patch"])
    outcome = delta_outcome(base, patch, base_trace_id=LINK.BASE_TRACE, anchor_expired=facts.anchor_expired)
    if outcome.disposition.value != "CLARIFY":
        return [f"the delta says {outcome.disposition.value}"]
    made = T.P.pending_from_question(outcome.pending, base=base, base_trace_id=LINK.BASE_TRACE, asked_trace_id="t1", now=T.T0)
    out = []
    if made.disposition.value != "CLARIFY" or made.pending is None:
        return [f"the creation says {made.disposition.value} {made.violations}"]
    p = made.pending
    if p.reason != case["expected"]["code"]:
        out.append(f"reason {p.reason} != {case['expected']['code']}")
    if p.asks != 1 or p.expires_at_utc - p.created_at_utc != T.timedelta(minutes=30):
        out.append("§8.2 plazo or asks")
    if p.reason == "CONTEXT_EXPIRED_CONFIRM" and (len(p.candidate_patches) != 1 or p.ambiguous_span is not None):
        out.append("H1: one candidate «continuar», no span")
    if p.reason.startswith("UNRESOLVED_VALUE") or p.reason in ("KEEP_ON_EMPTY_SLOT", "DELTA_SLOT_CONFLICT"):
        if p.candidate_patches:  # §8.4 table: the other causes ask with no candidate
            out.append("a cause that offers no option got candidates")
    return out


@pytest.mark.parametrize("gid", applicable_clarify("DEV"))
def test_dev_every_applicable_clarify_of_the_gold_becomes_a_possible_pending(gid):
    assert creation_problems(gid) == []


def main(partition: str) -> int:
    rows = measure(partition)
    creations = {gid: creation_problems(gid) for gid in applicable_clarify(partition)}
    for gid, (ok, seen) in sorted(rows.items()):
        print(f"{gid} {'OK ' if ok else 'RED'} {seen}" + (f"   [declared red: {RED_DECLARED[gid]}]" if gid in RED_DECLARED else ""))
    for gid, problems in sorted(creations.items()):
        print(f"{gid} creation {'OK' if not problems else problems}")
    clean = [g for g in rows if g not in RED_DECLARED]
    green = sum(rows[g][0] for g in clean)
    print(f"{partition}: {green}/{len(clean)} not declared red; {sum(not rows[g][0] for g in RED_DECLARED if g in rows)} of "
          f"{sum(g in RED_DECLARED for g in rows)} declared-red cases disagree; creation "
          f"{sum(not p for p in creations.values())}/{len(creations)}")
    return 0 if green == len(clean) and not any(creations.values()) else 1


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "DEV"))
    except ContractError as error:
        raise SystemExit(f"adapter refused: {error}")
