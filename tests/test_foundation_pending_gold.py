"""M2A-4: the pending protocol against the sealed ``M2A_GOLD`` (P01–P10 of ``M2A_TRACEABILITY_MATRIX``).

The oracle of M2A-3 marks the cases of M2A-4 ``DEPENDIENTE(M2A-4)`` and gives them no typed input (``PENDING_PRIOR``): it works
on dicts and the protocol of pendings was not its subject. This adapter types them with the oracle's OWN typing
(``m2a_oracle_typing``: hints become closed values, the same hint the same value) and runs ``respond`` of M2A-4:

* the **prior** of a case (``prior.ir`` + ``prior.pending``) becomes a typed ``PendingClarification``: the frame is the typed
  prior IR, the candidates are its ``options`` resolved at ask time (a SET of the option's slot and hint; for the pending of
  §6.7 R3-1, C1 «quitar orden y límite» = ``CLEAR(ORDER)`` + ``CLEAR(CARDINALITY)`` and the ``SET_ORDER`` of the option), its
  ``answer_slots`` are the Gold's ``candidate_slots``;
* the **turn** of a case is typed from its ops: an ``ANSWER`` is a SET (the producer of the wire does the typing), an event
  names the option its value equals, ``CANCEL_PENDING`` is the directive, an ``unresolved`` value is a slot nobody resolved
  (``gap_codes`` ``ANY_OPTION`` would be the label ``no_selection``, but that is a labeller's note, not an op: SF061 is
  not measured, ZEUS P3-5); «selecciona oN» is the producer emitting the whole
  patch of option N (§6.7: «el productor emite las operaciones completas de C1»);
* its facts are the closed facts of P. The Gold carries NONE of them (the oracle types them only for the cases of M2A-3), so
  they are not derived from the verdict: an ANSWER (``turn_kind = ANSWER_TO_CLARIFICATION``) takes the declared neutral
  facts of an answer (¬Continuación, ¬Marco, ¬Admisible sola). A case whose turn is a NEW_SEARCH or a DELTA, or whose
  verdict is an INFORM from the wire, would need the producer's facts to be told apart from what the label already says:
  it is **«no medible sin hechos del productor»**, not green. It can still be RUN, with the facts the label implies, only
  as information (``informational=True``), and is never counted.

Partition discipline (as in M2A-3): pytest MEASURES DEV only. TEST is measured ONCE, at the freeze, with
``python -m tests.test_foundation_pending_gold TEST`` (the suite only TYPES the TEST cases, to prove the adapter reaches
them). Three statuses, no more: MEASURED (an answer, no §19 dependency), NO_MEDIBLE (needs the producer's facts) and
RED_DECLARED (§19, never dispensed: SF063 ``DEP-DERIVED-VIEW`` of wave F; SF192/SF199/SF200/SF201 ``DEP-E-ORDER-CANDIDATES``,
E-ORDER enumerates the ``SET_ORDER`` candidates, SF199 also M2A-5). The asking patch (F24) is supplied where the Gold lets
it be known (the pending of §6.7 asks about ``CLEAR(ORDER)``); elsewhere the harness DECLARES there is none
(``no_asked_patch``): the Gold has no case that exercises F24, which the direct tests, the sweeps and the model do.
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
ANSWER_KIND = "ANSWER_TO_CLARIFICATION"


# The decisive input of these cases is taken from the label's ``gap_codes``, a labeller's note, not from a typed fact: the
# label ``no_selection`` of the wire (§8.3 row 6) is not in the Gold as an op (ZEUS P3-5).
GAP_DERIVED_INPUT = ("ANY_OPTION",)


def status_of(gid: str) -> str:
    """Honest R4 coverage: sealed prior pendings lack original causal history. Section19 reds stay separate.
    No label, expected result, partition or case bytes are changed by this adapter status."""
    return "RED_DECLARED" if gid in RED_DECLARED else "NO_VERIFICABLES"


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
    """The sealed prior-pending format has no original frame_ops/asked_ops or accredited historical base.
    R4 must not reconstruct them from frame/options/reason or interpret absence as empty. This corpus can be
    inventoried, but its original prior pendings cannot be promoted to pending/2 by this adapter."""
    raise ContractError("GOLD_CAUSAL_HISTORY_UNVERIFIABLE")


def typed_turn(gid: str, label: dict, pending: fa.PendingClarification, options: dict, informational: bool = False):
    """``(turn, event)``: the typed turn P (a ``PendingTurn``), or the option event for an event turn. ``informational``: the
    facts of a NEW_SEARCH, a DELTA or an INFORM are the ones the LABEL implies (never counted as a measurement)."""
    listed = label["prior"]["pending"]["options"]
    directive = any(op["op"] == "CANCEL_PENDING" for op in label["ops"])
    if label["input_kind"] == "EVENT":  # the event names the option the answer equals (O11)
        answer = next(op for op in label["ops"] if op["op"] == "ANSWER")
        chosen = next(o for o in listed if o["value_hint"] == answer["value_hint"])
        return None, T.P.OptionEvent(pending.pending_id, options["options"][chosen["option_id"]])
    no_selection = informational and any(g in GAP_DERIVED_INPUT for g in label["gap_codes"])  # a note, never a measurement
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
    if not informational:
        facts = T.facts()  # an answer: ¬Continuación, ¬Marco, ¬Admisible sola (declared, not derived from the verdict)
    elif expected == "NEW_SEARCH":
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


def run_case(gid: str, which: str = "label_A", informational: bool = False) -> tuple[bool, str]:
    """No original v2 causal input exists in this sealed prior-pending slice; informational mode cannot invent it."""
    typed_pending(gid, GOLD[gid][which])  # explicit closed refusal; neither expected label nor an option supplies history
    raise ContractError("GOLD_CAUSAL_HISTORY_UNVERIFIABLE")


def by_status(partition: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {"MEASURED": [], "NO_MEDIBLE": [], "NO_VERIFICABLES": [], "RED_DECLARED": []}
    for gid in owned_ids(partition):
        out[status_of(gid)].append(gid)
    return out


def measure(partition: str) -> dict[str, tuple[bool, str]]:
    """MEASURED cases only: the ones that count."""
    return {gid: run_case(gid) for gid in by_status(partition)["MEASURED"]}


# ---- the adapter reaches every case (TYPING only for TEST) ----------------------------------------------------------
@pytest.mark.parametrize("gid", owned_ids())
def test_prior_gold_histories_are_refused_without_fabricating_v2_inputs(gid):
    # Metadata/refusal gate only, never a successful Gold consuming replay.
    for which in ("label_A", "label_B"):
        label = GOLD[gid][which]
        assert "frame_ops" not in label["prior"]["pending"] and "asked_ops" not in label["prior"]["pending"]
        with pytest.raises(ContractError, match="GOLD_CAUSAL_HISTORY_UNVERIFIABLE"):
            typed_pending(gid, label)


def test_the_cases_owned_by_m2a_4_are_the_ones_the_traceability_matrix_names():
    ids = set(owned_ids())
    assert {f"SF{n:03d}" for n in (47, 48, 49, 50, 51, 52, 53, 55, 56, 57, 58, 59, 60, 61, 63, 188, 189, 190, 191, 199, 200,
                                   201)} == ids
    assert {g for g in ids if partition_of(g) == "DEV"} == {"SF047", "SF048", "SF050", "SF059", "SF061", "SF189", "SF191",
                                                              "SF199"}
    assert set(RED_DECLARED) - {"SF192"} <= ids


# ---- the status of every case, and honest denominators ------------------------------------------------------------------
def test_every_case_has_one_status_and_the_denominators_are_these():
    """R4 reports uncovered history explicitly. Section19 reds are not converted into green or unverifiable labels."""
    dev, test = by_status("DEV"), by_status("TEST")
    assert dev == {"MEASURED": [], "NO_MEDIBLE": [],
                   "NO_VERIFICABLES": ["SF047", "SF048", "SF050", "SF059", "SF061", "SF189", "SF191"],
                   "RED_DECLARED": ["SF199"]}
    assert test == {"MEASURED": [], "NO_MEDIBLE": [],
                    "NO_VERIFICABLES": ["SF049", "SF051", "SF052", "SF053", "SF055", "SF056", "SF057", "SF058",
                                       "SF060", "SF188", "SF190"],
                    "RED_DECLARED": ["SF063", "SF200", "SF201"]}
    assert sum(len(v) for v in (*dev.values(), *test.values())) == len(owned_ids()) == 22
    with pytest.raises(ContractError, match="GOLD_CAUSAL_HISTORY_UNVERIFIABLE"):
        run_case("SF047")


def test_dev_gold_pending_consumption_has_zero_measured_cases_and_no_silent_passes():
    assert measure("DEV") == {}
    for gid in by_status("DEV")["NO_VERIFICABLES"]:
        for informational in (False, True):
            with pytest.raises(ContractError, match="GOLD_CAUSAL_HISTORY_UNVERIFIABLE"):
                run_case(gid, informational=informational)


def test_dev_sf199_stays_declared_red_and_its_missing_history_is_not_filled():
    assert status_of("SF199") == "RED_DECLARED" and "SF199" not in by_status("DEV")["MEASURED"]
    with pytest.raises(ContractError, match="GOLD_CAUSAL_HISTORY_UNVERIFIABLE"):
        typed_pending("SF199", GOLD["SF199"]["label_A"])


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


def h5_problems(gid: str) -> list[str]:
    """F12 has alternatives, but its original asking op is unresolved in the sealed producer input.
    An alternative is not the original typed operation. No causal asking patch is invented here."""
    label = GOLD[gid]["label_A"]
    assert all(op["unresolved"] for op in label["ops"])
    return ["UNVERIFIABLE_ORIGINAL_TYPED_ASKING_PATCH"]


def test_dev_p09_reports_missing_typed_original_without_promoting_an_alternative():
    assert h5_problems("SF163") == ["UNVERIFIABLE_ORIGINAL_TYPED_ASKING_PATCH"]


def main(partition: str) -> int:
    statuses = by_status(partition)
    rows = measure(partition)
    creations = {gid: creation_problems(gid) for gid in applicable_clarify(partition)}
    if partition == partition_of("SF163"):
        creations["SF163"] = h5_problems("SF163")
    for gid, (ok, seen) in sorted(rows.items()):
        print(f"{gid} {'OK ' if ok else 'RED'} MEASURED     {seen}")
    for gid in statuses["NO_VERIFICABLES"]:
        print(f"{gid} --- NO_VERIFICABLES (original causal history absent; not run, not counted green)")
    for gid in statuses["RED_DECLARED"]:
        print(f"{gid} --- RED_DECLARED [{RED_DECLARED[gid]}] (prior causal history absent; not run)")
    for gid, problems in sorted(creations.items()):
        print(f"{gid} creation {'OK' if not problems else problems}")
    green = sum(ok for ok, _ in rows.values())
    print(f"{partition}: MEASURED {green}/{len(rows)}; NO_MEDIBLE {len(statuses['NO_MEDIBLE'])} (not counted); "
          f"NO_VERIFICABLES {len(statuses['NO_VERIFICABLES'])} (not run, not counted); "
          f"RED_DECLARED {len(statuses['RED_DECLARED'])} (not counted); of {len(owned_ids(partition))} cases owned; "
          f"creation {sum(not p for p in creations.values())}/{len(creations)}")
    return 0 if green == len(rows) and not any(creations.values()) else 1


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "DEV"))
    except ContractError as error:
        raise SystemExit(f"adapter refused: {error}")
