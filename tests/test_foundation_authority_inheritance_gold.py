"""M2A-3b: the Gold's DEP-INHERITANCE cases (contract §19) against the input this lane produces.

The gate of the step is NOT that those cases turn green (they stay a declared red until M2A-3c: the rule of receipt does not
exist yet). It is that, for each one, the INPUT the host would hand the authority is what §15.4 / §16.1 / §16.3 demand:
PRESENT with its value, ABSENT, or AMBIGUOUS with its reason. Never a disposition, never an effective.

Each case is typed from the Gold's own ``prior`` IR by the frozen oracle's typing (``m2a_oracle_typing``), and its expected
input is derived from the Gold's OWN LABEL (``ops[].inherited``, ``disposition_reason``, the prior and resulting IR), never
from this lane's code. Adapter facts, declared (they change nothing the input reads):

* A1 the oracle cannot type ``UNSPECIFIED`` + Product View (wave F, ``AcceptedRequest.view`` is not representable yet): the
  case is typed with the universe of its records (``CONTRACTS``). The input never reads the universe.
* A2 the oracle types a ``relative_period`` window as a plain interval (no provenance marker): the base of this test carries
  ``year_source = RELATIVE_TO_CLOCK`` on it, as a production base does (contract §3.2, §11). Without that marker a relative
  window is indistinguishable from a range inside one year (see ``test_a_window_without_its_marker...``).
* A3 the base trace is the oracle's ``t0`` (the record whose effective is the base).

Partition discipline (the repo's, ``test_m2a3_oracle_link``): pytest measures DEV only. TEST is measured ONCE, at freeze,
with ``python -m tests.test_foundation_authority_inheritance_gold TEST`` (the private REAL slice joins it through
``M2A_GOLD_REAL_DIR``; only ids, units and years ever leave this module, never REAL content).
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

import app.core.foundation_accepted as fa
import app.core.foundation_authority_inheritance as inh
from scripts.audits.m2a_conversation import m2a_oracle_typing as T

ROOT = Path(__file__).resolve().parents[1]
GOLD = ROOT / "docs/audits/m2a_conversation/gold"
S19 = json.loads((GOLD / "CONTRACT_S19_DEPENDENCIES.json").read_text("utf-8"))
SEAL = json.loads((GOLD / "M2A_GOLD_SEAL.json").read_text("utf-8"))
REAL_DIR = os.environ.get("M2A_GOLD_REAL_DIR")
BASE = "t0"  # A3

# The 16 cases of DEP-INHERITANCE, named by the brief of the step and read back from §19's sealed extraction.
GATE = ("R0496", "R0497", "R0498", "R0531", "R0532", "SF031", "SF032", "SF033", "SF034", "SF035", "SF101", "SF103", "SF104",
        "SF106", "SF167", "SF183")
# Beyond the 16, in M2A_GOLD (never FRONTIER): the base with two units (CLARIFY UNIT_AMBIGUOUS) and the three cases of the
# previous-referent rule (DEP-TIME-RELATIVE). SF168 (SMMLV over 2023-2025) is NOT here: the frozen oracle cannot type its
# prior (``SMMLV_YEAR_NOT_SINGLE``: an unreachable pre-state), so it would never reach the module.
EXTRA = ("SF102", "SF037", "SF042", "SF174")
REFERENT_CASES = ("SF037", "SF042", "SF174")
LOWER = {"greater_than", "greater_or_equal"}


def partition_of(gid: str) -> str:
    return next(part for part in ("DEV", "TEST") for source in ("REAL", "SYNTHETIC") if gid in SEAL["partition"][part][source])


def gold_ids() -> set[str]:
    return {i for source in ("REAL", "SYNTHETIC") for i in SEAL["slices"]["M2A_GOLD"][source]["ids"]}


def frontier_ids() -> set[str]:
    return {r["gold_id"] for source in ("REAL", "SYNTHETIC") for r in SEAL["slices"]["FOLLOWUP_FRONTIER"][source]["records"]}


def load_rows(real_dir: str | None) -> dict[str, dict]:
    rows = {}
    paths = [GOLD / "M2A_GOLD_synthetic.jsonl"] + ([Path(real_dir) / "M2A_GOLD_real.jsonl"] if real_dir else [])
    for path in paths:
        for line in path.read_text("utf-8").splitlines():
            row = json.loads(line)
            rows[row["gold_id"]] = row
    return rows


# ---- typing the prior (the oracle's) -------------------------------------------------------------------------------------------
def typed_base(gid: str, prior: dict) -> fa.AcceptedInterpretation:
    ir = {**prior, "universe": "CONTRACTS" if prior["universe"] == "UNSPECIFIED" else prior["universe"]}  # A1
    data = T.accepted_dict(gid, ir)
    windows = {c["id"] for c in prior["conditions"] if c["dimension"] == "TIME" and c["operator"] == "relative_period"}
    for condition in data["conditions"]:  # A2
        if condition["condition_id"] in windows:
            condition["provenance"] = {**condition["provenance"], "year_source": "RELATIVE_TO_CLOCK"}
    return fa.canonical_interpretation(
        tuple(fa.AcceptedRequest.from_dict(i) for i in data["requests"]),
        tuple(fa.AcceptedCondition.from_dict(i) for i in data["conditions"]), None, (), data["base_trace_id"],
        data["root_trace_id"])


# ---- what the Gold's label says the input must be (independent of the module) -----------------------------------------------
def year_in(hint: str) -> int:
    return int(re.search(r"(?<![\d])(\d{4})(?![\d])", hint).group(1))


def span_years(hint: str) -> tuple[int, int]:
    years = [int(y) for y in re.findall(r"(?<![\d])(\d{4})(?![\d])", hint)]
    return min(years), max(years)


def months_of(condition: dict) -> int:
    operator, hint = condition["operator"], condition["value_hint"]
    if operator == "during_month":
        return 1
    if operator == "during_year":
        return 12
    start, end = (date.fromisoformat(part) for part in hint.split(".."))
    after = end + timedelta(days=1)
    assert start.day == 1 and after.day == 1, "a Gold window of a referent case is aligned to months"
    return (after.year - start.year) * 12 + after.month - start.month


def expectations(label: dict, gid: str) -> dict[str, tuple]:
    """The input each label demands: ``unit`` / ``year`` / ``referent`` → a plain tuple. §15.4, §16.1, §16.3 as the Gold
    labels them: an inherited unit, a month whose year is inherited or inferred, the reasons that ask, the previous referent."""
    prior, ops, reason, result = label["prior"]["ir"]["conditions"], label["ops"], label["disposition_reason"], label["resulting_ir"]
    out: dict[str, tuple] = {}
    bounds = [c for c in prior if c["dimension"] == "VALUE"]
    inherited = [o for o in ops if (o["inherited"] or {}).get("unit_source") == "PREVIOUS_ACCEPTED_VALUE"]
    if inherited:  # the unit the Gold gives the inherited bounds in the RESULT, and the one unit the prior had (G10)
        sides = {o["slot"].partition(".")[2] for o in inherited}
        units = {c["unit"] for c in result["conditions"] if c["dimension"] == "VALUE"
                 and ("lower" if c["operator"] in LOWER else "upper") in sides}
        assert len(units) == 1 and {c["unit"] for c in bounds} == units, f"{gid}: the Gold's own unit facts disagree"
        out["unit"] = ("PRESENT", units.pop())
    elif reason == "UNIT_AMBIGUOUS":
        out["unit"] = ("AMBIGUOUS", "MULTIPLE_UNITS")
    times = [c for c in prior if c["dimension"] == "TIME" and c["polarity"] == "POSITIVE"]
    month = [o for o in ops if o["slot"] == "TIME" and o["operator"] == "during_month"
             and (o["inherited"] or {}).get("year_source") in ("PREVIOUS_ACCEPTED_TIME", "CLOCK_INFERRED")]
    if reason == "MONTH_YEAR_AMBIGUOUS":
        (only,) = times
        if only["operator"] == "relative_period":
            out["year"] = ("AMBIGUOUS", "RELATIVE_WINDOW")
        else:
            assert span_years(only["value_hint"])[0] < span_years(only["value_hint"])[1], f"{gid}: not a plurianual prior"
            out["year"] = ("AMBIGUOUS", "MULTI_YEAR")
    elif month and month[0]["inherited"]["year_source"] == "PREVIOUS_ACCEPTED_TIME":
        (only,) = times
        assert span_years(only["value_hint"])[0] == span_years(only["value_hint"])[1] == year_in(
            next(c for c in result["conditions"] if c["dimension"] == "TIME")["value_hint"]), f"{gid}: the Gold's year facts"
        out["year"] = ("PRESENT", year_in(only["value_hint"]))
    elif month:  # CLOCK_INFERRED: the base holds no active TIME, the host's clock decides (year.inferred)
        assert not times, f"{gid}: a clock-inferred month over a base with a TIME"
        out["year"] = ("ABSENT",)
    if gid in REFERENT_CASES:
        (only,) = times
        first, last = span_years(only["value_hint"])
        out["referent"] = ("PRESENT", first, last, months_of(next(c for c in result["conditions"] if c["dimension"] == "TIME")))
    return out


# ---- the module's answer, as the same plain tuples -----------------------------------------------------------------------------
def answers(result) -> dict[str, tuple]:
    """By ``.value``, never by class identity: the same function reads a mutant's answer (its enums are its own)."""
    unit, year, referent = result.unit, result.year, result.referent

    def read(part, present: tuple) -> tuple:
        status = part.status.value
        return ("PRESENT", *present) if status == "PRESENT" else ("AMBIGUOUS", part.reason.value) if (
            status == "AMBIGUOUS") else ("ABSENT",)

    return {"unit": read(unit, (unit.unit.value if unit.unit else None,)), "year": read(year, (year.year,)),
            "referent": read(referent, (referent.first_year, referent.last_year, referent.months))}


def check_case(gid: str, row: dict, module=inh) -> list[str]:
    problems = []
    exp_a, exp_b = expectations(row["label_A"], gid), expectations(row["label_B"], gid)
    if exp_a != exp_b:
        problems.append("the two labelers demand different inputs")
    if not exp_a:
        problems.append("the label demands no input: the case is not an inheritance case")
    try:
        base = typed_base(gid, row["label_A"]["prior"]["ir"])
    except T.Untypeable as error:
        return problems + [f"untypeable: {error.motive}"]
    result = module.host_inheritance_inputs(base, base_trace_id=BASE)
    got = answers(result)
    for key, expected in exp_a.items():
        if got[key] != expected:
            problems.append(f"{key}: expected {expected}, produced {got[key]}")
    for part in (result.unit, result.year, result.referent):
        if part.status.value == "PRESENT" and part.inherited_from != BASE:
            problems.append("a PRESENT input not inherited from the base record")
    return problems


def measure(partition: str, real_dir: str | None = None) -> dict[str, list[str]]:
    rows = load_rows(real_dir)
    return {gid: check_case(gid, rows[gid]) for gid in (*GATE, *EXTRA) if gid in rows and partition_of(gid) == partition}


# ---- the tests ---------------------------------------------------------------------------------------------------------------------
def test_the_gate_cases_are_exactly_dep_inheritance_and_none_is_frontier():
    dependency = next(d for d in S19["dependencies"] if d["dep_id"] == "DEP-INHERITANCE")
    assert tuple(dependency["cases"]) == GATE and dependency["cases_count"] == len(GATE) == 16
    assert set(GATE) | set(EXTRA) <= gold_ids() and not (set(GATE) | set(EXTRA)) & frontier_ids()  # FRONTIER is no gate
    assert {d["dep_id"]: d["cases"] for d in S19["dependencies"]}["DEP-TIME-RELATIVE"] == list(REFERENT_CASES)


DEV_SYNTHETIC = sorted(gid for gid in (*GATE, *EXTRA) if gid.startswith("SF") and partition_of(gid) == "DEV")


@pytest.mark.parametrize("gid", DEV_SYNTHETIC)
def test_the_input_of_each_dev_case_is_what_the_contract_demands(gid):
    assert check_case(gid, load_rows(None)[gid]) == []


def test_every_gate_case_has_a_partition_and_pytest_measures_only_the_dev_ones():
    sides = {gid: partition_of(gid) for gid in (*GATE, *EXTRA)}
    assert set(sides.values()) == {"DEV", "TEST"}  # both exist: TEST is measured once, at freeze (see the module docstring)
    assert DEV_SYNTHETIC == sorted(g for g, side in sides.items() if side == "DEV" and g.startswith("SF"))
    assert not [g for g in DEV_SYNTHETIC if sides[g] != "DEV"]


def test_the_gate_demands_an_input_for_every_dev_case_and_the_expectations_differ_across_them():
    """Not vacuous: DEV spans PRESENT units, PRESENT years, AMBIGUOUS years of both reasons, an ABSENT year and a referent."""
    kinds = set()
    for gid in DEV_SYNTHETIC:
        for key, expected in expectations(load_rows(None)[gid]["label_A"], gid).items():
            kinds.add((key, expected[0], expected[1] if expected[0] == "AMBIGUOUS" else None))
    assert {("unit", "PRESENT", None), ("year", "PRESENT", None), ("year", "ABSENT", None),
            ("year", "AMBIGUOUS", "MULTI_YEAR"), ("year", "AMBIGUOUS", "RELATIVE_WINDOW"),
            ("referent", "PRESENT", None)} <= kinds


@pytest.mark.parametrize("mutant_name, gids", [
    ("year_first_of_a_plurianual_time", ["SF031"]),
    ("time_relative_window_accepted", ["SF033"]),
    ("time_relative_clock_marker_ignored", ["SF033"]),
    ("referent_months_off_by_one", ["SF037", "SF042"]),
    ("unit_reads_only_the_lower_bound", ["SF103"]),  # SF103's prior holds an UPPER bound only (SF183's holds a lower one)
])
def test_the_gold_harness_is_not_vacuous_it_sees_the_mutants_of_the_rules(mutant_name, gids):
    """The mutants of the rules that a DEV case can show (the others are killed by the tables and the properties)."""
    from tests import test_foundation_authority_inheritance_mutants as mutants

    mutant = mutants._load(mutants.mutated_source(*mutants.MUTANTS[mutant_name][1]))
    rows = load_rows(None)
    assert all(not check_case(gid, rows[gid]) for gid in gids)  # the real module passes them
    assert any(check_case(gid, rows[gid], mutant) for gid in gids), f"{mutant_name}: the Gold harness did not see it"


def test_a_window_without_its_marker_is_not_distinguishable_from_a_range_inside_one_year():
    """Why A2 exists, and an obligation for whoever builds bases (M2A-1b ``from_acceptance`` and the TIME authority): the
    oracle's typing of SF033's window is a plain interval inside 2026. Without ``year_source = RELATIVE_TO_CLOCK`` the input
    is PRESENT(2026), which the Gold refutes (MONTH_YEAR_AMBIGUOUS)."""
    row = load_rows(None)["SF033"]
    data = T.accepted_dict("SF033", row["label_A"]["prior"]["ir"])
    unmarked = fa.canonical_interpretation(
        tuple(fa.AcceptedRequest.from_dict(i) for i in data["requests"]),
        tuple(fa.AcceptedCondition.from_dict(i) for i in data["conditions"]), None, (), data["base_trace_id"],
        data["root_trace_id"])
    assert answers(inh.host_inheritance_inputs(unmarked, base_trace_id=BASE))["year"] == ("PRESENT", 2026)
    assert answers(inh.host_inheritance_inputs(typed_base("SF033", row["label_A"]["prior"]["ir"]),
                                               base_trace_id=BASE))["year"] == ("AMBIGUOUS", "RELATIVE_WINDOW")


@pytest.mark.skipif(REAL_DIR is None, reason="M2A_GOLD_REAL_DIR (private GOLD_CANDIDATE) not set")
def test_dev_with_the_real_slice():
    report = measure("DEV", REAL_DIR)
    assert all(not problems for problems in report.values())


def main(partition: str) -> int:
    """TEST is measured once, at freeze: ``python -m tests.test_foundation_authority_inheritance_gold TEST``."""
    report = measure(partition, REAL_DIR)
    for gid, problems in sorted(report.items()):
        print(f"{gid} {'PASS' if not problems else 'FAIL ' + '; '.join(problems)}")
    print(f"{partition}: {sum(not p for p in report.values())}/{len(report)} inputs as demanded")
    return int(any(report.values()))


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "DEV"))
