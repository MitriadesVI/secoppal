"""M2A-8a0: every input the oracle hands to the reducer is a value the M2A-1 STRICT readers accept.

The oracle builds ``m2a-accepted/1`` and ``m2a-patch/1`` dicts by hand from the shapes of contract §3 and §5. M2A-3 consumes
them through the strict readers of M2A-1 (``AcceptedInterpretation.from_dict``, ``SemanticPatch.from_dict``), which refuse what
the contract forbids: an inherited unit or year without its origin (``INHERITANCE_WITHOUT_ORIGIN``, §15.4, §3.2), a year without
its source (``YEAR_WITHOUT_SOURCE``), a SMMLV bound without its amount and year, an unknown receipt rule... This file builds ALL
the applicable inputs, and the faithful effective of each, with those readers, and requires that each one round-trips to the
same dict.

The independence of the oracle is that of its CODE (``scripts/``): the link to M2a lives here, in ``tests/``, and never in the
oracle. This file is NOT one of the files the independence pins scan, and it only validates FORM: it never decides an
expectation (the expectations are the Gold's).
"""

from __future__ import annotations

import collections
import os
from pathlib import Path

import pytest

from app.core.foundation_accepted import AcceptedInterpretation, SemanticPatch
from scripts.audits.m2a_conversation import m2a_reducer_oracle as oracle
from scripts.audits.m2a_conversation import m2a_oracle_typing as typing_

REAL_DIR = Path(os.environ["M2A_GOLD_REAL_DIR"]) if os.environ.get("M2A_GOLD_REAL_DIR") else None
needs_real = pytest.mark.skipif(REAL_DIR is None, reason="M2A_GOLD_REAL_DIR (private GOLD_CANDIDATE) not set")


def invalid_inputs(cases: list[dict]) -> list[tuple[str, str]]:
    """``(gold_id, why)`` for every applicable case whose base, patch or faithful effective a strict reader refuses or does not
    round-trip."""
    problems = []
    for case in cases:
        if case["applicability"] != oracle.APPLICABLE:
            continue
        gid, inputs = case["gold_id"], case["inputs"]
        for name, reader, data in (("base", AcceptedInterpretation, inputs["base"]), ("patch", SemanticPatch, inputs["patch"]),
                                   ("effective", AcceptedInterpretation, oracle.realize_effective(case))):
            if data is None:
                continue
            try:
                if reader.from_dict(data).to_dict() != data:
                    problems.append((gid, f"{name} does not round-trip"))
            except Exception as error:  # ContractError and friends: the reader said no
                problems.append((gid, f"{name}: {type(error).__name__} {error}"))
    return problems


CASES = oracle.load_cases()


def test_every_applicable_input_of_the_synthetic_gold_is_read_by_the_m2a_1_strict_readers():
    assert sum(c["applicability"] == oracle.APPLICABLE for c in CASES) > 100
    assert invalid_inputs(CASES) == []


@needs_real
def test_every_applicable_input_of_the_real_slice_is_read_by_the_m2a_1_strict_readers():
    cases = oracle.load_cases(REAL_DIR)
    assert sum(c["applicability"] == oracle.APPLICABLE and c["source"] == "REAL" for c in cases) > 20
    assert invalid_inputs(cases) == []


def provenances(case: dict):
    inputs = case["inputs"]
    for condition in inputs["base"]["conditions"]:
        yield "base", condition["provenance"]
    for op in inputs["patch"]["ops"]:
        if op["condition"] is not None:
            yield "patch", op["condition"]["provenance"]


def test_an_inherited_provenance_always_says_where_it_comes_from_and_a_stated_one_never_does():
    """§15.4 (unit.inherited, unit_source = PREVIOUS_ACCEPTED_VALUE, inherited_from) and §16.1 (year.inherited,
    year_source = PREVIOUS_ACCEPTED_TIME): the origin is the trace of the record whose effective is the base, the case's anchor."""
    seen = collections.Counter()
    for case in CASES:
        if case["applicability"] != oracle.APPLICABLE:
            continue
        for where, prov in provenances(case):
            inherited = prov["unit_source"] == "PREVIOUS_ACCEPTED_VALUE" or prov["year_source"] == "PREVIOUS_ACCEPTED_TIME"
            assert (prov["inherited_from"] is not None) == inherited, (case["gold_id"], where, prov)
            if inherited:
                assert prov["inherited_from"] == case["inputs"]["base"]["root_trace_id"] == typing_.ANCHOR_TRACE
                seen["unit" if prov["unit_source"] == "PREVIOUS_ACCEPTED_VALUE" else "year"] += 1
    assert seen["unit"] >= 6 and seen["year"] >= 1  # SF101, SF103, SF104, SF106, SF167, SF183 and the inherited years


def test_the_inherited_unit_cases_the_orchestrator_named_are_valid_now():
    for gid in ("SF101", "SF103", "SF104", "SF106", "SF167", "SF183"):
        case = next(c for c in CASES if c["gold_id"] == gid)
        assert case["applicability"] == oracle.APPLICABLE
        bound = next(op["condition"] for op in case["inputs"]["patch"]["ops"] if op["condition"] and op["condition"]["dimension"] == "VALUE")
        assert bound["provenance"]["unit_source"] == "PREVIOUS_ACCEPTED_VALUE" and bound["provenance"]["inherited_from"] == "t0"
        SemanticPatch.from_dict(case["inputs"]["patch"])


def test_a_year_and_its_source_travel_together_in_every_typed_provenance():
    for case in CASES:
        if case["applicability"] != oracle.APPLICABLE:
            continue
        for where, prov in provenances(case):
            assert (prov["year"] is None) == (prov["year_source"] is None), (case["gold_id"], where, prov)
            assert (prov["unit"] is None) == (prov["unit_source"] is None), (case["gold_id"], where, prov)
            if prov["unit"] == "SMMLV":  # §15.5: recalculated from source_amount, never from COP
                assert prov["source_amount"] is not None and prov["year"] is not None, (case["gold_id"], where, prov)


def test_the_strict_reader_would_have_refused_the_old_inputs():
    """The test is not vacuous: an inherited unit without its origin, and a year without its source, are refused."""
    case = next(c for c in CASES if c["gold_id"] == "SF103")
    broken = {**case["inputs"]["patch"], "ops": [dict(op) for op in case["inputs"]["patch"]["ops"]]}
    for op in broken["ops"]:
        if op["condition"] is not None and op["condition"]["dimension"] == "VALUE":
            op["condition"] = {**op["condition"], "provenance": {**op["condition"]["provenance"], "inherited_from": None}}
    with pytest.raises(Exception, match="INHERITANCE_WITHOUT_ORIGIN"):
        SemanticPatch.from_dict(broken)
    year_without_source = dict(case["inputs"]["base"]["conditions"][0])
    year_without_source["provenance"] = {**year_without_source["provenance"], "year": 2025}
    with pytest.raises(Exception, match="YEAR_WITHOUT_SOURCE"):
        AcceptedInterpretation.from_dict({**case["inputs"]["base"],
                                          "conditions": [year_without_source, *case["inputs"]["base"]["conditions"][1:]]})
