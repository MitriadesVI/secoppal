"""M2A-3 phase 2: the reducer against the FROZEN oracle M2A-8a0 (``f7d1b8b8``; read-only here).

The oracle's cases (dicts) go through the strict readers of ``foundation_accepted`` (the base through the one
constructor path, ``canonical_interpretation``, because a persisted base is canonical; the patch through
``SemanticPatch.from_dict``), then through ``classify_turn`` and ``delta_outcome``. The output is written in the
oracle's ``actuals`` shape and judged by its own ``audit``, ``audit_transitions`` and ``audit_classification``.
Nothing of the oracle is changed or relaxed.

Partition discipline: pytest measures DEV only. TEST is measured ONCE, at freeze, with
``python -m tests.test_m2a3_oracle_link TEST`` (never by the suite).

Adapter facts (declared, not decisions): the oracle's bases carry the trace ``t0``; the anchor is expired when the
Gold's ``gap_seconds`` reaches 30 minutes (§10.1: the injected fact M2A-6 computes); the subject-to-dataset check is
relational on the Gold's bases (their datasets are placeholders, ``LG-DATASET-VALUE``) and exact on generated ones.
"""
from __future__ import annotations

import os
import sys
from functools import lru_cache
from pathlib import Path

import pytest

import app.core.foundation_accepted as fa
from app.core.foundation_delta_outcome import DeltaDisposition, delta_outcome
from app.core.foundation_turn_scope import AnchorState, InputKind, TurnFacts, classify_turn, search_effects
from app.core.interpretation_contract import ContractError
from scripts.audits.m2a_conversation import m2a_reducer_oracle as oracle

EXPIRY_SECONDS = 30 * 60  # §10.1
BASE_TRACE = "t0"  # the trace of every oracle base (its origins say so)


def base_of(data: dict) -> fa.AcceptedInterpretation:
    requests = tuple(fa.AcceptedRequest.from_dict(item) for item in data["requests"])
    conditions = tuple(fa.AcceptedCondition.from_dict(item) for item in data["conditions"])
    logic = None if data["logic"] is None else fa.expression_from_dict(data["logic"])
    policies = tuple(fa.policy_from_dict(item) for item in data["policies"])
    return fa.canonical_interpretation(requests, conditions, logic, policies, data["base_trace_id"],
                                       data["root_trace_id"])


def facts_of(inputs: dict) -> TurnFacts:
    f, context = inputs["p_facts"], inputs["context"]
    gap = context.get("gap_seconds")
    return TurnFacts(InputKind.TEXT, AnchorState.FOUNDATION_SEARCH if f["anchor"] == "SEARCH" else AnchorState.NONE,
                     anchor_expired=gap is not None and gap >= EXPIRY_SECONDS, pending=bool(f["pending"]),
                     continuation=f["continuation"], new_content=f["new"], frame=f["frame"],
                     admissible_alone=f["admissible_alone"], own_conditions=f["own_conditions"])


def run(inputs: dict) -> tuple[dict | None, str | None, str | None]:
    """(actual, scope, problem). The scope reads only the facts of P; ``problem``: why the composition could not run (an
    input the types refuse). A case whose facts do not determine the scope (the oracle's NO_MEDIBLE for
    classification, ``LG-CLASSIFICATION-CERTIFIED-BY-8B``) is composed as a delta: the oracle leaves no NEW_SEARCH
    expectation undetermined (``NoProducer``)."""
    facts = facts_of(inputs)
    try:
        classification = classify_turn(facts)
    except ContractError as error:
        classification, scope = None, f"UNCLASSIFIED:{error}"
    else:
        scope = classification.scope.value
    try:
        base = base_of(inputs["base"])
        patch = fa.SemanticPatch.from_dict(inputs["patch"])
    except ContractError as error:
        return None, scope, f"UNREPRESENTABLE_INPUT:{error}"
    if classification is not None and classification.scope is fa.TurnScope.IN_NEW_SEARCH:
        conditions = tuple(op.condition for op in patch.ops if op.condition is not None)
        effects = search_effects(base, conditions, anchor_expired=facts.anchor_expired, pending_cancelled=False)
        return {"disposition": "NEW_SEARCH", "code": None, "effective": None, "effects": sorted(effects)}, scope, None
    outcome = delta_outcome(base, patch, base_trace_id=BASE_TRACE, anchor_expired=facts.anchor_expired)
    effective = None if outcome.effective is None else outcome.effective.to_dict()
    disposition = outcome.disposition.value if outcome.disposition is not DeltaDisposition.DEPENDENCY else (
        f"DEPENDENCY:{outcome.dependency.dep_id}")
    return ({"disposition": disposition, "code": outcome.code, "effective": effective,
             "effects": sorted(e.value for e in outcome.effects)}, scope, None)


@lru_cache(maxsize=None)
def cases(real_dir: str | None = None) -> tuple:
    """The oracle's cases; with the private REAL directory (never in git) the REAL slice too (ids and digests only
    ever leave this function: the oracle's reports never carry REAL content)."""
    return tuple(oracle.load_cases(None if real_dir is None else Path(real_dir)))


def measure(partition: str, real_dir: str | None = None) -> dict:
    chosen = oracle.select_partition(list(cases(real_dir)), partition)
    actuals, scopes, problems = {}, {}, {}
    for case in chosen:
        if case["applicability"] != oracle.APPLICABLE:
            continue
        actual, scope, problem = run(case["inputs"])
        if case["expected"]["classification"]["measurable"]:
            scopes[case["gold_id"]] = scope
        if problem is not None:
            problems[case["gold_id"]] = problem
            continue
        actuals[case["gold_id"]] = actual
    return {"cases": chosen, "actuals": actuals, "scopes": scopes, "problems": problems,
            "audit": oracle.audit(actuals, chosen,
                                  require_sources=("SYNTHETIC", "REAL") if real_dir else ("SYNTHETIC",)),
            "transitions": oracle.audit_transitions(actuals, chosen),
            "classification": oracle.audit_classification(scopes, chosen)}


@lru_cache(maxsize=None)
def dev() -> dict:
    return measure("DEV")


# The BLOCKER of phase 2 (an inherited unit typed without ``inherited_from``, §15.4) was fixed by the oracle in
# ``f95275b4``: every applicable input is built by the strict readers, and the audit is complete.
def test_dev_every_applicable_case_runs():
    assert dev()["problems"] == {}


def test_dev_gold_audit_has_no_mismatch():
    report = dev()["audit"]
    assert report["mismatches"] == [] and report["extra"] == [] and report["s19_problems"] == []


def test_dev_gold_audit_is_complete():
    assert dev()["audit"]["missing"] == [] and dev()["audit"]["ok"]


def test_dev_transitions_are_lawful():
    report = dev()["transitions"]
    assert report["ok"], report["violations"]


def test_dev_classification_matches():
    report = dev()["classification"]
    assert report["ok"], report


REAL_DIR = os.environ.get("M2A_GOLD_REAL_DIR")


@pytest.mark.skipif(REAL_DIR is None, reason="M2A_GOLD_REAL_DIR (private GOLD_CANDIDATE) not set")
def test_dev_with_the_real_slice():
    report = measure("DEV", REAL_DIR)
    assert report["problems"] == {} and report["audit"]["ok"]
    assert report["audit"]["coverage"]["applicable_expected"]["REAL"] > 0
    assert report["transitions"]["ok"] and report["classification"]["ok"]


def summary(report: dict) -> str:
    audit, transitions, classification = report["audit"], report["transitions"], report["classification"]
    applicable = audit["expected"]
    matched = applicable - len(audit["missing"]) - len({m["gold_id"] for m in audit["mismatches"]})
    lines = [f"sources {audit['coverage']['applicable_expected']} complete={audit['complete']}",
             f"applicable {applicable}: matched {matched}, missing {len(audit['missing'])}, "
             f"mismatches {len(audit['mismatches'])}, ok={audit['ok']}",
             f"transitions: ok={transitions['ok']} checked={transitions['checked']} refusals="
             f"{transitions['refusals_checked']} violations={len(transitions['violations'])}",
             f"classification: ok={classification['ok']} expected={classification['expected']} "
             f"mismatches={len(classification['mismatches'])}",
             f"problems: {report['problems']}"]
    lines += [f"  mismatch {m}" for m in audit["mismatches"]]
    lines += [f"  violation {v}" for v in transitions["violations"]]
    lines += [f"  classification {m}" for m in classification["mismatches"]]
    return "\n".join(lines)


if __name__ == "__main__":
    print(summary(measure(sys.argv[1] if len(sys.argv) > 1 else "DEV", os.environ.get("M2A_GOLD_REAL_DIR"))))
