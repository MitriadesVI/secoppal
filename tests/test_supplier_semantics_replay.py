"""Replay of experiment a2-polarity-sidecar-x1 through the runtime adjudicator.

The measured gates belong to the harness that produced them until the same
recorded answers are shown to fail closed here. This file replays all 210
recorded calls against the runtime's own validation and reduction, with no
provider call, and additionally proves that the host-owned refs the runtime
derives are the very refs the experiment classified -- otherwise the evidence
would describe a different question than the one production asks.
"""
from __future__ import annotations

import json
import pathlib

import pytest

from app.core.capability_binding.supplier_semantics import (
    MentionRef,
    VetoOutcome,
    adjudicate,
    mention_refs,
    reduce_turn,
)
from app.core.capability_binding.supplier_request_authority import assess_supplier_request

# Vendored with the promotion candidate: the measured evidence is a regression
# of this runtime, so it travels with it. A missing file is a failure, never a
# skip -- a gate that disappears from the totals is worse than a red one.
EXPERIMENT = pathlib.Path(__file__).parent / "fixtures" / "a2_supplier_semantics" / "x1"


def _load(name: str):
    path = EXPERIMENT / name
    assert path.exists(), f"vendored x1 evidence missing: {path}"
    return path


@pytest.fixture(scope="module")
def corpus():
    return {c["id"]: c for c in json.loads(_load("corpus_polarity_v1.json").read_text())["cases"]}


@pytest.fixture(scope="module")
def oracle():
    import csv
    rows = csv.DictReader(_load("oracle_polarity_v1.csv").open(encoding="utf-8"))
    verdicts: dict[str, dict[tuple[str, int], str]] = {}
    for row in rows:
        if row.get("id"):
            verdicts.setdefault(row["id"], {})[(row["mention"], int(row["occurrence"]))] = row["polarity"]
    return verdicts


@pytest.fixture(scope="module")
def runs():
    return [json.loads(line) for line in
            _load("runs.jsonl").read_text(encoding="utf-8").splitlines()]


def test_the_experiment_covered_every_case_three_times(runs, corpus):
    assert len(runs) == 210
    assert len({r["id"] for r in runs}) == len(corpus) == 70
    assert sorted({r["run"] for r in runs}) == [1, 2, 3]


def test_runtime_refs_match_the_refs_the_experiment_classified(corpus):
    """Evidence about other refs would be evidence about another question."""
    # Counted outside the loop as well: re-introducing a skip is an equivalent
    # mutation while every case still yields mentions, so the intent is stated
    # here rather than left to a guard that cannot currently fire.
    compared = 0
    for case in corpus.values():
        evidence = assess_supplier_request(case["query"])
        runtime = [(r.quote, r.occurrence) for r in mention_refs(case["query"], evidence)]
        measured = [(m["quote"], m["occurrence"]) for m in case["mention_index"]]
        # No tolerance: a case that stops producing mentions is exactly the
        # failure this oracle exists to catch, so it may not be skipped.
        assert runtime == measured, case["id"]
        assert runtime, case["id"]
        compared += 1
    assert compared == len(corpus) == 70


def test_every_recorded_answer_survives_runtime_validation(runs, corpus):
    """No recorded answer may be rejected as malformed by the runtime."""
    rejected = []
    for record in runs:
        case = corpus[record["id"]]
        refs = mention_refs(case["query"], assess_supplier_request(case["query"]))
        verdict = adjudicate(refs, json.loads(record["raw"]))
        if verdict.outcome is VetoOutcome.UNAVAILABLE:
            rejected.append((record["run"], record["id"], verdict.detail))
    assert rejected == []


def test_no_recorded_answer_confirms_a_turn_the_oracle_calls_non_positive(runs, corpus, oracle):
    """The load-bearing gate: the sidecar never opens SUPPLIERS against intent.

    Counted per call. A case answered correctly twice and wrongly once is one
    failure here, exactly as it would be one wrong answer in production.
    """
    failures = []
    for record in runs:
        case = corpus[record["id"]]
        refs = mention_refs(case["query"], assess_supplier_request(case["query"]))
        verdict = adjudicate(refs, json.loads(record["raw"]))
        truth = reduce_turn(tuple(oracle[record["id"]][(r.quote, r.occurrence)] for r in refs))
        if verdict.outcome is VetoOutcome.CONFIRMED and truth != "POSITIVE":
            failures.append((record["run"], record["id"], case["query"], truth))
    assert failures == []


def test_no_recorded_answer_mislabels_a_negated_mention_as_positive(runs, corpus, oracle):
    failures = []
    for record in runs:
        for item in json.loads(record["raw"])["mentions"]:
            key = (item["quote"], int(item["occurrence"]))
            if oracle[record["id"]].get(key) == "NEGATED" and item["polarity"] == "POSITIVE":
                failures.append((record["run"], record["id"], key))
    assert failures == []


def test_the_deterministic_path_alone_would_have_opened_nine_of_these(corpus, oracle):
    """Why the sidecar is wired at all, pinned as a number rather than a claim.

    If this count ever drops, the deterministic authority has improved and the
    sidecar's justification must be re-argued, not assumed.
    """
    unsafe = []
    for cid, case in corpus.items():
        truth = reduce_turn(tuple(oracle[cid].values()))
        if assess_supplier_request(case["query"]).polarity.value == "POSITIVE" and truth != "POSITIVE":
            unsafe.append(cid)
    assert sorted(unsafe) == ["C08", "C10", "D04", "D05", "D06", "D07", "D08", "D09", "D10"]


def test_every_case_answered_identically_in_all_three_runs(runs, corpus):
    by_case: dict[str, list[str]] = {}
    for record in runs:
        case = corpus[record["id"]]
        refs = tuple(MentionRef(m["quote"], m["occurrence"]) for m in case["mention_index"])
        verdict = adjudicate(refs, json.loads(record["raw"]))
        by_case.setdefault(record["id"], []).append(verdict.turn_polarity)
    unstable = {cid: v for cid, v in by_case.items() if len(set(v)) > 1}
    assert unstable == {}


def test_the_sidecar_never_abstained_which_is_recorded_not_relied_on(runs):
    """UNKNOWN 0 of 258 is why the wiring gives it veto power only."""
    labels = [m["polarity"] for r in runs for m in json.loads(r["raw"])["mentions"]]
    assert len(labels) == 258
    assert labels.count("UNKNOWN") == 0
    assert labels.count("MIXED") == 0
