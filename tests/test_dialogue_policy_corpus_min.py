"""Validate structure of dialogue_policy_corpus_min.yaml — no runner yet."""

from pathlib import Path

import yaml

CORPUS = yaml.safe_load(
    Path("tests/fixtures/dialogue_policy_corpus_min.yaml").read_text()
)


def test_corpus_has_30_cases():
    assert CORPUS["metadata"]["total_cases"] == 30
    assert len(CORPUS["cases"]) == 30


def test_corpus_composition():
    cats = {c["category"] for c in CORPUS["cases"]}
    assert "city_as_scope" in cats
    assert "city_not_entity" in cats
    assert "implicit_opportunity" in cats
    assert "temporal_month" in cats
    assert "number_phrase_scrub" in cats
    assert "discourse_filler" in cats
    assert "non_query_followup" in cats
    assert "negative" in cats


def test_corpus_negatives_present():
    negatives = [c for c in CORPUS["cases"] if c["id"].startswith("NEG-")]
    assert len(negatives) == 10


def test_corpus_real_cases_cite_backlog():
    reals = [
        c for c in CORPUS["cases"]
        if "source" in c and "CORPUS_BACKLOG" in c.get("source", "")
    ]
    assert len(reals) == 10


def test_corpus_all_cases_have_ids():
    for case in CORPUS["cases"]:
        assert case.get("id"), f"Case missing id: {case}"


def test_corpus_severities_valid():
    valid = {"HIGH", "MEDIUM", "LOW"}
    for case in CORPUS["cases"]:
        assert case["severity"] in valid, f"Bad severity in {case['id']}"


def test_corpus_should_execute_is_boolean():
    for case in CORPUS["cases"]:
        assert isinstance(case["should_execute"], bool), f"{case['id']}"
