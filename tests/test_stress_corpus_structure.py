"""Structural validation of the broad-query stress corpus.

Does NOT hit SECOP — only checks the fixture is well-formed so the future
latency/timeout runner can rely on it.
"""

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")

FIXTURE = Path(__file__).parent / "fixtures" / "stress_broad_queries.yaml"

_VALID_FAILURE_MODES = {
    "timeout", "oversized", "bad_filter",
    "soql_error", "wrong_universe", "where_1_1",
}
_VALID_COVERAGE = {"covered", "partial", "gap"}
_REQUIRED_FIELDS = {
    "id", "query", "category", "failure_mode",
    "expected_guard", "current_coverage",
}


@pytest.fixture(scope="module")
def corpus():
    return yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))


def test_fixture_loads(corpus):
    assert "cases" in corpus and corpus["cases"]


def test_every_case_has_required_fields(corpus):
    for case in corpus["cases"]:
        missing = _REQUIRED_FIELDS - set(case)
        assert not missing, f"{case.get('id', '?')} missing fields: {missing}"


def test_ids_are_unique(corpus):
    ids = [c["id"] for c in corpus["cases"]]
    dupes = {i for i in ids if ids.count(i) > 1}
    assert not dupes, f"duplicate ids: {dupes}"


def test_failure_modes_are_known(corpus):
    for case in corpus["cases"]:
        assert case["failure_mode"] in _VALID_FAILURE_MODES, case["id"]


def test_coverage_values_are_known(corpus):
    for case in corpus["cases"]:
        assert case["current_coverage"] in _VALID_COVERAGE, case["id"]


def test_has_at_least_one_gap_to_track(corpus):
    # Sanity: the catalog exists precisely because some scenarios are gaps.
    coverage = {c["current_coverage"] for c in corpus["cases"]}
    assert "gap" in coverage
