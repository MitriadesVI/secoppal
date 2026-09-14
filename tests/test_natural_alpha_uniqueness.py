"""Post-ground integrity: reject the complete proposal, never repair the jobs."""
import json
from unittest.mock import Mock

import pytest
from hypothesis import given, strategies as st

from app.core.natural_alpha.admission import observe
from app.core.natural_alpha.grounding import validate_and_ground
from app.core.natural_alpha.provider import ProviderResult
from app.core.natural_alpha.uniqueness import validate_job_uniqueness
from tests.test_natural_alpha_contract import IDENTITY, config, payload


def test_duplicate_grounded_job_invalidates_complete_proposal():
    jobs = json.loads(payload())["jobs"] * 2
    provider = Mock(observe=Mock(return_value=ProviderResult(json.dumps({"jobs": jobs}), provider_calls=1)))
    result = observe(config(), "busca", "streamlit", IDENTITY, provider)
    assert result.status == "INVALID"
    assert not result.jobs
    assert not result.terminal
    provider.observe.assert_called_once()


def _job(quote, occurrence=1, action="SEARCH", aspect="CONTRACTS"):
    return {"action": action, "aspect": aspect, "evidence_quote": quote, "occurrence": occurrence}


@pytest.mark.parametrize("raw,jobs,valid", [
    ("busca contratos", [_job("busca"), _job("busca contratos")], False),
    ("abcde", [_job("abcd"), _job("cde")], False),
    ("aaaa", [_job("aa", 1), _job("aa", 2)], False),
    ("busca", [_job("busca"), _job("busca", action="OTHER", aspect="OTHER")], False),
    ("busca", [_job("busca", action="OTHER", aspect="OTHER"), _job("busca")], False),
    ("busca busca", [_job("busca", 1), _job("busca", 2)], True),
    ("abcde", [_job("ab"), _job("cde")], True),
    ("busca", [_job("busca"), _job("busca", action="ANALYZE", aspect="TOTAL_VALUE")], True),
    ("busca luego", [_job("busca"), _job("luego", action="OTHER", aspect="OTHER")], True),
])
def test_overlap_and_occurrence_contract(raw, jobs, valid):
    provider = Mock(observe=Mock(return_value=ProviderResult(json.dumps({"jobs": jobs}), provider_calls=1)))
    result = observe(config(), raw, "streamlit", IDENTITY, provider)
    assert result.status == ("MULTI_JOB_BLOCKED" if valid else "INVALID")
    assert len(result.jobs) == (2 if valid else 0)
    provider.observe.assert_called_once()


@given(st.text(alphabet=st.characters(blacklist_characters="|", blacklist_categories=("Cs",)),
               min_size=1, max_size=20))
def test_exact_repetitions_fail_but_disjoint_occurrences_survive(text):
    raw = text + "|" + text
    repeated = json.dumps({"jobs": [_job(text), _job(text)]})
    with pytest.raises(ValueError):
        validate_job_uniqueness(validate_and_ground(raw, repeated))
    distinct = json.dumps({"jobs": [_job(text), _job(text, 2)]})
    # The separator ensures the second full quote has a disjoint occurrence.
    jobs = validate_and_ground(raw, distinct)
    validate_job_uniqueness(jobs)
    assert jobs[0].end <= jobs[1].start


@given(st.integers(min_value=2, max_value=30), st.integers(min_value=1, max_value=20))
def test_partial_overlap_rejected_for_same_pair_and_allowed_for_distinct_pairs(width, extra):
    raw = "a" * (width + extra)
    first, second = _job("a" * width, 1), _job("a" * width, 2)
    with pytest.raises(ValueError):
        validate_job_uniqueness(validate_and_ground(raw, json.dumps({"jobs": [first, second]})))
    second.update(action="ANALYZE", aspect="TOTAL_VALUE")
    validate_job_uniqueness(validate_and_ground(raw, json.dumps({"jobs": [first, second]})))
