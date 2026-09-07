"""Tests for the ADR-094 performance re-adjudication rule.

These exercise the adjudicator, not the resolver runtime. The point is that the
0/1/>=2 rule and the frozen thresholds cannot drift while nobody is looking.
"""
from __future__ import annotations

import math

import pytest

from scripts.audits.actor_prefix_adjudicator import (
    ENUMERATION_P95_MAX_MS,
    EXISTENCE_P95_MAX_MS,
    GO,
    INCONCLUSIVE,
    MEASUREMENTS_PER_VERSION,
    NO_GO,
    REQUIRED_CAMPAIGNS,
    RESOLVER_DELTA_MAX_MS,
    WARMUPS_PER_VERSION,
    adjudicate,
    judge_campaign,
    percentile,
)


def passing(index):
    return judge_campaign(index, 0.14, 0.27, 0.02, 0.07)


def failing(index, gate="resolver_delta"):
    if gate == "resolver_delta":
        return judge_campaign(index, 0.14, 1.66, 0.02, 0.07)
    if gate == "existence":
        return judge_campaign(index, 0.14, 0.27, 0.9, 0.07)
    return judge_campaign(index, 0.14, 0.27, 0.02, 1.4)


def test_frozen_thresholds_are_the_contract_values():
    assert RESOLVER_DELTA_MAX_MS == 0.5
    assert EXISTENCE_P95_MAX_MS == 0.5
    assert ENUMERATION_P95_MAX_MS == 1.0
    assert REQUIRED_CAMPAIGNS == 5
    assert MEASUREMENTS_PER_VERSION == 2000
    assert WARMUPS_PER_VERSION == 20


def test_percentile_matches_the_frozen_artifact_estimator():
    values = [float(n) for n in range(1, 51)]
    assert percentile(values, 0.95) == values[math.ceil(50 * 0.95) - 1]
    assert percentile(values, 0.5) == values[math.ceil(50 * 0.5) - 1]
    assert percentile(values, 1.0) == 50.0
    with pytest.raises(ValueError):
        percentile([], 0.95)


def test_campaign_fails_on_resolver_delta():
    verdict = judge_campaign(1, 0.1, 0.1 + RESOLVER_DELTA_MAX_MS + 0.000001)
    assert verdict.failed_gates == ("resolver_delta",)
    assert not verdict.passed


def test_campaign_fails_on_existence():
    verdict = judge_campaign(1, 0.1, 0.2, EXISTENCE_P95_MAX_MS + 0.000001, 0.07)
    assert verdict.failed_gates == ("existence",)


def test_campaign_fails_on_enumeration():
    verdict = judge_campaign(1, 0.1, 0.2, 0.02, ENUMERATION_P95_MAX_MS + 0.000001)
    assert verdict.failed_gates == ("enumeration",)


def test_campaign_reports_every_breached_gate():
    verdict = judge_campaign(1, 0.1, 2.0, 0.9, 1.9)
    assert verdict.failed_gates == ("resolver_delta", "existence", "enumeration")


def test_thresholds_are_inclusive_upper_bounds():
    """Exactly at the limit passes; the gate is `>`, as in the AFTER run."""
    verdict = judge_campaign(1, 0.1, 0.1 + RESOLVER_DELTA_MAX_MS,
                             EXISTENCE_P95_MAX_MS, ENUMERATION_P95_MAX_MS)
    assert verdict.passed
    assert verdict.delta_ms == pytest.approx(RESOLVER_DELTA_MAX_MS)


def test_absent_phase_measurements_cannot_fail_a_campaign():
    """A query with no challengers never enumerates; absence is not a breach."""
    assert judge_campaign(1, 0.1, 0.2, 0.02, None).passed
    assert judge_campaign(1, 0.1, 0.2, None, None).passed


def test_zero_failures_is_go():
    report = adjudicate([passing(n) for n in range(1, 6)])
    assert report["performance_adjudication"] == GO
    assert report["fail_campaigns"] == []
    assert report["performance_regression_confirmed"] is False
    assert report["original_spike_reproduced"] is False
    assert report["original_gate_failure_retained"] is True
    assert report["merge_recommended"] is True


def test_one_failure_is_inconclusive_and_never_merges():
    report = adjudicate([failing(1), passing(2), passing(3), passing(4), passing(5)])
    assert report["performance_adjudication"] == INCONCLUSIVE
    assert report["fail_campaigns"] == [1]
    assert report["performance_regression_confirmed"] is None
    assert report["original_spike_reproduced"] is True
    assert report["merge_recommended"] is False


def test_two_failures_are_no_go():
    report = adjudicate([failing(1), failing(2), passing(3), passing(4), passing(5)])
    assert report["performance_adjudication"] == NO_GO
    assert report["fail_campaigns"] == [1, 2]
    assert report["performance_regression_confirmed"] is True
    assert report["merge_recommended"] is False


@pytest.mark.parametrize("count", [3, 4, 5])
def test_three_or_more_failures_stay_no_go(count):
    campaigns = [failing(n) for n in range(1, count + 1)]
    campaigns += [passing(n) for n in range(count + 1, 6)]
    assert adjudicate(campaigns)["performance_adjudication"] == NO_GO


def test_mixed_gate_failures_still_count_as_campaign_failures():
    report = adjudicate([failing(1, "existence"), failing(2, "enumeration"),
                         passing(3), passing(4), passing(5)])
    assert report["performance_adjudication"] == NO_GO
    assert report["fail_campaigns"] == [1, 2]


def test_a_sixth_campaign_is_refused():
    """No tie-breaker run: an INCONCLUSIVE verdict cannot be argued away."""
    with pytest.raises(ValueError, match="exactly 5 campaigns"):
        adjudicate([failing(1)] + [passing(n) for n in range(2, 7)])


def test_fewer_than_five_campaigns_is_refused():
    """Nor can a failing campaign be dropped as an outlier."""
    with pytest.raises(ValueError, match="exactly 5 campaigns"):
        adjudicate([failing(1), passing(2), passing(3), passing(4)])
