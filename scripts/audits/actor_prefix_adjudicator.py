"""Predefined re-adjudication rule for the ADR-094 resolver performance gate.

Pure decision logic: no clock, no SQLite, no filesystem. The thresholds are the
ones frozen in ``budget_contract.json`` before the AFTER run and are not
negotiable here -- this module re-applies them to five independent campaigns,
it never relaxes them.

The primary gate verdict (NO_GO, ``after_benchmark.json``) is historical fact
and stays. This rule only answers whether that single observation reproduces.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field

# Frozen in budget_contract.json before the AFTER measurement. Do not change.
RESOLVER_DELTA_MAX_MS = 0.5
EXISTENCE_P95_MAX_MS = 0.5
ENUMERATION_P95_MAX_MS = 1.0

REQUIRED_CAMPAIGNS = 5
MEASUREMENTS_PER_VERSION = 2000
WARMUPS_PER_VERSION = 20

GO = "GO"
INCONCLUSIVE = "INCONCLUSIVE"
NO_GO = "NO_GO"


def percentile(values, fraction):
    """Same estimator as baseline_benchmark.json / after_benchmark.json."""
    if not values:
        raise ValueError("percentile of an empty sample is undefined")
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * fraction) - 1]


@dataclass(frozen=True)
class CampaignVerdict:
    index: int
    before_p95_ms: float
    after_p95_ms: float
    delta_ms: float
    existence_p95_ms: float | None
    enumeration_p95_ms: float | None
    failed_gates: tuple[str, ...] = field(default_factory=tuple)

    @property
    def passed(self) -> bool:
        return not self.failed_gates


def judge_campaign(index, before_p95_ms, after_p95_ms,
                   existence_p95_ms=None, enumeration_p95_ms=None) -> CampaignVerdict:
    """A campaign fails on resolver delta, existence p95 or enumeration p95."""
    delta = after_p95_ms - before_p95_ms
    failed = []
    if delta > RESOLVER_DELTA_MAX_MS:
        failed.append("resolver_delta")
    if existence_p95_ms is not None and existence_p95_ms > EXISTENCE_P95_MAX_MS:
        failed.append("existence")
    if enumeration_p95_ms is not None and enumeration_p95_ms > ENUMERATION_P95_MAX_MS:
        failed.append("enumeration")
    return CampaignVerdict(index, before_p95_ms, after_p95_ms, delta,
                           existence_p95_ms, enumeration_p95_ms, tuple(failed))


def adjudicate(campaigns) -> dict:
    """0 fail -> GO, exactly 1 -> INCONCLUSIVE, 2 or more -> NO_GO.

    Exactly five campaigns are admissible. A sixth is not a tie-breaker and a
    failed campaign is never re-run or discarded as an outlier: rejecting the
    wrong count here is what stops that from happening quietly.
    """
    campaigns = list(campaigns)
    if len(campaigns) != REQUIRED_CAMPAIGNS:
        raise ValueError(
            f"adjudication requires exactly {REQUIRED_CAMPAIGNS} campaigns, got {len(campaigns)}"
        )
    failed = [c for c in campaigns if not c.passed]
    if not failed:
        adjudication, regression = GO, False
    elif len(failed) == 1:
        adjudication, regression = INCONCLUSIVE, None
    else:
        adjudication, regression = NO_GO, True
    return {
        "performance_adjudication": adjudication,
        "fail_campaigns": [c.index for c in failed],
        "fail_count": len(failed),
        "performance_regression_confirmed": regression,
        "original_gate_failure_retained": True,
        "original_spike_reproduced": bool(failed),
        "merge_recommended": adjudication == GO,
        "thresholds": {
            "resolver_delta_max_ms": RESOLVER_DELTA_MAX_MS,
            "existence_p95_max_ms": EXISTENCE_P95_MAX_MS,
            "enumeration_p95_max_ms": ENUMERATION_P95_MAX_MS,
        },
    }
