"""P2-C: una métrica no finita nunca acredita un monto, ni invalida el conteo."""
from __future__ import annotations

import pytest

from app.core.entity_profile.frame_evidence import (
    MetricStatus,
    accredit_metric,
    bind_observation,
)


@pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity", None])
def test_nonfinite_amount_leaves_the_count_valid(raw):
    count = accredit_metric("contract_count", "4")
    amount = accredit_metric("total_contract_value", raw)
    assert count.status is MetricStatus.VALID and count.value == 4
    assert amount.status is not MetricStatus.VALID
    assert amount.value is None


@pytest.mark.parametrize("raw", ["1e-999", "0.5", "-1"])
def test_a_count_the_source_did_not_state_is_never_zero(raw):
    metric = accredit_metric("contract_count", raw)
    assert metric.status is MetricStatus.INVALID
    assert metric.value is None


def test_explicit_zero_is_a_fact():
    metric = accredit_metric("contract_count", "0")
    assert metric.status is MetricStatus.VALID and metric.value == 0


def test_transport_success_without_rows_is_not_a_zero():
    observation = bind_observation(
        plan_id="p1", job_id="j1", fingerprint="fp", dataset_id="jbjy-vk9h",
        sql="SELECT count(*) WHERE 1=2", status="SUCCESS", rows=(),
    )
    assert observation.metric_status("contract_count") is MetricStatus.MISSING


def test_a_not_requested_section_is_not_a_failure():
    observation = bind_observation(
        plan_id="p1", job_id="j1", fingerprint="fp", dataset_id="jbjy-vk9h",
        sql="", status="NOT_REQUESTED", rows=(), attempted=False,
    )
    assert observation.metric_status("contract_count") is MetricStatus.NOT_REQUESTED


def test_every_observation_names_the_plan_that_authorized_it():
    observation = bind_observation(
        plan_id="p1", job_id="j1", fingerprint="fp", dataset_id="jbjy-vk9h",
        sql="SELECT count(*) AS contract_count WHERE x", status="SUCCESS",
        rows=({"contract_count": "4"},),
    )
    assert observation.plan_id == "p1" and observation.job_id == "j1"
    assert observation.predicate_fingerprint == "fp"
    assert observation.metric_status("contract_count") is MetricStatus.VALID
