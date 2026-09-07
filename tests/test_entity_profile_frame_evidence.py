"""P2-C: una métrica no finita nunca acredita un monto, ni invalida el conteo."""
from __future__ import annotations

import pytest

from app.core.entity_profile.frame_evidence import (
    MetricStatus,
    accredit_metric,
    bind_observation,
    observe_call,
)
from entity_profile_frame_harness import BUYER, observe


def _plan():
    result = observe(f"analiza contratos de {BUYER}")
    assert result.ready
    return result.decision.plan


def _metadata(plan):
    job = plan.jobs[0]
    return {
        "plan": plan,
        "plan_id": plan.plan_id,
        "job_id": job.job_id,
        "fingerprint": plan.source_plans[job.source].fingerprint,
        "dataset_id": job.dataset_id,
        "sql": job.sql,
    }


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
    plan = _plan()
    observation = bind_observation(
        **_metadata(plan), status="SUCCESS", rows=(),
    )
    assert observation.metric_status("contract_count") is MetricStatus.MISSING


def test_a_not_requested_section_is_not_a_failure():
    plan = _plan()
    observation = bind_observation(
        **_metadata(plan), status="NOT_REQUESTED", rows=(), attempted=False,
    )
    assert observation.metric_status("contract_count") is MetricStatus.NOT_REQUESTED


def test_every_observation_names_the_plan_that_authorized_it():
    plan = _plan()
    metadata = _metadata(plan)
    observation = bind_observation(
        **metadata, status="SUCCESS", rows=({"contract_count": "4"},),
    )
    assert observation.plan_id == plan.plan_id
    assert observation.job_id == metadata["job_id"]
    assert observation.predicate_fingerprint == metadata["fingerprint"]
    assert observation.metric_status("contract_count") is MetricStatus.VALID


@pytest.mark.parametrize("field", ["plan_id", "job_id", "fingerprint", "dataset_id", "sql"])
def test_forged_metadata_is_rejected_against_the_plan(field):
    metadata = _metadata(_plan())
    metadata[field] = "foreign"
    with pytest.raises(ValueError):
        bind_observation(
            **metadata, status="SUCCESS", rows=({"contract_count": "4"},),
        )


def test_observe_call_derives_identity_from_the_authorized_job():
    plan = _plan()
    job = plan.jobs[0]
    observation = observe_call(
        plan, job.job_id, job.sql,
        status="SUCCESS", rows=({"contract_count": "4"},),
    )
    assert observation.plan_id == plan.plan_id
    assert observation.dataset_id == job.dataset_id
    assert observation.metric_status("contract_count") is MetricStatus.VALID
