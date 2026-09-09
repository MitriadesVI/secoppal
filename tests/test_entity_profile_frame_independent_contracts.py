"""Independent positive contract checks and original CORE numeric ports."""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json
import socket

from hypothesis import given, settings, strategies as st
import pytest

from app.core.entity_profile.frame import EntityProfileFrame, FrameSchemaError
from app.core.entity_profile.frame_evidence import MetricStatus, accredit_metric
from app.core.entity_profile.frame_migration import MigrationStatus, migrate_legacy_context
from app.core.entity_profile.frame_values import DATE_FIELD, SourceKind
from app.core.entity_profile.patch import (
    EntityProfilePatch, PatchOp, PatchOperation, PatchTarget, TargetKind, apply_patch,
)
from entity_profile_frame_harness import BUYER, TODAY, observe
from test_entity_profile_frame_migration import BUYER_PARAMS, LEGACY_BASE


@pytest.fixture(autouse=True)
def isolated_io(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("independent contracts never call the network")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    monkeypatch.setattr("app.core.entity_profile.claim_adapters.civil_today", lambda _: TODAY)


def test_r1_orders_keep_exact_source_date_columns():
    for phrase in (
        "procesos publicados en julio y contratos firmados en agosto",
        "contratos firmados en agosto y procesos publicados en julio",
    ):
        out = observe(f"analiza {phrase} de {BUYER}")
        assert out.ready
        for source, month in ((SourceKind.PROCESSES, "07"), (SourceKind.CONTRACTS, "08")):
            assert out.sql_for(source)
            for sql in out.sql_for(source):
                assert f"{DATE_FIELD[source]} >= '2026-{month}-01'" in sql


def test_r4_fingerprints_where_and_visible_boolean_are_distinct():
    left = observe(f"analiza contratos de mantenimiento o vias de {BUYER}")
    right = observe(f"analiza contratos de mantenimiento y vias de {BUYER}")
    assert left.ready and right.ready
    source = SourceKind.CONTRACTS
    a, b = left.decision.plan.source_plans[source], right.decision.plan.source_plans[source]
    assert a.fingerprint != b.fingerprint and a.where != b.where
    assert left.scope_lines != right.scope_lines


def test_exact_roundtrip_and_explicit_keep_set_clear_conflict():
    frame = observe(f"analiza contratos de software de {BUYER} en agosto").frame
    payload = frame.to_dict()
    revived = EntityProfileFrame.from_dict(json.loads(json.dumps(payload)))
    assert revived.to_dict() == payload
    target = PatchTarget(TargetKind.FOCUS)
    patch = EntityProfilePatch("audit", "t", base_frame_ref=(frame.frame_id, frame.revision))
    kept = apply_patch(frame, replace(patch, operations=(PatchOp(target, PatchOperation.KEEP),)))
    assert kept.frame.focus == frame.focus
    assert kept.frame.source_predicates == frame.source_predicates
    changed = apply_patch(frame, replace(patch, operations=(
        PatchOp(target, PatchOperation.SET, "suppliers"),
    )))
    assert changed.frame.focus == "suppliers" and frame.focus == "overview"
    conflict = apply_patch(frame, replace(patch, operations=(
        PatchOp(target, PatchOperation.SET, "suppliers"),
        PatchOp(target, PatchOperation.SET, "values"),
    )))
    assert conflict.frame is None and conflict.reason == "CONFLICTING_OPERATIONS"
    clear = apply_patch(frame, replace(patch, operations=(
        PatchOp(PatchTarget(TargetKind.TEMPORAL, SourceKind.CONTRACTS), PatchOperation.CLEAR),
    )))
    assert clear.frame.predicates_for(SourceKind.CONTRACTS).temporal.period.is_unbounded


@pytest.mark.parametrize("version", [None, 0, 1, 3, "1", 999])
def test_unknown_frame_version_rejected(version):
    payload = observe(f"analiza contratos de {BUYER}").frame.to_dict()
    payload["schema_version"] = version
    with pytest.raises((FrameSchemaError, ValueError)):
        EntityProfileFrame.from_dict(payload)


@pytest.mark.parametrize("raw", ["1e-999", "4.0000000000000001", "9007199254740993"])
def test_original_decimal_count_invariant(raw):
    metric = accredit_metric("contract_count", raw)
    exact = Decimal(raw)
    if exact != exact.to_integral_value():
        assert metric.status is MetricStatus.INVALID and metric.value is None
    else:
        assert metric.status is MetricStatus.VALID and metric.value == int(exact)


@settings(max_examples=16, derandomize=True, deadline=None)
@given(exponent=st.integers(min_value=325, max_value=999))
def test_original_underflow_property_never_accredits_zero(exponent):
    metric = accredit_metric("contract_count", f"1e-{exponent}")
    assert metric.status is MetricStatus.INVALID and metric.value is None


def test_singular_legacy_unsupported_event_remains_blocked():
    result = migrate_legacy_context({**LEGACY_BASE, "unsupported_events": [],
                                     "unsupported_event": "terminados"}, BUYER_PARAMS)
    assert result.status is MigrationStatus.BLOCKED and result.frame is None


@pytest.mark.parametrize("phrase", ["", "contratos de", "procesos de", "proveedores de contratos de"])
def test_utility_is_ready_and_sources_are_only_planned(phrase):
    out = observe(f"analiza {phrase} {BUYER}")
    assert out.ready and out.jobs
    assert out.decision.plan.planned_sources
    assert not hasattr(out.decision.plan, "executed_datasets")
