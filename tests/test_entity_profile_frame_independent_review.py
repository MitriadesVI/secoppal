"""Independent V1.2-A audit: intended invariants, with unhidden red tests."""
from __future__ import annotations

from dataclasses import asdict, replace
import json
import os
from pathlib import Path
import socket

import pytest

from app.core.entity_profile import claim_composition, frame_compiler, frame_predicates
from app.core.entity_profile.claims import ClaimDimension
from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.frame_compiler import PlanStatus, compile_profile
from app.core.entity_profile.frame_evidence import MetricStatus, bind_observation
from app.core.entity_profile.frame_migration import MigrationStatus, migrate_legacy_context
from app.core.entity_profile.frame_render import build_compiled_scope, render_compiled_scope
from app.core.entity_profile.frame_values import SourceKind, TopicAll, TopicAny
from app.core.soql_builder import SoQLBuilder
from entity_profile_frame_harness import BUYER, TODAY, observe
from test_entity_profile_frame_migration import BUYER_PARAMS, LEGACY_BASE


@pytest.fixture(autouse=True)
def isolated_io(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("independent audit never calls the network")
    monkeypatch.setattr(socket.socket, "connect", blocked)
    # The author's harness fixes QueryRouter's clock but omitted the claims clock.
    monkeypatch.setattr("app.core.entity_profile.claim_adapters.civil_today", lambda _: TODAY)


def save(name, value):
    output = os.environ.get("FRAME_REVIEW_OUTPUT")
    if output:
        Path(output, name + ".json").write_text(
            json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n",
        )


def snapshot(out):
    return {
        "query": out.query, "params": out.params,
        "claims": [asdict(c) for c in out.claims.claims],
        "frame": out.frame.to_dict() if out.frame else None,
        "status": out.status, "issues": out.issues,
        "jobs": [{"source": j.source.value, "sql": j.sql} for j in out.jobs],
        "scope": out.scope_lines,
    }


@pytest.mark.parametrize("case,phrase,source,terms", [
    ("R3-A", "contratos de software y gestion de procesos industriales",
     SourceKind.CONTRACTS, ("software", "gestion", "procesos", "industriales")),
    ("R3-B", "contratos de software, gestion de procesos industriales",
     SourceKind.CONTRACTS, ("software", "gestion", "procesos", "industriales")),
    ("R3-C", "procesos de software y administracion de contratos",
     SourceKind.PROCESSES, ("software", "administracion", "contratos")),
])
def test_r3_full_topic_survives_every_layer(case, phrase, source, terms):
    out = observe(f"analiza {phrase} de {BUYER}")
    save(case, snapshot(out))
    assert out.ready and out.sources == frozenset({source})
    topic_claims = out.claims.by_dimension(ClaimDimension.TOPIC)
    layers = {
        "parser": repr(out.params.get("objeto")),
        "claim": repr([c.canonical_value for c in topic_claims]),
        "frame": repr(out.predicates(source).topic),
        "sql": "\n".join(out.sql),
        "visible_topic": "\n".join(s for s in out.scope_lines if "Tema:" in s),
    }
    lost = {layer: [t for t in terms if t not in value.lower()]
            for layer, value in layers.items()}
    assert not any(lost.values()), lost


@pytest.mark.parametrize("phrase,source,month,event,state_field", [
    ("procesos publicados en julio actualmente abiertos", SourceKind.PROCESSES,
     "07", "PROCESS_PUBLICATION", "estado_del_procedimiento"),
    ("contratos firmados en agosto actualmente terminados", SourceKind.CONTRACTS,
     "08", "CONTRACT_SIGNATURE", "estado_contrato"),
])
def test_supported_event_and_current_state_both_survive(phrase, source, month, event, state_field):
    out = observe(f"analiza {phrase} de {BUYER}")
    save("R2-" + source.value, snapshot(out))
    assert out.ready, out.issues
    predicate = out.predicates(source)
    assert predicate.temporal.event.value == event
    assert predicate.temporal.period.lower == f"2026-{month}-01"
    assert not predicate.current_state.is_true
    assert all(state_field in sql and f"2026-{month}-01" in sql for sql in out.sql)


@pytest.mark.parametrize("phrase", [
    "contratos terminados en agosto pero actualmente activos",
    "contratos terminados en agosto que actualmente estan activos",
    "contratos terminados en agosto aunque actualmente activos",
    "hoy los contratos terminados en agosto",
])
def test_unsupported_history_never_executes(phrase):
    out = observe(f"analiza {phrase} de {BUYER}")
    assert out.clarifies and not out.jobs


@pytest.mark.parametrize("request_delta,params_delta", [
    ({"topic": ["software"]}, {"objeto": ["mantenimiento"]}),
    ({"topic": ["software", "vias"]}, {"objeto": [["software", "vias"]]}),
    ({"value_min": 100}, {"valor_min": 200}),
    ({}, {"modalidad_exclude_procedure_ids": ["contratacion_directa", "contratacion_directa_con_ofertas"]}),
])
def test_migration_never_prefers_a_conflicting_material_copy(request_delta, params_delta):
    outcome = migrate_legacy_context(
        {**LEGACY_BASE, **request_delta}, {**BUYER_PARAMS, **params_delta},
    )
    decision = compile_profile(outcome.frame, soql_builder=SoQLBuilder()) if outcome.frame else None
    save("migration-" + next(iter(params_delta)), {
        "request": {**LEGACY_BASE, **request_delta},
        "params": {**BUYER_PARAMS, **params_delta},
        "migration": outcome.status.value,
        "frame": outcome.frame.to_dict() if outcome.frame else None,
        "compile": decision.status.value if decision else None,
        "sql": [j.sql for j in decision.plan.jobs] if decision and decision.plan else [],
    })
    assert outcome.status is MigrationStatus.INVALID_CONTEXT, (
        outcome.status, outcome.frame.to_dict() if outcome.frame else outcome.reason,
    )


@pytest.mark.parametrize("span", [[3], [-1, 3], [True, 5], ["1", 5], [5, 5]])
def test_malformed_spans_fail_closed(span):
    request = {**LEGACY_BASE, "temporal_claims": [
        {"event": "CONTRACT_SIGNATURE", "span": span},
    ]}
    result = migrate_legacy_context(request, BUYER_PARAMS)
    assert result.status is MigrationStatus.INVALID_CONTEXT and result.frame is None


def test_original_legacy_state_port_is_not_silently_dropped():
    out = observe(f"analiza contratos de {BUYER}", overrides={
        "estado": ["Suspendido"], "estado_field": "estado_contrato",
    })
    save("legacy-state-port", snapshot(out))
    if out.clarifies:
        assert not out.jobs
        return
    assert out.ready
    assert all("estado_contrato" in sql and "Suspendido" in sql for sql in out.sql)
    assert any("Suspendido" in line for line in out.scope_lines)


def test_after_frame_legacy_mutation_cannot_restore_cleared_period(monkeypatch):
    from entity_profile_frame_harness import conversation

    turns = conversation((f"analiza contratos firmados de {BUYER} en agosto",
                          "sin filtro de fecha"))
    out = turns[-1]
    assert out.ready
    frame = EntityProfileFrame.from_dict(out.frame.to_dict())
    out.params.update(fecha_desde="2026-08-01", fecha_hasta="2026-08-31")

    def forbidden(*args, **kwargs):
        raise AssertionError("legacy decision layer was called")

    monkeypatch.setattr(SoQLBuilder, "_build_where", forbidden)
    decision = compile_profile(frame, soql_builder=SoQLBuilder())
    assert decision.status is PlanStatus.READY
    assert frame.predicates_for(SourceKind.CONTRACTS).temporal.period.is_unbounded
    assert all("2026-08-01" not in j.sql for j in decision.plan.jobs)


def test_compiled_plan_does_not_allow_scope_to_diverge_from_frozen_sql():
    july = observe(f"analiza contratos firmados en julio de {BUYER}").decision.plan
    august = observe(f"analiza contratos firmados en agosto de {BUYER}").decision.plan
    before = render_compiled_scope(build_compiled_scope(july))
    try:
        july.source_plans[SourceKind.CONTRACTS] = august.source_plans[SourceKind.CONTRACTS]
    except TypeError:
        return
    after = render_compiled_scope(build_compiled_scope(july))
    save("mutable-plan", {"before": before, "after": after,
                          "sql": [j.sql for j in july.jobs]})
    assert after == before, "unchanged plan/jobs now render a different month"


@pytest.mark.parametrize("wrong", ["job_id", "fingerprint", "dataset_id", "sql"])
def test_evidence_cannot_accredit_foreign_job_metadata(wrong):
    plan = observe(f"analiza contratos firmados en agosto de {BUYER}").decision.plan
    job = plan.jobs[0]
    metadata = dict(plan_id=plan.plan_id, job_id=job.job_id,
                    fingerprint=plan.source_plans[job.source].fingerprint,
                    dataset_id=job.dataset_id, sql=job.sql)
    metadata[wrong] = "foreign-to-authorizing-plan"
    try:
        call = bind_observation(**metadata, status="SUCCESS", rows=({"contract_count": "4"},))
    except ValueError:
        return
    assert call.metric_status("contract_count") is not MetricStatus.VALID, (
        "no plan/job correspondence check exists", wrong,
    )


@pytest.mark.parametrize("mutation", ["globalize_period", "any_to_all", "drop_modality", "legacy_params"])
def test_required_mutation_is_killed_by_original_invariant(monkeypatch, mutation):
    """Run the SAME assertion before/after injection; mutation must fail it."""
    from test_entity_profile_frame_core import (
        test_r1_each_source_keeps_its_own_period,
        test_r4_negative_modality_is_executed_and_declared,
        test_r4_parser_or_compiles_differently_from_and,
    )

    if mutation == "globalize_period":
        invariant = test_r1_each_source_keeps_its_own_period
        original = claim_composition._temporal

        def changed(claims, owner):
            result = original(claims, owner)
            if result is None:
                return None
            period = claims.by_dimension(ClaimDimension.PERIOD)[-1].canonical_value
            return replace(result[0], period=period), result[1]

        target, name = claim_composition, "_temporal"
    elif mutation == "any_to_all":
        invariant = test_r4_parser_or_compiles_differently_from_and
        original = frame_predicates._topic_clause

        def changed(builder, spec, node):
            if isinstance(node, TopicAny):
                node = TopicAll(node.children)
            return original(builder, spec, node)

        target, name = frame_predicates, "_topic_clause"
    elif mutation == "drop_modality":
        invariant = test_r4_negative_modality_is_executed_and_declared

        def changed(*args):
            return ""

        target, name = frame_predicates, "_modality_clause"
    else:
        def invariant():
            out = observe(f"analiza contratos firmados en agosto de {BUYER}")
            assert out.ready and all("2026-08-01" in sql for sql in out.sql)

        def changed(self, spec, params, *, allow_global=False):
            return self._builder._build_where(spec, params, allow_global=allow_global)

        target, name = frame_compiler._FrozenWhereBuilder, "_build_where"
    invariant()
    monkeypatch.setattr(target, name, changed)
    with pytest.raises(AssertionError):
        invariant()
