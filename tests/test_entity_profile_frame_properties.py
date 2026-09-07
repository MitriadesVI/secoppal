"""Propiedades del core: invariantes que ningún ejemplo enumerado cubre.

Deterministas y sin red. Cada una expresa una regla del diseño congelado, no
una tautología sobre la estructura de datos.
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json

from hypothesis import given, settings, strategies as st
import pytest

from app.core.entity_profile.frame import (
    BuyerScope,
    BuyerScopeKind,
    EntityProfileFrame,
    SourcePredicates,
    SourceSelection,
    TemporalPredicate,
)
from app.core.entity_profile.frame_compiler import PlanStatus, compile_profile
from app.core.entity_profile.frame_evidence import MetricStatus, accredit_metric
from app.core.entity_profile.frame_migration import MigrationStatus, migrate_legacy_context
from app.core.entity_profile.frame_predicates import (
    UnsupportedPredicateError,
    compile_predicates,
)
from app.core.entity_profile.frame_values import (
    Period,
    Selection,
    SourceKind,
    SourceOrigin,
    StateExpression,
    StatePredicate,
    TemporalEventKind,
    TopicAll,
    TopicAny,
    TopicNot,
    TopicTerm,
    TopicTrue,
    ValueConstraint,
)
from app.core.entity_profile.semantic_request import EntityProfileSemanticRequest
from app.core.entity_profile.source_plan import project_source
from app.core.entity_profile.source_request import RequestedSources, SourceProvenance
from app.core.entity_profile.patch import (
    EntityProfilePatch,
    PatchOp,
    PatchOperation,
    PatchTarget,
    TargetKind,
    apply_patch,
)
from app.core.soql_builder import SoQLBuilder


BUYER = BuyerScope(
    kind=BuyerScopeKind.BUYER_UNIT, display_name="ALCALDIA MUNICIPAL DE BARANOA",
    canonical_buyers=("ALCALDIA MUNICIPAL DE BARANOA",),
    authority="entity_resolver", authority_version="1",
)

SETTINGS = settings(max_examples=25, derandomize=True, deadline=None)

terms = st.sampled_from(["mantenimiento", "vias", "software", "obra", "salud"])
periods = st.sampled_from([
    Period(), Period("2026-08-01", "2026-08-31"), Period("2025-01-01", "2025-12-31"),
])
events = st.sampled_from(list(TemporalEventKind))
focuses = st.sampled_from(
    ["overview", "suppliers", "values", "modalities", "recent", "temporal"],
)
source_sets = st.sampled_from([
    frozenset({SourceKind.PROCESSES}),
    frozenset({SourceKind.CONTRACTS}),
    frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS}),
])


@st.composite
def topics(draw, depth: int = 2):
    if depth == 0:
        return TopicTerm(draw(terms))
    kind = draw(st.sampled_from(["term", "all", "any", "not", "true"]))
    if kind == "term":
        return TopicTerm(draw(terms))
    if kind == "true":
        return TopicTrue()
    if kind == "not":
        return TopicNot(TopicTerm(draw(terms)))
    children = (draw(topics(depth - 1)), draw(topics(depth - 1)))
    return TopicAll(children) if kind == "all" else TopicAny(children)


@st.composite
def predicates(draw, source: SourceKind):
    return SourcePredicates(
        source=source,
        temporal=TemporalPredicate(event=draw(events), period=draw(periods)),
        current_state=draw(st.sampled_from([
            StateExpression(),
            StateExpression((StatePredicate(
                "estado_del_procedimiento" if source is SourceKind.PROCESSES
                else "estado_contrato", ("Publicado",),
            ),)),
        ])),
        topic=draw(topics()),
        modality=draw(st.sampled_from([Selection(), Selection(("licitacion_publica",))])),
        value=draw(st.sampled_from([
            ValueConstraint(), ValueConstraint(lower=Decimal("100000000")),
        ])),
    )


@st.composite
def frames(draw):
    selection = SourceSelection(
        requested=draw(source_sets), origin=draw(st.sampled_from(list(SourceOrigin))),
    )
    return EntityProfileFrame(
        frame_id="f-1", revision=1, buyer_scope=BUYER, focus=draw(focuses),
        source_selection=selection,
        source_predicates={s: draw(predicates(s)) for s in selection.ordered},
    )


def _patch(frame, *operations):
    return EntityProfilePatch(
        patch_id="p", turn_id="t", operations=tuple(operations),
        base_frame_ref=(frame.frame_id, frame.revision),
    )


# 1
@SETTINGS
@given(frame=frames())
def test_frame_roundtrip_preserves_semantic_equality(frame):
    revived = EntityProfileFrame.from_dict(json.loads(json.dumps(frame.to_dict())))
    assert revived.semantic_equal(frame)


# 2
@SETTINGS
@given(frame=frames(), focus=focuses)
def test_keep_preserves_every_unrelated_dimension(frame, focus):
    outcome = apply_patch(frame, _patch(
        frame, PatchOp(PatchTarget(TargetKind.FOCUS), PatchOperation.SET, focus),
    ))
    assert outcome.frame is not None
    for source in frame.sources:
        assert outcome.frame.predicates_for(source) == frame.predicates_for(source)


# 3
@SETTINGS
@given(frame=frames())
def test_clear_is_idempotent(frame):
    source = frame.sources[0]
    clear = PatchOp(PatchTarget(TargetKind.TEMPORAL, source), PatchOperation.CLEAR)
    once = apply_patch(frame, _patch(frame, clear)).frame
    twice = apply_patch(once, _patch(once, clear)).frame
    assert once.predicates_for(source) == twice.predicates_for(source)


# 4
@SETTINGS
@given(frame=frames(), topic=topics(), focus=focuses)
def test_independent_patches_commute(frame, topic, focus):
    source = frame.sources[0]
    set_focus = PatchOp(PatchTarget(TargetKind.FOCUS), PatchOperation.SET, focus)
    set_topic = PatchOp(
        PatchTarget(TargetKind.TOPIC, source), PatchOperation.SET, topic,
    )
    first = apply_patch(frame, _patch(frame, set_focus, set_topic)).frame
    second = apply_patch(frame, _patch(frame, set_topic, set_focus)).frame
    assert first.fingerprint() == second.fingerprint()


# 5
@SETTINGS
@given(frame=frames(), focus=focuses)
def test_building_a_candidate_never_mutates_the_base_frame(frame, focus):
    before = frame.to_dict()
    apply_patch(frame, _patch(
        frame, PatchOp(PatchTarget(TargetKind.FOCUS), PatchOperation.SET, focus),
    ))
    assert frame.to_dict() == before


# 6 y 8
@SETTINGS
@given(first=periods, second=periods)
def test_each_source_retains_its_own_period(first, second):
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=BUYER, focus="overview",
        source_selection=SourceSelection(
            frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS}),
            SourceOrigin.EXPLICIT,
        ),
        source_predicates={
            SourceKind.PROCESSES: SourcePredicates(
                SourceKind.PROCESSES, TemporalPredicate(period=first),
            ),
            SourceKind.CONTRACTS: SourcePredicates(
                SourceKind.CONTRACTS, TemporalPredicate(period=second),
            ),
        },
    )
    decision = compile_profile(frame, soql_builder=SoQLBuilder())
    assert decision.status is PlanStatus.READY
    plans = decision.plan.source_plans
    for source, period in (
        (SourceKind.PROCESSES, first), (SourceKind.CONTRACTS, second),
    ):
        where = plans[source].where
        if period.lower:
            assert period.lower in where
        other = second if source is SourceKind.PROCESSES else first
        if other.lower and other.lower != period.lower:
            assert other.lower not in where


# 7
@SETTINGS
@given(source=st.sampled_from(list(SourceKind)), event=events)
def test_an_event_subject_is_never_reassigned_to_fit(source, event):
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=BUYER, focus="overview",
        source_selection=SourceSelection(frozenset({source}), SourceOrigin.EXPLICIT),
        source_predicates={
            source: SourcePredicates(source, TemporalPredicate(event=event)),
        },
    )
    decision = compile_profile(frame, soql_builder=SoQLBuilder())
    if event.source is not None and event.source is not source:
        assert decision.status is PlanStatus.NEEDS_CLARIFICATION
        assert decision.plan is None
    else:
        assert decision.status is PlanStatus.READY


# 9
@SETTINGS
@given(left=terms, right=terms)
def test_any_and_all_compile_differently(left, right):
    builder = SoQLBuilder()
    source = SourceKind.CONTRACTS
    conjunction = compile_predicates(
        builder, source,
        SourcePredicates(source, topic=TopicAll((TopicTerm(left), TopicTerm(right)))),
        BUYER,
    ).where
    disjunction = compile_predicates(
        builder, source,
        SourcePredicates(source, topic=TopicAny((TopicTerm(left), TopicTerm(right)))),
        BUYER,
    ).where
    if left == right:
        return
    assert conjunction != disjunction


# 10
@SETTINGS
@given(left=terms, right=terms)
def test_an_unsupported_tree_is_never_approximated(left, right):
    unsupported = SourcePredicates(
        SourceKind.CONTRACTS,
        topic=TopicNot(TopicAny((TopicTerm(left), TopicTerm(right)))),
    )
    with pytest.raises(UnsupportedPredicateError):
        compile_predicates(SoQLBuilder(), SourceKind.CONTRACTS, unsupported, BUYER)


# 11 y 12
@SETTINGS
@given(focus=focuses, origin=st.sampled_from(list(SourceOrigin)))
def test_explicit_sources_never_vanish_and_default_ones_only_narrow(focus, origin):
    requested = frozenset({SourceKind.PROCESSES, SourceKind.CONTRACTS})
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=BUYER, focus=focus,
        source_selection=SourceSelection(requested, origin),
        source_predicates={s: SourcePredicates(s) for s in SourceKind},
    )
    decision = compile_profile(frame, soql_builder=SoQLBuilder())
    from app.core.entity_profile.frame_compiler import FOCUS_CAPABILITY

    capability = FOCUS_CAPABILITY[focus]
    if origin is SourceOrigin.EXPLICIT and requested - capability:
        assert decision.status is PlanStatus.NEEDS_CLARIFICATION
        return
    assert decision.status is PlanStatus.READY
    planned = decision.plan.planned_sources
    assert planned <= requested and planned <= capability
    if planned != requested:
        assert decision.plan.limitations


# 13
@SETTINGS
@given(raw=st.sampled_from(["NaN", "Infinity", "-Infinity", "nan", "-inf"]))
def test_nonfinite_metrics_never_accredit_an_amount(raw):
    metric = accredit_metric("total_contract_value", raw)
    assert metric.status is not MetricStatus.VALID and metric.value is None


# 14
@SETTINGS
@given(request_month=st.sampled_from(["07", "08"]),
       params_month=st.sampled_from(["07", "08"]))
def test_an_invalid_migration_never_yields_an_executable_frame(
    request_month, params_month,
):
    legacy = {
        "focus": "overview", "sources": "CONTRACTS",
        "sources_provenance": "EXPLICIT",
        "time_from": f"2026-{request_month}-01", "time_to": f"2026-{request_month}-28",
        "event": "NONE", "temporal_claims": [], "unsupported_events": [],
    }
    params = {
        "entidad_resolved": "ALCALDIA MUNICIPAL DE BARANOA",
        "fecha_desde": f"2026-{params_month}-01",
        "fecha_hasta": f"2026-{params_month}-28",
    }
    outcome = migrate_legacy_context(legacy, params)
    if request_month == params_month:
        assert outcome.status is MigrationStatus.MIGRATED
    else:
        assert outcome.status is MigrationStatus.INVALID_CONTEXT
        assert outcome.frame is None


@SETTINGS
@given(period=periods)
def test_compiled_plan_is_unchanged_when_caller_inputs_are_mutated(period):
    source = SourceKind.CONTRACTS
    predicates_by_source = {
        source: SourcePredicates(source, TemporalPredicate(period=period)),
    }
    frame = EntityProfileFrame(
        frame_id="immutable", revision=1, buyer_scope=BUYER, focus="overview",
        source_selection=SourceSelection(frozenset({source}), SourceOrigin.EXPLICIT),
        source_predicates=predicates_by_source,
    )
    plan = compile_profile(frame, soql_builder=SoQLBuilder()).plan
    before = (
        plan.source_plans[source].predicates.fingerprint(),
        plan.source_plans[source].where,
        tuple(job.sql for job in plan.jobs),
    )

    predicates_by_source.clear()

    assert (
        plan.source_plans[source].predicates.fingerprint(),
        plan.source_plans[source].where,
        tuple(job.sql for job in plan.jobs),
    ) == before
    with pytest.raises(TypeError):
        plan.source_plans[source] = plan.source_plans[source]


@SETTINGS
@given(term=terms)
def test_source_plan_takes_a_deeply_immutable_snapshot(term):
    params = {"objeto": [term], "nested": {"items": [term]}}
    request = EntityProfileSemanticRequest(
        focus="overview",
        sources=RequestedSources.CONTRACTS,
        sources_provenance=SourceProvenance.EXPLICIT,
    )
    plan = project_source(request, params, SourceKind.CONTRACTS.dataset_id)
    before = (plan.params["objeto"], plan.params["nested"]["items"])

    params["objeto"].append("mutated")
    params["nested"]["items"].clear()

    assert (plan.params["objeto"], plan.params["nested"]["items"]) == before
    with pytest.raises((TypeError, AttributeError)):
        plan.params["objeto"].append("mutated")
    with pytest.raises(TypeError):
        plan.params["nested"]["items"] = ("mutated",)


def test_a_conflicting_patch_has_no_candidate():
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=BUYER, focus="overview",
        source_selection=SourceSelection(
            frozenset({SourceKind.CONTRACTS}), SourceOrigin.EXPLICIT,
        ),
        source_predicates={SourceKind.CONTRACTS: SourcePredicates(SourceKind.CONTRACTS)},
    )
    target = PatchTarget(TargetKind.FOCUS)
    outcome = apply_patch(frame, _patch(
        frame,
        PatchOp(target, PatchOperation.SET, "suppliers"),
        PatchOp(target, PatchOperation.SET, "values"),
    ))
    assert outcome.frame is None and outcome.reason == "CONFLICTING_OPERATIONS"


def test_clear_cannot_empty_the_universe():
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=BUYER, focus="overview",
        source_selection=SourceSelection(
            frozenset({SourceKind.CONTRACTS}), SourceOrigin.EXPLICIT,
        ),
        source_predicates={SourceKind.CONTRACTS: SourcePredicates(SourceKind.CONTRACTS)},
    )
    outcome = apply_patch(frame, _patch(frame, PatchOp(
        PatchTarget(TargetKind.SOURCE_SELECTION), PatchOperation.CLEAR,
    )))
    assert outcome.frame is None
    assert outcome.reason == "CLEAR_ON_REQUIRED_DIMENSION"
    with pytest.raises(ValueError):
        replace(frame, source_selection=SourceSelection(frozenset(), SourceOrigin.DEFAULT))
