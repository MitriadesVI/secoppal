"""Cada mutación debe MATAR una aserción conductual, no un import.

Un test que sólo revienta cuando el módulo desaparece no prueba nada sobre el
contrato. Aquí se altera el comportamiento —se intercambia el sujeto, se
globaliza el periodo, se aplana un OR— y se comprueba que el core cambia lo
que RESPONDE. Si una mutación pasara inadvertida, la invariante que dice
protegerla no estaría protegida por ninguna aserción.
"""
from __future__ import annotations

from decimal import Decimal
import socket

import pytest

import app.core.query_router as query_router
from app.core.entity_profile import (
    claim_composition,
    frame_compiler,
    frame_evidence,
    frame_migration,
    frame_predicates,
)
from app.core.entity_profile.frame_evidence import MetricStatus
from app.core.entity_profile.frame_values import (
    SourceKind,
    SourceOrigin,
    TemporalEventKind,
    TopicAny,
    TopicNot,
    TopicTerm,
)
from entity_profile_frame_harness import BUYER, observe


@pytest.fixture(autouse=True)
def prevent_external_io(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("mutation tests never touch the network")
    monkeypatch.setattr(socket.socket, "connect", blocked)


SWAPPED = f"analiza contratos publicados y procesos firmados en agosto de {BUYER}"
SPLIT_PERIODS = (
    f"analiza procesos publicados en julio y contratos firmados en agosto de {BUYER}"
)


def test_mutation_swapping_the_event_subject_is_killed(monkeypatch):
    """Asignar el hecho por compatibilidad física convierte un bloqueo en READY."""
    assert observe(SWAPPED).clarifies

    original = claim_composition._temporal

    def by_physical_compatibility(claims, owner):
        from app.core.entity_profile.claims import ClaimDimension
        from app.core.entity_profile.frame import TemporalPredicate

        if owner is None:
            return original(claims, owner)
        events = claims.by_dimension(ClaimDimension.EVENT)
        fitting = [
            c for c in events if c.canonical_value.source is owner.canonical_value
        ]
        if not fitting:
            return original(claims, owner)
        periods = claims.by_dimension(ClaimDimension.PERIOD)
        period = periods[0].canonical_value if periods else None
        base = original(claims, owner)
        predicate = TemporalPredicate(
            event=fitting[0].canonical_value,
            period=period or (base[0].period if base else None),
        )
        return predicate, (fitting[0],)

    monkeypatch.setattr(claim_composition, "_temporal", by_physical_compatibility)
    assert not observe(SWAPPED).clarifies, "la mutación no cambió el comportamiento"


def test_mutation_globalizing_the_period_is_killed(monkeypatch):
    """Un periodo común borra la ventana propia de cada fuente."""
    clean = observe(SPLIT_PERIODS)
    assert clean.ready
    assert not any("2026-08-01" in sql for sql in clean.sql_for(SourceKind.PROCESSES))

    original = claim_composition._temporal

    def globalized(claims, owner):
        from app.core.entity_profile.claims import ClaimDimension
        from app.core.entity_profile.frame import TemporalPredicate

        result = original(claims, owner)
        if result is None:
            return None
        periods = claims.by_dimension(ClaimDimension.PERIOD)
        if not periods:
            return result
        predicate, evidence = result
        return TemporalPredicate(
            event=predicate.event, period=periods[-1].canonical_value,
        ), evidence

    monkeypatch.setattr(claim_composition, "_temporal", globalized)
    mutated = observe(SPLIT_PERIODS)
    assert any("2026-08-01" in sql for sql in mutated.sql_for(SourceKind.PROCESSES))


def test_mutation_flattening_any_into_all_is_killed(monkeypatch):
    disjunction = f"analiza contratos de mantenimiento o vias de {BUYER}"
    conjunction = f"analiza contratos de mantenimiento y vias de {BUYER}"
    assert observe(disjunction).sql != observe(conjunction).sql

    original = frame_predicates._topic_clause

    def flattened(builder, spec, node):
        if isinstance(node, TopicAny):
            from app.core.entity_profile.frame_values import TopicAll

            return original(builder, spec, TopicAll(node.children))
        return original(builder, spec, node)

    monkeypatch.setattr(frame_predicates, "_topic_clause", flattened)
    assert observe(disjunction).sql == observe(conjunction).sql


def test_mutation_omitting_the_negative_modality_is_killed(monkeypatch):
    query = f"analiza contratos excepto contratacion directa de {BUYER}"
    assert any("modalidad_de_contratacion NOT IN" in sql for sql in observe(query).sql)

    original = frame_predicates._modality_clause

    def include_only(builder, source, predicates):
        from dataclasses import replace

        from app.core.entity_profile.frame_values import Selection

        stripped = replace(
            predicates, modality=Selection(include=predicates.modality.include),
        )
        return original(builder, source, stripped)

    monkeypatch.setattr(frame_predicates, "_modality_clause", include_only)
    assert not any(
        "modalidad_de_contratacion NOT IN" in sql for sql in observe(query).sql
    )


def test_mutation_using_provenance_instead_of_origin_is_killed(monkeypatch):
    """Tratar una fuente EXPLÍCITA como heredada la deja acotarse en silencio."""
    query = f"analiza proveedores de procesos y contratos de {BUYER}"
    assert observe(query).clarifies

    original = frame_compiler.compile_profile

    def as_inherited(frame, **kwargs):
        from dataclasses import replace

        from app.core.entity_profile.frame import SourceSelection

        demoted = replace(
            frame,
            source_selection=SourceSelection(
                frame.source_selection.requested, SourceOrigin.DEFAULT,
            ),
        )
        return original(demoted, **kwargs)

    monkeypatch.setattr(frame_compiler, "compile_profile", as_inherited)
    import entity_profile_frame_harness as harness

    monkeypatch.setattr(harness, "compile_profile", as_inherited)
    assert observe(query).ready


def test_mutation_accepting_an_invalid_legacy_context_is_killed(monkeypatch):
    legacy = {
        "focus": "overview", "sources": "CONTRACTS", "sources_provenance": "EXPLICIT",
        "time_from": "2026-08-01", "time_to": "2026-08-31", "event": "NONE",
        "temporal_claims": [], "unsupported_events": [],
    }
    params = {
        "entidad_resolved": "ALCALDIA MUNICIPAL DE BARANOA",
        "fecha_desde": "2026-07-01", "fecha_hasta": "2026-07-31",
    }
    assert migrate_status(legacy, params) == "INVALID_CONTEXT"

    def prefer_the_request(request, params):
        from app.core.entity_profile.frame_values import Period

        return Period(lower=request.get("time_from"), upper=request.get("time_to"))

    monkeypatch.setattr(frame_migration, "_period", prefer_the_request)
    assert migrate_status(legacy, params) == "MIGRATED"


def test_mutation_reading_params_after_the_frame_is_killed(monkeypatch):
    """El WHERE congelado es la única entrada de los aspectos."""
    query = f"analiza contratos de {BUYER} en agosto"
    assert observe(query).ready

    def delegate_to_params(self, spec, params, *, allow_global=False):
        return self._builder._build_where(spec, params, allow_global=allow_global)

    monkeypatch.setattr(
        frame_compiler._FrozenWhereBuilder, "_build_where", delegate_to_params,
    )
    assert not observe(query).ready


def test_mutation_approximating_an_unsupported_filter_is_killed(monkeypatch):
    from app.core.entity_profile.frame import (
        BuyerScope,
        BuyerScopeKind,
        EntityProfileFrame,
        SourcePredicates,
        SourceSelection,
    )

    buyer = BuyerScope(
        BuyerScopeKind.BUYER_UNIT, "ALCALDIA MUNICIPAL DE BARANOA",
        ("ALCALDIA MUNICIPAL DE BARANOA",), "entity_resolver", "1",
    )
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=buyer, focus="values",
        source_selection=SourceSelection(
            frozenset({SourceKind.CONTRACTS}), SourceOrigin.EXPLICIT,
        ),
        source_predicates={SourceKind.CONTRACTS: SourcePredicates(
            SourceKind.CONTRACTS,
            topic=TopicNot(TopicAny((TopicTerm("obra"), TopicTerm("vias")))),
        )},
    )
    from app.core.soql_builder import SoQLBuilder

    assert frame_compiler.compile_profile(
        frame, soql_builder=SoQLBuilder(),
    ).status.value == "NEEDS_CLARIFICATION"

    original = frame_predicates._topic_clause

    def approximate(builder, spec, node):
        if isinstance(node, TopicNot) and not isinstance(node.operand, TopicTerm):
            return original(builder, spec, TopicNot(node.operand.children[0]))
        return original(builder, spec, node)

    monkeypatch.setattr(frame_predicates, "_topic_clause", approximate)
    assert frame_compiler.compile_profile(
        frame, soql_builder=SoQLBuilder(),
    ).status.value == "READY"


def test_mutation_marking_nonfinite_metrics_valid_is_killed(monkeypatch):
    assert frame_evidence.accredit_metric(
        "total_contract_value", "NaN",
    ).status is not MetricStatus.VALID

    def accept_everything(name, raw):
        return frame_evidence.AccreditedMetric(
            name, Decimal("NaN"), MetricStatus.VALID,
        )

    monkeypatch.setattr(frame_evidence, "accredit_metric", accept_everything)
    assert frame_evidence.accredit_metric(
        "total_contract_value", "NaN",
    ).status is MetricStatus.VALID


def test_mutation_reassigning_an_incompatible_event_at_compile_is_killed(monkeypatch):
    from app.core.entity_profile.frame import (
        BuyerScope,
        BuyerScopeKind,
        EntityProfileFrame,
        SourcePredicates,
        SourceSelection,
        TemporalPredicate,
    )
    from app.core.soql_builder import SoQLBuilder

    buyer = BuyerScope(
        BuyerScopeKind.BUYER_UNIT, "ALCALDIA MUNICIPAL DE BARANOA",
        ("ALCALDIA MUNICIPAL DE BARANOA",), "entity_resolver", "1",
    )
    frame = EntityProfileFrame(
        frame_id="f", revision=1, buyer_scope=buyer, focus="values",
        source_selection=SourceSelection(
            frozenset({SourceKind.CONTRACTS}), SourceOrigin.EXPLICIT,
        ),
        source_predicates={SourceKind.CONTRACTS: SourcePredicates(
            SourceKind.CONTRACTS,
            TemporalPredicate(event=TemporalEventKind.PROCESS_PUBLICATION),
        )},
    )
    assert frame_compiler.compile_profile(
        frame, soql_builder=SoQLBuilder(),
    ).plan is None

    monkeypatch.setattr(frame_compiler, "_validate_predicates", lambda frame: ())
    assert frame_compiler.compile_profile(
        frame, soql_builder=SoQLBuilder(),
    ).plan is not None


def migrate_status(legacy, params) -> str:
    return frame_migration.migrate_legacy_context(legacy, params).status.value


def test_mutation_restoring_mutable_source_plans_is_killed(monkeypatch):
    def invariant():
        plan = observe(
            f"analiza contratos firmados en agosto de {BUYER}",
        ).decision.plan
        try:
            plan.source_plans[SourceKind.CONTRACTS] = plan.source_plans[
                SourceKind.CONTRACTS
            ]
        except TypeError:
            return
        raise AssertionError("compiled source plans remained mutable")

    invariant()
    monkeypatch.setattr(frame_compiler, "deep_freeze", lambda value: value)
    with pytest.raises(AssertionError):
        invariant()


def test_mutation_accepting_a_forged_job_id_is_killed(monkeypatch):
    plan = observe(f"analiza contratos de {BUYER}").decision.plan
    job = plan.jobs[0]

    def invariant():
        try:
            frame_evidence.bind_observation(
                plan=plan, plan_id=plan.plan_id, job_id="forged-job",
                fingerprint=plan.source_plans[job.source].fingerprint,
                dataset_id=job.dataset_id, sql=job.sql,
                status="SUCCESS", rows=({"contract_count": "4"},),
            )
        except ValueError:
            return
        raise AssertionError("forged job metadata was accredited")

    invariant()
    monkeypatch.setattr(
        frame_evidence, "_authorized_job", lambda authorizing_plan, job_id: job,
    )
    with pytest.raises(AssertionError):
        invariant()


@pytest.mark.parametrize("preferred", ["request", "params"])
def test_mutation_preferring_either_legacy_copy_is_killed(monkeypatch, preferred):
    request = {
        "focus": "overview", "sources": "CONTRACTS",
        "sources_provenance": "EXPLICIT", "time_from": None, "time_to": None,
        "event": "NONE", "temporal_claims": [], "unsupported_events": [],
        "topic": ["software"], "modality": [],
        "value_min": None, "value_max": None,
    }
    params = {
        "entidad_resolved": "ALCALDIA MUNICIPAL DE BARANOA",
        "objeto": ["mantenimiento"],
    }

    def invariant():
        assert migrate_status(request, params) == "INVALID_CONTEXT"

    invariant()
    monkeypatch.setattr(frame_migration, "material_conflict", lambda *args: None)
    if preferred == "params":
        monkeypatch.setattr(
            frame_migration, "legacy_topic", lambda raw: TopicTerm("mantenimiento"),
        )
    with pytest.raises(AssertionError):
        invariant()


def test_mutation_dropping_unambiguous_legacy_state_is_killed(monkeypatch):
    request = {
        "focus": "overview", "sources": "CONTRACTS",
        "sources_provenance": "EXPLICIT", "time_from": None, "time_to": None,
        "event": "NONE", "temporal_claims": [], "unsupported_events": [],
        "topic": [], "modality": [], "value_min": None, "value_max": None,
    }
    params = {
        "entidad_resolved": "ALCALDIA MUNICIPAL DE BARANOA",
        "estado": ["Suspendido"], "estado_field": "estado_contrato",
    }

    def invariant():
        outcome = frame_migration.migrate_legacy_context(request, params)
        state = outcome.frame.source_predicates[SourceKind.CONTRACTS].current_state
        assert not state.is_true and state.values == ("Suspendido",)

    invariant()
    monkeypatch.setattr(frame_migration, "_state", lambda *args: {})
    with pytest.raises(AssertionError):
        invariant()


def test_mutation_consuming_a_thematic_source_word_is_killed(monkeypatch):
    query = f"analiza contratos de gestion de procesos industriales de {BUYER}"

    def invariant():
        topic = observe(query).predicates(SourceKind.CONTRACTS).topic.fingerprint()
        assert "procesos" in topic

    invariant()
    monkeypatch.setattr(query_router, "is_source_word", lambda token: False)
    with pytest.raises(AssertionError):
        invariant()
