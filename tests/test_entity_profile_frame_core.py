"""Contraejemplos CORE del diseño congelado, portados al Frame V1.2-A.

Cada caso viene de la revisión independiente `e024753` y se observa a través
de `entity_profile_frame_harness`, no de la API de V1.1. Los casos que sólo
pueden decidirse con workflow, persistencia o dispatcher —R5/R7 completos—
NO están aquí: pertenecen a V1.2-B y viven marcados en
`test_entity_profile_frame_integration_contract.py`.
"""
from __future__ import annotations

import socket

from hypothesis import given, settings, strategies as st
import pytest

from app.core.entity_profile.claims import ClaimDimension, RelationKind
from app.core.entity_profile.frame_values import (
    SourceKind,
    SourceOrigin,
    TemporalEventKind,
    TopicAll,
    TopicAny,
)
from entity_profile_frame_harness import BUYER, conversation, observe


@pytest.fixture(autouse=True)
def prevent_external_io(monkeypatch):
    def blocked(*args, **kwargs):
        raise AssertionError("frame core tests never touch the network")
    monkeypatch.setattr(socket.socket, "connect", blocked)


def _subject_of(claims, event_claim):
    """La fuente que gobierna este evento, según la relación acreditada."""
    for relation in event_claim.relations:
        if relation.kind is RelationKind.HAS_SUBJECT:
            return claims.get(relation.target_claim_id).canonical_value
    return None


# ---------------------------------------------------------------- R1
# Fuente, evento y periodo pertenecen a la ocurrencia que los nombró.

def test_r1_swapped_source_event_keeps_its_subjects_and_blocks():
    out = observe(f"analiza contratos publicados y procesos firmados en agosto de {BUYER}")
    events = out.claims.by_dimension(ClaimDimension.EVENT)
    owners = {
        claim.canonical_value.value: _subject_of(out.claims, claim)
        for claim in events
    }
    assert owners["PROCESS_PUBLICATION"] is SourceKind.CONTRACTS
    assert owners["CONTRACT_SIGNATURE"] is SourceKind.PROCESSES
    assert out.clarifies and not out.jobs


def test_r1_each_source_keeps_its_own_period():
    out = observe(
        f"analiza procesos publicados en julio y contratos firmados en agosto de {BUYER}",
    )
    assert out.ready, out.issues
    assert all("2026-07-01" in sql for sql in out.sql_for(SourceKind.PROCESSES))
    assert all("2026-08-01" in sql for sql in out.sql_for(SourceKind.CONTRACTS))
    assert not any("2026-08-01" in sql for sql in out.sql_for(SourceKind.PROCESSES))


def test_r1_clause_order_does_not_change_the_compiled_predicates():
    direct = observe(
        f"analiza procesos publicados en julio y contratos firmados en agosto de {BUYER}",
    )
    reverse = observe(
        f"analiza contratos firmados en agosto y procesos publicados en julio de {BUYER}",
    )
    assert direct.ready and reverse.ready
    for source in (SourceKind.PROCESSES, SourceKind.CONTRACTS):
        assert direct.predicates(source).fingerprint() == \
            reverse.predicates(source).fingerprint()


# ---------------------------------------------------------------- R2
# Un hecho histórico no soportado no se sustituye por el estado de hoy.

@pytest.mark.parametrize("phrase", [
    "contratos terminados en agosto pero actualmente activos",
    "contratos terminados en agosto que actualmente estan activos",
    "hoy los contratos terminados en agosto",
])
def test_r2_unsupported_history_beside_current_state_blocks(phrase):
    out = observe(f"analiza {phrase} de {BUYER}")
    assert out.clarifies and not out.jobs
    # El hecho histórico se conserva en la cuestión: no se sustituye por la
    # firma, ni por hoy, ni por el estado terminado.
    assert "terminados" in out.blocked_terms


def test_r2_both_sources_keep_their_own_current_state():
    out = observe(
        "analiza procesos actualmente abiertos y contratos actualmente terminados "
        f"en agosto de {BUYER}",
    )
    if out.clarifies:
        assert not out.jobs
        return
    process_state = out.predicates(SourceKind.PROCESSES).current_state
    contract_state = out.predicates(SourceKind.CONTRACTS).current_state
    assert not process_state.is_true and not contract_state.is_true
    assert process_state != contract_state


def test_conflicting_material_and_physical_legacy_state_never_executes():
    out = observe(
        f"analiza contratos de {BUYER}",
        overrides={
            "estado": ["Suspendido"],
            "estado_field": "estado_contrato",
            "estado_contrato": ["terminado"],
        },
    )
    assert out.clarifies and not out.jobs
    assert "LEGACY_STATE_INVALID_CONTEXT" in out.issues


@pytest.mark.parametrize("marker", ["actualmente", "todavia"])
def test_current_marker_governs_only_a_later_lifecycle_occurrence(marker):
    out = observe(
        f"analiza contratos firmados en agosto {marker} terminados de {BUYER}",
    )
    assert out.ready, out.issues
    predicates = out.predicates(SourceKind.CONTRACTS)
    assert predicates.temporal.event is TemporalEventKind.CONTRACT_SIGNATURE
    assert predicates.temporal.period.lower == "2026-08-01"
    assert not predicates.current_state.is_true


# ---------------------------------------------------------------- R3
# Una palabra de fuente dentro del tema no vuelve a elegir fuente.

@pytest.mark.parametrize("phrase,expected", [
    ("contratos de software y gestion de procesos industriales", SourceKind.CONTRACTS),
    ("contratos de software, gestion de procesos industriales", SourceKind.CONTRACTS),
    ("procesos de software y administracion de contratos", SourceKind.PROCESSES),
])
def test_r3_topic_words_never_become_sources(phrase, expected):
    out = observe(f"analiza {phrase} de {BUYER}")
    assert out.sources == frozenset({expected})
    assert out.frame.source_selection.origin is SourceOrigin.EXPLICIT


def test_r3_source_disjunction_is_a_choice_not_a_default():
    out = observe(f"analiza procesos o contratos de {BUYER}")
    assert out.clarifies and not out.jobs
    assert "SOURCE_CHOICE_AMBIGUOUS" in out.issues


# ---------------------------------------------------------------- R4
# AND y OR son universos distintos; una exclusión se ejecuta y se declara.

def test_r4_parser_or_compiles_differently_from_and():
    disjunction = observe(f"analiza contratos de mantenimiento o vias de {BUYER}")
    conjunction = observe(f"analiza contratos de mantenimiento y vias de {BUYER}")
    assert disjunction.ready and conjunction.ready
    assert isinstance(disjunction.predicates(SourceKind.CONTRACTS).topic, TopicAny)
    assert isinstance(conjunction.predicates(SourceKind.CONTRACTS).topic, TopicAll)
    assert disjunction.sql != conjunction.sql
    assert disjunction.scope_lines != conjunction.scope_lines


def test_r4_negative_modality_is_executed_and_declared():
    out = observe(f"analiza contratos excepto contratacion directa de {BUYER}")
    assert out.ready, out.issues
    # "NOT IN" a secas también lo emite el filtro de documentos de
    # proveedor: la exclusión se comprueba sobre SU propia columna.
    assert any(
        "modalidad_de_contratacion NOT IN" in sql
        for sql in out.sql_for(SourceKind.CONTRACTS)
    )
    assert any("excepto" in line.lower() for line in out.scope_lines)


# ---------------------------------------------------------------- source origin
def test_explicit_sources_cannot_silently_disappear():
    out = observe(f"analiza proveedores de procesos y contratos de {BUYER}")
    assert out.clarifies and not out.jobs
    assert "EXPLICIT_SOURCE_INCOMPLETE" in out.issues


def test_default_sources_may_narrow_by_capability_with_a_limitation():
    out = observe(f"analiza proveedores de {BUYER}")
    assert out.ready, out.issues
    assert {job.source for job in out.jobs} == {SourceKind.CONTRACTS}
    assert out.decision.plan.limitations


# ---------------------------------------------------------------- utility gates
@pytest.mark.parametrize("query,expected_sources", [
    (f"analiza {BUYER}", {SourceKind.PROCESSES, SourceKind.CONTRACTS}),
    (f"analiza contratos de {BUYER}", {SourceKind.CONTRACTS}),
    (f"analiza procesos de {BUYER}", {SourceKind.PROCESSES}),
    (f"analiza modalidades de {BUYER}", {SourceKind.PROCESSES}),
    (f"analiza montos de contratos de {BUYER}", {SourceKind.CONTRACTS}),
    (f"analiza recientes de {BUYER}", {SourceKind.PROCESSES, SourceKind.CONTRACTS}),
])
def test_utility_corpus_is_answerable(query, expected_sources):
    out = observe(query)
    assert out.ready, (query, out.issues)
    assert {job.source for job in out.jobs} == expected_sources
    assert out.sql and all(" WHERE " in sql for sql in out.sql)


def test_temporal_change_keeps_focus_and_filters():
    turns = conversation((
        f"analiza contratos firmados de {BUYER} en agosto", "proveedores",
    ))
    assert turns[-1].ready, turns[-1].issues
    assert turns[-1].frame.focus == "suppliers"
    assert all("2026-08-01" in sql for sql in turns[-1].sql)


def test_clearing_the_period_is_not_silently_restored():
    turns = conversation((
        f"analiza contratos firmados de {BUYER} en agosto", "sin filtro de fecha",
    ))
    assert turns[-1].ready, turns[-1].issues
    assert turns[-1].predicates(SourceKind.CONTRACTS).temporal.period.is_unbounded
    assert not any("2026-08-01" in sql for sql in turns[-1].sql)


def test_switching_source_does_not_inherit_an_incompatible_event():
    """La fuente que entra no hereda el hecho de la que sale.

    La fecha de firma de un contrato no describe la publicación de un proceso;
    adivinarlo contestaría una pregunta distinta de la que se hizo.
    """
    turns = conversation((
        f"analiza contratos firmados de {BUYER} en agosto", "ahora procesos",
    ))
    last = turns[-1]
    if last.clarifies:
        assert not last.jobs
        return
    assert last.sources == frozenset({SourceKind.PROCESSES})
    temporal = last.predicates(SourceKind.PROCESSES).temporal
    assert temporal.event is not TemporalEventKind.CONTRACT_SIGNATURE
    assert {job.source for job in last.jobs} == {SourceKind.PROCESSES}


def test_contract_type_exclusion_is_declared_unsupported_not_turned_into_topic():
    """El tipo negativo está modelado, pero la base no acredita su proyección."""
    out = observe(
        f"analiza contratos de {BUYER}",
        overrides={"contract_type_exclude": ["Obra"]},
    )
    assert out.clarifies and not out.jobs
    assert "CONTRACT_TYPE_EXCLUDE_UNSUPPORTED" in out.issues


@settings(max_examples=8, derandomize=True, deadline=None)
@given(connective=st.sampled_from(["pero", "que", "aunque"]),
       term=st.sampled_from(["terminados", "liquidados"]))
def test_property_current_marker_does_not_erase_a_historical_event(connective, term):
    """Un marcador de actualidad no borra el hecho fechado que lo acompaña."""
    out = observe(
        f"analiza contratos {term} en agosto {connective} actualmente activos "
        f"de {BUYER}",
    )
    assert out.clarifies and not out.jobs
    assert term in out.blocked_terms
