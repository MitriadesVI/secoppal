"""P2-A/P2-B: leer contexto legacy sin preferir en silencio una de dos copias."""
from __future__ import annotations

import pytest
from hypothesis import given, settings, strategies as st

from app.core.entity_profile.frame import EntityProfileFrame
from app.core.entity_profile.frame_migration import MigrationStatus, migrate_legacy_context
from app.core.entity_profile.frame_values import SourceKind


LEGACY_BASE = {
    "focus": "overview",
    "sources": "CONTRACTS",
    "sources_provenance": "EXPLICIT",
    "sources_origin": "EXPLICIT",
    "time_from": "2026-08-01",
    "time_to": "2026-08-31",
    "event": "CONTRACT_SIGNATURE",
    "temporal_claims": [
        {"event": "CONTRACT_SIGNATURE", "span": [9, 17], "term": "firmados"},
    ],
    "unsupported_events": [],
    "keeps_current_state": True,
    "topic": [],
    "modality": [],
    "value_min": None,
    "value_max": None,
}

BUYER_PARAMS = {
    "entidad_resolved": "ALCALDIA DE BARANOA",
    "fecha_desde": "2026-08-01",
    "fecha_hasta": "2026-08-31",
}


def test_v1_1_context_migrates_when_every_dimension_is_equivalent():
    outcome = migrate_legacy_context(LEGACY_BASE, BUYER_PARAMS)
    assert outcome.status is MigrationStatus.MIGRATED
    predicates = outcome.frame.source_predicates[SourceKind.CONTRACTS]
    assert predicates.temporal.period.lower == "2026-08-01"
    assert predicates.temporal.event.value == "CONTRACT_SIGNATURE"


def test_v1_context_without_origin_still_migrates_from_provenance():
    legacy = {key: value for key, value in LEGACY_BASE.items() if key != "sources_origin"}
    outcome = migrate_legacy_context(legacy, BUYER_PARAMS)
    assert outcome.status is MigrationStatus.MIGRATED
    assert outcome.frame.source_selection.origin.value == "EXPLICIT"


@pytest.mark.parametrize("params_period", [
    {},
    {"fecha_desde": "2026-07-01", "fecha_hasta": "2026-07-31"},
])
def test_conflicting_context_is_invalid_and_never_prefers_a_copy(params_period):
    params = {"entidad_resolved": "ALCALDIA DE BARANOA", **params_period}
    outcome = migrate_legacy_context(LEGACY_BASE, params)
    assert outcome.status is MigrationStatus.INVALID_CONTEXT
    assert outcome.frame is None
    assert outcome.reason == "PERIOD_CONFLICT"


def test_legacy_unsupported_event_survives_migration_as_a_block():
    legacy = {**LEGACY_BASE, "event": "NONE", "temporal_claims": [],
              "unsupported_events": ["terminados"]}
    outcome = migrate_legacy_context(legacy, BUYER_PARAMS)
    assert outcome.status is MigrationStatus.BLOCKED
    assert outcome.blocked_events == ("terminados",)
    assert outcome.frame is None


def test_malformed_span_invalidates_without_crashing():
    legacy = {**LEGACY_BASE, "temporal_claims": [
        {"event": "CONTRACT_SIGNATURE", "span": [3], "term": "firmados"},
    ]}
    outcome = migrate_legacy_context(legacy, BUYER_PARAMS)
    assert outcome.status is MigrationStatus.INVALID_CONTEXT
    assert outcome.frame is None


def test_legacy_state_migrates_only_with_an_unambiguous_field():
    params = {**BUYER_PARAMS, "estado": ["Suspendido"], "estado_field": "estado_contrato"}
    outcome = migrate_legacy_context(LEGACY_BASE, params)
    assert outcome.status is MigrationStatus.MIGRATED
    state = outcome.frame.source_predicates[SourceKind.CONTRACTS].current_state
    assert state.values == ("Suspendido",) and state.field == "estado_contrato"


def test_legacy_state_without_a_field_is_not_purged_in_silence():
    params = {**BUYER_PARAMS, "estado": ["Suspendido"]}
    outcome = migrate_legacy_context(LEGACY_BASE, params)
    assert outcome.status is MigrationStatus.NEEDS_CLARIFICATION
    assert outcome.reason == "AMBIGUOUS_STATE_FIELD"


def test_unknown_schema_version_is_invalid_not_best_effort():
    payload = {"schema_version": 99, "frame_id": "f", "revision": 1}
    with pytest.raises(ValueError):
        EntityProfileFrame.from_dict(payload)


@pytest.mark.parametrize(("request_delta", "params_delta", "reason"), [
    ({"topic_exclude": ["interventoria"]}, {}, "EXCLUSION_CONFLICT"),
    ({}, {"objeto_exclude": ["interventoria"]}, "EXCLUSION_CONFLICT"),
    (
        {"modality": ["licitacion_publica"]},
        {"modalidad_include_procedure_ids": ["contratacion_directa"]},
        "MODALITY_CONFLICT",
    ),
    (
        {"contract_type": ["Obra"]},
        {"contract_type_include": ["Servicios"]},
        "CONTRACT_TYPE_CONFLICT",
    ),
    (
        {"estado": ["Suspendido"], "estado_field": "estado_contrato"},
        {"estado": ["terminado"], "estado_field": "estado_contrato"},
        "CURRENT_STATE_CONFLICT",
    ),
    ({}, {"_entity_profile_sources": "PROCESSES"}, "SOURCES_CONFLICT"),
    ({}, {"event": "PROCESS_PUBLICATION"}, "EVENT_CONFLICT"),
])
def test_every_duplicated_material_dimension_must_be_equivalent(
    request_delta, params_delta, reason,
):
    result = migrate_legacy_context(
        {**LEGACY_BASE, **request_delta}, {**BUYER_PARAMS, **params_delta},
    )
    assert result.status is MigrationStatus.INVALID_CONTEXT
    assert result.reason == reason and result.frame is None


@settings(max_examples=20, derandomize=True, deadline=None)
@given(
    terms=st.lists(
        st.sampled_from(["software", "vias", "mantenimiento", "salud"]),
        min_size=1, max_size=4, unique=True,
    ),
)
def test_commutative_topic_order_is_semantically_equivalent(terms):
    result = migrate_legacy_context(
        {**LEGACY_BASE, "topic": terms},
        {**BUYER_PARAMS, "objeto": list(reversed(terms))},
    )
    assert result.status is MigrationStatus.MIGRATED
    assert result.frame is not None


def test_equivalent_exclusions_and_selections_are_preserved_in_frame():
    request = {
        **LEGACY_BASE,
        "topic": ["software"],
        "topic_exclude": ["interventoria"],
        "modality": ["licitacion_publica"],
        "contract_type": ["Obra"],
    }
    params = {
        **BUYER_PARAMS,
        "objeto": ["software"],
        "objeto_exclude": ["interventoria"],
        "modalidad_include_procedure_ids": ["licitacion_publica"],
        "contract_type_include": ["Obra"],
    }
    result = migrate_legacy_context(request, params)
    assert result.status is MigrationStatus.MIGRATED
    predicates = result.frame.predicates_for(SourceKind.CONTRACTS)
    assert "NOT(TERM(interventoria))" in predicates.topic.fingerprint()
    assert predicates.modality.include == ("licitacion_publica",)
    assert predicates.contract_type.include == ("obra",)
