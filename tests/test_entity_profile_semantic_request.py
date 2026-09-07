"""La petición semántica: qué pidió el usuario, antes de decidir qué ejecutar."""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core.entity_profile.scope import resolve_profile_scope
from app.core.entity_profile.semantic_request import (
    EntityProfileSemanticRequest,
    build_semantic_request,
    inherit_request,
    request_context,
)
from app.core.entity_profile.source_request import (
    RequestedSources,
    SourceProvenance,
    resolve_requested_sources,
)
from app.core.entity_profile.temporal_event import TemporalEvent
from app.core.entity_profile.turn import profile_query_params
from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter


TODAY = date(2026, 9, 5)


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))


def _request(router: QueryRouter, text: str, focus: str = "overview"):
    params = profile_query_params(router.parse(text, today=TODAY).params)
    scope = resolve_profile_scope(params).scope
    assert scope is not None, text
    return build_semantic_request(
        user_query=text, params=params, scope=scope, focus=focus,
    )


# ── Autoridad de fuente ─────────────────────────────────────────────────────

@pytest.mark.parametrize("text,expected", [
    ("analizame que contratos firmo alcaldia de baranoa en agosto",
     RequestedSources.CONTRACTS),
    ("analiza procesos de vias publicados por alcaldia de baranoa en agosto",
     RequestedSources.PROCESSES),
    ("analiza procesos y contratos de alcaldia de baranoa", RequestedSources.BOTH),
    ("analiza licitaciones de alcaldia de baranoa", RequestedSources.PROCESSES),
])
def test_named_source_is_explicit(router, text, expected):
    request = _request(router, text)
    assert request.sources is expected
    assert request.sources_provenance is SourceProvenance.EXPLICIT


def test_unnamed_source_defaults_to_both_and_never_to_the_frame_dataset(router):
    text = "analiza alcaldia de baranoa"
    params = profile_query_params(router.parse(text, today=TODAY).params)
    assert params["dataset"] == "procesos"  # el frame ya eligió; no es autoridad
    request = _request(router, text)
    assert request.sources is RequestedSources.BOTH
    assert request.sources_provenance is SourceProvenance.DEFAULT


def test_supplier_word_does_not_overwrite_an_explicit_process_request(router):
    """`main` 7868664 deja dataset=contratos aquí; la palabra del usuario manda."""
    text = "analiza procesos de alcaldia de baranoa con proveedores"
    params = profile_query_params(router.parse(text, today=TODAY).params)
    assert params["dataset"] == "contratos"
    assert params["dataset_explicit"] is True
    assert _request(router, text).sources is RequestedSources.PROCESSES


def test_a_source_word_inside_the_buyer_name_is_not_a_user_choice():
    params = {
        "entidad_resolved": "AGENCIA NACIONAL DE PROCESOS Y CONTRATOS",
        "entidad": "Agencia Nacional de Procesos y Contratos",
        "entidad_resolution": {
            "value": "AGENCIA NACIONAL DE PROCESOS Y CONTRATOS",
            "kind": "buyer_entity",
            "confidence": "high",
            "metadata": {"rewritten_from": "agencia nacional de procesos y contratos"},
        },
    }
    source = resolve_requested_sources(
        "analiza la agencia nacional de procesos y contratos", params,
    )
    assert source.sources is RequestedSources.BOTH
    assert source.provenance is SourceProvenance.DEFAULT


def test_inherited_source_survives_a_turn_that_names_none():
    source = resolve_requested_sources(
        "y en agosto", {"_entity_profile_sources": "CONTRACTS"},
    )
    assert source.sources is RequestedSources.CONTRACTS
    assert source.provenance is SourceProvenance.INHERITED


# ── Evento temporal vs estado actual ────────────────────────────────────────

def test_signature_event_is_read_from_the_period_anchor(router):
    request = _request(
        router, "analizame que contratos firmo alcaldia de baranoa en agosto",
    )
    assert request.event is TemporalEvent.CONTRACT_SIGNATURE
    assert request.time_from == "2026-08-01"
    assert request.time_to == "2026-08-31"
    assert request.topic == ()


def test_publication_event_does_not_survive_as_a_current_state(router):
    request = _request(
        router, "analiza procesos de vias publicados por alcaldia de baranoa en agosto",
    )
    assert request.event is TemporalEvent.PROCESS_PUBLICATION
    assert request.keeps_current_state is False
    assert request.topic == ("vias",)


def test_current_state_without_a_period_is_not_an_event(router):
    request = _request(
        router, "analiza procesos actualmente publicados por alcaldia de baranoa",
    )
    assert request.event is TemporalEvent.NONE
    assert request.keeps_current_state is True


def test_event_and_independent_current_state_both_survive(router):
    request = _request(
        router,
        "analiza contratos firmados en agosto y actualmente terminados "
        "de alcaldia de baranoa",
    )
    assert request.event is TemporalEvent.CONTRACT_SIGNATURE
    assert request.keeps_current_state is True
    assert request.unsupported_event is None


def test_unsupported_historical_event_is_named_and_not_translated(router):
    request = _request(
        router, "analiza contratos terminados en agosto de alcaldia de baranoa",
    )
    assert request.unsupported_event == "terminados"
    assert request.event is TemporalEvent.NONE


def test_a_signature_noun_is_never_a_signature_event(router):
    request = _request(
        router, "analiza contratos de firma digital de alcaldia de baranoa en agosto",
    )
    assert request.event is TemporalEvent.NONE
    assert request.unsupported_event is None
    assert request.topic == ("firma", "digital")


def test_a_period_alone_anchors_no_event(router):
    request = _request(router, "analiza alcaldia de baranoa en 2026")
    assert request.event is TemporalEvent.NONE
    assert request.time_from == "2026-01-01"


# ── Serialización y herencia ────────────────────────────────────────────────

def test_request_survives_a_json_round_trip(router):
    request = _request(
        router, "analiza contratos de mantenimiento de alcaldia de baranoa en agosto",
    )
    import json

    revived = EntityProfileSemanticRequest.from_dict(
        json.loads(json.dumps(request.to_dict())),
    )
    assert revived == request


@pytest.mark.parametrize("raw", [None, {}, "CONTRACTS", {"focus": "overview"}])
def test_an_unreadable_context_is_discarded_and_never_guessed(raw):
    assert EntityProfileSemanticRequest.from_dict(raw) is None


def test_changing_only_the_focus_does_not_reset_the_requested_source(router):
    previous = _request(
        router, "analizame que contratos firmo alcaldia de baranoa en agosto",
    )
    follow_up = _request(router, "analiza alcaldia de baranoa", focus="suppliers")
    merged = inherit_request(previous, follow_up, source_is_explicit=False)
    assert merged.sources is RequestedSources.CONTRACTS
    assert merged.sources_provenance is SourceProvenance.INHERITED
    assert merged.focus == "suppliers"


def test_an_explicit_source_change_replaces_the_inherited_one(router):
    previous = _request(
        router, "analizame que contratos firmo alcaldia de baranoa en agosto",
    )
    follow_up = _request(router, "analiza procesos de alcaldia de baranoa")
    merged = inherit_request(previous, follow_up, source_is_explicit=True)
    assert merged.sources is RequestedSources.PROCESSES


def test_request_context_is_json_safe(router):
    import json

    context = request_context(_request(router, "analiza alcaldia de baranoa"))
    assert json.loads(json.dumps(context)) == context
    assert context["_entity_profile_sources"] == "BOTH"
