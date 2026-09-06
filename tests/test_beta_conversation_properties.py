"""Metamorphic conversation and admission invariants, deterministic and offline."""
from copy import deepcopy

import pytest
from hypothesis import given, settings, strategies as st

from app.core.actor_resolution_admission import can_resolve_new_actor
from app.core.actor_scope_policy import evaluate_actor_scope
from app.core.entity_profile.followup import is_supplier_followup
from app.core.followup_engine import FollowupGuards, detect_and_merge
from app.core.query_frame import frame_from_params


BUYER = {
    "entidad_resolved": "MUNICIPIO DE GALAPA.",
    "entidad_resolution": {"value": "MUNICIPIO DE GALAPA.", "kind": "buyer_entity", "confidence": "high"},
}
STRONG_ACTOR = {
    "dataset": "contratos",
    "actor_resolution": "name_unresolved",
    "actor_conversation_operation": "SET_ACTOR",
    "actor_conversation_construction": "strong_actor_syntax",
    "actor_scope": {"role": "contractor", "resolution_method": "name_unresolved",
                    "requires_resolution": True, "observed_name": "actor alfa"},
}


@settings(max_examples=50, derandomize=True)
@given(removed=st.sets(st.sampled_from([
    "actor_resolution", "actor_conversation_operation", "actor_conversation_construction",
    "role", "resolution_method", "requires_resolution", "observed_name",
])))
def test_resolution_admission_needs_complete_syntax_and_never_execution_authority(removed):
    params = deepcopy(STRONG_ACTOR)
    for key in removed:
        params.pop(key, None)
        params["actor_scope"].pop(key, None)
    assert can_resolve_new_actor(params, "set_actor") == (not removed)
    assert not FollowupGuards.check_no_where_1_1(params)
    if params["actor_scope"] or params.get("actor_resolution"):
        assert not evaluate_actor_scope(params).should_execute


@settings(max_examples=40, derandomize=True)
@given(dataset=st.sampled_from(["procesos", "contratos"]),
       year=st.integers(min_value=2018, max_value=2026),
       value=st.integers(min_value=0, max_value=10**12))
def test_explicit_buyer_profile_never_inherits_actor_topic_or_filters(dataset, year, value):
    previous = {
        **deepcopy(STRONG_ACTOR), "dataset": dataset,
        "contratista": ["900111222"], "objeto": ["mantenimiento"],
        "fecha_desde": f"{year}-01-01", "valor_min": value,
    }
    current = {**BUYER, "dataset": "procesos", "objeto": ["analiza"]}
    intent, merged = detect_and_merge("analiza la alcaldia de galapa", current, frame_from_params(previous))
    fresh_intent, fresh = detect_and_merge("analiza la alcaldia de galapa", current, None)
    assert intent == fresh_intent == "new_search"
    assert merged == fresh


@settings(max_examples=50, derandomize=True)
@given(dataset=st.sampled_from(["procesos", "contratos"]),
       prefix=st.sampled_from(["", "y ", "ahora "]),
       question=st.sampled_from(["cuales son sus mayores proveedores", "quienes son sus principales proveedores",
                                "quien tiene mas contratos", "quienes contratan mas con esa entidad"]),
       suffix=st.sampled_from(["", "?", ", por favor"]),
       year=st.integers(min_value=2018, max_value=2026))
def test_supplier_question_keeps_accredited_buyer_period_and_topic(dataset, prefix, question, suffix, year):
    previous = {**BUYER, "dataset": dataset, "_entity_profile_active": True,
                "fecha_desde": f"{year}-01-01", "objeto": ["alimentacion"]}
    query = prefix + question + suffix
    assert is_supplier_followup(query, previous)
    intent, merged = detect_and_merge(
        query, {"dataset": "contratos", "dataset_explicit": True, "objeto": ["quienes"]},
        frame_from_params(previous),
    )
    assert intent == "refine_filter"
    for key in previous:
        assert merged[key] == previous[key]


@pytest.mark.parametrize("query", [
    "busca proveedor rodrigo ortiz", "proveedores de software",
    "quien tiene mas contratos de alimentacion", "quienes contratan mas en bogota",
    "cuanto suma lo contratado", "procesos abiertos", "y por valor",
])
def test_supplier_grammar_does_not_capture_new_content_or_other_analytics(query):
    assert not is_supplier_followup(query, {**BUYER, "_entity_profile_active": True})
