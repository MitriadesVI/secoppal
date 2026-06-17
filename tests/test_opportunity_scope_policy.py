"""Unit tests for the pure opportunity scope policy (OPP-SCOPE-001)."""

import copy

import pytest

from app.core.opportunity_scope_policy import (
    MIN_BROAD_OPPORTUNITY_VALUE_COP,
    evaluate_opportunity_scope,
)

OPP = {"intent_type": "opportunity_search", "estado_family": "oferta_abierta"}


def _opp(**extra):
    return {**OPP, **extra}


# ── Fail-open: non-opportunity queries always execute ────────────────────

def test_non_opportunity_fails_open():
    d = evaluate_opportunity_scope("contratos en bolívar", {"intent_type": "history_search"})
    assert d.should_execute is True
    assert d.reason == "not_opportunity_search"


def test_missing_intent_type_fails_open():
    d = evaluate_opportunity_scope("lo que sea", {})
    assert d.should_execute is True


# ── Blocked: opportunity search with only territory / nothing ────────────

def test_territory_only_blocks():
    d = evaluate_opportunity_scope(
        "qué procesos hay para presentarme en bolívar",
        _opp(departamento_resolved="Bolívar"),
    )
    assert d.should_execute is False
    assert d.missing_slots == ("topic",)
    assert "opportunity_scope_too_broad" in d.risk_flags
    assert d.clarification_question


def test_completely_empty_opportunity_blocks():
    # "licitaciones abiertas" with no place — P13 would only flag, we block.
    d = evaluate_opportunity_scope("licitaciones abiertas", _opp())
    assert d.should_execute is False


def test_open_state_alone_is_not_a_qualifier():
    d = evaluate_opportunity_scope("oportunidades en bolívar", _opp(ciudad="Cartagena"))
    assert d.should_execute is False


# ── Executes: any single qualifier is enough ─────────────────────────────

def test_topic_executes():
    assert evaluate_opportunity_scope("", _opp(objeto=["mantenimiento"])).should_execute


def test_objeto_or_executes():
    assert evaluate_opportunity_scope("", _opp(objeto_or=["aseo", "vigilancia"])).should_execute


@pytest.mark.parametrize("key", [
    "entidad", "entidad_resolved", "entidad_like",
    "entidad_ecosystem", "entidad_contains_all", "entidad_in_candidates",
])
def test_each_entity_key_executes(key):
    assert evaluate_opportunity_scope("", _opp(**{key: "Gobernación del Atlántico"})).should_execute


def test_modality_executes():
    assert evaluate_opportunity_scope("", _opp(modalidad="minima_cuantia")).should_execute


# ── Date window boundary ─────────────────────────────────────────────────

def test_window_exactly_60_days_executes():
    d = evaluate_opportunity_scope(
        "", _opp(fecha_desde="2026-06-01", fecha_hasta="2026-07-31"),  # 60 days
    )
    assert d.should_execute is True


def test_window_61_days_does_not_count():
    d = evaluate_opportunity_scope(
        "", _opp(fecha_desde="2026-06-01", fecha_hasta="2026-08-01"),  # 61 days
    )
    assert d.should_execute is False


def test_full_year_does_not_count():
    d = evaluate_opportunity_scope(
        "", _opp(fecha_desde="2026-01-01", fecha_hasta="2026-12-31"),
    )
    assert d.should_execute is False


def test_only_one_date_bound_does_not_count():
    assert evaluate_opportunity_scope("", _opp(fecha_desde="2026-06-01")).should_execute is False


# ── Value floor boundary ─────────────────────────────────────────────────

def test_value_below_floor_does_not_count():
    d = evaluate_opportunity_scope("", _opp(valor_min=MIN_BROAD_OPPORTUNITY_VALUE_COP - 1))
    assert d.should_execute is False


def test_value_at_floor_counts():
    d = evaluate_opportunity_scope("", _opp(valor_min=MIN_BROAD_OPPORTUNITY_VALUE_COP))
    assert d.should_execute is True


def test_value_as_string_counts():
    d = evaluate_opportunity_scope("", _opp(valor_min=str(MIN_BROAD_OPPORTUNITY_VALUE_COP)))
    assert d.should_execute is True


def test_valor_max_without_min_does_not_count():
    d = evaluate_opportunity_scope("", _opp(valor_max=5_000_000_000))
    assert d.should_execute is False


# ── Purity ───────────────────────────────────────────────────────────────

def test_params_not_mutated():
    params = _opp(departamento_resolved="Bolívar", objeto=[])
    snapshot = copy.deepcopy(params)
    evaluate_opportunity_scope("x", params)
    assert params == snapshot
