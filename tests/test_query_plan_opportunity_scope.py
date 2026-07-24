"""Integration tests for P15 (opportunity scope) inside compute_query_plan."""

import copy

from app.core.query_plan import ALL_RULES, compute_query_plan


def _base(**extra):
    return {
        "dataset": "procesos",
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        **extra,
    }


def test_p15_is_registered():
    assert "P15" in ALL_RULES


def test_p15_blocks_territory_only():
    params = _base(departamento_resolved="Bolívar")
    plan = compute_query_plan(
        params,
        "qué procesos hay para presentarme en bolívar",
        enabled_rules={"P15"},
    )
    assert plan.should_execute is False
    assert plan.missing_slots == ["topic"]
    assert "opportunity_scope_too_broad" in plan.risk_flags
    assert plan.clarification_question


def test_p15_allows_topic():
    params = _base(departamento_resolved="Bolívar", objeto=["mantenimiento"])
    plan = compute_query_plan(params, "mantenimiento en bolívar", enabled_rules={"P15"})
    assert plan.should_execute is True


def test_p15_allows_concrete_entity():
    params = _base(entidad_resolved="Gobernación del Atlántico")
    plan = compute_query_plan(
        params, "procesos abiertos de la gobernación del atlántico", enabled_rules={"P15"},
    )
    assert plan.should_execute is True


def test_p15_allows_modality():
    params = _base(
        ciudad="Barranquilla",
        modalidad_include_procedure_ids=["minima_cuantia"],
    )
    plan = compute_query_plan(params, "mínimas cuantías en barranquilla", enabled_rules={"P15"})
    assert plan.should_execute is True


def test_p15_exclude_only_does_not_qualify_scope():
    params = _base(
        ciudad="Barranquilla",
        modalidad_exclude_procedure_ids=["contratacion_directa"],
    )
    plan = compute_query_plan(
        params,
        "oportunidades excepto contratación directa en barranquilla",
        enabled_rules={"P15"},
    )
    assert plan.should_execute is False
    assert plan.reason == "opportunity_scope_too_broad"


def test_p15_legacy_scalar_does_not_qualify_scope():
    params = _base(ciudad="Barranquilla", modalidad="minima_cuantia")
    plan = compute_query_plan(
        params,
        "mínimas cuantías en barranquilla",
        enabled_rules={"P15"},
    )
    assert plan.should_execute is False
    assert plan.reason == "opportunity_scope_too_broad"


def test_p15_allows_narrow_date():
    params = _base(departamento_resolved="Cundinamarca",
                   fecha_desde="2026-06-01", fecha_hasta="2026-06-15")
    plan = compute_query_plan(params, "oportunidades esta semana", enabled_rules={"P15"})
    assert plan.should_execute is True


def test_p15_allows_high_value():
    params = _base(departamento_resolved="Bolívar", valor_min=1_000_000_000)
    plan = compute_query_plan(params, "oportunidades superiores a mil millones", enabled_rules={"P15"})
    assert plan.should_execute is True


def test_p15_ignores_non_opportunity():
    params = {"dataset": "contratos", "intent_type": "history_search",
              "departamento_resolved": "Bolívar"}
    plan = compute_query_plan(params, "contratos en bolívar", enabled_rules={"P15"})
    assert plan.should_execute is True


def test_p15_disabled_does_not_block():
    params = _base(departamento_resolved="Bolívar")
    plan = compute_query_plan(
        params, "qué procesos hay para presentarme en bolívar",
        enabled_rules=ALL_RULES - {"P15"},
    )
    assert plan.should_execute is True


def test_p15_does_not_mutate_input():
    params = _base(departamento_resolved="Bolívar")
    snapshot = copy.deepcopy(params)
    compute_query_plan(params, "presentarme en bolívar", enabled_rules={"P15"})
    assert params == snapshot


def test_p15_followup_with_inherited_topic_executes():
    # Simulates apply_context already merging the inherited topic before P15.
    params = _base(departamento_resolved="Bolívar", objeto=["mantenimiento"])
    plan = compute_query_plan(params, "mantenimiento", enabled_rules={"P15"})
    assert plan.should_execute is True
