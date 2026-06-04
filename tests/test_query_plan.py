"""Tests for QueryPlan — compute_query_plan with 9 rules P1..P9."""

from copy import deepcopy
from datetime import date

import pytest

from app.core.query_plan import compute_query_plan


# ═══════════════════════════════════════════════════════════════════════════
# P1 — command_incomplete
# ═══════════════════════════════════════════════════════════════════════════

def test_command_incomplete_blocks_execution():
    plan = compute_query_plan({}, "filtra por departamento", None)
    assert plan.should_execute is False
    assert "departamento" in plan.missing_slots
    assert "departamento" in (plan.clarification_question or "").lower()


# ═══════════════════════════════════════════════════════════════════════════
# P3 — city_mention_as_topic (CITY-MENTION-AS-OBJECT-001)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("query,token,city", [
    ("diseño ipiales", "ipiales", "Ipiales"),
    ("manejo de residuos sólidos olaya", "olaya", "Olaya"),
    ("silvopastoriles yarumal", "yarumal", "Yarumal"),
])
def test_city_extracted_from_objeto(query, token, city):
    params = {"objeto": query.split(), "dataset": "procesos"}
    plan = compute_query_plan(params, query, None)
    assert plan.params.get("ciudad") == city
    assert token not in [t.lower() for t in plan.params.get("objeto", [])]


def test_city_extracted_topic_missing_asks_for_topic():
    plan = compute_query_plan(
        {"objeto": ["ipiales"], "dataset": "procesos"}, "ipiales", None
    )
    assert plan.should_execute is False
    assert "topic" in plan.missing_slots or "tema" in (
        plan.clarification_question or ""
    ).lower()


# ═══════════════════════════════════════════════════════════════════════════
# P4 — implicit_opportunity (OPP-INTENT-001)
# ═══════════════════════════════════════════════════════════════════════════

def test_implicit_opportunity_promotes_intent():
    plan = compute_query_plan(
        {"objeto": ["mantenimiento"], "dataset": "procesos"},
        "algún proceso para mantenimiento",
        None,
    )
    assert plan.params.get("intent_type") == "opportunity_search"
    assert plan.params.get("estado_family") == "oferta_abierta"


# ═══════════════════════════════════════════════════════════════════════════
# P5 — temporal_month (TEMPORAL-MONTH-PARSING-001)
# ═══════════════════════════════════════════════════════════════════════════

def test_temporal_month_uses_injected_today():
    plan = compute_query_plan(
        {"objeto": ["aseo"], "departamento_resolved": "Putumayo", "dataset": "procesos"},
        "aseo putumayo en mayo",
        context={"today": date(2026, 5, 21)},
    )
    assert plan.params.get("fecha_desde") == "2026-05-01"
    assert plan.params.get("fecha_hasta") == "2026-05-31"


def test_temporal_month_pasado():
    plan = compute_query_plan(
        {"objeto": ["aseo"], "dataset": "procesos"},
        "aseo el mes pasado",
        context={"today": date(2026, 5, 21)},
    )
    assert plan.params.get("fecha_desde") == "2026-04-01"


# ═══════════════════════════════════════════════════════════════════════════
# P9 — bare_city_no_entity_promotion (CITY-TO-ENTITY-AUTOPROMOTION-001)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("query,initial_entity", [
    ("casas lúdicas barranquilla", "ALCALDÍA DE BARRANQUILLA"),
    ("aires acondicionados barranquilla", "DISTRITO DE BARRANQUILLA"),
])
def test_p9_purges_autopromoted_entity(query, initial_entity):
    params = {
        "objeto": query.split()[:-1],
        "ciudad": "Barranquilla",
        "entidad_resolved": initial_entity,
    }
    plan = compute_query_plan(params, query, None)
    assert plan.params.get("entidad_resolved") is None
    assert "entity_autopromotion_purged" in plan.risk_flags


@pytest.mark.parametrize("query", [
    "contratos de la alcaldía de barranquilla",
    "procesos del distrito de bogotá",
    "gobernación del atlántico contratos",
    "municipio de medellín obras",
    "secretaría de salud bogotá",
])
def test_p9_preserves_entity_when_marker_present(query):
    params = {
        "objeto": ["contratos"],
        "ciudad": "Barranquilla",
        "entidad_resolved": "ALCALDÍA DE BARRANQUILLA",
    }
    plan = compute_query_plan(params, query, None)
    assert plan.params.get("entidad_resolved") == "ALCALDÍA DE BARRANQUILLA"
    assert "entity_autopromotion_purged" not in plan.risk_flags


# ═══════════════════════════════════════════════════════════════════════════
# P6 — number_phrase_scrub
# ═══════════════════════════════════════════════════════════════════════════

def test_number_phrase_scrub_removes_verb_from_objeto():
    plan = compute_query_plan(
        {"objeto": ["transporte", "escolar", "supere", "los"],
         "valor_max": 25_000_000},
        "transporte escolar no supere los 25 millones",
        None,
    )
    objeto = [t.lower() for t in plan.params.get("objeto", [])]
    assert "supere" not in objeto
    assert "los" not in objeto
    assert "transporte" in objeto


# ═══════════════════════════════════════════════════════════════════════════
# P7 — discourse_filler
# ═══════════════════════════════════════════════════════════════════════════

def test_discourse_filler_scrubs_courtesy():
    plan = compute_query_plan(
        {"objeto": ["hola", "estoy", "interesado", "casas", "lúdicas"],
         "ciudad": "Barranquilla"},
        "hola estoy interesado en casas lúdicas en barranquilla",
        None,
    )
    objeto = plan.params.get("objeto", [])
    assert "hola" not in objeto
    assert "estoy" not in objeto


# ═══════════════════════════════════════════════════════════════════════════
# P8 — invalid_value_range
# ═══════════════════════════════════════════════════════════════════════════

def test_invalid_value_range_blocks():
    plan = compute_query_plan(
        {"valor_min": 1_000_000_000, "valor_max": 5_000_000,
         "objeto": ["mantenimiento"]},
        "mantenimiento entre 1000M y 5M",
        None,
    )
    assert plan.should_execute is False
    assert "invalid_range" in plan.risk_flags


# ═══════════════════════════════════════════════════════════════════════════
# P10 — signed dataset / invalid contract states
# ═══════════════════════════════════════════════════════════════════════════

def test_p10_signed_query_forces_contracts_and_purges_invalid_state():
    plan = compute_query_plan(
        {"dataset": "procesos", "estado": "Firmado", "objeto": ["aseo"]},
        "contratos firmados de aseo",
        None,
    )
    assert plan.params.get("dataset") == "contratos"
    assert "estado" not in plan.params
    assert "firmado_dataset_forced" in plan.risk_flags
    assert "invalid_estado_purged" in plan.risk_flags


def test_p10_firmados_en_ejecucion_preserves_active_states():
    plan = compute_query_plan(
        {
            "dataset": "contratos",
            "estado_contrato": ["Firmado", "En ejecución", "Modificado"],
            "estado_field": "estado_contrato",
            "objeto": ["mantenimiento"],
        },
        "contratos firmados en ejecución de mantenimiento",
        None,
    )
    assert plan.params.get("estado_contrato") == ["En ejecución", "Modificado"]
    assert plan.params.get("estado_field") == "estado_contrato"
    assert "invalid_estado_purged" in plan.risk_flags


# ═══════════════════════════════════════════════════════════════════════════
# P11 — opportunity invariant
# ═══════════════════════════════════════════════════════════════════════════

def test_p11_bidder_query_forces_process_opportunity_and_purges_closed_state():
    plan = compute_query_plan(
        {"dataset": "contratos", "estado_contrato": ["Cerrado"], "objeto": ["obras"]},
        "quiero presentarme a obras",
        None,
    )
    assert plan.params.get("dataset") == "procesos"
    assert plan.params.get("intent_type") == "opportunity_search"
    assert plan.params.get("estado_family") == "oferta_abierta"
    assert "estado_contrato" not in plan.params
    assert "opportunity_enforced" in plan.risk_flags


def test_p11_is_idempotent_with_existing_opportunity_params():
    params = {
        "dataset": "procesos",
        "intent_type": "opportunity_search",
        "estado_family": "oferta_abierta",
        "objeto": ["mantenimiento"],
    }
    once = compute_query_plan(params, "quiero presentarme", None)
    twice = compute_query_plan(once.params, "quiero presentarme", None)
    assert twice.params == once.params
    assert "opportunity_enforced" not in twice.risk_flags


# ═══════════════════════════════════════════════════════════════════════════
# P12 — oferta_abierta not contracts
# ═══════════════════════════════════════════════════════════════════════════

def test_p12_oferta_abierta_not_contracts_selects_processes():
    plan = compute_query_plan(
        {"dataset": "contratos", "estado_family": "oferta_abierta"},
        "procesos abiertos",
        None,
        enabled_rules={"P12"},
    )
    assert plan.params.get("dataset") == "procesos"
    assert plan.params.get("intent_type") == "opportunity_search"
    assert "oferta_abierta_not_contracts" in plan.risk_flags


# ═══════════════════════════════════════════════════════════════════════════
# P13 — substantial filter passive flag
# ═══════════════════════════════════════════════════════════════════════════

def test_p13_passively_flags_unsafe_global_candidate_without_blocking():
    plan = compute_query_plan(
        {"dataset": "procesos", "valor_min": 30_000_000},
        "procesos de más de 30 millones",
        None,
    )
    assert plan.should_execute is True
    assert "unsafe_global_candidate" in plan.risk_flags


def test_p13_does_not_flag_queries_with_scope_or_topic():
    plan = compute_query_plan(
        {"dataset": "procesos", "objeto": ["aseo"]},
        "procesos de aseo",
        None,
    )
    assert "unsafe_global_candidate" not in plan.risk_flags


# ═══════════════════════════════════════════════════════════════════════════
# Default — valid query
# ═══════════════════════════════════════════════════════════════════════════

def test_clean_query_executes():
    plan = compute_query_plan(
        {"objeto": ["mantenimiento"], "departamento_resolved": "Bolívar",
         "dataset": "contratos"},
        "contratos de mantenimiento en bolívar",
        None,
    )
    assert plan.should_execute is True
    assert "valid_query" in plan.reason


# ═══════════════════════════════════════════════════════════════════════════
# Idempotency & purity
# ═══════════════════════════════════════════════════════════════════════════

def test_compute_is_pure():
    params = {"objeto": ["mantenimiento"], "dataset": "contratos"}
    p1 = compute_query_plan(dict(params), "mantenimiento bolívar", None)
    p2 = compute_query_plan(dict(params), "mantenimiento bolívar", None)
    assert p1.should_execute == p2.should_execute
    assert p1.params == p2.params


def test_compute_does_not_mutate_input_params():
    params = {"objeto": ["diseño", "ipiales"], "dataset": "procesos"}
    original = deepcopy(params)
    compute_query_plan(params, "diseño ipiales", None)
    assert params == original, "compute_query_plan must not mutate input params"


def test_compute_does_not_mutate_input_context():
    context = {"today": date(2026, 5, 21), "turn_index": 1}
    original = deepcopy(context)
    compute_query_plan({"objeto": ["aseo"]}, "aseo en mayo", context)
    assert context == original


# ═══════════════════════════════════════════════════════════════════════════
# enabled_rules — granularity for B4 sub-blocks
# ═══════════════════════════════════════════════════════════════════════════

def test_enabled_rules_p3_only_activates_city_extraction():
    params = {"objeto": ["diseño", "ipiales"], "dataset": "procesos"}
    plan = compute_query_plan(params, "diseño ipiales", None, enabled_rules={"P3"})
    assert plan.params.get("ciudad") == "Ipiales"
    # P4 NOT active → intent_type should not be promoted
    assert plan.params.get("intent_type") != "opportunity_search"


def test_enabled_rules_none_means_all():
    params = {"objeto": ["mantenimiento"], "dataset": "procesos"}
    plan = compute_query_plan(
        params, "algún proceso para mantenimiento", None, enabled_rules=None
    )
    assert plan.params.get("intent_type") == "opportunity_search"


def test_enabled_rules_empty_set_means_noop():
    params = {"objeto": ["diseño", "ipiales"], "dataset": "procesos"}
    plan = compute_query_plan(params, "diseño ipiales", None, enabled_rules=set())
    # No rules active → params nearly identical to input
    assert "ipiales" in [t.lower() for t in plan.params.get("objeto", [])]
    assert plan.params.get("ciudad") in (None, "")
