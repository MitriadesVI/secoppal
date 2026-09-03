"""Characterization tests for QP-INV before consolidating QueryPlan invariants."""

from __future__ import annotations

from datetime import date

import pytest

from app.core.opportunity_policy import enforce_bidder_opportunity_policy
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder, UnsafeGlobalQueryError


TODAY = date(2026, 6, 3)


def run_offline_invariant_pipeline(raw_query: str, *, simulate_llm: bool = False) -> dict:
    """Replicate production order without network: parse -> optional LLM policy -> plan."""
    parsed = QueryRouter().parse(raw_query)
    params = dict(parsed.params)
    snap_after_parse = dict(params)

    if simulate_llm:
        params = enforce_bidder_opportunity_policy(raw_query, params)

    snap_before_plan = dict(params)
    plan = compute_query_plan(params, raw_query, {"today": TODAY})

    return {
        "after_parse": snap_after_parse,
        "before_plan": snap_before_plan,
        "after_plan": plan.params,
        "should_execute": plan.should_execute,
        "risk_flags": plan.risk_flags,
        "reason": plan.reason,
    }


def build_where(params: dict) -> str:
    dataset_id = SoQLBuilder.dataset_id_for(params.get("dataset"))
    soql = SoQLBuilder().build(dataset_id, params)
    return soql.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]


@pytest.mark.parametrize("simulate_llm", [False, True])
def test_signed_contracts_apply_no_state_filter(simulate_llm):
    # "firmado" ⇔ fecha_de_firma not null, NO un valor de estado_contrato.
    # Validado contra datos: los 8 estados con firma son ~100% firmados; el
    # mapeo legacy "firmado"→"Aprobado" tiraba ~97% del recall (Aprobado es
    # 184K de ~5.6M firmados). "firmado" solo fuerza dataset=contratos.
    snap = run_offline_invariant_pipeline(
        "contratos firmados de mantenimiento",
        simulate_llm=simulate_llm,
    )

    assert snap["after_plan"]["dataset"] == "contratos"
    assert snap["after_plan"]["objeto"] == ["mantenimiento"]
    assert snap["after_plan"].get("estado") is None
    assert snap["after_plan"].get("estado_field") is None
    assert "estado_contrato" not in snap["after_plan"]
    assert snap["should_execute"] is True
    assert snap["risk_flags"] == []
    assert snap["reason"] == "valid_query"


@pytest.mark.parametrize("simulate_llm", [False, True])
def test_signed_closed_contracts_preserve_real_closed_state(simulate_llm):
    snap = run_offline_invariant_pipeline(
        "contratos firmados cerrados",
        simulate_llm=simulate_llm,
    )

    assert snap["after_plan"]["dataset"] == "contratos"
    assert snap["after_plan"]["estado_contrato"] == ["Cerrado"]
    assert snap["after_plan"]["estado_family"] == "contrato_cerrado"
    assert snap["after_plan"]["estado"] == "Cerrado"
    assert snap["risk_flags"] == ["unsafe_global_candidate"]


@pytest.mark.parametrize("simulate_llm", [False, True])
def test_firmados_en_ejecucion_preserves_active_states(simulate_llm):
    snap = run_offline_invariant_pipeline(
        "contratos firmados que estén en ejecución de mantenimiento en Bogotá",
        simulate_llm=simulate_llm,
    )

    assert snap["after_plan"]["dataset"] == "contratos"
    assert snap["after_plan"]["departamento_resolved"] == "Distrito Capital de Bogotá"
    assert snap["after_plan"]["objeto"] == ["mantenimiento"]
    assert snap["after_plan"]["estado_contrato"] == [
        "En ejecución",
        "Modificado",
        "Prorrogado",
    ]
    assert snap["after_plan"]["estado_family"] == "contrato_activo"
    assert snap["should_execute"] is True
    assert snap["risk_flags"] == []


@pytest.mark.parametrize("simulate_llm", [False, True])
def test_bidder_query_routes_to_opportunity_search(simulate_llm):
    snap = run_offline_invariant_pipeline(
        "quiero presentarme a obras en el departamento de Boyacá",
        simulate_llm=simulate_llm,
    )

    assert snap["after_plan"]["dataset"] == "procesos"
    assert snap["after_plan"]["intent_type"] == "opportunity_search"
    assert snap["after_plan"]["estado_family"] == "oferta_abierta"
    assert snap["after_plan"]["departamento_resolved"] == "Boyacá"
    assert snap["after_plan"]["objeto"] == ["obras"]
    assert snap["should_execute"] is True
    assert snap["risk_flags"] == []


def test_implicit_opportunity_p4_does_not_create_conflicting_state_filters():
    snap = run_offline_invariant_pipeline("algún proceso para mantenimiento")

    assert snap["after_parse"] == {
        "dataset": "procesos",
        "dataset_explicit": True,
        "objeto": ["mantenimiento"],
    }
    assert snap["after_plan"]["intent_type"] == "opportunity_search"
    assert snap["after_plan"]["estado_family"] == "oferta_abierta"
    assert snap["reason"] == "implicit_opportunity_promoted;filler_scrubbed"

    where = build_where(snap["after_plan"])
    assert "estado_de_apertura_del_proceso = 'Abierto'" in where
    assert "estado_del_procedimiento" not in where


def test_topic_only_query_executes_in_query_plan_and_builds_substantial_where():
    snap = run_offline_invariant_pipeline("obras")

    assert snap["after_plan"] == {
        "dataset": "procesos",
        "dataset_explicit": False,
        "objeto": ["obras"],
    }
    assert snap["should_execute"] is True
    assert snap["risk_flags"] == []
    assert snap["reason"] == "valid_query"
    assert "nombre_del_procedimiento" in build_where(snap["after_plan"])


def test_value_only_query_plan_allows_but_soql_builder_blocks_global_query():
    snap = run_offline_invariant_pipeline("procesos entre 30 y 50 millones")

    assert snap["after_plan"] == {
        "dataset": "procesos",
        "dataset_explicit": True,
        "valor_min": 30_000_000,
        "valor_max": 50_000_000,
    }
    assert snap["should_execute"] is True
    assert snap["risk_flags"] == ["unsafe_global_candidate"]
    assert snap["reason"] == "valid_query"

    with pytest.raises(UnsafeGlobalQueryError):
        build_where(snap["after_plan"])
