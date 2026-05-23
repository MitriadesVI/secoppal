"""B4.2 — E2E tests for P4 (implicit_opportunity → opportunity_search)."""

import uuid

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow


def _wf():
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = lambda ds, soql: [{
        "precio_base": "1000000",
        "fecha_de_publicacion_del": "2026-01-01",
    }]
    wf.secop_client.count = lambda ds, soql: 1
    wf.narrator.narrate_with_grounding = lambda **kw: None
    return wf


@pytest.mark.parametrize("query", [
    "algún proceso para aseo en putumayo",
    "alguna oferta del sena en huila",
    "hay algún proceso de residuos en antioquia",
    "dónde me puedo presentar para transporte",
    "proceso para mantenimiento eléctrico",
])
def test_implicit_opportunity_e2e(query):
    wf = _wf()
    cid = f"test_b42_opp_{uuid.uuid4().hex[:8]}"
    result = wf.run_query(query, chat_id=cid)
    plan = result["query_plan"]
    assert plan["params"].get("intent_type") == "opportunity_search", (
        f"Expected opportunity_search, got {plan['params'].get('intent_type')}"
    )
    assert plan["params"].get("estado_family") == "oferta_abierta"


def test_explicit_historical_not_promoted():
    """Queries with historical markers should NOT trigger P4."""
    wf = _wf()
    cid = f"test_b42_hist_{uuid.uuid4().hex[:8]}"
    result = wf.run_query("procesos del año pasado", chat_id=cid)
    plan = result["query_plan"]
    assert plan["params"].get("intent_type") != "opportunity_search"
