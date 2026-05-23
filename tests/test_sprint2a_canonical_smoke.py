"""B4.5 — Canonical smoke test covering the 5 HIGH backlog bugs E2E."""

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


CANONICAL = [
    # (query, expected assertions dict)
    ("diseño ipiales", {"ciudad": "Ipiales", "objeto_not": ["ipiales"]}),
    ("casas lúdicas barranquilla", {
        "ciudad": "Barranquilla",
        "entidad_resolved": None,
        "risk_flag": "entity_autopromotion_purged",
    }),
    ("aseo putumayo en mayo", {"fecha_from_prefix": "2026-05"}),
    ("transporte escolar huila no supere 25 millones", {
        "valor_max": 25_000_000,
        "objeto_not": ["supere"],
    }),
    ("algún proceso para mantenimiento en bolívar", {
        "intent_type": "opportunity_search",
    }),
]


@pytest.mark.parametrize("query,checks", CANONICAL)
def test_canonical_5_bugs_e2e(query, checks):
    wf = _wf()
    cid = f"test_canon_{uuid.uuid4().hex[:6]}"
    result = wf.run_query(query, chat_id=cid)
    plan = result["query_plan"]
    params = plan["params"]

    if "ciudad" in checks:
        assert params.get("ciudad") == checks["ciudad"]
    if "objeto_not" in checks:
        objeto_lower = [t.lower() for t in params.get("objeto", [])]
        for tok in checks["objeto_not"]:
            assert tok not in objeto_lower, f"{tok} should not be in objeto"
    if "entidad_resolved" in checks:
        assert params.get("entidad_resolved") is checks["entidad_resolved"]
    if "risk_flag" in checks:
        assert checks["risk_flag"] in plan.get("risk_flags", [])
    if "fecha_from_prefix" in checks:
        assert (params.get("fecha_desde") or "").startswith(checks["fecha_from_prefix"])
    if "valor_max" in checks:
        assert params.get("valor_max") == checks["valor_max"]
    if "intent_type" in checks:
        assert params.get("intent_type") == checks["intent_type"]

    # All canonical cases should execute (none blocked)
    assert plan["should_execute"] is True


def test_all_canonical_queries_executed():
    """Quick smoke: all 5 canonical bugs pass without blocking."""
    for query, checks in CANONICAL:
        test_canonical_5_bugs_e2e(query, checks)
