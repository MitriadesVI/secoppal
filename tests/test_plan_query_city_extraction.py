"""B4.1 — E2E tests for P3 (city extraction) + P9 (entity autopromotion purge)."""

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


# ── P3: city extraction ────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_city", [
    ("diseño ipiales", "Ipiales"),
    ("manejo de residuos sólidos olaya", "Olaya"),
    ("silvopastoriles yarumal", "Yarumal"),
])
def test_city_extracted_e2e(query, expected_city):
    wf = _wf()
    result = wf.run_query(query, chat_id=f"test_b41_{expected_city.lower()}")
    plan = result["query_plan"]
    assert plan["params"].get("ciudad") == expected_city, (
        f"QueryPlan should set ciudad={expected_city}, got {plan['params'].get('ciudad')}"
    )
    # Verify should_execute is True (not blocked by topic_missing)
    assert plan["should_execute"] is True, (
        f"Query should execute, reason={plan.get('reason')}"
    )


# ── P9: entity autopromotion purge ─────────────────────────────────────────

@pytest.mark.parametrize("query", [
    "casas lúdicas barranquilla",
    "aires acondicionados barranquilla",
])
def test_p9_no_entity_autopromotion_e2e(query):
    wf = _wf()
    result = wf.run_query(query, chat_id="test_b41_p9")
    plan = result["query_plan"]
    assert plan["params"].get("ciudad") == "Barranquilla"
    assert plan["params"].get("entidad_resolved") in (None, "")
    assert "entity_autopromotion_purged" in plan["risk_flags"]


def test_p9_preserves_entity_with_explicit_marker_e2e():
    wf = _wf()
    result = wf.run_query(
        "contratos de la alcaldía de barranquilla", chat_id="test_b41_marker"
    )
    plan = result["query_plan"]
    ent = plan["params"].get("entidad_resolved")
    if ent:
        assert "barranquilla" in ent.lower()


# ── Regression: clean queries still work ───────────────────────────────────

def test_clean_query_still_works():
    wf = _wf()
    result = wf.run_query(
        "contratos de mantenimiento en bolívar", chat_id="test_b41_reg"
    )
    assert not result.get("needs_clarification"), "clean query should execute"
    assert result["query_plan"]["should_execute"] is True
