"""B0 — QUERY-PLAN-LIST-NESTED-001: tolerar grupos OR (listas anidadas) en objeto."""

import pytest

from app.core.query_plan import compute_query_plan


def test_compute_handles_or_groups_in_objeto():
    """D4 shape: objeto puede contener listas anidadas (OR groups).
    query_plan.py debe tolerarlas sin crashear."""
    params = {
        "objeto": ["mantenimiento", ["vial", "calles"]],
        "valor_max": 25_000_000,
        "dataset": "procesos",
    }
    plan = compute_query_plan(
        params, "mantenimiento vial o calles no supere 25M", None
    )
    assert plan is not None
    objeto = plan.params.get("objeto", [])
    assert any(isinstance(t, list) for t in objeto), (
        "OR group should pass through P6/P7 unchanged"
    )


def test_compute_or_group_with_filler():
    """Filler scrub no debe tocar grupos OR."""
    params = {"objeto": ["hola", ["canchas", "parques"]], "dataset": "procesos"}
    plan = compute_query_plan(params, "hola canchas o parques", None)
    assert plan is not None
    objeto = plan.params.get("objeto", [])
    assert "hola" not in objeto
    assert any(isinstance(t, list) for t in objeto)


def test_compute_pure_strings_still_work():
    """Regresión inversa: objeto sin listas sigue funcionando."""
    params = {"objeto": ["mantenimiento", "vial"], "dataset": "procesos"}
    plan = compute_query_plan(params, "mantenimiento vial", None)
    assert "mantenimiento" in plan.params["objeto"]
    assert "vial" in plan.params["objeto"]
