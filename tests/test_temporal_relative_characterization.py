"""Etapa 4 — Verificación: fechas relativas semana/día producen rangos correctos.

Este test reemplaza al de Etapa 0: ahora afirma el comportamiento CORRECTO tras el fix.
"""

import uuid
from datetime import date, timedelta

import pytest

from app.config import Settings
from app.core.orchestrator import SecopalWorkflow
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter

# Fixed reference date: Thursday, June 4, 2026
TODAY = date(2026, 6, 4)
# Monday of this week (ISO): June 1, 2026
THIS_MONDAY = date(2026, 6, 1)
# Last Monday: May 25, 2026
LAST_MONDAY = date(2026, 5, 25)
# Last Sunday: May 31, 2026
LAST_SUNDAY = date(2026, 5, 31)


def _parse_and_plan(query: str):
    """Helper: QueryRouter.parse() → compute_query_plan(), return QueryPlan."""
    result = QueryRouter().parse(query, today=TODAY)
    return compute_query_plan(result.params, query, {"today": TODAY})


# ═══════════════════════════════════════════════════════════════════════════
# "hoy" → desde = hasta = today
# ═══════════════════════════════════════════════════════════════════════════

def test_hoy_produce_rango_hoy():
    plan = _parse_and_plan("contratos hoy")
    assert plan.params["fecha_desde"] == "2026-06-04"
    assert plan.params["fecha_hasta"] == "2026-06-04"


# ═══════════════════════════════════════════════════════════════════════════
# "ayer" → desde = hasta = today - 1
# ═══════════════════════════════════════════════════════════════════════════

def test_ayer_produce_rango_ayer():
    plan = _parse_and_plan("contratos ayer")
    assert plan.params["fecha_desde"] == "2026-06-03"
    assert plan.params["fecha_hasta"] == "2026-06-03"


# ═══════════════════════════════════════════════════════════════════════════
# "esta semana" → semana civil ISO completa (lunes a domingo)
# ═══════════════════════════════════════════════════════════════════════════

def test_esta_semana_produce_rango_lunes_a_domingo():
    plan = _parse_and_plan("contratos esta semana")
    assert plan.params["fecha_desde"] == "2026-06-01"  # Monday
    assert plan.params["fecha_hasta"] == "2026-06-07"  # Sunday


# ═══════════════════════════════════════════════════════════════════════════
# "la semana pasada" / "semana pasada" → desde = lunes prev, hasta = domingo prev
# ═══════════════════════════════════════════════════════════════════════════

def test_semana_pasada_produce_rango_lunes_a_domingo_previo():
    plan = _parse_and_plan("contratos la semana pasada")
    assert plan.params["fecha_desde"] == "2026-05-25"  # Last Monday
    assert plan.params["fecha_hasta"] == "2026-05-31"  # Last Sunday


def test_semana_pasada_sin_la():
    plan = _parse_and_plan("contratos semana pasada")
    assert plan.params["fecha_desde"] == "2026-05-25"
    assert plan.params["fecha_hasta"] == "2026-05-31"


# ═══════════════════════════════════════════════════════════════════════════
# "últimos N días" → desde = today - N, hasta = today
# ═══════════════════════════════════════════════════════════════════════════

def test_ultimos_30_dias_produce_rango():
    plan = _parse_and_plan("contratos últimos 30 días")
    assert plan.params["fecha_desde"] == "2026-05-05"  # today - 30
    assert plan.params["fecha_hasta"] == "2026-06-04"


def test_ultimos_7_dias_produce_rango():
    plan = _parse_and_plan("contratos últimos 7 días")
    assert plan.params["fecha_desde"] == "2026-05-28"
    assert plan.params["fecha_hasta"] == "2026-06-04"


def test_ultimos_365_dias_produce_rango():
    """Boundary: 365 days back."""
    plan = _parse_and_plan("contratos últimos 365 días")
    assert plan.params["fecha_desde"] == "2025-06-04"  # today - 365
    assert plan.params["fecha_hasta"] == "2026-06-04"


def test_ultimos_dias_con_tilde():
    """'últimos' with accent also works."""
    plan = _parse_and_plan("contratos últimos 15 días")
    assert plan.params["fecha_desde"] == "2026-05-20"
    assert plan.params["fecha_hasta"] == "2026-06-04"


# ═══════════════════════════════════════════════════════════════════════════
# NO LEAK: tokens temporales NO se cuelan al objeto
# ═══════════════════════════════════════════════════════════════════════════

def test_semana_no_leak_al_objeto():
    plan = _parse_and_plan("contratos esta semana")
    objeto = [t for t in plan.params.get("objeto", []) if isinstance(t, str)]
    assert "semana" not in [t.lower() for t in objeto], (
        "FIX: 'semana' NO debe leak al objeto (ya está en STOPWORDS)"
    )


def test_dias_no_leak_al_objeto():
    plan = _parse_and_plan("contratos últimos 30 días")
    objeto = [t for t in plan.params.get("objeto", []) if isinstance(t, str)]
    obj_lower = [t.lower() for t in objeto]
    assert "dias" not in obj_lower, (
        "FIX: 'dias' NO debe leak al objeto (ya está en STOPWORDS)"
    )
    assert "días" not in obj_lower


def test_hoy_no_leak_al_objeto():
    plan = _parse_and_plan("contratos hoy")
    objeto = [t for t in plan.params.get("objeto", []) if isinstance(t, str)]
    assert "hoy" not in [t.lower() for t in objeto]


def test_ayer_no_leak_al_objeto():
    plan = _parse_and_plan("contratos ayer")
    objeto = [t for t in plan.params.get("objeto", []) if isinstance(t, str)]
    assert "ayer" not in [t.lower() for t in objeto]


# ═══════════════════════════════════════════════════════════════════════════
# No regresión: temporal_month existente sigue funcionando
# ═══════════════════════════════════════════════════════════════════════════

def test_temporal_month_no_afectado():
    """'en mayo' sigue produciendo rango de mes vía P5."""
    plan = _parse_and_plan("contratos en mayo")
    assert plan.params["fecha_desde"] == "2026-05-01"
    assert plan.params["fecha_hasta"] == "2026-05-31"


def test_temporal_conflict_cuando_ya_hay_fecha():
    """Si ya hay fecha_desde (ej. de un mes), temporal_range no sobreescribe."""
    plan = compute_query_plan(
        {"fecha_desde": "2026-01-01", "objeto": []},
        "contratos esta semana",
        {"today": TODAY},
    )
    assert plan.params["fecha_desde"] == "2026-01-01"  # unchanged
    assert "temporal_conflict" in plan.risk_flags
