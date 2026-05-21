"""B4.3 — E2E tests for P5 (temporal), P6 (number scrub), P7 (discourse filler)."""

import uuid
from datetime import date

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


# ── P5: temporal_month ─────────────────────────────────────────────────────

def test_temporal_month_en_mayo_e2e():
    """'aseo putumayo en mayo' → fecha_desde=2026-05-01, fecha_hasta=2026-05-31."""
    wf = _wf()
    cid = f"test_b43_tmp_{uuid.uuid4().hex[:6]}"
    result = wf.run_query("aseo putumayo en mayo", chat_id=cid)
    plan = result["query_plan"]
    assert plan["params"].get("fecha_desde") == f"{date.today().year}-05-01"
    assert plan["params"].get("fecha_hasta") == f"{date.today().year}-05-31"


def test_temporal_month_este_mes_e2e():
    """'contratos este mes' → should populate current month range."""
    wf = _wf()
    cid = f"test_b43_tmp2_{uuid.uuid4().hex[:6]}"
    result = wf.run_query("contratos este mes", chat_id=cid)
    plan = result["query_plan"]
    today = date.today()
    expected_from = f"{today.year}-{today.month:02d}-01"
    assert plan["params"].get("fecha_desde") == expected_from


# ── P6: number_phrase_scrub ─────────────────────────────────────────────────

def test_number_phrase_scrub_e2e():
    """'transporte escolar huila no supere 25 millones' → valor_max=25M, supere removed."""
    wf = _wf()
    cid = f"test_b43_scrub_{uuid.uuid4().hex[:6]}"
    result = wf.run_query(
        "transporte escolar huila no supere 25 millones", chat_id=cid
    )
    plan = result["query_plan"]
    assert plan["params"].get("valor_max") == 25_000_000


# ── P7: discourse_filler ────────────────────────────────────────────────────

def test_discourse_filler_e2e():
    """'hola estoy interesado en casas lúdicas en barranquilla' → courtesy scrubbed."""
    wf = _wf()
    cid = f"test_b43_fill_{uuid.uuid4().hex[:6]}"
    result = wf.run_query(
        "hola estoy interesado en casas lúdicas en barranquilla", chat_id=cid
    )
    plan = result["query_plan"]
    # Should have filler_scrubbed in reason
    assert "filler_scrubbed" in plan.get("reason", "")
