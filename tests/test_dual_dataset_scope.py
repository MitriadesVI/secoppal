"""Dual-dataset Commit 1: representación y parsing de dataset_scope.

Solo la SEÑAL. La ejecución de dos hijos (Commit 2) y la presentación agrupada
(Commit 3) van aparte. dataset_scope NO debe llegar al SoQLBuilder: cada hijo
recibe su propio {"dataset": ...}.
"""
from __future__ import annotations

import json

import pytest

from app.core.followup_engine import _merge_raw
from app.core.query_frame import frame_from_params, params_from_frame
from app.core.query_router import QueryRouter


def _p(q):
    return QueryRouter().parse(q).params


# ── parsing de la conjunción ─────────────────────────────────────────────────
@pytest.mark.parametrize("q", [
    "procesos o contratos de aire acondicionado en barranquilla",
    "contratos y procesos de aseo en cali",
    "ambos de mantenimiento vial",
    "buscar en procesos y contratos de alumbrado",
])
def test_conjunction_sets_dual_scope(q):
    p = _p(q)
    assert p.get("dataset_scope") == ["procesos", "contratos"]
    assert p.get("dataset") == "procesos"  # escalar primario, compat


def test_single_dataset_has_no_scope():
    assert "dataset_scope" not in _p("contratos de aseo en cali")
    assert "dataset_scope" not in _p("procesos de transporte escolar")


# ── round-trip por QueryFrame ────────────────────────────────────────────────
def test_round_trip_preserves_scope():
    params = {"objeto": [["aseo"]], "dataset": "procesos",
              "dataset_scope": ["procesos", "contratos"]}
    back = params_from_frame(frame_from_params(params))
    assert back.get("dataset_scope") == ["procesos", "contratos"]


# ── follow-up no pierde dataset_scope (hereda como legacy raw) ───────────────
def test_followup_year_change_keeps_scope():
    prev = {"dataset_scope": ["procesos", "contratos"], "objeto": [["aseo"]]}
    curr = {"fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31"}
    merged = _merge_raw(prev, curr, topic_inherited=True)
    assert merged["dataset_scope"] == ["procesos", "contratos"]


# ── serialización ────────────────────────────────────────────────────────────
def test_scope_is_json_safe():
    p = _p("procesos o contratos de aire acondicionado")
    json.dumps(p)
    assert isinstance(p["dataset_scope"], list)
    assert all(isinstance(x, str) for x in p["dataset_scope"])
