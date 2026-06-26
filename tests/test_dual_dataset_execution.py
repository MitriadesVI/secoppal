"""Dual-dataset Commit 2: ejecución de dos hijos independientes (sin presentar).

Cubre: sanitización por esquema (dual frame-leak), ejecución pura del hijo, fallo
parcial, agrupación, conteos independientes y cero pérdida de filas.
"""
from __future__ import annotations

from burr.core import State

from app.core.soql_builder import SoQLBuilder
from app.core.workflow.dual_dataset import (
    DatasetGroupResult,
    build_dataset_groups,
    execute_dataset_child,
    params_for_dataset,
)

PROCESOS_ID = "p6dx-8zbt"
CONTRATOS_ID = "jbjy-vk9h"


class _FakeSecop:
    def __init__(self, by_id, raise_for=None):
        self.by_id = by_id
        self.raise_for = raise_for or set()

    def query(self, dataset_id, soql):
        if dataset_id in self.raise_for:
            raise TimeoutError("SECOP timed out")
        return list(self.by_id.get(dataset_id, []))

    def count(self, dataset_id, soql):
        return len(self.by_id.get(dataset_id, []))


class _FakeDeps:
    def __init__(self, secop):
        self.soql_builder = SoQLBuilder()
        self.secop_client = secop


# ── sanitización por esquema (dual frame-leak) ───────────────────────────────
def test_params_for_dataset_drops_cross_schema_state():
    common = {
        "objeto": [["aseo"]], "objeto_phrases": ["manual de contratacion"],
        "entidad_resolved": "X", "fecha_desde": "2025-01-01",
        "estado_contrato": "En ejecución", "estado_del_procedimiento": ["Abierto"],
        "dataset_scope": ["procesos", "contratos"], "dataset": "procesos",
    }
    proc = params_for_dataset(common, "procesos")
    assert proc["dataset"] == "procesos"
    assert "dataset_scope" not in proc
    assert "estado_contrato" not in proc           # campo de contratos no cruza
    assert proc.get("estado_del_procedimiento") == ["Abierto"]  # propio se conserva
    assert proc["objeto_phrases"] == ["manual de contratacion"]  # frase llega a ambos
    assert proc["entidad_resolved"] == "X" and proc["fecha_desde"] == "2025-01-01"

    con = params_for_dataset(common, "contratos")
    assert con["dataset"] == "contratos"
    assert "estado_del_procedimiento" not in con   # campo de procesos no cruza
    assert con.get("estado_contrato") == "En ejecución"
    assert con["objeto_phrases"] == ["manual de contratacion"]


# ── ejecución pura del hijo ──────────────────────────────────────────────────
def test_execute_child_ok_and_no_row_loss():
    rows = [{"objeto_del_contrato": f"ASEO {i}", "descripcion_del_proceso": ""} for i in range(7)]
    deps = _FakeDeps(_FakeSecop({CONTRATOS_ID: rows}))
    g = execute_dataset_child("contratos", {"objeto": [["aseo"]]}, deps)
    assert g.status == "ok" and g.dataset == "contratos"
    assert g.counts["candidate_count"] == 7
    # invariante: ninguna fila se pierde entre ranked y cuarentena
    assert len(g.rows) + len(g.unvalidated_candidates) == 7


def test_execute_child_timeout_is_error_not_empty():
    deps = _FakeDeps(_FakeSecop({}, raise_for={CONTRATOS_ID}))
    g = execute_dataset_child("contratos", {"objeto": [["aseo"]]}, deps)
    assert g.status == "error"
    assert g.error and g.rows == []


# ── agrupación: dos hermanos, conteos independientes ─────────────────────────
def _dual_state(primary_rows):
    return State({
        "dataset_id": PROCESOS_ID,
        "resolved_params": {"objeto": [["aseo"]], "dataset_scope": ["procesos", "contratos"]},
        "results": primary_rows, "ranked_results": primary_rows,
        "unvalidated_candidates": [], "candidate_count": len(primary_rows),
        "total_count": len(primary_rows), "soql_query": "SELECT ... WHERE ...",
    })


def _dual_result():
    return {"resolved_params": {"objeto": [["aseo"]], "dataset_scope": ["procesos", "contratos"]},
            "query_plan": {"should_execute": True}, "query_error": ""}


def test_build_groups_runs_both_independently():
    proc_rows = [{"nombre_del_procedimiento": "ASEO P", "descripci_n_del_procedimiento": ""}]
    con_rows = [{"objeto_del_contrato": "ASEO C1", "descripcion_del_proceso": ""},
                {"objeto_del_contrato": "ASEO C2", "descripcion_del_proceso": ""}]
    deps = _FakeDeps(_FakeSecop({CONTRATOS_ID: con_rows}))
    groups = build_dataset_groups(deps, _dual_result(), _dual_state(proc_rows))
    assert set(groups) == {"procesos", "contratos"}
    assert groups["procesos"].status == "ok" and groups["contratos"].status == "ok"
    # conteos independientes, no se pisan
    assert groups["procesos"].counts["candidate_count"] == 1
    assert groups["contratos"].counts["candidate_count"] == 2


def test_build_groups_partial_failure_keeps_successful():
    proc_rows = [{"nombre_del_procedimiento": "ASEO P", "descripci_n_del_procedimiento": ""}]
    deps = _FakeDeps(_FakeSecop({}, raise_for={CONTRATOS_ID}))  # contratos falla
    groups = build_dataset_groups(deps, _dual_result(), _dual_state(proc_rows))
    assert groups["procesos"].status == "ok"     # el exitoso sobrevive
    assert groups["contratos"].status == "error"  # no convierte todo en cero


def test_single_dataset_query_has_no_groups():
    state = State({"dataset_id": CONTRATOS_ID,
                   "resolved_params": {"objeto": [["aseo"]]}, "results": [], "ranked_results": []})
    result = {"resolved_params": {"objeto": [["aseo"]]}, "query_plan": {"should_execute": True}}
    assert build_dataset_groups(_FakeDeps(_FakeSecop({})), result, state) is None
