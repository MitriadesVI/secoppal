"""Dual-dataset Commit 3: presentación agrupada (pura).

format_grouped no construye SoQL, no ejecuta, no rankea. Y el primario NO se
re-ejecuta: sale del estado del pipeline.
"""
from __future__ import annotations

from burr.core import State

from app.core.formatter import Formatter
from app.core.soql_builder import SoQLBuilder
from app.core.workflow.dual_dataset import DatasetGroupResult, build_dataset_groups

CONTRATOS_ID = "jbjy-vk9h"
PROCESOS_ID = "p6dx-8zbt"


def _proc_row(t="AIRES ACONDICIONADOS SEDES"):
    return {"nombre_del_procedimiento": t, "entidad": "DISTRITO BQ", "precio_base": "1000",
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-05-01T00:00:00.000", "urlproceso": "http://p1"}


def _con_row(t="REPUESTOS AIRES ACONDICIONADOS"):
    return {"objeto_del_contrato": t, "nombre_entidad": "DISTRITO BQ", "valor_del_contrato": "175000000",
            "estado_contrato": "En ejecución", "fecha_de_firma": "2026-05-19T00:00:00.000",
            "urlproceso": "http://c1"}


def _grp(name, status, rows, total=0, error=None):
    return DatasetGroupResult(name, status, rows, [],
                              {"total_count": total, "candidate_count": len(rows),
                               "validated_count": len(rows), "candidate_pool_full": False},
                              "SELECT ...", error, {})


# ── 4 casos de salida ────────────────────────────────────────────────────────
def test_both_ok_no_global_sum():
    groups = {"procesos": _grp("procesos", "ok", [_proc_row()], total=214),
              "contratos": _grp("contratos", "ok", [_con_row(), _con_row("OTRO")], total=5202)}
    text, displayed = Formatter().format_grouped(groups, "whatsapp")
    assert "🔎 Procesos" in text and "📄 Contratos" in text
    assert "214" in text and "5,202" in text
    assert "5,416" not in text  # NO suma universos heterogéneos
    assert displayed == {"procesos": 1, "contratos": 2}


def test_partial_timeout_keeps_successful():
    groups = {"procesos": _grp("procesos", "ok", [_proc_row()], total=18),
              "contratos": _grp("contratos", "error", [], error="timeout")}
    text, displayed = Formatter().format_grouped(groups, "whatsapp")
    assert "AIRES ACONDICIONADOS SEDES" in text          # procesos sobrevive
    assert "no respondió a tiempo" in text                # contratos marcado
    assert displayed["contratos"] == 0


def test_empty_distinct_from_error():
    groups = {"procesos": _grp("procesos", "ok", [], total=0),       # vacío, no error
              "contratos": _grp("contratos", "ok", [_con_row()], total=37)}
    text, _ = Formatter().format_grouped(groups, "whatsapp")
    assert "No encontré coincidencias" in text            # empty != error
    assert "no respondió a tiempo" not in text
    assert "REPUESTOS AIRES" in text


def test_both_error_no_fake_zero():
    groups = {"procesos": _grp("procesos", "error", [], error="timeout"),
              "contratos": _grp("contratos", "error", [], error="timeout")}
    text, _ = Formatter().format_grouped(groups, "whatsapp")
    assert "respondieron a tiempo" in text
    assert "0 resultados" not in text and "Encontré 0" not in text


# ── cero re-ejecución del primario ───────────────────────────────────────────
class _CountingSecop:
    def __init__(self, rows):
        self.rows = rows
        self.query_calls = 0

    def query(self, dataset_id, soql):
        self.query_calls += 1
        return list(self.rows)

    def count(self, dataset_id, soql):
        return len(self.rows)


class _Deps:
    def __init__(self, secop):
        self.soql_builder = SoQLBuilder()
        self.secop_client = secop


def test_primary_not_re_executed():
    secop = _CountingSecop([_con_row()])
    deps = _Deps(secop)
    state = State({"dataset_id": PROCESOS_ID,
                   "resolved_params": {"objeto": [["aire", "acondicionado"]],
                                       "dataset_scope": ["procesos", "contratos"]},
                   "results": [_proc_row()], "ranked_results": [_proc_row()],
                   "unvalidated_candidates": [], "candidate_count": 1, "total_count": 1,
                   "soql_query": "SELECT ... WHERE ..."})
    result = {"resolved_params": state.get("resolved_params"),
              "query_plan": {"should_execute": True}, "query_error": ""}
    build_dataset_groups(deps, result, state)
    # solo el SECUNDARIO (contratos) consulta SECOP; el primario sale del state.
    assert secop.query_calls == 1
