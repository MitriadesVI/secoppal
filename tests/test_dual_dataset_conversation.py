"""Dual-dataset Commit 4: conversación dual.

dataset_scope se HEREDA en follow-ups compatibles (año/orden conservan "ambos")
pero se SUELTA cuando el turno pivota a un dataset explícito ("solo procesos",
"ahora contratos"). "Otra vez en ambos" lo re-impone por su propio parsing.
"""
from __future__ import annotations

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.followup_engine import detect_and_merge
from app.core.orchestrator import SecopalWorkflow
from app.core.query_frame import frame_from_params

DUAL_PREV = frame_from_params({
    "objeto": [["automatizacion"]],
    "dataset": "procesos",
    "dataset_scope": ["procesos", "contratos"],
})


def _merge(text, current):
    return detect_and_merge(text, current, DUAL_PREV)[1]


# ── pivote a un dataset → suelta el scope, conserva el tema ───────────────────
def test_solo_procesos_collapses_to_single():
    merged = _merge("solo procesos", {"dataset": "procesos", "dataset_explicit": True})
    assert "dataset_scope" not in merged
    assert merged.get("dataset") == "procesos"
    assert merged.get("objeto")  # el tema se hereda (no es búsqueda nueva)


def test_ahora_contratos_pivots_to_single():
    merged = _merge("ahora contratos", {"dataset": "contratos", "dataset_explicit": True})
    assert "dataset_scope" not in merged
    assert merged.get("dataset") == "contratos"
    assert merged.get("objeto")


# ── follow-ups compatibles → conservan "ambos" ───────────────────────────────
def test_year_change_keeps_both():
    merged = _merge("y los de 2025", {"fecha_desde": "2025-01-01", "fecha_hasta": "2025-12-31"})
    assert merged.get("dataset_scope") == ["procesos", "contratos"]


def test_order_change_keeps_both():
    merged = _merge("los de mayor valor", {"ordering_signal": "valor_desc"})
    assert merged.get("dataset_scope") == ["procesos", "contratos"]


# ── volver a pedir ambos → su propio scope manda ─────────────────────────────
def test_again_both_keeps_dual():
    merged = _merge("buscar otra vez en ambos", {
        "dataset": "procesos", "dataset_explicit": True,
        "dataset_scope": ["procesos", "contratos"],
    })
    assert merged.get("dataset_scope") == ["procesos", "contratos"]


# ── "más" tras dual → preguntar cuál (no paginar uno en silencio) ────────────
def test_mas_after_dual_asks_which(tmp_path):
    wf = SecopalWorkflow(Settings())
    rows_proc = [{"nombre_del_procedimiento": "AUTOMATIZACION X", "entidad": "E",
                  "precio_base": "1", "estado_de_apertura_del_proceso": "Abierto",
                  "fecha_de_publicacion_del": "2026-01-01T00:00:00.000",
                  "urlproceso": "u", "referencia_del_proceso": "R"}]
    rows_con = [{"objeto_del_contrato": "AUTOMATIZACION Y", "nombre_entidad": "E",
                 "valor_del_contrato": "1", "estado_contrato": "En ejecución",
                 "fecha_de_firma": "2026-01-01T00:00:00.000", "urlproceso": "u"}]
    wf.secop_client.query = lambda ds, soql: rows_proc if ds == "p6dx-8zbt" else rows_con
    wf.secop_client.count = lambda ds, soql: 1
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    chat = "dual_pag"

    wf.run_query("procesos o contratos de automatizacion", channel="streamlit", chat_id=chat)
    r2 = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)
    resp = r2.get("response", "").lower()
    assert "procesos" in resp and "contratos" in resp   # pregunta por ambos grupos
    assert "más" in resp or "mas" in resp


def test_dual_buffers_advance_groups_independently(tmp_path):
    wf = SecopalWorkflow(Settings())

    def proc(i):
        return {"id_del_proceso": f"PID-{i}", "referencia_del_proceso": f"P-{i}",
                "nombre_del_procedimiento": f"AUTOMATIZACION P {i}", "entidad": "E",
                "precio_base": "1", "estado_de_apertura_del_proceso": "Abierto",
                "fecha_de_publicacion_del": "2026-01-01T00:00:00.000", "urlproceso": "u"}

    def con(i):
        return {"id_contrato": f"CID-{i}", "referencia_del_contrato": f"C-{i}",
                "objeto_del_contrato": f"AUTOMATIZACION C {i}", "nombre_entidad": "E",
                "valor_del_contrato": "1", "estado_contrato": "En ejecución",
                "fecha_de_firma": "2026-01-01T00:00:00.000", "urlproceso": "u"}

    calls = {"p6dx-8zbt": 0, "jbjy-vk9h": 0}

    def query(ds, _soql):
        calls[ds] += 1
        return [proc(i) for i in range(30)] if ds == "p6dx-8zbt" else [con(i) for i in range(30)]

    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: 30
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    chat = "dual_independent"

    first = wf.run_query(
        "procesos o contratos de automatizacion", channel="streamlit", chat_id=chat,
    )
    assert first["dataset_groups"]["procesos"]["displayed_so_far"] == 25
    assert first["dataset_groups"]["contratos"]["displayed_so_far"] == 25
    initial_calls = dict(calls)

    clarify = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)
    assert clarify["route_reason"] == "pagination_dual_clarify"
    more_proc = wf.run_query("muestrame mas procesos", channel="streamlit", chat_id=chat)
    more_con = wf.run_query("muestrame mas contratos", channel="streamlit", chat_id=chat)

    assert [r["id_del_proceso"] for r in more_proc["rows"]] == [f"PID-{i}" for i in range(25, 30)]
    assert [r["id_contrato"] for r in more_con["rows"]] == [f"CID-{i}" for i in range(25, 30)]
    assert more_proc["displayed_so_far"] == 30
    assert more_con["displayed_so_far"] == 30
    assert calls == initial_calls  # ambos salen de sus buffers; cero repesca
