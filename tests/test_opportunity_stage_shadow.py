"""Fase 2B — shadow del clasificador de etapa de oportunidad.

Parte A: summarizer puro (sin workflow).
Parte B: wiring en normal_flow (mock SECOP, mismo patrón que demo_alcalde).
"""
import copy
import json

import pytest

from app.core.opportunity_stage import (
    CLASSIFIER_VERSION,
    summarize_opportunity_stages,
)


def _proc(estado="Publicado", apertura="Abierto", fase="Presentación de oferta", ref="R"):
    return {
        "referencia_del_proceso": ref,
        "estado_del_procedimiento": estado,
        "estado_de_apertura_del_proceso": apertura,
        "fase": fase,
        "nombre_del_procedimiento": "Obra X",
    }


# ════════════ Parte A — summarizer puro ════════════

def test_counts_are_correct():
    rows = [
        _proc(fase="Presentación de oferta"),               # offer_phase_indicated
        _proc(fase="Presentación de observaciones"),         # published_pre_offer_signal
        _proc("Borrador", fase=None),                        # unpublished_draft_signal
        _proc(fase="Selección de ofertas"),                  # post_offer_signal
        _proc(apertura="Cerrado", fase="Presentación de oferta"),  # conflicting
        _proc(fase="Estimate Phase"),                        # unknown
    ]
    s = summarize_opportunity_stages(rows, total_count=99999)
    assert s["counts"] == {
        "offer_phase_indicated": 1, "published_pre_offer_signal": 1,
        "unpublished_draft_signal": 1, "post_offer_signal": 1,
        "conflicting_signals": 1, "unknown": 1,
    }
    assert s["classified_count"] == 6
    assert s["total_count"] == 99999
    assert s["computed_from"] == "current_result_set"
    assert s["classifier_version"] == CLASSIFIER_VERSION


def test_empty_results_valid_zero_payload():
    s = summarize_opportunity_stages([], total_count=0)
    assert s["classified_count"] == 0
    assert s["counts"] == {}
    assert s["sample"] == []
    assert s["computed_from"] == "current_result_set"


def test_payload_is_json_serializable():
    s = summarize_opportunity_stages([_proc(), _proc(apertura="Cerrado")])
    json.dumps(s)  # no debe lanzar


def test_sample_never_exceeds_limit():
    rows = [_proc(apertura="Cerrado", ref=f"C{i}") for i in range(20)]  # 20 conflictos
    s = summarize_opportunity_stages(rows, sample_limit=10)
    assert len(s["sample"]) <= 10


def test_sample_prioritizes_conflicts_then_unknown():
    rows = (
        [_proc(fase="Presentación de oferta", ref=f"O{i}") for i in range(8)]
        + [_proc(apertura="Cerrado", fase="Presentación de oferta", ref="CONF")]
        + [_proc(fase="Estimate Phase", ref="UNK")]
    )
    s = summarize_opportunity_stages(rows)
    stages = [e["stage"] for e in s["sample"]]
    assert stages[0] == "conflicting_signals"  # conflicto primero
    assert "unknown" in stages


def test_unknown_phases_aggregated_by_normalized_value_keeps_raw():
    rows = [
        _proc(fase="Estimate Phase", ref="A"),
        _proc(fase="estimate  phase", ref="B"),   # misma fase normalizada
        _proc(fase="Pré-Calificación", ref="C"),
    ]
    s = summarize_opportunity_stages(rows)
    # "estimate phase" agrega 2; conserva un ejemplo raw
    by_count = {tuple(x) for x in s["unknown_phases"]}
    assert ["Estimate Phase", 2] in s["unknown_phases"]
    assert any(raw == "Pré-Calificación" and n == 1 for raw, n in s["unknown_phases"])


def test_conflict_patterns_aggregated():
    rows = [_proc(apertura="Cerrado", fase="Presentación de oferta") for _ in range(3)]
    s = summarize_opportunity_stages(rows)
    assert ["offer_phase_but_opening_closed", 3] in s["conflict_patterns"]


def test_malformed_row_is_unknown_not_crash():
    s = summarize_opportunity_stages([{"foo": "bar"}, _proc()])
    assert s["counts"].get("unknown") == 1


def test_deterministic():
    rows = [_proc(apertura="Cerrado"), _proc(fase="Estimate Phase"), _proc()]
    assert summarize_opportunity_stages(rows) == summarize_opportunity_stages(copy.deepcopy(rows))


def test_does_not_mutate_rows():
    rows = [_proc()]
    snap = copy.deepcopy(rows)
    summarize_opportunity_stages(rows)
    assert rows == snap


# ════════════ Parte B — wiring en normal_flow ════════════

from app.config import Settings  # noqa: E402
from app.core.conversation_store import ConversationStore  # noqa: E402
from app.core.orchestrator import SecopalWorkflow  # noqa: E402


def _mk_rows(n=5):
    return [_proc(ref=f"P{i}") for i in range(n)]


@pytest.fixture
def wf(tmp_path):
    wf = SecopalWorkflow(Settings())
    calls = {"n": 0}

    def _query(dataset_id, soql_query):
        calls["n"] += 1
        return _mk_rows(5)

    def _count(dataset_id, soql_query):
        calls["n"] += 1
        return 5

    wf.secop_client.query = _query  # type: ignore[method-assign]
    wf.secop_client.count = _count  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    wf._secop_calls = calls
    return wf


def test_opportunity_emits_shadow(wf):
    r = wf.run_query("procesos abiertos de mantenimiento en barranquilla",
                     channel="telegram", chat_id="opp")
    sh = r.get("opportunity_stage_shadow")
    assert sh is not None
    assert sh["computed_from"] == "current_result_set"
    assert sum(sh["counts"].values()) == len(r.get("results", []))


def test_history_query_has_no_shadow(wf):
    r = wf.run_query("contratos de obra en atlantico 2025", channel="telegram", chat_id="hist")
    assert "opportunity_stage_shadow" not in r


def test_flag_off_no_shadow(wf, monkeypatch):
    import app.core.workflow.handlers.normal_flow as nf
    monkeypatch.setattr(nf, "OPPORTUNITY_STAGE_SHADOW_ENABLED", False)
    r = wf.run_query("procesos abiertos de mantenimiento en barranquilla",
                     channel="telegram", chat_id="off")
    assert "opportunity_stage_shadow" not in r


def test_shadow_failure_does_not_break_response(wf, monkeypatch):
    import app.core.opportunity_stage as os_mod

    def _boom(*a, **k):
        raise RuntimeError("boom")

    monkeypatch.setattr(os_mod, "summarize_opportunity_stages", _boom)
    r = wf.run_query("procesos abiertos de mantenimiento en barranquilla",
                     channel="telegram", chat_id="boom")
    assert r["response"]  # respuesta intacta
    assert "opportunity_stage_shadow" not in r


def test_shadow_adds_no_secop_calls(wf):
    r = wf.run_query("procesos abiertos de mantenimiento en barranquilla",
                     channel="telegram", chat_id="calls")
    # 1 query (resultados). El shadow es puro → no añade llamadas.
    assert wf._secop_calls["n"] == 1
    assert r.get("opportunity_stage_shadow") is not None
