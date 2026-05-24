"""Demo C — Perfil Veedor (mock E2E, Sprint 3 fase 2)."""
from __future__ import annotations

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


def _mk_results(n: int, entidad: str, modality: str = "Contratacion Directa") -> list[dict]:
    out: list[dict] = []
    for i in range(n):
        out.append({
            "nombre_del_procedimiento": f"Servicio {i+1}",
            "entidad": entidad,
            "precio_base": str(50_000_000 * (i + 1)),
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": f"2026-0{(i % 3) + 1}-15T00:00:00.000",
            "modalidad_de_contratacion": modality,
            "urlproceso": f"https://example.com/{i}",
        })
    return out


@pytest.fixture
def wf(tmp_path):
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = lambda dataset_id, soql_query: _mk_results(10, "INVIAS")  # type: ignore[method-assign]
    wf.secop_client.count = lambda dataset_id, soql_query: 10  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return wf


# ---------------------------------------------------------------------------
# Pipeline funcional
# ---------------------------------------------------------------------------

def test_turno1_pipeline_devuelve_resultados(wf):
    r = wf.run_query("contratos del INVIAS", channel="telegram", chat_id="demo_vee")
    assert r["followup"] is False
    assert len(r.get("results", [])) == 10


def test_turno2_hereda_entidad(wf):
    wf.run_query("contratos del INVIAS", channel="telegram", chat_id="demo_vee")
    r2 = wf.run_query("comparar entre años", channel="telegram", chat_id="demo_vee")
    assert r2["followup"] is True
    assert "INVIAS" in r2["soql_query"].upper()


def test_turno3_filtra_por_año(wf):
    wf.run_query("contratos del INVIAS", channel="telegram", chat_id="demo_vee")
    wf.run_query("comparar entre años", channel="telegram", chat_id="demo_vee")
    r3 = wf.run_query("solo los de 2025", channel="telegram", chat_id="demo_vee")
    assert r3["followup"] is True
    soql = r3["soql_query"]
    assert "2025" in soql or "fecha" in soql.lower()


# ---------------------------------------------------------------------------
# Observer insights (legacy verificados; 2.0 requiere mocks de agregacion)
# ---------------------------------------------------------------------------

def test_turno1_insights_legacy(wf):
    r = wf.run_query("contratos del INVIAS", channel="telegram", chat_id="demo_vee")
    resp = r["response"]
    assert "El 100% son de" in resp or "La mayoria son de" in resp


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno1_insights_entity_concentration(wf):
    r = wf.run_query("contratos del INVIAS", channel="telegram", chat_id="demo_vee")
    resp = r["response"]
    assert "concentra" in resp


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno1_insights_modality_dominance(wf):
    r = wf.run_query("contratos del INVIAS", channel="telegram", chat_id="demo_vee")
    resp = r["response"]
    assert "Contratacion Directa" in resp or "domina" in resp


# ---------------------------------------------------------------------------
# Sugerencias presentes
# ---------------------------------------------------------------------------

def test_turno1_contiene_sugerencias(wf):
    r = wf.run_query("contratos del INVIAS", channel="telegram", chat_id="demo_vee")
    resp = r["response"]
    assert "Sugerencias:" in resp
    assert "1." in resp
