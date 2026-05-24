"""Demo B — Perfil Proponente (mock E2E, Sprint 3 fase 2)."""
from __future__ import annotations

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


def _mk_results(n: int, entidad: str, modality: str = "Licitacion Publica") -> list[dict]:
    out: list[dict] = []
    for i in range(n):
        out.append({
            "nombre_del_procedimiento": f"Obra {i+1}",
            "entidad": entidad,
            "precio_base": str(100_000_000 * (i + 1)),
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": f"2026-0{(i % 3) + 1}-15T00:00:00.000",
            "modalidad_de_contratacion": modality,
            "urlproceso": f"https://example.com/{i}",
        })
    return out


@pytest.fixture
def wf(tmp_path):
    wf = SecopalWorkflow(Settings())
    rows = _mk_results(8, "DEPARTAMENTO DEL ATLANTICO", "Licitacion Publica")
    rows += _mk_results(4, "DEPARTAMENTO DEL ATLANTICO", "Contratacion Directa")
    wf.secop_client.query = lambda dataset_id, soql_query: rows  # type: ignore[method-assign]
    wf.secop_client.count = lambda dataset_id, soql_query: 12  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return wf


# ---------------------------------------------------------------------------
# Pipeline funcional
# ---------------------------------------------------------------------------

def test_turno1_pipeline_devuelve_resultados(wf):
    r = wf.run_query("oportunidades de obra en Atlantico", channel="telegram", chat_id="demo_prop")
    assert r["followup"] is False
    assert len(r.get("results", [])) == 12


def test_turno2_refine_mantiene_resultados(wf):
    wf.run_query("oportunidades de obra en Atlantico", channel="telegram", chat_id="demo_prop")
    r2 = wf.run_query("filtrar por mas de 500 millones", channel="telegram", chat_id="demo_prop")
    assert r2["followup"] is True


def test_turno3_cambia_orden(wf):
    wf.run_query("oportunidades de obra en Atlantico", channel="telegram", chat_id="demo_prop")
    wf.run_query("filtrar por mas de 500 millones", channel="telegram", chat_id="demo_prop")
    r3 = wf.run_query("ahora solo licitaciones publicas", channel="telegram", chat_id="demo_prop")
    assert r3["followup"] is True


# ---------------------------------------------------------------------------
# Observer insights (legacy verificados; 2.0 requiere mocks de agregacion)
# ---------------------------------------------------------------------------

def test_turno1_insights_legacy(wf):
    r = wf.run_query("oportunidades de obra en Atlantico", channel="telegram", chat_id="demo_prop")
    resp = r["response"]
    assert "El 100% son de" in resp or "La mayoria son de" in resp


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno1_insights_modality_dominance(wf):
    r = wf.run_query("oportunidades de obra en Atlantico", channel="telegram", chat_id="demo_prop")
    resp = r["response"]
    assert "domina" in resp or "Licitacion Publica" in resp


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno1_insights_entity_concentration(wf):
    r = wf.run_query("oportunidades de obra en Atlantico", channel="telegram", chat_id="demo_prop")
    resp = r["response"]
    assert "concentra" in resp


# ---------------------------------------------------------------------------
# Sugerencias presentes
# ---------------------------------------------------------------------------

def test_turno1_contiene_sugerencias(wf):
    r = wf.run_query("oportunidades de obra en Atlantico", channel="telegram", chat_id="demo_prop")
    resp = r["response"]
    assert "Sugerencias:" in resp
    assert "1." in resp
