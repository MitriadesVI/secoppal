"""Demo A — Perfil Alcalde (mock E2E, Sprint 3 fase 2)."""
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
    wf.secop_client.query = lambda dataset_id, soql_query: _mk_results(15, "ALCALDIA DE BARRANQUILLA")  # type: ignore[method-assign]
    wf.secop_client.count = lambda dataset_id, soql_query: 15  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return wf


# ---------------------------------------------------------------------------
# Pipeline funcional
# ---------------------------------------------------------------------------

def test_turno1_pipeline_devuelve_resultados(wf):
    r = wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    assert r["followup"] is False
    assert len(r.get("results", [])) == 15
    assert r["total_count"] == 15


def test_turno2_es_followup(wf):
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    r2 = wf.run_query("solo los de infraestructura vial", channel="telegram", chat_id="demo_alc")
    assert r2["followup"] is True


def test_turno3_ordena_por_valor(wf):
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    wf.run_query("solo los de infraestructura vial", channel="telegram", chat_id="demo_alc")
    r3 = wf.run_query("ordena por mayor valor", channel="telegram", chat_id="demo_alc")
    assert r3["followup"] is True
    assert "DESC" in r3["soql_query"]


# ---------------------------------------------------------------------------
# Observer insights (legacy verificados; 2.0 requiere mocks de agregacion)
# ---------------------------------------------------------------------------

def test_turno1_contiene_insights_legacy(wf):
    """Insights legacy (dominant_entity, temporal_concentration) se renderizan."""
    r = wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    resp = r["response"]
    assert "El 100% son de" in resp or "La mayoria son de" in resp


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno1_contiene_insights_entity_concentration(wf):
    r = wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    resp = r["response"]
    assert "concentra" in resp


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno1_contiene_insights_modality_dominance(wf):
    r = wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    resp = r["response"]
    assert "Licitacion Publica" in resp or "domina" in resp


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno1_contiene_insights_temporal_skew(wf):
    r = wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    resp = r["response"]
    assert "Q1 2026" in resp or "Concentrado" in resp


# ---------------------------------------------------------------------------
# Sugerencias presentes
# ---------------------------------------------------------------------------

def test_turno1_contiene_sugerencias(wf):
    r = wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    resp = r["response"]
    assert "Sugerencias:" in resp
    assert "1." in resp
