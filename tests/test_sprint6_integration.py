"""Integration tests E2E — Sprint 6 features: polysemic, ecosystem, BM25."""
from __future__ import annotations

import pytest

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow


def _mk_results(n: int, entidad: str = "ALCALDIA DE BOGOTA", obj_text: str = "Obra civil general") -> list[dict]:
    out: list[dict] = []
    for i in range(n):
        out.append({
            "nombre_del_procedimiento": f"{obj_text} {i+1}",
            "entidad": entidad,
            "precio_base": str(100_000_000 * (i + 1)),
            "estado_de_apertura_del_proceso": "Abierto",
            "fecha_de_publicacion_del": "2026-03-15T00:00:00.000",
            "modalidad_de_contratacion": "Licitacion Publica",
            "urlproceso": f"https://example.com/{i}",
        })
    return out


@pytest.fixture
def wf(tmp_path):
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = lambda dataset_id, soql_query: _mk_results(5)  # type: ignore[method-assign]
    wf.secop_client.count = lambda dataset_id, soql_query: 5  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    return wf


# POLYSEMIC — no crashea
def test_polysemic_no_crashea(wf):
    r = wf.run_query("busco alojamiento en Bogota", channel="streamlit")
    assert len(r["response"]) > 0


# ECOSYSTEM — SoQL contiene Bogotá
def test_ecosystem_bogota_soql(wf):
    r = wf.run_query("contratos de Bogota", channel="streamlit")
    soql = r.get("soql_query", "")
    assert len(soql) > 0
    assert "bogota" in soql.lower() or "Bogotá" in soql


# BM25 — no crashea sin objeto
def test_bm25_no_crashea(wf):
    r = wf.run_query("contratos en Bogota", channel="streamlit")
    assert len(r["response"]) > 0


# BM25 — activo con >=3 términos
def test_bm25_activo(wf):
    wf.secop_client.query = lambda dataset_id, soql_query: _mk_results(3, "ALCALDIA DE BOGOTA", obj_text="Control de calidad de agua potable")  # type: ignore[method-assign]
    r = wf.run_query("control de calidad de agua potable en Bogota", channel="streamlit")
    assert len(r["response"]) > 0
