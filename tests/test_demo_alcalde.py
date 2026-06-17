"""Demo A — Perfil Alcalde (mock E2E, Sprint 3 fase 2).

Flujo actualizado tras OPP-SCOPE-001 / P15 (Opción A):
  T1 "procesos abiertos en Barranquilla" → browse territorial sin rubro:
     P15 BLOQUEA y pide rubro SIN tocar SECOP (evita el volcado que
     generaba timeouts).
  T2 "solo los de infraestructura vial" → recupera: hereda Barranquilla
     (B5b), añade rubro, ejecuta. Aquí salen resultados + insights +
     sugerencias.
  T3 "ordena por mayor valor" → follow-up, ORDER BY DESC.
"""
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
    calls = {"query": 0, "count": 0}

    def _query(dataset_id, soql_query):
        calls["query"] += 1
        return _mk_results(15, "ALCALDIA DE BARRANQUILLA")

    def _count(dataset_id, soql_query):
        calls["count"] += 1
        return 15

    wf.secop_client.query = _query  # type: ignore[method-assign]
    wf.secop_client.count = _count  # type: ignore[method-assign]
    wf.narrator.narrate_with_grounding = lambda **kw: None  # type: ignore[method-assign]
    wf.conv_store = ConversationStore(base_path=tmp_path / "conversations")
    wf._secop_calls = calls  # test hook
    return wf


# ---------------------------------------------------------------------------
# T1 — browse territorial sin rubro: bloquea y pide rubro, sin tocar SECOP
# ---------------------------------------------------------------------------

def test_turno1_bloquea_y_pide_rubro(wf):
    r = wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    assert r["needs_clarification"] is True
    assert r["followup"] is False
    assert len(r.get("results", [])) == 0
    assert r.get("total_count") == 0
    # Pide un rubro/objeto
    resp = r["response"].lower()
    assert "producto" in resp or "servicio" in resp or "obra" in resp


def test_turno1_no_consulta_secop(wf):
    """La garantía de Opción A: la consulta amplia NO se ejecuta."""
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    assert wf._secop_calls == {"query": 0, "count": 0}


# ---------------------------------------------------------------------------
# T2 — recuperación: hereda Barranquilla, añade rubro, ejecuta
# ---------------------------------------------------------------------------

def test_turno2_recupera_y_devuelve_resultados(wf):
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    r2 = wf.run_query("solo los de infraestructura vial", channel="telegram", chat_id="demo_alc")
    assert r2["followup"] is True
    assert r2["needs_clarification"] is False
    assert len(r2.get("results", [])) == 15
    assert r2["total_count"] == 15
    assert wf._secop_calls["query"] == 1  # solo se consulta tras el rubro


def test_turno2_contiene_insights_legacy(wf):
    """Insights legacy (dominant_entity, temporal_concentration) se renderizan."""
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    r2 = wf.run_query("solo los de infraestructura vial", channel="telegram", chat_id="demo_alc")
    resp = r2["response"]
    assert "El 100% son de" in resp or "La mayoria son de" in resp


def test_turno2_contiene_sugerencias(wf):
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    r2 = wf.run_query("solo los de infraestructura vial", channel="telegram", chat_id="demo_alc")
    resp = r2["response"]
    assert "Sugerencias:" in resp
    assert "1." in resp


# ---------------------------------------------------------------------------
# Mecánica conversacional
# ---------------------------------------------------------------------------

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
# Observer 2.0 (requiere mocks de agregacion adicionales) — sobre T2
# ---------------------------------------------------------------------------

@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno2_contiene_insights_entity_concentration(wf):
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    r2 = wf.run_query("solo los de infraestructura vial", channel="telegram", chat_id="demo_alc")
    assert "concentra" in r2["response"]


@pytest.mark.xfail(reason="Observer 2.0 signals requieren mock de aggregate() adicional", strict=False)
def test_turno2_contiene_insights_modality_dominance(wf):
    wf.run_query("procesos abiertos en Barranquilla", channel="telegram", chat_id="demo_alc")
    r2 = wf.run_query("solo los de infraestructura vial", channel="telegram", chat_id="demo_alc")
    assert "Licitacion Publica" in r2["response"] or "domina" in r2["response"]
