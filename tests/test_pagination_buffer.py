"""Paginación A-buffer: cero solapamiento y cero skip entre páginas.

Origen: queries reales 👎 (2026-06-03) — "muestrame mas" repetía resultados
ya vistos. Causa: tres números distintos (fetch 50 / display 25 / avance 10)
que no concordaban. Ver docs/plans/vocab_pagination_plan.md (Track 2).

Estrategia del mock: a diferencia del mock trivial de
``test_newsearch_pagination`` (que devuelve filas idénticas y no permite medir
solapamiento), aquí ``query`` parsea OFFSET/LIMIT del SoQL y devuelve filas con
identidad única por posición absoluta. Cada fila lleva su índice en el título,
de modo que podemos reconstruir exactamente qué se mostró en cada página.
"""
from __future__ import annotations

import re

from app.config import Settings
from app.core.conversation_store import ConversationStore
from app.core.orchestrator import SecopalWorkflow

_TOTAL = 120  # universo simulado


def _make_query(total: int = _TOTAL):
    """Devuelve filas [offset, offset+limit) con id e índice únicos."""

    def query(_ds: str, soql: str) -> list[dict]:
        m_off = re.search(r"OFFSET (\d+)", soql)
        m_lim = re.search(r"LIMIT (\d+)", soql)
        off = int(m_off.group(1)) if m_off else 0
        lim = int(m_lim.group(1)) if m_lim else 50
        rows = []
        for i in range(off, min(off + lim, total)):
            rows.append(
                {
                    "nombre_del_procedimiento": f"Proceso numero {i}",
                    "objeto_del_contrato": f"Proceso numero {i}",
                    "entidad": "ALCALDIA DE BARRANQUILLA",
                    "nombre_entidad": "ALCALDIA DE BARRANQUILLA",
                    "precio_base": str(1_000_000 + i),
                    "valor_del_contrato": str(1_000_000 + i),
                    "estado_de_apertura_del_proceso": "Publicado",
                    "estado_contrato": "En ejecución",
                    "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",
                    "fecha_de_firma": "2026-06-01T00:00:00.000",
                    "urlproceso": f"https://secop.gov.co/p/{i}",
                    "referencia_del_proceso": f"REF-{i}",
                    "id_del_proceso": f"PID-{i}",
                    "referencia_del_contrato": f"REF-{i}",
                    "id_contrato": f"CID-{i}",
                }
            )
        return rows

    return query


def _wf(tmp_path, monkeypatch):
    # El observer (universo > 50) dispara queries de agregación que el mock no
    # sirve; lo neutralizamos para que el test sea rápido y enfocado en paginar.
    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = _make_query()
    wf.secop_client.count = lambda _ds, _soql: _TOTAL
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    return wf


def _shown_indices(result: dict) -> list[int]:
    """Índices realmente mostrados, leídos del título de cada row formateada."""
    out: list[int] = []
    for row in result.get("rows", []):
        m = re.search(r"Proceso numero (\d+)", str(row.get("titulo", "")))
        if m:
            out.append(int(m.group(1)))
    return out


def test_pagination_no_overlap_no_skip_across_three_pages(tmp_path, monkeypatch):
    wf = _wf(tmp_path, monkeypatch)
    chat = "buf_main"

    r1 = wf.run_query(
        "procesos de adulto mayor de alcaldia de barranquilla de 2026",
        channel="streamlit", chat_id=chat,
    )
    r2 = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)
    r3 = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)

    assert r2.get("route_reason") == "pagination_more", r2.get("route_reason")
    assert r3.get("route_reason") == "pagination_more", r3.get("route_reason")

    p1, p2, p3 = _shown_indices(r1), _shown_indices(r2), _shown_indices(r3)

    assert p1 and p2 and p3, f"alguna página vacía: {p1=} {p2=} {p3=}"

    # Cero solapamiento entre páginas consecutivas (el bug original).
    assert not (set(p1) & set(p2)), f"overlap p1∩p2={sorted(set(p1) & set(p2))}"
    assert not (set(p2) & set(p3)), f"overlap p2∩p3={sorted(set(p2) & set(p3))}"
    assert not (set(p1) & set(p3)), f"overlap p1∩p3={sorted(set(p1) & set(p3))}"

    # Cero skip: la unión es un rango contiguo desde 0.
    union = sorted(set(p1) | set(p2) | set(p3))
    assert union == list(range(len(union))), f"hay huecos: {union}"


# ── Guard del invariante que sostiene A-buffer: rank_and_dedupe determinista ──

def _rows(n: int) -> list[dict]:
    return [
        {
            "nombre_del_procedimiento": f"Proceso numero {i}",
            "estado_de_apertura_del_proceso": "Publicado" if i % 2 else "Adjudicado",
            "id_del_proceso": f"PID-{i}",
            "referencia_del_proceso": f"REF-{i}",
        }
        for i in range(n)
    ]


def test_rank_and_dedupe_is_deterministic():
    """Re-pescar y re-rankear la misma ventana debe dar SIEMPRE el mismo orden:
    es lo que garantiza que bloque[0:25] (página 1) y bloque[25:50] (página 2)
    sean rebanadas disjuntas del mismo orden estable."""
    from app.core.formatter import rank_and_dedupe

    params = {"objeto": ["construccion", "parques", "infantiles"]}  # ≥3 → activa BM25
    rows = _rows(40)
    a = rank_and_dedupe(rows, "p6dx-8zbt", params)
    b = rank_and_dedupe(list(rows), "p6dx-8zbt", params)
    ids_a = [r["id_del_proceso"] for r in a]
    ids_b = [r["id_del_proceso"] for r in b]
    assert ids_a == ids_b, "rank_and_dedupe no es determinista"


def test_rank_and_dedupe_collapses_versions_without_cutting():
    """Dedupe por id estable, pero SIN recortar a max_results (devuelve el bloque
    completo — el corte lo hace cada rebanada de página)."""
    from app.core.formatter import rank_and_dedupe

    rows = _rows(30) + _rows(30)  # cada id aparece dos veces
    out = rank_and_dedupe(rows, "p6dx-8zbt", {})
    ids = [r["id_del_proceso"] for r in out]
    assert len(ids) == len(set(ids)) == 30, f"esperaba 30 únicos, hubo {len(ids)}"


# ── Etapa 8A: orden estable por id + fingerprint guard ───────────────────────

def test_order_has_stable_id_tiebreak():
    """El ORDER BY termina en el id (desempate reproducible de fecha+valor empatados)."""
    from app.core.soql_builder import SoQLBuilder
    b = SoQLBuilder()
    soql_p = b.build("p6dx-8zbt", {"objeto": ["aseo"]})
    soql_c = b.build("jbjy-vk9h", {"objeto": ["aseo"]})
    assert "ORDER BY" in soql_p and soql_p.split("ORDER BY")[1].split("LIMIT")[0].strip().endswith("id_del_proceso DESC")
    assert soql_c.split("ORDER BY")[1].split("LIMIT")[0].strip().endswith("id_contrato DESC")


def test_page_fingerprint_stable_and_change_sensitive():
    from app.core.workflow.handlers.pagination import _page_fingerprint
    a = _page_fingerprint({"objeto": ["aseo"], "dataset": "contratos", "_display_offset": 25})
    b = _page_fingerprint({"objeto": ["aseo"], "dataset": "contratos", "_display_offset": 50})
    # El cursor (_display_offset) NO cambia la huella: misma consulta.
    assert a == b
    # Cambiar la consulta o el dataset SÍ la cambia.
    assert a != _page_fingerprint({"objeto": ["vias"], "dataset": "contratos"})
    assert a != _page_fingerprint({"objeto": ["aseo"], "dataset": "procesos"})


def test_pagination_resets_on_incompatible_cursor(tmp_path, monkeypatch):
    """Si el contrato de paginación cambia (deploy), el cursor previo es incompatible
    (huella distinta) → 'más' NO continúa la página vieja: reinicia desde la página 1."""
    import app.core.workflow.handlers.pagination as pag
    wf = _wf(tmp_path, monkeypatch)
    chat = "buf_fp"
    wf.run_query(
        "procesos de adulto mayor de alcaldia de barranquilla de 2026",
        channel="streamlit", chat_id=chat,
    )
    r2 = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)
    p2 = _shown_indices(r2)
    assert p2 and min(p2) >= 25, f"la página 2 debería empezar tras la 1: {p2}"

    # Simular un deploy que cambia el contrato de orden/ranking/validación.
    monkeypatch.setattr(pag, "_PAGINATION_CONTRACT_VERSION", "e8a-CHANGED")
    r3 = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)
    p3 = _shown_indices(r3)
    assert 0 in p3, f"cursor incompatible → debió reiniciar en la página 1: {p3}"
