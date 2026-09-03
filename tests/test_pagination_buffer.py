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
    # Gate E8B.1: no basta la disjuncion; el orden concatenado debe ser el
    # prefijo exacto de la secuencia completa (literal simple).
    assert p1 + p2 + p3 == list(range(len(p1 + p2 + p3)))


def test_characterization_phrase_pagination_matches_validated_ranked_prefix(tmp_path, monkeypatch):
    """E8B.1: ``mas`` debe continuar la secuencia que produjo la pagina 1.

    El pool recall-first mezcla 30 coincidencias validadas con 30 candidatos que
    solo comparten ruido. La ruta historica de paginacion no ejecutaba el
    validador PhraseSpec y dejaba reaparecer ese ruido en la pagina 2.
    """
    from app.core.phrase_ranking import rank_candidate_pool

    rows = []
    for i in range(60):
        valid = i < 30
        objeto = (
            f"Elaboracion del manual de contratacion {i}"
            if valid
            else f"Manual operativo para pavimentacion {i}"
        )
        rows.append({
            "objeto_del_contrato": objeto,
            "descripcion_del_proceso": "",
            "nombre_entidad": "ALCALDIA DE BARRANQUILLA",
            "valor_del_contrato": str(1_000_000 + i),
            "estado_contrato": "En ejecucion",
            "fecha_de_firma": "2026-06-01T00:00:00.000",
            "urlproceso": f"https://secop.gov.co/c/{i}",
            "referencia_del_contrato": f"REF-{i:03d}",
            "id_contrato": f"CID-{i:03d}",
        })

    def query(_ds: str, soql: str) -> list[dict]:
        m_off = re.search(r"OFFSET (\d+)", soql)
        m_lim = re.search(r"LIMIT (\d+)", soql)
        off = int(m_off.group(1)) if m_off else 0
        lim = int(m_lim.group(1)) if m_lim else 50
        return rows[off:off + lim]

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: len(rows)
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")

    chat = "e8b_phrase_characterization"
    first = wf.run_query(
        "contratos de manual de contratacion de 2026",
        channel="streamlit", chat_id=chat,
    )
    second = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)

    params = first["resolved_params"]
    expected = rank_candidate_pool(rows, params, "jbjy-vk9h").ranked_results
    expected_ids = [row["id_contrato"] for row in expected]
    displayed_ids = [
        row["id_contrato"]
        for result in (first, second)
        for row in result.get("rows", [])
    ]

    assert displayed_ids == expected_ids[:len(displayed_ids)]


def test_exclusions_and_source_ties_keep_exact_concatenated_prefix(tmp_path, monkeypatch):
    """Exclusiones sobreviven al refetch y el id desempata fecha+valor iguales."""
    source = []
    for i in reversed(range(65)):  # simula ORDER BY fecha, valor, id DESC
        excluded = i % 6 == 0
        title = f"Aseo con interventoria {i}" if excluded else f"Servicio de aseo {i}"
        source.append({
            "nombre_del_procedimiento": title,
            "descripci_n_del_procedimiento": "",
            "entidad": "ALCALDIA DE BARRANQUILLA",
            "precio_base": "1000000",  # empate de valor
            "estado_de_apertura_del_proceso": "Publicado",
            "fecha_de_publicacion_del": "2026-06-01T00:00:00.000",  # empate de fecha
            "urlproceso": f"https://secop.gov.co/p/{i}",
            "referencia_del_proceso": f"REF-{i:03d}",
            "id_del_proceso": f"PID-{i:03d}",
        })

    expected = [row for row in source if "interventoria" not in row["nombre_del_procedimiento"]]

    def query(_ds: str, soql: str) -> list[dict]:
        rows = expected if "NOT (" in soql else source
        m_off = re.search(r"OFFSET (\d+)", soql)
        m_lim = re.search(r"LIMIT (\d+)", soql)
        off = int(m_off.group(1)) if m_off else 0
        lim = int(m_lim.group(1)) if m_lim else 50
        return rows[off:off + lim]

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: len(expected)
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")

    chat = "e8b_exclusions_ties"
    pages = [wf.run_query(
        "procesos de aseo excepto interventoria de 2026",
        channel="streamlit", chat_id=chat,
    )]
    pages.append(wf.run_query("muestrame mas", channel="streamlit", chat_id=chat))
    pages.append(wf.run_query("muestrame mas", channel="streamlit", chat_id=chat))

    displayed_ids = [
        row["id_del_proceso"]
        for page in pages
        for row in page.get("rows", [])
    ]
    expected_ids = [row["id_del_proceso"] for row in expected]

    assert pages[0]["resolved_params"]["objeto_exclude"] == ["interventoria"]
    assert displayed_ids == expected_ids[:len(displayed_ids)]
    assert len(displayed_ids) == len(expected_ids)


def test_canonical_route_preserves_first_page_rows_and_order():
    """Aplicar la ruta canonica antes del formatter no cambia la pagina 1."""
    from app.core.formatter import Formatter
    from app.core.phrase_ranking import rank_candidate_pool, validate_rank_and_dedupe

    rows = [
        {
            "objeto_del_contrato": "manual de contratacion",
            "descripcion_del_proceso": "",
            "nombre_entidad": "Entidad",
            "estado_contrato": "Liquidado" if i % 2 else "En ejecucion",
            "id_contrato": f"CID-{i}",
            "referencia_del_contrato": f"REF-{i}",
            "valor_del_contrato": str(i),
        }
        for i in range(30)
    ]
    rows += [dict(rows[0])]  # version duplicada: el display historico ya la colapsaba
    params = {"objeto_phrases": ["manual de contratacion"]}
    formatter = Formatter(max_results=25)

    previous = formatter.to_rows(
        rank_candidate_pool(rows, params, "jbjy-vk9h").ranked_results,
        "jbjy-vk9h", params,
    )
    canonical = formatter.to_rows(
        validate_rank_and_dedupe(rows, params, "jbjy-vk9h").ranked_results,
        "jbjy-vk9h", params,
    )

    assert canonical == previous


def test_combined_phrase_levels_dedupe_ties_and_three_page_prefix(tmp_path, monkeypatch):
    """Gate combinado: 100 candidatos y tres paginas del mismo orden canonico."""
    from copy import deepcopy

    from app.core.phrase_ranking import validate_rank_and_dedupe

    source = []
    for i in range(100):
        if i < 50:
            objeto = f"Manual de contratacion {i}"  # direct
        elif i < 85:
            objeto = f"Manual para uso de contratacion {i}"  # probable (gap=3)
        else:
            objeto = f"Manual operativo de pavimentacion {i}"  # none
        identity = i - 45 if 45 <= i < 50 else i  # cinco duplicados direct
        source.append({
            "objeto_del_contrato": objeto,
            "descripcion_del_proceso": "",
            "nombre_entidad": "ALCALDIA DE BARRANQUILLA",
            "valor_del_contrato": "1000000",  # empate de valor
            "estado_contrato": "En ejecución",
            "fecha_de_firma": "2026-06-01T00:00:00.000",  # empate de fecha
            "urlproceso": f"https://secop.gov.co/c/{i}",
            "referencia_del_contrato": f"REF-{identity:03d}",
            "id_contrato": f"CID-{identity:03d}",
        })

    def query(_ds: str, soql: str) -> list[dict]:
        m_off = re.search(r"OFFSET (\d+)", soql)
        m_lim = re.search(r"LIMIT (\d+)", soql)
        off = int(m_off.group(1)) if m_off else 0
        lim = int(m_lim.group(1)) if m_lim else 200
        return source[off:off + lim]

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: len(source)
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")

    chat = "e8b_combined_100"
    pages = [wf.run_query(
        "contratos de manual de contratacion de 2026",
        channel="streamlit", chat_id=chat,
    )]
    pages.append(wf.run_query("muestrame mas", channel="streamlit", chat_id=chat))
    pages.append(wf.run_query("muestrame mas", channel="streamlit", chat_id=chat))

    params = pages[0]["resolved_params"]
    source_before, params_before = deepcopy(source), deepcopy(params)
    canonical = validate_rank_and_dedupe(source, params, "jbjy-vk9h")
    repeated = validate_rank_and_dedupe(source, params, "jbjy-vk9h")
    expected_ids = [row["id_contrato"] for row in canonical.ranked_results]
    displayed_ids = [
        row["id_contrato"]
        for page in pages
        for row in page.get("rows", [])
    ]

    assert canonical.phrase_counts == {"direct": 50, "probable": 35, "weak": 0, "none": 15}
    assert len(expected_ids) == 80  # 85 validados menos cinco versiones duplicadas
    assert displayed_ids == expected_ids[:75]
    assert pages[1]["candidate_count"] == 95  # candidatos unicos persistidos
    assert pages[1]["validated_count"] == 80
    assert pages[1]["quarantined_count"] == 15
    assert pages[1]["pagination_validation_ms"] >= 0
    assert pages[0]["source_candidate_total"] == 100
    assert pages[0]["validated_total_known"] == 80
    assert pages[0]["displayed_so_far"] == 25
    assert pages[1]["displayed_so_far"] == 50
    assert pages[2]["displayed_so_far"] == 75
    assert [row["id_contrato"] for row in repeated.ranked_results] == expected_ids
    assert source == source_before and params == params_before


def test_pagination_keeps_source_without_post_query_ecosystem(tmp_path, monkeypatch):
    """ADR-074: ninguna expansión geográfica ocurre tras consultar la fuente."""
    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "app.core.entity_ecosystem.should_expand_ecosystem",
        lambda _params: True,
    )
    expansions: list[dict] = []

    def _ecosystem(**kwargs):
        expansions.append(kwargs)
        return ["ENTIDAD INYECTADA DESPUES DEL FETCH"]

    monkeypatch.setattr(
        "app.core.entity_ecosystem.get_ecosystem_entities", _ecosystem,
    )
    data_query = _make_query(total=30)
    seen_soql: list[str] = []

    def query(ds: str, soql: str) -> list[dict]:
        seen_soql.append(soql)
        return data_query(ds, soql)

    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: 30
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")

    chat = "e8b_post_query_params"
    wf.run_query(
        "procesos de mantenimiento en bogota de 2026",
        channel="streamlit", chat_id=chat,
    )
    # D elimina esa expansión: ni el primer turno ni la página siguiente
    # pueden inventar compradores a partir de la geografía consultada.
    assert not expansions, "se inyectaron compradores después del fetch"
    # INV-PURE-ORDERING-PRESERVES-SOURCE-UNIVERSE: y no entra al historial.
    # Antes sí entraba, y el siguiente follow-up la heredaba como si fuera un
    # filtro que el usuario hubiera pedido.
    persisted = wf.conv_store.get_history(chat, last_n=1)[0].parsed_params
    assert "entidad_ecosystem" not in persisted

    page_2 = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)

    assert page_2["route_reason"] == "pagination_more"
    assert len(seen_soql) == 1  # pagina 2 sale del buffer, sin repesca
    assert page_2["pagination_buffer_hit"] is True
    assert "ENTIDAD INYECTADA" not in seen_soql[0]
    assert not expansions


def test_refill_deduplicates_boundary_row_between_source_batches(tmp_path, monkeypatch):
    """El ultimo id del lote anterior no reaparece si SECOP lo repite al ampliar."""
    source = _rows(74)
    for row in source:
        row["estado_de_apertura_del_proceso"] = "Publicado"

    def query(_ds: str, soql: str) -> list[dict]:
        m_off = re.search(r"OFFSET (\d+)", soql)
        off = int(m_off.group(1)) if m_off else 0
        if off == 0:
            return source[:50]
        if off == 50:
            return [source[49], *source[50:74]]
        return []

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: 75
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")

    chat = "e8b_refill_dedupe"
    pages = [wf.run_query(
        "procesos de aseo de 2026", channel="streamlit", chat_id=chat,
    )]
    pages.append(wf.run_query("muestrame mas", channel="streamlit", chat_id=chat))
    pages.append(wf.run_query("muestrame mas", channel="streamlit", chat_id=chat))
    ids = [
        row["id_del_proceso"]
        for page in pages
        for row in page.get("rows", [])
    ]
    assert ids == [f"PID-{i}" for i in range(74)]
    assert len(ids) == len(set(ids))


def test_buffer_survives_workflow_restart_without_refetch(tmp_path, monkeypatch):
    calls = 0
    data_query = _make_query(total=40)

    def query(ds: str, soql: str) -> list[dict]:
        nonlocal calls
        calls += 1
        return data_query(ds, soql)

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    base = tmp_path / "conv"
    first = SecopalWorkflow(Settings())
    first.secop_client.query = query
    first.secop_client.count = lambda _ds, _soql: 40
    first.narrator.narrate_with_grounding = lambda **kw: None
    first.conv_store = ConversationStore(base_path=base)
    first.run_query("procesos de aseo de 2026", channel="streamlit", chat_id="restart")
    assert calls == 1

    restarted = SecopalWorkflow(Settings())
    restarted.secop_client.query = query
    restarted.secop_client.count = lambda _ds, _soql: 40
    restarted.narrator.narrate_with_grounding = lambda **kw: None
    restarted.conv_store = ConversationStore(base_path=base)
    page_2 = restarted.run_query("muestrame mas", channel="streamlit", chat_id="restart")

    assert page_2["pagination_buffer_hit"] is True
    assert calls == 1


def test_expired_buffer_refetches_from_page_one(tmp_path, monkeypatch):
    from dataclasses import replace
    from datetime import datetime, timedelta, timezone

    from app.core.workflow.handlers.pagination import _page_fingerprint

    calls = 0
    data_query = _make_query(total=40)

    def query(ds: str, soql: str) -> list[dict]:
        nonlocal calls
        calls += 1
        return data_query(ds, soql)

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: 40
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    first = wf.run_query("procesos de aseo de 2026", channel="streamlit", chat_id="expired")
    params = first["resolved_params"]
    fp = _page_fingerprint(params)
    buffer = wf.conv_store.get_pagination_buffer("expired", "procesos", fp)
    expired = replace(
        buffer,
        expires_at_utc=(datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
    )
    wf.conv_store.save_pagination_buffer("expired", expired)

    restarted = wf.run_query("muestrame mas", channel="streamlit", chat_id="expired")
    assert restarted["pagination_buffer_hit"] is False
    assert calls == 2
    assert _shown_indices(restarted) == _shown_indices(first)


def test_approximate_source_total_does_not_stop_refill(tmp_path, monkeypatch):
    """Un count fallido produce X+; no significa que la primera ventana sea todo."""
    calls = 0
    data_query = _make_query(total=120)

    def query(ds: str, soql: str) -> list[dict]:
        nonlocal calls
        calls += 1
        return data_query(ds, soql)

    monkeypatch.setattr(
        "app.core.workflow.actions.execution.observe_universe_fn",
        lambda *a, **k: None,
    )
    wf = SecopalWorkflow(Settings())
    wf.secop_client.query = query
    wf.secop_client.count = lambda _ds, _soql: (_ for _ in ()).throw(TimeoutError("count"))
    wf.narrator.narrate_with_grounding = lambda **kw: None
    wf.conv_store = ConversationStore(base_path=tmp_path / "conv")
    chat = "e8b_approx_total"

    first = wf.run_query("procesos de aseo de 2026", channel="streamlit", chat_id=chat)
    second = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)
    third = wf.run_query("muestrame mas", channel="streamlit", chat_id=chat)

    assert first["source_total_approximate"] is True
    assert second["pagination_buffer_hit"] is True and calls == 2
    assert _shown_indices(first) + _shown_indices(second) + _shown_indices(third) == list(range(75))


# ── Compatibilidad de consumidores legados de rank_and_dedupe ────────────────

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
    """El helper legado conserva orden determinista para sus otros consumidores."""
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
