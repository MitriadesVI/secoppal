"""Commit 6D: etapa formal de ranking ANTES de format_response.

Congela el bug RELEVANCE-RANKING-AFTER-FORMAT-001 (BM25 sí afecta el orden visible)
y la composición nivel→BM25→orden original con cuarentena de none.
"""
from __future__ import annotations

from burr.core import State

from app.core.phrase_catalog import PHRASE_CATALOG
from app.core.phrase_ranking import rank_by_phrase
from app.core.workflow.actions.ranking import rank_results

SPEC = PHRASE_CATALOG[0]
DS = "jbjy-vk9h"


def _row(objeto):
    return {"objeto_del_contrato": objeto, "descripcion_del_proceso": "", "_o": objeto}


def _state(results, objeto=None, objeto_phrases=None):
    rp = {}
    if objeto is not None:
        rp["objeto"] = objeto
    if objeto_phrases is not None:
        rp["objeto_phrases"] = objeto_phrases
    return State({"results": results, "resolved_params": rp, "dataset_id": DS, "timings_ms": {}})


# ── 2. La categoría de frase domina al BM25 ──────────────────────────────────
def test_phrase_level_dominates_bm25():
    direct = _row("manual de contratacion")            # direct, score bajo
    weak = _row("manual de funciones y la contratacion")  # weak, score alto
    score = {id(direct): 0.1, id(weak): 99.0}
    ranked, _, _ = rank_by_phrase([weak, direct], [SPEC], DS, score_of=lambda r: score[id(r)])
    assert ranked[0] is direct  # el direct gana pese a menor BM25


# ── 3. Orden estable con nivel y score iguales ───────────────────────────────
def test_stable_order_within_level():
    a = _row("manual de contratacion uno")
    b = _row("manual de contratacion dos")
    ranked, _, _ = rank_by_phrase([a, b], [SPEC], DS, score_of=lambda r: 5.0)
    assert ranked == [a, b]  # mismo nivel y score → orden original


# ── 4. Cuarentena: sin pérdida ni duplicación ────────────────────────────────
def test_quarantine_no_loss():
    rows = [_row("manual de contratacion"), _row("pavimentacion"), _row("alumbrado")]
    ranked, _, unvalidated = rank_by_phrase(rows, [SPEC], DS)
    assert len(ranked) + len(unvalidated) == len(rows)


# ── 1. El BM25 SÍ modifica el display (bug-freeze) ───────────────────────────
def test_bm25_reorders_without_phrase():
    irrelevant = _row("pavimentacion de vias urbanas")
    relevant = _row("control de calidad de agua para consumo")
    state = _state([irrelevant, relevant], objeto=["control", "calidad", "agua"])
    out = rank_results(state)
    ranked = out.get("ranked_results")
    assert ranked[0] is relevant  # BM25 sube el relevante; antes del 6D no pasaba


# ── 6. Sin señales suficientes: orden original ───────────────────────────────
def test_short_query_keeps_original_order():
    a, b = _row("agua potable"), _row("pavimentacion")
    state = _state([a, b], objeto=["agua"])  # 1 término < 3 → sin BM25
    out = rank_results(state)
    assert out.get("ranked_results") == [a, b]


# ── 5/phrase: la acción enruta por frase y deja ranked_results + cuarentena ──
def test_action_phrase_path_sets_ranked_and_quarantine():
    direct = _row("manual de contratacion")
    noise = _row("pavimentacion")
    state = _state([direct, noise], objeto=[["elaboracion"]], objeto_phrases=["manual de contratacion"])
    out = rank_results(state)
    assert out.get("ranked_results") == [direct]
    assert out.get("unvalidated_candidates") == [noise]
    counts = out.get("phrase_counts")
    assert counts["direct"] == 1 and counts["none"] == 1
