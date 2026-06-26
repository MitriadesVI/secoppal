"""Ranking action — el punto FORMAL donde se decide el orden visible (Commit 6D).

Corre entre `suggester_action` y `format_response`, así `format_response` solo
presenta `ranked_results` y corta a 25. Repara RELEVANCE-RANKING-AFTER-FORMAT-001:
antes el BM25 reordenaba `results` DESPUÉS de que el formatter ya había congelado
las filas, por lo que nunca cambiaba lo que veía el usuario.

Composición del orden:
  - Con `objeto_phrases`:  nivel (direct>probable>weak) → BM25 dentro del nivel
                           → orden original. `none` → cuarentena.
  - Sin frase, objeto≥3 términos: BM25 → orden original (desempate estable).
  - Si no hay señal suficiente: se conserva el orden original.

NO consulta SECOP, NO genera SoQL, NO formatea. No sobrescribe `results` (crudo):
escribe `ranked_results` aparte.
"""
from __future__ import annotations

import time

from burr.core import State, action

from app.core.relevance import RelevanceScorer
from app.core.workflow.helpers import _with_timing


def _objeto_flat(resolved: dict) -> list[str]:
    flat: list[str] = []
    for term in resolved.get("objeto", []) or []:
        flat.extend(term if isinstance(term, list) else [term])
    return [str(t) for t in flat]


def _bm25_scores(resolved: dict, rows: list[dict], dataset_id: str) -> dict[int, float]:
    """BM25 por fila (mapa id(row)→score). Vacío si <3 términos (igual que el guard
    histórico) o si el scorer falla — en ese caso el orden original se conserva."""
    flat = _objeto_flat(resolved)
    if len(flat) < 3:
        return {}
    field = "objeto_del_contrato" if dataset_id == "jbjy-vk9h" else "nombre_del_procedimiento"
    try:
        scored = RelevanceScorer().score_rows(flat, rows, field=field)
        return {id(row): s.bm25_score for row, s in scored}
    except Exception:
        return {}


@action(reads=["results", "resolved_params", "dataset_id", "timings_ms"], writes=["ranked_results", "timings_ms"])
def rank_results(state: State) -> State:
    start = time.perf_counter()
    results = state.get("results") or []
    resolved = state.get("resolved_params", {}) or {}
    dataset_id = state.get("dataset_id", "")

    if not results:
        return _with_timing(state, "rank_ms", start, ranked_results=results)

    scores = _bm25_scores(resolved, results, dataset_id)
    def score_of(row: dict) -> float:
        return scores.get(id(row), 0.0)

    objeto_phrases = resolved.get("objeto_phrases") or []
    if objeto_phrases:
        from app.core.phrase_catalog import spec_for_canonical
        from app.core.phrase_ranking import rank_by_phrase

        specs = [s for s in (spec_for_canonical(str(c)) for c in objeto_phrases) if s]
        if specs:
            ranked, counts, unvalidated = rank_by_phrase(results, specs, dataset_id, score_of=score_of)
            # regla de seguridad: si TODO quedó none, no destruir recall — mostrar el pool.
            visible = ranked if ranked else results
            return _with_timing(
                state, "rank_ms", start,
                ranked_results=visible, phrase_counts=counts, unvalidated_candidates=unvalidated,
            )

    # Sin frase: BM25 (estable; con <3 términos scores={} → orden original intacto).
    ranked = sorted(results, key=score_of, reverse=True) if scores else results
    return _with_timing(state, "rank_ms", start, ranked_results=ranked)
