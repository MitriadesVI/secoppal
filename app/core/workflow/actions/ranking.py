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

from app.core.phrase_ranking import rank_candidate_pool
from app.core.workflow.helpers import _with_timing


@action(reads=["results", "resolved_params", "dataset_id", "timings_ms"], writes=["ranked_results", "timings_ms"])
def rank_results(state: State) -> State:
    """Rankea el dataset PRIMARIO con `rank_candidate_pool` (mismo algoritmo que
    usa `execute_dataset_child` para el secundario en dual-dataset)."""
    start = time.perf_counter()
    rr = rank_candidate_pool(
        state.get("results") or [],
        state.get("resolved_params", {}) or {},
        state.get("dataset_id", ""),
    )
    return _with_timing(
        state, "rank_ms", start,
        ranked_results=rr.ranked_results,
        phrase_counts=rr.phrase_counts,
        unvalidated_candidates=rr.unvalidated_candidates,
    )
