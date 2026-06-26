"""Ranking de candidatos por validación de frase (Commit 6B).

Toma las filas candidatas (traídas por el WHERE recall-first), valida cada una
con `phrase_validator`, descarta las `none` y ordena `direct > probable > weak`
con desempate por el ORDEN ORIGINAL (estable y reproducible).

Invariante: direct + probable + weak + none == len(rows).
No consulta SECOP, no genera SoQL, no formatea: solo juicio + orden.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from app.core.phrase_spec import PhraseSpec
from app.core.phrase_validator import validate_phrase
from app.core.soql_builder import SoQLBuilder

_RANK = {"direct": 0, "probable": 1, "weak": 2, "none": 3}


def rank_by_phrase(
    rows: list[dict],
    specs: list[PhraseSpec],
    dataset_id: str,
    score_of: Callable[[dict], float] | None = None,
) -> tuple[list[dict], dict[str, int], list[dict]]:
    """Devuelve (validadas_ordenadas, conteos_por_nivel, no_validadas).

    `none` NO se borra: se separa como `no_validadas` (cuarentena) para que un
    falso negativo del validador no desaparezca del sistema. Orden:
        (nivel direct>probable>weak, -bm25, posición original)
    BM25 (`score_of`) solo desempata DENTRO de cada nivel — nunca promueve un
    `weak` sobre un `direct`. Sin `score_of`, el desempate es el orden original.
    Invariante: direct + probable + weak + none == len(rows).
    """
    spec = SoQLBuilder.SPECS.get(dataset_id)
    obj_field = spec.object_name if spec else "objeto_del_contrato"
    desc_field = spec.description if spec else "descripcion_del_proceso"

    counts = {"direct": 0, "probable": 0, "weak": 0, "none": 0}
    kept: list[tuple[int, float, int, dict]] = []
    unvalidated: list[dict] = []
    for idx, row in enumerate(rows):
        best = "none"
        for ps in specs:
            level = validate_phrase(ps, row.get(obj_field), row.get(desc_field or "")).level
            if _RANK[level] < _RANK[best]:
                best = level
        counts[best] += 1
        if best == "none":
            unvalidated.append(row)
        else:
            score = score_of(row) if score_of else 0.0
            kept.append((_RANK[best], -score, idx, row))

    kept.sort(key=lambda x: (x[0], x[1], x[2]))
    return [row for *_, row in kept], counts, unvalidated


# ── Núcleo de ranking reutilizable (Commit 6D extraído en dual-dataset C2) ───
# UN solo algoritmo, dos consumidores: la acción `rank_results` (dataset primario
# en el grafo) y `execute_dataset_child` (dataset secundario en dual-dataset).


@dataclass(frozen=True)
class RankingResult:
    ranked_results: list[dict]
    unvalidated_candidates: list[dict]
    phrase_counts: dict | None
    ranking_trace: dict


def _objeto_flat(params: dict) -> list[str]:
    flat: list[str] = []
    for term in params.get("objeto", []) or []:
        flat.extend(term if isinstance(term, list) else [term])
    return [str(t) for t in flat]


def _bm25_scores(params: dict, rows: list[dict], dataset_id: str) -> dict[int, float]:
    """BM25 por fila (id(row)→score). Vacío si <3 términos o si falla → orden original."""
    flat = _objeto_flat(params)
    if len(flat) < 3:
        return {}
    from app.core.relevance import RelevanceScorer

    field = "objeto_del_contrato" if dataset_id == "jbjy-vk9h" else "nombre_del_procedimiento"
    try:
        scored = RelevanceScorer().score_rows(flat, rows, field=field)
        return {id(row): s.bm25_score for row, s in scored}
    except Exception:
        return {}


def rank_candidate_pool(rows: list[dict], params: dict, dataset_id: str) -> RankingResult:
    """Valida + rankea un pool de candidatos. Composición:
    con frase → nivel(direct>probable>weak) > BM25 dentro del nivel > orden original,
    `none` en cuarentena; sin frase, ≥3 términos → BM25; si no, orden original.
    """
    if not rows:
        return RankingResult([], [], None, {"strategy": "empty", "n_in": 0, "n_ranked": 0})

    scores = _bm25_scores(params, rows, dataset_id)

    def score_of(row: dict) -> float:
        return scores.get(id(row), 0.0)

    objeto_phrases = params.get("objeto_phrases") or []
    if objeto_phrases:
        from app.core.phrase_catalog import spec_for_canonical

        specs = [s for s in (spec_for_canonical(str(c)) for c in objeto_phrases) if s]
        if specs:
            ranked, counts, unvalidated = rank_by_phrase(rows, specs, dataset_id, score_of=score_of)
            visible = ranked if ranked else rows  # seguridad: todo none → mostrar pool
            return RankingResult(visible, unvalidated, counts,
                                 {"strategy": "phrase_tier_then_bm25", "n_in": len(rows), "n_ranked": len(visible)})

    ranked = sorted(rows, key=score_of, reverse=True) if scores else rows
    return RankingResult(ranked, [], None,
                         {"strategy": "bm25" if scores else "original", "n_in": len(rows), "n_ranked": len(ranked)})
