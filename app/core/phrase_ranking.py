"""Ranking de candidatos por validación de frase (Commit 6B).

Toma las filas candidatas (traídas por el WHERE recall-first), valida cada una
con `phrase_validator`, descarta las `none` y ordena `direct > probable > weak`
con desempate por el ORDEN ORIGINAL (estable y reproducible).

Invariante: direct + probable + weak + none == len(rows).
No consulta SECOP, no genera SoQL, no formatea: solo juicio + orden.
"""
from __future__ import annotations

from collections.abc import Callable

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
