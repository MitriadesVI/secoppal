"""Validador puro de frases sobre una fila de SECOP (Commit 4).

Responde UNA pregunta: ¿esta fila contiene la unidad de significado del
`PhraseSpec`, tras normalizar el texto? Clasifica la fuerza del match.

NO genera SoQL, NO consulta SECOP, NO pagina, NO aplica sinónimos, NO resuelve
polisemia, NO ordena, NO interpreta la acción (elaboración/actualización/ajuste).
La acción y la frase son señales distintas; este módulo valida solo la frase.

Reutiliza `retrieval_text.normalize_retrieval_text` (única normalización) y los
primitivos de `phrase_match` (mismos que la detección en consulta).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.core.phrase_match import find_subseq, locate_anchors
from app.core.phrase_spec import PhraseSpec
from app.core.retrieval_text import normalize_retrieval_text

# Banda estrecha de "probable" por encima del gap directo. Conservadora a
# propósito: el development fixture debe demostrar qué patrón real la necesita.
_PROBABLE_EXTRA_GAP = 2

Level = Literal["direct", "probable", "weak", "none"]
Field = Literal["objeto", "descripcion", "none"]
_RANK = {"direct": 0, "probable": 1, "weak": 2, "none": 3}


@dataclass(frozen=True)
class PhraseMatch:
    level: Level
    field: Field
    matched_text: str | None = None
    matched_variant: str | None = None
    gap_tokens: int | None = None


def _evaluate_field(words: list[str], spec: PhraseSpec) -> tuple[Level, str | None, str | None, int | None]:
    # 1) variante/canónica literal (incl. plural) → direct
    for variant in (spec.canonical, *spec.variants):
        span = find_subseq(words, variant.split())
        if span:
            return ("direct", " ".join(words[span[0] : span[1] + 1]), variant, None)
    # 2) anclas en orden, clasificadas por gap (mismo campo garantizado por construcción)
    loc = locate_anchors(words, list(spec.ordered_anchors))
    if loc is None:
        return ("none", None, None, None)
    start, end, gap = loc
    text = " ".join(words[start : end + 1])
    if gap <= spec.max_gap_tokens:
        level: Level = "direct"
    elif gap <= spec.max_gap_tokens + _PROBABLE_EXTRA_GAP:
        level = "probable"
    else:
        level = "weak"
    return (level, text, None, gap)


def validate_phrase(spec: PhraseSpec, objeto: str | None, descripcion: str | None) -> PhraseMatch:
    """Valida `spec` contra los dos campos de una fila; devuelve el mejor match.

    Gate de mismo-campo: cada campo se evalúa sobre sus propios tokens, así que
    una ancla en `objeto` y otra en `descripcion` NUNCA forman una frase.
    Prioridad: nivel (direct>probable>weak) y, a igualdad, objeto sobre descripcion.
    """
    candidates: list[PhraseMatch] = []
    for field, text in (("objeto", objeto), ("descripcion", descripcion)):
        words = normalize_retrieval_text(text).split()
        level, mtext, mvar, gap = _evaluate_field(words, spec)
        if level != "none":
            candidates.append(PhraseMatch(level, field, mtext, mvar, gap))  # type: ignore[arg-type]
    if not candidates:
        return PhraseMatch("none", "none")
    candidates.sort(key=lambda m: (_RANK[m.level], 0 if m.field == "objeto" else 1))
    return candidates[0]
