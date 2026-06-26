"""Catálogo de frases contractuales y detección en la consulta (Commit 3).

`detect_phrases()` reconoce colocaciones de dominio (`PHRASE_CATALOG`) en la
consulta del usuario ANTES del scrub de tokens, para que la unidad de significado
(p.ej. "manual de contratación") sobreviva intacta en vez de partirse en
"manuales" + "de"(descartado) + "contratación".

Es una CAPACIDAD GENERAL, no un parche: el matcher (`_match_spec`) funciona para
cualquier `PhraseSpec`; ampliar la cobertura = añadir una entrada de datos al
catálogo (idealmente tras una sonda de sinónimos, ver
tests/fixtures/phrase_retrieval), no escribir código nuevo.

Alcance Commit 3: solo detección en la consulta → `objeto_phrases: list[str]`
(JSON-safe). NO genera SoQL, NO valida filas de SECOP, NO rankea. La detección
sobre FILAS (validador tolerante) es Commit 4.
"""
from __future__ import annotations

from app.core.phrase_match import find_subseq, locate_anchors
from app.core.phrase_spec import PhraseSpec
from app.core.retrieval_text import normalize_retrieval_text

# Catálogo de dominio. Formas ya normalizadas (minúsculas, sin tildes).
PHRASE_CATALOG: tuple[PhraseSpec, ...] = (
    PhraseSpec(
        canonical="manual de contratacion",
        ordered_anchors=("manual", "contratacion"),
        variants=("manuales de contratacion",),
        max_gap_tokens=2,
    ),
)


_CANONICAL_TO_SPEC: dict[str, PhraseSpec] = {s.canonical: s for s in PHRASE_CATALOG}


def spec_for_canonical(canonical: str) -> PhraseSpec | None:
    """Resuelve un canonical de `objeto_phrases` a su PhraseSpec (anclas/variantes)."""
    return _CANONICAL_TO_SPEC.get(canonical)


def _match_spec(words: list[str], spec: PhraseSpec) -> tuple[int, int] | None:
    """Span (inicio, fin) del primer match de `spec` en `words`, o None.

    `max_gap_tokens` = nº máximo de tokens intermedios entre anclas vecinas tras
    normalizar. "manual de contratacion" → gap 1 ('de'); "manual interno de
    contratacion" → gap 2 ('interno de').
    """
    for variant in spec.variants:
        span = find_subseq(words, variant.split())
        if span:
            return span
    loc = locate_anchors(words, list(spec.ordered_anchors))
    if loc and loc[2] <= spec.max_gap_tokens:
        return (loc[0], loc[1])
    return None


def detect_phrases(text: str) -> tuple[list[str], str]:
    """Detecta frases del catálogo en `text`.

    Devuelve (canonicals_detectados, texto_restante) donde los tokens de cada
    frase reconocida se han consumido del texto para no recrear el objeto roto.
    El orden de los canonicals es estable; sin duplicados.
    """
    words = normalize_retrieval_text(text).split()
    consumed = [False] * len(words)
    detected: list[str] = []
    for spec in PHRASE_CATALOG:
        span = _match_spec(words, spec)
        if span and spec.canonical not in detected:
            detected.append(spec.canonical)
            for j in range(span[0], span[1] + 1):
                consumed[j] = True
    remaining = " ".join(w for j, w in enumerate(words) if not consumed[j])
    return detected, remaining
