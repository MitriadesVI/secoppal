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


def _find_subseq(words: list[str], seq: list[str]) -> tuple[int, int] | None:
    """Primera ocurrencia de `seq` como tokens consecutivos; (inicio, fin) inclusive."""
    n, m = len(words), len(seq)
    for i in range(n - m + 1):
        if words[i : i + m] == seq:
            return (i, i + m - 1)
    return None


def _find_anchors(words: list[str], anchors: tuple[str, ...], max_gap: int) -> tuple[int, int] | None:
    """Anclas en orden, con hasta `max_gap` tokens entre cada par consecutivo.

    `max_gap_tokens` = nº máximo de tokens intermedios entre anclas vecinas tras
    normalizar. "manual de contratacion" → gap 1 ('de'); "manual interno de
    contratacion" → gap 2 ('interno de').
    """
    n = len(words)
    for start in range(n):
        if words[start] != anchors[0]:
            continue
        pos, ok = start, True
        for anchor in anchors[1:]:
            nxt = -1
            for j in range(pos + 1, min(pos + 2 + max_gap, n)):
                if words[j] == anchor:
                    nxt = j
                    break
            if nxt < 0:
                ok = False
                break
            pos = nxt
        if ok:
            return (start, pos)
    return None


def _match_spec(words: list[str], spec: PhraseSpec) -> tuple[int, int] | None:
    """Span (inicio, fin) del primer match de `spec` en `words`, o None."""
    for variant in spec.variants:
        span = _find_subseq(words, variant.split())
        if span:
            return span
    return _find_anchors(words, spec.ordered_anchors, spec.max_gap_tokens)


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
