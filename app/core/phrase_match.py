"""Primitivos de matching de frases a nivel de tokens (compartido).

Sin estado, sin dominio, sin red. Lo usan `phrase_catalog` (detección de frases
en la consulta) y `phrase_validator` (validación de frases sobre filas de SECOP),
para no duplicar la lógica de anclas/gap. La normalización vive aparte en
`retrieval_text.normalize_retrieval_text`; este módulo opera sobre tokens ya
normalizados.
"""
from __future__ import annotations


def find_subseq(words: list[str], seq: list[str]) -> tuple[int, int] | None:
    """Primera ocurrencia de `seq` como tokens consecutivos; (inicio, fin) inclusive."""
    n, m = len(words), len(seq)
    if m == 0:
        return None
    for i in range(n - m + 1):
        if words[i : i + m] == seq:
            return (i, i + m - 1)
    return None


def locate_anchors(words: list[str], anchors: list[str]) -> tuple[int, int, int] | None:
    """Anclas en ORDEN; devuelve (inicio, fin, max_gap) del match más ajustado.

    `max_gap` = mayor nº de tokens entre dos anclas consecutivas. Greedy por
    cada posición de la primera ancla (siguiente ancla = ocurrencia más cercana),
    y se queda con el match de menor `max_gap`. None si falta una ancla o el
    orden no se cumple. NO impone límite de gap: quien llama decide el umbral.
    """
    if not anchors:
        return None
    n = len(words)
    best: tuple[int, int, int] | None = None
    for start in range(n):
        if words[start] != anchors[0]:
            continue
        pos, max_gap, ok = start, 0, True
        for anchor in anchors[1:]:
            nxt = next((j for j in range(pos + 1, n) if words[j] == anchor), -1)
            if nxt < 0:
                ok = False
                break
            max_gap = max(max_gap, nxt - pos - 1)
            pos = nxt
        if ok and (best is None or max_gap < best[2]):
            best = (start, pos, max_gap)
    return best
