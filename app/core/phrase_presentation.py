"""Encabezado y trace honestos para resultados de frase (Commit 6C-UX).

DETERMINÍSTICO — no pasa por el narrator/LLM, para que nadie convierta
"validé 184 entre 200 analizados" en "encontré 184 en todo SECOP". Distingue:

    secop_total_count = universo que SECOP dice que coincide con el WHERE
    candidate_count   = filas efectivamente descargadas para evaluar
    validated_count   = coincidencias confirmadas (direct+probable+weak)
    displayed_count   = tarjetas mostradas en esta página

Funciones puras: sin estado, sin red.
"""
from __future__ import annotations

_DISPLAY_LIMIT = 25


def _validated(counts: dict[str, int]) -> int:
    return counts.get("direct", 0) + counts.get("probable", 0) + counts.get("weak", 0)


def build_phrase_header(
    counts: dict[str, int], candidate_count: int, candidate_pool_full: bool,
    secop_total_count: int, displayed_count: int,
) -> str:
    """Header honesto según si el pool se llenó y si SECOP reporta más."""
    validated = _validated(counts)
    if not candidate_pool_full:
        # se evaluó (aparentemente) todo el universo recuperable
        return (f"Encontré {validated} coincidencias pertinentes. "
                f"Te muestro las {displayed_count} mejor clasificadas.")
    if secop_total_count and secop_total_count > candidate_count:
        return (f"SECOP reporta {secop_total_count:,} candidatos textuales. Analicé los "
                f"primeros {candidate_count} y validé {validated} coincidencias. "
                f"Te muestro las {displayed_count} mejor clasificadas.")
    return (f"Validé {validated} coincidencias entre los primeros {candidate_count} "
            f"candidatos. Te muestro las {displayed_count} mejor clasificadas.")


def build_phrase_trace(
    counts: dict[str, int], candidate_count: int, candidate_pool_full: bool,
    secop_total_count: int, total_count_approximate: bool, displayed_count: int,
) -> dict:
    """Trace técnico completo. Invariantes:
    validated = direct+probable+weak; candidate = validated+unvalidated;
    displayed <= validated.
    """
    return {
        "secop_total_count": secop_total_count,
        "total_count_approximate": total_count_approximate,
        "candidate_count": candidate_count,
        "candidate_pool_full": candidate_pool_full,
        "direct_count": counts.get("direct", 0),
        "probable_count": counts.get("probable", 0),
        "weak_count": counts.get("weak", 0),
        "validated_count": _validated(counts),
        "unvalidated_count": counts.get("none", 0),
        "display_limit": _DISPLAY_LIMIT,
        "displayed_count": displayed_count,
        "ranking_applied": True,
        "ranking_strategy": "phrase_tier_then_bm25",
    }
