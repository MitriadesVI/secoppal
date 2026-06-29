"""H4 — gate semántico del ORDER BY (presentacional vs semántico).

El fallback de H4 (recuperación degradada) puede **retirar el ORDER BY** de una query
que hizo timeout y reclasificar la página localmente. Eso preserva la intención SOLO
cuando el orden es **presentacional** (un default del sistema, p.ej. "más recientes
primero" en una búsqueda normal). Cuando el usuario pide explícitamente un orden
—"los más caros", "mayor valor", "los últimos", "más recientes"— el orden es
**semántico**: parte sustantiva de la pregunta. Retirarlo y devolver una muestra sin
ordenar produciría una respuesta rápida pero **falsa** ("este es el más caro" sobre 50
filas arbitrarias). En ese caso H4 NO degrada: devuelve inconcluso/parcial y nunca
afirma máximos/mínimos/posiciones.

La señal ya existe: ``QueryRouter`` setea ``ordering_signal`` (``valor_desc`` /
``fecha_desc``) SOLO cuando el usuario pidió un orden explícito. Su ausencia ⟹ el
``ORDER BY`` del SoQL es el default presentacional. El gate se apoya en eso; no inventa
señales nuevas.

Garantía de orden del resultado (``result_order_guarantee``), expuesta al formateo para
que nunca presente un ranking exacto sobre una muestra:

- ``exact_order``      — la query honró el ORDER BY (éxito normal).
- ``unordered_sample`` — el fallback retiró el ORDER BY; la página se ordenó localmente
                         pero NO hay garantía de orden global (no apta para "el máximo").
- ``unavailable``      — orden semántico que no se pudo satisfacer (timeout); sin afirmar
                         ranking.
"""
from __future__ import annotations

ORDER_EXACT = "exact_order"
ORDER_SAMPLE = "unordered_sample"
ORDER_UNAVAILABLE = "unavailable"


def order_is_semantic(params: dict) -> bool:
    """True si el usuario pidió un orden explícito (superlativo/ranking).

    Se apoya en ``ordering_signal`` (lo setea QueryRouter ante "más caros"/"mayor
    valor"→``valor_desc`` y "últimos"/"más recientes"→``fecha_desc``). Su presencia ⟹
    el orden es sustantivo y NO puede retirarse en el fallback.
    """
    return bool((params or {}).get("ordering_signal"))


def order_is_presentational(params: dict) -> bool:
    """True si el ORDER BY es un default del sistema (retirable en el fallback)."""
    return not order_is_semantic(params)


def _coerce_float(v) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return float("-inf")


def sort_page_presentational(rows: list[dict], date_field: str, value_field: str) -> list[dict]:
    """Ordena la PÁGINA recuperada por el default presentacional (fecha DESC, valor DESC).

    Best-effort sobre las filas ya traídas: NO es una garantía de orden global (la query
    se trajo sin ORDER BY), solo deja la muestra presentable. El caller marca
    ``result_order_guarantee = unordered_sample`` para que el formateo no afirme rankings.
    Nunca lanza: valores ausentes/no parseables van al fondo.
    """
    return sorted(
        list(rows or []),
        key=lambda r: (str(r.get(date_field) or ""), _coerce_float(r.get(value_field))),
        reverse=True,
    )
