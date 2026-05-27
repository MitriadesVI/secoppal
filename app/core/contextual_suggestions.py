"""contextual_suggestions.py — Sugerencias contextuales + detección de duplicados/pruebas.

Feature: Barra de refinamiento contextual calculada del result set actual.
Cuando total_count > 20, muestra filtros aplicables (estado, fecha, monto, entidad).
Cada clic refina inmediatamente sin nueva query natural.
"""
from __future__ import annotations

import math
import re
from datetime import date, datetime, timedelta
from typing import Any

# ---------------------------------------------------------------------------
# Patrones de detección de duplicados/pruebas (Nivel 1 — conservador)
# ---------------------------------------------------------------------------

_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bcopia\b", re.IGNORECASE), "posible duplicado"),
    (re.compile(r"\bprueba\b", re.IGNORECASE), "posible proceso de prueba"),
    (re.compile(r"\btest\b", re.IGNORECASE), "posible test"),
    (re.compile(r"x{3,}", re.IGNORECASE), "placeholder no rellenado"),
    (re.compile(r"placeholder", re.IGNORECASE), "placeholder no rellenado"),
    (re.compile(r"^[A-Z]+-\s*$", re.IGNORECASE), "referencia incompleta"),
]

# Campos donde buscar (NO descripción larga — muchos falsos positivos)
_SEARCH_FIELDS_PROCESOS = ("nombre_del_procedimiento", "referencia_del_proceso")
_SEARCH_FIELDS_CONTRATOS = ("objeto_del_contrato", "referencia_del_contrato")


def detect_test_or_duplicate(row: dict, dataset_id: str) -> str | None:
    """Detecta patrones de baja calidad en una fila de resultados.

    Busca en nombre + referencia + objeto (NO en descripción larga).
    Retorna la PRIMERA coincidencia encontrada por posición en el texto,
    no por orden de patrón en la lista.
    """
    fields = (
        _SEARCH_FIELDS_PROCESOS if dataset_id == "p6dx-8zbt"
        else _SEARCH_FIELDS_CONTRATOS
    )

    best_match: tuple[int, str] | None = None  # (position, reason)

    for field in fields:
        value = str(row.get(field, "") or "")
        if not value:
            continue
        for pattern, reason in _PATTERNS:
            m = pattern.search(value)
            if m:
                pos = m.start()
                if best_match is None or pos < best_match[0]:
                    best_match = (pos, reason)

    return best_match[1] if best_match else None


# ---------------------------------------------------------------------------
# Filtros contextuales
# ---------------------------------------------------------------------------

def _short(value: str, max_len: int = 25) -> str:
    """Trunca nombres largos para UI."""
    if len(value) <= max_len:
        return value
    return value[:max_len - 2].rstrip() + ".."


def _estado_field_for(dataset_id: str) -> str:
    """Retorna el campo de estado correcto según dataset."""
    return (
        "estado_de_apertura_del_proceso" if dataset_id == "p6dx-8zbt"
        else "estado_contrato"
    )


def _entity_field_for(dataset_id: str) -> str:
    """Retorna el campo de entidad correcto según dataset."""
    return "entidad" if dataset_id == "p6dx-8zbt" else "nombre_entidad"


def compute_contextual_filters(
    results: list[dict],
    dataset_id: str,
    params: dict,
) -> dict:
    """Calcula filtros contextuales basados en el result set actual.

    Args:
        results: Lista de resultados del query actual.
        dataset_id: 'p6dx-8zbt' (procesos) o 'jbjy-vk9h' (contratos).
        params: Parámetros YA resueltos de la query actual.

    Returns:
        Dict con categorías de filtros, cada una con hasta 3 opciones.
        Categorías con <2 opciones distintas no se incluyen.
        No sugiere filtros que el usuario ya aplicó.
    """
    if not results:
        return {}

    estado_field = _estado_field_for(dataset_id)
    entity_field = _entity_field_for(dataset_id)
    date_field = (
        "fecha_de_publicacion_del" if dataset_id == "p6dx-8zbt"
        else "fecha_de_firma"
    )
    value_field = (
        "precio_base" if dataset_id == "p6dx-8zbt"
        else "valor_del_contrato"
    )

    filters: dict[str, list[dict]] = {}

    # ── Estado ───────────────────────────────────────────────────────
    # No sugerir si el usuario ya filtró por estado
    has_estado = bool(
        params.get("estado")
        or params.get("estado_family")
        or params.get("estado_de_apertura_del_proceso")
        or params.get("estado_del_procedimiento")
        or params.get("estado_contrato")
    )

    if not has_estado:
        estado_counts: dict[str, int] = {}
        for r in results:
            val = str(r.get(estado_field, "") or "").strip()
            if val:
                estado_counts[val] = estado_counts.get(val, 0) + 1

        if len(estado_counts) >= 2:
            sorted_estados = sorted(estado_counts.items(), key=lambda x: -x[1])
            filters["estado"] = [
                {"label": f"{k} ({v})", "value": k, "count": v}
                for k, v in sorted_estados[:3]
            ]

    # ── Fecha ─────────────────────────────────────────────────────────
    # No sugerir si el usuario ya filtró por fecha
    has_fecha = bool(params.get("fecha_desde") or params.get("fecha_hasta"))

    if not has_fecha:
        today = date.today()
        day30 = today - timedelta(days=30)
        day90 = today - timedelta(days=90)
        year2026_start = date(2026, 1, 1)

        count_30d = 0
        count_90d = 0
        count_2026 = 0

        for r in results:
            raw = r.get(date_field)
            if not raw:
                continue
            try:
                if isinstance(raw, str):
                    d = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
                else:
                    d = raw
                if d >= day30:
                    count_30d += 1
                if d >= day90:
                    count_90d += 1
                if d >= year2026_start:
                    count_2026 += 1
            except (ValueError, TypeError):
                continue

        fecha_options = []
        if count_30d > 0:
            fecha_options.append({"label": f"Últimos 30d ({count_30d})", "value": "30d", "count": count_30d})
        if count_90d > 0:
            fecha_options.append({"label": f"Últimos 90d ({count_90d})", "value": "90d", "count": count_90d})
        if count_2026 > 0:
            fecha_options.append({"label": f"Solo 2026 ({count_2026})", "value": "2026", "count": count_2026})

        if len(fecha_options) >= 2:
            filters["fecha"] = fecha_options[:3]

    # ── Monto ─────────────────────────────────────────────────────────
    has_valor = bool(
        params.get("valor_min") is not None or params.get("valor_max") is not None
    )

    if not has_valor:
        values = sorted(
            float(r.get(value_field) or 0)
            for r in results
            if (r.get(value_field) or 0) > 0
        )
        thresholds = _adaptive_monto_thresholds(values)
        monto_options = []
        for thr in thresholds:
            count = sum(1 for v in values if v >= thr)
            if count > 0:
                label = f">{_format_cop_compact(thr)} ({count})"
                monto_options.append({"label": label, "value": thr, "count": count})

        if len(monto_options) >= 2:
            filters["monto"] = monto_options[:3]

    # ── Entidad ───────────────────────────────────────────────────────
    # No sugerir si el usuario ya filtró por entidad
    has_entity = bool(
        params.get("entidad_resolved")
        or params.get("entidad_like")
        or params.get("entidad")
        or params.get("entidad_ecosystem")
    )

    if not has_entity:
        entity_counts: dict[str, int] = {}
        for r in results:
            val = str(r.get(entity_field, "") or "").strip()
            if val:
                entity_counts[val] = entity_counts.get(val, 0) + 1

        if len(entity_counts) >= 2:
            sorted_entities = sorted(entity_counts.items(), key=lambda x: -x[1])
            filters["entidad"] = [
                {"label": f"{_short(k)} ({v})", "value": k, "count": v}
                for k, v in sorted_entities[:3]
            ]

    # ── Ciudad ────────────────────────────────────────────────────────
    # Solo si el usuario filtró por departamento pero no ciudad
    has_ciudad = bool(params.get("ciudad"))
    has_depto = bool(params.get("departamento_resolved"))

    if has_depto and not has_ciudad:
        ciudad_field = "ciudad_entidad" if dataset_id == "p6dx-8zbt" else "ciudad"
        ciudad_counts: dict[str, int] = {}
        for r in results:
            val = str(r.get(ciudad_field, "") or "").strip()
            if val:
                ciudad_counts[val] = ciudad_counts.get(val, 0) + 1

        if len(ciudad_counts) >= 2:
            sorted_ciudades = sorted(ciudad_counts.items(), key=lambda x: -x[1])
            filters["ciudad"] = [
                {"label": f"{_short(k)} ({v})", "value": k, "count": v}
                for k, v in sorted_ciudades[:3]
            ]

    return filters


def apply_refinement(params: dict, filter_key: str, filter_value: Any) -> dict:
    """Mergea filtro adicional sin perder contexto previo.

    Args:
        params: Parámetros actuales (ya resueltos).
        filter_key: Categoría del filtro ('estado', 'fecha', 'monto', 'entidad', 'ciudad').
        filter_value: Valor del filtro a aplicar.

    Returns:
        Nuevo dict de params con el filtro mergeado.
    """
    refined = dict(params)

    if filter_key == "estado":
        refined["estado"] = filter_value
    elif filter_key == "fecha":
        today = date.today()
        if filter_value == "30d":
            refined["fecha_desde"] = (today - timedelta(days=30)).isoformat()
        elif filter_value == "90d":
            refined["fecha_desde"] = (today - timedelta(days=90)).isoformat()
        elif filter_value == "2026":
            refined["fecha_desde"] = "2026-01-01"
    elif filter_key == "monto":
        refined["valor_min"] = filter_value
    elif filter_key == "entidad":
        refined["entidad_resolved"] = filter_value
    elif filter_key == "ciudad":
        refined["ciudad"] = filter_value

    return refined


def _format_cop_compact(value: float) -> str:
    """Formato compacto colombiano para chips: 500M, 1MM, 1B.

    Alineado con app.utils.money.format_money:
    - billón (10^12) → B
    - mil millones (10^9) → MM
    - millón (10^6) → M
    """
    if value >= 1_000_000_000_000:
        b = value / 1_000_000_000_000
        return f"{b:.0f}B" if b == int(b) else f"{b:.1f}B"
    if value >= 1_000_000_000:
        mm = value / 1_000_000_000
        return f"{mm:.0f}MM" if mm == int(mm) else f"{mm:.1f}MM"
    m = value / 1_000_000
    return f"{m:.0f}M"


_NICE_STEPS = (1, 2, 3, 5, 7)


def _round_to_nice(value: float) -> float:
    """Redondea HACIA ABAJO a la escala {1, 2, 3, 5, 7} × 10^N.

    Garantiza nice ≤ value (evita thresholds por encima del max).
    La escala fina (5 pasos por década) permite diferenciar percentiles
    aún cuando el rango de valores cae dentro de una sola década.
    """
    if value <= 0:
        return 0.0
    exponent = math.floor(math.log10(value))
    scale = 10 ** exponent
    norm = value / scale  # ∈ [1, 10)
    nice = next((s for s in reversed(_NICE_STEPS) if s <= norm), 1)
    return float(nice * scale)


def _adaptive_monto_thresholds(sorted_values: list[float]) -> list[float]:
    """Devuelve hasta 3 thresholds 'lindos' que parten el result set.

    Usa percentiles p33/p66/p90 de los valores ordenados, los redondea a
    1/2/5×10^N y dedupea. Descarta thresholds que excedan el max real
    (evita chips con count=0 por redondeo hacia arriba). Si <4 valores,
    no devuelve nada — categoría no aplica.
    """
    n = len(sorted_values)
    if n < 4:
        return []
    max_val = sorted_values[-1]
    raw = [
        sorted_values[n // 3],
        sorted_values[(2 * n) // 3],
        sorted_values[min((9 * n) // 10, n - 1)],
    ]
    seen: list[float] = []
    for r in raw:
        nice = _round_to_nice(r)
        if nice > 0 and nice <= max_val and nice not in seen:
            seen.append(nice)
    return sorted(seen)
