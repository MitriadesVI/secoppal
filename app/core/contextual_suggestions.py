"""contextual_suggestions.py — Sugerencias contextuales + detección de duplicados/pruebas.

Feature: Barra de refinamiento contextual calculada del result set actual.
Cuando total_count > 20, muestra filtros aplicables (estado, fecha, monto, entidad).
Cada clic refina inmediatamente sin nueva query natural.
"""
from __future__ import annotations

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
        thresholds = [500_000_000, 1_000_000_000, 5_000_000_000]
        monto_options = []
        for thr in thresholds:
            count = sum(
                1 for r in results
                if (r.get(value_field) or 0) >= thr
            )
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
    """Formato compacto: 500000000 → '500M', 1000000000 → '1B'."""
    if value >= 1_000_000_000:
        b = value / 1_000_000_000
        return f"{b:.0f}B" if b == int(b) else f"{b:.1f}B"
    m = value / 1_000_000
    return f"{m:.0f}M"
