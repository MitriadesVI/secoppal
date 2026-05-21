"""observer.py — Tarea 2.6: análisis estadístico del universo de resultados.

Produce UniverseInsights: estadísticas crudas + señales interpretadas
pre-calculadas para que el narrator (2.7) genere texto grounded en datos.

El observer es best-effort: cualquier query fallida deja ese campo None.
Si falla el conjunto, observe() retorna None.
"""
from __future__ import annotations

import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeout
from dataclasses import dataclass
from typing import TYPE_CHECKING

from app.utils.logging import get_logger

if TYPE_CHECKING:
    from app.core.secop_client import SecopClient
    from app.core.soql_builder import SoQLBuilder

logger = get_logger(__name__)

# ── Timeouts de agregación (N6, audit3) ──────────────────────────────────
OBSERVER_AGG_TIMEOUT_PER_TASK = 4   # timeout pasado a secop_client.aggregate
OBSERVER_AGG_TIMEOUT_GLOBAL = 5     # timeout de as_completed sobre todas las tareas


@dataclass
class UniverseInsights:
    # Datos crudos
    total_count: int
    sample_size: int
    date_range: tuple[str, str] | None
    top_entities: list[tuple[str, int]]        # [(nombre, count), ...]
    value_stats: dict | None                   # {mean, median, min, max} — median solo en sample
    top_modalities: list[tuple[str, int]]      # [(modalidad, count), ...]
    temporal_distribution: dict[int, int]      # {año: count}
    computed_from: str                         # "sample" | "aggregation"

    # Señales interpretadas (input listo para el narrator)
    has_dominant_entity: bool
    dominant_entity_name: str | None
    dominant_entity_pct: float | None

    has_temporal_concentration: bool
    dominant_year: int | None
    dominant_year_pct: float | None

    has_value_outlier: bool
    outlier_value: float | None

    has_diverse_modalities: bool
    truncation_warning: bool = False
    truncation_ratio: float | None = None


# ---------------------------------------------------------------------------
# Punto de entrada público
# ---------------------------------------------------------------------------

def observe(
    results: list[dict],
    total_count: int,
    dataset_id: str,
    params: dict,
    secop_client: "SecopClient",
    soql_builder: "SoQLBuilder",
) -> "UniverseInsights | None":
    """Retorna UniverseInsights o None si algo crítico falla."""
    try:
        limit = int(params.get("limit", 50) or 50)
        if total_count <= 50:
            return _compute_from_sample(results, total_count, limit=limit)
        else:
            return _compute_from_aggregation(
                total_count, dataset_id, params, secop_client, soql_builder
            )
    except Exception as exc:
        logger.warning("observe() failed — universe_insights=None: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Cómputo desde sample (total_count <= 50)
# ---------------------------------------------------------------------------

def _compute_from_sample(
    results: list[dict], total_count: int, limit: int = 50
) -> UniverseInsights:
    spec_procesos_entity = "entidad"
    # Detectar dataset por keys del primer row
    sample_size = len(results)

    if not results:
        raw = {
            "top_entities": [],
            "value_stats": None,
            "top_modalities": [],
            "date_range": None,
            "temporal_dist": {},
        }
        return _build_insights(raw, total_count, sample_size, "sample", limit=limit)

    # Detectar campo de entidad/fecha/valor/modalidad desde las keys del primer row
    first = results[0]
    entity_field = "nombre_entidad" if "nombre_entidad" in first else "entidad"
    value_field = "valor_del_contrato" if "valor_del_contrato" in first else "precio_base"
    date_field = "fecha_de_firma" if "fecha_de_firma" in first else "fecha_de_publicacion_del"
    modality_field = "modalidad_de_contratacion"

    # top_entities
    from collections import Counter
    entity_counts = Counter(
        r.get(entity_field, "") for r in results if r.get(entity_field)
    )
    top_entities = [(name, cnt) for name, cnt in entity_counts.most_common(5)]

    # value_stats (con mediana — disponible en sample)
    values: list[float] = []
    for r in results:
        try:
            v = r.get(value_field)
            if v is not None:
                values.append(float(v))
        except (TypeError, ValueError):
            pass
    if values:
        value_stats = {
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
        }
    else:
        value_stats = None

    # top_modalities
    modality_counts = Counter(
        r.get(modality_field, "") for r in results if r.get(modality_field)
    )
    top_modalities = [(m, c) for m, c in modality_counts.most_common(3)]

    # date_range + temporal_dist
    dates: list[str] = [r[date_field] for r in results if r.get(date_field)]
    date_range = (min(dates)[:10], max(dates)[:10]) if dates else None

    year_counts: dict[int, int] = {}
    for d in dates:
        try:
            yr = int(d[:4])
            year_counts[yr] = year_counts.get(yr, 0) + 1
        except (ValueError, IndexError):
            pass

    raw = {
        "top_entities": top_entities,
        "value_stats": value_stats,
        "top_modalities": top_modalities,
        "date_range": date_range,
        "temporal_dist": year_counts,
    }
    return _build_insights(raw, total_count, sample_size, "sample", limit=limit)


# ---------------------------------------------------------------------------
# Cómputo desde aggregation (total_count > 50)
# ---------------------------------------------------------------------------

def _compute_from_aggregation(
    total_count: int,
    dataset_id: str,
    params: dict,
    secop_client: "SecopClient",
    soql_builder: "SoQLBuilder",
) -> UniverseInsights:
    # SAFE-SOQL-001: el observer corre DESPUÉS de una consulta tabular ya
    # validada por el guard del orchestrator. Compila estadísticas sobre el
    # universo seleccionado, así que opta por allow_global=True para no
    # rechazarse a sí mismo cuando params son una vista parcial del estado.
    queries = {
        "top_entities":   soql_builder.build_top_entities(dataset_id, params, allow_global=True),
        "value_stats":    soql_builder.build_value_stats(dataset_id, params, allow_global=True),
        "top_modalities": soql_builder.build_top_modalities(dataset_id, params, allow_global=True),
        "date_range":     soql_builder.build_date_range(dataset_id, params, allow_global=True),
        "temporal_dist":  soql_builder.build_temporal_dist(dataset_id, params, allow_global=True),
    }

    agg_results: dict[str, list[dict] | None] = {k: None for k in queries}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                secop_client.aggregate, dataset_id, q, OBSERVER_AGG_TIMEOUT_PER_TASK
            ): name
            for name, q in queries.items()
        }
        try:
            for future in as_completed(futures, timeout=OBSERVER_AGG_TIMEOUT_GLOBAL):
                name = futures[future]
                try:
                    agg_results[name] = future.result()
                except Exception as exc:
                    logger.warning("aggregate %s failed: %s", name, exc)
                    agg_results[name] = None
        except FuturesTimeout:
            logger.warning(
                "observe_universe: global timeout (%ds) — using partial results",
                OBSERVER_AGG_TIMEOUT_GLOBAL,
            )

    # Si todas las queries fallaron, sube la excepción para que observe() retorne None
    if all(v is None for v in agg_results.values()):
        raise RuntimeError("all aggregation queries failed")

    spec = soql_builder.SPECS.get(dataset_id)
    if spec is None:
        raise KeyError(f"dataset_id '{dataset_id}' not found in SoQLBuilder.SPECS")
    raw = _parse_agg_results(agg_results, spec)
    limit = int(params.get("limit", 50) or 50)
    return _build_insights(
        raw, total_count, sample_size=0, computed_from="aggregation", limit=limit
    )


def _parse_agg_results(agg: dict, spec) -> dict:
    """Convierte rows crudos de Socrata en las estructuras esperadas por _build_insights."""
    # top_entities
    top_entities: list[tuple[str, int]] = []
    if agg.get("top_entities"):
        for row in agg["top_entities"]:
            name = row.get(spec.entity, "")
            try:
                cnt = int(row.get("cnt", 0))
            except (TypeError, ValueError):
                cnt = 0
            if name:
                top_entities.append((name, cnt))

    # value_stats — SoQL devuelve avg/min/max; median no disponible en agg
    value_stats = None
    if agg.get("value_stats"):
        row = agg["value_stats"][0] if agg["value_stats"] else {}
        try:
            value_stats = {
                "mean": float(row["mean"]) if row.get("mean") is not None else None,
                "median": None,  # no disponible vía SoQL sin percentile
                "min": float(row["min_val"]) if row.get("min_val") is not None else None,
                "max": float(row["max_val"]) if row.get("max_val") is not None else None,
            }
        except (KeyError, TypeError, ValueError):
            value_stats = None

    # top_modalities
    top_modalities: list[tuple[str, int]] = []
    if agg.get("top_modalities"):
        for row in agg["top_modalities"]:
            mod = row.get(spec.modality, "")
            try:
                cnt = int(row.get("cnt", 0))
            except (TypeError, ValueError):
                cnt = 0
            if mod:
                top_modalities.append((mod, cnt))

    # date_range
    date_range = None
    if agg.get("date_range"):
        row = agg["date_range"][0] if agg["date_range"] else {}
        d_min = row.get("date_min", "")
        d_max = row.get("date_max", "")
        if d_min and d_max:
            date_range = (str(d_min)[:10], str(d_max)[:10])

    # temporal_dist
    temporal_dist: dict[int, int] = {}
    if agg.get("temporal_dist"):
        for row in agg["temporal_dist"]:
            yr_raw = row.get("yr", "")
            try:
                yr = int(str(yr_raw)[:4])
                cnt = int(row.get("cnt", 0))
                temporal_dist[yr] = cnt
            except (TypeError, ValueError):
                pass

    return {
        "top_entities": top_entities,
        "value_stats": value_stats,
        "top_modalities": top_modalities,
        "date_range": date_range,
        "temporal_dist": temporal_dist,
    }


# ---------------------------------------------------------------------------
# Señales interpretadas
# ---------------------------------------------------------------------------

def _derive_signals(raw: dict, total_count: int) -> dict:
    """Computa los 8 campos de señales. Pura, sin side effects."""
    top_entities: list[tuple[str, int]] = raw.get("top_entities", [])
    value_stats: dict | None = raw.get("value_stats")
    top_modalities: list[tuple[str, int]] = raw.get("top_modalities", [])
    temporal_dist: dict[int, int] = raw.get("temporal_dist", {})

    # Entidad dominante
    has_dominant_entity = False
    dominant_entity_name = None
    dominant_entity_pct = None
    if top_entities and total_count > 0:
        top_name, top_cnt = top_entities[0]
        pct = top_cnt / total_count
        if pct > 0.40:
            has_dominant_entity = True
            dominant_entity_name = top_name
            dominant_entity_pct = round(pct * 100, 1)

    # Concentración temporal
    has_temporal_concentration = False
    dominant_year = None
    dominant_year_pct = None
    if temporal_dist and total_count > 0:
        yr, yr_cnt = max(temporal_dist.items(), key=lambda x: x[1])
        pct = yr_cnt / total_count
        if pct > 0.60:
            has_temporal_concentration = True
            dominant_year = yr
            dominant_year_pct = round(pct * 100, 1)

    # Outlier de valor
    has_value_outlier = False
    outlier_value = None
    if value_stats:
        median = value_stats.get("median")
        max_val = value_stats.get("max")
        if median and max_val and median > 0 and max_val > 5 * median:
            has_value_outlier = True
            outlier_value = max_val

    # Diversidad de modalidades
    has_diverse_modalities = False
    if top_modalities and total_count > 0:
        top_mod_cnt = top_modalities[0][1]
        if top_mod_cnt / total_count < 0.50:
            has_diverse_modalities = True

    return {
        "has_dominant_entity": has_dominant_entity,
        "dominant_entity_name": dominant_entity_name,
        "dominant_entity_pct": dominant_entity_pct,
        "has_temporal_concentration": has_temporal_concentration,
        "dominant_year": dominant_year,
        "dominant_year_pct": dominant_year_pct,
        "has_value_outlier": has_value_outlier,
        "outlier_value": outlier_value,
        "has_diverse_modalities": has_diverse_modalities,
    }


# ---------------------------------------------------------------------------
# Constructor de UniverseInsights
# ---------------------------------------------------------------------------

def _build_insights(
    raw: dict,
    total_count: int,
    sample_size: int,
    computed_from: str,
    limit: int = 50,
) -> UniverseInsights:
    signals = _derive_signals(raw, total_count)
    truncation_warning = total_count > limit
    truncation_ratio = (
        limit / total_count if truncation_warning and total_count else None
    )
    return UniverseInsights(
        total_count=total_count,
        sample_size=sample_size,
        date_range=raw.get("date_range"),
        top_entities=raw.get("top_entities", []),
        value_stats=raw.get("value_stats"),
        top_modalities=raw.get("top_modalities", []),
        temporal_distribution=raw.get("temporal_dist", {}),
        computed_from=computed_from,
        truncation_warning=truncation_warning,
        truncation_ratio=truncation_ratio,
        **signals,
    )
