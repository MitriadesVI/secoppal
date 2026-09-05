"""Bounded yearly process/contract metrics for temporal drill-down."""
from __future__ import annotations

from typing import Any

from app.core.entity_profile.aspects.common import as_float, as_int, build_where
from app.core.entity_profile.models import CONTRACT_DATASET, PeriodMetric


YEAR_LIMIT = 50


def build_temporal_query(builder: Any, dataset_id: str, params: dict) -> str:
    spec, where = build_where(builder, dataset_id, params)
    value = (
        f", sum({spec.value}) AS contract_value"
        if dataset_id == CONTRACT_DATASET
        else ""
    )
    return (
        f"SELECT date_trunc_y({spec.date}) AS period, count(*) AS row_count{value} "
        f"WHERE {where} AND {spec.date} IS NOT NULL "
        "GROUP BY period ORDER BY period ASC "
        f"LIMIT {YEAR_LIMIT}"
    )


def merge_periods(
    process_rows: list[dict] | None,
    contract_rows: list[dict] | None,
) -> tuple[PeriodMetric, ...]:
    periods: dict[str, dict[str, int | float | None]] = {}
    for row in process_rows or ():
        raw = row.get("period")
        if not isinstance(raw, str) or len(raw) < 4:
            continue
        periods.setdefault(raw[:4], {})["process_count"] = as_int(row.get("row_count"))
    for row in contract_rows or ():
        raw = row.get("period")
        if not isinstance(raw, str) or len(raw) < 4:
            continue
        slot = periods.setdefault(raw[:4], {})
        slot["contract_count"] = as_int(row.get("row_count"))
        slot["contract_value"] = as_float(row.get("contract_value")) or 0.0
    return tuple(
        PeriodMetric(
            period=period,
            process_count=_optional_int(values.get("process_count")),
            contract_count=_optional_int(values.get("contract_count")),
            contract_value=_optional_float(values.get("contract_value")),
        )
        for period, values in sorted(periods.items())
    )


def _optional_int(value: object) -> int | None:
    return int(value) if isinstance(value, (int, float)) else None


def _optional_float(value: object) -> float | None:
    return float(value) if isinstance(value, (int, float)) else None
