"""Small parsing and query helpers shared by Entity Profile aspects."""
from __future__ import annotations

from typing import Any


def build_where(builder: Any, dataset_id: str, params: dict) -> tuple[Any, str]:
    spec = builder.SPECS[dataset_id]
    where = builder._build_where(spec, params)
    if not where.strip() or "1=1" in where:
        raise ValueError("Entity Profile refuses an unscoped SECOP query")
    return spec, where


def as_int(value: object) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def as_float(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def iso_date(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value[:10]


def scalar_url(value: object) -> str | None:
    if isinstance(value, dict):
        value = value.get("url")
    return value if isinstance(value, str) and value else None
