"""Contract count and reported values from one server-side aggregate."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import math
from typing import Any

from app.core.entity_profile.aspects.common import as_float, build_where
from app.core.entity_profile.models import CONTRACT_DATASET


@dataclass(frozen=True, slots=True)
class ContractValues:
    contract_count: int
    total_contract_value: float
    average_contract_value: float | None


def build_contract_values_query(builder: Any, params: dict) -> str:
    spec, where = build_where(builder, CONTRACT_DATASET, params)
    value = spec.value
    return (
        f"SELECT count(*) AS contract_count, sum({value}) AS total_contract_value, "
        f"avg({value}) AS average_contract_value WHERE {where}"
    )


def parse_contract_values(rows: list[dict]) -> ContractValues | None:
    """Only an explicit, valid count can establish an empty contract universe."""
    if len(rows) != 1 or not isinstance(rows[0], dict):
        return None
    row = rows[0]
    try:
        count = Decimal(str(row.get("contract_count")))
    except InvalidOperation:
        return None
    if not count.is_finite() or count < 0 or count != count.to_integral_value():
        return None
    total = as_float(row.get("total_contract_value"))
    if total is None and count == 0 and row.get("total_contract_value") is None:
        total = 0.0  # SUM(NULL) over the explicitly confirmed empty universe.
    if total is None or not math.isfinite(total) or (count == 0 and total != 0):
        return None
    average = as_float(row.get("average_contract_value"))
    if average is not None and not math.isfinite(average):
        return None
    return ContractValues(
        contract_count=int(count),
        total_contract_value=total,
        average_contract_value=average,
    )
