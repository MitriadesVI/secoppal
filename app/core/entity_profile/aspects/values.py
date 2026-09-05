"""Contract count and reported values from one server-side aggregate."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.core.entity_profile.aspects.common import as_float, as_int, build_where
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


def parse_contract_values(rows: list[dict]) -> ContractValues:
    row = rows[0] if rows else {}
    return ContractValues(
        contract_count=as_int(row.get("contract_count")),
        total_contract_value=as_float(row.get("total_contract_value")) or 0.0,
        average_contract_value=as_float(row.get("average_contract_value")),
    )
