"""Bounded aggregate execution and exact reconciliation."""
from __future__ import annotations

import re
from typing import Any

from .models import (
    CONTRACTS_DATASET,
    ContractActivityGroup,
    ContractActivityPlan,
    ContractActivityResult,
)


NULL_GROUP = "Sin dato reportado"
BLANK_GROUP = "Valor en blanco"


class ContractActivityUnavailable(RuntimeError):
    """The source evidence cannot authorize publication."""


def execute_plan(plan: ContractActivityPlan, secop_client: Any) -> ContractActivityResult:
    witness = _aggregate(secop_client, plan.identity_sql)
    if witness:
        raise ContractActivityUnavailable("SOURCE_GRAIN_UNACCREDITED")

    scalar_rows = _aggregate(secop_client, plan.scalar_sql)
    scalar = _parse_scalar(scalar_rows, grouped=plan.groups_sql is not None)
    row_count, id_count, total, group_non_null, distinct_groups = scalar
    if row_count != id_count or id_count != total:
        raise ContractActivityUnavailable("SCALAR_IDENTITY_MISMATCH")
    if plan.groups_sql is None:
        return ContractActivityResult(plan.request.operation, total)

    group_rows = _aggregate(secop_client, plan.groups_sql)
    groups = _parse_groups(
        group_rows,
        row_count=row_count,
        id_count=id_count,
        total=total,
        group_non_null=group_non_null,
        distinct_groups=distinct_groups,
    )
    return ContractActivityResult(plan.request.operation, total, groups)


def _aggregate(secop_client: Any, sql: str) -> list[dict]:
    try:
        rows = secop_client.with_timeout(10).aggregate(CONTRACTS_DATASET, sql)
    except Exception as exc:
        raise ContractActivityUnavailable("SOURCE_CALL_UNAVAILABLE") from exc
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ContractActivityUnavailable("MALFORMED_SOURCE_ROWS")
    return rows


def _parse_scalar(
    rows: list[dict], *, grouped: bool,
) -> tuple[int, int, int, int | None, int | None]:
    if len(rows) != 1:
        raise ContractActivityUnavailable("SCALAR_ROW_SHAPE_INVALID")
    required = {"row_count", "id_count", "distinct_contract_count"}
    if grouped:
        required |= {"group_non_null_count", "distinct_group_count"}
    if set(rows[0]) != required:
        raise ContractActivityUnavailable("SCALAR_COLUMNS_INVALID")
    row_count = _strict_int(rows[0]["row_count"])
    id_count = _strict_int(rows[0]["id_count"])
    total = _strict_int(rows[0]["distinct_contract_count"])
    group_non_null = _strict_int(rows[0]["group_non_null_count"]) if grouped else None
    distinct_groups = _strict_int(rows[0]["distinct_group_count"]) if grouped else None
    return row_count, id_count, total, group_non_null, distinct_groups


def _parse_groups(
    rows: list[dict], *, row_count: int, id_count: int, total: int,
    group_non_null: int | None, distinct_groups: int | None,
) -> tuple[ContractActivityGroup, ...]:
    required = {"row_count", "id_count", "distinct_contract_count"}
    parsed: list[tuple[object, str, int, int, int]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        if set(row) not in (required, required | {"group_label"}):
            raise ContractActivityUnavailable("GROUP_COLUMNS_INVALID")
        raw_label = row.get("group_label")
        label, key = _group_label(raw_label, "group_label" in row)
        if key in seen:
            raise ContractActivityUnavailable("DUPLICATE_GROUP_BUCKET")
        seen.add(key)
        parsed.append((
            raw_label,
            label,
            _strict_int(row["row_count"]),
            _strict_int(row["id_count"]),
            _strict_int(row["distinct_contract_count"]),
        ))
    if any(rows_ != ids or ids != distinct for _, _, rows_, ids, distinct in parsed):
        raise ContractActivityUnavailable("GROUP_IDENTITY_MISMATCH")
    if sum(item[2] for item in parsed) != row_count:
        raise ContractActivityUnavailable("GROUP_ROW_RECONCILIATION_FAILED")
    if sum(item[3] for item in parsed) != id_count:
        raise ContractActivityUnavailable("GROUP_ID_RECONCILIATION_FAILED")
    if sum(item[4] for item in parsed) != total:
        raise ContractActivityUnavailable("GROUP_TOTAL_RECONCILIATION_FAILED")
    non_null_count = sum(item[2] for item in parsed if item[0] is not None)
    non_null_groups = sum(1 for item in parsed if item[0] is not None)
    if group_non_null != non_null_count or distinct_groups != non_null_groups:
        raise ContractActivityUnavailable("GROUP_COMPLETENESS_FAILED")
    groups = tuple(
        ContractActivityGroup(
            label=item[1],
            contract_count=item[4],
            share=(item[4] / total if total else 0.0),
        )
        for item in sorted(parsed, key=lambda value: (-value[4], value[1].casefold()))
    )
    return groups


def _group_label(value: object, present: bool) -> tuple[str, tuple[str, str]]:
    if not present or value is None:
        return NULL_GROUP, ("null", "")
    if not isinstance(value, str):
        raise ContractActivityUnavailable("GROUP_LABEL_INVALID")
    if not value.strip():
        return BLANK_GROUP, ("blank", value)
    if value != value.strip():
        raise ContractActivityUnavailable("GROUP_LABEL_WHITESPACE_INVALID")
    return value, ("value", value)


def _strict_int(value: object) -> int:
    if type(value) is int and value >= 0:  # noqa: E721
        return value
    if isinstance(value, str) and re.fullmatch(r"0|[1-9]\d*", value):
        return int(value)
    raise ContractActivityUnavailable("COUNT_VALUE_INVALID")
