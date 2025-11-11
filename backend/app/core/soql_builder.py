"""Helpers to generate SoQL queries for SECOP datasets."""
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Optional

from .entity_resolver import ResolvedEntity
from .query_parser import QueryParams


class SoQLBuilder:
    """Build SECOP compatible SoQL query payloads."""

    def __init__(self, dataset_map: Optional[Dict[str, str]] = None) -> None:
        self._dataset_map = dataset_map or {}

    def build(self, params: QueryParams, entity: Optional[ResolvedEntity] = None) -> Dict[str, Any]:
        dataset_identifier = self._resolve_dataset(params, entity)
        soql_params: Dict[str, Any] = {}

        where_clause = self._build_where(params.filters)
        if where_clause:
            soql_params["$where"] = where_clause

        if params.metrics:
            soql_params["$select"] = ", ".join(params.metrics)

        if params.limit is not None:
            soql_params["$limit"] = params.limit

        return {"dataset": dataset_identifier, "params": soql_params}

    # ------------------------------------------------------------------
    def _resolve_dataset(self, params: QueryParams, entity: Optional[ResolvedEntity]) -> str:
        if entity and entity.metadata.get("dataset_id"):
            return entity.metadata["dataset_id"]
        return self._dataset_map.get(params.entity, params.entity)

    def _build_where(self, filters: Dict[str, Any]) -> str:
        clauses = []
        for key, value in filters.items():
            clauses.append(self._render_filter(key, value))
        return " AND ".join(filter(None, clauses))

    def _render_filter(self, key: str, value: Any) -> str:
        if isinstance(value, (int, float)):
            return f"{key} = {value}"
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"upper({key}) LIKE upper('%{escaped}%')"
        if isinstance(value, Iterable):
            values = ", ".join(self._quote(v) for v in value)
            return f"{key} IN ({values})"
        return f"{key} = {json.dumps(value)}"

    @staticmethod
    def _quote(value: Any) -> str:
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"


__all__ = ["SoQLBuilder"]
