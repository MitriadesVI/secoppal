from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from backend.app.core.entity_resolver import ResolvedEntity
from backend.app.core.query_parser import QueryParams
from backend.app.core.soql_builder import SoQLBuilder


def test_build_includes_limit_and_filters():
    params = QueryParams(
        entity="contracts",
        filters={"buyer": "Bogotá"},
        metrics=["total_amount"],
        raw_query="",
        limit=10,
    )
    builder = SoQLBuilder()

    payload = builder.build(params)

    assert payload["dataset"] == "contracts"
    assert payload["params"]["$select"] == "total_amount"
    assert payload["params"]["$limit"] == 10
    assert "upper(buyer) LIKE upper('%Bogotá%')" == payload["params"]["$where"]


def test_resolved_entity_metadata_precedence():
    params = QueryParams(entity="contracts", raw_query="")
    builder = SoQLBuilder(dataset_map={"contracts": "default-dataset"})
    resolved = ResolvedEntity(name="Contracts", score=0.9, metadata={"dataset_id": "secop-123"})

    payload = builder.build(params, resolved)

    assert payload["dataset"] == "secop-123"
    assert payload["params"] == {}
