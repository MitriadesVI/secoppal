"""Money V2 / M3 — evidencia semántica para NIT frente a dinero."""

from __future__ import annotations

import re

import pytest

from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


router = QueryRouter()
builder = SoQLBuilder()


def _params(query: str) -> dict:
    return dict(router.parse(query).params)


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("contratos de alimentacion del NIT 900123456", ["900123456"]),
        # AP2-B añadió la forma con DV separado: la fuente la publica tal cual
        # ("900454994-9" existe en documento_proveedor) y el filtro la perdía.
        ("contratos del proveedor nit 900.123.456-7",
         ["900123456", "9001234567", "900123456-7"]),
        ("800091140", ["800091140"]),
    ],
)
def test_identifier_evidence_wins_and_never_creates_money(query: str, expected: list[str]) -> None:
    params = _params(query)
    assert params.get("contratista") == expected
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None


@pytest.mark.parametrize(
    ("query", "field", "expected"),
    [
        ("contratos de alimentacion de mas de $1.500.000.000", "valor_min", 1_500_000_000),
        ("contratos de alimentacion por 2.350.000.000 pesos", "valor_min", 1_880_000_000),
        ("contratos de alimentacion de mas de 900123456 COP", "valor_min", 900_123_456),
    ],
)
def test_explicit_money_evidence_wins_over_identifier_shape(
    query: str, field: str, expected: int
) -> None:
    params = _params(query)
    assert params.get("contratista") is None
    assert params.get(field) == expected


@pytest.mark.parametrize("query", ["proceso 338-2026", "proceso CD-08-2026-5533"])
def test_process_references_never_become_money(query: str) -> None:
    params = _params(query)
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None


def test_nit_does_not_reach_where_as_value_predicate() -> None:
    params = _params("contratos de alimentacion del NIT 900123456")
    soql = builder.build(builder.dataset_id_for(params["dataset"]), params)
    assert "documento_proveedor = '900123456'" in soql
    assert re.search(r"valor_del_contrato\s*[<>]=", soql) is None


def test_identifier_and_money_can_coexist_in_one_query() -> None:
    params = _params(
        "contratos del NIT 900123456 de mas de $1.500.000.000"
    )
    assert params.get("contratista") == ["900123456"]
    assert params.get("valor_min") == 1_500_000_000
