"""Money V2 / M4 — cardinales monetarios españoles prácticos."""

from __future__ import annotations

import pytest

from app.core.query_router import QueryRouter
from app.utils.money import money_to_cop


router = QueryRouter()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("cien millones", 100_000_000),
        ("doscientos millones", 200_000_000),
        ("setecientos cincuenta millones", 750_000_000),
        ("mil doscientos millones", 1_200_000_000),
        ("cuatro mil millones", 4_000_000_000),
        ("veinte mil millones", 20_000_000_000),
        ("treinta y cinco millones", 35_000_000),
        ("un billon", 1_000_000_000_000),
    ],
)
def test_practical_cardinal_generalization(raw: str, expected: int) -> None:
    assert money_to_cop(raw) == expected


def test_verbal_threshold_keeps_existing_minimum_polarity() -> None:
    params = router.parse(
        "contratos de alimentacion de mas de dos mil millones"
    ).params
    assert params.get("valor_min") == 2_000_000_000
    assert params.get("valor_max") is None
    assert "dos" not in (params.get("objeto") or [])


def test_verbal_range_inherits_million_unit() -> None:
    params = router.parse(
        "contratos de alimentacion entre mil quinientos y tres mil millones"
    ).params
    assert params.get("valor_min") == 1_500_000_000
    assert params.get("valor_max") == 3_000_000_000


@pytest.mark.parametrize(
    "query",
    [
        "proceso 338-2026 de alimentacion",
        "contratos de dos proveedores de alimentacion",
        "proceso CD-08-2026-5533",
    ],
)
def test_non_monetary_numbers_do_not_create_filters(query: str) -> None:
    params = router.parse(query).params
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None


def test_deliberately_unsupported_cardinal_is_not_guessed() -> None:
    assert money_to_cop("medio millon") is None
    assert money_to_cop("un millon y medio") is None
