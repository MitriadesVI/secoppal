"""INV-MONEY-RANGE-ATOMIC — un rango de dos extremos no se aplica a medias.

Un techo sin piso no es una versión "menos precisa" de un intervalo: es otra
pregunta, y devuelve resultados perfectamente plausibles por debajo de lo que el
usuario quería ver. Por eso el caso parcial se bloquea en vez de degradarse.

Las pruebas recorren la familia de escalas y marcadores, no las dos frases que
se observaron fallar: el defecto era que la escala ``mil`` y la escala
``millones`` se comportaban distinto ante la misma gramática.
"""
from __future__ import annotations

import pytest

from app.core.money_admission import RangeVerdict, admit_range, shared_currency_marker
from app.core.query_router import QueryRouter


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


# ── El marcador de moneda se escribe una vez y gobierna los dos extremos ──

@pytest.mark.parametrize("marker", ["pesos", "$", "cop"])
def test_currency_marker_is_shared_across_both_operands(marker: str) -> None:
    assert shared_currency_marker("120 mil", f"350 mil {marker}") != ""
    assert admit_range(120_000, "120 mil", 350_000, f"350 mil {marker}") is (
        RangeVerdict.EXECUTE
    )


@pytest.mark.parametrize(
    "connector", ["entre {lo} y {hi}", "de {lo} a {hi}", "desde {lo} hasta {hi}"]
)
@pytest.mark.parametrize(
    ("low", "high", "expected"),
    [
        ("40 mil", "80 mil pesos", (40_000, 80_000)),
        ("120 mil", "350 mil pesos", (120_000, 350_000)),
        ("700 mil", "900 mil pesos", (700_000, 900_000)),
        ("120 millones", "350 millones", (120_000_000, 350_000_000)),
    ],
)
def test_explicit_range_survives_every_connector_and_scale(
    router: QueryRouter,
    connector: str,
    low: str,
    high: str,
    expected: tuple[int, int],
) -> None:
    """La misma petición no puede depender de la escala ni del conector."""
    query = f"contratos de aseo {connector.format(lo=low, hi=high)} en 2024"
    params = router.parse(query).params
    assert (params.get("valor_min"), params.get("valor_max")) == expected


# ── Nunca una cota unilateral ────────────────────────────────────────────

_PARTIAL_RANGES = (
    "contratos de aseo entre 500 mil y 3 millones",
    "contratos de aseo entre 2 mil y 5 mil millones",
)


@pytest.mark.parametrize("query", _PARTIAL_RANGES)
def test_partial_range_blocks_instead_of_degrading(
    router: QueryRouter, query: str,
) -> None:
    params = router.parse(query).params
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None
    assert params.get("money_clarification"), (
        "un rango a medias debe pedir precisión, no ejecutarse"
    )


def test_a_range_with_no_monetary_reading_stays_silent(router: QueryRouter) -> None:
    """Años y referencias no son dinero, y tampoco disparan una aclaración."""
    params = router.parse("contratos de obra entre 2025 y 2026").params
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None
    assert not params.get("money_clarification")


def test_one_sided_comparators_are_untouched(router: QueryRouter) -> None:
    """El invariante gobierna rangos, no comparadores unilaterales legítimos."""
    params = router.parse("contratos de obra por mas de 100 mil pesos").params
    assert params["valor_min"] == 100_000
    assert params.get("valor_max") is None


def test_compound_polarity_range_keeps_its_roles(router: QueryRouter) -> None:
    params = router.parse(
        "contratos de mas de 350 millones y menos de 700 millones"
    ).params
    assert params["valor_min"] == 350_000_000
    assert params["valor_max"] == 700_000_000


# ── Contrato del decisor, aislado del router ─────────────────────────────

def test_admit_range_verdicts() -> None:
    assert admit_range(1, "1", None, "dos") is RangeVerdict.ABSENT
    assert admit_range(2_000_000, "2 millones", None, "x") is RangeVerdict.BLOCK
    assert admit_range(
        2_000_000, "2 millones", 5_000_000, "5 millones",
    ) is RangeVerdict.EXECUTE
