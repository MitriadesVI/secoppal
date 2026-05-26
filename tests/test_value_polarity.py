import pytest

from app.core.query_router import QueryRouter


router = QueryRouter()


POSITIVE_CASES = [
    ("mayores a 30 millones", {"valor_min": 30_000_000}),
    ("menos de 20 millones", {"valor_max": 20_000_000}),
    ("entre 20 y 30 millones", {"valor_min": 20_000_000, "valor_max": 30_000_000}),
    ("al menos 50 millones", {"valor_min": 50_000_000}),
    ("hasta 100 millones", {"valor_max": 100_000_000}),
]

NEGATION_CASES = [
    ("no sean mayores a 30 millones", {"valor_max": 30_000_000}),
    ("que no supere los 25 millones", {"valor_max": 25_000_000}),
    ("que no exceda 50 millones", {"valor_max": 50_000_000}),
    ("no sean menores a 10 millones", {"valor_min": 10_000_000}),
    ("que no baje de 5 millones", {"valor_min": 5_000_000}),
]

# Sprint 4 — MIN-MAX-INVERSION-001 nuevos patrones
SPRINT4_CASES = [
    ("menor o igual a 40 millones", {"valor_max": 40_000_000}),
    ("mayor o igual a 60 millones", {"valor_min": 60_000_000}),
    ("a lo sumo 80 millones", {"valor_max": 80_000_000}),
    ("menor o igual que 35 millones", {"valor_max": 35_000_000}),
    ("mayor o igual que 75 millones", {"valor_min": 75_000_000}),
]

# PARSER-POLARITY-MONETARY-002: post-number polarity patterns (dogfood 2026-05-25)
POST_NUMBER_CASES = [
    ("500 millones o menos", {"valor_max": 500_000_000, "valor_min": None}),
    ("100 millones o mas", {"valor_min": 100_000_000}),
    ("1000 millones y mayores", {"valor_min": 1_000_000_000}),
    ("500 millones para arriba", {"valor_min": 500_000_000}),
    ("500 millones en adelante", {"valor_min": 500_000_000}),
]

NO_FALSE_RANGE_CASES = [
    (
        "contratos por 500 millones o menos",
        {"valor_max": 500_000_000, "valor_min": None, "objeto_not_contains": ["menos"]},
    ),
    (
        "contratos de 500 millones en adelante",
        {"valor_min": 500_000_000, "objeto_not_contains": ["adelante"]},
    ),
]


@pytest.mark.parametrize("query,expected", POSITIVE_CASES + NEGATION_CASES + SPRINT4_CASES + POST_NUMBER_CASES)
def test_value_polarity(query, expected):
    params = router.parse(query).params
    for key, value in expected.items():
        assert params.get(key) == value


@pytest.mark.parametrize("query,expected", NO_FALSE_RANGE_CASES)
def test_value_phrase_scrub(query, expected):
    params = router.parse(query).params
    objeto = params.get("objeto", [])
    for forbidden in expected["objeto_not_contains"]:
        assert forbidden not in objeto
    if "valor_max" in expected:
        assert params.get("valor_max") == expected["valor_max"]
    if "valor_min" in expected:
        assert params.get("valor_min") == expected["valor_min"]
