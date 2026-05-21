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

NO_FILLER_CASES = [
    (
        "transporte escolar no sean mayores a 30 millones",
        {"valor_max": 30_000_000, "objeto_not_contains": ["mayores", "sean", "no"]},
    ),
]


@pytest.mark.parametrize("query,expected", POSITIVE_CASES + NEGATION_CASES)
def test_value_polarity(query, expected):
    params = router.parse(query).params
    for key, value in expected.items():
        assert params.get(key) == value


@pytest.mark.parametrize("query,expected", NO_FILLER_CASES)
def test_value_phrase_scrub(query, expected):
    params = router.parse(query).params
    objeto = params.get("objeto", [])
    assert params.get("valor_max") == expected["valor_max"]
    for forbidden in expected["objeto_not_contains"]:
        assert forbidden not in objeto
