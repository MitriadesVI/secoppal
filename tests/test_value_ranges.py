"""Tests for monetary range parsing (VALUE-RANGE-001 + DOGFOOD-RANGE-007)."""

import pytest
from app.core.query_router import QueryRouter


router = QueryRouter()


# ── Baseline: existing DOGFOOD-RANGE-005 cases ─────────────────────────────

def test_hyphen_range_millones():
    result = router.parse("contratos de mantenimiento entre 1000-3000 millones de 2026")
    params = result.params if hasattr(result, "params") else result
    assert params.get("valor_min") == 1_000_000_000
    assert params.get("valor_max") == 3_000_000_000


def test_hyphen_range_con_y():
    result = router.parse("contratos de mantenimiento entre 1000 y 3000 millones de 2026")
    params = result.params if hasattr(result, "params") else result
    assert params.get("valor_min") == 1_000_000_000
    assert params.get("valor_max") == 3_000_000_000


def test_range_de_a_millones():
    result = router.parse("contratos de mantenimiento de 1000 a 3000 millones")
    params = result.params if hasattr(result, "params") else result
    assert params.get("valor_min") == 1_000_000_000
    assert params.get("valor_max") == 3_000_000_000


# ── Fase 1: DOGFOOD-RANGE-005 regression tests ─────────────────────────────

RANGE_005_CASES = [
    (
        "contratos entre 500 y mil millones en Santander",
        {"valor_min": 500_000_000, "valor_max": 1_000_000_000},
    ),
    (
        "contratos entre 1 y 2 mil millones en Santander",
        {"valor_min": 1_000_000_000, "valor_max": 2_000_000_000},
    ),
    (
        "contratos entre 100 y 550 millones en Santander",
        {"valor_min": 100_000_000, "valor_max": 550_000_000},
    ),
    (
        "contratos 1000-3000 millones en Santander",
        {"valor_min": 1_000_000_000, "valor_max": 3_000_000_000},
    ),
]


@pytest.mark.parametrize("query,expected", RANGE_005_CASES)
def test_range_005_regression(query, expected):
    params = router.parse(query).params
    for key, value in expected.items():
        assert params.get(key) == value


# ── Fase 2: RANGE-007 new cases ────────────────────────────────────────────

RANGE_007_CASES = [
    (
        "contratos desde 500 hasta 1000 millones en Santander",
        {"valor_min": 500_000_000, "valor_max": 1_000_000_000},
    ),
    (
        "contratos de 500 a 1000 millones en Santander",
        {"valor_min": 500_000_000, "valor_max": 1_000_000_000},
    ),
    (
        "contratos entre 500 millones y 1000 en Santander",
        {"valor_min": 500_000_000, "valor_max": 1_000_000_000},
    ),
]


@pytest.mark.parametrize("query,expected", RANGE_007_CASES)
def test_range_007_explicit_prefix(query, expected):
    params = router.parse(query).params
    for key, value in expected.items():
        assert params.get(key) == value


def test_range_entre_1_y_2_billones():
    params = router.parse("contratos entre 1 y 2 billones en Santander").params
    assert params.get("valor_min") == 1_000_000_000_000
    assert params.get("valor_max") == 2_000_000_000_000


# ── Compound polarity ranges (original DOGFOOD-RANGE-007) ──────────────────

COMPOUND_RANGE_CASES = [
    (
        "contratos más de 350 millones y menos de 700 en Santander",
        {"valor_min": 350_000_000, "valor_max": 700_000_000},
    ),
    (
        "contratos mayor a 100 millones y menor a 500 en Santander",
        {"valor_min": 100_000_000, "valor_max": 500_000_000},
    ),
    (
        "contratos por encima de 50 millones y por debajo de 200 en Santander",
        {"valor_min": 50_000_000, "valor_max": 200_000_000},
    ),
    (
        "contratos al menos 50 millones y a lo sumo 150 en Santander",
        {"valor_min": 50_000_000, "valor_max": 150_000_000},
    ),
]


@pytest.mark.parametrize("query,expected", COMPOUND_RANGE_CASES)
def test_compound_polarity_range(query, expected):
    params = router.parse(query).params
    for key, value in expected.items():
        assert params.get(key) == value


# ── Negative cases ─────────────────────────────────────────────────────────

NEGATIVE_RANGE_CASES = [
    ("contratos entre 100 y 500 en Santander", {}),
    ("contratos entre sena y magdalena", {}),
    ("procesos entre bogota y medellin", {}),
]


@pytest.mark.parametrize("query,expected", NEGATIVE_RANGE_CASES)
def test_no_range_when_no_units(query, expected):
    params = router.parse(query).params
    assert params.get("valor_min") is None
    assert params.get("valor_max") is None


# ── Objeto scrub: range tokens must not leak into object ───────────────────

SCRUB_CASES = [
    (
        "contratos desde 500 hasta 1000 millones en Santander",
        ["desde", "hasta", "millones"],
    ),
    (
        "contratos entre 100 y 550 millones en Santander",
        ["entre", "millones"],
    ),
    (
        "contratos más de 350 millones y menos de 700 en Santander",
        ["mas", "menos", "millones"],
    ),
]


@pytest.mark.parametrize("query,forbidden", SCRUB_CASES)
def test_range_tokens_scrubbed_from_objeto(query, forbidden):
    params = router.parse(query).params
    objeto = params.get("objeto", []) or []
    objeto_lower = [str(t).lower() for t in objeto]
    for word in forbidden:
        assert word not in objeto_lower, f"'{word}' leaked into objeto: {objeto}"
