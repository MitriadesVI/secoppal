"""Money V2 / M2 — tests de la autoridad de representación numérica monetaria.

Prueban ``app.core.money_resolution`` directamente, aislada de unidades, de
polaridad y del router. Los contratos end-to-end viven en
``tests/test_money_v2_invariants.py``.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.core.money_resolution import (
    find_numeric_literal,
    resolve_amount,
    resolve_numeric_literal,
)
from app.utils.money import money_to_cop


# ── Agrupación de miles con punto (la regla que cierra H1) ────────────────

GROUPING_DOT = [
    ("1.500", 1500),
    ("1.050", 1050),
    ("2.500", 2500),
    ("10.500", 10500),
    ("12.345", 12345),
    ("999.999", 999999),
    ("1.001", 1001),
]


@pytest.mark.parametrize("literal,expected", GROUPING_DOT)
def test_dot_groups_thousands(literal: str, expected: int) -> None:
    assert resolve_numeric_literal(literal) == Decimal(expected)


# ── Agrupación con coma (convención internacional) ────────────────────────

@pytest.mark.parametrize("literal,expected", [("1,500", 1500), ("1,500,000", 1500000)])
def test_comma_groups_thousands(literal: str, expected: int) -> None:
    assert resolve_numeric_literal(literal) == Decimal(expected)


# ── Múltiples grupos ──────────────────────────────────────────────────────

MULTI_GROUP = [
    ("1.500.000", 1_500_000),
    ("1.001.000", 1_001_000),
    ("10.000.000", 10_000_000),
    ("2.350.000.000", 2_350_000_000),
    ("999.999.999", 999_999_999),
    ("1.500.000.000", 1_500_000_000),
]


@pytest.mark.parametrize("literal,expected", MULTI_GROUP)
def test_multiple_groups(literal: str, expected: int) -> None:
    assert resolve_numeric_literal(literal) == Decimal(expected)


# ── Marca decimal: 1-2 dígitos a la derecha ───────────────────────────────

DECIMALS = [
    ("1.5", Decimal("1.5")),
    ("1,5", Decimal("1.5")),
    ("1.25", Decimal("1.25")),
    ("1,25", Decimal("1.25")),
    ("1.01", Decimal("1.01")),
    ("999.99", Decimal("999.99")),
]


@pytest.mark.parametrize("literal,expected", DECIMALS)
def test_one_or_two_digits_is_a_decimal_mark(literal: str, expected: Decimal) -> None:
    assert resolve_numeric_literal(literal) == expected


def test_the_discriminant_is_the_digit_count() -> None:
    """Mismo prefijo, tres dígitos frente a uno: agrupación frente a decimal."""
    assert resolve_numeric_literal("1.500") == Decimal(1500)
    assert resolve_numeric_literal("1.5") == Decimal("1.5")
    assert resolve_numeric_literal("1.500") == 1000 * resolve_numeric_literal("1.5")


def test_leading_zero_is_never_a_thousands_group() -> None:
    """Nadie escribe miles como "0.500"; el cero inicial delata un decimal."""
    assert resolve_numeric_literal("0.500") == Decimal("0.5")
    assert resolve_numeric_literal("0,500") == Decimal("0.5")


# ── Separadores mixtos: el más a la derecha manda ─────────────────────────

@pytest.mark.parametrize(
    "literal,expected",
    [
        ("1.234.567,89", Decimal("1234567.89")),
        ("1,234,567.89", Decimal("1234567.89")),
        ("1.500,25", Decimal("1500.25")),
        ("1,500.25", Decimal("1500.25")),
    ],
)
def test_mixed_separators(literal: str, expected: Decimal) -> None:
    assert resolve_numeric_literal(literal) == expected


# ── Entradas inválidas o ambiguas: None, nunca una cifra adivinada ────────

@pytest.mark.parametrize("literal", ["", "abc", "1..500", "1.50.000", "1.23.456", ".", ",", "-5"])
def test_invalid_literals_resolve_to_none(literal: str) -> None:
    assert resolve_numeric_literal(literal) is None


def test_resolve_amount_supports_cardinal_words_without_digits() -> None:
    assert resolve_amount("mil millones") == Decimal(1000)
    assert resolve_amount("sin valor") is None


# ── Localización del literal dentro de una frase ──────────────────────────

@pytest.mark.parametrize(
    "text,expected",
    [
        ("1.500 millones", "1.500"),
        ("$1.500.000.000", "1.500.000.000"),
        ("2.350.000.000 pesos", "2.350.000.000"),
        ("cop 1.500.000", "1.500.000"),
        ("900.123.456-7", "900.123.456"),
        ("sin numeros", None),
    ],
)
def test_find_numeric_literal(text: str, expected: str | None) -> None:
    assert find_numeric_literal(text) == expected


def test_currency_markers_do_not_change_the_value() -> None:
    """"$", "pesos" y "COP" marcan moneda; no alteran la cifra."""
    assert resolve_amount("1.500.000") == resolve_amount("$1.500.000")
    assert resolve_amount("$1.500.000") == resolve_amount("1.500.000 pesos")
    assert resolve_amount("1.500.000 pesos") == resolve_amount("cop 1.500.000")


# ══════════════════════════════════════════════════════════════════════════
# Integración con la capa de unidades (money_to_cop), que M2 no reescribió.
# ══════════════════════════════════════════════════════════════════════════

UNIT_SCALES = [
    ("1 millon", 1_000_000),
    ("2 millones", 2_000_000),
    ("1.500 millones", 1_500_000_000),
    ("mil millones", 1_000_000_000),
    ("2 mil millones", 2_000_000_000),
    ("1 billon", 1_000_000_000_000),
    ("2 billones", 2_000_000_000_000),
    ("1.5 billones", 1_500_000_000_000),
    ("1 palo", 1_000_000),
    ("500 barras", 500_000_000),
    ("500 mil", 500_000),
]


@pytest.mark.parametrize("text,expected", UNIT_SCALES)
def test_units_scale_the_resolved_amount(text: str, expected: int) -> None:
    assert money_to_cop(text) == expected


def test_billion_invariant_survives_the_new_numeric_layer() -> None:
    assert money_to_cop("1 billón") == 1000 * money_to_cop("mil millones")


EXPLICIT_COP = [
    ("1.500.000", 1_500_000),
    ("1,500,000", 1_500_000),
    ("$1.500.000", 1_500_000),
    ("$1.500.000.000", 1_500_000_000),
    ("2.350.000.000 pesos", 2_350_000_000),
    ("999.999.999 pesos", 999_999_999),
]


@pytest.mark.parametrize("text,expected", EXPLICIT_COP)
def test_explicit_cop_amounts(text: str, expected: int) -> None:
    """Cantidades COP explícitas, resueltas en aislamiento.

    Que la autoridad monetaria sepa leerlas NO implica que QueryRouter ya las
    reconozca dentro de una consulta: ahí compiten con el detector de NIT, y
    ese arbitraje es M3.
    """
    assert money_to_cop(text) == expected


def test_generalization_beyond_the_fixtures() -> None:
    """La regla generaliza; no es una colección de casos particulares."""
    assert money_to_cop("999.999 millones") == 999_999_000_000
    assert money_to_cop("12.345 millones") == 12_345_000_000
    assert money_to_cop("1.001 millones") == 1_001_000_000
    assert money_to_cop("100.000 millones") == 100_000_000_000


def test_no_float_rounding_in_the_money_path() -> None:
    """Decimal, no float: la cifra no depende de representación binaria."""
    assert isinstance(resolve_numeric_literal("1.25"), Decimal)
    # 0.1+0.2 en float no es 0.3; la ruta monetaria no debe heredar eso.
    assert money_to_cop("1.15 millones") == 1_150_000
    assert money_to_cop("2.35 millones") == 2_350_000


# ── Paridad con la regla que ya existía en el repo ────────────────────────

def test_parity_with_notify_grouping_numeric_value() -> None:
    """Una sola política de separadores en todo el proyecto.

    ``app.core.notify.grouping.numeric_value`` ya resolvía separadores
    colombianos para el radar. M2 replica esa política en vez de inventar otra,
    porque dos autoridades que devuelven cifras distintas para el mismo texto es
    justo el defecto que M0 diagnosticó. Este test detecta la divergencia si
    alguien cambia una de las dos.
    """
    from app.core.notify.grouping import numeric_value

    shared = [
        "3.000", "3,000", "3.50", "3,50",
        "1.234.567,89", "1,234,567.89",
        "1.500", "1.500.000", "1,500,000", "1.5", "1,25", "999.999",
    ]
    for text in shared:
        assert resolve_numeric_literal(text) == numeric_value(text), text
