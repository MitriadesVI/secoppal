"""Tests para format_money — convención Colombia (MM = mil millones, B = billones)."""

from app.utils.money import format_money


def test_billones():
    assert format_money(80_000_000_000_000) == "$80.00B"
    assert format_money(1_500_000_000_000) == "$1.50B"


def test_mil_millones():
    assert format_money(80_000_000_000) == "$80.00MM"
    assert format_money(63_800_000_000) == "$63.80MM"
    assert format_money(1_442_664_960) == "$1.44MM"
    assert format_money(1_000_000_000) == "$1.00MM"


def test_millones():
    assert format_money(500_000_000) == "$500M"
    assert format_money(1_000_000) == "$1M"
    assert format_money(999_999_999) == "$1000M"  # redondeo en límite


def test_menor_a_millon():
    assert format_money(500_000) == "$500,000"
    assert format_money(1) == "$1"


def test_cero_o_nulo():
    assert format_money(0) == "Sin valor"
    assert format_money(None) == "Sin valor"
    assert format_money("") == "Sin valor"
