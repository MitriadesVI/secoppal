"""Tests for money_to_cop — Colombian monetary expression parser."""

from app.utils.money import money_to_cop


def test_billon_is_10_power_12():
    assert money_to_cop("1 billón") == 1_000_000_000_000
    assert money_to_cop("2 billones") == 2_000_000_000_000
    assert money_to_cop("1.5 billones") == 1_500_000_000_000


def test_mil_millones_is_10_power_9():
    assert money_to_cop("mil millones") == 1_000_000_000
    assert money_to_cop("1 mil millones") == 1_000_000_000
    assert money_to_cop("2 mil millones") == 2_000_000_000


def test_millones_is_10_power_6():
    assert money_to_cop("500 millones") == 500_000_000
    assert money_to_cop("1 millón") == 1_000_000


def test_billon_no_regression_on_other_units():
    """mil millones and millones must NOT be affected by billón fix."""
    assert money_to_cop("mil millones") == 1_000_000_000
    assert money_to_cop("100 millones") == 100_000_000
    assert money_to_cop("500 mil") == 500_000
    assert money_to_cop("1 billón") == 1_000_000_000_000
    assert money_to_cop("2 billones") == 2_000_000_000_000


def test_money_to_cop_edge_cases():
    assert money_to_cop("hola") is None
    assert money_to_cop("10 pesos") == 10
