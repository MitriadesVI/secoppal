"""M1 wave 1 R5 (ZEUS4 P3, mutant R4-16): «de 25» that is not adjacent to the window is a QUANTITY.

A two-digit figure is a year only right after the window («los últimos 3 meses de 25») and before the
end of the clause. Anywhere else «de 25» counts something (entities, municipalities…) and the window
keeps its meaning. Adding ``TWO_DIGITS`` to the forms found at any position of the text («_YEAR_FORMS»)
turned «… para entidades de 25» (end of text) and «de 25, …» (comma) into an explicit-year anchor: the
window asked instead of resolving. Those two shapes are what kills that mutant.
"""
from __future__ import annotations

from datetime import date

import pytest

from app.core import non_canonical_year as ncy
from app.core.temporal_semantics import resolve_temporal_scope

TODAY = date(2026, 10, 1)
QUANTITIES = [
    ("procesos de 25 municipios en los últimos 30 días", ("2026-09-01", "2026-10-01")),
    ("contratos de los últimos 3 meses de 25 entidades", ("2026-07-01", "2026-10-01")),
    ("contratos de los últimos 3 meses de aseo para entidades de 25", ("2026-07-01", "2026-10-01")),
    ("en los últimos 3 meses contratos de entidades de 25", ("2026-07-01", "2026-10-01")),
    ("procesos de 25, en los últimos 30 días", ("2026-09-01", "2026-10-01")),
    ("contratos de 25. los últimos 6 meses", ("2026-04-01", "2026-10-01")),
    ("contratos con 25 entidades de 25 en los últimos 3 meses", ("2026-07-01", "2026-10-01")),
]


@pytest.mark.parametrize("text,window", QUANTITIES)
def test_a_figure_not_adjacent_to_the_window_is_a_quantity(text, window):
    resolution = resolve_temporal_scope(text, today=TODAY)
    assert resolution.error_code is None, text
    assert (resolution.fecha_desde, resolution.fecha_hasta) == window, text


@pytest.mark.parametrize("text", [
    "contratos en los últimos 3 meses de 25",
    "contratos en los últimos 3 meses de 25.",
    "contratos en los últimos 3 meses, de 25",
])
def test_the_same_figure_right_after_the_window_is_still_a_year(text):
    assert resolve_temporal_scope(text, today=TODAY).error_code == "relative_window_explicit_anchor"


def test_the_two_digit_form_is_adjacent_only():
    """The text-wide search has no two-digit form; only the adjacent one does (mutant R4-16)."""
    assert ncy.TWO_DIGITS not in ncy._YEAR_FORMS
    assert ncy.find("entidades de 25") is None and ncy.find("de 25, aparte") is None
    assert ncy.find_adjacent("de 25", 0) is not None
