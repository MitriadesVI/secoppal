"""M1-H0 R2: the digit boundaries of the TIME guard (mutants of R1 that ZEUS showed alive).

`_SMALL_NUMBER` (1-3 digits) is glue, anything longer is not: «hasta 3025 2025» has a number between the
bound and the year, so the walk stops and the year is not read as bounded.  `_DATE_BEFORE` reads numeric dates
of 1 to 4 digits before the year («5/2025», «15/03/2025»).  Both are limited guarantees (CANDIDATE.md R2).
"""

from __future__ import annotations

import pytest

from app.core.foundation_time_coverage import qualifies_year


def _qualified(raw: str) -> bool:  # the LAST «2025» is the cited year
    at = raw.rindex("2025")
    return qualifies_year(raw, at, at + 4)


@pytest.mark.parametrize("number", ["1", "7", "15", "99", "100", "999"])
def test_a_number_of_one_to_three_digits_is_glue_between_a_bound_and_the_year(number):
    assert _qualified(f"hasta {number} 2025")


@pytest.mark.parametrize("number", ["1000", "3025", "1899", "4000", "5555", "12025", "100000"])
def test_a_longer_number_is_not_glue_so_it_cuts_the_walk(number):
    assert not _qualified(f"hasta {number} 2025")
    assert not _qualified(f"desde {number} 2025")
    at = f"2025 {number} hasta".index("2025")  # the same boundary after the year
    assert not qualifies_year(f"2025 {number} hasta", at, at + 4)


@pytest.mark.parametrize("digits", ["5", "15", "123", "3025"])
@pytest.mark.parametrize("separator", ["/", "-", ".", " / ", " - ", "/ ", " ."])
def test_a_numeric_date_of_one_to_four_digits_before_the_year_is_a_qualifier(digits, separator):
    assert _qualified(f"contratos {digits}{separator}2025")


def test_without_a_separator_the_digits_before_the_year_are_not_a_date():
    assert not _qualified("contratos 15 2025") and not _qualified("contratos de 2025")
