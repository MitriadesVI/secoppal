"""M1 wave 1 R3: open-language temporal residue, FROZEN as known regressions (xfail, strict).

Policy of the orchestrator (bounded hardening): what is finite and derivable from a lexicon
the repo already has is repaired (``test_m1_wave1_r3_window_lexicon``); the residue of open
language that the base ALREADY got wrong in the same way is frozen here, with the query and
the correct behaviour, and is not chased. Owner of every family:
``E-TIME (Foundation v5 time authority ACTIVE)``.

Each test asserts the CORRECT behaviour (the temporal authority asks, or resolves the period
the person meant) and is ``xfail(strict=True)``: when E-TIME fixes a family the test XPASSes,
strict turns that into a failure, and the entry must be deleted.

Every xfail below already failed in the base ``4ec99bc1`` (verified): silently with NO period
(the lone «el último mes»), or silently with the wrong one (the whole anchor, or 2015 for a decree).
R4 closed the first group of R3 (window + «vigencia/administración anterior», year in words,
«2.025», «25») with ``temporal_noun_anchor``: those five are ordinary tests below. Eight xfail remain (two new in R4: a temporal noun phrase with no window).
"""
from __future__ import annotations

from datetime import date

import pytest

from app.core.temporal_semantics import resolve_temporal_scope

TODAY = date(2026, 10, 1)
OWNER = "E-TIME (Foundation v5 time authority ACTIVE)"
B = "la Alcaldía de Galapa"


def _resolve(text: str):
    return resolve_temporal_scope(text, today=TODAY)


def _asks(text: str) -> bool:
    found = _resolve(text)
    return bool(found.error_code) and not (found.fecha_desde or found.fecha_hasta)


def _frozen(reason: str):
    return pytest.mark.xfail(strict=True, reason=f"{reason} Owner: {OWNER}.")


# ── window + an anchor the lexicon does not know: CLOSED in R4 (``temporal_noun_anchor``) ────
# These five were frozen in R3 as xfail; R4 declares the finite vocabulary (temporal nouns, years
# in words / with a separator / in two digits) and they are now ordinary tests. The property
# lives in ``test_m1_wave1_r4_noun_anchor``.

def test_window_of_the_previous_vigencia_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de la vigencia anterior")


def test_window_of_a_year_written_in_words_asks():
    assert _asks(f"contratos de {B} en los últimos tres meses de dos mil veinticinco")


def test_window_of_a_year_with_a_thousands_separator_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de 2.025")


def test_window_of_a_two_digit_year_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de 25")


def test_window_of_the_previous_administration_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de la administración anterior")


# ── a temporal noun phrase with NO window is a period the authority drops (R4: open residue) ──

@_frozen("«antes de la pandemia» names a period the authority cannot place and no window is involved "
         "(R4 only asks when a relative window shares the turn): the search runs with no period, "
         "exactly as in the base.")
def test_a_period_noun_phrase_without_a_window_asks():
    assert _asks(f"contratos de {B} antes de la pandemia")


@_frozen("«de la vigencia anterior» alone is dropped (no period) as in the base; R4 asks only next to "
         "a relative window.")
def test_the_previous_vigencia_alone_asks():
    assert _asks(f"contratos de {B} de la vigencia anterior")


# ── a partial period of an anchor is resolved as the whole anchor ─────────────────────────────

@_frozen("«último bimestre» is not in the lexicon: the whole year 2025 is executed.")
def test_last_bimester_of_a_year_does_not_widen_to_the_year():
    found = _resolve(f"contratos de {B} en el último bimestre de 2025")
    assert (found.fecha_desde, found.fecha_hasta) != ("2025-01-01", "2025-12-31")


@_frozen("«última quincena» is not in the lexicon: the whole month is executed.")
def test_last_fortnight_of_a_month_does_not_widen_to_the_month():
    found = _resolve(f"contratos de {B} en la última quincena de diciembre de 2025")
    assert (found.fecha_desde, found.fecha_hasta) != ("2025-12-01", "2025-12-31")


@_frozen("«a finales de» is not in the lexicon: the whole year 2025 is executed.")
def test_the_end_of_a_year_does_not_widen_to_the_year():
    found = _resolve(f"contratos de {B} a finales de 2025")
    assert (found.fecha_desde, found.fecha_hasta) != ("2025-01-01", "2025-12-31")


@_frozen("«hace 3 meses de 2025»: the relative reading wins over the explicit year (2026-07).")
def test_months_ago_with_an_explicit_year_asks():
    assert _asks(f"contratos de {B} hace 3 meses de 2025")


# ── a window of time_relative with no anchor is not resolved by the authority at all ──────────

@_frozen("time_relative is wired only into the dormant time_authority: the lone window «el último "
         "mes» is dropped and the search runs with no period.")
def test_a_lone_window_of_time_relative_is_not_dropped():
    found = _resolve(f"contratos de {B} en el último mes")
    assert found.matched and (found.fecha_desde or found.error_code)


# ── a norm citation alone is read as a period ─────────────────────────────────────────────────

@_frozen("The year of «decreto 1082 de 2015» is read as the period 2015 when nothing else is "
         "temporal (R3 only stops it from being an ANCHOR of a window).")
def test_a_norm_citation_alone_is_not_a_period():
    found = _resolve("contratos por decreto 1082 de 2015")
    assert found.fecha_desde != "2015-01-01"
