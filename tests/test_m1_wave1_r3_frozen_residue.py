"""M1 wave 1 R3: open-language temporal residue, FROZEN as known regressions (xfail, strict).

Policy of the orchestrator (bounded hardening): what is finite and derivable from a lexicon
the repo already has is repaired (``test_m1_wave1_r3_window_lexicon``); the residue of open
language that the base ALREADY got wrong in the same way is frozen here, with the query and
the correct behaviour, and is not chased. Owner of every family:
``E-TIME (Foundation v5 time authority ACTIVE)``.

Each test asserts the CORRECT behaviour (the temporal authority asks, or resolves the period
the person meant) and is ``xfail(strict=True)``: when E-TIME fixes a family the test XPASSes,
strict turns that into a failure, and the entry must be deleted.

Every query below already failed in the base ``4ec99bc1`` (verified): silently with NO period
(vigencia/administración anterior, year in words, «2.025», «25», the lone «el último mes»), or
silently with the wrong one (the whole anchor, or 2015 for a decree). R1 turned the first group
into a window counted from today; none of them asks, and none is a regression of this wave.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.core.entity_resolver import EntityResolver
from app.core.query_router import QueryRouter
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


# ── window + an anchor the lexicon does not know: the window wins from the clock, silently ──

@_frozen("«vigencia anterior» is not in the lexicon: the window is counted from today and the "
         "anchor is ignored (base: no period at all).")
def test_window_of_the_previous_vigencia_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de la vigencia anterior")


@_frozen("A year written in words is not read: the window runs from today, disjoint from the year asked.")
def test_window_of_a_year_written_in_words_asks():
    assert _asks(f"contratos de {B} en los últimos tres meses de dos mil veinticinco")


@_frozen("«2.025» (thousands separator) is not read as a year: the window runs from today.")
def test_window_of_a_year_with_a_thousands_separator_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de 2.025")


@_frozen("A two-digit year («25») is not read: the window runs from today.")
def test_window_of_a_two_digit_year_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de 25")


@_frozen("«administración anterior» names a period of office the lexicon cannot place: the window "
         "runs from today.")
def test_window_of_the_previous_administration_asks():
    assert _asks(f"contratos de {B} en los últimos 3 meses de la administración anterior")


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


# ── VALUE: a proper noun after the plural adjective suffix (a wave-1 regression vs the base) ───

@_frozen("«300 millones o mayores Galapa»: the base read the bound (valor_min) and the buyer; since R2 "
         "the suffix is a bound only before a closed class (end, punctuation, connector, participle "
         "or verb), and a proper noun is indistinguishable from a noun («o mayores plazos») without "
         "a lexicon of nouns. This is the ONLY VALUE query of the zeus2 probes that executed in the "
         "base and asks now (declared).")
def test_a_proper_noun_after_the_plural_suffix_keeps_the_bound():
    router = QueryRouter(EntityResolver(Path("app/data/aliases_db.json")))
    params = router.parse("contratos de la Alcaldía de Galapa de 300 millones o mayores Galapa", today=TODAY).params
    assert params.get("valor_min") == 300_000_000
