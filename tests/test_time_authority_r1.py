"""M1-D-TIME R1: año implícito y periodos relativos del Gold sellado.

Dos reglas, ambas contra un oráculo de aritmética de calendario propio del test:

* mes / día / rango de meses o días SIN año = la ocurrencia más reciente que no sea
  posterior a ``today`` (``year_inferred=True`` para que el host anuncie el año);
* periodos relativos (este mes/trimestre/semestre/año, últimos N …, el último año,
  estos N últimos meses, hace N semanas) con el reloj inyectado; las ventanas móviles
  terminan hoy (inclusive) y lo dicen en ``precision``/``moving_window``.
"""
from __future__ import annotations

from datetime import date, timedelta

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core.temporal_semantics import resolve_temporal_scope
from app.core.time_authority import TimeResolution, TimeValue, resolve_time_condition

POS = "POSITIVE"
TODAY = date(2026, 9, 30)  # el reloj del Gold

MONTHS = [
    ("enero", 1), ("febrero", 2), ("marzo", 3), ("abril", 4), ("mayo", 5), ("junio", 6),
    ("julio", 7), ("agosto", 8), ("septiembre", 9), ("octubre", 10), ("noviembre", 11),
    ("diciembre", 12),
]
ABBREVIATIONS = [
    ("ene", 1), ("feb", 2), ("mar", 3), ("abr", 4), ("may", 5), ("jun", 6), ("jul", 7),
    ("ago", 8), ("sep", 9), ("sept", 9), ("oct", 10), ("nov", 11), ("dic", 12),
]
# Un «hoy» por cada mes del año, incluidos primer y último día, cruce de año y bisiesto.
CLOCKS = (
    [date(2026, month, day) for month in range(1, 13) for day in (1, 15)]
    + [date(2026, 12, 31), date(2026, 1, 31), date(2025, 1, 1), date(2024, 2, 29),
       date(2024, 2, 28), date(2024, 3, 1), date(2027, 3, 1), date(2028, 2, 29)]
)


def run(quote: str, operator: str, today: date, left=(), right=()) -> TimeResolution:
    return resolve_time_condition(quote, left, right, operator, POS, today=today)


def value_of(quote: str, operator: str, today: date) -> TimeValue:
    result = run(quote, operator, today)
    assert result.decision == "RESOLVED", (quote, operator, today, result)
    assert result.value is not None
    return result.value


def first_of_next_month(year: int, month: int) -> date:
    return date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)


def latest_month_year(month: int, today: date) -> int:
    """Año más reciente cuyo mes `month` no es posterior al mes de `today`."""
    return today.year if month <= today.month else today.year - 1


def latest_day_year(month: int, day: int, today: date) -> int | None:
    """Año más reciente en que existe `day/month` y no es posterior a `today`."""
    for year in range(today.year, today.year - 9, -1):
        try:
            candidate = date(year, month, day)
        except ValueError:
            continue
        if candidate <= today:
            return year
    return None


def months_back(day: date, months: int) -> date:
    index = day.year * 12 + (day.month - 1) - months
    year, zero = divmod(index, 12)
    month = zero + 1
    last = (first_of_next_month(year, month) - timedelta(days=1)).day
    return date(year, month, min(day.day, last))


# ----------------------------------------------------------------------- mes sin año


@pytest.mark.parametrize("today", CLOCKS)
@pytest.mark.parametrize(("name", "number"), MONTHS + ABBREVIATIONS)
def test_month_without_year_is_the_latest_occurrence_not_after_today(name, number, today):
    year = latest_month_year(number, today)
    expected = TimeValue(
        date(year, number, 1), first_of_next_month(year, number), "month", year_inferred=True,
    )
    for quote in (name, f"en {name}", f"durante {name}", f"del mes de {name}"):
        assert value_of(quote, "during_month", today) == expected, (quote, today)


def test_current_month_counts_as_not_after_today_and_next_month_goes_back_a_year():
    today = date(2026, 5, 3)
    assert value_of("mayo", "during_month", today).start == date(2026, 5, 1)
    assert value_of("junio", "during_month", today).start == date(2025, 6, 1)
    assert value_of("abril", "during_month", today).start == date(2026, 4, 1)
    january = date(2026, 1, 15)
    assert value_of("enero", "during_month", january).start == date(2026, 1, 1)
    assert value_of("diciembre", "during_month", january).start == date(2025, 12, 1)


@pytest.mark.parametrize("quote", ["mayo de 2024", "mayo del 2024", "en mayo, 2024"])
def test_an_explicit_year_is_never_inferred_and_never_flagged(quote):
    for today in CLOCKS[:6]:
        found = value_of(quote, "during_month", today)
        assert found == TimeValue(date(2024, 5, 1), date(2024, 6, 1), "month")
        assert found.year_inferred is False


@pytest.mark.parametrize("operator", ["during_year", "between_dates", "relative_period"])
def test_inferred_month_still_obeys_the_operator_check(operator):
    result = run("mayo", operator, TODAY)
    assert (result.decision, result.detail) == ("CLARIFY", "TIME_OPERATOR_EVIDENCE_MISMATCH")


@pytest.mark.parametrize("today", CLOCKS)
@pytest.mark.parametrize(("name", "number"), MONTHS)
def test_bounds_on_a_month_without_year_use_the_inferred_year(name, number, today):
    year = latest_month_year(number, today)
    first, after = date(year, number, 1), first_of_next_month(year, number)
    assert value_of(f"desde {name}", "between_dates", today).start == first
    assert value_of(f"a partir de {name}", "between_dates", today).start == first
    assert value_of(f"después de {name}", "between_dates", today).start == after
    assert value_of(f"antes de {name}", "between_dates", today).end_exclusive == first
    assert value_of(f"hasta {name}", "between_dates", today).end_exclusive == after
    assert value_of(f"hasta {name}", "between_dates", today).year_inferred is True


# ---------------------------------------------------------------------- día sin año


@pytest.mark.parametrize("today", CLOCKS)
@pytest.mark.parametrize(("name", "number"), MONTHS)
@pytest.mark.parametrize("day", [1, 9, 15, 28, 29, 30, 31])
def test_day_without_year_is_the_latest_existing_date_not_after_today(name, number, day, today):
    year = latest_day_year(number, day, today)
    result = run(f"el {day} de {name}", "between_dates", today)
    if year is None:  # 30/31 que no existen en ese mes: nunca se inventa
        assert (result.decision, result.detail) == ("CLARIFY", "TIME_INVALID_DATE")
        return
    expected = TimeValue(
        date(year, number, day), date(year, number, day) + timedelta(days=1), "day",
        year_inferred=True,
    )
    assert result.decision == "RESOLVED" and result.value == expected, (day, name, today)


@pytest.mark.parametrize(
    ("today", "expected"),
    [
        (date(2027, 3, 1), date(2024, 2, 29)),  # el 29 de febrero se busca hacia atrás
        (date(2024, 2, 29), date(2024, 2, 29)),  # hoy mismo no es posterior
        (date(2024, 2, 28), date(2020, 2, 29)),
        (date(2028, 3, 1), date(2028, 2, 29)),
    ],
)
def test_leap_day_without_year_walks_back_to_the_last_leap_year(today, expected):
    assert value_of("29 de febrero", "between_dates", today).start == expected


def test_a_day_that_never_exists_is_asked_whatever_the_clock():
    for today in CLOCKS:
        for quote in ("31 de abril", "30 de febrero", "31 de noviembre"):
            result = run(quote, "between_dates", today)
            assert (result.decision, result.detail) == ("CLARIFY", "TIME_INVALID_DATE")
    result = run("29 de febrero de 2025", "between_dates", TODAY)  # año dicho: no se busca
    assert (result.decision, result.detail) == ("CLARIFY", "TIME_INVALID_DATE")


# ------------------------------------------------------------------ rangos sin año


@pytest.mark.parametrize("today", CLOCKS)
@pytest.mark.parametrize(("first_name", "first"), MONTHS)
@pytest.mark.parametrize(("last_name", "last"), MONTHS)
def test_month_range_without_year_uses_the_year_whose_end_is_not_after_today(
    first_name, first, last_name, last, today,
):
    if first_name == last_name:
        return
    quotes = (f"de {first_name} a {last_name}", f"entre {first_name} y {last_name}")
    for quote in quotes:
        result = run(quote, "between_dates", today)
        if first > last:  # cruce de año: no se adivina
            assert (result.decision, result.detail) == ("CLARIFY", "TIME_REVERSE_RANGE")
            continue
        year = latest_month_year(last, today)
        expected = TimeValue(
            date(year, first, 1), first_of_next_month(year, last), "month_range",
            year_inferred=True,
        )
        assert result.decision == "RESOLVED" and result.value == expected, (quote, today)


def test_gold_month_range_de_enero_a_junio():
    found = value_of("de enero a junio", "between_dates", TODAY)
    assert (found.start, found.end_exclusive) == (date(2026, 1, 1), date(2026, 7, 1))
    # Si todavía no llegó julio, el rango en curso sería futuro: el año anterior.
    early = value_of("de marzo a julio", "between_dates", date(2026, 2, 10))
    assert (early.start, early.end_exclusive) == (date(2025, 3, 1), date(2025, 8, 1))


@pytest.mark.parametrize("today", CLOCKS)
@pytest.mark.parametrize(
    ("quote", "start", "end"),
    [
        ("desde el 1 de agosto hasta el 9 de septiembre", (8, 1), (9, 9)),
        ("desde 1 agosto hasta el 9 de septiembre", (8, 1), (9, 9)),
        ("desde el 15 de enero hasta el 3 de marzo", (1, 15), (3, 3)),
        ("desde el 10 de noviembre hasta el 20 de diciembre", (11, 10), (12, 20)),
        ("del 1 de abril al 30 de junio", (4, 1), (6, 30)),
        ("del 5 al 20 de octubre", (10, 5), (10, 20)),
    ],
)
def test_day_ranges_without_year_share_one_year_with_the_end_not_after_today(
    quote, start, end, today,
):
    year = latest_day_year(end[0], end[1], today)
    found = run(quote, "between_dates", today)
    assert found.decision == "RESOLVED", (quote, today, found)
    assert found.value == TimeValue(
        date(year, *start), date(year, *end) + timedelta(days=1), found.value.precision,
        year_inferred=True,
    )


@pytest.mark.parametrize(
    "quote",
    ["de noviembre a febrero", "desde noviembre hasta febrero",
     "desde el 10 de noviembre hasta el 3 de febrero", "del 20 al 5 de marzo"],
)
@pytest.mark.parametrize("today", CLOCKS)
def test_range_whose_start_is_after_its_end_is_asked_never_wrapped(quote, today):
    result = run(quote, "between_dates", today)
    assert (result.decision, result.detail) == ("CLARIFY", "TIME_REVERSE_RANGE")


@pytest.mark.parametrize(
    "quote",
    ["desde 2025-03-01 y hasta abril", "desde abril y hasta el 31 de marzo de 2026",
     "después de 2020 y antes de marzo", "desde marzo y hasta 2027"],
)
def test_a_range_mixing_an_explicit_year_with_an_implicit_one_is_asked(quote):
    # No se adivina si el año dicho gobierna también a la otra cota.
    result = run(quote, "between_dates", TODAY)
    assert (result.decision, result.detail) == ("CLARIFY", "TIME_MULTIPLE_TEMPORAL_FACTS")


@pytest.mark.parametrize("tail", ["hasta acá", "hasta aca", "hasta aquí", "hasta aqui",
                                  "hasta hoy", "HASTA ACÁ", "hasta acá."])
@pytest.mark.parametrize("today", CLOCKS)
def test_since_a_day_until_here_closes_at_today_inclusive(tail, today):
    year = latest_day_year(8, 1, today)
    found = value_of(f"desde 1 agosto {tail}", "between_dates", today)
    assert found == TimeValue(
        date(year, 8, 1), today + timedelta(days=1), "bounded", year_inferred=True,
    )


def test_until_here_with_an_explicit_start_is_closed_and_not_flagged():
    found = value_of("desde 2024 hasta hoy", "between_dates", TODAY)
    assert found == TimeValue(date(2024, 1, 1), TODAY + timedelta(days=1), "bounded")
    assert found.year_inferred is False
    # «hasta acá» sin cota abierta delante no es un periodo.
    assert run("hasta acá", "between_dates", TODAY).decision == "CLARIFY"


# ------------------------------------------------------- legado intacto (sin año)


@pytest.mark.parametrize("today", CLOCKS[:8])
def test_legacy_resolver_still_uses_the_clock_year_when_the_year_is_not_stated(today):
    found = resolve_temporal_scope("marzo", today=today)
    assert found.fecha_desde == f"{today.year}-03-01" and found.clock == "inferred_year"
    ranged = resolve_temporal_scope("de enero a junio", today=today)
    assert ranged.error_code == "missing_range_year"
    day = resolve_temporal_scope("del 5 al 20 de octubre", today=today)
    assert day.fecha_desde == f"{today.year}-10-05"
    assert resolve_temporal_scope("marzo", today=today, implicit_year=2019).fecha_desde == (
        "2019-03-01"
    )


# --------------------------------------------------------------- periodos relativos

NUMBER_WORDS = {
    1: "un", 2: "dos", 3: "tres", 4: "cuatro", 5: "cinco", 6: "seis", 7: "siete", 8: "ocho",
    9: "nueve", 10: "diez", 11: "once", 12: "doce", 15: "quince", 20: "veinte", 30: "treinta",
}
RELATIVE_CLOCKS = [date(2026, 9, 30), date(2026, 1, 1), date(2026, 3, 31), date(2024, 2, 29),
                   date(2024, 3, 31), date(2025, 12, 31), date(2026, 8, 31), date(2027, 5, 31)]


def moving(start: date, today: date, precision: str) -> TimeValue:
    return TimeValue(start, today + timedelta(days=1), precision)


@pytest.mark.parametrize("today", RELATIVE_CLOCKS)
@pytest.mark.parametrize("count", sorted(NUMBER_WORDS))
def test_trailing_windows_take_digits_or_words_and_end_today(count, today):
    spellings = (str(count), NUMBER_WORDS[count])
    for token in spellings:
        for lead in ("", "los "):
            assert value_of(f"{lead}últimos {token} meses", "relative_period", today) == moving(
                months_back(today, count), today, "trailing_months")
            assert value_of(f"{lead}ultimos {token} años", "relative_period", today) == moving(
                months_back(today, 12 * count), today, "trailing_years")
            assert value_of(f"{lead}últimos {token} días", "relative_period", today) == moving(
                today - timedelta(days=count), today, "trailing_days")
        for lead in ("", "las "):
            assert value_of(f"{lead}últimas {token} semanas", "relative_period", today) == moving(
                today - timedelta(days=7 * count), today, "trailing_weeks")


@pytest.mark.parametrize("today", RELATIVE_CLOCKS)
@pytest.mark.parametrize("count", sorted(NUMBER_WORDS))
def test_the_count_before_ultimos_is_the_same_window(count, today):
    for token in (str(count), NUMBER_WORDS[count]):
        for lead in ("", "estos ", "los "):
            assert value_of(f"{lead}{token} ultimos meses", "relative_period", today) == moving(
                months_back(today, count), today, "trailing_months")
            assert value_of(f"{lead}{token} últimos años", "relative_period", today) == moving(
                months_back(today, 12 * count), today, "trailing_years")
            assert value_of(f"{lead}{token} últimos días", "relative_period", today) == moving(
                today - timedelta(days=count), today, "trailing_days")
        for lead in ("", "estas ", "las "):
            assert value_of(f"{lead}{token} últimas semanas", "relative_period", today) == moving(
                today - timedelta(days=7 * count), today, "trailing_weeks")


@pytest.mark.parametrize("today", RELATIVE_CLOCKS)
@pytest.mark.parametrize("count", [1, 2, 3, 4, 8, 12, 20, 52])
def test_hace_n_semanas_is_the_announced_window_not_a_calendar_week(count, today):
    word = NUMBER_WORDS.get(count, str(count))
    for token in (str(count), word):
        found = value_of(f"hace {token} semanas", "relative_period", today)
        assert found == moving(today - timedelta(days=7 * count), today, "trailing_weeks")
        assert found.moving_window is True
    assert value_of("hace una semana", "relative_period", today).start == today - timedelta(7)


@pytest.mark.parametrize("today", RELATIVE_CLOCKS)
def test_el_ultimo_anio_and_mes_are_moving_windows_not_the_calendar_past(today):
    for quote in ("último año", "el último año", "el ultimo año"):
        assert value_of(quote, "relative_period", today) == moving(
            months_back(today, 12), today, "trailing_years")
    for quote in ("último mes", "el último mes", "el ultimo mes"):
        assert value_of(quote, "relative_period", today) == moving(
            months_back(today, 1), today, "trailing_months")


@pytest.mark.parametrize("today", CLOCKS)
def test_este_trimestre_and_semestre_are_the_calendar_slot_in_course(today):
    quarter_first = (today.month - 1) // 3 * 3 + 1
    semester_first = (today.month - 1) // 6 * 6 + 1
    quarter = value_of("este trimestre", "relative_period", today)
    assert quarter == TimeValue(
        date(today.year, quarter_first, 1), first_of_next_month(today.year, quarter_first + 2),
        "quarter",
    )
    semester = value_of("este semestre", "relative_period", today)
    assert semester == TimeValue(
        date(today.year, semester_first, 1), first_of_next_month(today.year, semester_first + 5),
        "semester",
    )
    assert quarter.moving_window is False and semester.moving_window is False
    assert quarter.start <= today < quarter.end_exclusive
    assert semester.start <= today < semester.end_exclusive


@pytest.mark.parametrize("today", CLOCKS)
def test_este_mes_este_ano_and_la_ultima_semana(today):
    assert value_of("este mes", "relative_period", today) == TimeValue(
        date(today.year, today.month, 1), first_of_next_month(today.year, today.month), "month")
    assert value_of("este año", "relative_period", today) == TimeValue(
        date(today.year, 1, 1), date(today.year + 1, 1, 1), "year")
    assert value_of("la última semana", "relative_period", today) == moving(
        today - timedelta(days=7), today, "trailing_weeks")


@pytest.mark.parametrize(
    ("quote", "months", "precision"),
    [("últimos 3 años", 36, "trailing_years"), ("últimos 6 meses", 6, "trailing_months"),
     ("los ultimos dos años", 24, "trailing_years"),
     ("estos dos ultimos meses", 2, "trailing_months"), ("último año", 12, "trailing_years")],
)
def test_gold_relative_phrases(quote, months, precision):
    assert value_of(quote, "relative_period", TODAY) == moving(
        months_back(TODAY, months), TODAY, precision)


def test_hace_dos_semanas_gold():
    assert value_of("hace dos semanas", "relative_period", TODAY) == TimeValue(
        date(2026, 9, 16), date(2026, 10, 1), "trailing_weeks")


@pytest.mark.parametrize(
    "quote",
    ["hace 0 semanas", "hace 53 semanas", "0 últimos meses", "121 últimos meses",
     "31 últimos años", "366 últimos días", "53 últimas semanas"],
)
def test_counts_out_of_range_are_asked(quote):
    result = run(quote, "relative_period", TODAY)
    assert (result.decision, result.detail) == ("CLARIFY", "TIME_COUNT_OUT_OF_RANGE")


@pytest.mark.parametrize(
    "quote",
    ["últimos 6 meses", "estos dos ultimos meses", "último año", "hace dos semanas",
     "este trimestre", "este semestre", "la última semana", "últimas 3 semanas"],
)
@pytest.mark.parametrize("operator", ["during_year", "during_month", "between_dates"])
def test_new_relative_forms_only_execute_under_relative_period(quote, operator):
    result = run(quote, operator, TODAY)
    assert (result.decision, result.detail) == ("CLARIFY", "TIME_OPERATOR_EVIDENCE_MISMATCH")
    assert run(quote, "relative_period", TODAY).decision == "RESOLVED"


@pytest.mark.parametrize(
    "quote",
    ["este trimestre de 2025", "hace dos semanas en marzo", "estos dos ultimos meses de 2025",
     "último año y este trimestre", "este trimestre y este semestre"],
)
def test_a_relative_form_next_to_another_period_is_never_silently_dropped(quote):
    result = run(quote, "relative_period", TODAY)
    assert result.decision == "CLARIFY" and result.value is None


@pytest.mark.parametrize("quote", ["este trimestre", "hace dos semanas", "último año"])
def test_relative_forms_do_not_survive_a_qualifier_in_the_window(quote):
    refused = run(quote, "relative_period", TODAY, left=("antes", "de"))
    assert (refused.decision, refused.detail) == ("CLARIFY", "TIME_UNCITED_QUALIFIER")


def test_relative_period_follows_the_injected_clock_only():
    first, second = date(2024, 5, 3), date(2031, 11, 20)
    for quote in ("hace dos semanas", "este trimestre", "último año", "estos dos ultimos meses"):
        assert value_of(quote, "relative_period", first) != value_of(
            quote, "relative_period", second)


# --------------------------------------------------------------------- Hypothesis

CLOCK_STRATEGY = st.dates(min_value=date(2001, 1, 1), max_value=date(2090, 12, 31))
def _month_oracle(number: int):
    def oracle(today: date):
        year = latest_month_year(number, today)
        return date(year, number, 1), first_of_next_month(year, number)
    return oracle


def _day_oracle(month: int, day: int, tail=None):
    def oracle(today: date):
        year = latest_day_year(month, day, today)
        if tail == "today":
            return date(year, 8, 1), today + timedelta(days=1)
        if tail is not None:
            return date(year, 8, 1), date(year, *tail) + timedelta(days=1)
        return date(year, month, day), date(year, month, day) + timedelta(days=1)
    return oracle


def _range_oracle(first: int, last: int):
    def oracle(today: date):
        year = latest_month_year(last, today)
        return date(year, first, 1), first_of_next_month(year, last)
    return oracle


IMPLICIT_ORACLES = {
    **{name: ("during_month", _month_oracle(number)) for name, number in MONTHS},
    "de enero a junio": ("between_dates", _range_oracle(1, 6)),
    "de marzo a octubre": ("between_dates", _range_oracle(3, 10)),
    "el 15 de marzo": ("between_dates", _day_oracle(3, 15)),
    "el 29 de febrero": ("between_dates", _day_oracle(2, 29)),
    "desde el 1 de agosto hasta el 9 de septiembre": (
        "between_dates", _day_oracle(9, 9, (9, 9))),
    "desde 1 agosto hasta acá": ("between_dates", _day_oracle(8, 1, "today")),
    "antes de mayo": (
        "between_dates", lambda today: (None, _month_oracle(5)(today)[0])),
}


@settings(max_examples=500, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(today=CLOCK_STRATEGY, quote=st.sampled_from(sorted(IMPLICIT_ORACLES)))
def test_property_an_inferred_year_is_the_latest_one_not_after_today(today, quote):
    operator, oracle = IMPLICIT_ORACLES[quote]
    found = value_of(quote, operator, today)
    assert found.year_inferred is True
    assert (found.start, found.end_exclusive) == oracle(today), (quote, today)
    assert found.start is None or found.start <= today


@settings(max_examples=300, deadline=None)
@given(
    today=CLOCK_STRATEGY,
    count=st.integers(min_value=1, max_value=30),
    unit=st.sampled_from(["meses", "años", "días", "semanas"]),
)
def test_property_trailing_window_always_ends_today_and_is_wide_enough(today, count, unit):
    ultimos = "últimas" if unit == "semanas" else "últimos"
    quote = f"{ultimos} {count} {unit}"
    result = run(quote, "relative_period", today)
    limit = {"meses": 120, "años": 30, "días": 365, "semanas": 52}[unit]
    assert (result.decision == "RESOLVED") == (count <= limit)
    if result.decision != "RESOLVED":
        assert result.detail == "TIME_COUNT_OUT_OF_RANGE"
        return
    found = result.value
    assert found is not None and found.moving_window is True and found.year_inferred is False
    assert found.end_exclusive == today + timedelta(days=1)
    assert found.start is not None and found.start < today
    # Misma ventana escrita con el número antes de «últimos».
    assert run(f"{count} {ultimos} {unit}", "relative_period", today).value == found


@settings(max_examples=200, deadline=None)
@given(today=CLOCK_STRATEGY, year=st.integers(min_value=2000, max_value=2099))
def test_property_explicit_year_ignores_the_clock(today, year):
    expected = TimeValue(date(year, 5, 1), date(year, 6, 1), "month")
    assert value_of(f"mayo de {year}", "during_month", today) == expected


def test_leap_day_search_reaches_back_the_full_gap_between_leap_years():
    # 2100 no es bisiesto: desde 2103 el último 29 de febrero está siete años atrás.
    assert value_of("29 de febrero", "between_dates", date(2103, 3, 1)).start == date(2096, 2, 29)
    assert value_of("29 de febrero", "between_dates", date(2100, 2, 28)).start == date(2096, 2, 29)
