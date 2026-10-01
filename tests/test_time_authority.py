"""M1-D-TIME: `resolve_time_condition` y las correcciones de la autoridad temporal.

El léxico es CERRADO (12 meses, abreviaturas, marcadores de cota y rango), así que
las propiedades lo recorren entero; el oráculo calcula las fechas con aritmética
de calendario propia del test, nunca con la tabla del módulo bajo prueba.
"""
from __future__ import annotations

import inspect
from datetime import date

import pytest
from hypothesis import HealthCheck, given, settings, strategies as st

from app.core import temporal_semantics
from app.core.temporal_semantics import resolve_temporal_scope
from app.core.time_authority import AUTHORITY, TimeResolution, resolve_time_condition

TODAY = date(2026, 8, 25)
POS = "POSITIVE"

MONTHS = [
    ("enero", 1), ("febrero", 2), ("marzo", 3), ("abril", 4), ("mayo", 5), ("junio", 6),
    ("julio", 7), ("agosto", 8), ("septiembre", 9), ("octubre", 10), ("noviembre", 11),
    ("diciembre", 12),
]
ABBREVIATIONS = [
    ("ene", 1), ("feb", 2), ("mar", 3), ("abr", 4), ("may", 5), ("jun", 6), ("jul", 7),
    ("ago", 8), ("sep", 9), ("sept", 9), ("oct", 10), ("nov", 11), ("dic", 12),
]
YEAR_JOINS = [" de ", " del ", ", ", " ", "-", "/", " , "]


def month_bounds(year: int, month: int) -> tuple[date, date]:
    """[primer día, primer día del mes siguiente) por aritmética propia del test."""
    start = date(year, month, 1)
    nxt = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    return start, nxt


def run(quote: str, operator: str, today: date = TODAY, polarity: str = POS) -> TimeResolution:
    return resolve_time_condition(quote, operator, polarity, today=today)


def resolved(quote: str, operator: str, today: date = TODAY) -> tuple[date | None, date | None]:
    result = run(quote, operator, today)
    assert result.decision == "RESOLVED", (quote, operator, result)
    assert result.authority == AUTHORITY == "time-authority@1"
    return result.fecha_desde, result.fecha_hasta_exclusive


def refused(quote: str, operator: str, decision: str, reason: str, today: date = TODAY) -> None:
    result = run(quote, operator, today)
    assert (result.decision, result.reason) == (decision, reason), (quote, operator, result)
    assert result.fecha_desde is None and result.fecha_hasta_exclusive is None


# --------------------------------------------------------------------------- meses


@pytest.mark.parametrize("year", [2024, 2025])
@pytest.mark.parametrize("join", YEAR_JOINS)
@pytest.mark.parametrize(("name", "number"), MONTHS + ABBREVIATIONS)
def test_every_month_form_binds_its_own_year_never_the_clock_year(name, number, join, year):
    """12 meses + abreviaturas × «de/del/,/ /-//»: el año de la cita gana al del reloj."""
    expected = month_bounds(year, number)
    assert resolved(f"{name}{join}{year}", "during_month") == expected
    assert resolved(f"en {name}{join}{year}", "during_month") == expected
    assert resolve_temporal_scope(f"{name}{join}{year}", today=TODAY).fecha_desde == (
        expected[0].isoformat()
    )


def test_september_del_2025_is_not_september_2026():
    # A2 TIME-006: `month_re` no aceptaba «del <año>» y devolvía el año del reloj.
    assert resolved("septiembre del 2025", "during_month") == (date(2025, 9, 1), date(2025, 10, 1))


@pytest.mark.parametrize(("name", "number"), MONTHS + ABBREVIATIONS)
def test_month_without_year_asks_instead_of_inventing_the_clock_year(name, number):
    refused(name, "during_month", "CLARIFY", "TIME_YEAR_NOT_STATED")
    refused(f"en {name}", "between_dates", "CLARIFY", "TIME_YEAR_NOT_STATED")
    refused(f"antes de {name}", "between_dates", "CLARIFY", "TIME_YEAR_NOT_STATED")


# ---------------------------------------------------------------- cotas (comparadores)

# marcador -> familia. Cubre TODOS los marcadores cerrados de temporal_bounds.
FROM_MARKERS = ["desde", "desde el", "a partir de", "a partir del", "A PARTIR DE"]
AFTER_MARKERS = [
    "después de", "despues de", "después del", "posteriores a", "posterior a",
    "posteriores al", "luego de", "tras", "con posterioridad a",
]
BEFORE_MARKERS = [
    "antes de", "antes del", "anteriores a", "anterior a", "previos a", "previo a",
    "con anterioridad a",
]
UNTIL_MARKERS = ["hasta", "hasta el", "hasta la"]

UNITS = {
    # texto -> (primer día incluido, primer día posterior) del período citado
    "2024": (date(2024, 1, 1), date(2025, 1, 1)),
    "2022": (date(2022, 1, 1), date(2023, 1, 1)),
    "marzo de 2024": (date(2024, 3, 1), date(2024, 4, 1)),
    "diciembre de 2023": (date(2023, 12, 1), date(2024, 1, 1)),
    "enero de 2025": (date(2025, 1, 1), date(2025, 2, 1)),
    "el 15 de marzo de 2024": (date(2024, 3, 15), date(2024, 3, 16)),
    "el 29 de febrero de 2024": (date(2024, 2, 29), date(2024, 3, 1)),
    "el 31 de diciembre de 2023": (date(2023, 12, 31), date(2024, 1, 1)),
}


def _unit_text(marker: str, unit: str) -> str:
    # «hasta el» + «el 15 de …» sería «hasta el el»: se omite el artículo duplicado.
    if unit.startswith("el ") and marker.split()[-1] in {"el", "del", "la"}:
        return unit[3:]
    return unit


@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("marker", FROM_MARKERS)
def test_from_markers_include_the_cited_period(marker, unit):
    first, _ = UNITS[unit]
    assert resolved(f"{marker} {_unit_text(marker, unit)}", "between_dates") == (first, None)


@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("marker", AFTER_MARKERS)
def test_after_markers_exclude_the_cited_period(marker, unit):
    _, after = UNITS[unit]
    assert resolved(f"{marker} {_unit_text(marker, unit)}", "between_dates") == (after, None)


@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("marker", BEFORE_MARKERS)
def test_before_markers_exclude_the_cited_period(marker, unit):
    first, _ = UNITS[unit]
    assert resolved(f"{marker} {_unit_text(marker, unit)}", "between_dates") == (None, first)


@pytest.mark.parametrize("unit", UNITS)
@pytest.mark.parametrize("marker", UNTIL_MARKERS)
def test_until_markers_include_the_cited_period(marker, unit):
    _, after = UNITS[unit]
    assert resolved(f"{marker} {_unit_text(marker, unit)}", "between_dates") == (None, after)


@pytest.mark.parametrize("suffix", ["en adelante", "a la fecha", "hacia adelante", "hasta hoy",
                                    "hasta la fecha", "al presente"])
@pytest.mark.parametrize("prefix", ["de ", "", "desde "])
def test_year_with_open_suffix_is_an_open_from_bound(prefix, suffix):
    assert resolved(f"{prefix}2024 {suffix}", "between_dates") == (date(2024, 1, 1), None)


@pytest.mark.parametrize(
    "quote",
    ["después de 2022", "antes de 2024", "a partir de 2023", "posteriores a 2022",
     "de 2024 a la fecha", "desde 2023", "hasta 2025", "después de marzo de 2025"],
)
@pytest.mark.parametrize("operator", ["during_year", "during_month", "relative_period"])
def test_bound_quotes_never_execute_under_a_period_operator(quote, operator):
    # A2 TIME-056..059: during_year + «antes de 2024» ejecutaba el AÑO 2024.
    refused(quote, operator, "CLARIFY", "TIME_OPERATOR_EVIDENCE_MISMATCH")


@pytest.mark.parametrize("quote", ["desde 2023", "hasta 2025"])
def test_open_bound_is_none_not_a_sentinel_date(quote):
    desde, hasta = resolved(quote, "between_dates")
    assert (desde is None) != (hasta is None)


def test_composite_opposite_bounds_resolve_in_either_order():
    expected = (date(2021, 1, 1), date(2024, 1, 1))
    assert resolved("después de 2020 y antes de 2024", "between_dates") == expected
    assert resolved("antes de 2024 y después de 2020", "between_dates") == expected
    assert resolved("desde 2021 y hasta 2023", "between_dates") == expected


@pytest.mark.parametrize(
    "quote",
    ["después de 2020 y después de 2022", "antes de 2020 y antes de 2022",
     "después de 2024 y antes de 2020", "marzo de 2025 y 2026", "2025 y 2027",
     "primer semestre de 2025 y 2026"],
)
def test_two_temporal_facts_that_do_not_form_bounds_are_never_silently_dropped(quote):
    result = run(quote, "between_dates")
    assert result.decision == "CLARIFY"
    assert result.reason in {
        "TIME_MULTIPLE_TEMPORAL_FACTS", "TIME_REVERSE_RANGE", "TIME_RANGE_OR_LIST_AMBIGUOUS",
    }


# ------------------------------------------------------------------- rangos y trimestres


@pytest.mark.parametrize(("name", "number"), MONTHS)
def test_single_month_day_range_uses_both_days(name, number):
    # A2 TIME-016: «del 1 al 15 de marzo de 2025» devolvía sólo el día 15.
    start = date(2025, number, 1)
    for quote in (f"del 1 al 15 de {name} de 2025", f"entre el 1 y el 15 de {name} de 2025",
                  f"del 1 al 15 de {name} del 2025"):
        assert resolved(quote, "between_dates") == (start, date(2025, number, 16))


def test_single_month_day_range_edges():
    assert resolved("del 1 al 29 de febrero de 2024", "between_dates") == (
        date(2024, 2, 1), date(2024, 3, 1))
    refused("del 1 al 29 de febrero de 2025", "between_dates", "CLARIFY", "TIME_INVALID_DATE")
    refused("del 20 al 5 de marzo de 2025", "between_dates", "CLARIFY", "TIME_REVERSE_RANGE")
    refused("del 1 al 15 de marzo", "between_dates", "CLARIFY", "TIME_YEAR_NOT_STATED")


@pytest.mark.parametrize(
    ("ordinal", "slot"),
    [("primer", 1), ("segundo", 2), ("tercer", 3), ("cuarto", 4), ("último", 4), ("ultimo", 4),
     ("1er", 1), ("2do", 2), ("3er", 3), ("4to", 4)],
)
@pytest.mark.parametrize("join", [" de ", " del "])
def test_every_quarter_names_only_its_three_months(ordinal, slot, join):
    first = (slot - 1) * 3 + 1
    expected = (date(2025, first, 1), month_bounds(2025, first + 2)[1])
    assert resolved(f"{ordinal} trimestre{join}2025", "between_dates") == expected


@pytest.mark.parametrize(
    ("ordinal", "slot"),
    [("primer", 1), ("segundo", 2), ("último", 2), ("1er", 1), ("2do", 2)],
)
def test_every_semester_names_only_its_six_months(ordinal, slot):
    expected = (date(2025, 1 + 6 * (slot - 1), 1), date(2025 + slot // 2, 1 + 6 * (slot % 2), 1))
    assert resolved(f"{ordinal} semestre de 2025", "between_dates") == expected


@pytest.mark.parametrize("quote", ["primer trimestre", "este trimestre", "tercer semestre de 2025",
                                   "quinto trimestre de 2025"])
def test_quarter_without_year_or_out_of_range_is_not_resolved(quote):
    result = run(quote, "between_dates")
    assert result.decision == "CLARIFY"


def test_quarter_is_not_the_whole_year_under_during_year():
    # A2 TIME-041/042: «primer trimestre de 2025» ejecutaba todo 2025.
    refused("primer trimestre de 2025", "during_year", "CLARIFY", "TIME_OPERATOR_EVIDENCE_MISMATCH")


@pytest.mark.parametrize(
    ("quote", "expected"),
    [
        ("entre enero y marzo de 2026", (date(2026, 1, 1), date(2026, 4, 1))),
        ("enero-marzo de 2026", (date(2026, 1, 1), date(2026, 4, 1))),
        ("de enero a marzo de 2026", (date(2026, 1, 1), date(2026, 4, 1))),
        ("entre 2022 y 2024", (date(2022, 1, 1), date(2025, 1, 1))),
        ("de 2022 a 2024", (date(2022, 1, 1), date(2025, 1, 1))),
        ("desde 2022 hasta 2024", (date(2022, 1, 1), date(2025, 1, 1))),
        ("2022-2024", (date(2022, 1, 1), date(2025, 1, 1))),
        ("2025-03-01 a 2025-03-31", (date(2025, 3, 1), date(2025, 4, 1))),
        ("entre el 1 de febrero y el 15 de marzo de 2026", (date(2026, 2, 1), date(2026, 3, 16))),
    ],
)
def test_explicit_ranges(quote, expected):
    assert resolved(quote, "between_dates") == expected


@pytest.mark.parametrize("quote", ["enero y marzo de 2026", "2023 y 2025", "de 2023 y 2025",
                                   "1 de marzo y 15 de abril de 2025"])
def test_bare_y_is_a_list_or_a_range_never_a_silent_range(quote):
    # Legacy convertía «2023 y 2025» en el rango 2023..2025 (incluye 2024).
    refused(quote, "between_dates", "CLARIFY", "TIME_RANGE_OR_LIST_AMBIGUOUS")


# ------------------------------------------------------------------- reloj inyectado


@pytest.mark.parametrize(
    ("quote", "operator", "today", "expected"),
    [
        ("este año", "relative_period", date(2026, 1, 1), (date(2026, 1, 1), date(2027, 1, 1))),
        ("este año", "during_year", date(2026, 12, 31), (date(2026, 1, 1), date(2027, 1, 1))),
        ("el año pasado", "relative_period", date(2026, 1, 1), (date(2025, 1, 1), date(2026, 1, 1))),
        ("el mes pasado", "relative_period", date(2026, 1, 15), (date(2025, 12, 1), date(2026, 1, 1))),
        ("el mes pasado", "during_month", date(2026, 3, 31), (date(2026, 2, 1), date(2026, 3, 1))),
        ("este mes", "during_month", date(2024, 2, 29), (date(2024, 2, 1), date(2024, 3, 1))),
        ("hace 2 años", "relative_period", date(2026, 1, 1), (date(2024, 1, 1), date(2025, 1, 1))),
        ("hace dos años", "during_year", date(2024, 2, 29), (date(2022, 1, 1), date(2023, 1, 1))),
        ("hace un año", "relative_period", date(2026, 1, 1), (date(2025, 1, 1), date(2026, 1, 1))),
        ("hace 3 meses", "relative_period", date(2026, 1, 31), (date(2025, 10, 1), date(2025, 11, 1))),
        ("hace dos meses", "during_month", date(2026, 8, 25), (date(2026, 6, 1), date(2026, 7, 1))),
        ("últimos 6 meses", "relative_period", date(2026, 8, 25), (date(2026, 2, 25), date(2026, 8, 26))),
        ("los últimos 6 meses", "relative_period", date(2026, 1, 1), (date(2025, 7, 1), date(2026, 1, 2))),
        ("los últimos seis meses", "relative_period", date(2026, 1, 1), (date(2025, 7, 1), date(2026, 1, 2))),
        # bisiesto: el día 29 se acota al último día del mes destino
        ("últimos 12 meses", "relative_period", date(2024, 2, 29), (date(2023, 2, 28), date(2024, 3, 1))),
        ("los últimos 3 años", "relative_period", date(2024, 2, 29), (date(2021, 2, 28), date(2024, 3, 1))),
        ("los últimos 4 años", "relative_period", date(2024, 2, 29), (date(2020, 2, 29), date(2024, 3, 1))),
        ("los últimos 3 años", "relative_period", date(2026, 8, 25), (date(2023, 8, 25), date(2026, 8, 26))),
        ("la última semana", "relative_period", date(2026, 8, 25), (date(2026, 8, 18), date(2026, 8, 26))),
        ("últimos 30 días", "relative_period", date(2026, 1, 10), (date(2025, 12, 11), date(2026, 1, 11))),
        ("esta semana", "relative_period", date(2026, 8, 25), (date(2026, 8, 24), date(2026, 8, 31))),
        ("ayer", "relative_period", date(2026, 1, 1), (date(2025, 12, 31), date(2026, 1, 1))),
        ("desde el año pasado", "between_dates", date(2026, 1, 1), (date(2025, 1, 1), None)),
        ("hasta el año pasado", "between_dates", date(2026, 8, 25), (None, date(2026, 1, 1))),
        ("desde el mes pasado", "relative_period", date(2026, 1, 20), (date(2025, 12, 1), None)),
    ],
)
def test_relative_periods_use_only_the_injected_clock(quote, operator, today, expected):
    assert resolved(quote, operator, today) == expected


def test_relative_period_moves_with_the_clock_and_explicit_does_not():
    first, second = date(2024, 5, 3), date(2031, 11, 20)
    assert resolved("últimos 6 meses", "relative_period", first) != resolved(
        "últimos 6 meses", "relative_period", second)
    assert resolved("en 2025", "during_year", first) == resolved("en 2025", "during_year", second)


@pytest.mark.parametrize("quote", ["hace 0 años", "hace 31 años", "últimos 0 meses",
                                   "últimos 121 meses", "los últimos 31 años", "hace 121 meses"])
def test_out_of_range_counts_are_asked(quote):
    result = run(quote, "relative_period")
    assert (result.decision, result.reason) == ("CLARIFY", "TIME_COUNT_OUT_OF_RANGE")


def test_clock_is_mandatory_and_never_read_from_the_machine(monkeypatch):
    parameter = inspect.signature(resolve_time_condition).parameters["today"]
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is parameter.empty
    with pytest.raises(TypeError):
        resolve_time_condition("este mes", "relative_period", POS)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        resolve_time_condition("este mes", "relative_period", POS, today="2026-08-25")  # type: ignore[arg-type]

    class NoToday(date):
        @classmethod
        def today(cls):
            raise AssertionError("date.today() was read")

    monkeypatch.setattr(temporal_semantics, "Date", NoToday)
    assert resolved("hace 3 meses", "relative_period") == (date(2026, 5, 1), date(2026, 6, 1))


# ------------------------------------------------------------------------- negativos


@pytest.mark.parametrize("quote", ["2025", "marzo de 2025", "desde 2023", "este mes", "basura"])
@pytest.mark.parametrize("operator", ["during_year", "during_month", "between_dates",
                                      "relative_period"])
def test_negative_polarity_is_blocked_for_every_operator(quote, operator):
    result = run(quote, operator, polarity="NEGATIVE")
    assert (result.decision, result.reason) == ("BLOCK", "TIME_NEGATION_UNSUPPORTED")
    assert result.fecha_desde is None and result.fecha_hasta_exclusive is None


@pytest.mark.parametrize("polarity", ["", "positive", "neutral", None])
def test_unknown_polarity_is_blocked_not_assumed_positive(polarity):
    result = run("2025", "during_year", polarity=polarity)  # type: ignore[arg-type]
    assert (result.decision, result.reason) == ("BLOCK", "TIME_POLARITY_INVALID")


@pytest.mark.parametrize("operator", ["", "during_day", "DURING_YEAR", "equals", "greater_than"])
def test_unknown_operator_is_blocked(operator):
    refused_result = run("2025", operator)
    assert (refused_result.decision, refused_result.reason) == ("BLOCK", "TIME_OPERATOR_UNKNOWN")


# Cita representativa de cada forma y los operadores que le corresponden.
SHAPES = {
    "explicit_year": ("en 2025", {"during_year"}),
    "explicit_month": ("marzo de 2025", {"during_month"}),
    "explicit_day": ("el 15 de marzo de 2025", {"between_dates"}),
    "explicit_range": ("entre enero y marzo de 2025", {"between_dates"}),
    "explicit_open": ("desde 2023", {"between_dates"}),
    "quarter": ("primer trimestre de 2025", {"between_dates"}),
    "relative_trailing": ("últimos 6 meses", {"relative_period"}),
    "relative_week": ("esta semana", {"relative_period"}),
    "relative_year": ("este año", {"relative_period", "during_year"}),
    "relative_month": ("el mes pasado", {"relative_period", "during_month"}),
}


@pytest.mark.parametrize("operator", ["during_year", "during_month", "between_dates",
                                      "relative_period"])
@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_operator_must_be_what_the_quote_says(shape, operator):
    quote, accepted = SHAPES[shape]
    if operator in accepted:
        assert run(quote, operator).decision == "RESOLVED"
    else:
        refused(quote, operator, "CLARIFY", "TIME_OPERATOR_EVIDENCE_MISMATCH")


@pytest.mark.parametrize("quote", ["hoy", "ahora", "Ahora.", "actualmente", "en este momento",
                                   "en la actualidad", "ahorita"])
@pytest.mark.parametrize("operator", ["relative_period", "between_dates"])
def test_deictics_are_not_time_conditions(quote, operator):
    # M1.0 §11 C3.
    refused(quote, operator, "BLOCK", "TIME_DEICTIC_NOT_CONDITION")


@pytest.mark.parametrize("quote", ["", "   ", "contratos", "aseo", "mil millones", "1999",
                                   "2000 millones"])
def test_no_temporal_evidence_is_asked_not_defaulted(quote):
    refused(quote, "during_year", "CLARIFY", "TIME_NO_TEMPORAL_EVIDENCE")


@pytest.mark.parametrize(
    ("quote", "reason"),
    [
        ("del 31 de febrero de 2025", "TIME_INVALID_DATE"),
        ("2025-02-30", "TIME_INVALID_DATE"),
        ("entre 2025 y 2023", "TIME_REVERSE_RANGE"),
        ("entre marzo y enero de 2025", "TIME_REVERSE_RANGE"),
        ("entre enero y marzo", "TIME_YEAR_NOT_STATED"),
        ("este trimestre", "TIME_UNSUPPORTED_EXPRESSION"),
    ],
)
def test_invalid_expressions_are_asked(quote, reason):
    refused(quote, "between_dates", "CLARIFY", reason)


def test_authority_reads_only_the_quote():
    # Mismo texto alrededor, misma cita: el resultado no depende de nada más.
    assert run("2025", "during_year") == run("2025", "during_year")
    # Un año fuera de la cita no puede entrar: la cita es la única entrada textual.
    parameters = list(inspect.signature(resolve_time_condition).parameters)
    assert parameters == ["quote", "operator", "polarity", "today"]


def test_time_resolution_is_frozen_and_carries_its_authority():
    result = run("en 2025", "during_year")
    assert isinstance(result, TimeResolution) and result.authority == "time-authority@1"
    with pytest.raises(AttributeError):
        result.decision = "BLOCK"  # type: ignore[misc]


# -------------------------------------------------------------------- Hypothesis

OPERATOR_STRATEGY = st.sampled_from(
    ["during_year", "during_month", "between_dates", "relative_period", "other"])
POLARITY_STRATEGY = st.sampled_from(["POSITIVE", "NEGATIVE", "x"])
TOKENS = ["2025", "2024", "marzo", "de", "del", "antes", "después", "desde", "hasta", "y", "a",
          "entre", "este", "mes", "año", "últimos", "6", "meses", "primer", "trimestre", "hoy",
          "15", "1", "al", "-", ",", "pasado", "hace", "dos", "semana", "2023-01-05"]


@settings(max_examples=400, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    words=st.lists(st.sampled_from(TOKENS), min_size=0, max_size=8),
    operator=OPERATOR_STRATEGY,
    polarity=POLARITY_STRATEGY,
    today=st.dates(min_value=date(2000, 1, 1), max_value=date(2099, 12, 31)),
)
def test_property_total_function_with_consistent_output(words, operator, polarity, today):
    result = resolve_time_condition(" ".join(words), operator, polarity, today=today)
    assert result.authority == "time-authority@1"
    if result.decision == "RESOLVED":
        assert polarity == "POSITIVE" and operator != "other"
        assert result.reason is None and result.precision
        assert result.fecha_desde is not None or result.fecha_hasta_exclusive is not None
        if result.fecha_desde and result.fecha_hasta_exclusive:
            assert result.fecha_desde < result.fecha_hasta_exclusive
    else:
        assert result.reason and result.reason.startswith("TIME_")
        assert result.fecha_desde is None and result.fecha_hasta_exclusive is None
    if polarity == "NEGATIVE":
        assert (result.decision, result.reason) == ("BLOCK", "TIME_NEGATION_UNSUPPORTED")


@settings(max_examples=300, deadline=None)
@given(
    year=st.integers(min_value=2000, max_value=2098),
    today=st.dates(min_value=date(2000, 1, 1), max_value=date(2099, 12, 31)),
    join=st.sampled_from(YEAR_JOINS),
    name=st.sampled_from([name for name, _ in MONTHS]),
)
def test_property_explicit_period_is_independent_of_the_clock(year, today, join, name):
    number = dict(MONTHS)[name]
    assert run(f"{name}{join}{year}", "during_month", today) == run(
        f"{name}{join}{year}", "during_month", date(2026, 8, 25))
    assert resolved(f"{name}{join}{year}", "during_month", today) == month_bounds(year, number)
    assert resolved(f"en {year}", "during_year", today) == (
        date(year, 1, 1), date(year + 1, 1, 1))


@settings(max_examples=300, deadline=None)
@given(year=st.integers(min_value=2000, max_value=2098))
def test_property_after_and_before_partition_the_year_line(year):
    after = resolved(f"después de {year}", "between_dates")
    before = resolved(f"antes de {year + 1}", "between_dates")
    same_year = resolved(f"en {year}", "during_year")
    assert after[0] == same_year[1] and after[1] is None
    assert before[0] is None and before[1] == same_year[1]
    from_year = resolved(f"desde {year}", "between_dates")
    assert from_year[0] == same_year[0]
