"""M1-D-TIME: la gramática de la ruta de CITA no reescribe periodos en silencio (A2 §4).

Estos casos vivían en ``tests/test_time_semantics_v2.py`` contra ``temporal_semantics``. Esa
autoridad legada vuelve byte-idéntica a ``main`` (paridad temporal, opción A); la gramática
reescrita vive en ``app/core/time_grammar.py`` y estos casos la fijan directamente.
"""
from datetime import date

import pytest

from app.core.time_grammar import resolve_temporal_scope

CLOCK = date(2026, 8, 25)


# --- M1-D-TIME: bugs silenciosos de la autoridad (A2 §4) --------------------------
# Cada fila fallaba en la base 4ec99bc1: devolvía otro período sin avisar.
@pytest.mark.parametrize(
    ("query", "desde", "hasta", "precision"),
    [
        ("septiembre del 2025", "2025-09-01", "2025-09-30", "month"),
        ("en marzo, 2025", "2025-03-01", "2025-03-31", "month"),
        ("marzo-2025", "2025-03-01", "2025-03-31", "month"),
        ("después de 2022", "2023-01-01", None, "open_from"),
        ("despues de 2022", "2023-01-01", None, "open_from"),
        ("antes de 2024", None, "2023-12-31", "open_until"),
        ("a partir de 2023", "2023-01-01", None, "open_from"),
        ("posteriores a 2022", "2023-01-01", None, "open_from"),
        ("de 2024 a la fecha", "2024-01-01", None, "open_from"),
        ("2024 en adelante", "2024-01-01", None, "open_from"),
        ("desde 2024 hasta hoy", "2024-01-01", None, "open_from"),
        ("desde el año pasado", "2025-01-01", None, "open_from"),
        ("hasta el año pasado", None, "2025-12-31", "open_until"),
        ("antes de marzo", None, "2026-02-28", "open_until"),
        ("después de marzo de 2025", "2025-04-01", None, "open_from"),
        ("antes del 15 de marzo de 2025", None, "2025-03-14", "open_until"),
        ("desde el 1 de marzo de 2025", "2025-03-01", None, "open_from"),
        ("hasta el 15 de marzo de 2025", None, "2025-03-15", "open_until"),
        ("antes de 2025-03-01", None, "2025-02-28", "open_until"),
        ("primer trimestre de 2025", "2025-01-01", "2025-03-31", "quarter"),
        ("segundo semestre de 2025", "2025-07-01", "2025-12-31", "semester"),
        ("último trimestre de 2025", "2025-10-01", "2025-12-31", "quarter"),
        ("primer trimestre 2025", "2025-01-01", "2025-03-31", "quarter"),
        ("el mes de marzo de 2025", "2025-03-01", "2025-03-31", "month"),
        ("el día 15 de marzo de 2025", "2025-03-15", "2025-03-15", "day"),
        ("del 1 al 15 de marzo de 2025", "2025-03-01", "2025-03-15", "day_range"),
        ("entre el 1 y el 15 de marzo de 2025", "2025-03-01", "2025-03-15", "day_range"),
        ("últimos 6 meses", "2026-02-25", "2026-08-25", "trailing_months"),
        ("los últimos 3 años", "2023-08-25", "2026-08-25", "trailing_years"),
        ("hace 2 años", "2024-01-01", "2024-12-31", "year"),
        ("hace dos meses", "2026-06-01", "2026-06-30", "month"),
        ("la última semana", "2026-08-18", "2026-08-25", "trailing_weeks"),
    ],
)
def test_comparators_quarters_and_trailing_periods_are_not_silently_rewritten(
    query, desde, hasta, precision,
):
    result = resolve_temporal_scope(query, today=CLOCK)

    assert (result.fecha_desde, result.fecha_hasta, result.precision) == (desde, hasta, precision)
    assert result.error_code is None


def test_comparator_is_part_of_the_masked_span():
    # El consumidor enmascara el span: «después de» no puede quedar como tema.
    query = "contratos de aseo después de 2022"
    result = resolve_temporal_scope(query, today=CLOCK)

    assert query[slice(*result.span)] == "después de 2022"


def test_year_less_quarter_and_unresolved_expressions_still_ask():
    for query in ("primer trimestre", "este trimestre", "último año"):
        result = resolve_temporal_scope(query, today=CLOCK)
        assert result.error_code == "unsupported_temporal_expression"


def test_trailing_months_and_years_clamp_to_leap_day():
    leap = date(2024, 2, 29)

    assert resolve_temporal_scope("últimos 12 meses", today=leap).fecha_desde == "2023-02-28"
    assert resolve_temporal_scope("los últimos 3 años", today=leap).fecha_desde == "2021-02-28"
    assert resolve_temporal_scope("los últimos 4 años", today=leap).fecha_desde == "2020-02-29"
