from datetime import date

import pytest

from app.core.temporal_semantics import resolve_temporal_scope
from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params
from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.response_policy import _build_interpretation
from app.core.soql_builder import SoQLBuilder


CLOCK = date(2026, 8, 25)


@pytest.mark.parametrize(
    ("query", "desde", "hasta"),
    [
        ("en junio de 2026", "2026-06-01", "2026-06-30"),
        ("de junio", "2026-06-01", "2026-06-30"),
        ("del mes pasado", "2026-07-01", "2026-07-31"),
        ("del año pasado", "2025-01-01", "2025-12-31"),
        ("del 15 de junio de 2026", "2026-06-15", "2026-06-15"),
        ("entre enero y marzo de 2026", "2026-01-01", "2026-03-31"),
        (
            "entre el 1 de febrero y el 15 de marzo de 2026",
            "2026-02-01",
            "2026-03-15",
        ),
        ("desde 2025", "2025-01-01", None),
        ("hasta 2025", None, "2025-12-31"),
        ("en 2025", "2025-01-01", "2025-12-31"),
        ("esta semana", "2026-08-24", "2026-08-30"),
        ("la semana pasada", "2026-08-17", "2026-08-23"),
        ("las ultimas 2 semanas", "2026-08-11", "2026-08-25"),
    ],
)
def test_authoritative_temporal_resolution(query, desde, hasta):
    result = resolve_temporal_scope(query, today=CLOCK)

    assert result.error_code is None
    assert result.fecha_desde == desde
    assert result.fecha_hasta == hasta


def test_month_without_year_uses_current_civil_year_even_when_future():
    result = resolve_temporal_scope("en diciembre", today=date(2026, 1, 10))

    assert result.fecha_desde == "2026-12-01"
    assert result.fecha_hasta == "2026-12-31"


def test_previous_month_crosses_year_boundary():
    result = resolve_temporal_scope("del mes pasado", today=date(2026, 1, 10))

    assert result.fecha_desde == "2025-12-01"
    assert result.fecha_hasta == "2025-12-31"


@pytest.mark.parametrize(
    ("query", "error"),
    [
        ("del 31 de febrero de 2026", "invalid_date"),
        ("entre marzo y enero de 2026", "reverse_range"),
        ("entre enero y marzo", "missing_range_year"),
        ("durante el segundo trimestre", "unsupported_temporal_expression"),
    ],
)
def test_unsafe_temporal_scope_requests_clarification(query, error):
    result = resolve_temporal_scope(query, today=CLOCK)

    assert result.error_code == error
    assert result.clarification


def test_leap_year_month_end_is_calendar_derived():
    leap = resolve_temporal_scope("febrero de 2024", today=CLOCK)
    common = resolve_temporal_scope("febrero de 2025", today=CLOCK)

    assert leap.fecha_hasta == "2024-02-29"
    assert common.fecha_hasta == "2025-02-28"


@pytest.mark.parametrize(
    "query",
    [
        "contratos de aseo desde 2025",
        "contratos de aseo hasta 2025",
        "contratos de aseo de los ultimos 30 dias",
        "contratos de aseo de las ultimas 2 semanas",
    ],
)
def test_temporal_spans_never_leak_into_object(query):
    params = QueryRouter(entity_resolver=None).parse(query, today=CLOCK).params
    object_terms = [str(value) for value in params.get("objeto", [])]

    assert object_terms == ["aseo"]
    assert not ({"desde", "hasta", "dias", "vias", "semanas", "mes", "ano"}
                & set(object_terms))


def test_negative_year_is_not_consumed_as_positive_time():
    parsed = QueryRouter(entity_resolver=None).parse(
        "contratos de aseo menos los de 2025",
        today=CLOCK,
    )

    assert "fecha_desde" not in parsed.params
    assert "fecha_hasta" not in parsed.params
    assert len(parsed.negation_requests) == 1
    assert parsed.negation_requests[0].dimension.value == "TIME"


def test_reference_number_with_month_words_is_not_promoted_to_date():
    params = QueryRouter(entity_resolver=None).parse(
        "contratos con referencia 15 de junio",
        today=CLOCK,
    ).params

    assert "fecha_desde" not in params
    assert "fecha_hasta" not in params


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        (
            "contratos de aseo del 15 de junio de 2026 por mas de 500 millones",
            {"fecha_desde": "2026-06-15", "valor_min": 500_000_000},
        ),
        (
            "contratos de aseo entre enero y marzo de 2026 en cucuta",
            {"fecha_desde": "2026-01-01", "departamento_resolved": "Norte de Santander"},
        ),
        (
            "contratos de aseo en marzo de 2026 por licitacion publica",
            {"fecha_desde": "2026-03-01"},
        ),
        (
            "contratos de aseo en marzo de 2026 de mayor valor",
            {"fecha_desde": "2026-03-01", "ordering_signal": "valor_desc"},
        ),
    ],
)
def test_temporal_parser_composes_with_orthogonal_dimensions(query, expected):
    params = QueryRouter().parse(query, today=CLOCK).params

    for field, value in expected.items():
        assert params.get(field) == value


def test_money_year_shape_is_not_a_temporal_year():
    params = QueryRouter(entity_resolver=None).parse(
        "contratos de aseo por 2000 millones",
        today=CLOCK,
    ).params

    assert "fecha_desde" not in params
    assert "fecha_hasta" not in params


def test_invalid_date_blocks_query_plan_without_partial_dates():
    parsed = QueryRouter(entity_resolver=None).parse(
        "contratos de aseo del 31 de febrero de 2026",
        today=CLOCK,
    )
    plan = compute_query_plan(parsed.params, "del 31 de febrero de 2026", {"today": CLOCK})

    assert plan.should_execute is False
    assert plan.reason == "invalid_temporal_scope"
    assert "fecha_desde" not in plan.params
    assert "fecha_hasta" not in plan.params


def test_followup_temporal_scope_replaces_both_endpoints_atomically():
    router = QueryRouter(entity_resolver=None)
    previous = router.parse(
        "contratos de mantenimiento desde 2025",
        today=CLOCK,
    ).params
    current = router.parse("hasta 2026", today=CLOCK).params

    intent, merged = detect_and_merge(
        "hasta 2026",
        current,
        frame_from_params(previous),
    )

    # "hasta 2026" es un follow-up de solo año: el motor lo clasifica como
    # change_year. Lo que este caso protege no es el nombre del intent sino la
    # atomicidad: el scope previo se reemplaza entero, no se mezclan extremos.
    assert intent == "change_year"
    assert "fecha_desde" not in merged
    assert merged["fecha_hasta"] == "2026-12-31"


@pytest.mark.parametrize(
    ("dataset_id", "date_field"),
    [
        (SoQLBuilder.PROCESOS_DATASET, "fecha_de_publicacion_del"),
        (SoQLBuilder.CONTRATOS_DATASET, "fecha_de_firma"),
    ],
)
def test_temporal_scope_projects_to_both_dataset_date_columns(dataset_id, date_field):
    query = SoQLBuilder().build(dataset_id, {
        "objeto": ["aseo"],
        "fecha_desde": "2026-06-15",
        "fecha_hasta": "2026-06-15",
    })

    assert f"{date_field} >= '2026-06-15'" in query
    assert f"{date_field} <= '2026-06-15'" in query


@pytest.mark.parametrize(
    ("params", "phrase"),
    [
        ({"dataset": "contratos", "fecha_desde": "2026-06-15", "fecha_hasta": "2026-06-15"}, "el 2026-06-15"),
        ({"dataset": "contratos", "fecha_desde": "2026-06-01", "fecha_hasta": "2026-06-30"}, "en junio de 2026"),
        ({"dataset": "contratos", "fecha_desde": "2026-01-01", "fecha_hasta": "2026-12-31"}, "en 2026"),
        ({"dataset": "contratos", "fecha_desde": "2025-01-01"}, "desde 2025"),
    ],
)
def test_interpretation_preserves_temporal_granularity(params, phrase):
    assert phrase in _build_interpretation(params)
