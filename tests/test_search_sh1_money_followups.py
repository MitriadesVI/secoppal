"""SEARCH-SH1 monetary follow-up composition and mutation invariants."""
from __future__ import annotations

import re

import pytest

from app.core.followup_engine import detect_and_merge
from app.core.query_frame import frame_from_params
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder


@pytest.fixture(scope="module")
def router() -> QueryRouter:
    return QueryRouter()


@pytest.fixture(scope="module")
def previous(router: QueryRouter) -> dict:
    params = router.parse(
        "contratos adulto mayor gobernacion de atlantico en 2026"
    ).params
    params.update({"offset": 25, "_display_offset": 25, "_shown_total": 25})
    return params


def _merge(router: QueryRouter, previous: dict, text: str) -> tuple[str, dict, dict]:
    trace: dict = {}
    intent, merged = detect_and_merge(
        text,
        router.parse(text).params,
        frame_from_params(previous),
        mutation_trace=trace,
    )
    return intent, merged, trace


@pytest.mark.parametrize(
    "text",
    [
        "por más de 100 millones",
        "más de 100 millones",
        "superiores a 100 millones",
        "desde 100 millones",
        "mínimo 100 millones",
        "solo los mayores de 100 millones",
        "ahora por encima de 100 millones",
    ],
)
def test_minimum_families_survive_followup(
    router: QueryRouter, previous: dict, text: str,
) -> None:
    intent, merged, trace = _merge(router, previous, text)

    assert intent == "refine_filter"
    assert merged["valor_min"] == 100_000_000
    assert "valor_max" not in merged
    assert merged["objeto"] == ["adulto_mayor"]
    assert "ATLANTICO" in merged["entidad_resolved"]
    assert "valor_min" in trace["accepted_fields"]


@pytest.mark.parametrize(
    "text",
    ["menos de 500 millones", "hasta 500 millones", "máximo 500 millones"],
)
def test_maximum_families_survive_followup(
    router: QueryRouter, previous: dict, text: str,
) -> None:
    intent, merged, trace = _merge(router, previous, text)

    assert intent == "refine_filter"
    assert merged["valor_max"] == 500_000_000
    assert "valor_min" not in merged
    assert "valor_max" in trace["accepted_fields"]


@pytest.mark.parametrize(
    "text",
    ["entre 100 y 500 millones", "de 100 a 500 millones", "100-500 millones"],
)
def test_range_families_keep_both_bounds(
    router: QueryRouter, previous: dict, text: str,
) -> None:
    intent, merged, trace = _merge(router, previous, text)

    assert intent == "refine_filter"
    assert merged["valor_min"] == 100_000_000
    assert merged["valor_max"] == 500_000_000
    assert {"valor_min", "valor_max"} <= set(trace["accepted_fields"])


def test_same_year_noop_does_not_suppress_money(
    router: QueryRouter, previous: dict,
) -> None:
    intent, merged, trace = _merge(
        router, previous, "por mas de 100 millones en 2026",
    )

    assert intent == "refine_filter"
    assert merged["fecha_desde"] == previous["fecha_desde"]
    assert merged["fecha_hasta"] == previous["fecha_hasta"]
    assert merged["valor_min"] == 100_000_000
    assert {"fecha_desde", "fecha_hasta", "valor_min"} <= set(
        trace["accepted_fields"]
    )


def test_scope_and_money_compose(router: QueryRouter, previous: dict) -> None:
    intent, merged, trace = _merge(
        router, previous, "ahora en Bolivar por mas de 500 millones",
    )

    assert intent == "change_scope"
    assert merged["departamento_resolved"] == "Bolívar"
    assert "entidad_resolved" not in merged
    assert merged["valor_min"] == 500_000_000
    assert merged["objeto"] == ["adulto_mayor"]
    assert {"departamento_resolved", "valor_min"} <= set(trace["accepted_fields"])


def test_modality_and_money_compose(router: QueryRouter, previous: dict) -> None:
    intent, merged, trace = _merge(
        router,
        previous,
        "solo contratacion directa por menos de 300 millones",
    )

    assert intent == "refine_filter"
    assert merged["modalidad_include_procedure_ids"] == [
        "contratacion_directa",
        "contratacion_directa_con_ofertas",
    ]
    assert merged["valor_max"] == 300_000_000
    assert {"modalidad_include_procedure_ids", "valor_max"} <= set(
        trace["accepted_fields"]
    )


@pytest.mark.parametrize(
    ("dataset", "dataset_id", "field"),
    [
        ("contratos", SoQLBuilder.CONTRATOS_DATASET, "valor_del_contrato"),
        ("procesos", SoQLBuilder.PROCESOS_DATASET, "precio_base"),
    ],
)
def test_followup_money_uses_dataset_value_projection(
    router: QueryRouter, dataset: str, dataset_id: str, field: str,
) -> None:
    base = {
        "dataset": dataset,
        "objeto": ["adulto_mayor"],
        "fecha_desde": "2026-01-01",
        "fecha_hasta": "2026-12-31",
    }
    _intent, merged, _trace = _merge(router, base, "más de 100 millones")
    soql = SoQLBuilder().build(dataset_id, merged)

    assert f"{field} >= 100000000" in soql
    other = "precio_base" if field == "valor_del_contrato" else "valor_del_contrato"
    assert not re.search(rf"\b{other}\s+>=", soql)


def test_incompatible_previous_max_is_replaced_by_new_min(router: QueryRouter) -> None:
    base = {
        "dataset": "contratos",
        "objeto": ["adulto_mayor"],
        "valor_max": 50_000_000,
    }
    _intent, merged, trace = _merge(router, base, "más de 100 millones")

    assert merged["valor_min"] == 100_000_000
    assert "valor_max" not in merged
    assert "valor_max" in trace["transition_cleared_fields"]


def test_filter_mutation_invalidates_all_pagination_cursors(
    router: QueryRouter, previous: dict,
) -> None:
    _intent, merged, trace = _merge(router, previous, "más de 100 millones")

    assert "offset" not in merged
    assert "_display_offset" not in merged
    assert "_shown_total" not in merged
    assert {"offset", "_display_offset", "_shown_total"} <= set(
        trace["transition_cleared_fields"]
    )

