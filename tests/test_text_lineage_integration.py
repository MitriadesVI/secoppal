from __future__ import annotations

from datetime import date
import sqlite3

import pytest

from app.core.query_plan import compute_query_plan
from app.core.query_router import QueryRouter
from app.core.soql_builder import SoQLBuilder
from app.core.text_lineage import DescendantKind


TODAY = date(2026, 9, 12)


def _parse_pair(raw: str):
    router = QueryRouter()
    return (
        router.parse(raw, today=TODAY),
        router.parse(raw, today=TODAY, text_lineage_request_id=f"trace:{raw}"),
    )


def _answer(parsed, raw: str, start: int, end: int):
    trace = parsed.text_lineage
    assert trace is not None
    return trace.descendants_of_raw_span(
        start,
        end,
        request_id=f"trace:{raw}",
        raw_sha256=trace.raw_sha256,
        seam_sha256=trace.current_sha256,
    )


@pytest.mark.parametrize(
    "raw",
    [
        "procesos de mantenimiento por licitación pública",
        "contratos de aseo sin mantenimiento en 2025",
        "contratos de obra para parqes",
        "contratos del NIT 860002400 de aseo",
        "contratos   de   parques en La Estrella",
        "contratos de mantenimiento en El Banco",
        "contratos de mantenimiento en San Andrés",
        "contratos de mantenimiento en Galapa",
        "contratos de aseo en 2025 ordenados por mayor valor",
    ],
)
def test_lineage_opt_in_preserves_parser_plan_and_soql(raw: str) -> None:
    off, on = _parse_pair(raw)

    assert off.params == on.params
    assert off.needs_llm == on.needs_llm
    assert off.route_reason == on.route_reason
    assert off.positive_text == on.positive_text
    assert off.text_lineage is None
    assert on.text_lineage is not None
    assert "text_lineage" not in on.params

    off_plan = compute_query_plan(dict(off.params), raw, {"today": TODAY})
    on_plan = compute_query_plan(dict(on.params), raw, {"today": TODAY})
    assert off_plan == on_plan
    if off_plan.should_execute:
        builder = SoQLBuilder()
        dataset = builder.dataset_id_for(off_plan.params.get("dataset"))
        assert builder.build(dataset, off_plan.params) == builder.build(
            dataset, on_plan.params
        )


def test_con_vial_negated_occurrence_does_not_jump_to_positive_surface() -> None:
    raw = "contratos de con 2025 vial y parques sin con viales"
    _off, parsed = _parse_pair(raw)
    trace = parsed.text_lineage
    assert trace is not None
    negative_start = raw.index("con vial", raw.index("sin"))
    positive_vial_start = raw.index("vial")

    negative = _answer(parsed, raw, negative_start, negative_start + len("con vial"))
    positive = _answer(parsed, raw, positive_vial_start, positive_vial_start + len("vial"))

    assert negative.kind is DescendantKind.NO_SURVIVING_DESCENDANT
    assert positive.kind is DescendantKind.EXACT_DESCENDANT
    assert trace.current_text[slice(*positive.seam_intervals[0])] == "vial"
    assert all(
        trace.current_text[slice(*interval)] != "con vial"
        for interval in negative.seam_intervals
    )


def test_con_vial_off_on_select_same_synthetic_row_and_reject_decoys() -> None:
    raw = "contratos de con 2025 vial y parques sin con viales"
    off, on = _parse_pair(raw)
    assert off.params == on.params
    assert off.params["objeto"] == ["vial", "parques"]
    assert off.params["objeto_exclude"] == ["viales"]

    rows = [
        ("positive_vial_parks", "mantenimiento vial de parques", "2025-06-01"),
        ("widening_parks_only", "mantenimiento de parques", "2025-06-01"),
        ("negated_viales", "mantenimiento de viales y parques", "2025-06-01"),
        ("wrong_year", "mantenimiento vial de parques", "2024-06-01"),
    ]
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE contracts (id TEXT, objeto TEXT, signed TEXT)")
        connection.executemany("INSERT INTO contracts VALUES (?, ?, ?)", rows)
        selected = connection.execute(
            """
            SELECT id FROM contracts
            WHERE lower(objeto) LIKE '%vial%'
              AND lower(objeto) LIKE '%parques%'
              AND lower(objeto) NOT LIKE '%viales%'
              AND signed BETWEEN '2025-01-01' AND '2025-12-31'
            ORDER BY id
            """
        ).fetchall()

    assert selected == [("positive_vial_parks",)]


@pytest.mark.parametrize(
    ("raw", "surface", "expected"),
    [
        ("procesos de mantenimiento por licitacion publica", "licitacion publica", DescendantKind.NO_SURVIVING_DESCENDANT),
        ("contratos de aseo sin mantenimiento", "sin mantenimiento", DescendantKind.NO_SURVIVING_DESCENDANT),
        ("contratos de aseo en 2025", "2025", DescendantKind.NO_SURVIVING_DESCENDANT),
        ("contratos de obra mantenimiento", "contratos de obra", DescendantKind.NO_SURVIVING_DESCENDANT),
        ("contratos del NIT 860002400 de aseo", "860002400", DescendantKind.NO_SURVIVING_DESCENDANT),
        ("contratos de parqes", "parqes", DescendantKind.TRANSFORMED_DESCENDANT),
    ],
)
def test_required_pre_control_transformations_emit_causal_answers(
    raw: str,
    surface: str,
    expected: DescendantKind,
) -> None:
    _off, parsed = _parse_pair(raw)
    start = raw.index(surface)
    answer = _answer(parsed, raw, start, start + len(surface))
    assert answer.kind is expected


def test_control_language_is_sealed_before_control_interpretation() -> None:
    raw = "contratos de parques ordenados por mayor valor"
    _off, parsed = _parse_pair(raw)
    trace = parsed.text_lineage
    assert trace is not None

    assert "ordenados por mayor valor" in trace.current_text
    assert parsed.params["ordering_signal"] == "valor_desc"


def test_contract_type_receipts_are_opt_in_and_come_from_actual_match() -> None:
    raw = "contratos de obra mantenimiento"
    off, on = _parse_pair(raw)

    assert off.contract_type_interpretation is not None
    assert on.contract_type_interpretation is not None
    assert off.contract_type_interpretation.mutation_receipts == ()
    receipts = on.contract_type_interpretation.mutation_receipts
    assert len(receipts) == 1
    assert receipts[0].input_text == "contratos de obra"
    assert receipts[0].operation == "MASK"


def test_repeated_protected_name_occurrences_keep_distinct_raw_identity() -> None:
    raw = "contratos de mantenimiento en La Estrella y La Estrella"
    _off, parsed = _parse_pair(raw)
    first_start = raw.index("La Estrella")
    second_start = raw.index("La Estrella", first_start + 1)

    first = _answer(parsed, raw, first_start, first_start + len("La Estrella"))
    second = _answer(parsed, raw, second_start, second_start + len("La Estrella"))
    assert first.kind is DescendantKind.TRANSFORMED_DESCENDANT
    assert second.kind is DescendantKind.TRANSFORMED_DESCENDANT
    assert first.seam_intervals != second.seam_intervals


def test_known_universe_then_temporal_offset_behavior_is_preserved() -> None:
    raw = "dame todos los registros del dataset de contratos en 2025 de parques"
    router = QueryRouter()

    with pytest.raises(ValueError) as off_error:
        router.parse(raw, today=TODAY)
    with pytest.raises(ValueError) as on_error:
        router.parse(raw, today=TODAY, text_lineage_request_id=f"trace:{raw}")
    assert str(off_error.value) == str(on_error.value)


def test_opt_in_preserves_invalid_query_exception() -> None:
    router = QueryRouter()

    with pytest.raises(TypeError) as off_error:
        router.parse(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError) as on_error:
        router.parse(None, text_lineage_request_id="invalid")  # type: ignore[arg-type]
    assert str(off_error.value) == str(on_error.value)
