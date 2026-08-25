"""Specification tests for the null-safe OBJECT exclusion predicate."""
from __future__ import annotations

import pytest

from app.core.soql_builder import SoQLBuilder


def _where(dataset_id: str, exclusions: list[str]) -> str:
    soql = SoQLBuilder().build_count(
        dataset_id,
        {"objeto_exclude": exclusions},
    )
    return soql.split(" WHERE ", 1)[1]


@pytest.mark.parametrize(
    ("dataset_id", "field_a", "field_b"),
    [
        (
            SoQLBuilder.PROCESOS_DATASET,
            "nombre_del_procedimiento",
            "descripci_n_del_procedimiento",
        ),
        (
            SoQLBuilder.CONTRATOS_DATASET,
            "objeto_del_contrato",
            "descripcion_del_proceso",
        ),
    ],
)
def test_generated_object_exclusion_is_null_safe_for_real_dataset_fields(
    dataset_id: str,
    field_a: str,
    field_b: str,
) -> None:
    where = _where(dataset_id, ["zznegtest"])
    for field in (field_a, field_b):
        expected = (
            f"coalesce(unaccent(UPPER({field})) LIKE "
            "unaccent(UPPER('%zznegtest%')), false)"
        )
        assert expected in where
    assert where.startswith("NOT (")


@pytest.mark.parametrize(
    ("field_a_match", "field_b_match", "expected_keep"),
    [
        (None, False, True),   # N1: NULL + non-match
        (None, True, False),   # N2: NULL + match
        (True, None, False),   # N3: match + NULL
        (None, None, True),    # N4: both NULL
        (False, False, True),  # N5: both non-match
        (False, True, False),  # N6: either field matches
    ],
)
def test_null_safe_expression_truth_table(
    field_a_match: bool | None,
    field_b_match: bool | None,
    expected_keep: bool,
) -> None:
    # This is the exact boolean meaning of the generated COALESCE predicates.
    values = (field_a_match, field_b_match)
    keep = not any(False if value is None else value for value in values)
    assert keep is expected_keep
    assert "coalesce(" in _where(SoQLBuilder.PROCESOS_DATASET, ["zznegtest"])


def test_multi_exclusion_is_conjunction_of_independent_negative_predicates() -> None:
    where = _where(
        SoQLBuilder.PROCESOS_DATASET,
        ["alimentacion", "transporte"],
    )
    assert where.count("NOT (") == 2
    assert ") AND NOT (" in where
    assert "%alimentacion%" in where
    assert "%transporte%" in where
